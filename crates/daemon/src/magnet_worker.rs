use crate::engine;
use crate::server::TorrentState;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use torrent_core::bencode::Bencode;
use torrent_core::magnet::MagnetLink;
use torrent_core::TorrentId;
use torrent_peer::protocol::{Handshake, PeerMessage};
use torrent_tracker::TrackerClient;
use tracing::{error, info, warn};

pub struct MagnetWorker {
    pub id: TorrentId,
    pub magnet: MagnetLink,
    pub download_dir: PathBuf,
    pub state: Arc<Mutex<TorrentState>>,
}

impl MagnetWorker {
    pub async fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            info!(
                "Starting magnet metadata fetch for: {}",
                self.magnet.info_hash
            );

            match self.fetch_metadata().await {
                Ok(meta) => {
                    info!("Successfully fetched metadata for magnet link!");

                    let size = match &meta.info.mode {
                        torrent_core::meta::FileMode::Single { length } => *length,
                        torrent_core::meta::FileMode::Multi { files } => {
                            files.iter().map(|f| f.length).sum()
                        }
                    };

                    // Update state with real metadata
                    {
                        let mut lock = self.state.lock().await;
                        lock.name = meta.info.name.clone();
                        lock.size = size;
                        lock.status = "Downloading".to_string();
                    }

                    let mut peer_id = [0u8; 20];
                    peer_id[0..8].copy_from_slice(b"-AG0001-");

                    let downloader = Arc::new(engine::TorrentDownloader::new(
                        self.id,
                        meta,
                        self.download_dir.clone(),
                        peer_id,
                        Arc::clone(&self.state),
                    ));

                    downloader.start().await;
                }
                Err(e) => {
                    error!("Failed to fetch metadata: {}", e);
                    let mut lock = self.state.lock().await;
                    lock.status = format!("Failed: {}", e);
                }
            }
        });
    }

    async fn fetch_metadata(&self) -> Result<torrent_core::meta::TorrentMeta, anyhow::Error> {
        let mut peer_id = [0u8; 20];
        peer_id[0..8].copy_from_slice(b"-AG0001-");

        let mut last_error = String::from("no attempts made");

        // Retry loop: re-announce and try peers up to 3 times
        for attempt in 0..3 {
            if attempt > 0 {
                info!(
                    "Metadata fetch retry {}/3, re-announcing to trackers...",
                    attempt + 1
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            let all_peers = self.discover_peers(peer_id).await;

            if all_peers.is_empty() {
                last_error = "No peers found from any tracker".to_string();
                continue;
            }

            info!(
                "Attempt {}: discovered {} unique peers",
                attempt + 1,
                all_peers.len()
            );

            // Try peers concurrently in batches of 5
            for chunk in all_peers.chunks(5) {
                let mut handles = Vec::new();
                for &peer_addr in chunk {
                    let info_hash = self.magnet.info_hash;
                    handles.push(tokio::spawn(async move {
                        let result =
                            Self::try_fetch_from_peer_static(peer_addr, info_hash.0, peer_id)
                                .await;
                        (peer_addr, result)
                    }));
                }

                for handle in handles {
                    if let Ok((peer_addr, result)) = handle.await {
                        match result {
                            Ok(meta) => {
                                info!("Successfully fetched metadata from {}", peer_addr);
                                return Ok(meta);
                            }
                            Err(e) => {
                                let err_msg = format!("{}: {}", peer_addr, e);
                                warn!("Failed to fetch metadata from {}", err_msg);
                                last_error = err_msg;
                            }
                        }
                    }
                }
            }
        }

        anyhow::bail!("Could not fetch metadata after 3 attempts. Last error: {}", last_error)
    }

    async fn discover_peers(&self, peer_id: [u8; 20]) -> Vec<std::net::SocketAddr> {
        let tracker = TrackerClient::new();
        let mut all_peers = Vec::new();

        for tr in &self.magnet.trackers {
            info!("Announcing to tracker: {}", tr);
            let res = if tr.starts_with("udp://") {
                let host_port = tr.trim_start_matches("udp://");
                match tracker
                    .announce_udp(host_port, self.magnet.info_hash.0, peer_id, 6881)
                    .await
                {
                    Ok(p) => Ok(p),
                    Err(e) => {
                        warn!("UDP tracker {} failed: {}, trying HTTP fallback", tr, e);
                        let http_fallback = tr.replace("udp://", "http://");
                        tracker
                            .announce_http(&http_fallback, self.magnet.info_hash.0, peer_id, 6881)
                            .await
                    }
                }
            } else if tr.starts_with("http://") {
                tracker
                    .announce_http(tr, self.magnet.info_hash.0, peer_id, 6881)
                    .await
            } else {
                warn!("Skipping unsupported tracker: {}", tr);
                continue;
            };
            match res {
                Ok(peers) => {
                    info!("Tracker {} returned {} peers", tr, peers.len());
                    all_peers.extend(peers);
                }
                Err(e) => {
                    warn!("Tracker {} failed: {}", tr, e);
                }
            }
        }

        // Deduplicate
        all_peers.sort();
        all_peers.dedup();
        all_peers
    }

    async fn try_fetch_from_peer_static(
        addr: std::net::SocketAddr,
        info_hash: [u8; 20],
        our_peer_id: [u8; 20],
    ) -> Result<torrent_core::meta::TorrentMeta, anyhow::Error> {
        let mut stream = timeout(Duration::from_secs(5), TcpStream::connect(addr)).await??;

        let handshake = Handshake::new(info_hash, our_peer_id);
        stream.write_all(&handshake.serialize()).await?;

        let response_hs = timeout(Duration::from_secs(5), Handshake::read(&mut stream)).await??;

        if response_hs.info_hash != info_hash {
            anyhow::bail!("Info hash mismatch in handshake");
        }

        if response_hs.extensions[5] & 0x10 == 0 {
            anyhow::bail!("Peer does not support extension protocol");
        }

        info!(
            "Peer {} supports extension protocol, sending extended handshake",
            addr
        );

        // Send extended handshake
        let mut m = BTreeMap::new();
        m.insert(b"ut_metadata".to_vec(), Bencode::Int(1));
        let mut root = BTreeMap::new();
        root.insert(b"m".to_vec(), Bencode::Dict(m));

        let ext_msg = PeerMessage::Extended {
            msg_id: 0,
            payload: Bencode::Dict(root).encode(),
        };
        stream.write_all(&ext_msg.serialize()).await?;

        // Wait for their extended handshake
        let mut ut_metadata_id = None;
        let mut metadata_size = None;

        for attempt in 0..20 {
            let msg =
                timeout(Duration::from_secs(5), PeerMessage::read(&mut stream)).await??;
            match &msg {
                PeerMessage::Extended { msg_id, payload } if *msg_id == 0 => {
                    let dict = Bencode::decode(payload)?;
                    if let Bencode::Dict(d) = dict {
                        if let Some(Bencode::Dict(m_dict)) = d.get(b"m".as_ref()) {
                            if let Some(Bencode::Int(id)) =
                                m_dict.get(b"ut_metadata".as_ref())
                            {
                                ut_metadata_id = Some(*id as u8);
                            }
                        }
                        if let Some(Bencode::Int(size)) =
                            d.get(b"metadata_size".as_ref())
                        {
                            metadata_size = Some(*size as usize);
                        }
                    }
                    break;
                }
                PeerMessage::Bitfield(_)
                | PeerMessage::Have { .. }
                | PeerMessage::Unchoke
                | PeerMessage::Choke
                | PeerMessage::Interested
                | PeerMessage::NotInterested
                | PeerMessage::KeepAlive
                | PeerMessage::Port(_)
                | PeerMessage::Unknown { .. } => {
                    // Normal messages before extended handshake, skip them
                    continue;
                }
                other => {
                    warn!(
                        "Peer {}: unexpected message while waiting for ext handshake (attempt {}): {:?}",
                        addr, attempt, std::mem::discriminant(other)
                    );
                }
            }
        }

        let ut_metadata_id = ut_metadata_id
            .ok_or_else(|| anyhow::anyhow!("Peer doesn't support ut_metadata"))?;
        let metadata_size = metadata_size
            .ok_or_else(|| anyhow::anyhow!("No metadata_size in extended handshake"))?;

        info!(
            "Peer {} has metadata: size={}, ut_metadata_id={}",
            addr, metadata_size, ut_metadata_id
        );

        if metadata_size > 10 * 1024 * 1024 {
            anyhow::bail!("Metadata too large: {} bytes", metadata_size);
        }

        let num_pieces = metadata_size.div_ceil(16384);
        let mut metadata_bytes = vec![0u8; metadata_size];
        let mut pieces_received = 0;

        for i in 0..num_pieces {
            let mut req = BTreeMap::new();
            req.insert(b"msg_type".to_vec(), Bencode::Int(0));
            req.insert(b"piece".to_vec(), Bencode::Int(i as i64));
            let payload = Bencode::Dict(req).encode();

            let req_msg = PeerMessage::Extended {
                msg_id: ut_metadata_id,
                payload,
            };
            stream.write_all(&req_msg.serialize()).await?;

            // Read response, skip non-extension messages
            let mut got_piece = false;
            for _ in 0..30 {
                let msg =
                    timeout(Duration::from_secs(5), PeerMessage::read(&mut stream))
                        .await??;
                if let PeerMessage::Extended { msg_id, payload } = msg {
                    if msg_id == ut_metadata_id {
                        let mut offset = 0;
                        let dict =
                            torrent_core::bencode::Bencode::decode_inner(&payload, &mut offset);
                        if let Ok(Bencode::Dict(d)) = dict {
                            if let Some(Bencode::Int(msg_type)) = d.get(b"msg_type".as_ref()) {
                                if *msg_type == 1 {
                                    // data message
                                    let data = &payload[offset..];
                                    let start = i * 16384;
                                    let end = (start + data.len()).min(metadata_size);
                                    metadata_bytes[start..end]
                                        .copy_from_slice(&data[..end - start]);
                                    pieces_received += 1;
                                    got_piece = true;
                                    break;
                                } else if *msg_type == 2 {
                                    // reject message
                                    anyhow::bail!("Peer rejected metadata piece {}", i);
                                }
                            }
                        }
                    }
                }
                // Otherwise skip non-extension messages (Have, Bitfield, etc.)
            }
            if !got_piece {
                anyhow::bail!("Timed out waiting for metadata piece {} from {}", i, addr);
            }
        }

        info!(
            "Received {}/{} metadata pieces from {}",
            pieces_received, num_pieces, addr
        );

        // Verify SHA1
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.update(&metadata_bytes);
        let hash = hasher.finalize();
        if hash[..] != info_hash {
            anyhow::bail!("Metadata hash mismatch!");
        }

        info!("Metadata hash verified successfully!");

        // We have the info dict! Now synthesize a full TorrentMeta
        let info_dict = Bencode::decode(&metadata_bytes)?;
        let mut root = BTreeMap::new();
        root.insert(b"info".to_vec(), info_dict);

        let full_meta_bytes = Bencode::Dict(root).encode();
        let meta = torrent_core::meta::TorrentMeta::from_bytes(&full_meta_bytes)?;

        Ok(meta)
    }
}
