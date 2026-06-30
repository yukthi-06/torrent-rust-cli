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
        let tracker = TrackerClient::new();
        let mut peer_id = [0u8; 20];
        peer_id[0..8].copy_from_slice(b"-AG0001-");

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
                            .announce_http(
                                &http_fallback,
                                self.magnet.info_hash.0,
                                peer_id,
                                6881,
                            )
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

        if all_peers.is_empty() {
            anyhow::bail!("No peers found from any tracker");
        }

        info!("Total peers discovered: {}", all_peers.len());

        // Deduplicate peers
        all_peers.sort();
        all_peers.dedup();

        for peer_addr in &all_peers {
            info!("Attempting metadata fetch from {}", peer_addr);
            match self.try_fetch_from_peer(*peer_addr, peer_id).await {
                Ok(meta) => return Ok(meta),
                Err(e) => {
                    warn!("Failed to fetch metadata from {}: {}", peer_addr, e);
                }
            }
        }

        anyhow::bail!(
            "Could not fetch metadata from any of {} peers",
            all_peers.len()
        )
    }

    async fn try_fetch_from_peer(
        &self,
        addr: std::net::SocketAddr,
        our_peer_id: [u8; 20],
    ) -> Result<torrent_core::meta::TorrentMeta, anyhow::Error> {
        let mut stream =
            timeout(Duration::from_secs(10), TcpStream::connect(addr)).await??;

        let handshake = Handshake::new(self.magnet.info_hash.0, our_peer_id);
        stream.write_all(&handshake.serialize()).await?;

        let response_hs =
            timeout(Duration::from_secs(10), Handshake::read(&mut stream)).await??;

        if response_hs.info_hash != self.magnet.info_hash.0 {
            anyhow::bail!("Info hash mismatch in handshake");
        }

        if response_hs.extensions[5] & 0x10 == 0 {
            anyhow::bail!("Peer does not support extension protocol");
        }

        info!("Peer {} supports extension protocol, sending extended handshake", addr);

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
                timeout(Duration::from_secs(10), PeerMessage::read(&mut stream)).await??;
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
                | PeerMessage::KeepAlive => {
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
                    timeout(Duration::from_secs(10), PeerMessage::read(&mut stream))
                        .await??;
                if let PeerMessage::Extended { msg_id, payload } = msg {
                    if msg_id == ut_metadata_id {
                        let mut offset = 0;
                        let dict = torrent_core::bencode::Bencode::decode_inner(
                            &payload,
                            &mut offset,
                        );
                        if let Ok(Bencode::Dict(d)) = dict {
                            if let Some(Bencode::Int(msg_type)) =
                                d.get(b"msg_type".as_ref())
                            {
                                if *msg_type == 1 {
                                    // data message
                                    let data = &payload[offset..];
                                    let start = i * 16384;
                                    let end =
                                        (start + data.len()).min(metadata_size);
                                    metadata_bytes[start..end]
                                        .copy_from_slice(&data[..end - start]);
                                    pieces_received += 1;
                                    got_piece = true;
                                    break;
                                } else if *msg_type == 2 {
                                    // reject message
                                    anyhow::bail!(
                                        "Peer rejected metadata piece {}",
                                        i
                                    );
                                }
                            }
                        }
                    }
                }
                // Otherwise skip non-extension messages (Have, Bitfield, etc.)
            }
            if !got_piece {
                anyhow::bail!(
                    "Timed out waiting for metadata piece {} from {}",
                    i,
                    addr
                );
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
        if hash[..] != self.magnet.info_hash.0 {
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
