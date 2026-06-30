use crate::engine;
use crate::server::TorrentState;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

                    // We must spawn the TorrentDownloader now
                    let mut peer_id = [0u8; 20];
                    peer_id[0..8].copy_from_slice(b"-AG0001-");

                    let downloader = Arc::new(engine::TorrentDownloader::new(
                        self.id,
                        meta,
                        self.download_dir.clone(),
                        peer_id,
                        Arc::clone(&self.state),
                    ));

                    {
                        let mut lock = self.state.lock().await;
                        lock.status = "Downloading".to_string();
                    }

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
            let res = if tr.starts_with("udp://") {
                tracker
                    .announce_udp(tr, self.magnet.info_hash.0, peer_id, 6881)
                    .await
            } else {
                tracker
                    .announce_http(tr, self.magnet.info_hash.0, peer_id, 6881)
                    .await
            };
            if let Ok(peers) = res {
                all_peers.extend(peers);
            }
        }

        if all_peers.is_empty() {
            anyhow::bail!("No peers found from trackers");
        }

        for peer_addr in all_peers {
            info!("Attempting metadata fetch from {}", peer_addr);
            if let Ok(meta) = self.try_fetch_from_peer(peer_addr, peer_id).await {
                return Ok(meta);
            }
        }

        anyhow::bail!("Could not fetch metadata from any peer")
    }

    async fn try_fetch_from_peer(
        &self,
        addr: std::net::SocketAddr,
        our_peer_id: [u8; 20],
    ) -> Result<torrent_core::meta::TorrentMeta, anyhow::Error> {
        let mut stream = timeout(Duration::from_secs(5), TcpStream::connect(addr)).await??;

        let handshake = Handshake::new(self.magnet.info_hash.0, our_peer_id);
        stream.write_all(&handshake.serialize()).await?;

        let response_hs = timeout(Duration::from_secs(5), Handshake::read(&mut stream)).await??;

        if response_hs.info_hash != self.magnet.info_hash.0 {
            anyhow::bail!("Info hash mismatch in handshake");
        }

        if response_hs.extensions[5] & 0x10 == 0 {
            anyhow::bail!("Peer does not support extension protocol");
        }

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

        for _ in 0..10 {
            let msg = timeout(Duration::from_secs(5), PeerMessage::read(&mut stream)).await??;
            if let PeerMessage::Extended { msg_id, payload } = msg {
                if msg_id == 0 {
                    let dict = Bencode::decode(&payload)?;
                    if let Bencode::Dict(d) = dict {
                        if let Some(Bencode::Dict(m_dict)) = d.get(b"m".as_ref()) {
                            if let Some(Bencode::Int(id)) = m_dict.get(b"ut_metadata".as_ref()) {
                                ut_metadata_id = Some(*id as u8);
                            }
                        }
                        if let Some(Bencode::Int(size)) = d.get(b"metadata_size".as_ref()) {
                            metadata_size = Some(*size as usize);
                        }
                    }
                    break;
                }
            }
        }

        let ut_metadata_id =
            ut_metadata_id.ok_or_else(|| anyhow::anyhow!("Peer doesn't support ut_metadata"))?;
        let metadata_size = metadata_size
            .ok_or_else(|| anyhow::anyhow!("No metadata_size in extended handshake"))?;

        if metadata_size > 10 * 1024 * 1024 {
            // 10 MB sanity check
            anyhow::bail!("Metadata too large");
        }

        let num_pieces = (metadata_size + 16383) / 16384;
        let mut metadata_bytes = vec![0u8; metadata_size];

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

            // Read response
            for _ in 0..20 {
                let msg = timeout(Duration::from_secs(5), PeerMessage::read(&mut stream)).await??;
                if let PeerMessage::Extended { msg_id, payload } = msg {
                    if msg_id == ut_metadata_id {
                        let mut offset = 0;
                        let dict =
                            torrent_core::bencode::Bencode::decode_inner(&payload, &mut offset);
                        if let Ok(Bencode::Dict(d)) = dict {
                            if let Some(Bencode::Int(msg_type)) = d.get(b"msg_type".as_ref()) {
                                if *msg_type == 1 {
                                    let data = &payload[offset..];
                                    let start = i * 16384;
                                    let end = (start + data.len()).min(metadata_size);
                                    metadata_bytes[start..end]
                                        .copy_from_slice(&data[..end - start]);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Verify SHA1
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.update(&metadata_bytes);
        let hash = hasher.finalize();
        if hash[..] != self.magnet.info_hash.0 {
            anyhow::bail!("Metadata hash mismatch!");
        }

        // We have the info dict! Now synthesize a full TorrentMeta
        // We wrap it in a root dict: { "info": <metadata_bytes> }
        let info_dict = Bencode::decode(&metadata_bytes)?;
        let mut root = BTreeMap::new();
        root.insert(b"info".to_vec(), info_dict);

        let full_meta_bytes = Bencode::Dict(root).encode();
        let meta = torrent_core::meta::TorrentMeta::from_bytes(&full_meta_bytes)?;

        Ok(meta)
    }
}
