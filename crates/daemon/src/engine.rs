use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use torrent_core::meta::{FileMode, TorrentMeta};
use torrent_core::TorrentId;
use torrent_peer::protocol::{Handshake, PeerMessage};
use torrent_tracker::TrackerClient;
use tracing::{error, info};

pub struct TorrentDownloader {
    pub id: TorrentId,
    pub meta: TorrentMeta,
    pub download_dir: PathBuf,
    pub peer_id: [u8; 20],
    pub state: Arc<Mutex<super::server::TorrentState>>,
}

impl TorrentDownloader {
    pub fn new(
        id: TorrentId,
        meta: TorrentMeta,
        download_dir: PathBuf,
        peer_id: [u8; 20],
        state: Arc<Mutex<super::server::TorrentState>>,
    ) -> Self {
        Self {
            id,
            meta,
            download_dir,
            peer_id,
            state,
        }
    }

    pub async fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            info!(
                "Starting download worker for torrent: {}",
                self.meta.info.name
            );

            // 1. Initialize files on disk
            if let Err(e) = self.initialize_files() {
                error!("Failed to initialize files for torrent {}: {}", self.id, e);
                return;
            }

            // 2. Announce loop
            loop {
                let mut trackers = Vec::new();
                trackers.push(self.meta.announce.clone());
                if let Some(list) = &self.meta.announce_list {
                    for tier in list {
                        for url in tier {
                            if !trackers.contains(url) {
                                trackers.push(url.clone());
                            }
                        }
                    }
                }

                let mut peers = Vec::new();
                for tracker_url in &trackers {
                    let tracker = TrackerClient::new();
                    if tracker_url.starts_with("udp://") {
                        let host_port = tracker_url.trim_start_matches("udp://");
                        info!("Trying UDP tracker: {}", tracker_url);
                        match tracker
                            .announce_udp(host_port, self.meta.info_hash.0, self.peer_id, 6881)
                            .await
                        {
                            Ok(p) => {
                                if !p.is_empty() {
                                    peers = p;
                                    info!("Tracker {} returned {} peers", tracker_url, peers.len());
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Tracker announce failed for {}: {}", tracker_url, e);
                                // Fallback: try same tracker over HTTP
                                let http_fallback = tracker_url.replace("udp://", "http://");
                                info!("Trying HTTP fallback: {}", http_fallback);
                                match tracker
                                    .announce_http(
                                        &http_fallback,
                                        self.meta.info_hash.0,
                                        self.peer_id,
                                        6881,
                                    )
                                    .await
                                {
                                    Ok(p) => {
                                        if !p.is_empty() {
                                            peers = p;
                                            info!("HTTP fallback {} returned {} peers", http_fallback, peers.len());
                                            break;
                                        }
                                    }
                                    Err(he) => {
                                        error!("HTTP fallback failed for {}: {}", http_fallback, he);
                                    }
                                }
                            }
                        }
                    } else if tracker_url.starts_with("http://") {
                        info!("Trying HTTP tracker: {}", tracker_url);
                        match tracker
                            .announce_http(tracker_url, self.meta.info_hash.0, self.peer_id, 6881)
                            .await
                        {
                            Ok(p) => {
                                if !p.is_empty() {
                                    peers = p;
                                    info!("Tracker {} returned {} peers", tracker_url, peers.len());
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Tracker announce failed for {}: {}", tracker_url, e);
                            }
                        }
                    }
                }

                info!(
                    "Discovered {} peers from tracker for torrent {}",
                    peers.len(),
                    self.id
                );

                // Start connection workers for each peer
                for peer in peers {
                    let self_clone = Arc::clone(&self);
                    tokio::spawn(async move {
                        if let Err(e) = self_clone.handle_peer(peer).await {
                            // Peer disconnected
                            tracing::debug!("Peer connection to {} ended: {}", peer, e);
                        }
                    });
                }

                // Re-announce every 60 seconds
                sleep(Duration::from_secs(60)).await;
            }
        });
    }

    fn initialize_files(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.download_dir)?;
        match &self.meta.info.mode {
            FileMode::Single { length } => {
                let file_path = self.download_dir.join(&self.meta.info.name);
                if !file_path.exists() {
                    let file = File::create(file_path)?;
                    file.set_len(*length)?;
                }
            }
            FileMode::Multi { files } => {
                let parent_dir = self.download_dir.join(&self.meta.info.name);
                std::fs::create_dir_all(&parent_dir)?;
                for f in files {
                    if f.path.is_empty() {
                        continue;
                    }
                    let mut full_path = parent_dir.clone();
                    for part in &f.path {
                        full_path.push(part);
                    }
                    if let Some(p) = full_path.parent() {
                        std::fs::create_dir_all(p)?;
                    }
                    if !full_path.exists() {
                        let file = File::create(full_path)?;
                        file.set_len(f.length)?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_peer(&self, addr: SocketAddr) -> Result<(), anyhow::Error> {
        let mut stream = TcpStream::connect(addr).await?;

        // Handshake
        let handshake = Handshake::new(self.meta.info_hash.0, self.peer_id);
        stream.write_all(&handshake.serialize()).await?;

        let server_handshake = Handshake::read(&mut stream).await?;
        if server_handshake.info_hash != self.meta.info_hash.0 {
            anyhow::bail!("Info hash mismatch");
        }

        // Send Interested and Unchoke
        stream
            .write_all(&PeerMessage::Interested.serialize())
            .await?;
        stream.write_all(&PeerMessage::Unchoke.serialize()).await?;

        let mut piece_index = 0;
        let mut block_offset = 0;
        let piece_length = self.meta.info.piece_length as u32;
        let total_pieces = self.meta.info.pieces.len() as u32;

        // Simple download loop from peer
        loop {
            let msg = PeerMessage::read(&mut stream).await?;
            match msg {
                PeerMessage::KeepAlive => {}
                PeerMessage::Choke => {
                    // Peer choked us. Currently we pause requests.
                }
                PeerMessage::Unchoke => {
                    if piece_index < total_pieces {
                        let req = PeerMessage::Request {
                            index: piece_index,
                            begin: block_offset,
                            length: 16384,
                        };
                        stream.write_all(&req.serialize()).await?;
                    }
                }
                PeerMessage::Piece {
                    index,
                    begin,
                    block,
                } => {
                    // Write block to disk
                    self.write_block(index, begin, &block)?;

                    let block_len = block.len() as u64;
                    {
                        let mut lock = self.state.lock().await;
                        lock.downloaded += block_len;
                        if lock.downloaded >= lock.size {
                            lock.status = "Completed".to_string();
                        } else {
                            lock.status = "Downloading".to_string();
                        }
                    }

                    // Advance to next block
                    block_offset += block.len() as u32;
                    if block_offset >= piece_length {
                        block_offset = 0;
                        piece_index += 1;
                    }

                    if piece_index < total_pieces {
                        let req = PeerMessage::Request {
                            index: piece_index,
                            begin: block_offset,
                            length: 16384,
                        };
                        stream.write_all(&req.serialize()).await?;
                    }
                }
                _ => {}
            }
        }
    }

    fn write_block(&self, piece_index: u32, offset: u32, data: &[u8]) -> std::io::Result<()> {
        let absolute_offset = (piece_index as u64 * self.meta.info.piece_length) + offset as u64;

        match &self.meta.info.mode {
            FileMode::Single { .. } => {
                let file_path = self.download_dir.join(&self.meta.info.name);
                let mut file = OpenOptions::new().write(true).open(file_path)?;
                file.seek(SeekFrom::Start(absolute_offset))?;
                file.write_all(data)?;
            }
            FileMode::Multi { files } => {
                // Find which file the absolute_offset falls into
                let parent_dir = self.download_dir.join(&self.meta.info.name);
                let mut current_file_start = 0u64;
                for f in files {
                    let file_len = f.length;
                    if absolute_offset >= current_file_start
                        && absolute_offset < current_file_start + file_len
                    {
                        let mut full_path = parent_dir.clone();
                        for part in &f.path {
                            full_path.push(part);
                        }
                        let mut file = OpenOptions::new().write(true).open(full_path)?;
                        let relative_offset = absolute_offset - current_file_start;
                        file.seek(SeekFrom::Start(relative_offset))?;
                        file.write_all(data)?;
                        break;
                    }
                    current_file_start += file_len;
                }
            }
        }
        Ok(())
    }
}
