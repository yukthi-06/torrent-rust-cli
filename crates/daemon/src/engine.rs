use sha1::{Digest, Sha1};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
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
use tracing::{error, info, warn};

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

            // 2. Verify existing pieces on disk and resume from correct position
            let completed_pieces = self.verify_pieces();
            let total_pieces = self.meta.info.pieces.len() as u32;
            let first_missing = completed_pieces
                .iter()
                .position(|&done| !done)
                .map(|i| i as u32)
                .unwrap_or(total_pieces);

            let verified_downloaded =
                completed_pieces
                    .iter()
                    .enumerate()
                    .fold(0u64, |acc, (i, &done)| {
                        if done {
                            let piece_len = if i as u32 == total_pieces - 1 {
                                // Last piece may be shorter
                                let total_size = {
                                    match &self.meta.info.mode {
                                        FileMode::Single { length } => *length,
                                        FileMode::Multi { files } => {
                                            files.iter().map(|f| f.length).sum()
                                        }
                                    }
                                };
                                let full_pieces_size =
                                    (total_pieces as u64 - 1) * self.meta.info.piece_length;
                                total_size - full_pieces_size
                            } else {
                                self.meta.info.piece_length
                            };
                            acc + piece_len
                        } else {
                            acc
                        }
                    });

            {
                let mut lock = self.state.lock().await;
                let total_size = lock.size;
                lock.downloaded = verified_downloaded.min(total_size);
                lock.status = if first_missing >= total_pieces {
                    "Completed".to_string()
                } else {
                    "Downloading".to_string()
                };
            }

            if first_missing >= total_pieces {
                info!(
                    "Torrent {} already complete, skipping download",
                    self.meta.info.name
                );
                return;
            }

            info!(
                "Torrent {}: {}/{} pieces verified, resuming from piece {}",
                self.meta.info.name,
                completed_pieces.iter().filter(|&&d| d).count(),
                total_pieces,
                first_missing
            );

            // 3. Announce loop
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
                                            info!(
                                                "HTTP fallback {} returned {} peers",
                                                http_fallback,
                                                peers.len()
                                            );
                                            break;
                                        }
                                    }
                                    Err(he) => {
                                        error!(
                                            "HTTP fallback failed for {}: {}",
                                            http_fallback, he
                                        );
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
                        if let Err(e) = self_clone.handle_peer(peer, first_missing).await {
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

    /// Reads each piece from disk and SHA1-hashes it against the expected piece hash.
    /// Returns a Vec<bool> where true means the piece is complete and verified.
    fn verify_pieces(&self) -> Vec<bool> {
        let total_pieces = self.meta.info.pieces.len();
        let total_size: u64 = match &self.meta.info.mode {
            FileMode::Single { length } => *length,
            FileMode::Multi { files } => files.iter().map(|f| f.length).sum(),
        };

        let mut completed = vec![false; total_pieces];

        for (piece_idx, completed_flag) in completed.iter_mut().enumerate() {
            let expected_hash = &self.meta.info.pieces[piece_idx];

            // Calculate actual byte range for this piece
            let piece_start = piece_idx as u64 * self.meta.info.piece_length;
            let piece_end = (piece_start + self.meta.info.piece_length).min(total_size);
            if piece_start >= total_size {
                break;
            }
            let actual_len = (piece_end - piece_start) as usize;

            // Read piece bytes from disk
            let piece_data = match self.read_piece_from_disk(piece_start, actual_len) {
                Ok(d) => d,
                Err(e) => {
                    warn!("Piece {}: failed to read from disk: {}", piece_idx, e);
                    continue;
                }
            };



            // SHA1 hash check
            let mut hasher = Sha1::new();
            hasher.update(&piece_data);
            let hash: [u8; 20] = hasher.finalize().into();
            if &hash == expected_hash {
                *completed_flag = true;
            }
        }

        completed
    }

    /// Reads `len` bytes starting at absolute byte offset `offset` across the torrent's files.
    fn read_piece_from_disk(&self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        let mut buf_pos = 0;

        match &self.meta.info.mode {
            FileMode::Single { .. } => {
                let file_path = self.download_dir.join(&self.meta.info.name);
                let mut file = File::open(&file_path)?;
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut buf)?;
            }
            FileMode::Multi { files } => {
                let parent_dir = self.download_dir.join(&self.meta.info.name);
                let mut current_file_start = 0u64;
                let mut remaining_offset = offset;

                for f in files {
                    let file_end = current_file_start + f.length;

                    if remaining_offset >= f.length {
                        remaining_offset -= f.length;
                        current_file_start = file_end;
                        continue;
                    }

                    // This file contributes to the piece
                    let mut full_path = parent_dir.clone();
                    for part in &f.path {
                        full_path.push(part);
                    }

                    if !full_path.exists() {
                        current_file_start = file_end;
                        continue;
                    }

                    let mut file = File::open(&full_path)?;
                    file.seek(SeekFrom::Start(remaining_offset))?;

                    let available = f.length - remaining_offset;
                    let to_read = (len - buf_pos).min(available as usize);
                    file.read_exact(&mut buf[buf_pos..buf_pos + to_read])?;
                    buf_pos += to_read;
                    remaining_offset = 0;
                    current_file_start = file_end;

                    if buf_pos >= len {
                        break;
                    }
                }
            }
        }

        Ok(buf)
    }

    fn initialize_files(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.download_dir)?;
        match &self.meta.info.mode {
            FileMode::Single { length } => {
                let file_path = self.download_dir.join(&self.meta.info.name);
                if !file_path.exists() {
                    let file = File::create(&file_path)?;
                    file.set_len(*length)?;
                } else {
                    let file = OpenOptions::new().write(true).open(&file_path)?;
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
                        let file = File::create(&full_path)?;
                        file.set_len(f.length)?;
                    } else {
                        let file = OpenOptions::new().write(true).open(&full_path)?;
                        file.set_len(f.length)?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_peer(
        &self,
        addr: SocketAddr,
        start_piece_index: u32,
    ) -> Result<(), anyhow::Error> {
        let mut stream = TcpStream::connect(addr).await?;

        // Handshake
        let handshake = Handshake::new(self.meta.info_hash.0, self.peer_id);
        stream.write_all(&handshake.serialize()).await?;

        let server_handshake = Handshake::read(&mut stream).await?;
        if server_handshake.info_hash != self.meta.info_hash.0 {
            anyhow::bail!("Info hash mismatch");
        }

        // Successfully connected! Track peer connection count
        {
            let mut lock = self.state.lock().await;
            lock.peers_connected += 1;
        }

        struct PeerGuard {
            state: Arc<Mutex<crate::server::TorrentState>>,
        }
        impl Drop for PeerGuard {
            fn drop(&mut self) {
                let state = Arc::clone(&self.state);
                tokio::spawn(async move {
                    let mut lock = state.lock().await;
                    lock.peers_connected = lock.peers_connected.saturating_sub(1);
                });
            }
        }
        let _guard = PeerGuard {
            state: Arc::clone(&self.state),
        };

        // Send Interested and Unchoke
        stream
            .write_all(&PeerMessage::Interested.serialize())
            .await?;
        stream.write_all(&PeerMessage::Unchoke.serialize()).await?;

        let mut piece_index = start_piece_index;
        let mut block_offset = 0;
        let piece_length = self.meta.info.piece_length as u32;
        let total_pieces = self.meta.info.pieces.len() as u32;

        let total_size: u64 = match &self.meta.info.mode {
            FileMode::Single { length } => *length,
            FileMode::Multi { files } => files.iter().map(|f| f.length).sum(),
        };

        let get_request_length = |p_index: u32, b_offset: u32| -> u32 {
            let piece_start = p_index as u64 * piece_length as u64;
            let remaining_in_torrent = total_size.saturating_sub(piece_start + b_offset as u64);
            let remaining_in_piece = (piece_length - b_offset) as u64;
            16384.min(remaining_in_torrent).min(remaining_in_piece) as u32
        };

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
                        let req_len = get_request_length(piece_index, block_offset);
                        if req_len > 0 {
                            let req = PeerMessage::Request {
                                index: piece_index,
                                begin: block_offset,
                                length: req_len,
                            };
                            stream.write_all(&req.serialize()).await?;
                        }
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
                        lock.downloaded = (lock.downloaded + block_len).min(lock.size);
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
                        let req_len = get_request_length(piece_index, block_offset);
                        if req_len > 0 {
                            let req = PeerMessage::Request {
                                index: piece_index,
                                begin: block_offset,
                                length: req_len,
                            };
                            stream.write_all(&req.serialize()).await?;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn write_block(&self, piece_index: u32, offset: u32, data: &[u8]) -> std::io::Result<()> {
        let absolute_offset = (piece_index as u64 * self.meta.info.piece_length) + offset as u64;

        match &self.meta.info.mode {
            FileMode::Single { length } => {
                let file_path = self.download_dir.join(&self.meta.info.name);
                let mut file = OpenOptions::new().write(true).open(file_path)?;
                file.seek(SeekFrom::Start(absolute_offset))?;

                let remaining = length.saturating_sub(absolute_offset);
                let to_write = (data.len() as u64).min(remaining) as usize;
                file.write_all(&data[..to_write])?;
            }
            FileMode::Multi { files } => {
                let parent_dir = self.download_dir.join(&self.meta.info.name);
                let mut current_file_start = 0u64;
                let mut remaining_offset = absolute_offset;
                let mut data_pos = 0;
                let data_len = data.len();

                for f in files {
                    let file_end = current_file_start + f.length;

                    if remaining_offset >= f.length {
                        remaining_offset -= f.length;
                        current_file_start = file_end;
                        continue;
                    }

                    let mut full_path = parent_dir.clone();
                    for part in &f.path {
                        full_path.push(part);
                    }

                    if full_path.exists() {
                        let mut file = OpenOptions::new().write(true).open(full_path)?;
                        file.seek(SeekFrom::Start(remaining_offset))?;

                        let available = f.length - remaining_offset;
                        let to_write = (data_len - data_pos).min(available as usize);

                        file.write_all(&data[data_pos..data_pos + to_write])?;
                        data_pos += to_write;
                    }

                    remaining_offset = 0;
                    current_file_start = file_end;

                    if data_pos >= data_len {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
