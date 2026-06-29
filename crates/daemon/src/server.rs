use crate::engine;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use torrent_core::meta::{FileMode, TorrentMeta};
use torrent_core::TorrentId;
use torrent_rpc::{
    receive_request, send_response,
    transport::{get_ipc_path, ServerConnection},
    Request, Response, SystemStats, TorrentStatus,
};
use tracing::{error, info, warn};

/// Path to the persistent state file that records added torrents.
fn state_file_path() -> PathBuf {
    PathBuf::from("torrents.json")
}

/// Loads saved torrent state from disk. Returns (id, path, downloaded_bytes).
fn load_state() -> Vec<(u32, String, u64)> {
    let path = state_file_path();
    match std::fs::read_to_string(&path) {
        Ok(contents) => serde_json::from_str(&contents).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Persists the current list of torrent (id, path, downloaded) tuples to disk.
fn save_state(entries: &[(u32, String, u64)]) {
    let path = state_file_path();
    match serde_json::to_string_pretty(entries) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                error!("Failed to save torrent state: {}", e);
            }
        }
        Err(e) => error!("Failed to serialize torrent state: {}", e),
    }
}

pub struct TorrentState {
    pub id: TorrentId,
    pub name: String,
    pub info_hash: String,
    pub size: u64,
    pub downloaded: u64,
    pub status: String,
}

pub struct RpcServer {
    torrents: Mutex<HashMap<TorrentId, Arc<Mutex<TorrentState>>>>,
    /// Tracks (id, torrent_file_path) for persistence
    saved_paths: Mutex<HashMap<u32, String>>,
    next_id: AtomicU32,
}

impl RpcServer {
    pub fn new() -> Self {
        Self {
            torrents: Mutex::new(HashMap::new()),
            saved_paths: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(1),
        }
    }

    /// Loads previously saved torrents from disk and starts their downloaders.
    pub async fn restore_state(self: Arc<Self>) {
        let entries = load_state();
        if entries.is_empty() {
            return;
        }
        info!("Restoring {} torrent(s) from saved state...", entries.len());
        let mut max_id = 0u32;
        for (id, path, downloaded) in &entries {
            max_id = max_id.max(*id);
            let server = Arc::clone(&self);
            let path = path.clone();
            let id = *id;
            let downloaded = *downloaded;
            tokio::spawn(async move {
                server.restore_torrent(id, &path, downloaded).await;
            });
        }
        // Advance next_id past all restored IDs so new torrents get unique IDs
        let current = self.next_id.load(Ordering::SeqCst);
        if max_id + 1 > current {
            self.next_id.store(max_id + 1, Ordering::SeqCst);
        }
        let mut saved = self.saved_paths.lock().await;
        for (id, path, _downloaded) in entries {
            saved.insert(id, path);
        }
    }

    async fn restore_torrent(&self, id: u32, path: &str, downloaded: u64) {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                error!("Restore: failed to read {}: {}", path, e);
                return;
            }
        };
        let meta = match TorrentMeta::from_bytes(&bytes) {
            Ok(m) => m,
            Err(e) => {
                error!("Restore: failed to parse {}: {}", path, e);
                return;
            }
        };
        let size = match &meta.info.mode {
            FileMode::Single { length } => *length,
            FileMode::Multi { files } => files.iter().map(|f| f.length).sum(),
        };
        let torrent_id = TorrentId(id);
        let status = if downloaded >= size && size > 0 {
            "Completed".to_string()
        } else {
            "Downloading".to_string()
        };
        let torrent_state = Arc::new(Mutex::new(TorrentState {
            id: torrent_id,
            name: meta.info.name.clone(),
            info_hash: meta.info_hash.to_string(),
            size,
            downloaded,
            status,
        }));
        {
            let mut map = self.torrents.lock().await;
            map.insert(torrent_id, Arc::clone(&torrent_state));
        }
        let download_dir = PathBuf::from("downloads");
        let mut peer_id = [0u8; 20];
        peer_id[0..8].copy_from_slice(b"-AG0001-");
        let downloader = Arc::new(engine::TorrentDownloader::new(
            torrent_id,
            meta,
            download_dir,
            peer_id,
            torrent_state,
        ));
        info!(
            "Resumed torrent ID {} ({} bytes already downloaded)",
            id, downloaded
        );
        downloader.start().await;
    }

    /// Flushes current download progress of all torrents to the state file.
    pub async fn flush_progress(&self) {
        let torrents = self.torrents.lock().await;
        let saved = self.saved_paths.lock().await;
        let mut entries: Vec<(u32, String, u64)> = Vec::new();
        for (&id, state_lock) in torrents.iter() {
            if let Some(path) = saved.get(&id.0) {
                let state = state_lock.lock().await;
                entries.push((id.0, path.clone(), state.downloaded));
            }
        }
        if !entries.is_empty() {
            save_state(&entries);
        }
    }

    pub async fn run(
        self: Arc<Self>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let path = get_ipc_path();

        #[cfg(unix)]
        {
            // Remove UDS socket file if it already exists
            if std::path::Path::new(path).exists() {
                let _ = std::fs::remove_file(path);
            }
            let listener = tokio::net::UnixListener::bind(path)?;
            info!("RPC Server listening on Unix Domain Socket: {}", path);

            loop {
                tokio::select! {
                    res = listener.accept() => {
                        match res {
                            Ok((stream, _addr)) => {
                                let server = Arc::clone(&self);
                                tokio::spawn(async move {
                                    if let Err(e) = server.handle_connection(ServerConnection::Unix(stream)).await {
                                        let msg = e.to_string();
                                        if msg.contains("early eof") || msg.contains("connection reset") || msg.contains("Broken pipe") {
                                            tracing::debug!("Client disconnected: {}", msg);
                                        } else {
                                            warn!("Connection error: {:?}", e);
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                error!("Failed to accept Unix connection: {:?}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("RPC Server shutting down Unix listener.");
                        break;
                    }
                }
            }
            // Cleanup socket file on shutdown
            let _ = std::fs::remove_file(path);
        }

        #[cfg(windows)]
        {
            use tokio::net::windows::named_pipe::ServerOptions;
            info!("RPC Server listening on Named Pipe: {}", path);

            let mut is_first = true;
            loop {
                // To accept a connection on Windows named pipe, we create the pipe instance first
                let server_pipe = ServerOptions::new()
                    .first_pipe_instance(is_first)
                    .create(path)?;
                is_first = false;

                tokio::select! {
                    connect_res = server_pipe.connect() => {
                        match connect_res {
                            Ok(()) => {
                                let server = Arc::clone(&self);
                                tokio::spawn(async move {
                                    if let Err(e) = server.handle_connection(ServerConnection::Windows(server_pipe)).await {
                                        let msg = e.to_string();
                                        if msg.contains("early eof") || msg.contains("connection reset") || msg.contains("Broken pipe") || msg.contains("pipe") {
                                            tracing::debug!("Client disconnected: {}", msg);
                                        } else {
                                            warn!("Connection error: {:?}", e);
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                error!("Failed to connect Windows named pipe: {:?}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("RPC Server shutting down Windows listener.");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_connection(&self, mut connection: ServerConnection) -> anyhow::Result<()> {
        loop {
            let request = match receive_request(&mut connection).await {
                Ok(req) => req,
                Err(e) => {
                    // Check if it's a clean client disconnect (early EOF after response sent)
                    let err_str = format!("{:?}", e);
                    if err_str.contains("early eof")
                        || err_str.contains("connection reset")
                        || err_str.contains("Broken pipe")
                        || err_str.contains("Failed to read request packet")
                        // Windows: pipe broken or closed
                        || err_str.contains("os error 109")
                        || err_str.contains("os error 232")
                    {
                        break;
                    }
                    return Err(e);
                }
            };

            let response = self.process_request(request).await;
            send_response(&mut connection, &response).await?;
        }
        Ok(())
    }

    async fn process_request(&self, request: Request) -> Response {
        match request {
            Request::Version => Response::Version {
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            Request::Add { path_or_magnet } => {
                // Check if it's a .torrent file path
                let path = std::path::Path::new(&path_or_magnet);
                let meta = match std::fs::read(path) {
                    Ok(bytes) => match TorrentMeta::from_bytes(&bytes) {
                        Ok(m) => m,
                        Err(e) => {
                            return Response::Error(format!(
                                "Failed to parse torrent metainfo: {}",
                                e
                            ))
                        }
                    },
                    Err(e) => {
                        return Response::Error(format!("Failed to read torrent file: {}", e))
                    }
                };

                // Compute total size
                let size = match &meta.info.mode {
                    FileMode::Single { length } => *length,
                    FileMode::Multi { files } => files.iter().map(|f| f.length).sum(),
                };

                // Reject duplicate torrents (same info_hash already loaded)
                let info_hash_str = meta.info_hash.to_string();
                {
                    let map = self.torrents.lock().await;
                    for existing in map.values() {
                        let existing = existing.lock().await;
                        if existing.info_hash == info_hash_str {
                            return Response::Error(format!(
                                "Torrent '{}' is already added (ID {})",
                                existing.name, existing.id
                            ));
                        }
                    }
                }

                let new_id = TorrentId(self.next_id.fetch_add(1, Ordering::SeqCst));
                let torrent_state = Arc::new(Mutex::new(TorrentState {
                    id: new_id,
                    name: meta.info.name.clone(),
                    info_hash: meta.info_hash.to_string(),
                    size,
                    downloaded: 0,
                    status: "Downloading".to_string(),
                }));

                {
                    let mut map = self.torrents.lock().await;
                    map.insert(new_id, Arc::clone(&torrent_state));
                }

                // Persist the torrent file path so it survives restarts
                {
                    let mut saved = self.saved_paths.lock().await;
                    saved.insert(new_id.0, path_or_magnet.clone());
                    let entries: Vec<(u32, String, u64)> =
                        saved.iter().map(|(k, v)| (*k, v.clone(), 0u64)).collect();
                    save_state(&entries);
                }

                // Spawn downloader worker
                let download_dir = PathBuf::from("downloads");
                let mut peer_id = [0u8; 20];
                peer_id[0..8].copy_from_slice(b"-AG0001-"); // Client prefix

                let downloader = Arc::new(engine::TorrentDownloader::new(
                    new_id,
                    meta,
                    download_dir,
                    peer_id,
                    torrent_state,
                ));
                downloader.start().await;

                Response::TorrentAdded { id: new_id }
            }
            Request::List => {
                let map = self.torrents.lock().await;
                let mut list = Vec::new();
                for t_lock in map.values() {
                    let t = t_lock.lock().await;
                    list.push(TorrentStatus {
                        id: t.id,
                        name: t.name.clone(),
                        info_hash: t.info_hash.clone(),
                        size: t.size,
                        downloaded: t.downloaded,
                        uploaded: 0,
                        status: t.status.clone(),
                        progress: if t.size > 0 {
                            ((t.downloaded as f32 / t.size as f32) * 100.0).min(100.0)
                        } else {
                            0.0
                        },
                        download_rate: 0,
                        upload_rate: 0,
                        peers_connected: 0,
                    });
                }
                Response::TorrentList(list)
            }
            Request::Status { id } => {
                let map = self.torrents.lock().await;
                let status_id = match id {
                    Some(sid) => sid,
                    None => {
                        if let Some(&first_id) = map.keys().next() {
                            first_id
                        } else {
                            return Response::Error("No torrents loaded".to_string());
                        }
                    }
                };

                if let Some(t_lock) = map.get(&status_id) {
                    let t = t_lock.lock().await;
                    Response::TorrentStatus(TorrentStatus {
                        id: t.id,
                        name: t.name.clone(),
                        info_hash: t.info_hash.clone(),
                        size: t.size,
                        downloaded: t.downloaded,
                        uploaded: 0,
                        status: t.status.clone(),
                        progress: if t.size > 0 {
                            ((t.downloaded as f32 / t.size as f32) * 100.0).min(100.0)
                        } else {
                            0.0
                        },
                        download_rate: 0,
                        upload_rate: 0,
                        peers_connected: 0,
                    })
                } else {
                    Response::Error(format!("Torrent ID {} not found", status_id))
                }
            }
            Request::Remove { id, delete_data: _ } => {
                let mut map = self.torrents.lock().await;
                if map.remove(&id).is_some() {
                    // Remove from persisted state
                    let mut saved = self.saved_paths.lock().await;
                    saved.remove(&id.0);
                    let entries: Vec<(u32, String, u64)> =
                        saved.iter().map(|(k, v)| (*k, v.clone(), 0u64)).collect();
                    save_state(&entries);
                    Response::TorrentRemoved
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Pause { id } => {
                let map = self.torrents.lock().await;
                if let Some(t_lock) = map.get(&id) {
                    let mut t = t_lock.lock().await;
                    t.status = "Paused".to_string();
                    Response::Ok
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Resume { id } => {
                let map = self.torrents.lock().await;
                if let Some(t_lock) = map.get(&id) {
                    let mut t = t_lock.lock().await;
                    t.status = "Downloading".to_string();
                    Response::Ok
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Verify { id } => {
                let map = self.torrents.lock().await;
                if map.contains_key(&id) {
                    Response::Ok
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Create { .. } => Response::Ok,
            Request::Info { id } => {
                let map = self.torrents.lock().await;
                if let Some(t_lock) = map.get(&id) {
                    let t = t_lock.lock().await;
                    Response::Info(format!(
                        "Name:      {}\nHash:      {}\nSize:      {:.1} MB\nStatus:    {}",
                        t.name,
                        t.info_hash,
                        t.size as f32 / 1_048_576.0,
                        t.status
                    ))
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Stats => {
                let map = self.torrents.lock().await;
                let count = map.len();
                Response::Stats(SystemStats {
                    download_rate: 0,
                    upload_rate: 0,
                    total_downloaded: 0,
                    total_uploaded: 0,
                    num_torrents: count,
                })
            }
            Request::GetConfig => {
                Response::Config("download_dir = \"downloads\"\nlisten_port = 6881\n".to_string())
            }
        }
    }
}
