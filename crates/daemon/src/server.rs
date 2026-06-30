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

/// Loads saved torrent state from disk. Returns (id, path).
fn load_state() -> Vec<(u32, String)> {
    let path = state_file_path();
    match std::fs::read_to_string(&path) {
        Ok(contents) => serde_json::from_str(&contents).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Persists the current list of torrent (id, path) tuples to disk.
fn save_state(entries: &[(u32, String)]) {
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
    pub peers_connected: usize,
}

pub struct TorrentHandle {
    pub state: Arc<Mutex<TorrentState>>,
    pub worker_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

pub struct RpcServer {
    config: Arc<torrent_config::Config>,
    torrents: Mutex<HashMap<TorrentId, Arc<TorrentHandle>>>,
    /// Tracks (id, torrent_file_path) for persistence
    saved_paths: Mutex<HashMap<u32, String>>,
    next_id: AtomicU32,
}

impl RpcServer {
    pub fn new(config: Arc<torrent_config::Config>) -> Self {
        Self {
            config,
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
        for (id, path) in &entries {
            max_id = max_id.max(*id);
            let server = Arc::clone(&self);
            let path = path.clone();
            let id = *id;
            tokio::spawn(async move {
                server.restore_torrent(id, &path).await;
            });
        }
        // Advance next_id past all restored IDs so new torrents get unique IDs
        let current = self.next_id.load(Ordering::SeqCst);
        if max_id + 1 > current {
            self.next_id.store(max_id + 1, Ordering::SeqCst);
        }
        let mut saved = self.saved_paths.lock().await;
        for (id, path) in entries {
            saved.insert(id, path);
        }
    }

    async fn restore_torrent(&self, id: u32, path: &str) {
        // Handle magnet links
        if path.starts_with("magnet:?") {
            let magnet = match torrent_core::magnet::MagnetLink::parse(path) {
                Ok(m) => m,
                Err(e) => {
                    error!("Restore: failed to parse magnet link: {}", e);
                    return;
                }
            };

            let info_hash_str = magnet.info_hash.to_string();
            let torrent_name = magnet
                .name
                .clone()
                .unwrap_or_else(|| info_hash_str.clone());
            let cache_path = format!("{}/{}.torrent", self.config.metadata_dir, info_hash_str);
            if std::path::Path::new(&cache_path).exists() {
                info!("Found cached metadata for magnet link: {}", info_hash_str);
                // We re-bind path to the cache file and break out of the magnet block
                // so it falls through to the .torrent file handler below!
            } else {
                let torrent_id = TorrentId(id);
                let torrent_state = Arc::new(Mutex::new(TorrentState {
                    id: torrent_id,
                    name: torrent_name,
                    info_hash: info_hash_str,
                    size: 0,
                    downloaded: 0,
                    status: "Fetching Metadata".to_string(),
                    peers_connected: 0,
                }));

                let handle = Arc::new(TorrentHandle {
                    state: torrent_state,
                    worker_handle: Mutex::new(None),
                });
                {
                    let mut map = self.torrents.lock().await;
                    map.insert(torrent_id, Arc::clone(&handle));
                }

                let download_dir = PathBuf::from(&self.config.download_dir);
                let worker = Arc::new(crate::magnet_worker::MagnetWorker {
                    id: torrent_id,
                    magnet,
                    download_dir,
                    metadata_dir: PathBuf::from(&self.config.metadata_dir),
                    state: Arc::clone(&handle.state),
                });
                info!("Resumed magnet torrent ID {}", id);
                let join_handle = tokio::spawn(async move {
                    worker.start().await;
                });
                *handle.worker_handle.lock().await = Some(join_handle);
                return;
            }
        }

        // Use the cache path if we found one, otherwise use the original .torrent path
        let path = if path.starts_with("magnet:?") {
            let magnet = torrent_core::magnet::MagnetLink::parse(path).unwrap();
            format!("{}/{}.torrent", self.config.metadata_dir, magnet.info_hash)
        } else {
            path.to_string()
        };

        // Handle .torrent file paths
        let bytes = match std::fs::read(&path) {
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
        let status = "Checking".to_string();
        let torrent_state = Arc::new(Mutex::new(TorrentState {
            id: torrent_id,
            name: meta.info.name.clone(),
            info_hash: meta.info_hash.to_string(),
            size,
            downloaded: 0, // Engine will correct this via hash check
            status,
            peers_connected: 0,
        }));
        let handle = Arc::new(TorrentHandle {
            state: torrent_state,
            worker_handle: Mutex::new(None),
        });
        {
            let mut map = self.torrents.lock().await;
            map.insert(torrent_id, Arc::clone(&handle));
        }
        let download_dir = PathBuf::from(&self.config.download_dir);
        let mut peer_id = [0u8; 20];
        peer_id[0..8].copy_from_slice(b"-AG0001-");
        let downloader = Arc::new(engine::TorrentDownloader::new(
            torrent_id,
            meta,
            download_dir,
            peer_id,
            Arc::clone(&handle.state),
        ));
        info!(
            "Resumed torrent ID {} (hashing pieces...)",
            id
        );
        let join_handle = tokio::spawn(async move {
            downloader.start().await;
        });
        *handle.worker_handle.lock().await = Some(join_handle);
    }

    /// Flushes current download progress of all torrents to the state file.
    pub async fn flush_progress(&self) {
        let torrents = self.torrents.lock().await;
        let saved = self.saved_paths.lock().await;
        let mut entries: Vec<(u32, String)> = Vec::new();
        for (&id, _handle) in torrents.iter() {
            if let Some(path) = saved.get(&id.0) {
                entries.push((id.0, path.clone()));
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

    async fn handle_connection(self: Arc<Self>, mut connection: ServerConnection) -> anyhow::Result<()> {
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

    async fn process_request(self: &Arc<Self>, request: Request) -> Response {
        match request {
            Request::Version => Response::Version {
                version: format!(
                    "{} (Git: {} | Date: {})",
                    env!("CARGO_PKG_VERSION"),
                    env!("GIT_HASH"),
                    env!("GIT_DATE")
                ),
            },
            Request::Add { path_or_magnet } => {
                let mut path_or_magnet = path_or_magnet.trim().to_string();
                
                // If it's exactly 40 characters of hex, treat it as a bare info hash
                if path_or_magnet.len() == 40 && path_or_magnet.chars().all(|c| c.is_ascii_hexdigit()) {
                    let mut trackers_str = String::new();
                    for tracker in &self.config.default_trackers {
                        let encoded = url::form_urlencoded::byte_serialize(tracker.as_bytes()).collect::<String>();
                        trackers_str.push_str(&format!("&tr={}", encoded));
                    }
                    path_or_magnet = format!(
                        "magnet:?xt=urn:btih:{}{}",
                        path_or_magnet, trackers_str
                    );
                }

                if path_or_magnet.to_lowercase().starts_with("magnet:?") {
                    let magnet = match torrent_core::magnet::MagnetLink::parse(&path_or_magnet) {
                        Ok(m) => m,
                        Err(e) => return Response::Error(format!("Invalid magnet link: {}", e)),
                    };

                    let info_hash_str = magnet.info_hash.to_string();
                    {
                        let map = self.torrents.lock().await;
                        for existing_handle in map.values() {
                            let existing = existing_handle.state.lock().await;
                            if existing.info_hash == info_hash_str {
                                return Response::Error(format!(
                                    "Torrent '{}' is already added (ID {})",
                                    existing.name, existing.id
                                ));
                            }
                        }
                    }

                    let cache_path = format!("{}/{}.torrent", self.config.metadata_dir, info_hash_str);
                    if std::path::Path::new(&cache_path).exists() {
                        info!("Found cached metadata for magnet link: {}", info_hash_str);
                        
                        // We must still reserve the ID and persist the ORIGINAL magnet link
                        let new_id = TorrentId(self.next_id.fetch_add(1, Ordering::SeqCst));
                        {
                            let mut saved = self.saved_paths.lock().await;
                            saved.insert(new_id.0, path_or_magnet.clone());
                            let entries: Vec<(u32, String)> =
                                saved.iter().map(|(k, v)| (*k, v.clone())).collect();
                            save_state(&entries);
                        }
                        
                        // Let the logic fall through to the .torrent handler below using the cache path!
                        path_or_magnet = cache_path;
                        
                    } else {
                        let new_id = TorrentId(self.next_id.fetch_add(1, Ordering::SeqCst));
                        let torrent_name = magnet.name.clone().unwrap_or_else(|| info_hash_str.clone());
                        let torrent_state = Arc::new(Mutex::new(TorrentState {
                            id: new_id,
                            name: torrent_name,
                            info_hash: info_hash_str,
                            size: 0,
                            downloaded: 0,
                            status: "Fetching Metadata".to_string(),
                            peers_connected: 0,
                        }));

                        let handle = Arc::new(TorrentHandle {
                            state: torrent_state,
                            worker_handle: Mutex::new(None),
                        });
                        {
                            let mut map = self.torrents.lock().await;
                            map.insert(new_id, Arc::clone(&handle));
                        }

                        // Persist the original magnet file path so it survives restarts
                        {
                            let mut saved = self.saved_paths.lock().await;
                            saved.insert(new_id.0, path_or_magnet.clone());
                            let entries: Vec<(u32, String)> =
                                saved.iter().map(|(k, v)| (*k, v.clone())).collect();
                            save_state(&entries);
                        }

                        let download_dir = PathBuf::from(&self.config.download_dir);
                        let worker = Arc::new(crate::magnet_worker::MagnetWorker {
                            id: new_id,
                            magnet,
                            download_dir,
                            metadata_dir: PathBuf::from(&self.config.metadata_dir),
                            state: Arc::clone(&handle.state),
                        });
                        let join_handle = tokio::spawn(async move {
                            worker.start().await;
                        });
                        *handle.worker_handle.lock().await = Some(join_handle);

                        return Response::TorrentAdded { id: new_id };
                    }
                }

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
                    for existing_handle in map.values() {
                        let existing = existing_handle.state.lock().await;
                        if existing.info_hash == info_hash_str {
                            return Response::Error(format!(
                                "Torrent '{}' is already added (ID {})",
                                existing.name, existing.id
                            ));
                        }
                    }
                }

                let new_id = if path_or_magnet.starts_with(&format!("{}/", self.config.metadata_dir)) {
                    // We already allocated an ID above if we fell through from the magnet cache logic
                    let max_id = self.next_id.load(Ordering::SeqCst) - 1;
                    TorrentId(max_id)
                } else {
                    let new_id = TorrentId(self.next_id.fetch_add(1, Ordering::SeqCst));
                    
                    // Only persist if we didn't just persist it in the magnet cache logic
                    {
                        let mut saved = self.saved_paths.lock().await;
                        saved.insert(new_id.0, path_or_magnet.clone());
                        let entries: Vec<(u32, String)> =
                            saved.iter().map(|(k, v)| (*k, v.clone())).collect();
                        save_state(&entries);
                    }
                    new_id
                };

                let torrent_state = Arc::new(Mutex::new(TorrentState {
                    id: new_id,
                    name: meta.info.name.clone(),
                    info_hash: meta.info_hash.to_string(),
                    size,
                    downloaded: 0,
                    status: "Checking".to_string(),
                    peers_connected: 0,
                }));

                let handle = Arc::new(TorrentHandle {
                    state: torrent_state,
                    worker_handle: Mutex::new(None),
                });
                {
                    let mut map = self.torrents.lock().await;
                    map.insert(new_id, Arc::clone(&handle));
                }

                // Spawn downloader worker
                let download_dir = PathBuf::from(&self.config.download_dir);
                let mut peer_id = [0u8; 20];
                peer_id[0..8].copy_from_slice(b"-AG0001-"); // Client prefix

                let downloader = Arc::new(engine::TorrentDownloader::new(
                    new_id,
                    meta,
                    download_dir,
                    peer_id,
                    Arc::clone(&handle.state),
                ));
                let join_handle = tokio::spawn(async move {
                    downloader.start().await;
                });
                *handle.worker_handle.lock().await = Some(join_handle);

                Response::TorrentAdded { id: new_id }
            }
            Request::List => {
                let map = self.torrents.lock().await;
                let mut list = Vec::new();
                for handle in map.values() {
                    let t = handle.state.lock().await;
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
                        peers_connected: t.peers_connected,
                    });
                }
                list.sort_by_key(|t| t.id.0);
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

                if let Some(handle) = map.get(&status_id) {
                    let t = handle.state.lock().await;
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
                        peers_connected: t.peers_connected,
                    })
                } else {
                    Response::Error(format!("Torrent ID {} not found", status_id))
                }
            }
            Request::Remove { id, delete_data: _ } => {
                let mut map = self.torrents.lock().await;
                if let Some(handle) = map.remove(&id) {
                    if let Some(worker) = handle.worker_handle.lock().await.take() {
                        worker.abort();
                    }
                    let mut saved = self.saved_paths.lock().await;
                    saved.remove(&id.0);
                    let entries: Vec<(u32, String)> =
                        saved.iter().map(|(k, v)| (*k, v.clone())).collect();
                    save_state(&entries);
                    Response::TorrentRemoved
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Pause { id } => {
                let map = self.torrents.lock().await;
                if let Some(handle) = map.get(&id) {
                    if let Some(worker) = handle.worker_handle.lock().await.take() {
                        worker.abort();
                    }
                    let mut t = handle.state.lock().await;
                    t.status = "Paused".to_string();
                    Response::Ok
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Resume { id } => {
                let map = self.torrents.lock().await;
                if let Some(handle) = map.get(&id) {
                    let worker_lock = handle.worker_handle.lock().await;
                    if worker_lock.is_some() {
                        return Response::Error(format!("Torrent ID {} is already running", id));
                    }
                    let mut t = handle.state.lock().await;
                    t.status = "Checking".to_string();
                    
                    let path = {
                        let saved = self.saved_paths.lock().await;
                        saved.get(&id.0).cloned()
                    };
                    
                    if let Some(p) = path {
                        // Drop locks before spawning to avoid deadlocks
                        drop(t);
                        drop(worker_lock);
                        drop(map);
                        let server = Arc::clone(self);
                        tokio::spawn(async move {
                            server.restore_torrent(id.0, &p).await;
                        });
                        Response::Ok
                    } else {
                        Response::Error("Torrent data path not found".to_string())
                    }
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Verify { id } => {
                let map = self.torrents.lock().await;
                if let Some(handle) = map.get(&id) {
                    if let Some(worker) = handle.worker_handle.lock().await.take() {
                        worker.abort();
                    }
                    let mut t = handle.state.lock().await;
                    t.status = "Checking".to_string();
                    
                    let path = {
                        let saved = self.saved_paths.lock().await;
                        saved.get(&id.0).cloned()
                    };
                    
                    if let Some(p) = path {
                        drop(t);
                        drop(map);
                        let server = Arc::clone(self);
                        tokio::spawn(async move {
                            server.restore_torrent(id.0, &p).await;
                        });
                        Response::Ok
                    } else {
                        Response::Error("Torrent data path not found".to_string())
                    }
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Create { .. } => Response::Ok,
            Request::Info { id } => {
                let map = self.torrents.lock().await;
                if let Some(handle) = map.get(&id) {
                    let t = handle.state.lock().await;
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
