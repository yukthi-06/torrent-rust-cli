use std::collections::HashMap;
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

pub struct TorrentState {
    pub id: TorrentId,
    pub name: String,
    pub info_hash: String,
    pub size: u64,
    pub status: String,
}

pub struct RpcServer {
    torrents: Mutex<HashMap<TorrentId, TorrentState>>,
    next_id: AtomicU32,
}

impl RpcServer {
    pub fn new() -> Self {
        Self {
            torrents: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(1),
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
                                        warn!("Connection error: {:?}", e);
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
                                        warn!("Connection error: {:?}", e);
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
                    // Check if it's EOF (clean client disconnect)
                    let err_str = e.to_string();
                    if err_str.contains("early eof")
                        || err_str.contains("connection reset")
                        || err_str.contains("Broken pipe")
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

                let new_id = TorrentId(self.next_id.fetch_add(1, Ordering::SeqCst));
                let mut map = self.torrents.lock().await;
                map.insert(
                    new_id,
                    TorrentState {
                        id: new_id,
                        name: meta.info.name.clone(),
                        info_hash: meta.info_hash.to_string(),
                        size,
                        status: "Stopped".to_string(),
                    },
                );

                Response::TorrentAdded { id: new_id }
            }
            Request::List => {
                let map = self.torrents.lock().await;
                let list = map
                    .values()
                    .map(|t| TorrentStatus {
                        id: t.id,
                        name: t.name.clone(),
                        info_hash: t.info_hash.clone(),
                        size: t.size,
                        downloaded: 0,
                        uploaded: 0,
                        status: t.status.clone(),
                        progress: 0.0,
                        download_rate: 0,
                        upload_rate: 0,
                        peers_connected: 0,
                    })
                    .collect();
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

                if let Some(t) = map.get(&status_id) {
                    Response::TorrentStatus(TorrentStatus {
                        id: t.id,
                        name: t.name.clone(),
                        info_hash: t.info_hash.clone(),
                        size: t.size,
                        downloaded: 0,
                        uploaded: 0,
                        status: t.status.clone(),
                        progress: 0.0,
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
                    Response::TorrentRemoved
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Pause { id } => {
                let mut map = self.torrents.lock().await;
                if let Some(t) = map.get_mut(&id) {
                    t.status = "Paused".to_string();
                    Response::Ok
                } else {
                    Response::Error(format!("Torrent ID {} not found", id))
                }
            }
            Request::Resume { id } => {
                let mut map = self.torrents.lock().await;
                if let Some(t) = map.get_mut(&id) {
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
                if let Some(t) = map.get(&id) {
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
