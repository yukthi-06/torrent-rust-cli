use std::sync::Arc;
use torrent_core::TorrentId;
use torrent_rpc::{
    receive_request, send_response,
    transport::{get_ipc_path, ServerConnection},
    Request, Response, SystemStats, TorrentStatus,
};
use tracing::{error, info, warn};

pub struct RpcServer {
    // We will later share actual torrent engine state here
}

impl RpcServer {
    pub fn new() -> Self {
        Self {}
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
            Request::List => {
                // Return dummy mock torrent list for this milestone
                Response::TorrentList(vec![TorrentStatus {
                    id: TorrentId(1),
                    name: "ubuntu-24.04-desktop-amd64.iso".to_string(),
                    info_hash: "d24b611e85ae6574f8cb4edca0f2b3e8114f62bf".to_string(),
                    size: 4398301184,
                    downloaded: 2199150592,
                    uploaded: 12345678,
                    status: "Downloading".to_string(),
                    progress: 50.0,
                    download_rate: 5120000,
                    upload_rate: 250000,
                    peers_connected: 42,
                }])
            }
            Request::Stats => Response::Stats(SystemStats {
                download_rate: 5120000,
                upload_rate: 250000,
                total_downloaded: 2199150592,
                total_uploaded: 12345678,
                num_torrents: 1,
            }),
            Request::GetConfig => {
                Response::Config("download_dir = \"downloads\"\nlisten_port = 6881\n".to_string())
            }
            _ => Response::Error(format!(
                "Command {:?} is not yet fully implemented in this milestone",
                request
            )),
        }
    }
}
