mod engine;
pub mod magnet_worker;
mod server;

use server::RpcServer;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set up file logging
    let file_appender = tracing_appender::rolling::daily("logs", "torrentd.log");
    let (file_writer, _guard) = tracing_appender::non_blocking(file_appender);

    // Disable ANSI color codes when stdout is not a terminal (e.g. piped or run as a service)
    let ansi_colors = std::io::IsTerminal::is_terminal(&std::io::stdout());
    
    use tracing_subscriber::fmt::writer::MakeWriterExt;
    
    tracing_subscriber::fmt()
        .with_ansi(ansi_colors)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug")), // default to debug
        )
        .with_writer(std::io::stdout.and(file_writer))
        .init();
    info!("Starting torrentd background daemon...");

    let server = Arc::new(RpcServer::new());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Reload torrents saved from previous session
    Arc::clone(&server).restore_state().await;

    // Clone before moving into spawned tasks
    let flush_server = Arc::clone(&server);
    let mut flush_shutdown_rx = shutdown_rx.clone();

    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.run(shutdown_rx).await {
            tracing::error!("RPC Server error: {:?}", e);
        }
    });

    // Background task: flush download progress to disk every 30 seconds
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    flush_server.flush_progress().await;
                }
                _ = flush_shutdown_rx.changed() => {
                    // Final flush on shutdown
                    flush_server.flush_progress().await;
                    break;
                }
            }
        }
    });

    // Wait for Ctrl+C (graceful shutdown)
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received. Shutting down daemon...");

    let _ = shutdown_tx.send(true);
    let _ = server_handle.await;

    info!("Daemon stopped cleanly.");
    Ok(())
}
