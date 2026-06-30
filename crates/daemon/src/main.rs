mod engine;
pub mod magnet_worker;
mod server;

use server::RpcServer;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set up unbuffered file logging to guarantee immediate writes
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("torrentd.log")
        .expect("Failed to open torrentd.log");
    let file_appender = std::sync::Arc::new(log_file);

    // Disable ANSI color codes when stdout is not a terminal (e.g. piped or run as a service)
    let ansi_colors = std::io::IsTerminal::is_terminal(&std::io::stdout());
    
    let timer = tracing_subscriber::fmt::time::LocalTime::new(
        time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"),
    );

    tracing_subscriber::fmt()
        .with_timer(timer)
        .with_ansi(ansi_colors)
        .with_env_filter(tracing_subscriber::EnvFilter::new("debug"))
        .with_writer(file_appender)
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
