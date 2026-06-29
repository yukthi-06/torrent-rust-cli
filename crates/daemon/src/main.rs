mod engine;
mod server;

use server::RpcServer;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Disable ANSI color codes when stdout is not a terminal (e.g. piped or run as a service)
    let ansi_colors = std::io::IsTerminal::is_terminal(&std::io::stdout());
    tracing_subscriber::fmt()
        .with_ansi(ansi_colors)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    info!("Starting torrentd background daemon...");

    let server = Arc::new(RpcServer::new());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.run(shutdown_rx).await {
            tracing::error!("RPC Server error: {:?}", e);
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
