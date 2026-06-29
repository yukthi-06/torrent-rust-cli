use clap::{Parser, Subcommand};
use torrent_core::TorrentId;
use torrent_rpc::{
    connect_daemon, send_request, receive_response,
    Request, Response,
};

#[derive(Parser)]
#[command(name = "torrent")]
#[command(about = "BitTorrent foreground CLI client", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a .torrent file from a file or folder
    Create {
        /// Path to the file or folder
        path: String,
    },
    /// Add a new torrent (from .torrent file or magnet link)
    Add {
        /// Path to .torrent file or magnet link
        torrent: String,
    },
    /// Remove a torrent by ID
    Remove {
        /// Torrent ID
        id: u32,
        /// Also delete downloaded files
        #[arg(long, default_value_t = false)]
        delete_data: bool,
    },
    /// Pause a torrent
    Pause {
        /// Torrent ID
        id: u32,
    },
    /// Resume a paused torrent
    Resume {
        /// Torrent ID
        id: u32,
    },
    /// List all torrents
    List,
    /// Get status of torrents
    Status {
        /// Optional Torrent ID for specific status
        id: Option<u32>,
    },
    /// Get global stats
    Stats,
    /// Show detailed info for a torrent
    Info {
        /// Torrent ID
        id: u32,
    },
    /// Verify torrent pieces
    Verify {
        /// Torrent ID
        id: u32,
    },
    /// Show configuration
    Config,
    /// Show version details
    Version,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Map CLI subcommand to RPC Request
    let request = match cli.command {
        Commands::Create { path } => Request::Create { path },
        Commands::Add { torrent } => Request::Add { path_or_magnet: torrent },
        Commands::Remove { id, delete_data } => Request::Remove { id: TorrentId(id), delete_data },
        Commands::Pause { id } => Request::Pause { id: TorrentId(id) },
        Commands::Resume { id } => Request::Resume { id: TorrentId(id) },
        Commands::List => Request::List,
        Commands::Status { id } => Request::Status { id: id.map(TorrentId) },
        Commands::Stats => Request::Stats,
        Commands::Info { id } => Request::Info { id: TorrentId(id) },
        Commands::Verify { id } => Request::Verify { id: TorrentId(id) },
        Commands::Config => Request::GetConfig,
        Commands::Version => Request::Version,
    };

    // Connect to the daemon
    let mut stream = match connect_daemon().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: Could not connect to torrentd daemon: {}.", e);
            eprintln!("Please make sure the background daemon 'torrentd' is running.");
            std::process::exit(1);
        }
    };

    // Send request
    send_request(&mut stream, &request).await?;

    // Receive and print response
    match receive_response(&mut stream).await? {
        Response::Ok => {
            println!("Operation completed successfully.");
        }
        Response::Error(msg) => {
            eprintln!("Error from daemon: {}", msg);
            std::process::exit(1);
        }
        Response::TorrentAdded { id } => {
            println!("Torrent added successfully with ID: {}", id);
        }
        Response::TorrentRemoved => {
            println!("Torrent removed successfully.");
        }
        Response::TorrentList(list) => {
            if list.is_empty() {
                println!("No torrents loaded.");
            } else {
                println!("{:<4} {:<35} {:<10} {:<10} {:<12} {:<6}", "ID", "Name", "Size", "Progress", "Status", "Peers");
                println!("{}", "-".repeat(80));
                for t in list {
                    let progress_str = format!("{:.1}%", t.progress);
                    let size_mb = format!("{:.1} MB", t.size as f32 / 1_048_576.0);
                    println!("{:<4} {:<35} {:<10} {:<10} {:<12} {:<6}", t.id, t.name, size_mb, progress_str, t.status, t.peers_connected);
                }
            }
        }
        Response::TorrentStatus(t) => {
            println!("Torrent details:");
            println!("  ID:         {}", t.id);
            println!("  Name:       {}", t.name);
            println!("  Hash:       {}", t.info_hash);
            println!("  Size:       {:.1} MB", t.size as f32 / 1_048_576.0);
            println!("  Downloaded: {:.1} MB", t.downloaded as f32 / 1_048_576.0);
            println!("  Uploaded:   {:.1} MB", t.uploaded as f32 / 1_048_576.0);
            println!("  Status:     {}", t.status);
            println!("  Progress:   {:.1}%", t.progress);
            println!("  Down Rate:  {:.1} KB/s", t.download_rate as f32 / 1024.0);
            println!("  Up Rate:    {:.1} KB/s", t.upload_rate as f32 / 1024.0);
            println!("  Peers:      {}", t.peers_connected);
        }
        Response::Stats(stats) => {
            println!("System Status:");
            println!("  Total Torrents:  {}", stats.num_torrents);
            println!("  Download Rate:   {:.1} KB/s", stats.download_rate as f32 / 1024.0);
            println!("  Upload Rate:     {:.1} KB/s", stats.upload_rate as f32 / 1024.0);
            println!("  Total Downloaded:{:.1} MB", stats.total_downloaded as f32 / 1_048_576.0);
            println!("  Total Uploaded:  {:.1} MB", stats.total_uploaded as f32 / 1_048_576.0);
        }
        Response::Info(info_str) => {
            println!("{}", info_str);
        }
        Response::Config(config_str) => {
            println!("{}", config_str);
        }
        Response::Version { version } => {
            println!("Daemon version: {}", version);
            println!("CLI version:    {}", env!("CARGO_PKG_VERSION"));
        }
    }

    Ok(())
}

