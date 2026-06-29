use clap::{Parser, Subcommand};

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

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Create { path } => {
            println!("Creating torrent for: {}", path);
        }
        Commands::Add { torrent } => {
            println!("Adding torrent: {}", torrent);
        }
        Commands::Remove { id, delete_data } => {
            println!("Removing torrent ID: {} (delete data: {})", id, delete_data);
        }
        Commands::Pause { id } => {
            println!("Pausing torrent ID: {}", id);
        }
        Commands::Resume { id } => {
            println!("Resuming torrent ID: {}", id);
        }
        Commands::List => {
            println!("Listing torrents...");
        }
        Commands::Status { id } => {
            if let Some(id) = id {
                println!("Status for torrent ID: {}", id);
            } else {
                println!("Status for all torrents...");
            }
        }
        Commands::Stats => {
            println!("System stats...");
        }
        Commands::Info { id } => {
            println!("Info for torrent ID: {}", id);
        }
        Commands::Verify { id } => {
            println!("Verifying torrent ID: {}", id);
        }
        Commands::Config => {
            println!("Showing configuration...");
        }
        Commands::Version => {
            println!("torrent version {}", env!("CARGO_PKG_VERSION"));
        }
    }
}
