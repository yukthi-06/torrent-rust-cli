use crate::engine::TorrentDownloader;
use crate::server::TorrentState;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use torrent_core::magnet::MagnetLink;
use torrent_core::TorrentId;
use tracing::{error, info};

pub struct MagnetWorker {
    pub id: TorrentId,
    pub magnet: MagnetLink,
    pub download_dir: PathBuf,
    pub state: Arc<Mutex<TorrentState>>,
}

impl MagnetWorker {
    pub async fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            info!(
                "Starting magnet metadata fetch for: {}",
                self.magnet.info_hash
            );

            // For now, we will simulate the metadata fetching failure
            // since BEP 9 (ut_metadata) state machine requires a significant peer management loop
            // comparable to TorrentDownloader.
            // A real implementation would:
            // 1. Announce to trackers in self.magnet.trackers
            // 2. Connect to peers and send Handshake with BEP 10 extension bit
            // 3. Send Extended(0) handshake to declare ut_metadata support
            // 4. Request metadata pieces
            // 5. Verify SHA1 of metadata against self.magnet.info_hash
            // 6. Build TorrentMeta and spawn TorrentDownloader.

            {
                let mut lock = self.state.lock().await;
                lock.status = "Failed (Metadata fetch not fully implemented)".to_string();
            }

            error!("Magnet link metadata fetching is not yet fully implemented.");
        });
    }
}
