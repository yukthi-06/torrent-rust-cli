use serde::{Deserialize, Serialize};
use torrent_core::TorrentId;

/// The protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

/// RPC request commands sent from CLI to Daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Create { path: String, trackers: Option<Vec<String>> },
    CreateAdd { path: String, trackers: Option<Vec<String>> },
    Add { path_or_magnet: String },
    Remove { id: TorrentId, delete_data: bool },
    Pause { id: TorrentId },
    Resume { id: TorrentId },
    List,
    Status { id: Option<TorrentId> },
    Stats,
    Info { id: TorrentId },
    Verify { id: TorrentId },
    GetConfig,
    Version,
}

/// A summary of a single torrent's status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentStatus {
    pub id: TorrentId,
    pub name: String,
    pub info_hash: String,
    pub size: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub status: String, // "Downloading", "Seeding", "Paused", "Checking"
    pub progress: f32,
    pub download_rate: usize,
    pub upload_rate: usize,
    pub peers_connected: usize,
}

/// Global system stats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStats {
    pub download_rate: usize,
    pub upload_rate: usize,
    pub total_downloaded: u64,
    pub total_uploaded: u64,
    pub num_torrents: usize,
}

/// RPC responses sent from Daemon to CLI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Error(String),
    TorrentAdded { id: TorrentId },
    TorrentRemoved,
    TorrentList(Vec<TorrentStatus>),
    TorrentStatus(TorrentStatus),
    Stats(SystemStats),
    Info(String),   // detailed multi-line info string or JSON
    Config(String), // serialized config
    Version { version: String },
}

/// Standard Packet header structure.
/// Binary Layout:
/// - Version: 1 byte
/// - Command/Response discriminant: 4 bytes (derived from serialization)
/// - Length: 4 bytes
/// - Payload: `length` bytes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePacket {
    pub version: u8,
    pub payload: Vec<u8>,
}

pub mod transport;
pub use transport::connect_daemon;

use anyhow::Context;

pub async fn send_request<S: transport::IpcStream>(
    stream: &mut S,
    req: &Request,
) -> Result<(), anyhow::Error> {
    let payload = bincode::serialize(req).context("Failed to serialize Request")?;
    transport::write_packet(stream, PROTOCOL_VERSION, 0, &payload)
        .await
        .context("Failed to write request packet")?;
    Ok(())
}

pub async fn receive_request<S: transport::IpcStream>(
    stream: &mut S,
) -> Result<Request, anyhow::Error> {
    let (version, _cmd, payload) = transport::read_packet(stream)
        .await
        .context("Failed to read request packet")?;
    if version != PROTOCOL_VERSION {
        anyhow::bail!(
            "Protocol version mismatch: expected {}, got {}",
            PROTOCOL_VERSION,
            version
        );
    }
    let req: Request = bincode::deserialize(&payload).context("Failed to deserialize Request")?;
    Ok(req)
}

pub async fn send_response<S: transport::IpcStream>(
    stream: &mut S,
    resp: &Response,
) -> Result<(), anyhow::Error> {
    let payload = bincode::serialize(resp).context("Failed to serialize Response")?;
    transport::write_packet(stream, PROTOCOL_VERSION, 1, &payload)
        .await
        .context("Failed to write response packet")?;
    Ok(())
}

pub async fn receive_response<S: transport::IpcStream>(
    stream: &mut S,
) -> Result<Response, anyhow::Error> {
    let (version, _cmd, payload) = transport::read_packet(stream)
        .await
        .context("Failed to read response packet")?;
    if version != PROTOCOL_VERSION {
        anyhow::bail!(
            "Protocol version mismatch: expected {}, got {}",
            PROTOCOL_VERSION,
            version
        );
    }
    let resp: Response =
        bincode::deserialize(&payload).context("Failed to deserialize Response")?;
    Ok(resp)
}
