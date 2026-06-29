use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;

#[derive(Default)]
pub struct TrackerClient;

impl TrackerClient {
    pub fn new() -> Self {
        Self
    }

    /// Announces to a UDP tracker and returns the list of discovered peer SocketAddrs.
    pub async fn announce_udp(
        &self,
        tracker_addr_str: &str,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        port: u16,
    ) -> Result<Vec<SocketAddr>, anyhow::Error> {
        // Parse the tracker address.
        // Format of tracker_addr_str: "tracker.coppersurfer.tk:6969"
        let addrs: Vec<SocketAddr> = tracker_addr_str.to_socket_addrs()?.collect();
        if addrs.is_empty() {
            anyhow::bail!("Could not resolve tracker address: {}", tracker_addr_str);
        }
        let tracker_addr = addrs[0];

        // Bind local socket
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.connect(tracker_addr).await?;

        // 1. Connection Phase
        let transaction_id: u32 = rand_u32();
        let mut connect_req = [0u8; 16];
        // Connection ID: 0x41727101980
        connect_req[0..8].copy_from_slice(&0x0000_0417_2710_1980i64.to_be_bytes());
        connect_req[8..12].copy_from_slice(&0u32.to_be_bytes()); // Action: connect (0)
        connect_req[12..16].copy_from_slice(&transaction_id.to_be_bytes());

        // Send connection request with timeout
        socket.send(&connect_req).await?;
        let mut connect_resp = [0u8; 16];
        let _ = timeout(Duration::from_secs(5), socket.recv(&mut connect_resp)).await??;

        let resp_action = u32::from_be_bytes([
            connect_resp[0],
            connect_resp[1],
            connect_resp[2],
            connect_resp[3],
        ]);
        let resp_transaction_id = u32::from_be_bytes([
            connect_resp[4],
            connect_resp[5],
            connect_resp[6],
            connect_resp[7],
        ]);
        if resp_action != 0 || resp_transaction_id != transaction_id {
            anyhow::bail!("Invalid tracker connect response");
        }

        let connection_id = &connect_resp[8..16];

        // 2. Announce Phase
        let announce_transaction_id: u32 = rand_u32();
        let mut announce_req = Vec::with_capacity(98);
        announce_req.extend_from_slice(connection_id);
        announce_req.extend_from_slice(&1u32.to_be_bytes()); // Action: announce (1)
        announce_req.extend_from_slice(&announce_transaction_id.to_be_bytes());
        announce_req.extend_from_slice(&info_hash);
        announce_req.extend_from_slice(&peer_id);
        announce_req.extend_from_slice(&0i64.to_be_bytes()); // downloaded (0)
        announce_req.extend_from_slice(&0i64.to_be_bytes()); // left (0)
        announce_req.extend_from_slice(&0i64.to_be_bytes()); // uploaded (0)
        announce_req.extend_from_slice(&0u32.to_be_bytes()); // event: none (0)
        announce_req.extend_from_slice(&0u32.to_be_bytes()); // ip address: default (0)
        announce_req.extend_from_slice(&rand_u32().to_be_bytes()); // key
        announce_req.extend_from_slice(&(-1i32).to_be_bytes()); // num_want: default (-1)
        announce_req.extend_from_slice(&(port as u32).to_be_bytes()); // port

        socket.send(&announce_req).await?;

        // Receives announce response
        let mut announce_resp = vec![0u8; 4096];
        let n = timeout(Duration::from_secs(5), socket.recv(&mut announce_resp)).await??;
        if n < 20 {
            anyhow::bail!("Tracker announce response too short");
        }

        let resp_action = u32::from_be_bytes([
            announce_resp[0],
            announce_resp[1],
            announce_resp[2],
            announce_resp[3],
        ]);
        let resp_transaction_id = u32::from_be_bytes([
            announce_resp[4],
            announce_resp[5],
            announce_resp[6],
            announce_resp[7],
        ]);
        if resp_action != 1 || resp_transaction_id != announce_transaction_id {
            anyhow::bail!("Invalid tracker announce response match");
        }

        let peers_data = &announce_resp[20..n];
        let mut peers = Vec::new();
        for chunk in peers_data.chunks_exact(6) {
            let ip = std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            peers.push(SocketAddr::new(std::net::IpAddr::V4(ip), port));
        }

        Ok(peers)
    }
}

fn rand_u32() -> u32 {
    let mut bytes = [0u8; 4];
    // Simple mock random using system time if rand is not in workspace
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    bytes.copy_from_slice(&(time as u32).to_be_bytes());
    u32::from_be_bytes(bytes)
}
