use tokio::io::{AsyncRead, AsyncReadExt};

pub const HANDSHAKE_PREFIX: &[u8] = b"\x13BitTorrent protocol";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    pub fn new(info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Self { info_hash, peer_id }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(68);
        buf.extend_from_slice(HANDSHAKE_PREFIX);
        buf.extend_from_slice(&[0u8; 8]); // 8 reserved bytes
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);
        buf
    }

    pub async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Self> {
        let mut prefix = [0u8; 20];
        reader.read_exact(&mut prefix).await?;
        if prefix != HANDSHAKE_PREFIX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid handshake protocol prefix",
            ));
        }

        let mut reserved = [0u8; 8];
        reader.read_exact(&mut reserved).await?;

        let mut info_hash = [0u8; 20];
        reader.read_exact(&mut info_hash).await?;

        let mut peer_id = [0u8; 20];
        reader.read_exact(&mut peer_id).await?;

        Ok(Self { info_hash, peer_id })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        index: u32,
    },
    Bitfield(Vec<u8>),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
}

impl PeerMessage {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            PeerMessage::KeepAlive => {
                buf.extend_from_slice(&0u32.to_be_bytes());
            }
            PeerMessage::Choke => {
                buf.extend_from_slice(&1u32.to_be_bytes());
                buf.push(0);
            }
            PeerMessage::Unchoke => {
                buf.extend_from_slice(&1u32.to_be_bytes());
                buf.push(1);
            }
            PeerMessage::Interested => {
                buf.extend_from_slice(&1u32.to_be_bytes());
                buf.push(2);
            }
            PeerMessage::NotInterested => {
                buf.extend_from_slice(&1u32.to_be_bytes());
                buf.push(3);
            }
            PeerMessage::Have { index } => {
                buf.extend_from_slice(&5u32.to_be_bytes());
                buf.push(4);
                buf.extend_from_slice(&index.to_be_bytes());
            }
            PeerMessage::Bitfield(ref bits) => {
                let len = (1 + bits.len()) as u32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.push(5);
                buf.extend_from_slice(bits);
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(6);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
            }
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => {
                let len = (9 + block.len()) as u32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.push(7);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(block);
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(8);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
            }
        }
        buf
    }

    pub async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Self> {
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        if len == 0 {
            return Ok(PeerMessage::KeepAlive);
        }

        let mut id_bytes = [0u8; 1];
        reader.read_exact(&mut id_bytes).await?;
        let id = id_bytes[0];

        match id {
            0 => Ok(PeerMessage::Choke),
            1 => Ok(PeerMessage::Unchoke),
            2 => Ok(PeerMessage::Interested),
            3 => Ok(PeerMessage::NotInterested),
            4 => {
                let mut idx_bytes = [0u8; 4];
                reader.read_exact(&mut idx_bytes).await?;
                Ok(PeerMessage::Have {
                    index: u32::from_be_bytes(idx_bytes),
                })
            }
            5 => {
                let mut bitfield = vec![0u8; len - 1];
                reader.read_exact(&mut bitfield).await?;
                Ok(PeerMessage::Bitfield(bitfield))
            }
            6 => {
                let mut buf = [0u8; 12];
                reader.read_exact(&mut buf).await?;
                Ok(PeerMessage::Request {
                    index: u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]),
                    begin: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
                    length: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
                })
            }
            7 => {
                let mut header = [0u8; 8];
                reader.read_exact(&mut header).await?;
                let index = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
                let begin = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
                let mut block = vec![0u8; len - 9];
                reader.read_exact(&mut block).await?;
                Ok(PeerMessage::Piece {
                    index,
                    begin,
                    block,
                })
            }
            8 => {
                let mut buf = [0u8; 12];
                reader.read_exact(&mut buf).await?;
                Ok(PeerMessage::Cancel {
                    index: u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]),
                    begin: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
                    length: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
                })
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown message ID: {}", id),
            )),
        }
    }
}
