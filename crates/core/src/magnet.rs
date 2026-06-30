use crate::InfoHash;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MagnetError {
    #[error("Invalid magnet link prefix")]
    InvalidPrefix,
    #[error("Missing exact topic (xt)")]
    MissingXt,
    #[error("Invalid info hash format")]
    InvalidInfoHash,
    #[error("Invalid URL encoding")]
    InvalidUrlEncoding,
}

#[derive(Debug, Clone)]
pub struct MagnetLink {
    pub info_hash: InfoHash,
    pub trackers: Vec<String>,
    pub name: Option<String>,
}

impl MagnetLink {
    pub fn parse(url: &str) -> Result<Self, MagnetError> {
        if !url.starts_with("magnet:?") {
            return Err(MagnetError::InvalidPrefix);
        }

        let query = &url[8..];
        let mut info_hash = None;
        let mut trackers = Vec::new();
        let mut name = None;

        for pair in query.split('&') {
            if let Some(xt) = pair.strip_prefix("xt=") {
                if let Some(hash) = xt.strip_prefix("urn:btih:") {
                    if hash.len() == 40 {
                        let mut bytes = [0u8; 20];
                        if decode_hex(hash, &mut bytes) {
                            info_hash = Some(InfoHash(bytes));
                        }
                    } else if hash.len() == 32 {
                        // Base32 support could go here if needed
                    }
                }
            } else if let Some(tr) = pair.strip_prefix("tr=") {
                if let Ok(decoded) = url_decode(tr) {
                    trackers.push(decoded);
                }
            } else if let Some(dn) = pair.strip_prefix("dn=") {
                if let Ok(decoded) = url_decode(dn) {
                    name = Some(decoded);
                }
            }
        }

        let info_hash = info_hash.ok_or(MagnetError::MissingXt)?;

        Ok(Self {
            info_hash,
            trackers,
            name,
        })
    }
}

fn decode_hex(s: &str, out: &mut [u8]) -> bool {
    if !s.len().is_multiple_of(2) || s.len() / 2 != out.len() {
        return false;
    }
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let high = parse_hex_digit(chunk[0]);
        let low = parse_hex_digit(chunk[1]);
        if high == 255 || low == 255 {
            return false;
        }
        out[i] = (high << 4) | low;
    }
    true
}

fn parse_hex_digit(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 255,
    }
}

fn url_decode(s: &str) -> Result<String, MagnetError> {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 < bytes.len() {
                let high = parse_hex_digit(bytes[i + 1]);
                let low = parse_hex_digit(bytes[i + 2]);
                if high != 255 && low != 255 {
                    out.push((high << 4) | low);
                    i += 3;
                    continue;
                }
            }
            return Err(MagnetError::InvalidUrlEncoding);
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| MagnetError::InvalidUrlEncoding)
}
