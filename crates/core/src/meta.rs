use crate::bencode::{self, Bencode};
use crate::InfoHash;
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("Bencode decoding error: {0}")]
    BencodeError(#[from] bencode::BencodeError),
    #[error("Missing dictionary field: '{0}'")]
    MissingField(String),
    #[error("Invalid field type for '{0}'")]
    InvalidType(String),
    #[error("Missing info dictionary slice in bencode")]
    MissingInfoSlice,
    #[error("Invalid pieces field length: expected multiple of 20, got {0}")]
    InvalidPiecesLength(usize),
}

#[derive(Debug, Clone)]
pub struct TorrentMeta {
    pub announce: String,
    pub announce_list: Option<Vec<Vec<String>>>,
    pub info: InfoDict,
    pub info_hash: InfoHash,
}

#[derive(Debug, Clone)]
pub struct InfoDict {
    pub name: String,
    pub piece_length: u64,
    pub pieces: Vec<[u8; 20]>,
    pub mode: FileMode,
}

#[derive(Debug, Clone)]
pub enum FileMode {
    Single { length: u64 },
    Multi { files: Vec<TorrentFile> },
}

#[derive(Debug, Clone)]
pub struct TorrentFile {
    pub length: u64,
    pub path: Vec<String>,
}

impl TorrentMeta {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MetaError> {
        let bencode = Bencode::decode(bytes)?;
        let dict = match bencode {
            Bencode::Dict(d) => d,
            _ => return Err(MetaError::InvalidType("root".to_string())),
        };

        // Extract announce URL
        let announce = get_string(&dict, b"announce")?;

        // Extract announce-list (optional tier-list of trackers)
        let announce_list = get_announce_list(&dict);

        // Extract info dictionary representation
        let info_val = dict.get(b"info".as_ref())
            .ok_or_else(|| MetaError::MissingField("info".to_string()))?;
        
        let info_dict = match info_val {
            Bencode::Dict(d) => d,
            _ => return Err(MetaError::InvalidType("info".to_string())),
        };

        let name = get_string(info_dict, b"name")?;
        let piece_length = get_int(info_dict, b"piece length")? as u64;

        // Parse pieces (a single concatenated byte string of 20-byte SHA-1 hashes)
        let pieces_bytes = get_bytes(info_dict, b"pieces")?;
        if pieces_bytes.len() % 20 != 0 {
            return Err(MetaError::InvalidPiecesLength(pieces_bytes.len()));
        }
        let mut pieces = Vec::new();
        for chunk in pieces_bytes.chunks_exact(20) {
            let mut arr = [0u8; 20];
            arr.copy_from_slice(chunk);
            pieces.push(arr);
        }

        // Determine if single or multi file mode
        let mode = if info_dict.contains_key(b"length".as_ref()) {
            let length = get_int(info_dict, b"length")? as u64;
            FileMode::Single { length }
        } else if info_dict.contains_key(b"files".as_ref()) {
            let files_val = info_dict.get(b"files".as_ref()).unwrap();
            let files_list = match files_val {
                Bencode::List(l) => l,
                _ => return Err(MetaError::InvalidType("files".to_string())),
            };
            let mut files = Vec::new();
            for file_val in files_list {
                let file_dict = match file_val {
                    Bencode::Dict(d) => d,
                    _ => return Err(MetaError::InvalidType("file item".to_string())),
                };
                let length = get_int(file_dict, b"length")? as u64;
                let path_val = file_dict.get(b"path".as_ref())
                    .ok_or_else(|| MetaError::MissingField("path".to_string()))?;
                let path_list = match path_val {
                    Bencode::List(l) => l,
                    _ => return Err(MetaError::InvalidType("path".to_string())),
                };
                let mut path = Vec::new();
                for segment_val in path_list {
                    let segment = match segment_val {
                        Bencode::ByteString(s) => String::from_utf8_lossy(s).into_owned(),
                        _ => return Err(MetaError::InvalidType("path segment".to_string())),
                    };
                    path.push(segment);
                }
                files.push(TorrentFile { length, path });
            }
            FileMode::Multi { files }
        } else {
            return Err(MetaError::MissingField("length or files".to_string()));
        };

        // Calculate exact info dictionary hash
        let info_slice = bencode::find_info_dict_slice(bytes)
            .ok_or(MetaError::MissingInfoSlice)?;
        let mut hasher = Sha1::new();
        hasher.update(info_slice);
        let info_hash = InfoHash(hasher.finalize().into());

        Ok(TorrentMeta {
            announce,
            announce_list,
            info: InfoDict {
                name,
                piece_length,
                pieces,
                mode,
            },
            info_hash,
        })
    }
}

// Helper methods to extract values from Bencode Dict
fn get_string(dict: &BTreeMap<Vec<u8>, Bencode>, key: &[u8]) -> Result<String, MetaError> {
    let bytes = get_bytes(dict, key)?;
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

fn get_bytes<'a>(dict: &'a BTreeMap<Vec<u8>, Bencode>, key: &[u8]) -> Result<&'a [u8], MetaError> {
    let val = dict.get(key)
        .ok_or_else(|| MetaError::MissingField(String::from_utf8_lossy(key).into_owned()))?;
    match val {
        Bencode::ByteString(ref s) => Ok(s),
        _ => Err(MetaError::InvalidType(String::from_utf8_lossy(key).into_owned())),
    }
}

fn get_int(dict: &BTreeMap<Vec<u8>, Bencode>, key: &[u8]) -> Result<i64, MetaError> {
    let val = dict.get(key)
        .ok_or_else(|| MetaError::MissingField(String::from_utf8_lossy(key).into_owned()))?;
    match val {
        Bencode::Int(i) => Ok(*i),
        _ => Err(MetaError::InvalidType(String::from_utf8_lossy(key).into_owned())),
    }
}

fn get_announce_list(dict: &BTreeMap<Vec<u8>, Bencode>) -> Option<Vec<Vec<String>>> {
    let val = dict.get(b"announce-list".as_ref())?;
    let outer_list = match val {
        Bencode::List(l) => l,
        _ => return None,
    };
    let mut result = Vec::new();
    for inner_val in outer_list {
        let inner_list = match inner_val {
            Bencode::List(l) => l,
            _ => continue,
        };
        let mut sub_result = Vec::new();
        for string_val in inner_list {
            if let Bencode::ByteString(s) = string_val {
                sub_result.push(String::from_utf8_lossy(s).into_owned());
            }
        }
        if !sub_result.is_empty() {
            result.push(sub_result);
        }
    }
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_file_torrent() {
        // Construct a mock single-file torrent bencoded payload:
        // d
        //   8:announce 43:http://torrent.ubuntu.com:6969/announce
        //   4:info d
        //     6:length i42e
        //     4:name 10:ubuntu.iso
        //     12:piece length i16384e
        //     6:pieces 20:abcdefghijklmnopqrst
        //   e
        // e
        let data = b"d8:announce43:http://torrent.ubuntu.com:6969/announce4:infod6:lengthi42e4:name10:ubuntu.iso12:piece lengthi16384e6:pieces20:abcdefghijklmnopqrstee";
        
        let meta = TorrentMeta::from_bytes(data).unwrap();
        assert_eq!(meta.announce, "http://torrent.ubuntu.com:6969/announce");
        assert_eq!(meta.info.name, "ubuntu.iso");
        assert_eq!(meta.info.piece_length, 16384);
        assert_eq!(meta.info.pieces.len(), 1);
        assert_eq!(&meta.info.pieces[0], b"abcdefghijklmnopqrst");

        if let FileMode::Single { length } = meta.info.mode {
            assert_eq!(length, 42);
        } else {
            panic!("Expected single file mode");
        }

        // Infohash check
        assert_eq!(meta.info_hash.0.len(), 20);
    }
}

