use anyhow::Result;
use sha1::{Digest, Sha1};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use torrent_core::meta::{FileMode, InfoDict, TorrentFile, TorrentMeta};
use torrent_core::InfoHash;

pub const DEFAULT_PIECE_LENGTH: u64 = 524288; // 512KB

fn visit_dirs(dir: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, files)?;
            } else {
                files.push(path);
            }
        }
    }
    Ok(())
}

pub fn create_torrent(path_str: &str, trackers: Vec<String>) -> Result<Vec<u8>> {
    let path = Path::new(path_str);
    if !path.exists() {
        return Err(anyhow::anyhow!("Path does not exist: {}", path_str));
    }

    let mut pieces = Vec::new();
    let mode;
    let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();

    let mut buffer = vec![0u8; DEFAULT_PIECE_LENGTH as usize];
    let mut current_piece_len = 0;
    let mut hasher = Sha1::new();

    let mut hash_piece = |data: &[u8], len: usize| {
        let mut offset = 0;
        while offset < len {
            let space = (DEFAULT_PIECE_LENGTH as usize) - current_piece_len;
            let take = space.min(len - offset);
            hasher.update(&data[offset..offset + take]);
            current_piece_len += take;
            offset += take;

            if current_piece_len == DEFAULT_PIECE_LENGTH as usize {
                let mut hash = [0u8; 20];
                hash.copy_from_slice(&hasher.finalize_reset());
                pieces.push(hash);
                current_piece_len = 0;
            }
        }
    };

    if path.is_file() {
        let length = path.metadata()?.len();
        mode = FileMode::Single { length };

        let mut file = File::open(path)?;
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash_piece(&buffer, n);
        }
    } else {
        let mut file_paths = Vec::new();
        visit_dirs(path, &mut file_paths)?;
        
        // BitTorrent standard: files sorted by path
        file_paths.sort();

        let mut files = Vec::new();
        for file_path in file_paths {
            let length = file_path.metadata()?.len();
            
            // Get relative path components
            let rel_path = file_path.strip_prefix(path)?;
            let path_components = rel_path
                .components()
                .map(|c| c.as_os_str().to_string_lossy().to_string())
                .collect::<Vec<_>>();

            files.push(TorrentFile {
                length,
                path: path_components,
            });

            let mut file = File::open(file_path)?;
            loop {
                let n = file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                hash_piece(&buffer, n);
            }
        }
        mode = FileMode::Multi { files };
    }

    // Hash the final piece if there is any remaining data
    if current_piece_len > 0 {
        let mut hash = [0u8; 20];
        hash.copy_from_slice(&hasher.finalize_reset());
        pieces.push(hash);
    }

    let info = InfoDict {
        name,
        piece_length: DEFAULT_PIECE_LENGTH,
        pieces,
        mode,
    };

    // Calculate info_hash
    let info_bytes = info.to_bencode().encode();
    let mut info_hasher = Sha1::new();
    info_hasher.update(&info_bytes);
    let mut info_hash_arr = [0u8; 20];
    info_hash_arr.copy_from_slice(&info_hasher.finalize());

    println!("[DEBUG] Daemon create_torrent called with {} trackers", trackers.len());

    let announce = trackers
        .first()
        .cloned()
        .unwrap_or_else(|| "udp://tracker.opentrackr.org:1337/announce".to_string());

    let announce_list = if trackers.len() > 1 {
        let list: Vec<Vec<String>> = trackers.into_iter().map(|t| vec![t]).collect();
        println!("[DEBUG] Generating announce-list with {} tiers", list.len());
        Some(list)
    } else {
        println!("[DEBUG] Skipping announce-list because trackers count is <= 1");
        None
    };

    let meta = TorrentMeta {
        announce,
        announce_list,
        info,
        info_hash: InfoHash(info_hash_arr),
    };

    Ok(meta.into_bytes())
}
