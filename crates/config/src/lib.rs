use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub download_dir: String,
    pub metadata_dir: String,
    pub data_write_frequency_secs: u64,
    pub default_trackers: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            download_dir: "downloads".to_string(),
            metadata_dir: "metadata".to_string(),
            data_write_frequency_secs: 5,
            default_trackers: vec![
                "udp://tracker.opentrackr.org:1337/announce".to_string(),
                "udp://tracker.openbittorrent.com:80/announce".to_string(),
            ],
        }
    }
}

impl Config {
    pub fn load() -> Self {
        let path = "config.json";
        if let Ok(data) = std::fs::read_to_string(path) {
            match serde_json::from_str(&data) {
                Ok(config) => config,
                Err(e) => {
                    eprintln!("Failed to parse config.json, using defaults: {}", e);
                    Self::default()
                }
            }
        } else {
            let default = Self::default();
            if let Ok(json) = serde_json::to_string_pretty(&default) {
                let _ = std::fs::write(path, json);
            }
            default
        }
    }
}
