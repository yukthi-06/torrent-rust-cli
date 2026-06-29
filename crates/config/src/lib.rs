use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub download_dir: PathBuf,
    pub listen_port: u16,
    pub max_upload: Option<usize>,   // in bytes/sec, None means unlimited
    pub max_download: Option<usize>, // in bytes/sec, None means unlimited
    pub max_connections: usize,
    pub enable_dht: bool,
    pub enable_utp: bool,
    pub enable_upnp: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            download_dir: PathBuf::from("downloads"),
            listen_port: 6881,
            max_upload: None,
            max_download: None,
            max_connections: 200,
            enable_dht: true,
            enable_utp: true,
            enable_upnp: true,
        }
    }
}

impl Config {
    pub fn load_from_str(s: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(s)
    }

    pub fn to_string(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }
}
