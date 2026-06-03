use std::collections::HashSet;
use std::sync::Arc;
use std::{error::Error, fs, path::Path};

use solana_pubkey::Pubkey;
use std::sync::RwLock;

/// A thread-safe, updatable config store.
/// The scheduler reads from this at runtime; the HTTP server writes to it.
#[derive(Debug, Clone)]
pub struct ConfigStore {
    inner: Arc<RwLock<ConfigData>>,
}

impl Default for ConfigStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ConfigData::default())),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConfigData {
    pub host_name: String,
    #[serde(default)]
    pub nats_servers: Vec<String>,
    pub filter_keys: HashSet<Pubkey>,
    pub scheduler: SchedulerConfigData,
}

impl Default for ConfigData {
    fn default() -> Self {
        Self {
            host_name: "dev".to_string(),
            nats_servers: Vec::new(),
            filter_keys: HashSet::new(),
            scheduler: SchedulerConfigData::Batch(BatchSchedulerConfigData::default()),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub enum SchedulerConfigData {
    Batch(BatchSchedulerConfigData),
    Fifo,
    GreedyRevenue,
    GreedyThroughput,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BatchSchedulerConfigData {
    pub keypair_path: String,
    pub tip: TipDistributionConfigData,
    pub jito: JitoConfigData,
    // Runtime-tunable params (not persisted, updated via UI)
    pub unchecked_capacity: usize,
    pub checked_capacity: usize,
    pub bundle_capacity: usize,
    pub block_fill_cutoff: u8,
    pub max_check_batches: u8,
    pub bundle_expiry_ms: u64,
    pub progress_timeout_sec: u64,
}

impl Default for BatchSchedulerConfigData {
    fn default() -> Self {
        Self {
            keypair_path: String::new(),
            tip: TipDistributionConfigData::default(),
            jito: JitoConfigData::default(),
            unchecked_capacity: 64 * 1024,
            checked_capacity: 64 * 1024,
            bundle_capacity: 1024,
            block_fill_cutoff: 20,
            max_check_batches: 4,
            bundle_expiry_ms: 200,
            progress_timeout_sec: 5,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TipDistributionConfigData {
    pub vote_account: String,
    pub merkle_authority: String,
    pub commission_bps: u16,
}

impl Default for TipDistributionConfigData {
    fn default() -> Self {
        Self {
            vote_account: String::new(),
            merkle_authority: String::new(),
            commission_bps: 0,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct JitoConfigData {
    pub http_rpc: String,
    pub ws_rpc: String,
    pub block_engine: String,
}

impl Default for JitoConfigData {
    fn default() -> Self {
        Self {
            http_rpc: String::new(),
            ws_rpc: String::new(),
            block_engine: String::new(),
        }
    }
}

impl ConfigStore {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let contents = fs::read_to_string(path)?;
        let file_config: FileConfigData = toml::from_str(&contents)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(file_config.into())),
        })
    }

    pub fn read(&self) -> ConfigData {
        self.inner.read().unwrap().clone()
    }

    pub fn update(&self, update: ConfigUpdate) {
        let mut data = self.inner.write().unwrap();

        if let Some(host_name) = update.host_name {
            data.host_name = host_name;
        }

        if let Some(scheduler) = update.scheduler {
            data.scheduler = scheduler;
        }

        if let Some(filter_keys) = update.filter_keys {
            data.filter_keys = filter_keys;
        }
    }
}

/// Updates that can be applied to the config store.
#[derive(Debug, Default, serde::Deserialize)]
pub struct ConfigUpdate {
    pub host_name: Option<String>,
    pub scheduler: Option<SchedulerConfigData>,
    pub filter_keys: Option<HashSet<Pubkey>>,
}

#[derive(Debug, serde::Deserialize)]
struct FileConfigData {
    host_name: String,
    #[serde(default)]
    nats_servers: Vec<String>,
    #[serde(default)]
    filter_keys: HashSet<Pubkey>,
    scheduler: FileSchedulerConfigData,
}

#[derive(Debug, serde::Deserialize)]
struct FileSchedulerConfigData {
    #[serde(rename = "Batch")]
    batch: FileBatchSchedulerConfigData,
}

#[derive(Debug, serde::Deserialize)]
struct FileBatchSchedulerConfigData {
    keypair_path: String,
    tip: TipDistributionConfigData,
    jito: JitoConfigData,
    #[serde(default = "default_unchecked_capacity")]
    unchecked_capacity: usize,
    #[serde(default = "default_checked_capacity")]
    checked_capacity: usize,
    #[serde(default = "default_bundle_capacity")]
    bundle_capacity: usize,
    #[serde(default = "default_block_fill_cutoff")]
    block_fill_cutoff: u8,
    #[serde(default = "default_max_check_batches")]
    max_check_batches: u8,
    #[serde(default = "default_bundle_expiry_ms")]
    bundle_expiry_ms: u64,
    #[serde(default = "default_progress_timeout_sec")]
    progress_timeout_sec: u64,
}

impl From<FileConfigData> for ConfigData {
    fn from(file_config: FileConfigData) -> Self {
        Self {
            host_name: file_config.host_name,
            nats_servers: file_config.nats_servers,
            filter_keys: file_config.filter_keys,
            scheduler: SchedulerConfigData::Batch(file_config.scheduler.batch.into()),
        }
    }
}

impl From<FileBatchSchedulerConfigData> for BatchSchedulerConfigData {
    fn from(file_config: FileBatchSchedulerConfigData) -> Self {
        Self {
            keypair_path: file_config.keypair_path,
            tip: file_config.tip,
            jito: file_config.jito,
            unchecked_capacity: file_config.unchecked_capacity,
            checked_capacity: file_config.checked_capacity,
            bundle_capacity: file_config.bundle_capacity,
            block_fill_cutoff: file_config.block_fill_cutoff,
            max_check_batches: file_config.max_check_batches,
            bundle_expiry_ms: file_config.bundle_expiry_ms,
            progress_timeout_sec: file_config.progress_timeout_sec,
        }
    }
}

fn default_unchecked_capacity() -> usize {
    64 * 1024
}

fn default_checked_capacity() -> usize {
    64 * 1024
}

fn default_bundle_capacity() -> usize {
    1024
}

fn default_block_fill_cutoff() -> u8 {
    20
}

fn default_max_check_batches() -> u8 {
    4
}

fn default_bundle_expiry_ms() -> u64 {
    200
}

fn default_progress_timeout_sec() -> u64 {
    5
}
