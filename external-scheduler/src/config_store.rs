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
    #[serde(default)]
    pub logs_server: Vec<String>,
    pub filter_keys: HashSet<Pubkey>,
    pub scheduler: SchedulerConfigData,
}

impl Default for ConfigData {
    fn default() -> Self {
        Self {
            logs_server: Vec::new(),
            filter_keys: HashSet::new(),
            scheduler: SchedulerConfigData::BatchScheduler(BatchSchedulerConfigData::default()),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub enum SchedulerConfigData {
    BatchScheduler(BatchSchedulerConfigData),
    TighterBatchScheduler(TighterBatchSchedulerConfigData),
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TighterBatchSchedulerConfigData {
    pub keypair_path: String,
    pub tip: TipDistributionConfigData,
    pub jito: JitoConfigData,
    // Scoring weights for composite value-score
    pub weight_fee: u64,
    pub weight_efficiency: u64,
    pub min_score: u64,
    // Runtime-tunable params (not persisted, updated via UI)
    pub unchecked_capacity: usize,
    pub checked_capacity: usize,
    pub bundle_capacity: usize,
    pub block_fill_cutoff: u8,
    pub max_check_batches: u8,
    pub bundle_expiry_ms: u64,
    pub progress_timeout_sec: u64,
}

impl Default for TighterBatchSchedulerConfigData {
    fn default() -> Self {
        Self {
            keypair_path: String::new(),
            tip: TipDistributionConfigData::default(),
            jito: JitoConfigData::default(),
            weight_fee: 1,
            weight_efficiency: 1,
            min_score: 0,
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
}

#[derive(Debug, serde::Deserialize)]
struct FileConfigData {
    #[serde(default)]
    logs_server: Vec<String>,
    #[serde(default)]
    filter_keys: HashSet<Pubkey>,
    scheduler: FileSchedulerConfigData,
}

#[derive(Debug, serde::Deserialize)]
struct FileSchedulerConfigData {
    #[serde(rename = "Batch")]
    batch: FileBatchSchedulerConfigData,
    #[serde(rename = "TighterBatch")]
    tighter_batch: FileTighterBatchSchedulerConfigData,
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

#[derive(Debug, serde::Deserialize)]
struct FileTighterBatchSchedulerConfigData {
    keypair_path: String,
    tip: TipDistributionConfigData,
    jito: JitoConfigData,
    #[serde(default = "default_weight_fee")]
    weight_fee: u64,
    #[serde(default = "default_weight_efficiency")]
    weight_efficiency: u64,
    #[serde(default = "default_min_score")]
    min_score: u64,
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
            logs_server: file_config.logs_server,
            filter_keys: file_config.filter_keys,
            scheduler: SchedulerConfigData::BatchScheduler(file_config.scheduler.batch.into()),
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

impl From<FileTighterBatchSchedulerConfigData> for TighterBatchSchedulerConfigData {
    fn from(file_config: FileTighterBatchSchedulerConfigData) -> Self {
        Self {
            keypair_path: file_config.keypair_path,
            tip: file_config.tip,
            jito: file_config.jito,
            weight_fee: file_config.weight_fee,
            weight_efficiency: file_config.weight_efficiency,
            min_score: file_config.min_score,
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

fn default_weight_fee() -> u64 {
    1
}

fn default_weight_efficiency() -> u64 {
    1
}

fn default_min_score() -> u64 {
    0
}
