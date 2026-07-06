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
    pub logs_server: Option<String>,
    pub filter_keys: HashSet<Pubkey>,
    pub scheduler: SchedulerConfig,
}

impl Default for ConfigData {
    fn default() -> Self {
        Self {
            logs_server: None,
            filter_keys: HashSet::new(),
            scheduler: SchedulerConfig::default(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct SchedulerConfig {
    #[serde(rename = "Batch")]
    pub batch: Option<BatchSchedulerConfigData>,
    #[serde(rename = "Auction")]
    pub auction: Option<AuctionBatchSchedulerConfigData>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AuctionBatchSchedulerConfigData {
    pub keypair_path: String,
    pub tip: TipDistributionConfigData,
    pub jito: JitoConfigData,
    pub unchecked_capacity: usize,
    pub checked_capacity: usize,
    pub bundle_capacity: usize,
    pub block_fill_cutoff: u8,
    pub max_check_batches: u8,
    pub bundle_expiry_ms: u64,
    pub progress_timeout_sec: u64,
    pub scoring: Option<AuctionBatchScoringConfig>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AuctionBatchScoringConfig {
    pub min_score: u64,
}

impl Default for AuctionBatchSchedulerConfigData {
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
            scoring: None,
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
        let file_config: ConfigData = toml::from_str(&contents)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(file_config)),
        })
    }

    pub fn read(&self) -> ConfigData {
        self.inner.read().unwrap().clone()
    }
}

#[allow(dead_code)]
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

#[allow(dead_code)]
fn default_unchecked_capacity() -> usize {
    64 * 1024
}

#[allow(dead_code)]
fn default_checked_capacity() -> usize {
    64 * 1024
}

#[allow(dead_code)]
fn default_bundle_capacity() -> usize {
    1024
}

#[allow(dead_code)]
fn default_block_fill_cutoff() -> u8 {
    20
}

#[allow(dead_code)]
fn default_max_check_batches() -> u8 {
    4
}

#[allow(dead_code)]
fn default_bundle_expiry_ms() -> u64 {
    200
}

#[allow(dead_code)]
fn default_progress_timeout_sec() -> u64 {
    5
}

#[allow(dead_code)]
fn default_weight_fee() -> u64 {
    1
}

#[allow(dead_code)]
fn default_weight_efficiency() -> u64 {
    1
}

#[allow(dead_code)]
fn default_min_score() -> u64 {
    0
}
