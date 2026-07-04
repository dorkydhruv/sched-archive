//! Auction-based block scheduler with centralized lock management.
//!
//! This module implements a real-time block auction engine that prices
//! CU, locks, time, and space separately, then accepts transactions
//! that create net positive value for the entire block.
//!
//! ## Design
//!
//! - **Centralized lock management**: A single `LockManager` owns all lock state.
//!   Locks are acquired at schedule-time (not queue-time) and released on
//!   execution response (OK/ERR/Unprocessed).
//!
//! - **Auction scoring**: Each transaction is scored using auction surplus:
//!     `surplus = fee - resource_cost * (1 - flexibility_discount) + parallelism_bonus`
//!   Positive surplus = adds value to the block.
//!
//! - **Bundle awareness**: Bundles are pre-scanned (all txs validated together).
//!   If any tx in a bundle fails validation, the entire bundle is dropped.
//!   Bundles are scheduled atomically — all txs or none.
//!
//! - **Schedule-time locking**: Locks are only acquired when a transaction
//!   is actually scheduled for execution, not when it enters the queue.
//!   This prevents stale locks from blocking valid transactions.

use std::collections::HashMap;
use std::time::Instant;

use agave_scheduling_utils::bridge::TransactionKey;
use solana_pubkey::Pubkey;

/// Tracks which transactions hold locks on each account.
#[derive(Debug, Clone)]
pub struct AccountLockers {
    /// Map from transaction key to lock type (true = write, false = read).
    lockers: HashMap<TransactionKey, bool>,
}

impl Default for AccountLockers {
    fn default() -> Self {
        Self {
            lockers: HashMap::new(),
        }
    }
}

impl AccountLockers {
    /// Check if the requested lock can be granted.
    pub fn can_lock(&self, writable: bool) -> bool {
        if writable {
            // Write lock requires no existing locks (read or write).
            self.lockers.is_empty()
        } else {
            // Read lock requires no existing write locks.
            self.lockers.values().all(|&w| !w)
        }
    }

    /// Acquire a lock for the given transaction.
    pub fn acquire(&mut self, tx_key: TransactionKey, writable: bool) {
        self.lockers.insert(tx_key, writable);
    }

    /// Release all locks for the given transaction.
    pub fn release(&mut self, tx_key: TransactionKey) {
        self.lockers.remove(&tx_key);
    }

    /// Number of transactions holding locks on this account.
    pub fn len(&self) -> usize {
        self.lockers.len()
    }

    /// Whether any transaction holds a lock on this account.
    pub fn is_locked(&self) -> bool {
        !self.lockers.is_empty()
    }

    /// Whether the lockers map is empty.
    pub fn is_empty(&self) -> bool {
        self.lockers.is_empty()
    }
}

/// Centralized lock manager for the auction engine.
///
/// This is the single source of truth for all lock state. It tracks:
/// - Which transactions hold locks on which accounts
/// - Queue depth per account (for serialization penalty calculation)
///
/// Lock lifecycle:
/// 1. **Check**: `try_acquire` — dry-run, no state change
/// 2. **Acquire**: `acquire` — lock at schedule-time
/// 3. **Release**: `release` — on execution response (OK/ERR/Unprocessed)
#[derive(Debug)]
pub struct LockManager {
    /// Per-account lock state.
    account_locks: HashMap<Pubkey, AccountLockers>,
    /// Per-account queue depth (how many txs are waiting for this account).
    queue_depths: HashMap<Pubkey, f64>,
    /// Decay rate for queue depths.
    decay_rate: f64,
}

impl Default for LockManager {
    fn default() -> Self {
        Self {
            account_locks: HashMap::new(),
            queue_depths: HashMap::new(),
            decay_rate: 0.1,
        }
    }
}

impl LockManager {
    /// Create a new lock manager with a custom decay rate.
    pub fn with_decay_rate(decay_rate: f64) -> Self {
        Self {
            decay_rate,
            ..Self::default()
        }
    }

    /// Check if locks can be acquired without actually acquiring them.
    /// Returns true if all locks can be granted.
    pub fn try_acquire(&self, tx_locks: &[(Pubkey, bool)]) -> bool {
        for (addr, writable) in tx_locks {
            match self.account_locks.get(addr) {
                None => continue,
                Some(lockers) if !lockers.can_lock(*writable) => return false,
                _ => {}
            }
        }
        true
    }

    /// Acquire locks for a transaction.
    pub fn acquire(&mut self, tx_key: TransactionKey, tx_locks: &[(Pubkey, bool)]) {
        for (addr, writable) in tx_locks {
            let lockers = self.account_locks.entry(*addr).or_default();
            lockers.acquire(tx_key, *writable);
        }
    }

    /// Release all locks for a transaction.
    pub fn release(&mut self, tx_key: TransactionKey, tx_locks: &[(Pubkey, bool)]) {
        for (addr, _) in tx_locks {
            if let Some(lockers) = self.account_locks.get_mut(addr) {
                lockers.release(tx_key);
                if lockers.is_empty() {
                    self.account_locks.remove(addr);
                }
            }
        }
    }

    /// Increment queue depth for a transaction (called when tx enters queue).
    pub fn queue_transaction(&mut self, tx_locks: &[(Pubkey, bool)]) {
        for (addr, _) in tx_locks {
            let depth = self.queue_depths.entry(*addr).or_insert(0.0);
            *depth += 1.0;
        }
    }

    /// Decrement queue depth for a transaction (called when tx leaves queue).
    pub fn dequeue_transaction(&mut self, tx_locks: &[(Pubkey, bool)]) {
        for (addr, _) in tx_locks {
            if let Some(depth) = self.queue_depths.get_mut(addr) {
                *depth = depth.max(0.0) - 1.0;
                if *depth < 0.01 {
                    self.queue_depths.remove(addr);
                }
            }
        }
    }

    /// Decay all queue depths based on elapsed time (in milliseconds).
    pub fn decay(&mut self, elapsed_ms: f64) {
        // Decay rate is fraction decayed per second (1000ms).
        let decay_factor = (1.0 - self.decay_rate).powf(elapsed_ms / 1000.0);
        for depth in self.queue_depths.values_mut() {
            *depth *= decay_factor;
        }
        self.queue_depths.retain(|_, depth| *depth > 0.01);
    }

    /// Compute serialization penalty for a transaction.
    ///
    /// Formula: `(sum(queue_depths) * AVG_EXEC_MS_PER_TX) / MAX_SLOT_MS`
    pub fn serialization_penalty(&self, tx_locks: &[(Pubkey, bool)]) -> f64 {
        let avg_exec_ms = 0.075;
        let max_slot_ms = 400.0;

        let queue_depth_sum: f64 = tx_locks
            .iter()
            .map(|(addr, _)| self.queue_depths.get(addr).copied().unwrap_or(0.0))
            .sum();

        let predicted_blocked_ms = queue_depth_sum * avg_exec_ms;
        (predicted_blocked_ms / max_slot_ms).clamp(0.0, 0.99)
    }

    /// Get total queue depth across all accounts.
    pub fn total_queue_depth(&self) -> f64 {
        self.queue_depths.values().sum()
    }

    /// Get queue depth for a specific account.
    pub fn queue_depth(&self, account: &Pubkey) -> f64 {
        self.queue_depths.get(account).copied().unwrap_or(0.0)
    }

    /// Reset all state (call at slot boundary).
    pub fn reset(&mut self) {
        self.account_locks.clear();
        // Do not clear queue_depths, as queued transactions carry over to the next slot.
    }

    /// Number of accounts with active locks.
    pub fn tracked_accounts(&self) -> usize {
        self.account_locks.len()
    }
}

/// Configuration for the auction engine.
#[derive(Debug, Clone)]
pub struct AuctionEngineConfig {
    /// CU price update alpha.
    pub cu_alpha: f64,
    /// Lock price update alpha.
    pub lock_alpha: f64,
    /// Time price update alpha.
    pub time_alpha: f64,
    /// Space price update alpha.
    pub space_alpha: f64,
    /// Promote threshold for CU allocation.
    pub promote_threshold: f64,
    /// Release threshold for CU allocation.
    pub release_threshold: f64,
    /// Lock queue decay rate.
    pub lock_decay_rate: f64,
    /// Entropy flexibility theta.
    pub entropy_theta: f64,
    /// Entropy flexibility k.
    pub entropy_k: f64,
    /// Maximum tick budget in milliseconds.
    pub max_tick_ms: f64,

    // Base prices for resource costing in lamports
    pub base_cu_price: f64,
    pub base_write_lock_price: f64,
    pub base_read_lock_price: f64,
    pub base_time_price: f64,
    pub base_space_price: f64,
}

impl Default for AuctionEngineConfig {
    fn default() -> Self {
        Self {
            cu_alpha: 0.1,
            lock_alpha: 0.15,
            time_alpha: 0.2,
            space_alpha: 0.05,
            promote_threshold: 5.0,
            release_threshold: 0.5,
            lock_decay_rate: 0.1,
            entropy_theta: 0.5,
            entropy_k: 2.0,
            max_tick_ms: 1.5,
            base_cu_price: 0.01, // lamports per CU (200k CU = 2000 lamports)
            base_write_lock_price: 1000.0, // lamports per write lock
            base_read_lock_price: 200.0, // lamports per read lock
            base_time_price: 500.0, // base time penalty multiplier
            base_space_price: 1000.0, // fragmentation penalty multiplier
        }
    }
}

/// Statistics from a single tick.
#[derive(Debug, Clone, Default)]
pub struct TickStats {
    /// Total CU demanded this tick.
    pub cu_demand: f64,
    /// Total write locks this tick.
    pub lock_demand: f64,
    /// Milliseconds elapsed this tick.
    pub time_elapsed: f64,
    /// Fragmentation index (0-1).
    pub fragmentation: f64,
    /// P90 CU price.
    pub p90_price: f64,
    /// CU variance.
    pub cu_variance: f64,
    /// Lock count variance.
    pub lock_variance: f64,
}

/// The real-time block auction engine.
///
/// This engine manages the complete auction lifecycle:
/// 1. **Tick processing**: Update prices, entropy, and CU allocation
/// 2. **Transaction scoring**: Compute auction surplus for candidates
/// 3. **Batch construction**: Greedily build batches with positive marginal gain
/// 4. **Lock management**: Centralized lock tracking with schedule-time acquisition
/// 5. **Bundle handling**: All-or-nothing bundle scheduling with pre-scan validation
pub struct AuctionEngine {
    /// Configuration.
    config: AuctionEngineConfig,
    /// Resource prices for all dimensions.
    prices: ResourcePrices,
    /// Opportunity entropy tracker.
    entropy: OpportunityEntropy,
    /// CU allocation (committed vs tentative).
    cu_allocation: CUAllocation,
    /// Centralized lock manager.
    lock_manager: LockManager,
    /// Batch scorer for marginal gain computation.
    scorer: BatchScorer,
    /// Current tick stats for metrics.
    tick_stats: TickStats,
    /// Last tick time for time price calculation.
    last_tick_time: Option<Instant>,
}

impl AuctionEngine {
    /// Create a new auction engine with the given configuration.
    pub fn new(config: AuctionEngineConfig, max_block_cu: u64) -> Self {
        Self {
            config: config.clone(),
            prices: ResourcePrices::new(&config),
            entropy: OpportunityEntropy::default(),
            cu_allocation: CUAllocation::new(
                max_block_cu,
                config.promote_threshold,
                config.release_threshold,
            ),
            lock_manager: LockManager::with_decay_rate(config.lock_decay_rate),
            scorer: BatchScorer::default(),
            tick_stats: TickStats::default(),
            last_tick_time: None,
        }
    }

    /// Process a new tick: update prices, entropy, and CU allocation.
    pub fn process_tick(&mut self, stats: &TickStats, remaining_cu: u64, ms_remaining: f64) {
        let now = Instant::now();
        let time_elapsed = match self.last_tick_time {
            Some(last) => now.duration_since(last).as_millis() as f64,
            None => 1.0,
        };
        self.last_tick_time = Some(now);

        self.prices.update(
            stats.cu_demand,
            remaining_cu as f64,
            stats.lock_demand,
            100.0,
            time_elapsed,
            ms_remaining,
            stats.fragmentation,
            remaining_cu as f64,
        );

        self.entropy
            .update(stats.p90_price, stats.lock_demand as u64, stats.cu_demand);

        self.lock_manager.decay(time_elapsed);
        self.cu_allocation.adjust(self.prices.cu.get());

        self.tick_stats = stats.clone();
    }

    /// Score a transaction candidate for batch inclusion.
    pub fn score_transaction(&self, tx: &TransactionCandidate) -> f64 {
        self.scorer.auction_surplus(tx, &self.prices)
    }

    /// Check if a transaction should be accepted into a batch.
    pub fn should_accept(&self, tx: &TransactionCandidate) -> bool {
        self.scorer.should_accept(tx, &self.prices)
    }

    /// Find the best batch for a transaction (greedy approach).
    pub fn find_best_batch(
        &self,
        tx: &TransactionCandidate,
        batches: &[TransactionBatch],
        max_cu: u64,
    ) -> Option<usize> {
        self.scorer
            .find_best_batch(tx, batches, &self.prices, max_cu)
    }

    /// Build batches from a list of transaction candidates.
    ///
    /// Uses a greedy approach: iterate candidates by priority, find the best
    /// batch for each, and add if surplus is positive.
    pub fn build_batches(
        &self,
        candidates: &[TransactionCandidate],
        max_cu: u64,
    ) -> Vec<TransactionBatch> {
        let mut batches: Vec<TransactionBatch> = Vec::new();

        for tx in candidates {
            if let Some(batch_idx) = self.find_best_batch(tx, &batches, max_cu) {
                batches[batch_idx].add(tx);
            } else {
                let mut new_batch = TransactionBatch::new();
                if new_batch.can_add(&tx.all_locks, tx.cu_limit, max_cu) {
                    let surplus = self.score_transaction(tx);
                    if surplus > self.scorer.config.acceptance_threshold {
                        new_batch.add(tx);
                        batches.push(new_batch);
                    }
                }
            }
        }

        batches
    }

    /// Build a bundle batch: validate all txs together, add all or none.
    ///
    /// Returns Some(batch) if the entire bundle can be added, None otherwise.
    pub fn build_bundle_batch(
        &self,
        bundle_txs: &[&TransactionCandidate],
        existing_batches: &mut Vec<TransactionBatch>,
        max_cu: u64,
    ) -> Option<TransactionBatch> {
        // Pre-scan: check if all bundle txs can fit together
        let mut candidate_batch = TransactionBatch::new();

        for tx in bundle_txs {
            if !candidate_batch.can_add(&tx.all_locks, tx.cu_limit, max_cu) {
                return None; // Can't fit all txs
            }
            candidate_batch.add(tx);
        }

        // Check if this bundle batch fits into any existing batch or creates a new one
        if let Some(batch_idx) =
            self.find_best_batch(bundle_txs.first().unwrap(), existing_batches, max_cu)
        {
            for tx in bundle_txs {
                existing_batches[batch_idx].add(tx);
            }
            Some(existing_batches[batch_idx].clone())
        } else {
            let surplus = self.score_transaction(bundle_txs.first()?);
            if surplus > self.scorer.config.acceptance_threshold {
                existing_batches.push(candidate_batch.clone());
                Some(candidate_batch)
            } else {
                None
            }
        }
    }

    /// Acquire locks for a transaction (schedule-time locking).
    /// Returns true if locks were acquired, false if conflicts exist.
    pub fn acquire_locks(&mut self, tx_key: TransactionKey, tx_locks: &[(Pubkey, bool)]) -> bool {
        if self.lock_manager.try_acquire(tx_locks) {
            self.lock_manager.acquire(tx_key, tx_locks);
            true
        } else {
            false
        }
    }

    /// Release locks for a transaction (on execution response).
    pub fn release_locks(&mut self, tx_key: TransactionKey, tx_locks: &[(Pubkey, bool)]) {
        for (addr, _) in tx_locks {
            if let Some(lockers) = self.lock_manager.account_locks.get_mut(addr) {
                lockers.release(tx_key);
                if lockers.is_empty() {
                    self.lock_manager.account_locks.remove(addr);
                }
            }
        }
    }

    /// Queue a transaction (increment queue depth).
    pub fn queue_transaction(&mut self, tx_locks: &[(Pubkey, bool)]) {
        self.lock_manager.queue_transaction(tx_locks);
    }

    /// Dequeue a transaction (decrement queue depth).
    pub fn dequeue_transaction(&mut self, tx_locks: &[(Pubkey, bool)]) {
        self.lock_manager.dequeue_transaction(tx_locks);
    }

    /// Get the available CU for scheduling.
    pub fn available_cu(&self) -> u64 {
        self.cu_allocation.available_cu()
    }

    /// Get the current CU price.
    pub fn cu_price(&self) -> f64 {
        self.prices.cu.get()
    }

    /// Get the current lock price.
    pub fn lock_price(&self) -> f64 {
        self.prices.lock.get()
    }

    /// Get the current flexibility coefficient.
    pub fn flexibility_coeff(&self) -> f64 {
        self.entropy
            .flexibility_coeff(self.config.entropy_theta, self.config.entropy_k)
    }

    /// Get reference to lock manager for metrics.
    pub fn lock_manager(&self) -> &LockManager {
        &self.lock_manager
    }

    /// Get reference to prices for metrics.
    pub fn prices(&self) -> &ResourcePrices {
        &self.prices
    }

    /// Get reference to CU allocation for metrics.
    pub fn cu_allocation(&self) -> &CUAllocation {
        &self.cu_allocation
    }

    /// Get tick stats for metrics.
    pub fn tick_stats(&self) -> &TickStats {
        &self.tick_stats
    }

    /// Reset all state (call at slot boundary).
    pub fn reset(&mut self) {
        self.lock_manager.reset();
        self.prices.reset();
        self.entropy.reset();
        self.cu_allocation.reset();
        self.last_tick_time = None;
    }
}

// ---------------------------------------------------------------------------
// Resource pricing
// ---------------------------------------------------------------------------

/// Minimum and maximum price bounds.
pub const MIN_PRICE: f64 = 0.001;
pub const MAX_PRICE: f64 = 10.0;

/// Sliding window for demand smoothing.
#[derive(Debug, Clone)]
struct SlidingDemand {
    samples: [f64; 64],
    head: usize,
    count: usize,
}

impl Default for SlidingDemand {
    fn default() -> Self {
        Self {
            samples: [0.0; 64],
            head: 0,
            count: 0,
        }
    }
}

impl SlidingDemand {
    fn add(&mut self, value: f64) {
        let idx = self.head % self.samples.len();
        self.samples[idx] = value;
        if self.count < self.samples.len() {
            self.count += 1;
        }
        self.head += 1;
    }

    #[allow(dead_code)]
    fn average(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let sum: f64 = self.samples.iter().sum();
        sum / self.count as f64
    }
}

/// Exponentially weighted moving average.
#[derive(Debug, Clone)]
pub struct EWMA {
    value: f64,
    alpha: f64,
}

impl EWMA {
    fn new(alpha: f64) -> Self {
        Self { value: 0.0, alpha }
    }

    fn update(&mut self, observation: f64) {
        self.value = self.alpha * observation + (1.0 - self.alpha) * self.value;
    }

    #[inline]
    pub fn get(&self) -> f64 {
        self.value
    }
}

/// Resource prices for CU, locks, time, and space.
#[derive(Debug, Clone)]
pub struct ResourcePrices {
    /// CU price tracker.
    pub cu: EWMA,
    /// Lock price tracker.
    pub lock: EWMA,
    /// Time price tracker.
    pub time: EWMA,
    /// Space price tracker.
    pub space: EWMA,
    /// Demand history for smoothing.
    cu_demand: SlidingDemand,
    lock_demand: SlidingDemand,
    // Base prices (scaling multipliers to convert to lamports)
    pub base_cu_price: f64,
    pub base_write_lock_price: f64,
    pub base_read_lock_price: f64,
    pub base_time_price: f64,
    pub base_space_price: f64,
}

impl ResourcePrices {
    /// Create with config.
    pub fn new(config: &AuctionEngineConfig) -> Self {
        Self {
            cu: EWMA::new(config.cu_alpha),
            lock: EWMA::new(config.lock_alpha),
            time: EWMA::new(config.time_alpha),
            space: EWMA::new(config.space_alpha),
            cu_demand: SlidingDemand::default(),
            lock_demand: SlidingDemand::default(),
            base_cu_price: config.base_cu_price,
            base_write_lock_price: config.base_write_lock_price,
            base_read_lock_price: config.base_read_lock_price,
            base_time_price: config.base_time_price,
            base_space_price: config.base_space_price,
        }
    }

    /// Update prices based on current tick statistics.
    pub fn update(
        &mut self,
        cu_demand: f64,
        cu_capacity: f64,
        lock_demand: f64,
        lock_capacity: f64,
        time_elapsed: f64,
        ms_remaining: f64,
        fragmentation: f64,
        _space_available: f64,
    ) {
        // CU price based on utilization (clamped)
        let cu_utilization = if cu_capacity > 0.0 {
            (cu_demand / cu_capacity).clamp(MIN_PRICE, MAX_PRICE)
        } else {
            MAX_PRICE
        };
        self.cu.update(cu_utilization);

        // Lock price based on contention (clamped)
        let lock_utilization = if lock_capacity > 0.0 {
            (lock_demand / lock_capacity).clamp(MIN_PRICE, MAX_PRICE)
        } else {
            MAX_PRICE
        };
        self.lock.update(lock_utilization);

        // Time price based on slot progress (clamped)
        let time_price = if ms_remaining > 0.0 {
            (time_elapsed / ms_remaining).clamp(MIN_PRICE, MAX_PRICE)
        } else {
            MAX_PRICE
        };
        self.time.update(time_price);

        // Space price based on fragmentation (clamped)
        self.space.update(fragmentation.clamp(MIN_PRICE, MAX_PRICE));

        // Record demand for smoothing
        self.cu_demand.add(cu_demand);
        self.lock_demand.add(lock_demand);
    }

    /// Compute total resource cost for a transaction.
    pub fn total_cost(&self, cu: u64, num_write_locks: usize, num_read_locks: usize) -> f64 {
        let effective_cu = (cu as f64).max(1.0);
        let cu_cost = effective_cu * self.cu.get() * self.base_cu_price;
        let write_lock_cost = num_write_locks as f64 * self.lock.get() * self.base_write_lock_price;
        let read_lock_cost = num_read_locks as f64 * self.lock.get() * self.base_read_lock_price;
        let time_cost = self.time.get() * self.base_time_price;
        let space_cost = self.space.get() * self.base_space_price;
        cu_cost + write_lock_cost + read_lock_cost + time_cost + space_cost
    }

    /// Reset all prices (call at slot boundary).
    pub fn reset(&mut self) {
        // Do not reset general CU/lock prices to preserve carryover network pricing.
        // Reset only slot-specific time and space prices.
        self.time.value = MIN_PRICE;
        self.space.value = MIN_PRICE;
        self.cu_demand = SlidingDemand::default();
        self.lock_demand = SlidingDemand::default();
    }
}

// ---------------------------------------------------------------------------
// Opportunity entropy
// ---------------------------------------------------------------------------

/// Tracks opportunity entropy for diversity in batch construction.
#[derive(Debug, Clone)]
pub struct OpportunityEntropy {
    /// Current entropy value.
    entropy: f64,
    /// Price flexibility coefficient.
    #[allow(dead_code)]
    theta: f64,
    /// Base flexibility.
    #[allow(dead_code)]
    k: f64,
}

impl Default for OpportunityEntropy {
    fn default() -> Self {
        Self {
            entropy: 0.0,
            theta: 0.5,
            k: 2.0,
        }
    }
}

impl OpportunityEntropy {
    /// Update entropy based on current market conditions.
    pub fn update(&mut self, p90_price: f64, lock_count: u64, cu_demand: f64) {
        // Entropy is higher when there is more activity but less lock contention.
        let activity_ratio = if lock_count > 0 {
            cu_demand / (lock_count as f64 * 1000.0)
        } else {
            cu_demand / 1000.0
        };
        let current_entropy = (activity_ratio.max(1.0).ln() * (1.0 + p90_price)).min(10.0);
        self.entropy = 0.9 * self.entropy + 0.1 * current_entropy;
    }

    /// Compute flexibility coefficient based on entropy.
    pub fn flexibility_coeff(&self, theta: f64, k: f64) -> f64 {
        k * (1.0 - 1.0 / (1.0 + theta * self.entropy.abs()))
    }

    /// Reset entropy (call at slot boundary).
    pub fn reset(&mut self) {
        self.entropy = 0.0;
    }
}

// ---------------------------------------------------------------------------
// CU allocation
// ---------------------------------------------------------------------------

/// Manages CU allocation between committed and tentative allocations.
#[derive(Debug, Clone)]
pub struct CUAllocation {
    /// Total CU available per block.
    max_cu: u64,
    /// Committed CU (already allocated to scheduled transactions).
    committed: u64,
    /// Tentative CU (reserved but not yet committed).
    tentative: u64,
    /// Threshold for promoting tentative to committed.
    #[allow(dead_code)]
    promote_threshold: f64,
    /// Threshold for releasing tentative allocations.
    #[allow(dead_code)]
    release_threshold: f64,
}

impl CUAllocation {
    /// Create a new CU allocator.
    pub fn new(max_cu: u64, promote_threshold: f64, release_threshold: f64) -> Self {
        Self {
            max_cu,
            committed: 0,
            tentative: 0,
            promote_threshold,
            release_threshold,
        }
    }

    /// Get available CU for new allocations.
    pub fn available_cu(&self) -> u64 {
        if self.committed + self.tentative > self.max_cu {
            0
        } else {
            self.max_cu - self.committed - self.tentative
        }
    }

    /// Reserve CU tentatively.
    pub fn reserve(&mut self, cu: u64) -> bool {
        if self.available_cu() >= cu {
            self.tentative += cu;
            true
        } else {
            false
        }
    }

    /// Commit tentative allocation.
    pub fn commit(&mut self) {
        self.committed += self.tentative;
        self.tentative = 0;
    }

    /// Release tentative allocation.
    pub fn release(&mut self) {
        self.tentative = 0;
    }

    /// Adjust allocation based on current price.
    pub fn adjust(&mut self, _price: f64) {
        // Could implement dynamic adjustment based on price signals
    }

    /// Reset allocation (call at slot boundary).
    pub fn reset(&mut self) {
        self.committed = 0;
        self.tentative = 0;
    }
}

// ---------------------------------------------------------------------------
// Transaction candidate & batch
// ---------------------------------------------------------------------------

/// A candidate transaction for batch inclusion.
#[derive(Debug, Clone)]
pub struct TransactionCandidate {
    /// Compute unit limit.
    pub cu_limit: u64,
    /// All write locks held by this transaction.
    pub all_locks: Vec<(Pubkey, bool)>,
    /// Priority fee.
    pub priority_fee: u64,
}

/// A batch of transactions scheduled together.
#[derive(Debug, Clone, Default)]
pub struct TransactionBatch {
    /// CU used by this batch.
    cu_used: u64,
    /// Locks held by this batch.
    locks: HashMap<Pubkey, bool>,
    /// Transactions in this batch.
    transactions: Vec<TransactionCandidate>,
}

impl TransactionBatch {
    /// Create a new empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a transaction can be added to this batch.
    pub fn can_add(&mut self, tx_locks: &[(Pubkey, bool)], cu: u64, max_cu: u64) -> bool {
        // Check CU budget
        if self.cu_used + cu > max_cu {
            return false;
        }

        // Check lock conflicts
        for (addr, writable) in tx_locks {
            if let Some(existing) = self.locks.get(addr) {
                if *writable && *existing {
                    return false; // Write-write conflict
                }
                if *writable && !existing {
                    return false; // Write-read conflict
                }
            }
        }

        true
    }

    /// Add a transaction to this batch.
    pub fn add(&mut self, tx: &TransactionCandidate) {
        self.cu_used += tx.cu_limit;
        for (addr, writable) in &tx.all_locks {
            self.locks.insert(*addr, *writable);
        }
        self.transactions.push(tx.clone());
    }

    /// Get the number of transactions in this batch.
    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    /// Whether this batch is empty.
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Batch scorer
// ---------------------------------------------------------------------------

/// Configuration for batch scoring.
#[derive(Debug, Clone)]
pub struct BatchScoringConfig {
    /// Minimum surplus to accept a transaction.
    pub acceptance_threshold: f64,
}

impl Default for BatchScoringConfig {
    fn default() -> Self {
        Self {
            acceptance_threshold: 0.0,
        }
    }
}

/// Scores transactions for batch inclusion based on auction surplus.
#[derive(Debug, Clone)]
pub struct BatchScorer {
    /// Scoring configuration.
    pub config: BatchScoringConfig,
}

impl Default for BatchScorer {
    fn default() -> Self {
        Self {
            config: BatchScoringConfig::default(),
        }
    }
}

impl BatchScorer {
    /// Compute auction surplus for a transaction.
    pub fn auction_surplus(&self, tx: &TransactionCandidate, prices: &ResourcePrices) -> f64 {
        let num_write_locks = tx.all_locks.iter().filter(|(_, w)| *w).count();
        let num_read_locks = tx.all_locks.len() - num_write_locks;
        let cost = prices.total_cost(tx.cu_limit, num_write_locks, num_read_locks);
        tx.priority_fee as f64 - cost
    }

    /// Check if a transaction should be accepted into a batch.
    pub fn should_accept(&self, tx: &TransactionCandidate, prices: &ResourcePrices) -> bool {
        let surplus = self.auction_surplus(tx, prices);
        surplus > self.config.acceptance_threshold
    }

    /// Find the best batch for a transaction.
    pub fn find_best_batch(
        &self,
        tx: &TransactionCandidate,
        batches: &[TransactionBatch],
        prices: &ResourcePrices,
        max_cu: u64,
    ) -> Option<usize> {
        let mut best_idx: Option<usize> = None;
        let mut best_surplus = f64::NEG_INFINITY;

        for (idx, batch) in batches.iter().enumerate() {
            let mut batch_mut = TransactionBatch::new();
            batch_mut.cu_used = batch.cu_used;
            batch_mut.locks = batch.locks.clone();
            if batch_mut.can_add(&tx.all_locks, tx.cu_limit, max_cu) {
                let surplus = self.auction_surplus(tx, prices);
                if surplus > best_surplus {
                    best_surplus = surplus;
                    best_idx = Some(idx);
                }
            }
        }

        best_idx
    }
}
