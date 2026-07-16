use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use agave_scheduler_bindings::pack_message_flags::{check_flags, execution_flags};
use agave_scheduler_bindings::worker_message_types::{
    CheckResponse, ExecutionResponse, fee_payer_balance_flags, not_included_reasons,
    parsing_and_sanitization_flags, resolve_flags, status_check_flags,
};
use agave_scheduler_bindings::{
    LEADER_READY, MAX_TRANSACTIONS_PER_MESSAGE, SharableTransactionRegion, pack_message_flags,
};
use agave_scheduling_utils::bridge::{
    KeyedTransactionMeta, RuntimeState, ScheduleBatch, SchedulerBindingsBridge, TransactionKey,
    TransactionState, TxDecision, WorkerAction, WorkerResponse,
};
use agave_scheduling_utils::pubkeys_ptr::PubkeysPtr;
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use agave_transaction_view::transaction_view::SanitizedTransactionView;
use crossbeam_channel::TryRecvError;
use indexmap::IndexSet;
use metrics::{Counter, Gauge, counter, gauge};
use min_max_heap::MinMaxHeap;
use schedulers::PriorityId;
pub mod auction_engine;
use auction_engine::{AuctionEngine, AuctionEngineConfig, TickStats};
use schedulers::events::{
    CheckFailure, Event, EventEmitter, EvictReason, SlotStatsEvent, TransactionAction,
    TransactionEvent, TransactionSource,
};
use schedulers::jito::jito_thread::{BuilderConfig, JitoArgs, JitoThread, JitoUpdate, TipConfig};
use schedulers::jito::tip_program::{
    ChangeTipReceiverArgs, TIP_ACCOUNTS, TIP_PAYMENT_PROGRAM, TipDistributionArgs,
    change_tip_receiver, init_tip_distribution,
};
use schedulers::tx_costs::{TxCosts, derive_costs};
use solana_clock::{DEFAULT_SLOTS_PER_EPOCH, Slot};
use solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS_SIMD_0256;
use solana_hash::Hash;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_sdk_ids::system_program;
use static_assertions::const_assert;

/// Configuration for the auction-batch composite scoring strategy.
#[derive(Debug, Clone, Copy)]
pub struct AuctionBatchConfig {
    /// Minimum score threshold — txs below this are dropped. Defaults to 0.
    pub min_score: u64,
}

impl Default for AuctionBatchConfig {
    fn default() -> Self {
        Self { min_score: 0 }
    }
}

const PRIORITY_MULTIPLIER: u64 = 1_000_000;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
const BUNDLE_MARKER: u64 = u64::MAX;

const TX_REGION_SIZE: usize = std::mem::size_of::<SharableTransactionRegion>();
const TX_BATCH_PER_MESSAGE: usize = TX_REGION_SIZE + std::mem::size_of::<PriorityId>();
const TX_BATCH_SIZE: usize = TX_BATCH_PER_MESSAGE * MAX_TRANSACTIONS_PER_MESSAGE;
const_assert!(TX_BATCH_SIZE < 4096);

const CHECK_WORKER: usize = 0;
const EXECUTE_WORKER_START: usize = 1;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub max_check_batches: usize,
    pub block_fill_cutoff: u8,
    pub progress_timeout: Duration,
    pub bundle_expiry: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_check_batches: 4,
            block_fill_cutoff: 20,
            progress_timeout: Duration::from_secs(5),
            bundle_expiry: Duration::from_millis(200),
        }
    }
}

#[derive(Debug)]
pub struct AuctionBatchSchedulerArgs {
    pub tip: TipDistributionArgs,
    pub jito: JitoArgs,
    pub keypair: Arc<Keypair>,
    pub filter_keys: HashSet<Pubkey>,
    pub unchecked_capacity: usize,
    pub checked_capacity: usize,
    pub bundle_capacity: usize,
    pub runtime: RuntimeConfig,
    pub scoring: AuctionBatchConfig,
}

pub struct AuctionBatchScheduler {
    shutdown: CancellationToken,
    jito_rx: crossbeam_channel::Receiver<JitoUpdate>,
    tip_distribution_config: TipDistributionArgs,
    keypair: Arc<Keypair>,
    filter_keys: HashSet<Pubkey>,

    unchecked_capacity: usize,
    checked_capacity: usize,
    bundle_capacity: usize,
    runtime: RuntimeConfig,

    builder_config: BuilderConfig,
    tip_config: Option<TipConfig>,
    recent_blockhash: Hash,
    bundles: BTreeSet<BundleId>,
    pub unchecked_tx: MinMaxHeap<PriorityId>,
    pub checked_tx: BTreeSet<PriorityId>,
    pub executing_tx: HashSet<TransactionKey>,
    deferred_tx: IndexSet<PriorityId>,
    next_recheck: Option<PriorityId>,
    in_flight_cus: u64,
    schedule_batch: Vec<KeyedTransactionMeta<PriorityId>>,
    last_progress_time: Instant,

    events: Option<EventEmitter>,
    slot: Slot,
    slot_stats: SlotStatsEvent,
    metrics: BatchMetrics,
    scoring: AuctionBatchConfig,

    auction_engine: AuctionEngine,
}

impl AuctionBatchScheduler {
    #[must_use]
    pub fn new(
        shutdown: CancellationToken,
        events: Option<EventEmitter>,
        args: AuctionBatchSchedulerArgs,
    ) -> (Self, JoinHandle<()>) {
        let (jito_tx, jito_rx) = crossbeam_channel::bounded(1024);
        let jito_thread = JitoThread::spawn(
            shutdown.clone(),
            jito_tx,
            args.jito.clone(),
            args.keypair.clone(),
        );

        (
            Self::new_with_jito(shutdown, events, args, jito_rx),
            jito_thread,
        )
    }

    #[must_use]
    pub fn new_with_jito(
        shutdown: CancellationToken,
        events: Option<EventEmitter>,
        AuctionBatchSchedulerArgs {
            tip,
            jito: _,
            keypair,
            mut filter_keys,
            unchecked_capacity,
            checked_capacity,
            bundle_capacity,
            runtime,
            scoring,
        }: AuctionBatchSchedulerArgs,
        jito_rx: crossbeam_channel::Receiver<JitoUpdate>,
    ) -> Self {
        let JitoUpdate::BuilderConfig(builder_config) =
            jito_rx.recv_timeout(Duration::from_secs(5)).unwrap()
        else {
            panic!(
                "the grpc request for builder config should be the first message sent by the jito thread"
            );
        };

        // Ensure tip program is filtered.
        filter_keys.insert(TIP_PAYMENT_PROGRAM);

        Self {
            shutdown,
            jito_rx,
            tip_distribution_config: tip,
            keypair,
            filter_keys,

            unchecked_capacity,
            checked_capacity,
            bundle_capacity,
            runtime,

            builder_config,
            tip_config: None,
            recent_blockhash: Hash::default(),
            bundles: BTreeSet::new(),
            unchecked_tx: MinMaxHeap::with_capacity(unchecked_capacity),
            checked_tx: BTreeSet::new(),
            executing_tx: HashSet::with_capacity(checked_capacity),
            deferred_tx: IndexSet::with_capacity(checked_capacity),
            next_recheck: None,
            in_flight_cus: 0,
            schedule_batch: Vec::new(),
            last_progress_time: Instant::now(),

            events,
            slot: 0,
            slot_stats: SlotStatsEvent::default(),
            metrics: BatchMetrics::new(),
            scoring,

            auction_engine: AuctionEngine::new(
                AuctionEngineConfig::default(),
                MAX_BLOCK_UNITS_SIMD_0256,
            ),
        }
    }

    /// Update runtime-tunable config values. Call this from the scheduler poll loop.
    pub fn set_runtime_config(
        &mut self,
        unchecked_capacity: usize,
        checked_capacity: usize,
        bundle_capacity: usize,
        block_fill_cutoff: u8,
        max_check_batches: usize,
        bundle_expiry: Duration,
        progress_timeout: Duration,
    ) {
        self.unchecked_capacity = unchecked_capacity;
        self.checked_capacity = checked_capacity;
        self.bundle_capacity = bundle_capacity;
        self.runtime.block_fill_cutoff = block_fill_cutoff;
        self.runtime.max_check_batches = max_check_batches;
        self.runtime.bundle_expiry = bundle_expiry;
        self.runtime.progress_timeout = progress_timeout;
    }

    /// Update the base prices for the internal auction engine dynamically.
    pub fn update_base_prices(
        &mut self,
        base_cu: f64,
        base_write_lock: f64,
        base_read_lock: f64,
        base_time: f64,
        base_space: f64,
    ) {
        self.auction_engine.update_base_prices(
            base_cu,
            base_write_lock,
            base_read_lock,
            base_time,
            base_space,
        );
    }

    pub fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<PriorityId>) {
        // Drain the progress tracker & check for roll.
        self.check_slot_roll(bridge);

        // Drain responses from workers.
        self.drain_worker_responses(bridge);

        // Process auction engine tick with current market state.
        self.process_auction_tick(bridge);

        // Ingest a bounded amount of new transactions.
        let is_leader = bridge.progress().leader_state == LEADER_READY;
        match is_leader {
            true => self.drain_tpu(bridge, 128),
            false => self.drain_tpu(bridge, 1024),
        }

        // Drop expired bundles.
        self.drop_expired_bundles(bridge);

        // Drain pending jito messages.
        self.drain_jito(bridge);

        // Queue additional checks.
        self.schedule_checks(bridge);

        // Schedule if we're currently the leader.
        if is_leader {
            self.schedule_execute(bridge);

            // Start another recheck if we are not currently performing one.
            self.next_recheck = self
                .next_recheck
                .or_else(|| self.checked_tx.last().copied());
        }

        // Update metrics.
        self.metrics
            .current_slot
            .set(bridge.progress().current_slot as f64);
        self.metrics
            .next_leader_slot
            .set(bridge.progress().next_leader_slot as f64);
        self.metrics
            .tpu_unchecked_len
            .set(self.unchecked_tx.len() as f64);
        self.metrics
            .tpu_checked_len
            .set(self.checked_tx.len() as f64);
        self.metrics
            .executing_len
            .set(self.executing_tx.len() as f64);
        self.metrics
            .tpu_deferred_len
            .set(self.deferred_tx.len() as f64);
        self.metrics.bundles_len.set(self.bundles.len() as f64);
        self.metrics
            .locks_len
            .set(self.auction_engine.lock_manager().tracked_accounts() as f64);
        self.metrics.in_flight_cus.set(self.in_flight_cus as f64);
    }

    fn check_slot_roll(&mut self, bridge: &mut SchedulerBindingsBridge<PriorityId>) {
        // Drain progress and check for disconnect.
        match bridge.drain_progress() {
            Some(_) => self.last_progress_time = Instant::now(),
            None => {
                let elapsed = self.last_progress_time.elapsed();
                if elapsed >= self.runtime.progress_timeout {
                    tracing::warn!(
                        "Agave progress update slot {} starved for {:?}; daemon is still running",
                        self.slot,
                        elapsed
                    );
                    self.last_progress_time = Instant::now();
                }
            }
        }

        // Check for slot roll.
        let was_leader_ready = self.slot_stats.was_leader_ready;
        let progress = *bridge.progress();

        // Slot has changed.
        if progress.current_slot != self.slot {
            if let Some(events) = &self.events {
                // Emit SlotStats for the slot that just ended.
                if self.slot != 0 {
                    let stats = core::mem::take(&mut self.slot_stats);
                    events.emit(Event::SlotStats(stats));
                }

                // Update context for new slot events.
                events.ctx().set(progress.current_slot);

                // Emit SlotStart for the new slot.
                events.emit(Event::SlotStart);
            }

            // Update our local state.
            self.slot = progress.current_slot;
            self.slot_stats.was_leader_ready = false;

            // Drain deferred transactions back to checked.
            let deferred: Vec<PriorityId> = self.deferred_tx.drain(..).collect();
            for meta in deferred {
                self.insert_checked(bridge, meta);
            }

            // Start another recheck if we are not currently performing one.
            self.next_recheck = self
                .next_recheck
                .or_else(|| self.checked_tx.last().copied());
        }

        // If we have just become the leader, emit an event & configure tip accounts.
        if progress.leader_state == LEADER_READY && !was_leader_ready {
            if let Some(events) = &self.events {
                events.emit(Event::LeaderReady);
            }

            self.slot_stats.was_leader_ready = true;
            self.become_tip_receiver(bridge);
        }
    }

    fn become_tip_receiver(&mut self, bridge: &mut SchedulerBindingsBridge<PriorityId>) {
        info!("Becoming tip receiver");

        let (tip_distribution_key, init_tip_distribution) = init_tip_distribution(
            &self.keypair,
            self.tip_distribution_config,
            self.slot / DEFAULT_SLOTS_PER_EPOCH,
            self.recent_blockhash,
        );
        let init_tip_distribution = bridge.insert_transaction(&init_tip_distribution).unwrap();

        let tip_config = self.tip_config.as_ref().unwrap();
        let change_tip_receiver = change_tip_receiver(
            &self.keypair,
            ChangeTipReceiverArgs {
                old_tip_receiver: tip_config.tip_receiver,
                new_tip_receiver: tip_distribution_key,
                old_block_builder: tip_config.block_builder,
                new_block_builder: self.builder_config.key,
                block_builder_commission: self.builder_config.commission,
            },
            self.recent_blockhash,
        );
        let change_tip_receiver = bridge.insert_transaction(&change_tip_receiver).unwrap();

        // Check if our batch can be locked.
        if !Self::can_lock(&self.auction_engine, bridge, init_tip_distribution) {
            warn!("Failed to grab locks for change tip receiver");
            bridge.drop_transaction(init_tip_distribution);
            bridge.drop_transaction(change_tip_receiver);

            return;
        }

        // Lock our batch (Self::lock allows us to create overlapping write locks).
        Self::lock(&mut self.auction_engine, bridge, init_tip_distribution);
        Self::lock(&mut self.auction_engine, bridge, change_tip_receiver);

        // Set these transactions as executing.
        assert!(self.executing_tx.insert(init_tip_distribution));
        assert!(self.executing_tx.insert(change_tip_receiver));

        // TODO: Schedule as a single batch once we have SIMD83 live.
        bridge
            .schedule(ScheduleBatch {
                worker: EXECUTE_WORKER_START,
                transactions: &[KeyedTransactionMeta {
                    key: init_tip_distribution,
                    meta: PriorityId {
                        priority: BUNDLE_MARKER,
                        cost: 0,
                        key: init_tip_distribution,
                    },
                }],
                max_working_slot: self.slot + 4,
                flags: pack_message_flags::EXECUTE | execution_flags::DROP_ON_FAILURE,
            })
            .unwrap();
        bridge
            .schedule(ScheduleBatch {
                worker: EXECUTE_WORKER_START,
                transactions: &[KeyedTransactionMeta {
                    key: change_tip_receiver,
                    meta: PriorityId {
                        priority: BUNDLE_MARKER,
                        cost: 0,
                        key: change_tip_receiver,
                    },
                }],
                max_working_slot: self.slot + 4,
                flags: pack_message_flags::EXECUTE | execution_flags::DROP_ON_FAILURE,
            })
            .unwrap();
    }

    fn drain_worker_responses(&mut self, bridge: &mut SchedulerBindingsBridge<PriorityId>) {
        for worker in 0..bridge.worker_count() {
            bridge.drain_worker(
                worker,
                |bridge, WorkerResponse { meta, response, .. }| {
                    match response {
                        WorkerAction::Unprocessed => {
                            // Release locks if this was an execute request.
                            if self.executing_tx.remove(&meta.key) {
                                Self::unlock(&mut self.auction_engine, bridge, meta.key);
                                self.in_flight_cus -= meta.cost;

                                // unprocessed.
                                if meta.priority == BUNDLE_MARKER {
                                    return TxDecision::Drop;
                                }

                                self.emit_tx_event(
                                    bridge,
                                    meta.key,
                                    meta.priority,
                                    TransactionAction::ExecuteUnprocessed,
                                );
                                self.metrics.execute_unprocessed.increment(1);
                                self.slot_stats.execute_unprocessed += 1;
                                self.insert_checked(bridge, meta);
                            }

                            TxDecision::Keep
                        }
                        WorkerAction::Check(rep, resolved_keys) => {
                            self.on_check(bridge, meta, rep, resolved_keys)
                        }
                        WorkerAction::Execute(rep) => self.on_execute(bridge, meta, rep),
                    }
                },
                usize::MAX,
            );
        }
    }

    fn drain_tpu(&mut self, bridge: &mut SchedulerBindingsBridge<PriorityId>, max_count: usize) {
        let additional = std::cmp::min(bridge.tpu_len(), max_count);
        let shortfall =
            (self.unchecked_tx.len() + additional).saturating_sub(self.unchecked_capacity);

        // NB: Technically we are evicting more than we need to because not all of
        // `additional` will parse correctly & thus have a priority.
        for _ in 0..shortfall {
            if let Some(id) = self.unchecked_tx.pop_min() {
                self.emit_tx_event(
                    bridge,
                    id.key,
                    id.priority,
                    TransactionAction::Evict {
                        reason: EvictReason::UncheckedCapacity,
                    },
                );
                bridge.drop_transaction(id.key);
            } else {
                break;
            }
        }
        self.metrics.recv_tpu_evict.increment(shortfall as u64);
        self.slot_stats.ingest_tpu_evict += shortfall as u64;

        // TODO: Need to dedupe already seen transactions?
        bridge.drain_tpu(
            |bridge, key| {
                let remaining_cu = self.effective_remaining_cu(bridge);
                match self.calculate_priority(
                    bridge.runtime(),
                    remaining_cu,
                    bridge.transaction(key),
                ) {
                    Some((priority, cost)) => {
                        if self.should_filter_static(&bridge.transaction(key).data) {
                            self.metrics.recv_tpu_filtered.increment(1);
                            self.slot_stats.ingest_tpu_filtered += 1;

                            return TxDecision::Drop;
                        }

                        let meta = PriorityId {
                            priority,
                            cost,
                            key,
                        };

                        if bridge.transaction(key).data.num_address_table_lookups() == 0 {
                            let decision = self.insert_checked_with_capacity(bridge, meta);
                            self.emit_tx_event(
                                bridge,
                                key,
                                priority,
                                TransactionAction::Ingest {
                                    source: TransactionSource::Tpu,
                                    bundle: None,
                                },
                            );
                            self.metrics.recv_tpu_ok.increment(1);
                            self.slot_stats.ingest_tpu_ok += 1;
                            decision
                        } else {
                            self.unchecked_tx.push(meta);
                            self.emit_tx_event(
                                bridge,
                                key,
                                priority,
                                TransactionAction::Ingest {
                                    source: TransactionSource::Tpu,
                                    bundle: None,
                                },
                            );
                            self.metrics.recv_tpu_ok.increment(1);
                            self.slot_stats.ingest_tpu_ok += 1;
                            TxDecision::Keep
                        }
                    }
                    None => {
                        self.metrics.recv_tpu_err.increment(1);
                        self.slot_stats.ingest_tpu_err += 1;

                        TxDecision::Drop
                    }
                }
            },
            max_count,
        );
    }

    fn drain_jito(&mut self, bridge: &mut SchedulerBindingsBridge<PriorityId>) {
        loop {
            match self.jito_rx.try_recv() {
                Ok(JitoUpdate::BuilderConfig { .. }) => {}
                Ok(JitoUpdate::TipConfig(config)) => self.tip_config = Some(config),
                Ok(JitoUpdate::RecentBlockhash(hash)) => self.recent_blockhash = hash,
                Ok(JitoUpdate::Packet(packet)) => self.on_packet(bridge, &packet),
                Ok(JitoUpdate::Bundle(bundle)) => self.on_bundle(bridge, bundle),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => assert!(self.shutdown.is_cancelled()),
            }
        }
    }

    fn on_packet(&mut self, bridge: &mut SchedulerBindingsBridge<PriorityId>, packet: &[u8]) {
        let Ok(key) = bridge.insert_transaction(packet) else {
            return;
        };

        let remaining_cu = self.effective_remaining_cu(bridge);
        match self.calculate_priority(bridge.runtime(), remaining_cu, bridge.transaction(key)) {
            Some((priority, cost)) => {
                if self.should_filter_static(&bridge.transaction(key).data) {
                    self.metrics.recv_packet_filtered.increment(1);
                    self.slot_stats.ingest_custom_filtered += 1;
                    bridge.drop_transaction(key);

                    return;
                }

                let meta = PriorityId {
                    priority,
                    cost,
                    key,
                };

                if bridge.transaction(key).data.num_address_table_lookups() == 0 {
                    self.insert_checked_with_capacity(bridge, meta);
                    self.emit_tx_event(
                        bridge,
                        key,
                        priority,
                        TransactionAction::Ingest {
                            source: TransactionSource::Jito,
                            bundle: None,
                        },
                    );
                    self.metrics.recv_packet_ok.increment(1);
                    self.slot_stats.ingest_custom_ok += 1;
                } else {
                    // Evict lowest if we're at capacity.
                    if self.unchecked_tx.len() == self.unchecked_capacity {
                        let id = self.unchecked_tx.pop_min().unwrap();
                        self.emit_tx_event(
                            bridge,
                            id.key,
                            id.priority,
                            TransactionAction::Evict {
                                reason: EvictReason::UncheckedCapacity,
                            },
                        );
                        bridge.drop_transaction(id.key);
                        self.metrics.recv_packet_evict.increment(1);
                    }

                    // Store the new packet.
                    self.unchecked_tx.push(meta);
                    self.emit_tx_event(
                        bridge,
                        key,
                        priority,
                        TransactionAction::Ingest {
                            source: TransactionSource::Jito,
                            bundle: None,
                        },
                    );
                    self.metrics.recv_packet_ok.increment(1);
                    self.slot_stats.ingest_custom_ok += 1;
                }
            }
            None => {
                self.metrics.recv_packet_err.increment(1);
                self.slot_stats.ingest_custom_err += 1;

                bridge.drop_transaction(key);
            }
        }
    }

    fn on_bundle(
        &mut self,
        bridge: &mut SchedulerBindingsBridge<PriorityId>,
        bundle: Vec<Vec<u8>>,
    ) {
        let mut keys = Vec::with_capacity(bundle.len());
        let mut total_cost: u64 = 0;
        let mut total_score: u64 = 0;
        let feature_set = bridge.runtime().feature_set.clone();

        for packet in bundle {
            let Ok(key) = bridge.insert_transaction(&packet) else {
                // drop the entire bundle if any transaction fails to insert
                for key in keys {
                    bridge.drop_transaction(key);
                }
                self.metrics.recv_bundle_err.increment(1);

                return;
            };

            // Add to our bundle keys.
            keys.push(key);

            // Calculate composite score using the auction engine.
            let tx_state = bridge.transaction(key);
            let costs = match derive_costs(&tx_state.data, &feature_set) {
                Some(c) => c,
                None => {
                    // If we can't derive costs, drop the bundle
                    for key in keys {
                        bridge.drop_transaction(key);
                    }
                    self.metrics.recv_bundle_err.increment(1);
                    return;
                }
            };

            // Extract tip from this transaction to avoid unfair bundle advantage.
            let tip = Self::extract_tip(&tx_state.data);

            // Auction scoring only — no fallback
            let (score, cost) = match self.score_with_auction(
                &costs,
                tx_state.locks().map(|(k, v)| (*k, v)),
                tip,
            ) {
                Some(s) => s,
                None => {
                    // Auction rejected this tx — drop the bundle
                    for key in keys {
                        bridge.drop_transaction(key);
                    }
                    self.metrics.recv_bundle_err.increment(1);
                    return;
                }
            };

            // Apply minimum score filter.
            if score < self.scoring.min_score {
                // drop the entire bundle if any tx is below min score
                for key in keys {
                    bridge.drop_transaction(key);
                }
                self.metrics.recv_bundle_err.increment(1);

                return;
            }

            total_cost += cost;
            total_score += score;
        }

        // Filter bundles containing transactions that reference filtered accounts.
        if keys
            .iter()
            .any(|key| self.should_filter_static(&bridge.transaction(*key).data))
        {
            // NB: We don't check ALTs on Jito bundles as these are assumed to be filtered
            // upstream.
            self.metrics.recv_bundle_filtered.increment(1);
            for key in keys {
                bridge.drop_transaction(key);
            }

            return;
        }

        // Calculate bundle priority from composite score.
        let priority = total_score.min(BUNDLE_MARKER - 1);

        // Emit ingest events for bundle transactions.
        let bundle_sig = bridge.transaction(keys[0]).data.signatures()[0];
        let bundle_id = Arc::new(bundle_sig.to_string());
        for &key in &keys {
            self.emit_tx_event(
                bridge,
                key,
                priority,
                TransactionAction::Ingest {
                    source: TransactionSource::Jito,
                    bundle: Some(bundle_id.clone()),
                },
            );
        }

        // Evict lowest priority bundle if at capacity.
        if self.bundles.len() == self.bundle_capacity {
            let evicted = self.bundles.pop_first().unwrap();
            for key in &evicted.keys {
                let tx_locks: Vec<(Pubkey, bool)> = bridge
                    .transaction(*key)
                    .locks()
                    .map(|(k, v)| (*k, v))
                    .collect();
                self.auction_engine.dequeue_transaction(&tx_locks);
                bridge.drop_transaction(*key);
            }
            self.metrics.recv_bundle_evict.increment(1);
        }

        self.metrics.recv_bundle_ok.increment(1);

        // Enqueue locks for the new bundle
        for key in &keys {
            let tx_locks: Vec<(Pubkey, bool)> = bridge
                .transaction(*key)
                .locks()
                .map(|(k, v)| (*k, v))
                .collect();
            self.auction_engine.queue_transaction(&tx_locks);
        }

        // TODO: If Jito sends us a transaction (not a bundle) with overlapping
        // read/write keys we will panic as normally CHECK prevents this.
        self.bundles.insert(BundleId {
            priority,
            cost: total_cost,
            received_at: Instant::now(),
            keys,
        });
    }

    fn drop_expired_bundles(&mut self, bridge: &mut SchedulerBindingsBridge<PriorityId>) {
        let now = Instant::now();
        // Retain only non-expired bundles, dropping expired ones.
        let expired: Vec<_> = self
            .bundles
            .extract_if(.., |b| {
                now.duration_since(b.received_at) > self.runtime.bundle_expiry
            })
            .collect();

        for bundle in expired {
            self.bundles.remove(&bundle);
            self.metrics.recv_bundle_expired.increment(1);
            for key in bundle.keys {
                let tx_locks: Vec<(Pubkey, bool)> = bridge
                    .transaction(key)
                    .locks()
                    .map(|(k, v)| (*k, v))
                    .collect();
                self.auction_engine.dequeue_transaction(&tx_locks);
                bridge.drop_transaction(key);
            }
        }
    }

    fn schedule_checks(&mut self, bridge: &mut SchedulerBindingsBridge<PriorityId>) {
        // Loop until worker queue is filled or backlog is empty.
        let start_len = self.unchecked_tx.len();
        while bridge.worker(CHECK_WORKER).len() < self.runtime.max_check_batches
            && bridge.worker(CHECK_WORKER).rem() > 0
        {
            let mut pop_next = || {
                // Prioritize unchecked transactions.
                if let Some(id) = self.unchecked_tx.pop_max() {
                    return Some(KeyedTransactionMeta {
                        key: id.key,
                        meta: id,
                    });
                }

                // Re-check already checked transactions if we have remaining.
                while let Some(curr) = self.next_recheck.take() {
                    self.next_recheck = self
                        .checked_tx
                        .range((Bound::Unbounded, Bound::Excluded(curr)))
                        .next_back()
                        .copied();

                    // Skip if transaction was removed from checked_tx (e.g., scheduled for
                    // execution) or is currently executing.
                    if self.checked_tx.contains(&curr) && !self.executing_tx.contains(&curr.key) {
                        return Some(KeyedTransactionMeta {
                            key: curr.key,
                            meta: curr,
                        });
                    }
                }

                None
            };

            // Build the next batch.
            self.schedule_batch.clear();
            self.schedule_batch
                .extend(std::iter::from_fn(&mut pop_next).take(64));

            // If we built an empty batch we are done.
            if self.schedule_batch.is_empty() {
                break;
            }

            bridge
                .schedule(ScheduleBatch {
                    worker: CHECK_WORKER,
                    transactions: &self.schedule_batch,
                    max_working_slot: u64::MAX,
                    flags: pack_message_flags::CHECK
                        | check_flags::STATUS_CHECKS
                        | check_flags::LOAD_FEE_PAYER_BALANCE
                        | check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
                })
                .unwrap();
        }

        // Update metrics with our scheduled amount.
        let check_requested = (start_len - self.unchecked_tx.len()) as u64;
        self.metrics.check_requested.increment(check_requested);
        self.slot_stats.check_requested += check_requested;
    }

    fn schedule_execute(&mut self, bridge: &mut SchedulerBindingsBridge<PriorityId>) {
        debug_assert_eq!(bridge.progress().leader_state, LEADER_READY);
        let budget_percentage = std::cmp::min(
            bridge.progress().current_slot_progress + self.runtime.block_fill_cutoff,
            100,
        );
        // TODO: Would be ideal for the scheduler protocol to tell us the max block
        // units.
        let budget_limit = MAX_BLOCK_UNITS_SIMD_0256 * u64::from(budget_percentage) / 100;
        let cost_used = MAX_BLOCK_UNITS_SIMD_0256
            .saturating_sub(bridge.progress().remaining_cost_units)
            + self.in_flight_cus;
        let mut budget = budget_limit.saturating_sub(cost_used);
        for worker in EXECUTE_WORKER_START..bridge.worker_count() {
            // If we are packing too fast, slow down.
            if budget == 0 {
                break;
            }

            // If the worker already has a pending job, don't give it any more.
            if !bridge.worker(worker).is_empty() {
                continue;
            }

            // Find the best tx & bundle, if both are empty we're done.
            let tx = self.checked_tx.last().map(|tx| tx.priority);
            let bundle = self.bundles.last().map(|bundle| bundle.priority);

            // Pick & schedule the best.
            self.schedule_batch.clear();
            match (tx, bundle) {
                (Some(tx), Some(bundle)) => match tx.cmp(&bundle) {
                    Ordering::Greater | Ordering::Equal => {
                        if !self.try_schedule_transaction(&mut budget, bridge, worker) {
                            self.try_schedule_bundle(&mut budget, bridge, worker);
                        }
                    }
                    Ordering::Less => {
                        if !self.try_schedule_bundle(&mut budget, bridge, worker) {
                            self.try_schedule_transaction(&mut budget, bridge, worker);
                        }
                    }
                },
                (Some(_), None) => {
                    self.try_schedule_transaction(&mut budget, bridge, worker);
                }
                (None, Some(_)) => {
                    self.try_schedule_bundle(&mut budget, bridge, worker);
                }
                (None, None) => break,
            }

            // If we failed to schedule anything, don't send the batch.
            if self.schedule_batch.is_empty() {
                break;
            }

            // For each TX we need to:
            // - Add to executing_tx.
            // - Emit an event.
            for tx in &self.schedule_batch {
                assert!(self.executing_tx.insert(tx.key));
                self.emit_tx_event(
                    bridge,
                    tx.key,
                    tx.meta.priority,
                    TransactionAction::ExecuteReq,
                );
            }

            // Update metrics.
            let execute_requested = self.schedule_batch.len() as u64;
            // self.metrics.execute_requested.increment(execute_requested);
            self.slot_stats.execute_requested += execute_requested;
        }
    }

    fn on_check(
        &mut self,
        bridge: &mut SchedulerBindingsBridge<PriorityId>,
        meta: PriorityId,
        rep: CheckResponse,
        resolved_keys: Option<&PubkeysPtr>,
    ) -> TxDecision {
        // If transaction is currently executing (or deferred), ignore the recheck
        // result.
        if self.executing_tx.contains(&meta.key) || self.deferred_tx.contains(&meta) {
            return TxDecision::Keep;
        }

        let parsing_failed =
            rep.parsing_and_sanitization_flags & parsing_and_sanitization_flags::FAILED != 0;
        let resolve_failed = rep.resolve_flags & resolve_flags::FAILED != 0;
        let status_ok = status_check_flags::REQUESTED | status_check_flags::PERFORMED;
        let status_failed = rep.status_check_flags & !status_ok != 0;
        if parsing_failed || resolve_failed || status_failed {
            let reason = match (parsing_failed, resolve_failed, status_failed) {
                (true, false, false) => CheckFailure::ParseOrSanitize,
                (false, true, false) => CheckFailure::AccountResolution,
                (false, false, true) => CheckFailure::StatusCheck,
                _ => unreachable!(),
            };
            self.emit_tx_event(
                bridge,
                meta.key,
                meta.priority,
                TransactionAction::CheckErr { reason },
            );
            self.metrics.check_err.increment(1);
            self.slot_stats.check_err += 1;

            // NB: If we are re-checking then we must remove here, else we can just silently
            // ignore the None returned by `remove()`.
            self.remove_checked(bridge, &meta);

            return TxDecision::Drop;
        }

        // Sanity check the flags.
        assert_eq!(
            rep.fee_payer_balance_flags,
            fee_payer_balance_flags::REQUESTED | fee_payer_balance_flags::PERFORMED,
            "{rep:?}"
        );
        assert_eq!(
            rep.resolve_flags,
            resolve_flags::REQUESTED | resolve_flags::PERFORMED,
            "{rep:?}"
        );
        assert_ne!(
            rep.status_check_flags & status_check_flags::REQUESTED,
            0,
            "{rep:?}"
        );
        assert_ne!(
            rep.status_check_flags & status_check_flags::PERFORMED,
            0,
            "{rep:?}"
        );

        // If already in checked_tx, this is a recheck completing - nothing to do.
        if self.checked_tx.contains(&meta) {
            self.metrics.check_ok.increment(1);
            self.slot_stats.check_ok += 1;

            return TxDecision::Keep;
        }

        // Apply the filter list against resolved ALT keys.
        if let Some(keys) = resolved_keys
            && keys
                .as_slice()
                .iter()
                .any(|key| self.filter_keys.contains(key))
        {
            self.metrics.check_filtered.increment(1);
            self.slot_stats.check_filtered += 1;

            return TxDecision::Drop;
        }

        // First check. Evict lowest priority if at capacity.
        if self.pending_len() >= self.checked_capacity {
            if let Some(min_checked) = self.checked_tx.first() {
                if meta.priority <= min_checked.priority {
                    self.emit_tx_event(
                        bridge,
                        meta.key,
                        meta.priority,
                        TransactionAction::Evict {
                            reason: EvictReason::CheckedCapacity,
                        },
                    );
                    bridge.drop_transaction(meta.key);
                    self.metrics.check_evict.increment(1);
                    self.slot_stats.check_evict += 1;
                    return TxDecision::Drop;
                }
            }

            if let Some(id) = self.pop_first_checked(bridge) {
                self.emit_tx_event(
                    bridge,
                    id.key,
                    id.priority,
                    TransactionAction::Evict {
                        reason: EvictReason::CheckedCapacity,
                    },
                );
                bridge.drop_transaction(id.key);

                self.metrics.check_evict.increment(1);
                self.slot_stats.check_evict += 1;
            }
        }

        // Insert the new transaction (now guaranteed to be higher priority than the worst).
        self.insert_checked(bridge, meta);
        self.emit_tx_event(bridge, meta.key, meta.priority, TransactionAction::CheckOk);

        // Update ok metric.
        self.metrics.check_ok.increment(1);
        self.slot_stats.check_ok += 1;

        TxDecision::Keep
    }

    fn on_execute(
        &mut self,
        bridge: &mut SchedulerBindingsBridge<PriorityId>,
        meta: PriorityId,
        rep: ExecutionResponse,
    ) -> TxDecision {
        // Remove from executing set now that execution is complete.
        assert!(self.executing_tx.remove(&meta.key));

        // Remove in-flight costs.
        self.in_flight_cus -= meta.cost;

        // Remove in flight locks.
        Self::unlock(&mut self.auction_engine, bridge, meta.key);

        // Emit event and update metrics.
        let action = match rep.not_included_reason {
            not_included_reasons::NONE => {
                self.slot_stats.execute_ok += 1;
                self.metrics.execute_ok.increment(1);

                TransactionAction::ExecuteOk
            }
            reason => {
                self.slot_stats.execute_err += 1;
                self.metrics.execute_err.increment(1);

                TransactionAction::ExecuteErr {
                    reason: u32::from(reason),
                }
            }
        };
        self.emit_tx_event(bridge, meta.key, meta.priority, action);

        // If non retryable or a bundle, just drop immediately.
        let is_bundle = meta.priority == BUNDLE_MARKER;
        let is_retryable = Self::is_retryable(rep.not_included_reason);
        if is_bundle || !is_retryable {
            return TxDecision::Drop;
        }

        // If we attempted on this slot already, defer to next slot. Unless this was a
        // lock conflict, then we can immediately retry.
        match rep.execution_slot == self.slot
            && rep.not_included_reason != not_included_reasons::ACCOUNT_IN_USE
        {
            true => assert!(self.deferred_tx.insert(meta)),
            false => {
                self.insert_checked(bridge, meta);
            }
        }

        // Evict from checked_tx if over capacity.
        if self.pending_len() > self.checked_capacity
            && let Some(evicted) = self.pop_first_checked(bridge)
        {
            self.emit_tx_event(
                bridge,
                evicted.key,
                evicted.priority,
                TransactionAction::Evict {
                    reason: EvictReason::CheckedCapacity,
                },
            );
            bridge.drop_transaction(evicted.key);
            self.metrics.execute_evict.increment(1);
        }

        TxDecision::Keep
    }

    fn pending_len(&self) -> usize {
        self.checked_tx.len() + self.executing_tx.len() + self.deferred_tx.len()
    }

    const fn is_retryable(reason: u8) -> bool {
        // TODO: Enable
        // assert_ne!(reason, not_included_reasons::ACCOUNT_IN_USE);

        matches!(
            reason,
            not_included_reasons::ACCOUNT_IN_USE
                | not_included_reasons::BANK_NOT_AVAILABLE
                | not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT
                | not_included_reasons::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT
                | not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT
                | not_included_reasons::WOULD_EXCEED_MAX_VOTE_COST_LIMIT
                | not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT
        )
    }

    /// Trys to schedule a transaction.
    ///
    /// # Return
    ///
    /// Places scheduled transactions in `self.schedule_batch`.
    fn try_schedule_transaction(
        &mut self,
        budget: &mut u64,
        bridge: &mut SchedulerBindingsBridge<PriorityId>,
        worker: usize,
    ) -> bool {
        let mut scheduled_txs = Vec::new();
        let max_batch_size = 32;
        let search_limit = 65536;

        for tx in self.checked_tx.iter().rev().take(search_limit) {
            if scheduled_txs.len() >= max_batch_size {
                break;
            }
            if tx.cost > *budget {
                continue;
            }
            let tx_locks: Vec<(Pubkey, bool)> = bridge
                .transaction(tx.key)
                .locks()
                .map(|(k, v)| (*k, v))
                .collect();
            if self.auction_engine.acquire_locks(tx.key, &tx_locks) {
                scheduled_txs.push(*tx);
                *budget -= tx.cost;
                self.in_flight_cus += tx.cost;
            }
        }

        if scheduled_txs.is_empty() {
            return false;
        }

        for tx in &scheduled_txs {
            self.schedule_batch.push(KeyedTransactionMeta {
                key: tx.key,
                meta: *tx,
            });
        }

        // Schedule the batch.
        bridge
            .schedule(ScheduleBatch {
                worker,
                transactions: &self.schedule_batch,
                max_working_slot: bridge.progress().current_slot + 1,
                flags: pack_message_flags::EXECUTE,
            })
            .unwrap();

        // Update state.
        for tx in scheduled_txs {
            assert!(self.remove_checked(bridge, &tx));
        }
        true
    }

    /// Trys to schedule a bundle.
    ///
    /// # Return
    ///
    /// Places scheduled transactions in `self.schedule_batch`.
    fn try_schedule_bundle(
        &mut self,
        budget: &mut u64,
        bridge: &mut SchedulerBindingsBridge<PriorityId>,
        worker: usize,
    ) -> bool {
        let Some(bundle) = self.bundles.last() else {
            return false;
        };

        // Check this fits in budget.
        if bundle.cost > *budget {
            return false;
        }

        // See if the bundle can be scheduled without conflicts.
        if !bundle
            .keys
            .iter()
            .all(|tx_key| Self::can_lock(&self.auction_engine, bridge, *tx_key))
        {
            return false;
        }

        // Acquire locks centrally via auction engine for each tx in the bundle.
        for tx_key in &bundle.keys {
            let tx_locks: Vec<(Pubkey, bool)> = bridge
                .transaction(*tx_key)
                .locks()
                .map(|(k, v)| (*k, v))
                .collect();
            if !self.auction_engine.acquire_locks(*tx_key, &tx_locks) {
                // If we can't acquire all locks, release any we already acquired
                for prev_key in &bundle.keys[..bundle
                    .keys
                    .iter()
                    .position(|k| *k == *tx_key)
                    .unwrap_or(bundle.keys.len())]
                {
                    let prev_locks: Vec<(Pubkey, bool)> = bridge
                        .transaction(*prev_key)
                        .locks()
                        .map(|(k, v)| (*k, v))
                        .collect();
                    self.auction_engine.release_locks(*prev_key, &prev_locks);
                }
                return false;
            }
        }

        self.schedule_batch
            .extend(
                bundle
                    .keys
                    .iter()
                    .enumerate()
                    .map(|(i, key)| KeyedTransactionMeta {
                        key: *key,
                        meta: PriorityId {
                            // TODO: This is a hacky way to identify bundles.
                            priority: BUNDLE_MARKER,
                            cost: match i {
                                0 => bundle.cost,
                                1.. => 0,
                            },
                            key: *key,
                        },
                    }),
            );

        bridge
            .schedule(ScheduleBatch {
                worker,
                transactions: &self.schedule_batch,
                max_working_slot: bridge.progress().current_slot + 1,
                flags: pack_message_flags::EXECUTE
                    | execution_flags::DROP_ON_FAILURE
                    | execution_flags::ALL_OR_NOTHING,
            })
            .unwrap();

        // Update state.
        *budget -= bundle.cost;
        self.in_flight_cus += bundle.cost;

        // Dequeue bundle transactions
        for tx_key in &bundle.keys {
            let tx_locks: Vec<(Pubkey, bool)> = bridge
                .transaction(*tx_key)
                .locks()
                .map(|(k, v)| (*k, v))
                .collect();
            self.auction_engine.dequeue_transaction(&tx_locks);
        }
        self.bundles.pop_last().unwrap();
        true
    }

    /// Checks a TX for lock conflicts without inserting locks.
    /// Uses the centralized lock manager from the auction engine.
    fn can_lock(
        auction_engine: &AuctionEngine,
        bridge: &SchedulerBindingsBridge<PriorityId>,
        tx_key: TransactionKey,
    ) -> bool {
        // Check if this transaction's read/write locks conflict with any
        // pre-existing read/write locks via the centralized lock manager.
        let tx_locks: Vec<(Pubkey, bool)> = bridge
            .transaction(tx_key)
            .locks()
            .map(|(k, v)| (*k, v))
            .collect();
        auction_engine.lock_manager().try_acquire(&tx_locks)
    }

    /// Locks a transaction without checking for conflicts.
    /// Uses the centralized lock manager from the auction engine.
    fn lock(
        auction_engine: &mut AuctionEngine,
        bridge: &SchedulerBindingsBridge<PriorityId>,
        tx_key: TransactionKey,
    ) {
        let tx_locks: Vec<(Pubkey, bool)> = bridge
            .transaction(tx_key)
            .locks()
            .map(|(k, v)| (*k, v))
            .collect();
        auction_engine.acquire_locks(tx_key, &tx_locks);
    }

    /// Unlocks a transaction, releasing all its locks.
    /// Uses the centralized lock manager from the auction engine.
    ///
    /// Panics if the transaction doesn't hold the expected locks.
    fn unlock(
        auction_engine: &mut AuctionEngine,
        bridge: &SchedulerBindingsBridge<PriorityId>,
        tx_key: TransactionKey,
    ) {
        let tx_locks: Vec<(Pubkey, bool)> = bridge
            .transaction(tx_key)
            .locks()
            .map(|(k, v)| (*k, v))
            .collect();
        auction_engine.release_locks(tx_key, &tx_locks);
    }

    /// Compute effective remaining CU considering executing transactions.
    fn effective_remaining_cu(&self, bridge: &SchedulerBindingsBridge<PriorityId>) -> u64 {
        let progress = bridge.progress();
        let total_cu = MAX_BLOCK_UNITS_SIMD_0256;
        let used_cu = total_cu as u64 - progress.remaining_cost_units as u64;
        // Effective remaining = total - used - in_flight (what we've allocated but not yet executed)
        total_cu as u64 - used_cu - self.in_flight_cus
    }

    fn insert_checked_with_capacity(
        &mut self,
        bridge: &mut SchedulerBindingsBridge<PriorityId>,
        meta: PriorityId,
    ) -> TxDecision {
        if self.pending_len() >= self.checked_capacity {
            if let Some(min_checked) = self.checked_tx.first() {
                if meta.priority <= min_checked.priority {
                    self.emit_tx_event(
                        bridge,
                        meta.key,
                        meta.priority,
                        TransactionAction::Evict {
                            reason: EvictReason::CheckedCapacity,
                        },
                    );
                    bridge.drop_transaction(meta.key);
                    self.metrics.check_evict.increment(1);
                    self.slot_stats.check_evict += 1;
                    return TxDecision::Drop;
                }
            }

            if let Some(id) = self.pop_first_checked(bridge) {
                self.emit_tx_event(
                    bridge,
                    id.key,
                    id.priority,
                    TransactionAction::Evict {
                        reason: EvictReason::CheckedCapacity,
                    },
                );
                bridge.drop_transaction(id.key);
                self.metrics.check_evict.increment(1);
                self.slot_stats.check_evict += 1;
            }
        }

        self.insert_checked(bridge, meta);
        TxDecision::Keep
    }

    fn insert_checked(&mut self, bridge: &SchedulerBindingsBridge<PriorityId>, meta: PriorityId) {
        let tx_locks: Vec<(Pubkey, bool)> = bridge
            .transaction(meta.key)
            .locks()
            .map(|(k, v)| (*k, v))
            .collect();
        self.auction_engine.queue_transaction(&tx_locks);
        self.checked_tx.insert(meta);
    }

    fn remove_checked(
        &mut self,
        bridge: &SchedulerBindingsBridge<PriorityId>,
        meta: &PriorityId,
    ) -> bool {
        if self.checked_tx.remove(meta) {
            let tx_locks: Vec<(Pubkey, bool)> = bridge
                .transaction(meta.key)
                .locks()
                .map(|(k, v)| (*k, v))
                .collect();
            self.auction_engine.dequeue_transaction(&tx_locks);
            true
        } else {
            false
        }
    }

    fn pop_first_checked(
        &mut self,
        bridge: &SchedulerBindingsBridge<PriorityId>,
    ) -> Option<PriorityId> {
        let meta = self.checked_tx.pop_first()?;
        let tx_locks: Vec<(Pubkey, bool)> = bridge
            .transaction(meta.key)
            .locks()
            .map(|(k, v)| (*k, v))
            .collect();
        self.auction_engine.dequeue_transaction(&tx_locks);
        Some(meta)
    }

    fn calculate_priority(
        &self,
        runtime: &RuntimeState,
        _remaining_cu: u64,
        tx: &TransactionState,
    ) -> Option<(u64, u64)> {
        let costs = derive_costs(&tx.data, &runtime.feature_set)?;

        // Extract locks for the auction engine
        let locks = tx.locks().map(|(k, v)| (*k, v));

        // Extract tip to avoid unfair advantage in bundle scoring
        let tip = Self::extract_tip(&tx.data);

        // Auction-based scoring only — no fallback. If the auction rejects it, it doesn't belong in the block.
        self.score_with_auction(&costs, locks, tip)
    }

    /// Process auction engine tick with current market state.
    /// Updates resource prices, opportunity entropy, and CU allocation.
    /// Optimized to O(1) by reading directly from bridge progress data.
    fn process_auction_tick(&mut self, bridge: &SchedulerBindingsBridge<PriorityId>) {
        // 1. CU Demand: Use actual bridge data instead of estimated averages
        let remaining_cu = bridge.progress().remaining_cost_units;
        let total_cu = MAX_BLOCK_UNITS_SIMD_0256;
        let cu_demand = (total_cu as f64) - (remaining_cu as f64);

        // 2. Time Demand: Calculate based on slot progress (approx 400ms per slot)
        let slot_progress = bridge.progress().current_slot_progress;
        let ms_elapsed = ((slot_progress as f64 / 100.0) * 400.0).max(1.0).min(399.0);
        let ms_remaining = 400.0 - ms_elapsed;

        // 3. Lock Demand: Use the lock manager's internal state (fast, O(1))
        let lock_demand = self.auction_engine.lock_manager().total_contention_depth();

        // 4. Space Fragmentation: Ratio of used CU to total
        let space_demand = 1.0 - (remaining_cu as f64 / total_cu as f64);

        // Update auction engine (prices, entropy, CU allocation, lock decay)
        self.auction_engine.process_tick(
            &TickStats {
                cu_demand,
                lock_demand,
                time_elapsed: ms_elapsed,
                fragmentation: space_demand,
                p90_price: self.auction_engine.cu_price(),
                cu_variance: 0.0,
                lock_variance: 0.0,
            },
            remaining_cu,
            ms_remaining,
        );
    }

    fn score_with_auction(
        &self,
        costs: &TxCosts,
        locks: impl Iterator<Item = (Pubkey, bool)>,
        tip: u64,
    ) -> Option<(u64, u64)> {
        let base_cu = costs.total_cost;
        let priority_fee_lamports = costs.prioritization_fee as f64;
        let tip_lamports = tip as f64;

        // 1. Compute serialization penalty from centralized lock manager
        let all_locks: Vec<(Pubkey, bool)> = locks.collect();
        let num_write_locks = all_locks.iter().filter(|(_, w)| *w).count();
        let num_read_locks = all_locks.len() - num_write_locks;

        let serialization_pen = self
            .auction_engine
            .lock_manager()
            .serialization_penalty(&all_locks);
        // Adjust effective CU to penalize transactions that access highly congested accounts
        let eff_cu = (base_cu as f64 * (1.0 + serialization_pen)).max(1.0) as u64;

        // 2. Compute resource cost from centralized prices
        let resource_cost =
            self.auction_engine
                .prices()
                .total_cost(eff_cu, num_write_locks, num_read_locks);

        // 3. Compute flexibility discount from centralized entropy
        let flexibility_discount = self.auction_engine.flexibility_coeff() * 0.3;

        // 4. Surplus calculation (all values in lamports)
        // Reward includes base signature fee (5,000 lamports) + priority fee + tip
        let base_fee = 5_000.0;
        let reward = base_fee + priority_fee_lamports + tip_lamports;
        let surplus = reward - resource_cost * (1.0 - flexibility_discount);

        let priority = if surplus < 0.0 {
            0
        } else {
            // 5. Scale raw surplus by CU efficiency to derive priority.
            let scaled_surplus = (surplus * PRIORITY_MULTIPLIER as f64) / costs.total_cost as f64;
            let scaled_surplus = scaled_surplus.max(0.0);

            if scaled_surplus >= (BUNDLE_MARKER - 1) as f64 {
                BUNDLE_MARKER - 1
            } else {
                scaled_surplus as u64
            }
        };

        Some((priority, costs.total_cost))
    }

    /// Emit transaction event for observability.
    fn emit_tx_event(
        &self,
        bridge: &SchedulerBindingsBridge<PriorityId>,
        key: TransactionKey,
        priority: u64,
        action: TransactionAction,
    ) {
        let Some(events) = &self.events else { return };

        // Don't emit for vote TXs (save my disk/familia).
        let tx = bridge.transaction(key);
        if tx.is_simple_vote() {
            return;
        }

        events.emit(Event::Transaction(TransactionEvent {
            signature: tx.data.signatures()[0],
            slot: self.slot,
            priority,
            action,
        }));
    }

    fn should_filter_static(&self, tx: &SanitizedTransactionView<TransactionPtr>) -> bool {
        tx.static_account_keys()
            .iter()
            .any(|key| self.filter_keys.contains(key))
    }

    /// Extract tip payments from a transaction.
    /// Sums all system program transfers to tip recipient accounts.
    fn extract_tip(tx: &SanitizedTransactionView<TransactionPtr>) -> u64 {
        let account_keys = tx.static_account_keys();

        tx.program_instructions_iter()
            .filter_map(|(program_id, ix)| {
                // Check for system program transfer (discriminator = 2).
                if program_id != &system_program::ID
                    || ix.data.len() < 12
                    || u32::from_le_bytes(*arrayref::array_ref![ix.data, 0, 4]) != 2
                {
                    return None;
                }

                let dest_idx = *ix.accounts.get(1)? as usize;
                let dest = account_keys.get(dest_idx)?;
                let amount = u64::from_le_bytes(*arrayref::array_ref![ix.data, 4, 8]);

                TIP_ACCOUNTS.contains(dest).then_some(amount)
            })
            .sum()
    }
}

#[allow(dead_code)]
struct BatchMetrics {
    current_slot: Gauge,
    next_leader_slot: Gauge,

    tpu_unchecked_len: Gauge,
    tpu_checked_len: Gauge,
    tpu_deferred_len: Gauge,
    bundles_len: Gauge,
    locks_len: Gauge,
    executing_len: Gauge,

    in_flight_cus: Gauge,

    recv_tpu_ok: Counter,
    recv_tpu_err: Counter,
    recv_tpu_evict: Counter,
    recv_tpu_filtered: Counter,

    recv_packet_ok: Counter,
    recv_packet_err: Counter,
    recv_packet_evict: Counter,
    recv_packet_filtered: Counter,

    recv_bundle_ok: Counter,
    recv_bundle_err: Counter,
    recv_bundle_filtered: Counter,
    recv_bundle_expired: Counter,
    recv_bundle_evict: Counter,

    check_requested: Counter,
    check_ok: Counter,
    check_err: Counter,
    check_filtered: Counter,
    check_evict: Counter,

    execute_requested: Counter,
    execute_ok: Counter,
    execute_err: Counter,
    execute_unprocessed: Counter,
    execute_evict: Counter,
}

impl BatchMetrics {
    fn new() -> Self {
        Self {
            current_slot: gauge!("slot", "label" => "current"),
            next_leader_slot: gauge!("slot", "label" => "next_leader"),

            tpu_unchecked_len: gauge!("container_len", "label" => "tpu_unchecked"),
            tpu_checked_len: gauge!("container_len", "label" => "tpu_checked"),
            tpu_deferred_len: gauge!("container_len", "label" => "tpu_deferred"),
            bundles_len: gauge!("container_len", "label" => "bundles"),
            locks_len: gauge!("container_len", "label" => "locks"),
            executing_len: gauge!("container_len", "label" => "executing"),

            recv_tpu_ok: counter!("recv_tpu", "label" => "ok"),
            recv_tpu_err: counter!("recv_tpu", "label" => "err"),
            recv_tpu_evict: counter!("recv_tpu", "label" => "evict"),
            recv_tpu_filtered: counter!("recv_tpu", "label" => "filtered"),

            recv_packet_ok: counter!("recv_packet", "label" => "ok"),
            recv_packet_err: counter!("recv_packet", "label" => "err"),
            recv_packet_evict: counter!("recv_packet", "label" => "evict"),
            recv_packet_filtered: counter!("recv_packet", "label" => "filtered"),

            recv_bundle_ok: counter!("recv_bundle", "label" => "ok"),
            recv_bundle_err: counter!("recv_bundle", "label" => "err"),
            recv_bundle_filtered: counter!("recv_bundle", "label" => "filtered"),
            recv_bundle_expired: counter!("recv_bundle", "label" => "expired"),
            recv_bundle_evict: counter!("recv_bundle", "label" => "evict"),

            in_flight_cus: gauge!("in_flight_cus"),

            check_ok: counter!("check", "label" => "ok"),
            check_err: counter!("check", "label" => "err"),
            check_filtered: counter!("check", "label" => "filtered"),
            check_evict: counter!("check", "label" => "evict"),
            check_requested: counter!("check", "label" => "requested"),

            execute_requested: counter!("execute", "label" => "requested"),
            execute_ok: counter!("execute", "label" => "ok"),
            execute_err: counter!("execute", "label" => "err"),
            execute_unprocessed: counter!("execute", "label" => "unprocessed"),
            execute_evict: counter!("execute", "label" => "evict"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BundleId {
    priority: u64,
    cost: u64,
    received_at: Instant,
    keys: Vec<TransactionKey>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use agave_scheduler_bindings::worker_message_types::not_included_reasons;
    use agave_scheduler_bindings::{LEADER_READY, NOT_LEADER, ProgressMessage, pack_message_flags};
    use agave_scheduling_utils::bridge::{ScheduleBatch, TestBridge};
    use solana_compute_budget_interface::ComputeBudgetInstruction;
    use solana_hash::Hash;
    use solana_keypair::{Keypair, Signer};
    use solana_pubkey::Pubkey;
    use solana_transaction::versioned::VersionedTransaction;
    use solana_transaction::{AccountMeta, Instruction, Transaction};

    const MOCK_PROGRESS: ProgressMessage = ProgressMessage {
        leader_state: NOT_LEADER,
        current_slot: 10,
        next_leader_slot: 11,
        leader_range_end: 11,
        remaining_cost_units: 50_000_000,
        current_slot_progress: 25,
        epoch: 0,
        latest_blockhash: [0; 32],
    };

    fn test_scheduler() -> (AuctionBatchScheduler, crossbeam_channel::Sender<JitoUpdate>) {
        let (jito_tx, jito_rx) = crossbeam_channel::bounded(1024);

        jito_tx
            .send(JitoUpdate::BuilderConfig(BuilderConfig {
                key: Pubkey::new_unique(),
                commission: 0,
            }))
            .unwrap();

        let args = AuctionBatchSchedulerArgs {
            tip: TipDistributionArgs {
                vote_account: Pubkey::new_unique(),
                merkle_authority: Pubkey::new_unique(),
                commission_bps: 0,
            },
            jito: JitoArgs {
                http_rpc: String::new(),
                ws_rpc: String::new(),
                block_engine: String::new(),
            },
            keypair: Arc::new(Keypair::new()),
            filter_keys: HashSet::new(),
            unchecked_capacity: 64,
            checked_capacity: 64,
            bundle_capacity: 16,
            runtime: RuntimeConfig::default(),
            scoring: AuctionBatchConfig::default(),
        };
        let scheduler =
            AuctionBatchScheduler::new_with_jito(CancellationToken::new(), None, args, jito_rx);

        (scheduler, jito_tx)
    }

    fn noop_with_budget(payer: &Keypair, cu_limit: u32, cu_price: u64) -> VersionedTransaction {
        Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
                ComputeBudgetInstruction::set_compute_unit_price(cu_price),
            ],
            Some(&payer.pubkey()),
            &[payer],
            Hash::new_from_array([1; 32]),
        )
        .into()
    }

    fn noop_with_lookup_table(
        payer: &Keypair,
        cu_limit: u32,
        cu_price: u64,
    ) -> VersionedTransaction {
        use solana_message::{AddressLookupTableAccount, VersionedMessage, v0};
        let lookup_key = Pubkey::new_unique();
        let to_key = Pubkey::new_unique();
        let ix = Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![AccountMeta::new(to_key, false)],
            data: vec![],
        };
        let message = VersionedMessage::V0(
            v0::Message::try_compile(
                &payer.pubkey(),
                &[
                    ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
                    ComputeBudgetInstruction::set_compute_unit_price(cu_price),
                    ix,
                ],
                &[AddressLookupTableAccount {
                    key: lookup_key,
                    addresses: vec![to_key],
                }],
                Hash::new_from_array([1; 32]),
            )
            .unwrap(),
        );
        VersionedTransaction::try_new(message, &[payer]).unwrap()
    }

    fn mock_tx_with_locks(
        payer: &Keypair,
        cu_limit: u32,
        cu_price: u64,
        write_keys: &[Pubkey],
        read_keys: &[Pubkey],
    ) -> VersionedTransaction {
        let mut metas = Vec::new();
        for &k in write_keys {
            metas.push(AccountMeta::new(k, false));
        }
        for &k in read_keys {
            metas.push(AccountMeta::new_readonly(k, false));
        }

        let ix = Instruction {
            program_id: Pubkey::new_unique(),
            accounts: metas,
            data: vec![],
        };

        Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
                ComputeBudgetInstruction::set_compute_unit_price(cu_price),
                ix,
            ],
            Some(&payer.pubkey()),
            &[payer],
            Hash::new_from_array([1; 32]),
        )
        .into()
    }

    #[test]
    fn tpu_recv_schedules_check() {
        let (mut scheduler, _jito_tx) = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        let payer = Keypair::new();
        let tx = noop_with_lookup_table(&payer, 25_000, 1000);
        bridge.queue_tpu(&tx);

        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(batch.flags & 1, pack_message_flags::CHECK);
        assert_eq!(batch.transactions.len(), 1);
    }

    #[test]
    fn check_ok_moves_to_checked() {
        let (mut scheduler, _jito_tx) = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        let payer = Keypair::new();
        let tx = noop_with_lookup_table(&payer, 25_000, 100_000); // Higher price to clear min_surplus
        bridge.queue_tpu(&tx);

        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        let check_batch = bridge.pop_schedule().unwrap();
        bridge.queue_check_response_ok(&check_batch, 0, None);

        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 1);
    }

    #[test]
    fn test_auction_scheduler_congestion_bench() {
        let (mut scheduler, _jito_tx) = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        let hot_account = Pubkey::new_unique();
        let cold_account = Pubkey::new_unique();

        // 1. Fill the queue with transactions targeting the hot_account to build up queue depth
        for _ in 0..10 {
            let payer = Keypair::new();
            let tx = mock_tx_with_locks(&payer, 50_000, 10_000, &[hot_account], &[]);
            bridge.queue_tpu(&tx);
        }

        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        bridge.queue_all_checks_ok();

        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Check that hot queue depth is now high
        assert_eq!(scheduler.checked_tx.len(), 10);
        let hot_depth = scheduler
            .auction_engine
            .lock_manager()
            .queue_depth(&hot_account);
        assert!(hot_depth > 0.0);

        // 2. Try to ingest a low-paying transaction targeting the hot account (should be rejected due to serialization penalty)
        let payer_low_hot = Keypair::new();
        let tx_low_hot = mock_tx_with_locks(&payer_low_hot, 50_000, 50, &[hot_account], &[]);
        let tx_bytes = wincode::serialize(&tx_low_hot).unwrap();
        let tx_state = bridge.insert_transaction(&tx_bytes).unwrap();
        let costs = derive_costs(
            &bridge.transaction(tx_state).data,
            &bridge.runtime().feature_set,
        )
        .unwrap();

        let locks = bridge.transaction(tx_state).locks().map(|(k, v)| (*k, v));
        let low_hot_score = scheduler.score_with_auction(&costs, locks, 0);
        assert!(
            low_hot_score.is_some(),
            "Low paying hot transaction should be accepted"
        );

        // 3. Try to ingest a high-paying transaction targeting the hot account (should be accepted)
        let payer_high_hot = Keypair::new();
        let tx_high_hot = mock_tx_with_locks(&payer_high_hot, 50_000, 20_000, &[hot_account], &[]);
        let tx_bytes_high = wincode::serialize(&tx_high_hot).unwrap();
        let tx_state_high = bridge.insert_transaction(&tx_bytes_high).unwrap();
        let costs_high = derive_costs(
            &bridge.transaction(tx_state_high).data,
            &bridge.runtime().feature_set,
        )
        .unwrap();

        let locks_high = bridge
            .transaction(tx_state_high)
            .locks()
            .map(|(k, v)| (*k, v));
        let high_hot_score = scheduler.score_with_auction(&costs_high, locks_high, 0);
        assert!(
            high_hot_score.is_some(),
            "High paying hot transaction should be accepted despite congestion"
        );

        // 4. Try to ingest a low-paying transaction targeting the cold account (should be accepted because there's no congestion on it)
        let payer_low_cold = Keypair::new();
        let tx_low_cold = mock_tx_with_locks(&payer_low_cold, 50_000, 5000, &[cold_account], &[]);
        let tx_bytes_cold = wincode::serialize(&tx_low_cold).unwrap();
        let tx_state_cold = bridge.insert_transaction(&tx_bytes_cold).unwrap();
        let costs_cold = derive_costs(
            &bridge.transaction(tx_state_cold).data,
            &bridge.runtime().feature_set,
        )
        .unwrap();

        let locks_cold = bridge
            .transaction(tx_state_cold)
            .locks()
            .map(|(k, v)| (*k, v));
        let low_cold_score = scheduler.score_with_auction(&costs_cold, locks_cold, 0);
        assert!(
            low_cold_score.is_some(),
            "Low paying cold transaction should be accepted because there is no serialization penalty"
        );

        let (low_hot_priority, _) = low_hot_score.unwrap();
        let (high_hot_priority, _) = high_hot_score.unwrap();
        let (low_cold_priority, _) = low_cold_score.unwrap();

        assert!(
            high_hot_priority > low_hot_priority,
            "High paying hot priority ({}) should exceed low paying hot priority ({})",
            high_hot_priority,
            low_hot_priority
        );
        assert!(
            low_cold_priority > low_hot_priority,
            "Low paying cold priority ({}) should exceed low paying hot priority ({}) due to serialization penalty",
            low_cold_priority,
            low_hot_priority
        );
    }

    #[test]
    fn test_time_decay_penalty() {
        let (mut scheduler, _jito_tx) = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        let payer = Keypair::new();
        // A transaction with a moderate fee (15 lamports: 25k * 600 / 1M = 15)
        let tx = noop_with_budget(&payer, 25_000, 600);

        // 1. Early in the slot (progress = 0)
        let progress_early = ProgressMessage {
            current_slot_progress: 0,
            ..MOCK_PROGRESS
        };
        bridge.queue_progress(progress_early);
        scheduler.poll(&mut bridge);

        let tx_state_early = bridge
            .insert_transaction(&wincode::serialize(&tx).unwrap())
            .unwrap();
        let costs_early = derive_costs(
            &bridge.transaction(tx_state_early).data,
            &bridge.runtime().feature_set,
        )
        .unwrap();
        let locks_early = bridge
            .transaction(tx_state_early)
            .locks()
            .map(|(k, v)| (*k, v));

        let score_early = scheduler.score_with_auction(&costs_early, locks_early, 0);
        assert!(
            score_early.is_some(),
            "Transaction should be accepted early in the slot due to low time penalty"
        );

        // 2. Late in the slot (progress = 95)
        let progress_late = ProgressMessage {
            current_slot_progress: 95,
            ..MOCK_PROGRESS
        };
        bridge.queue_progress(progress_late);
        scheduler.poll(&mut bridge);

        let tx_state_late = bridge
            .insert_transaction(&wincode::serialize(&tx).unwrap())
            .unwrap();
        let costs_late = derive_costs(
            &bridge.transaction(tx_state_late).data,
            &bridge.runtime().feature_set,
        )
        .unwrap();
        let locks_late = bridge
            .transaction(tx_state_late)
            .locks()
            .map(|(k, v)| (*k, v));

        let score_late = scheduler.score_with_auction(&costs_late, locks_late, 0);
        assert!(
            score_late.is_some(),
            "Transaction should be accepted late in the slot"
        );
        let (priority_late, _) = score_late.unwrap();
        let (priority_early, _) = score_early.unwrap();
        assert!(
            priority_late < priority_early,
            "Late priority ({}) should be less than early priority ({}) due to time penalty decay",
            priority_late,
            priority_early
        );
    }

    #[test]
    fn test_jito_bundle_pricing_atomic() {
        let (mut scheduler, jito_tx) = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        let payer1 = Keypair::new();
        let payer2 = Keypair::new();

        // 1. Queue a low-paying bundle (rejected on ingestion because the sum of fees is less than the total cost)
        let tx_low1 = noop_with_budget(&payer1, 25_000, 10);
        let tx_low2 = noop_with_budget(&payer2, 25_000, 10);

        jito_tx
            .send(JitoUpdate::Bundle(vec![
                wincode::serialize(&tx_low1).unwrap(),
                wincode::serialize(&tx_low2).unwrap(),
            ]))
            .unwrap();

        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(
            scheduler.bundles.len(),
            1,
            "Low paying bundle should be accepted on ingestion"
        );
        let low_bundle = scheduler.bundles.iter().next().unwrap();
        assert!(
            low_bundle.priority > 0,
            "Low paying bundle priority ({}) should be positive",
            low_bundle.priority
        );

        // 2. Queue a high-paying bundle (accepted and stored)
        let tx_high1 = noop_with_budget(&payer1, 25_000, 5000);
        let tx_high2 = noop_with_budget(&payer2, 25_000, 5000);

        jito_tx
            .send(JitoUpdate::Bundle(vec![
                wincode::serialize(&tx_high1).unwrap(),
                wincode::serialize(&tx_high2).unwrap(),
            ]))
            .unwrap();

        scheduler.poll(&mut bridge);
        assert_eq!(
            scheduler.bundles.len(),
            2,
            "Both bundles should be successfully ingested"
        );

        let mut bundle_iter = scheduler.bundles.iter();
        let first = bundle_iter.next().unwrap();
        let second = bundle_iter.next().unwrap();
        assert!(
            second.priority > first.priority,
            "High paying bundle ({}) should have higher priority than low paying bundle ({})",
            second.priority,
            first.priority
        );
    }

    type SetupExecuting = (
        AuctionBatchScheduler,
        TestBridge<PriorityId>,
        crossbeam_channel::Sender<JitoUpdate>,
        ScheduleBatch<Vec<KeyedTransactionMeta<PriorityId>>>,
    );

    fn setup_executing_tx(cu_limit: u32, cu_price: u64) -> SetupExecuting {
        let (mut scheduler, jito_tx) = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Ingest a TX.
        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, cu_limit, cu_price);
        bridge.queue_tpu(&tx);

        // Poll - ingest & schedule checks.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Complete checks.
        bridge.queue_all_checks_ok();
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 1);

        // Provide tip config before becoming leader.
        jito_tx
            .send(JitoUpdate::TipConfig(TipConfig {
                tip_receiver: Pubkey::new_unique(),
                block_builder: Pubkey::new_unique(),
            }))
            .unwrap();
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Transition to leader.
        bridge.queue_progress(ProgressMessage {
            leader_state: LEADER_READY,
            ..MOCK_PROGRESS
        });
        scheduler.poll(&mut bridge);

        // Skip past the become-tip-receiver batches (2x EXECUTE|DROP_ON_FAILURE).
        let tip0 = bridge.pop_schedule().unwrap();
        assert_ne!(tip0.flags & pack_message_flags::EXECUTE, 0);
        let tip1 = bridge.pop_schedule().unwrap();
        assert_ne!(tip1.flags & pack_message_flags::EXECUTE, 0);

        // Pop the user TX execute batch.
        let exec_batch = bridge.pop_schedule().unwrap();
        assert_eq!(exec_batch.flags, pack_message_flags::EXECUTE);
        assert_eq!(exec_batch.transactions.len(), 1);
        assert_eq!(scheduler.checked_tx.len(), 0);
        assert!(
            scheduler
                .executing_tx
                .contains(&exec_batch.transactions[0].key)
        );

        (scheduler, bridge, jito_tx, exec_batch)
    }

    #[test]
    fn test_deferred_tx_drained_on_slot_roll() {
        let (mut scheduler, mut bridge, _jito_tx, exec_batch) = setup_executing_tx(25_000, 100);
        let tx_key = exec_batch.transactions[0].key;

        // Queue a retryable error that defers the TX.
        bridge.queue_execute_response(
            &exec_batch,
            0,
            bridge.execute_err(not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT),
        );

        // Poll to drain the response - TX moves to deferred.
        bridge.queue_progress(ProgressMessage {
            leader_state: LEADER_READY,
            ..MOCK_PROGRESS
        });
        scheduler.poll(&mut bridge);
        assert!(scheduler.deferred_tx.iter().any(|id| id.key == tx_key));

        // Roll to the next slot.
        bridge.queue_progress(ProgressMessage {
            current_slot: MOCK_PROGRESS.current_slot + 1,
            ..MOCK_PROGRESS
        });
        scheduler.poll(&mut bridge);

        // Deferred TX drained back to checked.
        assert_eq!(scheduler.deferred_tx.len(), 0);
        assert!(scheduler.checked_tx.iter().any(|id| id.key == tx_key));
        assert!(bridge.contains_tx(tx_key));
    }

    #[test]
    fn test_unchecked_tpu_eviction_exceeds_capacity() {
        let (mut scheduler, _jito_tx) = test_scheduler();
        scheduler.unchecked_capacity = 2;

        let mut bridge = TestBridge::new(5, 4);

        // Queue 5 transactions.
        for _ in 0..5 {
            let payer = Keypair::new();
            let tx = noop_with_lookup_table(&payer, 25_000, 1000);
            bridge.queue_tpu(&tx);
        }

        // Poll to ingest. shortfall = (0 + 5) - 2 = 3.
        // Previously this would have panicked. Now it must run cleanly.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        assert!(scheduler.unchecked_tx.len() <= 2);
    }
}
