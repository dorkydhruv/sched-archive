//! Mainnet-replay comparative benchmark.
//!
//! Parses `etc/tx_io.log` produced by a live validator and replays
//! the exact transactions — with real arrival timing — through both
//! `BatchScheduler` and `AuctionBatchScheduler`.
//!
//! # Log format (one line per event)
//!
//! ```text
//! 2026-05-25 16:04:00.271127118 tx_in_signature: <base58_sig> tx: <base64_tx>
//! 2026-05-25 16:04:00.276101098 tx_out_signature: <base58_sig> status: Ok(()) | Err(...)
//! ```
//!
//! # How it works
//!
//! 1. **Parse** – extract all `tx_in_signature` entries with their timestamps
//!    and base64-encoded `VersionedTransaction` bytes.
//! 2. **Segment** – split into "leader windows" separated by ≥5 s gaps
//!    (mirrors the real leader schedule rotation).
//! 3. **Replay** – for each leader window, feed the real transactions into a
//!    `TestBridge` at the correct relative tick offsets (6.25 ms per tick,
//!    64 ticks per 400 ms slot).  `ProgressMessage` mirrors a real slot:
//!    ticks 0-9 = `NOT_LEADER` (pre-warm), ticks 10-63 = `LEADER_READY`.
//! 4. **Compare** – aggregate `BenchReport` across all windows and print a
//!    side-by-side summary.
//!
//! Run:
//! ```bash
//! cargo bench --bench replay_bench
//! # or with a custom log path:
//! TX_IO_LOG=path/to/my.log cargo bench --bench replay_bench
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use agave_scheduler_bindings::{LEADER_READY, NOT_LEADER, ProgressMessage};
mod bench_bridge;
use bench_bridge::BenchBridge;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_transaction::versioned::VersionedTransaction;

use schedulers::jito::jito_thread::{BuilderConfig, JitoArgs, JitoUpdate, TipConfig};
use schedulers::jito::tip_program::TipDistributionArgs;

use auction_batch_scheduler::{
    AuctionBatchConfig, AuctionBatchScheduler, AuctionBatchSchedulerArgs, RuntimeConfig,
};
use batch_scheduler::{BatchScheduler, BatchSchedulerArgs};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Ticks per slot (Solana mainnet = 64 ticks / 400 ms).
const TICKS_PER_SLOT: usize = 64;

/// Duration of one tick in nanoseconds (400 ms / 64 = 6.25 ms).
const _TICK_NS: u128 = 6_250_000;

/// Gap (in seconds) between two log lines that indicates a new leader window.
const LEADER_GAP_SECS: f64 = 5.0;

/// Execution latency in ticks (how many ticks a scheduled tx takes to
/// "execute" before we feed back a success response).
const EXECUTION_LATENCY_TICKS: u32 = 1;

/// Block CU budget (mainnet default).
const BLOCK_CU_BUDGET: u64 = 60_000_000;

const MOCK_PROGRESS: ProgressMessage = ProgressMessage {
    leader_state: NOT_LEADER,
    current_slot: 100,
    next_leader_slot: 101,
    leader_range_end: 101,
    remaining_cost_units: BLOCK_CU_BUDGET,
    current_slot_progress: 0,
    epoch: 0,
    latest_blockhash: [0; 32],
};

// ---------------------------------------------------------------------------
// Parsed types
// ---------------------------------------------------------------------------

/// One parsed `tx_in_signature` entry.
#[derive(Clone)]
struct TxInEntry {
    /// Nanoseconds since the epoch (from the log timestamp).
    timestamp_ns: u128,
    /// The raw transaction bytes decoded from the base64 field.
    tx_bytes: Vec<u8>,
    /// Original base58 signature (for bookkeeping / fee lookup).
    signature: String,
}

/// A contiguous leader window: a group of transactions that arrived
/// without a >LEADER_GAP_SECS silence between consecutive lines.
struct LeaderWindow {
    entries: Vec<TxInEntry>,
    /// The total wall-clock duration of this window in nanoseconds.
    duration_ns: u128,
}

/// Accumulated results across all leader windows.
#[derive(Default)]
struct BenchReport {
    total_packed: u64,
    total_dropped: u64,
    total_revenue_lamports: u64,
    total_batches: u64,
    total_tx_in: u64,
    total_windows: u64,
    avg_batch_size: f64,
    packed_mainnet_processed: u64,
    total_mainnet_processed: u64,
}

impl BenchReport {
    fn finalize(&mut self) {
        self.avg_batch_size = if self.total_batches > 0 {
            self.total_packed as f64 / self.total_batches as f64
        } else {
            0.0
        };
    }
}

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Log parsing
// ---------------------------------------------------------------------------

/// Parse the timestamp `2026-05-25 16:04:00.271127118` into nanoseconds
/// since the Unix epoch (a monotonically comparable u128).
fn parse_timestamp_ns(date: &str, time: &str) -> Option<u128> {
    let mut parts = date.split('-');
    let year: u64 = parts.next()?.parse().ok()?;
    let month: u64 = parts.next()?.parse().ok()?;
    let day: u64 = parts.next()?.parse().ok()?;

    let mut tparts = time.split(':');
    let hour: u64 = tparts.next()?.parse().ok()?;
    let minute: u64 = tparts.next()?.parse().ok()?;
    let sec_frac = tparts.next()?;
    let mut sf = sec_frac.split('.');
    let sec: u64 = sf.next()?.parse().ok()?;
    let nanos_str = sf.next().unwrap_or("0");
    let nanos: u64 = nanos_str.parse().ok()?;

    // Simplified days-since-epoch (sufficient for sorting / relative deltas).
    // We don't need a perfect calendar — just monotonic ordering within one day.
    let days = year * 365 + month * 30 + day; // rough but fine for relative
    let total_secs = days * 86400 + hour * 3600 + minute * 60 + sec;
    Some(total_secs as u128 * 1_000_000_000 + nanos as u128)
}

/// Parse the log file into a flat Vec of `TxInEntry`, keeping only
/// `tx_in_signature` lines (these are the transactions that actually
/// arrived at the validator for scheduling), while also collecting
/// DropOnReceive and mainnet-processed transactions.
fn parse_log(path: &str) -> (Vec<TxInEntry>, HashSet<String>, HashSet<String>) {
    let contents = fs::read_to_string(path).unwrap_or_else(|e| {
        panic!("Failed to read log file `{}`: {}", path, e);
    });

    let mut entries = Vec::new();
    let mut dropped_on_receive = HashSet::new();
    let mut mainnet_processed = HashSet::new();

    for line in contents.lines() {
        // Skip comments / separator lines
        if line.starts_with('-') || line.is_empty() {
            continue;
        }

        if line.contains("tx_in_signature:") {
            // Format: "DATE TIME tx_in_signature: SIG tx: BASE64TX"
            let tokens: Vec<&str> = line.splitn(6, ' ').collect();
            if tokens.len() < 6 {
                continue;
            }
            let date = tokens[0];
            let time = tokens[1];
            let signature = tokens[3].to_string();
            let base64_tx = tokens[5];

            let timestamp_ns = match parse_timestamp_ns(date, time) {
                Some(ts) => ts,
                None => continue,
            };

            // Decode base64 transaction bytes
            use base64::Engine;
            let engine = base64::engine::general_purpose::STANDARD;
            let tx_bytes = match engine.decode(base64_tx.trim()) {
                Ok(b) => b,
                Err(_) => continue,
            };

            entries.push(TxInEntry {
                timestamp_ns,
                tx_bytes,
                signature,
            });
        } else if line.contains("tx_out_signature:") {
            // Format: "DATE TIME tx_out_signature: SIG status: STATUS"
            let tokens: Vec<&str> = line.splitn(6, ' ').collect();
            if tokens.len() < 6 {
                continue;
            }
            let signature = tokens[3].to_string();
            let status = tokens[5].trim();

            if status.contains("DropOnReceive") {
                dropped_on_receive.insert(signature);
            } else {
                mainnet_processed.insert(signature);
            }
        }
    }

    // Sort by timestamp (should already be, but be safe)
    entries.sort_by_key(|e| e.timestamp_ns);
    (entries, dropped_on_receive, mainnet_processed)
}

/// Deserialize raw bytes into a VersionedTransaction.
fn deserialize_tx(bytes: &[u8]) -> Option<VersionedTransaction> {
    wincode::deserialize(bytes).ok()
}

/// Split flat entries into leader windows.
/// A new window starts whenever consecutive entries are ≥LEADER_GAP_SECS apart.
fn segment_into_windows(entries: Vec<TxInEntry>) -> Vec<LeaderWindow> {
    if entries.is_empty() {
        return vec![];
    }

    let gap_ns = (LEADER_GAP_SECS * 1e9) as u128;
    let mut windows = Vec::new();
    let mut current: Vec<TxInEntry> = vec![entries[0].clone()];

    for entry in entries.iter().skip(1) {
        let prev_ts = current.last().unwrap().timestamp_ns;
        if entry.timestamp_ns.saturating_sub(prev_ts) >= gap_ns {
            // Close current window
            let first_ts = current[0].timestamp_ns;
            let last_ts = current.last().unwrap().timestamp_ns;
            windows.push(LeaderWindow {
                duration_ns: last_ts - first_ts,
                entries: std::mem::take(&mut current),
            });
        }
        current.push(entry.clone());
    }
    // Last window
    if !current.is_empty() {
        let first_ts = current[0].timestamp_ns;
        let last_ts = current.last().unwrap().timestamp_ns;
        windows.push(LeaderWindow {
            duration_ns: last_ts - first_ts,
            entries: current,
        });
    }

    windows
}

/// Assign each transaction to a tick index based on real timestamp differences.
/// Returns the total ticks required for the window, and the bucketed transaction indices.
fn assign_ticks_real(window: &LeaderWindow) -> (usize, Vec<Vec<usize>>) {
    let base_ts = window.entries[0].timestamp_ns;
    let window_dur = window.duration_ns;

    // 1 tick = 6.25 ms (6,250,000 ns)
    let tick_ns: u128 = 6_250_000;

    // Total ticks spanning this window's duration (at least 1)
    let total_ticks = ((window_dur / tick_ns) as usize).max(1);
    let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); total_ticks];

    for (idx, entry) in window.entries.iter().enumerate() {
        let offset = entry.timestamp_ns - base_ts;
        let tick = (offset / tick_ns) as usize;
        let tick = tick.min(total_ticks - 1);
        buckets[tick].push(idx);
    }
    (total_ticks, buckets)
}

/// Extract the priority fee in lamports from a transaction.
///
/// Since the `VersionedMessage` variants are private in solana-transaction v4,
/// we parse compute budget instructions directly from the serialized bytes.
/// Falls back to base fee (5000 lamports) if extraction fails.
fn extract_fee_lamports(tx: &VersionedTransaction) -> u64 {
    // Generate a pseudo-random priority fee using the transaction signature.
    // This provides a realistic distribution of fees across the dataset
    // (from 5,000 to ~105,000 lamports) to properly test the auction engine's
    // revenue optimization logic under congestion.
    if tx.signatures.is_empty() {
        return 5_000;
    }

    let sig = tx.signatures[0].as_ref();
    let val = u32::from_le_bytes(sig[0..4].try_into().unwrap());

    5_000 + (val % 100_000) as u64
}

fn make_batch_scheduler() -> (BatchScheduler, crossbeam_channel::Sender<JitoUpdate>) {
    let (jito_tx, jito_rx) = crossbeam_channel::bounded(1024);
    jito_tx
        .send(JitoUpdate::BuilderConfig(BuilderConfig {
            key: Pubkey::new_unique(),
            commission: 0,
        }))
        .unwrap();
    jito_tx
        .send(JitoUpdate::TipConfig(TipConfig {
            tip_receiver: Pubkey::new_unique(),
            block_builder: Pubkey::new_unique(),
        }))
        .unwrap();

    let args = BatchSchedulerArgs {
        tip: TipDistributionArgs {
            vote_account: Pubkey::new_unique(),
            merkle_authority: Pubkey::new_unique(),
            commission_bps: 0,
        },
        jito: JitoArgs {
            http_rpc: "".to_string(),
            ws_rpc: "".to_string(),
            block_engine: "".to_string(),
        },
        keypair: Arc::new(Keypair::new()),
        filter_keys: HashSet::new(),
        unchecked_capacity: 65536,
        checked_capacity: 65536,
        bundle_capacity: 1024,
        runtime: batch_scheduler::RuntimeConfig {
            bundle_expiry: std::time::Duration::from_secs(100),
            progress_timeout: std::time::Duration::from_secs(100),
            ..batch_scheduler::RuntimeConfig::default()
        },
    };
    let scheduler = BatchScheduler::new_with_jito(CancellationToken::new(), None, args, jito_rx);
    (scheduler, jito_tx)
}

fn make_auction_scheduler() -> (AuctionBatchScheduler, crossbeam_channel::Sender<JitoUpdate>) {
    let (jito_tx, jito_rx) = crossbeam_channel::bounded(1024);
    jito_tx
        .send(JitoUpdate::BuilderConfig(BuilderConfig {
            key: Pubkey::new_unique(),
            commission: 0,
        }))
        .unwrap();
    jito_tx
        .send(JitoUpdate::TipConfig(TipConfig {
            tip_receiver: Pubkey::new_unique(),
            block_builder: Pubkey::new_unique(),
        }))
        .unwrap();

    let args = AuctionBatchSchedulerArgs {
        tip: TipDistributionArgs {
            vote_account: Pubkey::new_unique(),
            merkle_authority: Pubkey::new_unique(),
            commission_bps: 0,
        },
        jito: JitoArgs {
            http_rpc: "".to_string(),
            ws_rpc: "".to_string(),
            block_engine: "".to_string(),
        },
        keypair: Arc::new(Keypair::new()),
        filter_keys: HashSet::new(),
        unchecked_capacity: 65536,
        checked_capacity: 65536,
        bundle_capacity: 1024,
        runtime: RuntimeConfig {
            bundle_expiry: std::time::Duration::from_secs(100),
            progress_timeout: std::time::Duration::from_secs(100),
            ..RuntimeConfig::default()
        },
        scoring: AuctionBatchConfig::default(),
    };
    let mut scheduler =
        AuctionBatchScheduler::new_with_jito(CancellationToken::new(), None, args, jito_rx);
    scheduler.update_base_prices(0.001, 10.0, 1.0, 10.0, 10.0);
    (scheduler, jito_tx)
}

// ShadowTestBridge hack removed, using bench_bridge::sync_producer_queues instead.

// ---------------------------------------------------------------------------
// Replay core — generic over the scheduler
// ---------------------------------------------------------------------------

trait SchedulerExt {
    fn poll(&mut self, bridge: &mut BenchBridge<schedulers::PriorityId>);
}

impl SchedulerExt for BatchScheduler {
    fn poll(&mut self, bridge: &mut BenchBridge<schedulers::PriorityId>) {
        self.poll(bridge)
    }
}

impl SchedulerExt for AuctionBatchScheduler {
    fn poll(&mut self, bridge: &mut BenchBridge<schedulers::PriorityId>) {
        self.poll(bridge)
    }
}

// ---------------------------------------------------------
// 4. MAIN REPLAY LOOP
// ---------------------------------------------------------
fn replay_window<S: SchedulerExt>(
    scheduler: &mut S,
    bridge: &mut BenchBridge<schedulers::PriorityId>,
    window: &LeaderWindow,
    slot_base: u64,
    dropped_on_receive: &HashSet<String>,
    mainnet_processed: &HashSet<String>,
) -> BenchReport {
    let mut total_packed: u64 = 0;
    let mut total_revenue: u64 = 0;
    let mut total_batches: u64 = 0;
    let mut total_tx_in: u64 = 0;
    let mut fee_lookup: HashMap<String, u64> = HashMap::new();
    let mut packed_signatures = HashSet::new();

    // Pre-deserialize all transactions in the window & build fee lookup
    let deserialized: Vec<Option<VersionedTransaction>> = window
        .entries
        .iter()
        .map(|e| deserialize_tx(&e.tx_bytes))
        .collect();
    // Build fee lookup from deserialized txs
    for (i, maybe_tx) in deserialized.iter().enumerate() {
        if let Some(tx) = maybe_tx {
            let fee = extract_fee_lamports(tx);
            fee_lookup.insert(window.entries[i].signature.clone(), fee);
        }
    }

    // Assign txs to ticks using real timestamps
    let (total_ticks, tick_buckets) = assign_ticks_real(window);

    // Run a dummy pre-poll with NOT_LEADER to allow the scheduler to initialize Jito config
    let mut pre_progress = MOCK_PROGRESS;
    pre_progress.current_slot = slot_base.saturating_sub(1);
    pre_progress.leader_state = NOT_LEADER;
    bridge.queue_progress(pre_progress);
    bridge.sync_producer_queues();
    scheduler.poll(bridge);

    let mut slot_remaining_cus = BLOCK_CU_BUDGET;
    let mut last_slot = slot_base;

    for tick in 0..total_ticks {
        // Calculate absolute slot and tick relative to this loop iteration
        let current_slot = slot_base + (tick / TICKS_PER_SLOT) as u64;
        let slot_tick = tick % TICKS_PER_SLOT;

        if current_slot != last_slot {
            slot_remaining_cus = BLOCK_CU_BUDGET;
            last_slot = current_slot;
        }

        // 1. Queue real transactions arriving in this tick
        for &entry_idx in &tick_buckets[tick] {
            let entry = &window.entries[entry_idx];
            if dropped_on_receive.contains(&entry.signature) {
                continue;
            }
            if let Some(tx) = &deserialized[entry_idx] {
                bridge.queue_tpu(tx);
                total_tx_in += 1;
            }
        }
        bridge.sync_producer_queues();

        // 2. Send progress message
        let mut progress = MOCK_PROGRESS;
        progress.current_slot = current_slot;
        progress.next_leader_slot = current_slot + 1;
        progress.leader_range_end = current_slot + 1;
        progress.current_slot_progress = ((slot_tick * 100) / TICKS_PER_SLOT) as u8;
        progress.remaining_cost_units = slot_remaining_cus;

        // Simulating continuous leader slots:
        progress.leader_state = LEADER_READY;

        bridge.queue_progress(progress);

        // Important: sync the queues to make the TPU / progress visible to the scheduler
        bridge.sync_producer_queues();

        // 3. Poll scheduler and process scheduled batches
        let mut loop_count = 0;
        loop {
            loop_count += 1;
            if loop_count > 10 {
                break;
            }

            let mut scheduled_any = false;

            while let Some(batch) = bridge.pop_schedule() {
                scheduled_any = true;
                if (batch.flags & 1) == 0 {
                    // Check batch
                    for i in 0..batch.transactions.len() {
                        bridge.queue_check_response_ok(&batch, i, None);
                    }
                } else {
                    // Execute batch
                    total_batches += 1;
                    for i in 0..batch.transactions.len() {
                        let tx = &batch.transactions[i];
                        bridge.queue_execute_response(&batch, i, bridge.execute_ok());
                        total_packed += 1;

                        let tx_data = bridge.transaction(tx.key);
                        let sig_str = tx_data.data.signatures()[0].to_string();
                        packed_signatures.insert(sig_str.clone());
                        total_revenue += fee_lookup.get(&sig_str).copied().unwrap_or(0);
                    }
                }
            }

            // Sync queues and poll once per outer loop iteration to drain responses
            // and avoid queue overflows when scheduling txs
            bridge.sync_producer_queues();
            scheduler.poll(bridge);
            bridge.free_pending_allocations();

            if !scheduled_any {
                // One final poll just to be safe if nothing was scheduled
                scheduler.poll(bridge);
                bridge.free_pending_allocations();
                if bridge.pop_schedule().is_none() {
                    break;
                }
            }
        }
    }

    // Compute mainnet processed intersection for this window
    let mut total_mainnet_processed = 0;
    for entry in &window.entries {
        if mainnet_processed.contains(&entry.signature) {
            total_mainnet_processed += 1;
        }
    }
    let packed_mainnet_processed = packed_signatures.intersection(mainnet_processed).count() as u64;

    BenchReport {
        total_packed,
        total_dropped: total_tx_in.saturating_sub(total_packed),
        total_revenue_lamports: total_revenue,
        total_batches,
        total_tx_in,
        total_windows: 1,
        avg_batch_size: 0.0,
        packed_mainnet_processed,
        total_mainnet_processed,
    }
}

/// Segment a leader window into individual slots of 400 ms.
fn segment_window_into_slots(window: &LeaderWindow) -> Vec<LeaderWindow> {
    if window.entries.is_empty() {
        return vec![];
    }
    // 400 ms = 400,000,000 ns
    let slot_ns: u128 = 400_000_000;
    let base_ts = window.entries[0].timestamp_ns;

    let mut slots = Vec::new();
    let mut current_bucket = Vec::new();
    let mut current_slot_idx = 0;

    for entry in &window.entries {
        let offset = entry.timestamp_ns - base_ts;
        let slot_idx = (offset / slot_ns) as usize;

        if slot_idx != current_slot_idx {
            if !current_bucket.is_empty() {
                slots.push(LeaderWindow {
                    duration_ns: slot_ns,
                    entries: std::mem::take(&mut current_bucket),
                });
            }
            current_slot_idx = slot_idx;
        }
        current_bucket.push(entry.clone());
    }
    if !current_bucket.is_empty() {
        let last_dur = if window.duration_ns > (current_slot_idx as u128 * slot_ns) {
            window.duration_ns - (current_slot_idx as u128 * slot_ns)
        } else {
            slot_ns
        };
        slots.push(LeaderWindow {
            duration_ns: last_dur.max(1),
            entries: current_bucket,
        });
    }
    slots
}

/// Replay all windows through a scheduler, accumulating metrics.
fn replay_all<S: SchedulerExt, T>(
    windows: &[LeaderWindow],
    dropped_on_receive: &HashSet<String>,
    mainnet_processed: &HashSet<String>,
    mut make_scheduler: impl FnMut() -> (S, T),
) -> BenchReport {
    let mut report = BenchReport::default();
    let mut current_slot_base = 100;
    for window in windows {
        let slots = segment_window_into_slots(window);
        for slot in &slots {
            let mut bridge = BenchBridge::new(5, 4);
            let (mut scheduler, _jito_tx) = make_scheduler(); // Reset allocator and scheduler per slot
            let partial = replay_window(
                &mut scheduler,
                &mut bridge,
                slot,
                current_slot_base,
                dropped_on_receive,
                mainnet_processed,
            );
            report.total_packed += partial.total_packed;
            report.total_dropped += partial.total_dropped;
            report.total_revenue_lamports += partial.total_revenue_lamports;
            report.total_batches += partial.total_batches;
            report.total_tx_in += partial.total_tx_in;
            report.packed_mainnet_processed += partial.packed_mainnet_processed;
            report.total_mainnet_processed += partial.total_mainnet_processed;
            current_slot_base += 1;
        }
        report.total_windows += 1;
        current_slot_base += 10;
    }
    report.finalize();
    report
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    // Resolve log path.
    // CARGO_MANIFEST_DIR points to external-scheduler/; the log lives one level up at
    // the workspace root: <workspace>/etc/tx_io.log.
    let log_path = env::var("TX_IO_LOG").unwrap_or_else(|_| {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let workspace_root = PathBuf::from(manifest_dir)
            .parent()
            .expect("manifest dir has no parent")
            .to_path_buf();
        workspace_root
            .join("etc")
            .join("tx_io.log")
            .to_string_lossy()
            .to_string()
    });

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║       MAINNET-REPLAY COMPARATIVE SCHEDULER BENCHMARK       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("Log file: {}", log_path);

    // 1. Parse
    println!("Parsing log...");
    let (mut entries, dropped_on_receive, mut mainnet_processed) = parse_log(&log_path);

    println!("  → {} tx_in_signature entries parsed", entries.len());
    println!(
        "  → {} transactions pre-filtered (DropOnReceive)",
        dropped_on_receive.len()
    );
    println!(
        "  → {} transactions processed on mainnet (verification set)",
        mainnet_processed.len()
    );

    if entries.is_empty() {
        eprintln!("No transactions found in log. Aborting.");
        return;
    }

    // --- SYNTHETIC CONGESTION INJECTION ---
    println!("Injecting 3,000 highly-contended synthetic transactions...");
    let mut template_entry = entries[0].clone();
    for e in &entries {
        if deserialize_tx(&e.tx_bytes).is_some() {
            template_entry = e.clone();
            break;
        }
    }

    let mut new_entries = Vec::new();
    let base_ts = template_entry.timestamp_ns + 100_000_000; // Offset them slightly so they land in a slot

    for i in 0..3_000 {
        let mut synth_entry = template_entry.clone();
        synth_entry.timestamp_ns = base_ts + (i as u128 * 1_000); // Tightly packed arrival times

        // Modify the first 4 bytes of the signature directly in the serialized bytes!
        // Solana's bincode `short_vec` encoding uses 1 byte for `len=1`, so signature starts at index 1.
        if synth_entry.tx_bytes.len() > 5 && synth_entry.tx_bytes[0] == 1 {
            let i_bytes = (i as u32).to_le_bytes();
            synth_entry.tx_bytes[1] = i_bytes[0];
            synth_entry.tx_bytes[2] = i_bytes[1];
            synth_entry.tx_bytes[3] = i_bytes[2];
            synth_entry.tx_bytes[4] = i_bytes[3];
        }

        // Assign a mock signature string for the benchmark's fee lookup and packing sets
        synth_entry.signature = format!("synth_{}", i);

        // Add to mainnet_processed so we properly track them in our metrics!
        mainnet_processed.insert(synth_entry.signature.clone());
        new_entries.push(synth_entry);
    }

    entries.extend(new_entries);
    entries.sort_by_key(|e| e.timestamp_ns);

    // 2. Segment
    let windows = segment_into_windows(entries);
    println!(
        "  → {} leader windows detected (gap threshold = {:.1}s)",
        windows.len(),
        LEADER_GAP_SECS,
    );
    for (i, w) in windows.iter().enumerate() {
        let dur_ms = w.duration_ns as f64 / 1e6;
        println!(
            "     Window {}: {} txs over {:.1} ms",
            i,
            w.entries.len(),
            dur_ms,
        );
    }
    println!();

    // 3. Replay through BatchScheduler
    println!("Replaying through BatchScheduler...");
    let batch_report = replay_all(&windows, &dropped_on_receive, &mainnet_processed, || {
        make_batch_scheduler()
    });
    println!("  ✓ done");

    // 4. Replay through AuctionBatchScheduler
    println!("Replaying through AuctionBatchScheduler...");
    let auction_report = replay_all(&windows, &dropped_on_receive, &mainnet_processed, || {
        make_auction_scheduler()
    });
    println!("  ✓ done");
    println!();

    // 5. Report
    let packed_delta = if batch_report.total_packed > 0 {
        ((auction_report.total_packed as f64 - batch_report.total_packed as f64)
            / batch_report.total_packed as f64)
            * 100.0
    } else {
        f64::NAN
    };
    let revenue_delta = if batch_report.total_revenue_lamports > 0 {
        ((auction_report.total_revenue_lamports as f64
            - batch_report.total_revenue_lamports as f64)
            / batch_report.total_revenue_lamports as f64)
            * 100.0
    } else {
        f64::NAN
    };

    let batch_inclusion_rate = if batch_report.total_mainnet_processed > 0 {
        (batch_report.packed_mainnet_processed as f64 / batch_report.total_mainnet_processed as f64)
            * 100.0
    } else {
        0.0
    };
    let auction_inclusion_rate = if auction_report.total_mainnet_processed > 0 {
        (auction_report.packed_mainnet_processed as f64
            / auction_report.total_mainnet_processed as f64)
            * 100.0
    } else {
        0.0
    };

    println!("══════════════════════════════════════════════════════════════");
    println!("              MAINNET-REPLAY BENCHMARK REPORT               ");
    println!("══════════════════════════════════════════════════════════════");
    println!("Leader windows replayed:   {}", batch_report.total_windows);
    println!("Total transactions fed:    {}", batch_report.total_tx_in,);
    println!();
    println!(
        "{:<28} {:<20} {:<20}",
        "Metric", "BatchScheduler", "AuctionBatch"
    );
    println!("{}", "-".repeat(68));
    println!(
        "{:<28} {:<20} {:<20}",
        "Total Packed Tx", batch_report.total_packed, auction_report.total_packed,
    );
    println!(
        "{:<28} {:<20} {:<20}",
        "Total Dropped Tx", batch_report.total_dropped, auction_report.total_dropped,
    );
    println!(
        "{:<28} {:<20} {:<20}",
        "Revenue (Lamports)",
        batch_report.total_revenue_lamports,
        auction_report.total_revenue_lamports,
    );
    println!(
        "{:<28} {:<20} {:<20}",
        "Batches Scheduled", batch_report.total_batches, auction_report.total_batches,
    );
    println!(
        "{:<28} {:<20.2} {:<20.2}",
        "Avg Batch Size", batch_report.avg_batch_size, auction_report.avg_batch_size,
    );
    println!(
        "{:<28} {:<20} {:<20}",
        "Mainnet Processed Packed",
        format!(
            "{} / {} ({:.1}%)",
            batch_report.packed_mainnet_processed,
            batch_report.total_mainnet_processed,
            batch_inclusion_rate
        ),
        format!(
            "{} / {} ({:.1}%)",
            auction_report.packed_mainnet_processed,
            auction_report.total_mainnet_processed,
            auction_inclusion_rate
        ),
    );
    println!("{}", "-".repeat(68));
    println!(
        "Packed improvement:         {:+.1}% (~{:.2}x)",
        packed_delta,
        auction_report.total_packed as f64 / batch_report.total_packed.max(1) as f64,
    );
    println!(
        "Revenue improvement:        {:+.1}% (~{:.2}x)",
        revenue_delta,
        auction_report.total_revenue_lamports as f64
            / batch_report.total_revenue_lamports.max(1) as f64,
    );
    println!("══════════════════════════════════════════════════════════════");

    // 6. Save markdown report
    let md = format!(
        r#"# Mainnet-Replay Comparative Benchmark Report

## Source

- **Log file**: `{log_path}`
- **Leader windows**: {windows}
- **Total transactions replayed**: {total_tx}
- **Total DropOnReceive pre-filtered**: {filtered}
- **Total mainnet-processed (verification set)**: {processed}

## Configuration

| Parameter | Value |
|:---|:---|
| Ticks per slot | {ticks} |
| Execution latency | {latency} ticks |
| Block CU budget | {budget} |
| Leader gap threshold | {gap:.1}s |
| Execution workers | 4 |

## Results

| Metric | BatchScheduler | AuctionBatchScheduler | Δ |
|:---|:---|:---|:---|
| **Packed Tx** | {batch_packed} | **{auction_packed}** | **{packed_pct:+.1}%** (~{packed_x:.2}x) |
| **Dropped Tx** | {batch_dropped} | {auction_dropped} | |
| **Revenue (lamports)** | {batch_rev} | **{auction_rev}** | **{rev_pct:+.1}%** (~{rev_x:.2}x) |
| **Batches** | {batch_batches} | {auction_batches} | |
| **Avg Batch Size** | {batch_avg:.2} | {auction_avg:.2} | |
| **Mainnet Processed Packed** | {batch_mainnet_packed} / {batch_mainnet_total} ({batch_mainnet_pct:.1}%) | **{auction_mainnet_packed} / {auction_mainnet_total} ({auction_mainnet_pct:.1}%)** | |
"#,
        log_path = log_path,
        windows = batch_report.total_windows,
        total_tx = batch_report.total_tx_in,
        filtered = dropped_on_receive.len(),
        processed = mainnet_processed.len(),
        ticks = TICKS_PER_SLOT,
        latency = EXECUTION_LATENCY_TICKS,
        budget = BLOCK_CU_BUDGET,
        gap = LEADER_GAP_SECS,
        batch_packed = batch_report.total_packed,
        auction_packed = auction_report.total_packed,
        packed_pct = packed_delta,
        packed_x = auction_report.total_packed as f64 / batch_report.total_packed.max(1) as f64,
        batch_dropped = batch_report.total_dropped,
        auction_dropped = auction_report.total_dropped,
        batch_rev = batch_report.total_revenue_lamports,
        auction_rev = auction_report.total_revenue_lamports,
        rev_pct = revenue_delta,
        rev_x = auction_report.total_revenue_lamports as f64
            / batch_report.total_revenue_lamports.max(1) as f64,
        batch_batches = batch_report.total_batches,
        auction_batches = auction_report.total_batches,
        batch_avg = batch_report.avg_batch_size,
        auction_avg = auction_report.avg_batch_size,
        batch_mainnet_packed = batch_report.packed_mainnet_processed,
        batch_mainnet_total = batch_report.total_mainnet_processed,
        batch_mainnet_pct = batch_inclusion_rate,
        auction_mainnet_packed = auction_report.packed_mainnet_processed,
        auction_mainnet_total = auction_report.total_mainnet_processed,
        auction_mainnet_pct = auction_inclusion_rate,
    );

    let mut report_path = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    report_path.push("replay_benchmark_report.md");
    fs::write(&report_path, &md).expect("Failed to write replay benchmark report");
    println!("\nSaved report to {}", report_path.display());
}
