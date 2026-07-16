use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::hash::{Hash as StdHash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use agave_scheduler_bindings::{LEADER_READY, NOT_LEADER, ProgressMessage};
use agave_scheduling_utils::bridge::TestBridge;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_hash::Hash;
use solana_keypair::{Keypair, Signer};
use solana_pubkey::Pubkey;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction::{AccountMeta, Instruction, Transaction};

use schedulers::jito::jito_thread::{BuilderConfig, JitoArgs, JitoUpdate, TipConfig};
use schedulers::jito::tip_program::TipDistributionArgs;

use auction_batch_scheduler::{
    AuctionBatchConfig, AuctionBatchScheduler, AuctionBatchSchedulerArgs, RuntimeConfig,
};
use batch_scheduler::{BatchScheduler, BatchSchedulerArgs};
use tokio_util::sync::CancellationToken;

const MOCK_PROGRESS: ProgressMessage = ProgressMessage {
    leader_state: NOT_LEADER,
    current_slot: 10,
    next_leader_slot: 11,
    leader_range_end: 11,
    remaining_cost_units: 48_000_000,
    current_slot_progress: 0,
    epoch: 0,
    latest_blockhash: [0; 32],
};

struct BenchReport {
    total_packed: u64,
    total_revenue_lamports: u64,
    total_batches: u64,
    avg_batch_size: f64,
}

fn noop_with_budget(
    payer: &Keypair,
    cu_limit: u32,
    cu_price: u64,
    locks: &[(Pubkey, bool)],
) -> VersionedTransaction {
    let mut metas = Vec::new();
    for &(k, w) in locks {
        if w {
            metas.push(AccountMeta::new(k, false));
        } else {
            metas.push(AccountMeta::new_readonly(k, false));
        }
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
        Hash::default(),
    )
    .into()
}

fn generate_tick_transactions(
    tick: usize,
    fee_lookup: &mut HashMap<String, u64>,
) -> Vec<VersionedTransaction> {
    let txs_per_tick = 200;
    let mut txs = Vec::new();

    let mut state = tick as u64 + 99999;
    let mut next_random = || {
        let mut hasher = DefaultHasher::new();
        state.hash(&mut hasher);
        state = hasher.finish();
        state
    };

    for _ in 0..txs_per_tick {
        let r = next_random();
        let is_hotspot = (r % 100) < 30; // 30% hotspot transactions

        let locks = if is_hotspot {
            // Hotspot targets a single congested account (0)
            let hot_idx = 0;
            let account = Pubkey::new_from_array([
                hot_idx as u8,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ]);
            vec![(account, true)] // write lock
        } else {
            // Cold targets a unique random account (from 10 to 5000)
            let cold_idx = (r % 4990) + 10;
            let account = Pubkey::new_from_array([
                (cold_idx & 0xFF) as u8,
                ((cold_idx >> 8) & 0xFF) as u8,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ]);
            vec![(account, true)] // write lock
        };

        // Fee assignment: micro-lamports per CU
        let priority_fee = 200_000;

        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, 25_000, priority_fee, &locks);
        let sig_str = tx.signatures[0].to_string();
        fee_lookup.insert(sig_str, 5000 + 5000); // base fee + prioritization fee

        txs.push(tx);
    }
    txs
}

fn run_batch_scheduler() -> BenchReport {
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
            http_rpc: "http://localhost:1234".to_string(),
            ws_rpc: "ws://localhost:1234".to_string(),
            block_engine: "http://localhost:1234".to_string(),
        },
        keypair: Arc::new(Keypair::new()),
        filter_keys: HashSet::new(),
        unchecked_capacity: 65536,
        checked_capacity: 65536,
        bundle_capacity: 1024,
        runtime: batch_scheduler::RuntimeConfig::default(),
    };
    let mut scheduler =
        BatchScheduler::new_with_jito(CancellationToken::new(), None, args, jito_rx);
    let mut bridge = TestBridge::new(9, 8); // 8 execute workers

    let mut total_packed = 0;
    let mut total_revenue = 0;
    let mut total_batches = 0;
    let mut fee_lookup: HashMap<String, u64> = HashMap::new();

    for tick in 0..64 {
        // 1. Generate random transactions arriving in this tick
        let new_txs = generate_tick_transactions(tick, &mut fee_lookup);

        for chunk in new_txs.chunks(128) {
            for tx in chunk {
                bridge.queue_tpu(tx);
            }
        }

        // 2. Send progress tick
        let mut progress = MOCK_PROGRESS;
        progress.current_slot_progress = ((tick * 100) / 64) as u8;
        if tick >= 10 {
            progress.leader_state = LEADER_READY;
        }
        bridge.queue_progress(progress);

        // 3. Continuously poll and process scheduled transactions (multi-stage loop)
        for _ in 0..5 {
            scheduler.poll(&mut bridge);

            let mut scheduled_any = false;
            while let Some(batch) = bridge.pop_schedule() {
                scheduled_any = true;
                if (batch.flags & 1) == 0 {
                    for i in 0..batch.transactions.len() {
                        if !bridge.contains_tx(batch.transactions[i].key) {
                            continue;
                        }
                        bridge.queue_check_response_ok(&batch, i, None);
                    }
                } else {
                    total_batches += 1;
                    for i in 0..batch.transactions.len() {
                        let tx = &batch.transactions[i];
                        if !bridge.contains_tx(tx.key) {
                            continue;
                        }
                        total_packed += 1;
                        let sig_str = bridge.transaction(tx.key).data.signatures()[0].to_string();
                        total_revenue += fee_lookup.get(&sig_str).copied().unwrap_or(0);
                        bridge.queue_execute_response(&batch, i, bridge.execute_ok());
                    }
                }
            }

            if !scheduled_any {
                break;
            }
        }
    }

    BenchReport {
        total_packed,
        total_revenue_lamports: total_revenue,
        total_batches,
        avg_batch_size: if total_batches > 0 {
            total_packed as f64 / total_batches as f64
        } else {
            0.0
        },
    }
}

fn run_auction_scheduler() -> BenchReport {
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
            http_rpc: "http://localhost:1234".to_string(),
            ws_rpc: "ws://localhost:1234".to_string(),
            block_engine: "http://localhost:1234".to_string(),
        },
        keypair: Arc::new(Keypair::new()),
        filter_keys: HashSet::new(),
        unchecked_capacity: 65536,
        checked_capacity: 65536,
        bundle_capacity: 1024,
        runtime: RuntimeConfig::default(),
        scoring: AuctionBatchConfig::default(),
    };
    let mut scheduler =
        AuctionBatchScheduler::new_with_jito(CancellationToken::new(), None, args, jito_rx);
    scheduler.update_base_prices(0.001, 10.0, 1.0, 10.0, 10.0);
    let mut bridge = TestBridge::new(9, 8); // 8 execute workers

    let mut total_packed = 0;
    let mut total_revenue = 0;
    let mut total_batches = 0;
    let mut fee_lookup: HashMap<String, u64> = HashMap::new();

    for tick in 0..64 {
        // 1. Generate random transactions arriving in this tick
        let new_txs = generate_tick_transactions(tick, &mut fee_lookup);

        for chunk in new_txs.chunks(128) {
            for tx in chunk {
                bridge.queue_tpu(tx);
            }
        }

        // 2. Send progress tick
        let mut progress = MOCK_PROGRESS;
        progress.current_slot_progress = ((tick * 100) / 64) as u8;
        if tick >= 10 {
            progress.leader_state = LEADER_READY;
        }
        bridge.queue_progress(progress);

        // 3. Continuously poll and process scheduled transactions (multi-stage loop)
        for _ in 0..5 {
            scheduler.poll(&mut bridge);

            let mut scheduled_any = false;
            while let Some(batch) = bridge.pop_schedule() {
                scheduled_any = true;
                if (batch.flags & 1) == 0 {
                    for i in 0..batch.transactions.len() {
                        if !bridge.contains_tx(batch.transactions[i].key) {
                            continue;
                        }
                        bridge.queue_check_response_ok(&batch, i, None);
                    }
                } else {
                    total_batches += 1;
                    for i in 0..batch.transactions.len() {
                        let tx = &batch.transactions[i];
                        if !bridge.contains_tx(tx.key) {
                            continue;
                        }
                        total_packed += 1;
                        let sig_str = bridge.transaction(tx.key).data.signatures()[0].to_string();
                        total_revenue += fee_lookup.get(&sig_str).copied().unwrap_or(0);
                        bridge.queue_execute_response(&batch, i, bridge.execute_ok());
                    }
                }
            }

            if !scheduled_any {
                break;
            }
        }
    }

    BenchReport {
        total_packed,
        total_revenue_lamports: total_revenue,
        total_batches,
        avg_batch_size: if total_batches > 0 {
            total_packed as f64 / total_batches as f64
        } else {
            0.0
        },
    }
}

fn main() {
    println!("Starting full slot stress benchmark (64 ticks / 400ms)...");

    let batch_report = run_batch_scheduler();
    let auction_report = run_auction_scheduler();

    println!("\n==================================================");
    println!("SCHEDULER STRESS BENCHMARK REPORT");
    println!("==================================================");
    println!("Metric                    BatchScheduler    AuctionBatchScheduler");
    println!("--------------------------------------------------");
    println!(
        "Total Settled/Packed Tx   {:<17} {:<22}",
        batch_report.total_packed, auction_report.total_packed
    );
    println!(
        "Total Revenue (Lamports)  {:<17} {:<22}",
        batch_report.total_revenue_lamports, auction_report.total_revenue_lamports
    );
    println!(
        "Total Batches Scheduled   {:<17} {:<22}",
        batch_report.total_batches, auction_report.total_batches
    );
    println!(
        "Avg Batch Size            {:<17.2} {:<22.2}",
        batch_report.avg_batch_size, auction_report.avg_batch_size
    );
    println!("==================================================");

    // Save report to markdown
    let md = format!(
        r#"# Scheduler Stress Benchmark Report

## Configuration
- **Throughput**: 200 transactions/tick (total 12,800 transactions fed).
- **Latency**: 0 ticks (immediate execution feedback).
- **Workers**: 8 execute workers.
- **Contention**: 30% hotspot write locks.

| Metric | BatchScheduler | AuctionBatchScheduler | Improvement |
| :--- | :--- | :--- | :--- |
| **Total Packed Tx** | {} | **{}** | **+{:.1}%** |
| **Total Revenue (Lamports)** | {} | **{}** | **+{:.1}%** |
| **Batches Scheduled** | {} | **{}** | - |
| **Avg Batch Size** | {:.2} | **{:.2}** | - |
"#,
        batch_report.total_packed,
        auction_report.total_packed,
        (auction_report.total_packed as f64 - batch_report.total_packed as f64)
            / batch_report.total_packed as f64
            * 100.0,
        batch_report.total_revenue_lamports,
        auction_report.total_revenue_lamports,
        (auction_report.total_revenue_lamports as f64 - batch_report.total_revenue_lamports as f64)
            / batch_report.total_revenue_lamports as f64
            * 100.0,
        batch_report.total_batches,
        auction_report.total_batches,
        batch_report.avg_batch_size,
        auction_report.avg_batch_size,
    );

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let report_path = PathBuf::from(manifest_dir).join("stress_benchmark_report.md");
    fs::write(&report_path, md).unwrap();
    println!("Saved stress benchmark report to {:?}", report_path);
}
