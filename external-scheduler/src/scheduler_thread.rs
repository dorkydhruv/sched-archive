use crate::args::Args;
use crate::config_store::ConfigStore;
use agave_scheduling_utils::bridge::SchedulerBindingsBridge;
use agave_scheduling_utils::handshake::{ClientLogon, client};
use auction_batch_scheduler::{AuctionBatchScheduler, AuctionBatchSchedulerArgs};
use batch_scheduler::{BatchScheduler, BatchSchedulerArgs};
use futures::{StreamExt, stream::FuturesUnordered};
use schedulers::PriorityId;
use schedulers::events::{EventContext, EventEmitter};
use schedulers::jito::jito_thread::JitoArgs;
use schedulers::jito::tip_program::TipDistributionArgs;
use solana_keypair::{EncodableKey, Keypair};
use solana_pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::JoinHandle as StdJoinHandle;
use std::{path::PathBuf, time::Duration};
use tokio::sync::mpsc;
use tokio::task::JoinHandle as TokioJoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub(crate) struct SchedulerThread {
    shutdown: CancellationToken,
    threads: FuturesUnordered<TokioJoinHandle<std::thread::Result<()>>>,
}

impl SchedulerThread {
    pub(crate) async fn run(
        args: Args,
        config_store: ConfigStore,
        shutdown: CancellationToken,
    ) -> std::thread::Result<()> {
        let server = SchedulerThread::setup(args, config_store, shutdown).await;
        server.await_shutdown().await
    }

    async fn setup(args: Args, config_store: ConfigStore, shutdown: CancellationToken) -> Self {
        let mut threads = Vec::default();
        let events = match &config_store.read().logs_server {
            None => None,
            Some(addr) => {
                let event_ctx = EventContext::new();
                let (event_tx, mut event_rx) = mpsc::channel(1024);
                let events = EventEmitter::new(event_ctx, event_tx);

                // Spawn TCP event exporter task
                let addr = addr.clone();
                let shutdown_token = shutdown.clone();
                tokio::task::spawn(async move {
                    loop {
                        if shutdown_token.is_cancelled() {
                            break;
                        }
                        match tokio::net::TcpStream::connect(&addr).await {
                            Ok(mut stream) => {
                                use tokio::io::AsyncWriteExt;
                                info!("Logs TCP server connected to {}", addr);
                                while let Some(event) = event_rx.recv().await {
                                    if shutdown_token.is_cancelled() {
                                        break;
                                    }
                                    let mut data = match serde_json::to_vec(&event) {
                                        Ok(d) => d,
                                        Err(e) => {
                                            error!("Failed to serialize event: {}", e);
                                            continue;
                                        }
                                    };
                                    data.push(b'\n');
                                    if let Err(e) = stream.write_all(&data).await {
                                        error!("TCP stream write error: {}. Reconnecting...", e);
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to connect to logs TCP server at {}: {}. Retrying in 1s...",
                                    addr, e
                                );
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                });

                Some(events)
            }
        };

        // Load initial config from store (synchronous, no block_on needed).
        let initial_config = config_store.read();
        if let Some(auction) = &initial_config.scheduler.auction {
            let keypair = Arc::new(Keypair::read_from_file(&auction.keypair_path).unwrap());
            let (scheduler, jito_thread) = AuctionBatchScheduler::new(
                shutdown.clone(),
                events,
                AuctionBatchSchedulerArgs {
                    tip: TipDistributionArgs {
                        vote_account: Pubkey::from_str(&auction.tip.vote_account).unwrap(),
                        merkle_authority: Pubkey::from_str(&auction.tip.merkle_authority).unwrap(),
                        commission_bps: auction.tip.commission_bps,
                    },
                    jito: JitoArgs {
                        http_rpc: auction.jito.http_rpc.clone(),
                        ws_rpc: auction.jito.ws_rpc.clone(),
                        block_engine: auction.jito.block_engine.clone(),
                    },
                    keypair,
                    filter_keys: initial_config.filter_keys,
                    unchecked_capacity: auction.unchecked_capacity,
                    checked_capacity: auction.checked_capacity,
                    bundle_capacity: auction.bundle_capacity,
                    runtime: auction_batch_scheduler::RuntimeConfig {
                        max_check_batches: auction.max_check_batches as usize,
                        block_fill_cutoff: auction.block_fill_cutoff,
                        progress_timeout: Duration::from_secs(auction.progress_timeout_sec),
                        bundle_expiry: Duration::from_millis(auction.bundle_expiry_ms),
                    },
                    scoring: auction
                        .scoring
                        .as_ref()
                        .map(|s| auction_batch_scheduler::AuctionBatchConfig {
                            min_score: s.min_score,
                        })
                        .unwrap_or_default(),
                },
            );

            threads.push(crate::scheduler_thread::spawn(
                shutdown.clone(),
                args.bindings_ipc,
                config_store.clone(),
                scheduler,
                5,
            ));
            threads.push(jito_thread);
        } else if let Some(batch) = &initial_config.scheduler.batch {
            let keypair = Arc::new(Keypair::read_from_file(&batch.keypair_path).unwrap());
            let (scheduler, jito_thread) = BatchScheduler::new(
                shutdown.clone(),
                events,
                BatchSchedulerArgs {
                    tip: TipDistributionArgs {
                        vote_account: Pubkey::from_str(&batch.tip.vote_account).unwrap(),
                        merkle_authority: Pubkey::from_str(&batch.tip.merkle_authority).unwrap(),
                        commission_bps: batch.tip.commission_bps,
                    },
                    jito: JitoArgs {
                        http_rpc: batch.jito.http_rpc.clone(),
                        ws_rpc: batch.jito.ws_rpc.clone(),
                        block_engine: batch.jito.block_engine.clone(),
                    },
                    keypair,
                    filter_keys: initial_config.filter_keys,
                    unchecked_capacity: batch.unchecked_capacity,
                    checked_capacity: batch.checked_capacity,
                    bundle_capacity: batch.bundle_capacity,
                    runtime: batch_scheduler::RuntimeConfig {
                        max_check_batches: batch.max_check_batches as usize,
                        block_fill_cutoff: batch.block_fill_cutoff,
                        progress_timeout: Duration::from_secs(batch.progress_timeout_sec),
                        bundle_expiry: Duration::from_millis(batch.bundle_expiry_ms),
                    },
                },
            );

            threads.push(crate::scheduler_thread::spawn(
                shutdown.clone(),
                args.bindings_ipc,
                config_store.clone(),
                scheduler,
                5,
            ));
            threads.push(jito_thread);
        } else {
            panic!("No scheduler configuration found (either Batch or Auction must be present)");
        }

        // Use tokio to listen on all thread exits concurrently.
        let threads = threads
            .into_iter()
            .map(|thread| {
                let name = thread.thread().name().unwrap().to_string();
                info!(name, "Thread spawned");

                tokio::task::spawn_blocking(move || thread.join())
            })
            .collect();

        SchedulerThread { shutdown, threads }
    }

    async fn await_shutdown(mut self) -> std::thread::Result<()> {
        let mut exit = tokio::select! {
           () = self.shutdown.cancelled() => Ok(()),

           opt = self.threads.next() => {
              match opt.unwrap() {
                  Ok(Ok(())) => {
                      error!("Thread exited unexpectedly");
                      let error: Box<dyn std::any::Any + Send> = Box::new("Thread exited unexpectedly");
                      Err(error)
                    }
                  Ok(Err(panic)) => Err(panic),
                  Err(join_error) => {
                      let error: Box<dyn std::any::Any + Send> = Box::new(join_error);
                      Err(error)
                    }
                }
            }
        };

        // Trigger shutdown.
        self.shutdown.cancel();

        // Wait for all threads to exit, reporting the first error as the ultimate
        // error.
        while let Some(thread) = self.threads.next().await {
            info!(?thread, "Thread exited");
            exit = exit.and(thread.unwrap());
        }

        exit
    }
}

pub(crate) fn spawn<S>(
    shutdown: CancellationToken,
    bindings_ipc: PathBuf,
    config_store: ConfigStore,
    mut scheduler: S,
    worker_threads: usize,
) -> StdJoinHandle<()>
where
    S: Scheduler + Send + 'static,
{
    std::thread::Builder::new()
        .name("Scheduler".to_string())
        .spawn(move || {
            let session = client::connect(
                bindings_ipc,
                ClientLogon {
                    worker_count: worker_threads,
                    allocator_size: 2 * 1024 * 1024 * 1204,
                    allocator_handles: 1,
                    tpu_to_pack_capacity: 2usize.pow(16),
                    progress_tracker_capacity: 128,
                    pack_to_worker_capacity: 128,
                    worker_to_pack_capacity: 256,
                    flags: 0,
                },
                Duration::from_secs(1),
            )
            .unwrap();
            let mut bridge = SchedulerBindingsBridge::new(session);

            while !shutdown.is_cancelled() {
                scheduler.poll(&mut bridge, &config_store);
            }
        })
        .unwrap()
}

pub(crate) trait Scheduler
where
    Self: Sized + 'static,
{
    type Meta: Copy;

    fn poll(
        &mut self,
        bridge: &mut SchedulerBindingsBridge<Self::Meta>,
        config_store: &ConfigStore,
    );
}

impl Scheduler for BatchScheduler {
    type Meta = PriorityId;

    fn poll(
        &mut self,
        bridge: &mut SchedulerBindingsBridge<Self::Meta>,
        config_store: &ConfigStore,
    ) {
        // Read runtime config from the shared store each poll cycle (synchronous, no block_on needed)
        let runtime_config = config_store.read();
        // Apply runtime-tunable config updates to the scheduler
        if let Some(batch_config) = &runtime_config.scheduler.batch {
            self.set_runtime_config(
                batch_config.unchecked_capacity,
                batch_config.checked_capacity,
                batch_config.bundle_capacity,
                batch_config.block_fill_cutoff,
                batch_config.max_check_batches as usize,
                Duration::from_millis(batch_config.bundle_expiry_ms),
                Duration::from_secs(batch_config.progress_timeout_sec),
            );
        }

        self.poll(bridge);
    }
}

impl Scheduler for AuctionBatchScheduler {
    type Meta = PriorityId;

    fn poll(
        &mut self,
        bridge: &mut SchedulerBindingsBridge<Self::Meta>,
        config_store: &ConfigStore,
    ) {
        // Read runtime config from the shared store each poll cycle (synchronous, no block_on needed)
        let runtime_config = config_store.read();
        // Apply runtime-tunable config updates to the scheduler
        if let Some(auction_config) = &runtime_config.scheduler.auction {
            self.set_runtime_config(
                auction_config.unchecked_capacity,
                auction_config.checked_capacity,
                auction_config.bundle_capacity,
                auction_config.block_fill_cutoff,
                auction_config.max_check_batches as usize,
                Duration::from_millis(auction_config.bundle_expiry_ms),
                Duration::from_secs(auction_config.progress_timeout_sec),
            );
        }

        self.poll(bridge);
    }
}
