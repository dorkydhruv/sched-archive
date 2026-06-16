use crate::args::Args;
use crate::config_store::{ConfigStore, SchedulerConfigData};
use agave_scheduling_utils::bridge::SchedulerBindingsBridge;
use agave_scheduling_utils::handshake::{ClientLogon, client};
use futures::{StreamExt, stream::FuturesUnordered};
use batch_scheduler::{BatchScheduler, BatchSchedulerArgs};
use tighter_batch_scheduler::{TighterBatchScheduler, TighterBatchSchedulerArgs};
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
        // Spawn metrics & events publishers (if NATS is configured).
        let mut threads = Vec::default();
        // The events publisher is only spawned if NATS is configured, otherwise we just pass None to the schedulers.
        // We'll worry about this later when we setup the web interface
        let events = match config_store.read().logs_server.is_empty() {
            true => None,
            false => {
                // let nats_client = Box::leak(Box::new(
                //     metrics_nats_exporter::async_nats::connect(config.nats_servers)
                //          .await
                //          .expect("NATS Client Connect"),
                //  ));
                // threads.push(
                //     metrics_nats_exporter::install(
                //         shutdown.token.clone(),
                //         metrics_nats_exporter::Config {
                //             interval_min: Duration::from_millis(50),
                //             interval_max: Duration::from_millis(1000),
                //             metric_prefix: Some(format!("metric.scheduler.{}", config.host_name)),
                //          },
                //         nats_client,
                //      )
                //      .unwrap(),
                //  );

                // Spawn events publisher.
                let event_ctx = EventContext::new();
                // The event receiver should be used to generate the analysis over our web server backend
                let (event_tx, _event_rx) = mpsc::channel(1024);
                let events = EventEmitter::new(event_ctx, event_tx);
                // threads.push(EventsThread::spawn(event_rx, nats_client, &config.host_name));

                Some(events)
            }
        };

        // Load initial config from store (synchronous, no block_on needed).
        let initial_config = config_store.read();
        match initial_config.scheduler {
            SchedulerConfigData::BatchScheduler(batch) => {
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
                            http_rpc: batch.jito.http_rpc,
                            ws_rpc: batch.jito.ws_rpc,
                            block_engine: batch.jito.block_engine,
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
            }
            // add more schedulers here as needed
             SchedulerConfigData::TighterBatchScheduler(tighter_batch) => {
                let keypair = Arc::new(Keypair::read_from_file(&tighter_batch.keypair_path).unwrap());
                let (scheduler, jito_thread) = TighterBatchScheduler::new(
                    shutdown.clone(),
                    events,
                    TighterBatchSchedulerArgs {
                        tip: TipDistributionArgs {
                            vote_account: Pubkey::from_str(&tighter_batch.tip.vote_account).unwrap(),
                            merkle_authority: Pubkey::from_str(&tighter_batch.tip.merkle_authority).unwrap(),
                            commission_bps: tighter_batch.tip.commission_bps,
                         },
                        jito: JitoArgs {
                            http_rpc: tighter_batch.jito.http_rpc,
                            ws_rpc: tighter_batch.jito.ws_rpc,
                            block_engine: tighter_batch.jito.block_engine,
                         },
                        keypair,
                        filter_keys: initial_config.filter_keys,
                        unchecked_capacity: tighter_batch.unchecked_capacity,
                        checked_capacity: tighter_batch.checked_capacity,
                        bundle_capacity: tighter_batch.bundle_capacity,
                        scoring: schedulers::tighter_batch::TighterBatchConfig {
                            weight_fee: tighter_batch.weight_fee,
                            weight_efficiency: tighter_batch.weight_efficiency,
                            min_score: tighter_batch.min_score,
                         },
                        runtime: tighter_batch_scheduler::RuntimeConfig {
                            max_check_batches: tighter_batch.max_check_batches as usize,
                            block_fill_cutoff: tighter_batch.block_fill_cutoff,
                            progress_timeout: Duration::from_secs(tighter_batch.progress_timeout_sec),
                            bundle_expiry: Duration::from_millis(tighter_batch.bundle_expiry_ms),
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
             }
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
        if let SchedulerConfigData::BatchScheduler(batch_config) = &runtime_config.scheduler {
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

impl Scheduler for TighterBatchScheduler {
    type Meta = PriorityId;

    fn poll(
         &mut self,
        bridge: &mut SchedulerBindingsBridge<Self::Meta>,
        config_store: &ConfigStore,
    ) {
         // Read runtime config from the shared store each poll cycle (synchronous, no block_on needed)
        let runtime_config = config_store.read();
         // Apply runtime-tunable config updates to the scheduler
        if let SchedulerConfigData::TighterBatchScheduler(tighter_config) = &runtime_config.scheduler {
            self.set_runtime_config(
                tighter_config.unchecked_capacity,
                tighter_config.checked_capacity,
                tighter_config.bundle_capacity,
                tighter_config.block_fill_cutoff,
                tighter_config.max_check_batches as usize,
                Duration::from_millis(tighter_config.bundle_expiry_ms),
                Duration::from_secs(tighter_config.progress_timeout_sec),
             );
         }

        self.poll(bridge);
     }
}
