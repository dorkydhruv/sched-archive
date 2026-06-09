use crate::args::Args;
use crate::config_store::{ConfigStore, SchedulerConfigData};
use agave_scheduling_utils::bridge::SchedulerBindingsBridge;
use agave_scheduling_utils::handshake::{ClientLogon, client};
use futures::{StreamExt, stream::FuturesUnordered};
use jito_scheduler::jito_thread::JitoArgs;
use jito_scheduler::scheduler::{JitoScheduler, JitoSchedulerArgs, RuntimeConfig};
use jito_scheduler::tip_program::TipDistributionArgs;
use schedulers::PriorityId;
use schedulers::events::{EventContext, EventEmitter};
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
            SchedulerConfigData::JitoScheduler(jito) => {
                let keypair = Arc::new(Keypair::read_from_file(&jito.keypair_path).unwrap());
                let (scheduler, jito_thread) = JitoScheduler::new(
                    shutdown.clone(),
                    events,
                    JitoSchedulerArgs {
                        tip: TipDistributionArgs {
                            vote_account: Pubkey::from_str(&jito.tip.vote_account).unwrap(),
                            merkle_authority: Pubkey::from_str(&jito.tip.merkle_authority).unwrap(),
                            commission_bps: jito.tip.commission_bps,
                        },
                        jito: JitoArgs {
                            http_rpc: jito.jito.http_rpc,
                            ws_rpc: jito.jito.ws_rpc,
                            block_engine: jito.jito.block_engine,
                        },
                        keypair,
                        filter_keys: initial_config.filter_keys,
                        unchecked_capacity: jito.unchecked_capacity,
                        checked_capacity: jito.checked_capacity,
                        bundle_capacity: jito.bundle_capacity,
                        runtime: RuntimeConfig {
                            max_check_batches: jito.max_check_batches as usize,
                            block_fill_cutoff: jito.block_fill_cutoff,
                            progress_timeout: Duration::from_secs(jito.progress_timeout_sec),
                            bundle_expiry: Duration::from_millis(jito.bundle_expiry_ms),
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
            SchedulerConfigData::Fifo => todo!(),
            SchedulerConfigData::GreedyRevenue => todo!(),
            SchedulerConfigData::GreedyThroughput => todo!(),
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

impl Scheduler for JitoScheduler {
    type Meta = PriorityId;

    fn poll(
        &mut self,
        bridge: &mut SchedulerBindingsBridge<Self::Meta>,
        config_store: &ConfigStore,
    ) {
        // Read runtime config from the shared store each poll cycle (synchronous, no block_on needed)
        let runtime_config = config_store.read();
        // Apply runtime-tunable config updates to the scheduler
        if let SchedulerConfigData::JitoScheduler(jito_config) = &runtime_config.scheduler {
            self.set_runtime_config(
                jito_config.unchecked_capacity,
                jito_config.checked_capacity,
                jito_config.bundle_capacity,
                jito_config.block_fill_cutoff,
                jito_config.max_check_batches as usize,
                Duration::from_millis(jito_config.bundle_expiry_ms),
                Duration::from_secs(jito_config.progress_timeout_sec),
            );
        }

        self.poll(bridge);
    }
}

// impl Scheduler for FifoScheduler {
//     type Meta = ();

//     fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>, _config_store: &ConfigStore) {
//         self.poll(bridge);
//      }
// }

// impl Scheduler for GreedyRevenueScheduler {
//     type Meta = PriorityId;

//     fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>, _config_store: &ConfigStore) {
//         self.poll(bridge);
//      }
// }

// impl Scheduler for GreedyThroughputScheduler {
//     type Meta = PriorityId;

//     fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>, _config_store: &ConfigStore) {
//         self.poll(bridge);
//      }
// }
