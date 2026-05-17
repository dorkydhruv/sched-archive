use agave_scheduling_utils::bridge::SchedulerBindingsBridge;
use agave_scheduling_utils::handshake::{ClientLogon, client};
use futures::{stream::FuturesUnordered, StreamExt};
use scheduler_batch::jito_thread::JitoArgs;
use scheduler_batch::tip_program::TipDistributionArgs;
use schedulers::shared::PriorityId;
use solana_keypair::{EncodableKey, Keypair};
use std::thread::JoinHandle as StdJoinHandle;
use tokio::runtime::Runtime;
use tokio::signal::unix::SignalKind;
use tokio::task::JoinHandle as TokioJoinHandle;
use tracing::{error, info};
use std::sync::Arc;
use std::{path::PathBuf, time::Duration};
use tokio_util::sync::CancellationToken;
use scheduler_batch::scheduler::{BatchScheduler, BatchSchedulerArgs};
use crate::args::Args;
use crate::config::{Config, SchedulerConfig};


pub(crate) struct SchedulerThread{
    shutdown: CancellationToken,
    threads: FuturesUnordered<TokioJoinHandle<std::thread::Result<()>>>,
}

impl SchedulerThread {
    pub(crate) fn run_in_place(args: Args, config: Config) -> std::thread::Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = rt.block_on(SchedulerThread::setup(&rt, args, config));
        rt.block_on(server.run())
    }

   async fn setup(runtime: &Runtime, args: Args, config: Config) -> Self {
        let shutdown = CancellationToken::new();

        // Spawn metrics & events publishers (if NATS is configured).
        let mut threads = Vec::default();
        // The events publisher is only spawned if NATS is configured, otherwise we just pass None to the schedulers.
        // We'll worry about this later when we setup the web interface
        // let events = match config.nats_servers.is_empty() {
        //     true => None,
        //     false => {
        //         let nats_client = Box::leak(Box::new(
        //             metrics_nats_exporter::async_nats::connect(config.nats_servers)
        //                 .await
        //                 .expect("NATS Client Connect"),
        //         ));
        //         threads.push(
        //             metrics_nats_exporter::install(
        //                 shutdown.token.clone(),
        //                 metrics_nats_exporter::Config {
        //                     interval_min: Duration::from_millis(50),
        //                     interval_max: Duration::from_millis(1000),
        //                     metric_prefix: Some(format!("metric.scheduler.{}", config.host_name)),
        //                 },
        //                 nats_client,
        //             )
        //             .unwrap(),
        //         );

        //         // Spawn events publisher.
        //         let event_ctx = EventContext::new();
        //         let (event_tx, event_rx) = mpsc::channel(1024);
        //         let events = EventEmitter::new(event_ctx, event_tx);
        //         threads.push(EventsThread::spawn(event_rx, nats_client, &config.host_name));

        //         Some(events)
        //     }
        // };
        let events = None;

        // Setup scheduler.
        match config.scheduler {
            SchedulerConfig::Batch(batch) => {
                let keypair = Arc::new(Keypair::read_from_file(batch.keypair_path).unwrap());
                let (scheduler, jito_thread) = BatchScheduler::new(
                    shutdown.clone(),
                    events,
                    BatchSchedulerArgs {
                        tip: TipDistributionArgs {
                            vote_account: batch.tip.vote_account,
                            merkle_authority: batch.tip.merkle_authority,
                            commission_bps: batch.tip.commission_bps,
                        },
                        jito: JitoArgs {
                            http_rpc: batch.jito.http_rpc,
                            ws_rpc: batch.jito.ws_rpc,
                            block_engine: batch.jito.block_engine,
                        },
                        keypair,
                        filter_keys: config.filter_keys,
                        unchecked_capacity: 64 * 1024,
                        checked_capacity: 64 * 1024,
                        bundle_capacity: 1024,
                    },
                );

                threads.push(crate::scheduler_thread::spawn(
                    shutdown.clone(),
                    args.bindings_ipc,
                    scheduler,
                    5,
                ));
                threads.push(jito_thread);
            }
            SchedulerConfig::Fifo => todo!(),
            SchedulerConfig::GreedyRevenue => todo!(),
            SchedulerConfig::GreedyThroughput => todo!(),
        }

        // Use tokio to listen on all thread exits concurrently.
        let threads = threads
            .into_iter()
            .map(|thread| {
                let name = thread.thread().name().unwrap().to_string();
                info!(name, "Thread spawned");

                runtime.spawn_blocking(move || thread.join())
            })
            .collect();

        SchedulerThread { shutdown, threads }
    }

    async fn run(mut self) -> std::thread::Result<()> {
        let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
        let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();

        let mut exit = tokio::select! {
            () = self.shutdown.cancelled() => Ok(()),

            _ = sigterm.recv() => {
                info!("SIGTERM caught, stopping server");

                Ok(())
            },
            _ = sigint.recv() => {
                info!("SIGINT caught, stopping server");

                Ok(())
            },
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
            info!( ?thread, "Thread exited");
            exit = exit.and(thread.unwrap());
        }

        exit
    }
}

pub(crate) fn spawn<S>(
    shutdown: CancellationToken,
    bindings_ipc: PathBuf,
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
                scheduler.poll(&mut bridge);
            }
        })
        .unwrap()
}

pub(crate) trait Scheduler
where
    Self: Sized + 'static,
{
    type Meta: Copy;

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>);
}

impl Scheduler for BatchScheduler {
    type Meta = PriorityId;

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
        self.poll(bridge);
    }
}

// impl Scheduler for FifoScheduler {
//     type Meta = ();

//     fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
//         self.poll(bridge);
//     }
// }

// impl Scheduler for GreedyRevenueScheduler {
//     type Meta = PriorityId;

//     fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
//         self.poll(bridge);
//     }
// }

// impl Scheduler for GreedyThroughputScheduler {
//     type Meta = PriorityId;

//     fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
//         self.poll(bridge);
//     }
// }
