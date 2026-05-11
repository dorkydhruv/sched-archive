use agave_scheduling_utils::bridge::SchedulerBindingsBridge;
use agave_scheduling_utils::handshake::{ClientLogon, client};
use std::{path::PathBuf, thread::JoinHandle, time::Duration};
use tokio_util::sync::CancellationToken;

pub(crate) fn spawn<S>(
    shutdown: CancellationToken,
    bindings_ipc: PathBuf,
    mut scheduler: S,
    worker_threads: usize,
) -> JoinHandle<()>
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
