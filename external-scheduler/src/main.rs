use clap::Parser;
use std::error::Error;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::config_store::ConfigStore;
use crate::scheduler_thread::SchedulerThread;
use crate::web_server::start_server;

mod args;
mod config_store;
mod scheduler_thread;
mod web_server;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = crate::args::Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Setup standard panic handling.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        error!(?panic_info, "Application panic");
        default_panic(panic_info);
    }));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let config_store = ConfigStore::from_file(&args.config)?;
    let shutdown = CancellationToken::new();

    runtime.block_on(async move {
        let mut server = tokio::spawn(start_server(
            config_store.clone(),
            args.port,
            shutdown.clone(),
        ));
        let mut scheduler = tokio::spawn(SchedulerThread::run(
            args,
            config_store.clone(),
            shutdown.clone(),
        ));

        tokio::select! {
            server_result = &mut server => {
                shutdown.cancel();
                scheduler.abort();

                match server_result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(error),
                    Err(join_error) => Err(join_error_to_error(join_error)),
                }
            },
            scheduler_result = &mut scheduler => {
                shutdown.cancel();
                server.abort();

                match scheduler_result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(panic)) => panic_result_to_error(panic),
                    Err(join_error) => Err(join_error_to_error(join_error)),
                }
            },
            _ = wait_for_shutdown_signal() => {
                shutdown.cancel();
                server.abort();
                scheduler.abort();

                let _ = server.await;
                let _ = scheduler.await;

                Ok(())
            },
        }
    })
}

async fn wait_for_shutdown_signal() {
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    let mut sigint =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();

    tokio::select! {
        _ = sigterm.recv() => (),
        _ = sigint.recv() => (),
    }
}

fn join_error_to_error(join_error: tokio::task::JoinError) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::other(join_error.to_string()))
}

fn panic_result_to_error(
    panic: Box<dyn std::any::Any + Send>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(message) = panic.downcast_ref::<&str>() {
        Err(Box::new(std::io::Error::other(*message)))
    } else if let Some(message) = panic.downcast_ref::<String>() {
        Err(Box::new(std::io::Error::other(message.clone())))
    } else {
        Err(Box::new(std::io::Error::other("task panicked")))
    }
}
