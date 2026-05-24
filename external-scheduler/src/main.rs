use std::collections::HashSet;

use clap::Parser;
use tracing::error;

use crate::{config::{Config, SchedulerConfig}, scheduler_thread::SchedulerThread};

mod args;
mod config;
mod scheduler_thread;
fn main() -> std::thread::Result<()> {
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

    // Load config (or use default).
    let config = args.config.as_ref().map_or_else(
        || Config {
            host_name: "dev".to_string(),
            filter_keys: HashSet::new(),
            scheduler: SchedulerConfig::GreedyThroughput,
        },
        |path| toml::from_str(&String::from_utf8(std::fs::read(path).unwrap_or_else(|_| panic!("failed to read the toml config file"))).unwrap()).unwrap(),
    );

    // Start server.
    SchedulerThread::run_in_place(args, config)
}
