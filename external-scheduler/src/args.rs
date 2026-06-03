use std::path::PathBuf;

use clap::{Parser, ValueHint};

#[derive(Debug, Parser)]
pub(crate) struct Args {
    /// Path to Agave IPC server.
    #[clap(short = 'i', long, value_hint = ValueHint::FilePath)]
    pub(crate) bindings_ipc: PathBuf,
    /// If provided, will write hourly log files to this directory.
    #[clap(long, value_hint = ValueHint::DirPath)]
    pub(crate) logs: Option<PathBuf>,
    /// Emit metrics via NATS.
    #[clap(long)]
    pub(crate) metrics: bool,
    /// Port for the web UI config server.
    #[clap(short = 'p', long, default_value_t = 3000)]
    pub(crate) port: u16,
    /// Path to scheduler config.
    #[clap(
        short = 'c',
        long,
        default_value = "/etc/batch.toml",
        value_hint = ValueHint::FilePath
    )]
    pub(crate) config: PathBuf,
}
