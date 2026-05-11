use clap::Parser;

mod args;
mod config;
mod scheduler_thread;
fn main() -> std::thread::Result<()> {
    let args = crate::args::Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    Ok(())
}
