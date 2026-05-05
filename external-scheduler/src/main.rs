use clap::Parser;

mod args;
mod config;
fn main() -> std::thread::Result<()>{
    let args = crate::args::Args::parse();
    
}