# Sched-Archive

Pluggable transaction scheduling engine and benchmark suite for Solana (Agave) validators. This repository provides alternative scheduler implementations designed to maximize block packaging efficiency, throughput, and validator revenue under hotspot congestion.


## Repository Structure

*   **`schedulers/batch-scheduler`**: The standard FIFO Agave batch scheduler.
*   **`schedulers/auction-batch-scheduler`**: An advanced scheduler utilizing out-of-order scheduling, congestion pricing, serialization penalties, and time-decay slots to process independent transactions in parallel under heavy state contention.
*   **`external-scheduler`**: The runner/daemon that runs alongside the validator and manages IPC bindings.


## Configuration & Usage

The external scheduler runner expects a simplified TOML configuration file. Examples are provided at the root:
*   [batch.toml](batch.toml): Standard BatchScheduler configuration.
*   [auction.toml](auction.toml): AuctionBatchScheduler configuration.

### Running the External Scheduler Daemon

To start the runner daemon:
```bash
cargo run -p agave-external-scheduler -- --bindings-ipc <PATH_TO_IPC_SOCKET> --config <PATH_TO_TOML_CONFIG>
```
