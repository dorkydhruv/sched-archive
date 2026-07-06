# Sched-Archive

Pluggable transaction scheduling engine and benchmark suite for Solana (Agave) validators. This repository provides alternative scheduler implementations designed to maximize block packaging efficiency and validator revenue under hotspot congestion.


## Repository Structure

*   **`schedulers/batch-scheduler`**: The standard FIFO Agave batch scheduler.
*   **`schedulers/auction-batch-scheduler`**: An advanced scheduler utilizing out-of-order scheduling, congestion pricing, and serialization penalties to process cold transactions in parallel under heavy state contention.
*   **`external-scheduler`**: The runner/daemon that runs alongside the validator and manages IPC bindings.


## Slot-Level Benchmark

We provide a comparative benchmark simulating a 400ms leader slot under severe lock contention (60% hotspot transaction volume targeting a single account). 

To execute the benchmark and generate the performance comparison report:
```bash
cargo bench -p agave-external-scheduler --bench comparative_bench
```

Running the benchmark automatically updates [benchmark_report.md](benchmark_report.md) at the workspace root.


## Configuration & Usage

The external scheduler runner expects a simplified TOML configuration file. Examples are provided at the root:
*   [batch.toml](batch.toml): Standard BatchScheduler configuration.
*   [auction.toml](auction.toml): AuctionBatchScheduler configuration.

### Running the External Scheduler Daemon

To start the runner:
```bash
cargo run -p agave-external-scheduler -- --bindings-ipc <PATH_TO_IPC_SOCKET> --config <PATH_TO_TOML_CONFIG>
```
