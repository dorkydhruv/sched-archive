# Comparative Scheduler Benchmark Report

## Benchmark Configuration

The benchmark simulates an entire 400ms leader slot consisting of 64 progress ticks:
- **Throughput**: 250 transactions generated per tick (totaling 16,000 transactions queued).
- **Contention Model**: 60% of incoming transactions target a single write-locked account (congested account 0). The remaining 40% are non-conflicting cold transactions targeting unique random accounts.
- **Execution Workers**: 4 parallel execution threads (plus 1 check worker).
- **Execution Latency**: Transactions take 4 ticks (25ms) to execute.
- **Prioritization Fee**: A flat fee of 200,000 micro-lamports per CU (5,000 lamports per transaction).

---

## Performance Summary

| Metric | BatchScheduler | AuctionBatchScheduler | Improvement |
| :--- | :--- | :--- | :--- |
| **Total Settled / Packed Tx** | 56 | **217** | **+287.5%** (~3.9x throughput) |
| **Total Revenue (Lamports)** | 270000 | **1075000** | **+298.1%** (~4.0x revenue) |
| **Average Batch Size** | 1.00 | 1.00 | - |
| **Execution Worker Efficiency** | Poor | **Excellent** | ~3.9x utilization |
