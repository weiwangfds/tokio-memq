# Performance Test Report: Async-MQ

## Overview
This report documents the performance characteristics of `async-mq`, a high-performance in-memory message queue. The tests cover baseline performance, high concurrency load, batch processing, large message handling, and endurance stability.

## Test Environment
- **OS:** macOS
- **Date:** 2025-12-18
- **Tool:** `examples/perf_runner.rs` (Custom benchmark runner)

## Methodology
The benchmark runner executes five distinct scenarios. Each test measures:
- **Throughput:** Messages processed per second.
- **Latency:** Average time per message (amortized).
- **Resource Usage:** CPU and Memory consumption.
- **Reliability:** Message delivery success and drop rates.

To prevent test hangs, the runner enforces a 5-second per-message consumer timeout and a 60-second global test timeout.

## Results Summary

| Test Scenario | Configuration | Throughput | Avg Latency | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Baseline** | 1 Prod, 1 Cons, 1KB Msg | **~798,000 msgs/sec** | 1.25 µs | PASS |
| **Load Test** | 10 Prod, 10 Cons, 1KB Msg | **~2,050,000 msgs/sec** | 0.49 µs | PASS |
| **Batching** | 1 Prod, 1 Cons, 1KB, Batch 100 | **~2,265,000 msgs/sec** | 0.44 µs | PASS |
| **Stress** | 1 Prod, 1 Cons, 1MB Msg | **~6,500 msgs/sec** (~6.5 GB/s) | 154.0 µs | PASS |
| **Endurance** | 2 Prod, 2 Cons, Batch 50 | **~1,890,000 msgs/sec** | 0.53 µs | PASS |

### Detailed Analysis

#### 1. Baseline Performance (1P/1C)
- **Performance:** ~798k ops/sec.
- **Observation:** Standard channel performance is excellent. 
- **Tuning:** Required increasing buffer size to 200,000 to prevent message drops when the consumer fell slightly behind the tight producer loop.

#### 2. Load Test (10P/10C)
- **Performance:** ~2.05 million ops/sec.
- **Observation:** The system demonstrates excellent scalability. With 10 producers and 10 consumers, the queue handles a "fan-out" workload (100k writes -> 1M reads) efficiently, increasing aggregate throughput by **2.5x** over the baseline.
- **Fix for Stuck Tests:** Previous tests hung because the default buffer was too small for the burst of 10 concurrent producers. Increasing `max_messages` to 1,000,000 resolved the drops and the hang.

#### 3. Batching (Batch Size 100)
- **Performance:** ~2.26 million ops/sec.
- **Observation:** Batching is the most effective optimization, providing a **~3x speedup** over the baseline. It reduces lock acquisition overhead significantly.
- **Latency:** Amortized latency drops to just 0.44 µs per message.

#### 4. Stress Test (1MB Messages)
- **Performance:** ~6.5 GB/sec data rate.
- **Observation:** The system handles large payloads efficiently, leveraging memory bandwidth effectively. Throughput is likely limited by `memcpy` speeds rather than queue overhead.

#### 5. Endurance Test
- **Performance:** Sustained ~1.89M ops/sec.
- **Observation:** The system remained stable under sustained load with multiple producers and consumers using batching, with **zero dropped messages**.

## Recommendations
1.  **Use Batching:** For high throughput, always use batch publishing (`publish_batch`) and consumption.
2.  **Tune Buffer Sizes:** The default buffer size may be too small for bursty producers. Configure `TopicOptions.max_messages` based on your expected burst rate and consumer processing speed.
3.  **Monitor Dropped Messages:** Use `mq.get_topic_stats()` to monitor `dropped_messages` in production to detect slow consumers.

## Conclusion
`async-mq` demonstrates high performance, capable of processing over **2.2 million messages per second** on a single node. The initial "stuck" issues were resolved by properly sizing the topic buffers, highlighting the importance of configuration for high-concurrency workloads.
