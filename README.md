# Tokio MemQ (Async-MQ)

[![Crates.io](https://img.shields.io/crates/v/tokio-memq.svg)](https://crates.io/crates/tokio-memq)
[![Documentation](https://docs.rs/tokio-memq/badge.svg)](https://docs.rs/tokio-memq)
[![License](https://img.shields.io/crates/l/tokio-memq.svg)](LICENSE)

**Tokio MemQ** is a high-performance, feature-rich in-memory asynchronous message queue designed for Rust applications. Built on top of the Tokio runtime, it provides extremely low latency, high throughput, and advanced messaging patterns like consumer groups, batching, and partitioned topics.

It is ideal for high-concurrency local event buses, inter-thread communication, and buffering heavy workloads (e.g., video processing, log aggregation).

---

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
tokio-memq = "1.0.0"
tokio = { version = "1", features = ["full"] }
anyhow = "1.0"
serde = { version = "1.0", features = ["derive"] }
```

---

## 1. Project Architecture

The architecture of `async-mq` is designed for maximum concurrency and minimal lock contention.

### 1.1 High-Level Design

```mermaid
graph TD
    subgraph "Producer Layer"
        P1[Publisher A]
        P2[Publisher B]
        P3[Batch Publisher]
    end

    subgraph "Core Broker (MessageQueue)"
        TM[TopicManager]
        
        subgraph "Topic Channel (RWLock)"
            Buffer[Ring Buffer (VecDeque)]
            LRU[LRU Eviction Policy]
            Stats[Topic Stats & Metrics]
        end
        
        subgraph "Partitioned Topic"
            Part0[Partition 0]
            Part1[Partition 1]
            Router[Routing Strategy]
        end
    end

    subgraph "Consumer Layer"
        S1[Subscriber (Broadcast)]
        S2[Consumer Group X - Member 1]
        S3[Consumer Group X - Member 2]
    end

    P1 -->|Publish| TM
    P2 -->|Publish| TM
    P3 -->|Batch Publish| TM
    
    TM -->|Route| Buffer
    TM -->|Route| Router
    Router --> Part0
    Router --> Part1

    Buffer -->|Notify| S1
    Buffer -->|Notify| S2
    Buffer -->|Notify| S3
```

### 1.2 Core Data Structures
- **Message Buffer**: A `VecDeque<TimestampedMessage>` protected by `RwLock`. This ring buffer enables efficient O(1) push/pop operations.
- **Notification System**: Uses `tokio::sync::watch` for broadcasting new message availability. This avoids the O(N) memory overhead of standard MPMC channels when fan-out is high.
- **Message Payload**: Wrapped in `Arc<Vec<u8>>` or `Arc<dyn Any>` (Native). This "Zero-Copy" design means large payloads (e.g., 100MB video frames) are never deeply copied during routing, only the reference count is incremented.

### 1.3 Memory Management
- **LRU Eviction**: When `max_messages` is reached, the oldest messages are automatically dropped from the front of the queue to make room for new ones.
- **TTL (Time-To-Live)**: Messages older than `message_ttl` are skipped during consumption and lazily cleaned up.

---

## 2. Key Features

### 🚀 Core Messaging
- **Asynchronous API**: Fully non-blocking, built on `async/await` and Tokio.
- **Broadcast & Unicast**: Supports both Pub/Sub (broadcast to all) and Queue (competing consumers) patterns.
- **Zero-Copy Messaging**: `publish_bytes` and `publish_shared_bytes` allow passing large data blobs (100MB+) with zero overhead.

### ⚙️ Advanced Control
- **Consumer Groups**: Load balance messages across multiple consumers using shared offsets.
- **Batch Processing**: `publish_batch` and `recv_batch` APIs for maximizing throughput (up to 3x speedup).
- **Partitioning (Sharding)**: Horizontal scaling with Round-Robin, Hash, or Random routing strategies.
- **Message Replay**: `seek(offset)` and `Earliest`/`Latest` modes allow replaying historical messages.
- **Stream API**: Full integration with `tokio_stream` for idiomatic stream processing.

### 🛡️ Reliability & Management
- **Flow Control**: Configurable buffer size (`max_messages`) and TTL (Time-To-Live).
- **Serialization Pipeline**: Built-in support for **Bincode**, **JSON**, **MessagePack**, and **Native** (raw bytes). Supports **Gzip/Zstd** compression.
- **Monitoring**: Real-time statistics via `get_topic_stats()` (message depth, dropped count, subscriber count).

---

## 3. Applicable Scenarios

`async-mq` is best suited for **local, in-memory** workloads where performance is critical.

### ✅ Best Use Cases
1.  **High-Performance Event Bus**: Connecting decoupled modules within a single high-load application (e.g., game servers, trading engines).
2.  **Thread Data Decoupling**: Safely passing data ownership between threads (e.g., UI thread to background worker) without complex `Mutex`/`RwLock` management.
3.  **Data Processing Pipelines**: Passing large data chunks (images, video frames, AI tensors) between processing stages without copying.
3.  **Log Buffering**: Aggregating logs/metrics from thousands of threads before batch-writing to disk/network.
4.  **Traffic Shaping**: Using the queue to smooth out bursty traffic before hitting a rate-limited external API.

### ❌ Not Suitable For
- **Distributed Systems**: This is not a replacement for Kafka/RabbitMQ across network boundaries (unless wrapped in a network layer).
- **Disk Persistence**: Messages are volatile and live in RAM. If the process crashes, data is lost.

### 🆚 Comparison

| Feature | `tokio::sync::broadcast` | `tokio::sync::mpsc` | `tokio-memq` |
| :--- | :--- | :--- | :--- |
| **Pattern** | Pub/Sub | Queue | Pub/Sub + Queue |
| **Persistence** | None | None | In-Memory (Configurable) |
| **Replay** | Limited (lag) | No | Yes (Seek/Offset) |
| **Consumer Groups** | No | Yes (Single Receiver) | Yes (Multiple Consumers) |
| **Serialization** | No | No | Built-in (Pluggable) |
| **Batching** | No | No | Yes |

---

## 4. Performance Testing

We conducted comprehensive performance tests on a standard macOS workstation (Apple Silicon).

### 4.1 Test Scenarios
1.  **Baseline**: 1 Producer / 1 Consumer (1KB payload).
2.  **High Concurrency**: 10 Producers / 10 Consumers (Fan-out).
3.  **Batch Processing**: Batch size 100/1000.
4.  **Large Object**: 1MB - 100MB payloads (Zero-Copy).

### 4.2 Benchmark Results

| Scenario | Payload | Config | Throughput | Message Rate | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Baseline** | 1 KB | 1P / 1C | ~780 MiB/s | **~798,000 ops/s** | ✅ |
| **Load Test** | 1 KB | 10P / 10C | ~2.0 GB/s | **~2,050,000 ops/s** | ✅ |
| **Batching** | 1 KB | Batch 100 | ~2.2 GB/s | **~2,265,000 ops/s** | ✅ |
| **Large Obj** | 1 MB | Zero-Copy | **~2.9 GB/s** | ~2,900 ops/s | ✅ |
| **Huge Obj** | 100 MB | Zero-Copy | **~2.1 GB/s** | ~21 ops/s | ✅ |

### 4.3 Optimization Tips
- **Enable Batching**: For small messages (<1KB), using `recv_batch(100)` can improve performance by 300%.
- **Use Native/Bytes for Large Data**: Avoid serialization overhead for large blobs by using `publish_bytes` or `SerializationFormat::Native`.
- **Tune Buffer Size**: For bursty producers (e.g., 10P/10C test), ensure `max_messages` is large enough (e.g., 1,000,000) to avoid LRU eviction drops.

---

## 5. Usage Examples

### 5.1 Basic Pub/Sub
```rust
use tokio_memq::mq::MessageQueue;
use tokio_memq::{MessageSubscriber, AsyncMessagePublisher};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mq = MessageQueue::new();
    let topic = "chat";

    // 1. Create Subscriber
    let sub = mq.subscriber(topic.to_string()).await?;

    // 2. Publish Message
    let pub_instance = mq.publisher(topic.to_string());
    tokio::spawn(async move {
        pub_instance.publish("Hello World".to_string()).await.unwrap();
    });

    // 3. Receive Message
    let msg = sub.recv().await?;
    let payload: String = msg.deserialize()?;
    println!("Received: {}", payload);

    Ok(())
}
```

### 5.2 Stream API (Async Iteration)
Consume messages using Rust's `Stream` trait, perfect for `while let` loops.

```rust
use tokio_stream::StreamExt;

let stream = sub.stream();
tokio::pin!(stream);

while let Some(msg_result) = stream.next().await {
    let msg = msg_result?;
    println!("Got msg: {:?}", msg);
}
```

### 5.3 High-Throughput Batching
```rust
// Publisher Side
let messages: Vec<i32> = (0..100).collect();
publisher.publish_batch(messages).await?;

// Subscriber Side
let batch_size = 100;
let msgs = sub.recv_batch(batch_size).await?;
println!("Processed {} messages", msgs.len());
```

### 5.4 Consumer Groups (Load Balancing)
```rust
use tokio_memq::mq::{ConsumptionMode, TopicOptions};

let options = TopicOptions::default();
let group_id = "worker_group_1";

// Create two consumers sharing the same Group ID
let worker1 = mq.subscriber_group_with_options(
    "jobs".to_string(), 
    options.clone(), 
    group_id.to_string(), 
    ConsumptionMode::LastOffset
).await?;

let worker2 = mq.subscriber_group_with_options(
    "jobs".to_string(), 
    options, 
    group_id.to_string(), 
    ConsumptionMode::LastOffset
).await?;

// Messages will be distributed between worker1 and worker2
```

### 5.5 Partitioned Topics (Sharding)
```rust
use tokio_memq::mq::{TopicOptions, PartitionRouting};

// Create a topic with 4 partitions
let opts = TopicOptions { partitions: Some(4), ..Default::default() };
mq.create_partitioned_topic("events".to_string(), opts, 4).await?;

// Auto-route messages (Round-Robin by default)
mq.set_partition_routing("events".to_string(), PartitionRouting::RoundRobin).await?;
mq.publish_to_partitioned(msg).await?;
```

### 5.6 Custom Serialization & Compression
```rust
use tokio_memq::mq::{
    SerializationFactory, SerializationFormat, SerializationConfig, 
    JsonConfig, PipelineConfig, CompressionConfig
};

let topic = "compressed_logs";

// Configure compression pipeline (Gzip Level 6)
let pipeline = PipelineConfig {
    compression: CompressionConfig::Gzip { level: Some(6) },
    pre: None, 
    post: None,
    use_magic_header: true, // Auto-detect format on receive
};

// Register defaults for a topic
SerializationFactory::register_topic_defaults(
    topic,
    SerializationFormat::Json,
    SerializationConfig::Json(JsonConfig { pretty: false }),
    Some(pipeline),
);

// Now all messages published to "compressed_logs" will be automatically
// serialized to JSON and Gzip compressed.
```

---

## 6. Configuration Reference

### TopicOptions
| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `max_messages` | `Option<usize>` | `Some(10000)` | Maximum number of messages to retain. Oldest messages are evicted (LRU) when exceeded. |
| `message_ttl` | `Option<Duration>` | `None` | Time-To-Live. Messages older than this are skipped during consumption. |
| `lru_enabled` | `bool` | `true` | Whether to drop old messages when `max_messages` is reached. If false, publish may block or fail. |
| `partitions` | `Option<usize>` | `None` | Number of partitions for sharded topics. |
| `idle_timeout` | `Option<Duration>` | `None` | (Reserved) Topic idle timeout. |
| `consume_idle_timeout` | `Option<Duration>` | `None` | (Reserved) Consumer idle timeout. |

### ConsumptionMode
| Mode | Description |
| :--- | :--- |
| `Earliest` | Start consuming from the oldest available message in the buffer. |
| `Latest` | Start consuming only new messages arriving after subscription. |
| `Offset(n)` | Start consuming exactly from offset `n`. |
| `LastOffset` | (Default) Continue from the last committed offset for this consumer group. |

---

## 7. Development & Testing

### Running Tests
```bash
cargo test
```

### Running Benchmarks
We provide a dedicated example for performance testing:
```bash
# Run standard performance test
cargo run --release --example perf_runner

# Run large object test (1MB - 100MB)
cargo run --release --example large_object_test
```

---

## 🤝 Contributing

We welcome all contributions! Please follow these steps to contribute to `tokio-memq`:

1.  **Fork the Repository**: Click the "Fork" button on the top right corner of the GitHub page.
2.  **Create a Branch**: Create a new branch for your feature or bug fix.
    ```bash
    git checkout -b feature/amazing-feature
    ```
3.  **Commit Changes**: Make sure your code follows the existing style and passes all tests.
    ```bash
    git commit -m "feat: add amazing feature"
    ```
4.  **Push to Branch**: Push your changes to your forked repository.
    ```bash
    git push origin feature/amazing-feature
    ```
5.  **Open a Pull Request**: Go to the original repository and click "New Pull Request". Provide a clear description of your changes.

### Reporting Issues
If you encounter any bugs or have feature requests, please open an issue on our [GitHub Issues](https://github.com/weiwangfds/tokio-memq/issues) page. Please include:
- A clear description of the issue.
- Minimal reproduction steps or code snippets.
- Your environment details (OS, Rust version).
