# Tokio MemQ

[![Crates.io](https://img.shields.io/crates/v/tokio-memq.svg)](https://crates.io/crates/tokio-memq)
[![docs.rs](https://img.shields.io/docsrs/tokio-memq)](https://docs.rs/tokio-memq)

High-performance, feature-rich in-memory async message queue powered by Tokio. Designed for high-throughput local messaging with advanced features like backpressure, TTL, consumer groups, and pluggable serialization.

## Features

- **Async & Stream API**: Built on Tokio, supporting `Stream` trait for idiomatic async consumption.
- **Backpressure & Flow Control**: Bounded channels, LRU eviction, and lag monitoring.
- **Advanced Consumption**:
  - **Batch Operations**: High-throughput `publish_batch` and `recv_batch`.
  - **Consumer Groups**: Support for `Earliest`, `Latest`, and `Offset` seeking.
  - **Filtering**: Server-side filtering with `recv_filter`.
  - **Manual Commit/Seek**: Precise offset control.
- **Serialization Pipeline**:
  - Pluggable formats (JSON, MessagePack, Bincode).
  - Compression support (Gzip, Zstd).
  - Per-topic and per-publisher configuration overrides.
  - Auto-format detection via Magic Headers.
- **Management & Monitoring**:
  - Topic deletion and creation options (TTL, Max Messages).
  - Real-time metrics (depth, subscriber count, lag).

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
tokio-memq = "0.1"
```

## Usage Guide

### 1. Basic Publish & Subscribe

The simplest way to use the queue with default settings.

```rust
use tokio_memq::mq::MessageQueue;
use tokio_memq::MessageSubscriber; // Import trait for recv()

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mq = MessageQueue::new();
    let topic = "demo_topic";

    // Publisher
    let pub1 = mq.publisher(topic.to_string());
    pub1.publish(&"Hello World").await?;

    // Subscriber
    let sub = mq.subscriber(topic.to_string()).await?;
    let msg = sub.recv().await?;
    let payload: String = msg.deserialize()?;
    println!("Received: {}", payload);

    Ok(())
}
```

### 2. Stream API

Consume messages as an async stream, ideal for continuous processing loops.

```rust
use tokio_stream::StreamExt;
use tokio::pin;

// Create a stream from the subscriber
let stream = sub.stream();
pin!(stream);

while let Some(msg_res) = stream.next().await {
    match msg_res {
        Ok(msg) => println!("Received: {:?}", msg),
        Err(e) => eprintln!("Error: {}", e),
    }
}
```

### 3. Batch Operations

Improve throughput by processing messages in batches.

```rust
// Batch Publish
let messages = vec![1, 2, 3, 4, 5];
publisher.publish_batch(messages).await?;

// Batch Receive
// Returns a vector of up to 10 messages
let batch = sub.recv_batch(10).await?; 
for msg in batch {
    println!("Batch msg: {:?}", msg);
}
```

### 4. Advanced Consumption (Filter, Timeout, Metadata)

```rust
use std::time::Duration;

// Receive with Timeout
match sub.recv_timeout(Duration::from_millis(500)).await? {
    Some(msg) => println!("Got msg: {:?}", msg),
    None => println!("Timed out"),
}

// Receive with Filter (Server-side filtering)
// Only receive messages where payload size > 100 bytes
let large_msg = sub.recv_filter(|msg| msg.payload.len() > 100).await?;

// Metadata-only Mode (Avoids full payload clone/deserialization)
let msg = sub.recv().await?;
let meta = msg.metadata();
println!("Offset: {}, Timestamp: {:?}", meta.offset, meta.created_at);
```

### 5. Consumer Groups & Offsets

Manage offsets manually or use consumer groups for persistent state.

```rust
use tokio_memq::mq::{ConsumptionMode, TopicOptions};

// Configure topic with retention limits
let options = TopicOptions {
    max_messages: Some(1000),
    message_ttl: None, // Some(Duration::from_secs(3600))
    lru_enabled: true,
    ..Default::default()
};

// Subscribe as part of a Consumer Group
// Modes: Earliest, Latest, Offset(n), LastOffset
let sub_group = mq.subscriber_group_with_options(
    "topic_name".to_string(),
    options,
    "group_id_1".to_string(),
    ConsumptionMode::LastOffset
).await?;

// Manual Commit
let msg = sub_group.recv().await?;
// Process message...
sub_group.commit(msg.offset); // Save progress
```

### 6. Serialization Configuration

Flexible serialization with per-topic and per-publisher overrides.

**Global/Topic Defaults:**

```rust
use tokio_memq::mq::{
    SerializationFactory, SerializationFormat, SerializationConfig, 
    JsonConfig, PipelineConfig, CompressionConfig
};

let topic = "compressed_logs";

// Configure compression pipeline
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
```

**Per-Publisher Overrides (Independent Keys):**

```rust
// Register specific settings for a publisher key
SerializationFactory::register_publisher_defaults(
    "legacy_system",
    SerializationFormat::MessagePack,
    SerializationConfig::Default,
    None
);

// Create a publisher using that key
let pub_legacy = mq.publisher_with_key(topic.to_string(), "legacy_system".to_string());
// Messages from this publisher will use MessagePack, regardless of topic defaults
pub_legacy.publish(&payload).await?;
```

### 7. Management & Monitoring

```rust
use tokio_memq::mq::TopicOptions;
use std::time::Duration;

// Create Topic with Options (Pre-provisioning)
// Useful for setting TTL or max size before usage
let options = TopicOptions {
    max_messages: Some(5000),
    message_ttl: Some(Duration::from_secs(3600)),
    lru_enabled: true,
    ..Default::default()
};
mq.create_topic("my_topic".to_string(), options).await?;

// Get Topic Statistics
if let Some(stats) = mq.get_topic_stats("my_topic".to_string()).await {
    println!(
        "Depth: {}, Subscribers: {}, Lag: {:?}", 
        stats.message_count, 
        stats.subscriber_count,
        stats.consumer_lags
    );
}

// Delete Topic
let deleted = mq.delete_topic("my_topic").await;
```

## Performance

Run the included benchmarks to test performance on your machine:

```bash
cargo bench
```

## Architecture

```mermaid
flowchart LR
    subgraph App["Application"]
        PUB["Publisher"]
        SUB["Subscriber"]
    end
    
    MQ["MessageQueue"]
    TM["TopicManager"]
    TC["TopicChannel"]
    
    BUF["VecDeque<TimestampedMessage>"]
    NO["watch::Sender<usize>"]
    RX["watch::Receiver<usize>"]
    OFF["consumer_offsets (RwLock<HashMap>)"]
    NEXT["next_offset (AtomicUsize)"]
    DROP["dropped_count (AtomicUsize)"]
    TTL["TTL Cleaner (background task)"]
    
    SER["SerializationFactory / Pipeline"]
    
    PUB -->|publish()| MQ --> TM
    TM -->|get_or_create| TC
    TC -->|add_to_buffer / add_to_buffer_batch| BUF
    TC --> NO
    NO --> RX
    SUB -->|recv()/stream()| RX
    SUB -->|fetch_from_buffer| BUF
    SUB -->|advance_offset| OFF
    
    TM -->|subscribe()| SUB
    TC --> OFF
    TC --> NEXT
    TC --> DROP
    TTL --> TC
    
    PUB -->|serialize| SER
    SUB -->|deserialize| SER
```

- Data Path: `Publisher -> MessageQueue -> TopicManager -> TopicChannel -> Buffer -> Subscriber`
- Flow Control: `watch::Sender/Receiver` used for notification; `max_messages` and TTL enforce retention; LRU via front eviction
- Offsets: `next_offset` atomically allocates; consumer group offsets stored in `RwLock<HashMap>`
- Serialization: Pluggable formats and compression via `SerializationFactory` and `PipelineConfig`
- Maintenance: Background TTL cleaner periodically removes expired messages from each topic

## License

Apache-2.0
