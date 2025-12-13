# Tokio MemQ

[![Crates.io](https://img.shields.io/crates/v/tokio-memq.svg)](https://crates.io/crates/tokio-memq)
[![docs.rs](https://img.shields.io/docsrs/tokio_memq)](https://docs.rs/tokio_memq)

Simple, high-performance in-memory async message queue powered by Tokio.

## Features

- TTL: Discard expired messages on receive
- LRU: Default-enabled eviction to prevent memory bloat
- Consumer Groups: Offset management with `Earliest`, `Latest`, `LastOffset` (default), `Offset(n)`
- Pluggable serialization (Bincode by default)

## Install

Add to your `Cargo.toml`:

```toml
[dependencies]
tokio-memq = "0.1"
```

## Quick Start

```rust
use tokio_memq::{MessageQueue, ConsumptionMode, TopicOptions};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mq = MessageQueue::new();
    let topic = "demo";

    // Publisher
    let pubr = mq.publisher(topic.to_string());
    pubr.publish_string("Hello".to_string()).await?;

    // Subscriber (default options)
    let sub = mq.subscriber(topic.to_string()).await?;
    let msg = sub.recv().await?;
    println!("Received: {}", msg.payload());

    Ok(())
}
```

## Consumer Groups & Offsets

```rust
use tokio_memq::{MessageQueue, ConsumptionMode};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mq = MessageQueue::new();
    let topic = "group_demo";
    let pubr = mq.publisher(topic.to_string());

    for i in 0..5 {
        pubr.publish_string(format!("Msg {}", i)).await?;
    }

    // A: earliest
    let sub_a = mq.subscriber_group(topic.to_string(), "g1".to_string(), ConsumptionMode::Earliest).await?;
    let _ = sub_a.recv().await?;
    let _ = sub_a.recv().await?;

    // B: resume from last offset
    drop(sub_a);
    let sub_b = mq.subscriber_group(topic.to_string(), "g1".to_string(), ConsumptionMode::LastOffset).await?;
    let next = sub_b.recv().await?;
    assert_eq!(next.payload(), "Msg 2");

    Ok(())
}
```

## TTL & LRU

```rust
use tokio_memq::{MessageQueue, TopicOptions};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mq = MessageQueue::new();
    let topic = "ttl_lru";

    // TTL: 500ms
    mq.create_topic(topic.to_string(), TopicOptions { message_ttl: Some(Duration::from_millis(500)), ..Default::default() }).await?;

    let pubr = mq.publisher(topic.to_string());
    let sub = mq.subscriber(topic.to_string()).await?;

    pubr.publish_string("will expire".to_string()).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(sub.try_recv().await.is_err(), "expired should not be received");

    Ok(())
}
```

## License

Licensed under either of

- Apache License, Version 2.0
- MIT license

at your option.
