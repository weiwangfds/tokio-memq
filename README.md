# Tokio MemQ

[![Crates.io](https://img.shields.io/crates/v/tokio-memq.svg)](https://crates.io/crates/tokio-memq)
[![docs.rs](https://img.shields.io/docsrs/tokio-memq)](https://docs.rs/tokio-memq)

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

## Serialization Examples

Tokio MemQ 提供可插拔的序列化机制，默认使用 Bincode。你也可以选择 JSON、MessagePack 或注册自定义格式。

### Basic: Bincode / JSON / MessagePack

```rust
use serde::{Serialize, Deserialize};
use tokio_memq::{SerializationHelper, SerializationFormat};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct User { id: u32, name: String }

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let u = User { id: 1, name: "alice".into() };

    // Bincode
    let bin = SerializationHelper::serialize(&u, &SerializationFormat::Bincode)?;
    let u1: User = SerializationHelper::deserialize(&bin, &SerializationFormat::Bincode)?;
    assert_eq!(u, u1);

    // JSON
    let json = SerializationHelper::serialize(&u, &SerializationFormat::Json)?;
    let u2: User = SerializationHelper::deserialize(&json, &SerializationFormat::Json)?;
    assert_eq!(u, u2);

    // MessagePack
    let msg = SerializationHelper::serialize(&u, &SerializationFormat::MessagePack)?;
    let u3: User = SerializationHelper::deserialize(&msg, &SerializationFormat::MessagePack)?;
    assert_eq!(u, u3);

    Ok(())
}
```

### JSON Pretty

```rust
use serde::{Serialize, Deserialize};
use tokio_memq::{SerializationHelper, SerializationFormat};
use tokio_memq::mq::serializer::{SerializationFactory, SerializationConfig, JsonConfig};

#[derive(Serialize, Deserialize)]
struct Item { id: u32, title: String }

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let s = SerializationFactory::create_serializer(
        "json",
        SerializationConfig::Json(JsonConfig { pretty: true })
    )?;

    let it = Item { id: 7, title: "doc".into() };
    let bytes = s.serialize(&it)?; // pretty JSON
    let txt = String::from_utf8(bytes)?;
    assert!(txt.contains('\n'));

    // 仍可用帮助器进行反序列化
    let it2: Item = SerializationHelper::deserialize(txt.as_bytes(), &SerializationFormat::Json)?;
    assert_eq!(it.id, it2.id);
    Ok(())
}
```

### Register Custom Format

```rust
use std::sync::Arc;
use erased_serde::{Serialize as ErasedSerialize, Deserializer as ErasedDeserializer};
use tokio_memq::mq::serializer::{
    Serializer, Deserializer, SerializationFormat, SerializationError,
    SerializationFactory,
};

#[derive(Default, Clone)]
struct MyFmt;

impl Serializer for MyFmt {
    fn serialize(&self, _data: &dyn ErasedSerialize) -> Result<Vec<u8>, SerializationError> {
        Ok(Vec::new())
    }
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Custom("myfmt".into())
    }
}

impl Deserializer for MyFmt {
    fn with_deserializer(
        &self,
        _data: &[u8],
        f: &mut dyn FnMut(&mut dyn ErasedDeserializer) -> Result<(), SerializationError>
    ) -> Result<(), SerializationError> {
        // 构造你的反序列化器并进行类型擦除后调用闭包
        // let mut de = myfmt::Deserializer::new(_data);
        // let mut erased = <dyn ErasedDeserializer>::erase(&mut de);
        // f(&mut erased)
        Ok(())
    }
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Custom("myfmt".into())
    }
}

fn main() {
    SerializationFactory::register_serializer("myfmt", Arc::new(MyFmt::default()));
    SerializationFactory::register_deserializer("myfmt", Arc::new(MyFmt::default()));
}
```

## License

Licensed under either of

- Apache License, Version 2.0
- MIT license

at your option.
