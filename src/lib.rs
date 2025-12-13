//! Tokio MemQ - 简单高性能的基于 Tokio 的内存消息队列
//!
//! Tokio MemQ - Simple, high-performance in-memory message queue powered by Tokio.
//!
//! # Examples
//!
//! Basic publish/subscribe:
//! ```rust
//! use tokio_memq::MessageQueue;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mq = MessageQueue::new();
//!     let topic = "demo";
//!
//!     let pubr = mq.publisher(topic.to_string());
//!     pubr.publish_string("Hello".to_string()).await?;
//!
//!     let sub = mq.subscriber(topic.to_string()).await?;
//!     let msg = sub.recv().await?;
//!     assert_eq!(msg.payload(), "Hello");
//!     Ok(())
//! }
//! ```
//!
//! Consumer groups and offsets:
//! ```rust
//! use tokio_memq::{MessageQueue, ConsumptionMode};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mq = MessageQueue::new();
//!     let topic = "group_demo";
//!     let pubr = mq.publisher(topic.to_string());
//!
//!     for i in 0..3 {
//!         pubr.publish_string(format!("Msg {}", i)).await?;
//!     }
//!
//!     let sub_a = mq.subscriber_group(topic.to_string(), "g1".to_string(), ConsumptionMode::Earliest).await?;
//!     let _ = sub_a.recv().await?;
//!     let _ = sub_a.recv().await?;
//!     drop(sub_a);
//!
//!     let sub_b = mq.subscriber_group(topic.to_string(), "g1".to_string(), ConsumptionMode::LastOffset).await?;
//!     let next = sub_b.recv().await?;
//!     assert_eq!(next.payload(), "Msg 2");
//!     Ok(())
//! }
//! ```
pub mod mq;

pub use mq::traits::{MessagePublisher, AsyncMessagePublisher, MessageSubscriber, QueueManager};
pub use mq::message::{TopicMessage, TopicOptions, TimestampedMessage, ConsumptionMode};
pub use mq::serializer::{SerializationFormat, SerializationHelper, Serializer, BincodeSerializer};
pub use mq::publisher::Publisher;
pub use mq::subscriber::Subscriber;
pub use mq::broker::TopicManager;
pub use mq::MessageQueue;
