//! Tokio MemQ - 简单高性能的基于 Tokio 的内存消息队列
//!
//! Tokio MemQ - Simple, high-performance in-memory message queue powered by Tokio.
pub mod mq;

pub use mq::traits::{MessagePublisher, AsyncMessagePublisher, MessageSubscriber, QueueManager};
pub use mq::message::{TopicMessage, TopicOptions, TimestampedMessage, ConsumptionMode};
pub use mq::serializer::{SerializationFormat, SerializationHelper, Serializer, BincodeSerializer};
pub use mq::publisher::Publisher;
pub use mq::subscriber::Subscriber;
pub use mq::broker::TopicManager;
pub use mq::MessageQueue;
