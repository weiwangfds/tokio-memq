//! # Tokio MemQ
//!
//! High-performance, feature-rich in-memory async message queue powered by Tokio.
//! Designed for high-throughput local messaging with advanced features like backpressure,
//! TTL, consumer groups, and pluggable serialization.
//!
//! ## Features
//!
//! - **Async & Stream API**: Built on Tokio, supporting `Stream` trait for idiomatic async consumption.
//! - **Backpressure & Flow Control**: Bounded channels, LRU eviction, and lag monitoring.
//! - **Advanced Consumption**:
//!   - **Batch Operations**: High-throughput `publish_batch` and `recv_batch`.
//!   - **Consumer Groups**: Support for `Earliest`, `Latest`, and `Offset` seeking.
//!   - **Filtering**: Server-side filtering with `recv_filter`.
//!   - **Manual Commit/Seek**: Precise offset control.
//! - **Serialization Pipeline**:
//!   - Pluggable formats (JSON, MessagePack, Bincode).
//!   - Compression support (Gzip, Zstd).
//!   - Per-topic and per-publisher configuration overrides.
//!   - Auto-format detection via Magic Headers.
//! - **Management & Monitoring**:
//!   - Topic deletion and creation options (TTL, Max Messages).
//!   - Real-time metrics (depth, subscriber count, lag).
//!
//! ## Usage Examples
//!
//! ### 1. Basic Publish & Subscribe
//!
//! The simplest way to use the queue with default settings.
//!
//! ```rust
//! use tokio_memq::mq::MessageQueue;
//! use tokio_memq::MessageSubscriber; // Import trait for recv()
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mq = MessageQueue::new();
//!     let topic = "demo_topic";
//!
//!     // Subscriber
//!     let sub = mq.subscriber(topic.to_string()).await?;
//!
//!     // Publisher
//!     let pub1 = mq.publisher(topic.to_string());
//!     
//!     // Publish asynchronously so subscriber can receive it
//!     tokio::spawn(async move {
//!         pub1.publish(&"Hello World").await.unwrap();
//!     });
//!
//!     let msg = sub.recv().await?;
//!     let payload: String = msg.deserialize()?;
//!     println!("Received: {}", payload);
//!
//!     Ok(())
//! }
//! ```
//!
//! ### 2. Stream API
//!
//! Consume messages as an async stream, ideal for continuous processing loops.
//!
//! ```rust
//! # use tokio_memq::mq::MessageQueue;
//! # use tokio_memq::MessageSubscriber;
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! # let mq = MessageQueue::new();
//! # let topic = "stream_topic";
//! # let sub = mq.subscriber(topic.to_string()).await?;
//! # let pub1 = mq.publisher(topic.to_string());
//! use tokio_stream::StreamExt;
//! use tokio::pin;
//!
//! // Create a stream from the subscriber
//! let stream = sub.stream();
//! pin!(stream);
//!
//! // Publish a message to trigger the stream
//! tokio::spawn(async move {
//!     pub1.publish(&"Stream Message").await.unwrap();
//! });
//!
//! while let Some(msg_res) = stream.next().await {
//!     match msg_res {
//!         Ok(msg) => println!("Received: {:?}", msg),
//!         Err(e) => eprintln!("Error: {}", e),
//!     }
//!     break; // Break for test
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ### 3. Batch Operations
//!
//! Improve throughput by processing messages in batches.
//!
//! ```rust
//! # use tokio_memq::mq::MessageQueue;
//! # use tokio_memq::MessageSubscriber;
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! # let mq = MessageQueue::new();
//! # let topic = "batch_topic";
//! # let publisher = mq.publisher(topic.to_string());
//! # let sub = mq.subscriber(topic.to_string()).await?;
//! // Batch Publish
//! let messages = vec![1, 2, 3, 4, 5];
//! publisher.publish_batch(messages).await?;
//!
//! // Batch Receive
//! // Returns a vector of up to 10 messages
//! let batch = sub.recv_batch(10).await?; 
//! for msg in batch {
//!     println!("Batch msg: {:?}", msg);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ### 4. Advanced Consumption (Filter, Timeout, Metadata)
//!
//! ```rust
//! # use tokio_memq::mq::MessageQueue;
//! # use tokio_memq::MessageSubscriber;
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! # let mq = MessageQueue::new();
//! # let sub = mq.subscriber("adv_topic".to_string()).await?;
//! use std::time::Duration;
//!
//! // Receive with Timeout
//! match sub.recv_timeout(Duration::from_millis(500)).await? {
//!     Some(msg) => println!("Got msg: {:?}", msg),
//!     None => println!("Timed out"),
//! }
//!
//! // Receive with Filter (Server-side filtering)
//! // Only receive messages where payload size > 100 bytes
//! // Note: This will block until a matching message arrives or channel closes
//! // let large_msg = sub.recv_filter(|msg| msg.payload.len() > 100).await?;
//!
//! // Metadata-only Mode (Avoids full payload clone/deserialization)
//! // let msg = sub.recv().await?;
//! // let meta = msg.metadata();
//! // println!("Offset: {}, Timestamp: {:?}", meta.offset, meta.created_at);
//! # Ok(())
//! # }
//! ```
//!
//! ### 5. Consumer Groups & Offsets
//!
//! Manage offsets manually or use consumer groups for persistent state.
//!
//! ```rust
//! use tokio_memq::mq::{ConsumptionMode, TopicOptions, MessageQueue};
//! use tokio_memq::MessageSubscriber;
//!
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! # let mq = MessageQueue::new();
//! // Configure topic with retention limits
//! let options = TopicOptions {
//!     max_messages: Some(1000),
//!     message_ttl: None,
//!     lru_enabled: true,
//!     ..Default::default()
//! };
//!
//! // Subscribe as part of a Consumer Group
//! // Modes: Earliest, Latest, Offset(n), LastOffset
//! let sub_group = mq.subscriber_group_with_options(
//!     "topic_name".to_string(),
//!     options,
//!     "group_id_1".to_string(),
//!     ConsumptionMode::LastOffset
//! ).await?;
//!
//! // Manual Commit
//! // let msg = sub_group.recv().await?;
//! // sub_group.commit(msg.offset); // Save progress
//! # Ok(())
//! # }
//! ```
//!
//! ### 6. Serialization Configuration
//!
//! Flexible serialization with per-topic and per-publisher overrides.
//!
//! ```rust
//! use tokio_memq::mq::{
//!     SerializationFactory, SerializationFormat, SerializationConfig, 
//!     JsonConfig, PipelineConfig, CompressionConfig
//! };
//!
//! let topic = "compressed_logs";
//!
//! // Configure compression pipeline
//! let pipeline = PipelineConfig {
//!     compression: CompressionConfig::Gzip { level: Some(6) },
//!     pre: None, 
//!     post: None,
//!     use_magic_header: true, // Auto-detect format on receive
//! };
//!
//! // Register defaults for a topic
//! SerializationFactory::register_topic_defaults(
//!     topic,
//!     SerializationFormat::Json,
//!     SerializationConfig::Json(JsonConfig { pretty: false }),
//!     Some(pipeline),
//! );
//! ```
//!
//! ### 7. Partitioned Topics (Sharding)
//! 
//! Create a partitioned topic with multiple routing strategies (RoundRobin, Hash, Random, Fixed),
//! publish messages with automatic routing, subscribe to a specific partition, and inspect per-partition stats.
//!
//! ```rust
//! use tokio_memq::mq::{MessageQueue, TopicOptions, ConsumptionMode, PartitionRouting};
//! # use tokio_memq::mq::TopicMessage;
//! # use tokio_memq::MessageSubscriber;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mq = MessageQueue::new();
//!     let topic = "partitioned_events".to_string();
//!
//!     // Create a topic with 4 partitions
//!     let options = TopicOptions {
//!         max_messages: Some(1000),
//!         partitions: Some(4),
//!         ..Default::default()
//!     };
//!     mq.create_partitioned_topic(topic.clone(), options, 4).await?;
//!
//!     // Set routing strategy
//!     mq.set_partition_routing(topic.clone(), PartitionRouting::RoundRobin).await?;
//!
//!     // Publish a few messages (auto-routed)
//!     for i in 1..=8 {
//!         let message = tokio_memq::TopicMessage::new(topic.clone(), &format!("msg {}", i))?;
//!         mq.publish_to_partitioned(message).await?;
//!     }
//!
//!     // Subscribe to a specific partition (0) and consume one message
//!     let sub0 = mq.subscribe_partition(
//!         topic.clone(), 
//!         0, 
//!         Some("partition_demo".to_string()),
//!         ConsumptionMode::Earliest
//!     ).await?;
//!
//!     if let Some(msg) = sub0.recv_timeout(Duration::from_millis(500)).await? {
//!         println!("Partition 0 got: {}", msg.payload_str());
//!     }
//!
//!     // Inspect partition stats
//!     if let Some(stats) = mq.get_partition_stats(topic.clone(), 0).await {
//!         println!("Partition 0: {} messages, {} subscribers", stats.message_count, stats.subscriber_count);
//!     }
//!
//!     Ok(())
//! }
//! ```

pub mod mq;

pub use mq::traits::{MessagePublisher, AsyncMessagePublisher, MessageSubscriber, QueueManager};
pub use mq::message::{TopicMessage, TopicOptions, TimestampedMessage, ConsumptionMode};
pub use mq::serializer::{
    SerializationFormat, SerializationHelper, Serializer, BincodeSerializer,
    SerializationFactory, SerializationConfig, JsonConfig, MessagePackConfig,
    PipelineConfig, CompressionConfig
};
pub use mq::publisher::Publisher;
pub use mq::subscriber::Subscriber;
pub use mq::broker::{
    TopicManager, PartitionedTopicChannel, PartitionRouting, PartitionStats, TopicStats
};
pub use mq::MessageQueue;
