use std::sync::Arc;
pub mod traits;
pub mod message;
pub mod serializer;
pub mod publisher;
pub mod subscriber;
pub mod broker;

// Re-export partition types
pub use broker::{PartitionRouting, PartitionStats};

pub use traits::{MessagePublisher, AsyncMessagePublisher, MessageSubscriber, QueueManager};
pub use message::{TopicMessage, TopicOptions, TimestampedMessage, ConsumptionMode};
pub use serializer::{
    SerializationFormat, 
    SerializationHelper, 
    Serializer, 
    BincodeSerializer,
    SerializationFactory,
    SerializationConfig,
    JsonConfig,
    MessagePackConfig,
    PipelineConfig,
    CompressionConfig
};
pub use publisher::Publisher;
pub use subscriber::Subscriber;
pub use broker::{TopicManager, TopicStats};

/// 简单高性能的异步内存消息队列
/// 
/// Simple, high-performance in-memory async message queue.
pub struct MessageQueue {
    topic_manager: TopicManager,
}

impl MessageQueue {
    /// 创建新的消息队列实例
    ///
    /// Create a new message queue instance.
    pub fn new() -> Self {
        log::debug!("创建新的消息队列实例 / Creating new MessageQueue instance");
        let topic_manager = TopicManager::new();
        log::trace!("消息队列初始化完成 / MessageQueue initialization completed");
        MessageQueue { topic_manager }
    }

    /// 获取主题统计信息
    ///
    /// Get topic statistics.
    pub async fn get_topic_stats(&self, topic: String) -> Option<TopicStats> {
        self.topic_manager.get_topic_stats(&topic).await
    }


    /// 创建指定主题的发布者
    ///
    /// Create a publisher for a topic.
    pub fn publisher(&self, topic: String) -> Publisher {
        Publisher::new(self.topic_manager.clone(), topic)
    }

    /// 创建带发布者键的发布者（每发布者独立默认设置）
    ///
    /// Create a publisher with a key (per-publisher defaults).
    pub fn publisher_with_key(&self, topic: String, publisher_key: String) -> Publisher {
        Publisher::new_with_key(self.topic_manager.clone(), topic, publisher_key)
    }

    /// 订阅指定主题（使用默认配置）
    ///
    /// Subscribe to a topic with default options.
    pub async fn subscriber(&self, topic: String) -> anyhow::Result<Subscriber> {
        self.topic_manager.subscribe(topic).await
    }

    /// 订阅指定主题（自定义 TopicOptions）
    ///
    /// Subscribe to a topic with custom `TopicOptions`.
    pub async fn subscriber_with_options(&self, topic: String, options: TopicOptions) -> anyhow::Result<Subscriber> {
        self.topic_manager.subscribe_with_options(topic, options).await
    }
    
    /// 订阅指定主题（自定义 TopicOptions 且指定消费模式）
    ///
    /// Subscribe to a topic with custom `TopicOptions` and `ConsumptionMode`.
    pub async fn subscriber_with_options_and_mode(&self, topic: String, options: TopicOptions, mode: ConsumptionMode) -> anyhow::Result<Subscriber> {
        self.topic_manager.subscribe_with_options_and_mode(topic, options, mode).await
    }

    /// 按消费者组 ID 订阅主题（支持偏移量模式）
    ///
    /// Subscribe to a topic with a consumer group ID and `ConsumptionMode`.
    pub async fn subscriber_group(&self, topic: String, consumer_id: String, mode: ConsumptionMode) -> anyhow::Result<Subscriber> {
        self.topic_manager.subscribe_group(topic, consumer_id, mode).await
    }

    /// 按消费者组 ID 订阅主题（带选项与偏移量模式）
    ///
    /// Subscribe with `TopicOptions`, consumer group ID, and `ConsumptionMode`.
    pub async fn subscriber_group_with_options(&self, topic: String, options: TopicOptions, consumer_id: String, mode: ConsumptionMode) -> anyhow::Result<Subscriber> {
        self.topic_manager.subscribe_group_with_options(topic, options, consumer_id, mode).await
    }

    /// 列出当前所有主题
    ///
    /// List all current topics.
    pub async fn list_topics(&self) -> Vec<String> {
        self.topic_manager.list_topics().await
    }

    /// 创建或更新主题配置
    ///
    /// Create a topic with options (idempotent).
    pub async fn create_topic(&self, topic: String, options: TopicOptions) -> anyhow::Result<()> {
        self.topic_manager.get_or_create_topic_with_options(Arc::new(topic), options).await;
        Ok(())
    }

    /// 删除主题
    ///
    /// Delete a topic.
    pub async fn delete_topic(&self, topic: &str) -> bool {
        self.topic_manager.delete_topic(topic).await
    }

    // ========== 分区主题 API ==========

    /// 创建分区主题
    ///
    /// Create a partitioned topic.
    pub async fn create_partitioned_topic(&self, topic: String, options: TopicOptions, partition_count: usize) -> anyhow::Result<()> {
        self.topic_manager.get_or_create_partitioned_topic(topic, options, partition_count).await?;
        Ok(())
    }

    /// 发布消息到分区主题（自动路由）
    ///
    /// Publish message to partitioned topic with automatic routing.
    pub async fn publish_to_partitioned(&self, message: TopicMessage) -> anyhow::Result<()> {
        self.topic_manager.publish_to_partitioned(message).await
    }

    /// 批量发布消息到分区主题（自动路由）
    ///
    /// Publish batch messages to partitioned topic with automatic routing.
    pub async fn publish_batch_to_partitioned(&self, messages: Vec<TopicMessage>) -> anyhow::Result<()> {
        self.topic_manager.publish_batch_to_partitioned(messages).await
    }

    /// 订阅分区主题的指定分区
    ///
    /// Subscribe to a specific partition of a partitioned topic.
    pub async fn subscribe_partition(
        &self,
        topic: String,
        partition: usize,
        consumer_id: Option<String>,
        mode: ConsumptionMode,
    ) -> anyhow::Result<Subscriber> {
        self.topic_manager.subscribe_partition(topic, partition, consumer_id, mode).await
    }

    /// 订阅分区主题的指定分区（使用选项）
    ///
    /// Subscribe to a specific partition of a partitioned topic with options.
    pub async fn subscribe_partition_with_options(
        &self,
        topic: String,
        partition: usize,
        options: TopicOptions,
        consumer_id: Option<String>,
        mode: ConsumptionMode,
    ) -> anyhow::Result<Subscriber> {
        self.create_partitioned_topic(topic.clone(), options, partition + 1).await?;
        self.subscribe_partition(topic, partition, consumer_id, mode).await
    }

    /// 获取分区统计信息
    ///
    /// Get partition statistics.
    pub async fn get_partition_stats(&self, topic: String, partition: usize) -> Option<PartitionStats> {
        self.topic_manager.get_partition_stats(topic, partition).await
    }

    /// 获取所有分区统计信息
    ///
    /// Get all partition statistics for a topic.
    pub async fn get_all_partition_stats(&self, topic: String) -> Vec<PartitionStats> {
        self.topic_manager.get_all_partition_stats(topic).await
    }

    /// 设置分区路由策略
    ///
    /// Set partition routing strategy.
    pub async fn set_partition_routing(&self, topic: String, strategy: PartitionRouting) -> anyhow::Result<()> {
        self.topic_manager.set_partition_routing(topic, strategy).await
    }

    /// 删除分区主题
    ///
    /// Delete a partitioned topic.
    pub async fn delete_partitioned_topic(&self, topic: &str) -> bool {
        self.topic_manager.delete_partitioned_topic(topic).await
    }

    /// 列出所有分区主题
    ///
    /// List all partitioned topics.
    pub async fn list_partitioned_topics(&self) -> Vec<String> {
        self.topic_manager.list_partitioned_topics().await
    }
}

impl Default for MessageQueue {
    /// 默认构造，等价于 `MessageQueue::new()`
    ///
    /// Default constructor, same as `MessageQueue::new()`.
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MessageQueue {
    /// 克隆消息队列实例（共享内部 TopicManager）
    ///
    /// Clone the message queue (shares internal `TopicManager`).
    fn clone(&self) -> Self {
        MessageQueue {
            topic_manager: self.topic_manager.clone(),
        }
    }
}
