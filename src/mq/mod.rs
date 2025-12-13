pub mod traits;
pub mod message;
pub mod serializer;
pub mod publisher;
pub mod subscriber;
pub mod broker;

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
        self.topic_manager.get_or_create_topic_with_options(topic, options).await;
        Ok(())
    }

    /// 删除主题
    ///
    /// Delete a topic.
    pub async fn delete_topic(&self, topic: &str) -> bool {
        self.topic_manager.delete_topic(topic).await
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
