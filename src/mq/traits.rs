// Publisher trait
pub trait MessagePublisher: Send + Sync {
    fn topic(&self) -> &str;
}

/// 异步版本的 Publisher trait / Async version of Publisher trait
/// 用于异步发布消息到指定主题 / Used for asynchronously publishing messages to a specific topic
#[async_trait::async_trait]
pub trait AsyncMessagePublisher: Send + Sync {
    /// 获取当前发布者对应的主题名称 / Get the topic name corresponding to the current publisher
    fn topic(&self) -> &str;
}

/// Subscriber trait - 定义订阅者接口 / Subscriber trait - Defines the subscriber interface
/// 提供异步接收消息及偏移量管理功能 / Provides asynchronous message reception and offset management functions
#[async_trait::async_trait]
pub trait MessageSubscriber: Send + Sync {
    /// 阻塞式接收消息，直到有消息到达或发生错误 / Blocking receive message until a message arrives or an error occurs
    async fn recv(&self) -> anyhow::Result<crate::mq::message::TopicMessage>;
    /// 非阻塞式尝试接收消息，立即返回结果 / Non-blocking attempt to receive message, returns result immediately
    async fn try_recv(&self) -> anyhow::Result<crate::mq::message::TopicMessage>;
    /// 获取当前消费偏移量 / Get current consumption offset
    async fn current_offset(&self) -> Option<usize>;
    /// 重置消费偏移量到初始位置 / Reset consumption offset to initial position
    async fn reset_offset(&self);
    /// 获取订阅者对应的主题名称 / Get the topic name corresponding to the subscriber
    fn topic(&self) -> &str;
}

/// QueueManager trait - 定义队列管理接口 / QueueManager trait - Defines queue management interface
/// 负责创建发布者与订阅者，并管理主题列表 / Responsible for creating publishers and subscribers, and managing topic lists
#[async_trait::async_trait]
pub trait QueueManager: Send + Sync {
    /// 根据主题名称创建对应的发布者实例 / Create corresponding publisher instance based on topic name
    async fn create_publisher(&self, topic: String) -> Box<dyn MessagePublisher>;
    /// 根据主题名称创建对应的订阅者实例 / Create corresponding subscriber instance based on topic name
    async fn create_subscriber(&self, topic: String) -> anyhow::Result<Box<dyn MessageSubscriber>>;
    /// 列出当前所有可用的主题 / List all currently available topics
    async fn list_topics(&self) -> Vec<String>;
}
