// Publisher trait
pub trait MessagePublisher: Send + Sync {
    fn topic(&self) -> &str;
}

use crate::mq::serializer::SerializationFormat;

/// 异步版本的 Publisher trait / Async version of Publisher trait
/// 用于异步发布消息到指定主题 / Used for asynchronously publishing messages to a specific topic
#[async_trait::async_trait]
pub trait AsyncMessagePublisher: Send + Sync {
    /// 获取当前发布者对应的主题名称 / Get the topic name corresponding to the current publisher
    fn topic(&self) -> &str;

    /// 发布任意可序列化数据 / Publish any serializable data
    async fn publish<T: serde::Serialize + Send + Sync + 'static>(&self, data: T) -> anyhow::Result<()>;

    /// 批量发布任意可序列化数据 / Batch publish any serializable data
    async fn publish_batch<T: serde::Serialize + Send + Sync + 'static>(&self, data_list: Vec<T>) -> anyhow::Result<()>;

    /// 发布已序列化的字符串负载 / Publish a pre-serialized string payload
    async fn publish_serialized(&self, payload: String) -> anyhow::Result<()>;

    /// 发布字节数组负载并指定格式 / Publish raw bytes with specified `SerializationFormat`
    async fn publish_bytes(&self, data: Vec<u8>, format: SerializationFormat) -> anyhow::Result<()>;
}

/// Subscriber trait - 定义订阅者接口 / Subscriber trait - Defines the subscriber interface
/// 提供异步接收消息及偏移量管理功能 / Provides asynchronous message reception and offset management functions
#[async_trait::async_trait]
pub trait MessageSubscriber: Send + Sync {
    /// 阻塞式接收消息，直到有消息到达或发生错误 / Blocking receive message until a message arrives or an error occurs
    async fn recv(&self) -> anyhow::Result<crate::mq::message::TopicMessage>;
    
    /// 非阻塞式尝试接收消息，立即返回结果 / Non-blocking attempt to receive message, returns result immediately
    async fn try_recv(&self) -> anyhow::Result<crate::mq::message::TopicMessage>;
    
    /// 接收并自动反序列化为指定类型 / Receive and automatically deserialize into specified type
    async fn recv_typed<T: serde::de::DeserializeOwned + Send + Sync + 'static + Clone>(&self) -> anyhow::Result<T>;
    
    /// 批量接收消息 / Batch receive messages
    async fn recv_batch(&self, n: usize) -> anyhow::Result<Vec<crate::mq::message::TopicMessage>>;

    /// 批量接收并反序列化消息 / Batch receive and deserialize messages
    async fn recv_batch_typed<T: serde::de::DeserializeOwned + Send + Sync + 'static + Clone>(&self, n: usize) -> anyhow::Result<Vec<T>>;

    /// 获取当前消费偏移量 / Get current consumption offset
    async fn current_offset(&self) -> Option<usize>;
    
    /// 重置消费偏移量到初始位置 / Reset consumption offset to initial position
    async fn reset_offset(&self);

    /// 手动提交偏移量 / Manually commit offset
    async fn commit(&self, offset: usize) -> anyhow::Result<()>;

    /// 跳转到指定偏移量 / Seek to specific offset
    async fn seek(&self, offset: usize) -> anyhow::Result<()>;

    /// 创建消息流 / Create a stream of messages
    fn stream(&self) -> impl tokio_stream::Stream<Item = anyhow::Result<crate::mq::message::TopicMessage>> + '_;

    /// 带超时的接收消息 / Receive message with timeout
    async fn recv_timeout(&self, duration: std::time::Duration) -> anyhow::Result<Option<crate::mq::message::TopicMessage>>;

    /// 获取订阅者对应的主题名称 / Get the topic name corresponding to the subscriber
    fn topic(&self) -> &str;
}

/// QueueManager trait - 定义队列管理接口 / QueueManager trait - Defines queue management interface
/// 负责创建发布者与订阅者，并管理主题列表 / Responsible for creating publishers and subscribers, and managing topic lists
#[async_trait::async_trait]
pub trait QueueManager: Send + Sync {
    /// 发布者类型 / Publisher type
    type Publisher: AsyncMessagePublisher;
    /// 订阅者类型 / Subscriber type
    type Subscriber: MessageSubscriber;

    /// 根据主题名称创建对应的发布者实例 / Create corresponding publisher instance based on topic name
    async fn create_publisher(&self, topic: String) -> Self::Publisher;
    /// 根据主题名称创建对应的订阅者实例 / Create corresponding subscriber instance based on topic name
    async fn create_subscriber(&self, topic: String) -> anyhow::Result<Self::Subscriber>;
    /// 列出当前所有可用的主题 / List all currently available topics
    async fn list_topics(&self) -> Vec<String>;
}
