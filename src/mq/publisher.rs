use crate::mq::traits::{MessagePublisher, AsyncMessagePublisher};
use crate::mq::message::TopicMessage;
use crate::mq::serializer::SerializationFormat;
use crate::mq::broker::TopicManager;
use log::{debug, info};

#[derive(Clone)]
/// 主题发布者
///
/// Topic publisher.
pub struct Publisher {
    topic_manager: TopicManager,
    topic: String,
}

impl Publisher {
    /// 创建新的发布者
    ///
    /// Create a new publisher.
    pub fn new(topic_manager: TopicManager, topic: String) -> Self {
        debug!("创建新的发布者，主题: {} / Creating new publisher, topic: {}", topic, topic);
        Publisher { topic_manager, topic }
    }

    /// 序列化并发布任意可序列化数据
    ///
    /// Serialize and publish any `serde::Serialize` data.
    pub async fn publish<T: serde::Serialize>(&self, data: &T) -> anyhow::Result<()> {
        debug!("开始序列化并发布消息到主题: {} / Start serializing and publishing message to topic: {}", self.topic, self.topic);
        
        let message = TopicMessage::new(self.topic.clone(), data)?;
        debug!("消息创建成功，格式: {:?}, 大小: {} 字节 / Message created successfully, format: {:?}, size: {} bytes", message.format, message.payload.len(), message.format, message.payload.len());
        info!("发布内容到主题 {}: {} / Publishing content to topic {}: {}", self.topic, message.payload_str, self.topic, message.payload_str);
        
        self.topic_manager.publish(message).await
    }

    /// 发布已序列化的字符串负载
    ///
    /// Publish a pre-serialized string payload.
    pub async fn publish_serialized(&self, payload: String) -> anyhow::Result<()> {
        debug!("发布已序列化的字符串数据到主题: {}, 大小: {} 字节 / Publishing serialized string data to topic: {}, size: {} bytes", self.topic, payload.len(), self.topic, payload.len());
        
        let message = TopicMessage::from_serialized(self.topic.clone(), payload);
        info!("发布内容到主题 {}: {} / Publishing content to topic {}: {}", self.topic, message.payload_str, self.topic, message.payload_str);
        self.topic_manager.publish(message).await
    }

    /// 发布普通字符串（与 `publish_serialized` 等价）
    ///
    /// Publish a plain string (alias to `publish_serialized`).
    pub async fn publish_string(&self, payload: String) -> anyhow::Result<()> {
        self.publish_serialized(payload).await
    }

    /// 指定序列化格式发布数据
    ///
    /// Publish data using a specified `SerializationFormat`.
    pub async fn publish_with_format<T: serde::Serialize>(
        &self, 
        data: &T, 
        format: SerializationFormat
    ) -> anyhow::Result<()> {
        debug!("使用 {:?} 格式发布消息到主题: {} / Publishing message to topic: {} using format: {:?}", format, self.topic, self.topic, format);
        
        let message = TopicMessage::new_with_format(self.topic.clone(), data, format)?;
        debug!("消息序列化完成，格式: {:?}, 大小: {} 字节 / Message serialization completed, format: {:?}, size: {} bytes", message.format, message.payload.len(), message.format, message.payload.len());
        info!("发布内容到主题 {}: {} / Publishing content to topic {}: {}", self.topic, message.payload_str, self.topic, message.payload_str);
        
        self.topic_manager.publish(message).await
    }

    /// 使用 Bincode 格式发布数据
    ///
    /// Publish data using `Bincode` format.
    pub async fn publish_bincode<T: serde::Serialize>(&self, data: &T) -> anyhow::Result<()> {
        self.publish_with_format(data, SerializationFormat::Bincode).await
    }

    /// 发布字节数组负载并指定格式
    ///
    /// Publish raw bytes with specified `SerializationFormat`.
    pub async fn publish_bytes(&self, data: Vec<u8>, format: SerializationFormat) -> anyhow::Result<()> {
        let message = TopicMessage::from_bytes(self.topic.clone(), data, format);
        info!("发布内容到主题 {}: {} / Publishing content to topic {}: {}", self.topic, message.payload_str, self.topic, message.payload_str);
        self.topic_manager.publish(message).await
    }

    /// 获取发布者主题名称
    ///
    /// Get publisher topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl MessagePublisher for Publisher {
    fn topic(&self) -> &str {
        &self.topic
    }
}

#[async_trait::async_trait]
impl AsyncMessagePublisher for Publisher {
    fn topic(&self) -> &str {
        &self.topic
    }
}
