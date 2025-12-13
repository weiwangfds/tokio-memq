use crate::mq::traits::{MessagePublisher, AsyncMessagePublisher};
use crate::mq::message::TopicMessage;
use crate::mq::serializer::{SerializationFormat, SerializationFactory, SerializationHelper};
use crate::mq::broker::TopicManager;
use log::{debug, info};

#[derive(Clone)]
/// 主题发布者
///
/// Topic publisher.
pub struct Publisher {
    topic_manager: TopicManager,
    topic: String,
    publisher_key: Option<String>,
}

impl Publisher {
    /// 创建新的发布者
    ///
    /// Create a new publisher.
    pub fn new(topic_manager: TopicManager, topic: String) -> Self {
        debug!("创建新的发布者，主题: {} / Creating new publisher, topic: {}", topic, topic);
        Publisher { topic_manager, topic, publisher_key: None }
    }

    /// 使用发布者键创建新的发布者（同一主题可有不同默认设置）
    ///
    /// Create a new publisher with a specific key (per-publisher defaults).
    pub fn new_with_key(topic_manager: TopicManager, topic: String, publisher_key: String) -> Self {
        debug!("创建带发布者键的发布者，主题: {}, 键: {} / Creating keyed publisher, topic: {}, key: {}", topic, publisher_key, topic, publisher_key);
        Publisher { topic_manager, topic, publisher_key: Some(publisher_key) }
    }

    /// 序列化并发布任意可序列化数据
    ///
    /// Serialize and publish any `serde::Serialize` data.
    pub async fn publish<T: serde::Serialize>(&self, data: &T) -> anyhow::Result<()> {
        debug!("开始序列化并发布消息到主题: {} / Start serializing and publishing message to topic: {}", self.topic, self.topic);

        let keyed = self.publisher_key.as_ref().and_then(|k| SerializationFactory::get_publisher_defaults(k));
        let message = if let Some(settings) = keyed.or_else(|| SerializationFactory::get_topic_defaults(&self.topic)) {
            let payload = SerializationHelper::serialize_with_settings(data, &settings)?;
            let format = settings.format.clone();
            TopicMessage::from_bytes(self.topic.clone(), payload, format)
        } else {
            TopicMessage::new(self.topic.clone(), data)?
        };
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

    /// 获取发布者键（如存在）
    ///
    /// Get publisher key if present.
    pub fn publisher_key(&self) -> Option<&str> {
        self.publisher_key.as_deref()
    }

    /// 批量发布消息
    ///
    /// Batch publish messages.
    pub async fn publish_batch<T: serde::Serialize>(&self, data_list: Vec<T>) -> anyhow::Result<()> {
        let mut messages = Vec::with_capacity(data_list.len());
        for data in data_list {
            let keyed = self.publisher_key.as_ref().and_then(|k| SerializationFactory::get_publisher_defaults(k));
            if let Some(settings) = keyed.or_else(|| SerializationFactory::get_topic_defaults(&self.topic)) {
                let payload = SerializationHelper::serialize_with_settings(&data, &settings)?;
                let format = settings.format.clone();
                messages.push(TopicMessage::from_bytes(self.topic.clone(), payload, format));
            } else {
                messages.push(TopicMessage::new(self.topic.clone(), &data)?);
            }
        }
        self.topic_manager.publish_batch(messages).await
    }

    /// 使用指定格式批量发布消息
    ///
    /// Batch publish messages with specified format.
    pub async fn publish_batch_with_format<T: serde::Serialize>(
        &self, 
        data_list: Vec<T>, 
        format: SerializationFormat
    ) -> anyhow::Result<()> {
        let mut messages = Vec::with_capacity(data_list.len());
        for data in data_list {
            messages.push(TopicMessage::new_with_format(self.topic.clone(), &data, format.clone())?);
        }
        self.topic_manager.publish_batch(messages).await
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

#[cfg(test)]
mod tests {
    // use super::*; // Unused
    use crate::mq::MessageQueue;
    use crate::mq::serializer::{SerializationFactory, SerializationConfig, JsonConfig, MessagePackConfig};
    use crate::mq::serializer::SerializationFormat;

    #[tokio::test]
    async fn test_per_publisher_defaults() -> anyhow::Result<()> {
        let mq = MessageQueue::new();
        let topic = "same_topic".to_string();

        // Register different defaults for two publishers on the same topic
        SerializationFactory::register_publisher_defaults(
            "pubA",
            SerializationFormat::Json,
            SerializationConfig::Json(JsonConfig { pretty: true }),
            None,
        );
        SerializationFactory::register_publisher_defaults(
            "pubB",
            SerializationFormat::MessagePack,
            SerializationConfig::MessagePack(MessagePackConfig { struct_map: false }),
            None,
        );

        // Also register a topic-level default to verify publisher-level overrides
        SerializationFactory::register_topic_defaults(
            &topic,
            SerializationFormat::Json,
            SerializationConfig::Json(JsonConfig { pretty: false }),
            None,
        );

        let pub_a = mq.publisher_with_key(topic.clone(), "pubA".to_string());
        let pub_b = mq.publisher_with_key(topic.clone(), "pubB".to_string());
        let subscriber = mq.subscriber(topic.clone()).await?;

        pub_a.publish(&serde_json::json!({"a":1})).await?;
        pub_b.publish(&serde_json::json!({"b":2})).await?;

        let batch = subscriber.recv_batch(2).await?;
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].serialization_format(), &SerializationFormat::Json);
        assert_eq!(batch[1].serialization_format(), &SerializationFormat::MessagePack);
        Ok(())
    }
}
