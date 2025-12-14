use std::time::{Duration, Instant};
use log::{debug, info, error};
use super::serializer::{SerializationFormat, SerializationHelper, SerializationError};

#[derive(Debug, Clone)]
/// 主题配置选项
///
/// Topic configuration options.
pub struct TopicOptions {
    /// 最大消息数（达到后触发 LRU 淘汰）
    ///
    /// Max number of messages (LRU eviction when capacity reached).
    pub max_messages: Option<usize>,
    /// 消息存活时间（TTL），超过后视为过期
    ///
    /// Message time-to-live (TTL); expired messages are skipped.
    pub message_ttl: Option<Duration>,
    /// 是否启用 LRU 淘汰策略
    ///
    /// Enable LRU eviction strategy.
    pub lru_enabled: bool,
    /// 主题空闲超时（预留）
    ///
    /// Topic idle timeout (reserved).
    pub idle_timeout: Option<Duration>,
    /// 消费空闲超时（预留）
    ///
    /// Consume idle timeout (reserved).
    pub consume_idle_timeout: Option<Duration>,
}

impl Default for TopicOptions {
    fn default() -> Self {
        TopicOptions {
            max_messages: Some(10000), 
            message_ttl: None,
            lru_enabled: true,        // 默认开启 LRU / Enable LRU by default
            idle_timeout: None,
            consume_idle_timeout: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
/// 消费模式
///
/// Consumption mode.
pub enum ConsumptionMode {
    /// 从最早消息开始
    ///
    /// Consume from the earliest message.
    Earliest,
    /// 从最新位置开始，仅接收新消息
    ///
    /// Consume from the latest position; only new messages.
    Latest,
    /// 从指定偏移量开始
    ///
    /// Consume from a specified offset.
    Offset(usize),
    /// 从上次提交的偏移量继续（默认）
    ///
    /// Continue from the last committed offset (default).
    #[default]
    LastOffset,
}

#[derive(Debug, Clone)]
/// 含时间戳与偏移量的消息包装
///
/// Message wrapper with timestamp and offset.
pub struct TimestampedMessage {
    pub message: TopicMessage,
    pub created_at: Instant,
    pub offset: usize,
}

impl TimestampedMessage {
    /// 创建新的包装消息
    ///
    /// Create a new timestamped message.
    pub fn new(mut message: TopicMessage, offset: usize) -> Self {
        // 使用 TopicMessage 自带的时间戳 / Use timestamp from TopicMessage
        let created_at = message.created_at;
        message.offset = Some(offset);
        Self {
            message,
            created_at,
            offset,
        }
    }

    /// 是否已过期
    ///
    /// Check if the message is expired by TTL.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }

    /// 获取消息元数据
    ///
    /// Get message metadata.
    pub fn metadata(&self) -> MessageMetadata {
        MessageMetadata {
            topic: self.message.topic.clone(),
            format: self.message.format.clone(),
            created_at: self.created_at,
            offset: self.offset,
            payload_size: self.message.payload.len(),
        }
    }
}

#[derive(Debug, Clone)]
/// 消息元数据（不含负载）
///
/// Message metadata (excluding payload).
pub struct MessageMetadata {
    pub topic: String,
    pub format: SerializationFormat,
    pub created_at: Instant,
    pub offset: usize,
    pub payload_size: usize,
}

#[derive(Debug, Clone)]
/// 主题消息
///
/// Topic message payload.
pub struct TopicMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub format: SerializationFormat,
    pub created_at: Instant,
    pub offset: Option<usize>,
}

impl TopicMessage {
    /// 使用默认序列化格式创建消息
    ///
    /// Create a message using the default serialization format.
    pub fn new<T: serde::Serialize>(topic: String, data: &T) -> Result<Self, SerializationError> {
        let format = Self::default_format();
        debug!("创建消息，主题: {}, 默认格式: {:?} / Creating message, topic: {}, default format: {:?}", topic, format, topic, format);
        
        match SerializationHelper::serialize(data, &format) {
            Ok(payload) => {
                debug!("消息创建成功，主题: {}, 格式: {:?}, 大小: {} 字节 / Message created successfully, topic: {}, format: {:?}, size: {} bytes",
                       topic, format, payload.len(), topic, format, payload.len());
                
                Ok(TopicMessage {
                    topic,
                    payload,
                    format,
                    created_at: Instant::now(),
                    offset: None,
                })
            }
            Err(e) => {
                error!("消息序列化失败，主题: {}, 格式: {:?}, 错误: {} / Message serialization failed, topic: {}, format: {:?}, error: {}", topic, format, e, topic, format, e);
                Err(e)
            }
        }
    }
    
    fn default_format() -> SerializationFormat {
        SerializationFormat::Bincode
    }

    /// 使用指定序列化格式创建消息
    ///
    /// Create a message with a specified `SerializationFormat`.
    pub fn new_with_format<T: serde::Serialize>(
        topic: String, 
        data: &T, 
        format: SerializationFormat
    ) -> Result<Self, SerializationError> {
        debug!("创建消息，主题: {}, 指定格式: {:?} / Creating message, topic: {}, specified format: {:?}", topic, format, topic, format);
        
        match SerializationHelper::serialize(data, &format) {
            Ok(payload) => {
                info!("消息创建成功，主题: {}, 格式: {:?}, 大小: {} 字节 / Message created successfully, topic: {}, format: {:?}, size: {} bytes", 
                      topic, format, payload.len(), topic, format, payload.len());
                
                Ok(TopicMessage {
                    topic,
                    payload,
                    format,
                    created_at: Instant::now(),
                    offset: None,
                })
            }
            Err(e) => {
                error!("消息序列化失败，主题: {}, 格式: {:?}, 错误: {} / Message serialization failed, topic: {}, format: {:?}, error: {}", topic, format, e, topic, format, e);
                Err(e)
            }
        }
    }

    /// 从字节数组创建消息
    ///
    /// Create a message from raw bytes.
    pub fn from_bytes(topic: String, data: Vec<u8>, format: SerializationFormat) -> Self {
        debug!("从字节数据创建消息，主题: {}, 格式: {:?}, 大小: {} 字节 / Creating message from bytes, topic: {}, format: {:?}, size: {} bytes", 
               topic, format, data.len(), topic, format, data.len());
        
        TopicMessage {
            topic,
            payload: data,
            format,
            created_at: Instant::now(),
            offset: None,
        }
    }

    /// 从字符串创建消息（自动探测格式）
    ///
    /// Create a message from serialized string (auto-detect format).
    pub fn from_serialized(topic: String, payload: String) -> Self {
        let bytes = payload.into_bytes();
        let format = SerializationHelper::auto_detect_format(&bytes);
        
        debug!("从序列化字符串创建消息，主题: {}, 自动检测格式: {:?}, 大小: {} 字节 / Creating message from serialized string, topic: {}, auto-detected format: {:?}, size: {} bytes", 
               topic, format, bytes.len(), topic, format, bytes.len());
        
        TopicMessage {
            topic,
            payload: bytes,
            format,
            created_at: Instant::now(),
            offset: None,
        }
    }

    /// 获取负载的字符串表示（有损转换）
    ///
    /// Get string representation of payload (lossy conversion).
    pub fn payload_str(&self) -> std::borrow::Cow<str> {
        String::from_utf8_lossy(&self.payload)
    }

    /// 创建 Bincode 格式消息
    ///
    /// Create a message in `Bincode` format.
    pub fn from_bincode(topic: String, data: Vec<u8>) -> Self {
        TopicMessage {
            topic,
            payload: data,
            format: SerializationFormat::Bincode,
            created_at: Instant::now(),
            offset: None,
        }
    }

    /// 判断消息是否按 TTL 过期
    ///
    /// Check if message is expired based on TTL.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }

    /// 反序列化负载为目标类型
    ///
    /// Deserialize payload into target type.
    pub fn deserialize<T: serde::de::DeserializeOwned>(&self) -> Result<T, SerializationError> {
        SerializationHelper::deserialize(&self.payload, &self.format)
    }

    /// 使用指定格式反序列化负载
    ///
    /// Deserialize payload with a specified `SerializationFormat`.
    pub fn deserialize_with_format<T: serde::de::DeserializeOwned>(
        &self, 
        format: &SerializationFormat
    ) -> Result<T, SerializationError> {
        SerializationHelper::deserialize(&self.payload, format)
    }

    /// 获取原始字节负载
    ///
    /// Get raw payload bytes.
    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload
    }

    /// 获取字符串负载
    ///
    /// Get payload as string.
    pub fn payload(&self) -> std::borrow::Cow<str> {
        self.payload_str()
    }

    /// 获取序列化格式
    ///
    /// Get serialization format.
    pub fn serialization_format(&self) -> &SerializationFormat {
        &self.format
    }

    /// 展示负载摘要（截断以便日志输出）
    ///
    /// Display payload summary (truncated for logging).
    pub fn display_payload(&self, max_len: usize) -> String {
        let s = self.payload_str();
        let truncated = if s.len() > max_len {
            format!("{}...", &s[..max_len])
        } else {
            s.into_owned()
        };
        format!(
            "{} (len={} fmt={})",
            truncated,
            self.payload.len(),
            self.format.as_str()
        )
    }

    /// 获取消息元数据
    ///
    /// Get message metadata.
    pub fn metadata(&self) -> MessageMetadata {
        MessageMetadata {
            topic: self.topic.clone(),
            format: self.format.clone(),
            created_at: self.created_at,
            offset: self.offset.unwrap_or(0),
            payload_size: self.payload.len(),
        }
    }
}
