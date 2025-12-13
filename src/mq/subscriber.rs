use async_channel::Receiver;
use std::collections::{VecDeque, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Notify};
use log::{debug, info, error};

use crate::mq::traits::MessageSubscriber;
use crate::mq::message::{TopicMessage, TimestampedMessage, TopicOptions, ConsumptionMode};
use crate::mq::serializer::SerializationFormat;

pub struct Subscriber {
    pub topic_name: String,
    receiver: Arc<Mutex<Receiver<TopicMessage>>>,
    _subscriber_count: Arc<Mutex<usize>>,
    current_offset: Arc<Mutex<Option<usize>>>,
    message_buffer: Arc<Mutex<VecDeque<TimestampedMessage>>>,
    pub options: TopicOptions,
    pub consumer_id: Option<String>,
    pub consumption_mode: ConsumptionMode,
    consumer_offsets: Arc<RwLock<HashMap<String, usize>>>,
    channel_notify: Arc<Notify>,
}

impl Subscriber {
    pub fn new(
        topic_name: String,
        receiver: Arc<Mutex<Receiver<TopicMessage>>>,
        subscriber_count: Arc<Mutex<usize>>,
        current_offset: Arc<Mutex<Option<usize>>>,
        message_buffer: Arc<Mutex<VecDeque<TimestampedMessage>>>,
        options: TopicOptions,
        consumer_id: Option<String>,
        consumption_mode: ConsumptionMode,
        consumer_offsets: Arc<RwLock<HashMap<String, usize>>>,
        channel_notify: Arc<Notify>,
    ) -> Self {
        Subscriber {
            topic_name,
            receiver,
            _subscriber_count: subscriber_count,
            current_offset,
            message_buffer,
            options,
            consumer_id,
            consumption_mode,
            consumer_offsets,
            channel_notify,
        }
    }

    async fn recv_log_based(&self, consumer_id: &str) -> anyhow::Result<TopicMessage> {
        loop {
            // 注册通知监听（防止检查后、等待前的竞态条件） / Register notification listener (prevent race condition between check and wait)
            let notification = self.channel_notify.notified();

            // 获取当前需要消费的 offset / Get current offset to consume
            let target_offset = {
                let offsets = self.consumer_offsets.read().await;
                *offsets.get(consumer_id).unwrap_or(&0)
            };

            // 尝试从 buffer 获取 / Try to get from buffer
            let message_opt = {
                let buffer = self.message_buffer.lock().await;
                
                // 查找 >= target_offset 的第一条消息 / Find first message >= target_offset
                if buffer.is_empty() {
                    debug!("[Subscriber {}] Buffer empty / 缓冲区为空", consumer_id);
                    None
                } else {
                    let last_offset = buffer.back().unwrap().offset;
                    if last_offset < target_offset {
                        debug!("[Subscriber {}] Waiting for new message. Target: {}, Last in buffer: {} / 等待新消息...", consumer_id, target_offset, last_offset);
                        None // 等待新消息 / Waiting for new message
                    } else {
                        let mut found_msg = None;
                        
                        // 检查是否落后太多（target_offset < front_offset） / Check if lagging too far behind (target_offset < front_offset)
                        let front_offset = buffer.front().unwrap().offset;
                        if target_offset < front_offset {
                            debug!("消费者落后太多，发生越界 (Target: {}, Front: {})，重置为 Front / Consumer lagging too far, boundary crossing (Target: {}, Front: {}), resetting to Front", target_offset, front_offset, target_offset, front_offset);
                            found_msg = Some(buffer.front().unwrap().clone());
                        } else {
                            // 正常查找 / Normal search
                            for msg in buffer.iter() {
                                if msg.offset >= target_offset {
                                    // 检查是否过期 / Check if expired
                                    if let Some(ttl) = self.options.message_ttl {
                                        if msg.is_expired(ttl) {
                                            continue; // 跳过过期消息 / Skip expired message
                                        }
                                    }
                                    found_msg = Some(msg.clone());
                                    break;
                                }
                            }
                        }
                        found_msg
                    }
                }
            };

            if let Some(msg) = message_opt {
                debug!("[Subscriber {}] Found message offset: {} / 找到消息偏移量: {}", consumer_id, msg.offset, msg.offset);
                // 更新 offset / Update offset
                let next_offset = msg.offset + 1;
                {
                    let mut offsets = self.consumer_offsets.write().await;
                    offsets.insert(consumer_id.to_string(), next_offset);
                }
                
                // 更新 Subscriber 的 current_offset 供查询 / Update Subscriber's current_offset for query
                let mut current_offset = self.current_offset.lock().await;
                *current_offset = Some(msg.offset);

                return Ok(msg.message);
            } else {
                debug!("[Subscriber {}] Waiting for notification... / 等待通知...", consumer_id);
                // 没找到消息，等待通知 / No message found, waiting for notification
                notification.await;
                debug!("[Subscriber {}] Woke up from notification / 从通知中唤醒", consumer_id);
            }
        }
    }

    async fn try_recv_log_based(&self, consumer_id: &str) -> anyhow::Result<TopicMessage> {
        let target_offset = {
            let offsets = self.consumer_offsets.read().await;
            *offsets.get(consumer_id).unwrap_or(&0)
        };

        let message_opt = {
            let buffer = self.message_buffer.lock().await;
            if buffer.is_empty() {
                None
            } else {
                let last_offset = buffer.back().unwrap().offset;
                if last_offset < target_offset {
                    None
                } else {
                    let mut found_msg = None;
                    let front_offset = buffer.front().unwrap().offset;
                    if target_offset < front_offset {
                        found_msg = Some(buffer.front().unwrap().clone());
                    } else {
                        for msg in buffer.iter() {
                            if msg.offset >= target_offset {
                                if let Some(ttl) = self.options.message_ttl {
                                    if msg.is_expired(ttl) {
                                        continue;
                                    }
                                }
                                found_msg = Some(msg.clone());
                                break;
                            }
                        }
                    }
                    found_msg
                }
            }
        };

        if let Some(msg) = message_opt {
            let next_offset = msg.offset + 1;
            {
                let mut offsets = self.consumer_offsets.write().await;
                offsets.insert(consumer_id.to_string(), next_offset);
            }
            
            let mut current_offset = self.current_offset.lock().await;
            *current_offset = Some(msg.offset);

            Ok(msg.message)
        } else {
            Err(anyhow::anyhow!("No message available (Log-based)"))
        }
    }
}

impl Subscriber {
    pub async fn recv_typed<T: serde::de::DeserializeOwned>(&self) -> anyhow::Result<T> {
        debug!("等待接收类型化消息，主题: {} / Waiting to receive typed message, topic: {}", self.topic_name, self.topic_name);
        
        let message = self.recv().await?;
        debug!("接收到消息，主题: {}, 大小: {} 字节, 格式: {:?} / Message received, topic: {}, size: {} bytes, format: {:?}", self.topic_name, message.payload.len(), message.format, self.topic_name, message.payload.len(), message.format);
        
        match message.deserialize::<T>() {
            Ok(data) => {
                debug!("消息反序列化成功，主题: {} / Message deserialization successful, topic: {}", self.topic_name, self.topic_name);
                Ok(data)
            }
            Err(e) => {
                error!("消息反序列化失败，主题: {}, 错误: {} / Message deserialization failed, topic: {}, error: {}", self.topic_name, e, self.topic_name, e);
                Err(anyhow::anyhow!("反序列化错误: {} / Deserialization error: {}", e, e))
            }
        }
    }

    pub async fn recv_string(&self) -> anyhow::Result<String> {
        debug!("等待接收字符串消息，主题: {} / Waiting to receive string message, topic: {}", self.topic_name, self.topic_name);
        
        let message = self.recv().await?;
        let payload = message.payload().to_string();
        info!("接收到消息，主题: {}, {} / Message received, topic: {}, {}", self.topic_name, message.payload_str, self.topic_name, message.payload_str);
        Ok(payload)
    }

    pub async fn try_recv_typed<T: serde::de::DeserializeOwned>(&self) -> anyhow::Result<T> {
        debug!("尝试接收类型化消息，主题: {} / Attempting to receive typed message, topic: {}", self.topic_name, self.topic_name);
        
        let message = self.try_recv().await?;
        info!("接收到消息（非阻塞），主题: {}, 大小: {} 字节, 格式: {:?} / Message received (non-blocking), topic: {}, size: {} bytes, format: {:?}", self.topic_name, message.payload.len(), message.format, self.topic_name, message.payload.len(), message.format);
        
        match message.deserialize::<T>() {
            Ok(data) => {
                info!("消息反序列化成功（非阻塞），主题: {} / Message deserialization successful (non-blocking), topic: {}", self.topic_name, self.topic_name);
                Ok(data)
            }
            Err(e) => {
                error!("消息反序列化失败（非阻塞），主题: {}, 错误: {} / Message deserialization failed (non-blocking), topic: {}, error: {}", self.topic_name, e, self.topic_name, e);
                Err(anyhow::anyhow!("反序列化错误: {} / Deserialization error: {}", e, e))
            }
        }
    }

    pub async fn try_recv_string(&self) -> anyhow::Result<String> {
        debug!("尝试接收字符串消息，主题: {} / Attempting to receive string message, topic: {}", self.topic_name, self.topic_name);
        
        let message = self.try_recv().await?;
        let payload = message.payload().to_string();
        
        info!("接收到字符串消息（非阻塞），主题: {}, 长度: {} 字符 / String message received (non-blocking), topic: {}, length: {} chars", self.topic_name, payload.len(), self.topic_name, payload.len());
        
        Ok(payload)
    }

    pub async fn recv_with_format<T: serde::de::DeserializeOwned>(
        &self, 
        format: SerializationFormat
    ) -> anyhow::Result<T> {
        let message = self.recv().await?;
        message.deserialize_with_format::<T>(&format).map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn try_recv_with_format<T: serde::de::DeserializeOwned>(
        &self, 
        format: SerializationFormat
    ) -> anyhow::Result<T> {
        let message = self.try_recv().await?;
        message.deserialize_with_format::<T>(&format).map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn recv_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let message = self.recv().await?;
        Ok(message.payload_bytes().to_vec())
    }

    pub async fn try_recv_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let message = self.try_recv().await?;
        Ok(message.payload_bytes().to_vec())
    }

    pub async fn recv_with_format_info(&self) -> anyhow::Result<(TopicMessage, SerializationFormat)> {
        let message = self.recv().await?;
        let format = message.serialization_format().clone();
        Ok((message, format))
    }

    pub async fn recv_bincode<T: serde::de::DeserializeOwned>(&self) -> anyhow::Result<T> {
        self.recv_with_format(SerializationFormat::Bincode).await
    }

    pub async fn recv_from_offset(&self, start_offset: usize) -> anyhow::Result<TopicMessage> {
        let mut buffer = self.message_buffer.lock().await;
        
        while let Some(timestamped_msg) = buffer.pop_front() {
            // 检查过期 / Check if expired
            if let Some(ttl) = self.options.message_ttl {
                if timestamped_msg.is_expired(ttl) {
                    continue;
                }
            }

            if timestamped_msg.offset >= start_offset {
                let mut current_offset = self.current_offset.lock().await;
                *current_offset = Some(timestamped_msg.offset);
                
                return Ok(timestamped_msg.message);
            }
        }
        
        Err(anyhow::anyhow!("No message found from offset {}", start_offset))
    }

    pub async fn current_offset(&self) -> Option<usize> {
        *self.current_offset.lock().await
    }

    pub async fn reset_offset(&self) {
        let mut offset = self.current_offset.lock().await;
        *offset = None;
    }

    pub fn topic(&self) -> &str {
        &self.topic_name
    }
}

#[async_trait::async_trait]
impl MessageSubscriber for Subscriber {
    async fn recv(&self) -> anyhow::Result<TopicMessage> {
        // 如果有 consumer_id，使用基于日志的消费模式 / If consumer_id exists, use log-based consumption mode
        if let Some(ref consumer_id) = self.consumer_id {
            return self.recv_log_based(consumer_id).await;
        }

        let receiver = self.receiver.lock().await;
        loop {
            let message = receiver.recv().await.map_err(|e| anyhow::anyhow!(e))?;
            
            // 检查过期 / Check if expired
            if let Some(ttl) = self.options.message_ttl {
                if message.is_expired(ttl) {
                    debug!("丢弃过期消息，主题: {} / Discarding expired message, topic: {}", self.topic_name, self.topic_name);
                    continue;
                }
            }

            let mut offset = self.current_offset.lock().await;
            *offset = Some(message.parse_offset()?);
            
            return Ok(message);
        }
    }

    async fn try_recv(&self) -> anyhow::Result<TopicMessage> {
        // 如果有 consumer_id，使用基于日志的消费模式（非阻塞） / If consumer_id exists, use log-based consumption mode (non-blocking)
        if let Some(ref consumer_id) = self.consumer_id {
            return self.try_recv_log_based(consumer_id).await;
        }

        let receiver = self.receiver.lock().await;
        loop {
            let message = receiver.try_recv().map_err(|e| anyhow::anyhow!(e))?;
            
            // 检查过期 / Check if expired
            if let Some(ttl) = self.options.message_ttl {
                if message.is_expired(ttl) {
                    debug!("丢弃过期消息（非阻塞），主题: {} / Discarding expired message (non-blocking), topic: {}", self.topic_name, self.topic_name);
                    // 继续尝试获取下一条 / Continue trying to get the next one
                    continue;
                }
            }

            let mut offset = self.current_offset.lock().await;
            *offset = Some(message.parse_offset()?);
            
            return Ok(message);
        }
    }

    async fn current_offset(&self) -> Option<usize> {
        *self.current_offset.lock().await
    }

    async fn reset_offset(&self) {
        let mut offset = self.current_offset.lock().await;
        *offset = None;
    }

    fn topic(&self) -> &str {
        &self.topic_name
    }
}
