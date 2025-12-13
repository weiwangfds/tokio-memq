use std::collections::{VecDeque, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Notify};
use log::{debug, info, error};
use tokio_stream::Stream;
use async_stream::try_stream;

use crate::mq::traits::MessageSubscriber;
use crate::mq::message::{TopicMessage, TimestampedMessage, TopicOptions, ConsumptionMode, MessageMetadata};
use crate::mq::serializer::SerializationFormat;

pub struct Subscriber {
    pub topic_name: String,
    // receiver removed
    _subscriber_count: Arc<Mutex<usize>>,
    /// 当前消费偏移量 (Next Offset to consume)
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
        // receiver removed
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
            // receiver removed
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

    /// 获取当前目标偏移量
    ///
    /// Get current target offset.
    async fn get_target_offset(&self) -> usize {
        if let Some(ref id) = self.consumer_id {
            let offsets = self.consumer_offsets.read().await;
            *offsets.get(id).unwrap_or(&0)
        } else {
            let current = self.current_offset.lock().await;
            current.unwrap_or(0)
        }
    }

    /// 更新偏移量
    ///
    /// Advance offset.
    async fn advance_offset(&self, new_offset: usize) {
        if let Some(ref id) = self.consumer_id {
            let mut offsets = self.consumer_offsets.write().await;
            offsets.insert(id.clone(), new_offset);
        }
        // Update local tracking (Next Offset)
        let mut current = self.current_offset.lock().await;
        *current = Some(new_offset);
    }

    /// 从缓冲区获取消息
    ///
    /// Fetch message from buffer.
    async fn fetch_from_buffer(&self, target_offset: usize) -> Option<TimestampedMessage> {
        let buffer = self.message_buffer.lock().await;
        
        if buffer.is_empty() {
            return None;
        }

        let last_offset = buffer.back().unwrap().offset;
        if last_offset < target_offset {
            return None;
        }

        let front_offset = buffer.front().unwrap().offset;
        if target_offset < front_offset {
            debug!("消费者落后太多 (Target: {}, Front: {})，重置为 Front / Consumer lagging too far (Target: {}, Front: {}), resetting to Front", target_offset, front_offset, target_offset, front_offset);
            return Some(buffer.front().unwrap().clone());
        }

        for msg in buffer.iter() {
            if msg.offset >= target_offset {
                if let Some(ttl) = self.options.message_ttl {
                    if msg.is_expired(ttl) {
                        continue;
                    }
                }
                return Some(msg.clone());
            }
        }
        None
    }

    /// 手动提交偏移量
    ///
    /// Manually commit offset.
    pub async fn commit(&self, offset: usize) -> anyhow::Result<()> {
        self.advance_offset(offset).await;
        Ok(())
    }

    /// 跳转到指定偏移量
    ///
    /// Seek to specific offset.
    pub async fn seek(&self, offset: usize) -> anyhow::Result<()> {
        self.commit(offset).await
    }


    async fn recv_metadata_log_based(&self, consumer_id: &str) -> anyhow::Result<MessageMetadata> {
        loop {
            let notification = self.channel_notify.notified();
            let target_offset = {
                let offsets = self.consumer_offsets.read().await;
                *offsets.get(consumer_id).unwrap_or(&0)
            };

            let meta_opt = {
                let buffer = self.message_buffer.lock().await;
                if buffer.is_empty() {
                    None
                } else {
                    let last_offset = buffer.back().unwrap().offset;
                    if last_offset < target_offset {
                        None
                    } else {
                        let mut found_meta = None;
                        let front_offset = buffer.front().unwrap().offset;
                        if target_offset < front_offset {
                             found_meta = Some(buffer.front().unwrap().metadata());
                        } else {
                            for msg in buffer.iter() {
                                if msg.offset >= target_offset {
                                    if let Some(ttl) = self.options.message_ttl {
                                        if msg.is_expired(ttl) {
                                            continue;
                                        }
                                    }
                                    found_meta = Some(msg.metadata());
                                    break;
                                }
                            }
                        }
                        found_meta
                    }
                }
            };

            if let Some(meta) = meta_opt {
                let next_offset = meta.offset + 1;
                {
                    let mut offsets = self.consumer_offsets.write().await;
                    offsets.insert(consumer_id.to_string(), next_offset);
                }
                let mut current_offset = self.current_offset.lock().await;
                *current_offset = Some(meta.offset);
                return Ok(meta);
            } else {
                notification.await;
            }
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
        self.seek(start_offset).await?;
        self.recv().await
    }

    pub async fn current_offset(&self) -> Option<usize> {
        if let Some(ref id) = self.consumer_id {
            let offsets = self.consumer_offsets.read().await;
            offsets.get(id).cloned()
        } else {
             *self.current_offset.lock().await
        }
    }

    pub async fn reset_offset(&self) {
        if let Some(ref id) = self.consumer_id {
            let mut offsets = self.consumer_offsets.write().await;
            offsets.insert(id.clone(), 0);
        }
        let mut offset = self.current_offset.lock().await;
        *offset = Some(0);
    }

    pub fn topic(&self) -> &str {
        &self.topic_name
    }

    pub fn stream(&self) -> impl Stream<Item = anyhow::Result<TopicMessage>> + '_ {
        try_stream! {
            loop {
                let msg = self.recv().await?;
                yield msg;
            }
        }
    }

    pub async fn recv_timeout(&self, duration: std::time::Duration) -> anyhow::Result<Option<TopicMessage>> {
        match tokio::time::timeout(duration, self.recv()).await {
            Ok(res) => res.map(Some),
            Err(_) => Ok(None),
        }
    }

    pub async fn recv_batch(&self, n: usize) -> anyhow::Result<Vec<TopicMessage>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        
        let mut messages = Vec::with_capacity(n);
        
        // First message (blocking wait)
        let first = self.recv().await?;
        messages.push(first);
        
        // Subsequent messages (non-blocking)
        for _ in 1..n {
             match self.try_recv().await {
                 Ok(msg) => messages.push(msg),
                 Err(_) => break,
             }
        }
        
        Ok(messages)
    }

    pub async fn recv_filter<F>(&self, predicate: F) -> anyhow::Result<TopicMessage>
    where F: Fn(&TopicMessage) -> bool 
    {
        loop {
            let msg = self.recv().await?;
            if predicate(&msg) {
                return Ok(msg);
            }
        }
    }

    pub async fn recv_metadata(&self) -> anyhow::Result<MessageMetadata> {
        if let Some(ref consumer_id) = self.consumer_id {
            return self.recv_metadata_log_based(consumer_id).await;
        }
        
        let msg = self.recv().await?;
        Ok(msg.metadata())
    }
}

#[async_trait::async_trait]
impl MessageSubscriber for Subscriber {
    async fn recv(&self) -> anyhow::Result<TopicMessage> {
        loop {
            // Register notification listener before checking buffer
            let notification = self.channel_notify.notified();
            let target_offset = self.get_target_offset().await;
            
            if let Some(msg) = self.fetch_from_buffer(target_offset).await {
                self.advance_offset(msg.offset + 1).await;
                return Ok(msg.message);
            }
            
            // Wait for notification
            notification.await;
        }
    }

    async fn try_recv(&self) -> anyhow::Result<TopicMessage> {
        let target_offset = self.get_target_offset().await;
        
        if let Some(msg) = self.fetch_from_buffer(target_offset).await {
            self.advance_offset(msg.offset + 1).await;
            Ok(msg.message)
        } else {
            Err(anyhow::anyhow!("No message available"))
        }
    }

    async fn current_offset(&self) -> Option<usize> {
        if let Some(ref id) = self.consumer_id {
            let offsets = self.consumer_offsets.read().await;
            offsets.get(id).cloned()
        } else {
             *self.current_offset.lock().await
        }
    }

    async fn reset_offset(&self) {
        if let Some(ref id) = self.consumer_id {
            let mut offsets = self.consumer_offsets.write().await;
            offsets.insert(id.clone(), 0);
        }
        let mut offset = self.current_offset.lock().await;
        *offset = Some(0);
    }

    fn topic(&self) -> &str {
        &self.topic_name
    }
}
