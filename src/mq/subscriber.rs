use std::collections::{VecDeque, HashMap};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::hash::{Hash, Hasher};
use tokio::sync::{Mutex, RwLock, watch};
use log::{debug, info, error};
use tokio_stream::Stream;
use async_stream::try_stream;

use crate::mq::traits::MessageSubscriber;
use crate::mq::message::{TopicMessage, TimestampedMessage, TopicOptions, ConsumptionMode, MessageMetadata};
use crate::mq::serializer::SerializationFormat;

pub struct Subscriber {
    pub topic_name: String,
    // receiver removed
    _subscriber_count: Arc<AtomicUsize>,
    /// 当前消费偏移量 (Next Offset to consume)
    current_offset: Arc<Mutex<Option<usize>>>,
    message_buffer: Arc<RwLock<VecDeque<TimestampedMessage>>>,
    pub options: TopicOptions,
    pub consumer_id: Option<String>,
    pub consumption_mode: ConsumptionMode,
    consumer_offsets: Arc<RwLock<HashMap<String, usize>>>,
    channel_notify: watch::Receiver<usize>,
    // 公平调度：消费者唯一标识符，用于随机延迟种子
    unique_seed: u64,
}

impl Subscriber {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topic_name: String,
        // receiver removed
        subscriber_count: Arc<AtomicUsize>,
        current_offset: Arc<Mutex<Option<usize>>>,
        message_buffer: Arc<RwLock<VecDeque<TimestampedMessage>>>,
        options: TopicOptions,
        consumer_id: Option<String>,
        consumption_mode: ConsumptionMode,
        consumer_offsets: Arc<RwLock<HashMap<String, usize>>>,
        channel_notify: watch::Receiver<usize>,
    ) -> Self {
        // 为每个消费者生成唯一的种子，用于公平调度
        let seed_str = format!("{}-{}-{}",
            consumer_id.as_ref().unwrap_or(&"anonymous".to_string()),
            topic_name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        seed_str.hash(&mut hasher);
        let unique_seed = hasher.finish();

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
            unique_seed,
        }
    }

    /// 获取当前目标偏移量
    ///
    /// Get current target offset.
    async fn get_target_offset(&self) -> usize {
        if let Some(ref id) = self.consumer_id {
            // 对于有消费者ID的订阅者，使用共享偏移量（竞争消费）
            let offsets = self.consumer_offsets.read().await;
            *offsets.get(id).unwrap_or(&0)
        } else {
            // 对于没有消费者ID的订阅者，使用本地偏移量（广播消费）
            let current = self.current_offset.lock().await;
            current.unwrap_or(0)
        }
    }

    /// 更新偏移量
    ///
    /// Advance offset.
    async fn advance_offset(&self, new_offset: usize) {
        if let Some(ref id) = self.consumer_id {
            // 对于有消费者ID的订阅者，更新共享偏移量（竞争消费）
            let mut offsets = self.consumer_offsets.write().await;
            offsets.insert(id.clone(), new_offset);
        }
        // Update local tracking (Next Offset) - 用于所有订阅者
        let mut current = self.current_offset.lock().await;
        *current = Some(new_offset);
    }

    /// 从缓冲区批量获取消息
    ///
    /// Fetch batch of messages from buffer.
    async fn fetch_batch_from_buffer(&self, target_offset: usize, batch_size: usize) -> Vec<TimestampedMessage> {
        let buffer = self.message_buffer.read().await;
        if buffer.is_empty() { return Vec::new(); }
        
        let last_offset = buffer.back().unwrap().offset;
        if last_offset < target_offset { return Vec::new(); }
        
        let mut messages = Vec::with_capacity(batch_size);
        let front_offset = buffer.front().unwrap().offset;
        
        let start_idx = if target_offset < front_offset {
            0
        } else {
            target_offset - front_offset
        };

        for msg in buffer.iter().skip(start_idx) {
            if messages.len() >= batch_size {
                break;
            }
            
            if msg.offset >= target_offset {
                if let Some(ttl) = self.options.message_ttl {
                    if msg.is_expired(ttl) { continue; }
                }
                messages.push(msg.clone());
            }
        }
        
        messages
    }

    /// 从缓冲区获取消息
    ///
    /// Fetch message from buffer.
    async fn fetch_from_buffer(&self, target_offset: usize) -> Option<TimestampedMessage> {
        let buffer = self.message_buffer.read().await;
        if buffer.is_empty() { return None; }
        let last_offset = buffer.back().unwrap().offset;
        if last_offset < target_offset { return None; }
        let front_offset = buffer.front().unwrap().offset;
        if target_offset < front_offset {
            // 对于广播消费者，从第一个可用消息开始
            for msg in buffer.iter() {
                if let Some(ttl) = self.options.message_ttl {
                    if msg.is_expired(ttl) { continue; }
                }
                return Some(msg.clone());
            }
            return None;
        }
        let idx = target_offset - front_offset;
        if let Some(msg) = buffer.get(idx) {
            if let Some(ttl) = self.options.message_ttl {
                if msg.is_expired(ttl) {
                    for msg2 in buffer.iter().skip(idx + 1) {
                        if msg2.offset >= target_offset {
                            if let Some(ttl) = self.options.message_ttl {
                                if msg2.is_expired(ttl) { continue; }
                            }
                            return Some(msg2.clone());
                        }
                    }
                    return None;
                }
            }
            return Some(msg.clone());
        }
        for msg in buffer.iter().skip(idx) {
            if msg.offset >= target_offset {
                if let Some(ttl) = self.options.message_ttl {
                    if msg.is_expired(ttl) { continue; }
                }
                return Some(msg.clone());
            }
        }
        None
    }

    /// 公平调度延迟计算
    ///
    /// Calculate fair scheduling delay based on consumer ID and unique seed.
    async fn fair_scheduling_delay(&self) -> u64 {
        if let Some(ref consumer_id) = self.consumer_id {
            // 使用消费者ID生成0-10ms的延迟，确保相同ID的消费者有公平竞争机会
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            consumer_id.hash(&mut hasher);
            self.unique_seed.hash(&mut hasher);
            let hash = hasher.finish();

            // 基于哈希值生成0-5ms的延迟
            let delay_ms = hash % 6;
            debug!("公平调度延迟: 消费者ID {}, 延迟: {}ms / Fair scheduling delay: consumer ID {}, delay: {}ms", consumer_id, delay_ms, consumer_id, delay_ms);
            delay_ms
        } else {
            0
        }
    }

    /// 手动提交偏移量
    ///
    /// Manually commit offset.
    pub(crate) async fn commit_internal(&self, offset: usize) -> anyhow::Result<()> {
        self.advance_offset(offset).await;
        Ok(())
    }

    /// 跳转到指定偏移量
    ///
    /// Seek to specific offset.
    pub(crate) async fn seek_internal(&self, offset: usize) -> anyhow::Result<()> {
        self.commit_internal(offset).await
    }


    async fn recv_metadata_log_based(&self, _consumer_id: &str) -> anyhow::Result<MessageMetadata> {
        loop {
            let mut rx = self.channel_notify.clone();
            let target_offset = {
                let offsets = self.consumer_offsets.read().await;
                if let Some(ref id) = self.consumer_id {
                    *offsets.get(id).unwrap_or(&0)
                } else {
                    0
                }
            };

            let meta_opt = {
                let buffer = self.message_buffer.read().await;
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
                             for msg in buffer.iter() {
                                 if let Some(ttl) = self.options.message_ttl {
                                     if msg.is_expired(ttl) {
                                         continue;
                                     }
                                 }
                                 found_meta = Some(msg.metadata());
                                 break;
                             }
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
                    if let Some(ref id) = self.consumer_id {
                        offsets.insert(id.to_string(), next_offset);
                    }
                }
                let mut current_offset = self.current_offset.lock().await;
                *current_offset = Some(meta.offset);
                return Ok(meta);
            } else if rx.changed().await.is_err() {
                return Err(anyhow::anyhow!("Topic closed"));
            }
        }
    }


}

impl Subscriber {
    pub(crate) async fn recv_typed_internal<T: serde::de::DeserializeOwned + std::any::Any + Send + Sync + Clone>(&self) -> anyhow::Result<T> {
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
        info!("接收到消息，主题: {}, {} / Message received, topic: {}, {}", self.topic_name, message.payload_str(), self.topic_name, message.payload_str());
        Ok(payload)
    }

    pub async fn try_recv_typed<T: serde::de::DeserializeOwned + std::any::Any + Send + Sync + Clone>(&self) -> anyhow::Result<T> {
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

    pub async fn recv_with_format<T: serde::de::DeserializeOwned + std::any::Any + Send + Sync + Clone>(
        &self,
        format: SerializationFormat
    ) -> anyhow::Result<T> {
        let message = self.recv().await?;
        message.deserialize_with_format::<T>(&format).map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn try_recv_with_format<T: serde::de::DeserializeOwned + std::any::Any + Send + Sync + Clone>(
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

    pub async fn recv_bincode<T: serde::de::DeserializeOwned + std::any::Any + Send + Sync + Clone>(&self) -> anyhow::Result<T> {
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

    pub(crate) fn stream_internal(&self) -> impl Stream<Item = anyhow::Result<TopicMessage>> + '_ {
        try_stream! {
            loop {
                let msg = self.recv().await?;
                yield msg;
            }
        }
    }

    pub(crate) async fn recv_timeout_internal(&self, duration: std::time::Duration) -> anyhow::Result<Option<TopicMessage>> {
        match tokio::time::timeout(duration, self.recv()).await {
            Ok(res) => res.map(Some),
            Err(_) => Ok(None),
        }
    }

    pub(crate) async fn recv_batch_internal(&self, n: usize) -> anyhow::Result<Vec<TopicMessage>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        
        loop {
            // Register notification listener before checking buffer
            let mut rx = self.channel_notify.clone();
            let target_offset = self.get_target_offset().await;
            
            let messages = self.fetch_batch_from_buffer(target_offset, n).await;
            
            if !messages.is_empty() {
                let last_offset = messages.last().unwrap().offset;
                self.advance_offset(last_offset + 1).await;
                return Ok(messages.into_iter().map(|m| m.message).collect());
            }
            
            // Wait for notification if no messages available
            if rx.changed().await.is_err() {
                 return Err(anyhow::anyhow!("Topic closed"));
            }
        }
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

impl Drop for Subscriber {
    fn drop(&mut self) {
        self._subscriber_count.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl MessageSubscriber for Subscriber {
    async fn recv(&self) -> anyhow::Result<TopicMessage> {
        loop {
            // Register notification listener before checking buffer
            let mut rx = self.channel_notify.clone();

            if let Some(ref id) = self.consumer_id {
                // Group consumer: lock offsets to ensure atomicity
                let mut offsets = self.consumer_offsets.write().await;
                let target_offset = *offsets.get(id).unwrap_or(&0);
                
                if let Some(msg) = self.fetch_from_buffer(target_offset).await {
                    let next_offset = msg.offset + 1;
                    offsets.insert(id.clone(), next_offset);
                    
                    // Update local offset tracking as well
                    let mut current = self.current_offset.lock().await;
                    *current = Some(next_offset);
                    
                    return Ok(msg.message);
                }
            } else {
                // Non-group consumer
                let target_offset = self.get_target_offset().await;
                if let Some(msg) = self.fetch_from_buffer(target_offset).await {
                    self.advance_offset(msg.offset + 1).await;
                    return Ok(msg.message);
                }
            }

            // Wait for notification
            if rx.changed().await.is_err() {
                 return Err(anyhow::anyhow!("Topic closed"));
            }

            // 公平调度：为相同消费者ID的订阅者添加随机延迟
            // 这样相同ID的消费者有公平机会获取消息
            if self.consumer_id.is_some() {
                // 使用消费者ID和唯一种子生成确定性延迟
                let delay_ms = self.fair_scheduling_delay().await;
                if delay_ms > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                }
            }
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

    async fn recv_typed<T: serde::de::DeserializeOwned + Send + Sync + 'static + Clone>(&self) -> anyhow::Result<T> {
        self.recv_typed_internal::<T>().await
    }

    async fn recv_batch(&self, n: usize) -> anyhow::Result<Vec<TopicMessage>> {
        self.recv_batch_internal(n).await
    }

    async fn recv_batch_typed<T: serde::de::DeserializeOwned + Send + Sync + 'static + Clone>(&self, n: usize) -> anyhow::Result<Vec<T>> {
        let messages = self.recv_batch_internal(n).await?;
        let mut results = Vec::with_capacity(messages.len());
        for msg in messages {
            results.push(msg.deserialize::<T>()?);
        }
        Ok(results)
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

    async fn commit(&self, offset: usize) -> anyhow::Result<()> {
        self.commit_internal(offset).await
    }

    async fn seek(&self, offset: usize) -> anyhow::Result<()> {
        self.seek_internal(offset).await
    }

    fn stream(&self) -> impl tokio_stream::Stream<Item = anyhow::Result<TopicMessage>> + '_ {
        self.stream_internal()
    }

    async fn recv_timeout(&self, duration: std::time::Duration) -> anyhow::Result<Option<TopicMessage>> {
        self.recv_timeout_internal(duration).await
    }

    fn topic(&self) -> &str {
        &self.topic_name
    }
}
