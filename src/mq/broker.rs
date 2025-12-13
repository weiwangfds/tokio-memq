use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::{RwLock, Mutex, watch};
use log::{debug, info, warn, trace};

use crate::mq::traits::QueueManager;
use crate::mq::message::{TopicMessage, TopicOptions, TimestampedMessage, ConsumptionMode};
use crate::mq::publisher::Publisher;
use crate::mq::subscriber::Subscriber;

#[derive(Debug, Clone)]
pub struct TopicStats {
    pub message_count: usize,
    pub subscriber_count: usize,
    pub dropped_messages: usize,
    pub consumer_lags: HashMap<String, usize>,
}

#[derive(Clone)]
pub struct TopicManager {
    topics: Arc<RwLock<HashMap<String, TopicChannel>>>,
}

#[derive(Clone)]
pub struct TopicChannel {
    subscriber_count: Arc<AtomicUsize>,
    options: TopicOptions,
    message_buffer: Arc<RwLock<VecDeque<TimestampedMessage>>>,
    next_offset: Arc<AtomicUsize>,
    consumer_offsets: Arc<RwLock<HashMap<String, usize>>>,
    notify: Arc<watch::Sender<usize>>,
    dropped_count: Arc<AtomicUsize>,
}

impl TopicChannel {
    pub fn new(options: TopicOptions) -> Self {
        let (tx, _) = watch::channel(0);
        TopicChannel {
            subscriber_count: Arc::new(AtomicUsize::new(0)),
            options,
            message_buffer: Arc::new(RwLock::new(VecDeque::new())),
            next_offset: Arc::new(AtomicUsize::new(0)),
            consumer_offsets: Arc::new(RwLock::new(HashMap::new())),
            notify: Arc::new(tx),
            dropped_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn add_subscriber(&self, consumer_id: Option<String>, mode: ConsumptionMode) -> Subscriber {
        self.subscriber_count.fetch_add(1, Ordering::SeqCst);

        // 如果指定了消费者 ID，初始化偏移量 / If consumer ID is specified, initialize offset
        let mut start_offset_local = 0;
        
        if let Some(ref id) = consumer_id {
            debug!("检查消费者偏移量: {} / Checking consumer offset: {}", id, id);
            let has_offset = {
                let offsets = self.consumer_offsets.read().await;
                offsets.contains_key(id)
            };

            if !has_offset {
                let mut offsets = self.consumer_offsets.write().await;
                if !offsets.contains_key(id) {
                    let buffer = self.message_buffer.read().await;
                    let start_offset = match mode {
                        ConsumptionMode::Earliest => {
                            buffer.front().map(|m| m.offset).unwrap_or(0)
                        },
                        ConsumptionMode::Latest => {
                            // Latest 应该是 buffer.back().offset + 1 / Latest should be buffer.back().offset + 1
                            buffer.back().map(|m| m.offset + 1).unwrap_or(0)
                        },
                        ConsumptionMode::Offset(offset) => offset,
                        ConsumptionMode::LastOffset => {
                            // 默认行为：如果无记录，使用 Latest / Default behavior: if no record, use Latest
                            buffer.back().map(|m| m.offset + 1).unwrap_or(0)
                        }
                    };
                    debug!("初始化消费者组偏移量 (Subscription Time), ID: {}, Mode: {:?}, Start Offset: {} / Initializing consumer group offset (Subscription Time), ID: {}, Mode: {:?}, Start Offset: {}", id, mode, start_offset, id, mode, start_offset);
                    offsets.insert(id.clone(), start_offset);
                    start_offset_local = start_offset;
                }
            } else {
                debug!("消费者 {} 已存在偏移量 / Consumer {} already has offset", id, id);
                // 如果已存在，读取它作为本地起始偏移量 (针对无 ID 订阅者逻辑复用，虽此时 consumer_id 有值)
                // If exists, read it as local start offset (logic reuse, though consumer_id is Some)
                let offsets = self.consumer_offsets.read().await;
                start_offset_local = *offsets.get(id).unwrap_or(&0);
            }
        } else {
            // 无 ID 订阅者，仅在本地维护偏移量 / Subscriber without ID, maintain offset locally
             let buffer = self.message_buffer.read().await;
             start_offset_local = match mode {
                ConsumptionMode::Earliest => {
                    buffer.front().map(|m| m.offset).unwrap_or(0)
                },
                ConsumptionMode::Latest => {
                    buffer.back().map(|m| m.offset + 1).unwrap_or(0)
                },
                ConsumptionMode::Offset(offset) => offset,
                ConsumptionMode::LastOffset => {
                    buffer.back().map(|m| m.offset + 1).unwrap_or(0)
                }
            };
        }
        
        Subscriber::new(
            String::new(), // Topic name will be set by caller usually, but here passed empty? Wait, Subscriber::new takes topic name?
            // Checking original Subscriber::new signature...
            // It was taking receiver. Now we remove it.
            Arc::clone(&self.subscriber_count),
            Arc::new(Mutex::new(Some(start_offset_local))), // Initialize local offset
            Arc::clone(&self.message_buffer),
            self.options.clone(),
            consumer_id,
            mode,
            Arc::clone(&self.consumer_offsets),
            self.notify.subscribe(),
        )
    }

    pub async fn cleanup_expired_messages(&self) {
        if let Some(ttl) = self.options.message_ttl {
            let mut buffer = self.message_buffer.write().await;
            let mut dropped = 0;
            while let Some(msg) = buffer.front() {
                if msg.is_expired(ttl) {
                    buffer.pop_front();
                    dropped += 1;
                } else {
                    break;
                }
            }
            if dropped > 0 {
                self.dropped_count.fetch_add(dropped, Ordering::SeqCst);
            }
        }
    }

    pub async fn add_to_buffer(&self, message: TopicMessage) -> anyhow::Result<()> {
        let current_offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        let next_val = current_offset + 1;
        
        let timestamped_msg = TimestampedMessage::new(message, current_offset);
        
        let mut buffer = self.message_buffer.write().await;
        let mut dropped = 0;
        
        if let Some(ttl) = self.options.message_ttl {
            while let Some(msg) = buffer.front() {
                if msg.is_expired(ttl) {
                    buffer.pop_front();
                    dropped += 1;
                } else {
                    break;
                }
            }
        }
        
        if let Some(max_messages) = self.options.max_messages {
            while buffer.len() >= max_messages {
                buffer.pop_front();
                dropped += 1;
            }
        }
        
        if dropped > 0 {
            self.dropped_count.fetch_add(dropped, Ordering::SeqCst);
        }

        buffer.push_back(timestamped_msg);
        // 通知等待的订阅者 / Notify waiting subscribers
        self.notify.send(next_val).ok();
        Ok(())
    }

    pub async fn add_to_buffer_batch(&self, messages: Vec<TopicMessage>) -> anyhow::Result<()> {
        let count = messages.len();
        if count == 0 { return Ok(()); }

        let start_offset = self.next_offset.fetch_add(count, Ordering::SeqCst);
        let next_val = start_offset + count;
        
        let mut buffer = self.message_buffer.write().await;
        let mut dropped = 0;
        
        // 清理过期消息 / Clean expired messages
        if let Some(ttl) = self.options.message_ttl {
            while let Some(msg) = buffer.front() {
                if msg.is_expired(ttl) {
                    buffer.pop_front();
                    dropped += 1;
                } else {
                    break;
                }
            }
        }
        
        // 检查容量并清理 / Check capacity and clean
        if let Some(max_messages) = self.options.max_messages {
            let total = buffer.len() + count;
            if total > max_messages {
                let to_remove = total - max_messages;
                let buffer_remove_count = std::cmp::min(buffer.len(), to_remove);
                for _ in 0..buffer_remove_count {
                    buffer.pop_front();
                    dropped += 1;
                }
            }
        }

        // 如果新消息本身超过容量，需要截断前面的 / If new messages exceed capacity, truncate the front
        let msgs_to_add = if let Some(max_messages) = self.options.max_messages {
             if count > max_messages {
                 &messages[count - max_messages..]
             } else {
                 &messages[..]
             }
        } else {
            &messages[..]
        };

        // 计算跳过的消息数量 / Calculate skipped messages count
        let skip_count = messages.len() - msgs_to_add.len();
        dropped += skip_count;

        for (i, msg) in msgs_to_add.iter().enumerate() {
            let original_index = skip_count + i;
            let timestamped_msg = TimestampedMessage::new(msg.clone(), start_offset + original_index);
            buffer.push_back(timestamped_msg);
        }
        
        if let Some(max_messages) = self.options.max_messages {
            while buffer.len() > max_messages {
                buffer.pop_front();
                dropped += 1;
            }
        }
        
        if dropped > 0 {
            self.dropped_count.fetch_add(dropped, Ordering::SeqCst);
        }

        self.notify.send(next_val).ok();
        Ok(())
    }

    pub async fn get_stats(&self) -> TopicStats {
        let buffer = self.message_buffer.read().await;
        let message_count = buffer.len();
        let subscriber_count = self.subscriber_count.load(Ordering::SeqCst);
        let dropped_messages = self.dropped_count.load(Ordering::SeqCst);
        
        let mut consumer_lags = HashMap::new();
        let offsets = self.consumer_offsets.read().await;
        let next_offset = self.next_offset.load(Ordering::SeqCst);
        
        for (id, offset) in offsets.iter() {
            let lag = if next_offset >= *offset {
                next_offset - *offset
            } else {
                0
            };
            consumer_lags.insert(id.clone(), lag);
        }

        TopicStats {
            message_count,
            subscriber_count,
            dropped_messages,
            consumer_lags,
        }
    }

    pub async fn get_valid_message(&self) -> Option<TopicMessage> {
        let mut buffer = self.message_buffer.write().await;
        
        while let Some(timestamped_msg) = buffer.pop_front() {
            if let Some(ttl) = self.options.message_ttl {
                if timestamped_msg.is_expired(ttl) {
                    continue;
                }
            }
            
            return Some(timestamped_msg.message);
        }
        
        None
    }
}

impl TopicManager {
    pub fn new() -> Self {
        debug!("创建新的主题管理器 / Creating new topic manager");
        trace!("初始化主题管理器完成 / Topic manager initialization completed");
        let topics: Arc<RwLock<HashMap<String, TopicChannel>>> = Arc::new(RwLock::new(HashMap::new()));
        {
            let topics_clone = Arc::clone(&topics);
            tokio::spawn(async move {
                let interval = std::time::Duration::from_millis(200);
                loop {
                    tokio::time::sleep(interval).await;
                    let topics_map = topics_clone.read().await;
                    for (_name, channel) in topics_map.iter() {
                        channel.cleanup_expired_messages().await;
                    }
                }
            });
        }
        TopicManager { topics }
    }

    pub async fn get_or_create_topic(&self, topic: String) -> TopicChannel {
        self.get_or_create_topic_with_options(topic, TopicOptions::default()).await
    }

    pub async fn get_or_create_topic_with_options(&self, topic: String, options: TopicOptions) -> TopicChannel {
        // Fast path: try read lock first
        {
            let topics = self.topics.read().await;
            if let Some(channel) = topics.get(&topic) {
                debug!("使用已存在的主题: {} / Using existing topic: {}", topic, topic);
                return channel.clone();
            }
        } // Drop read lock

        // Slow path: acquire write lock
        let mut topics = self.topics.write().await;
        
        // Double check
        if let Some(channel) = topics.get(&topic) {
             debug!("使用已存在的主题 (Double Check): {} / Using existing topic (Double Check): {}", topic, topic);
             return channel.clone();
        }

        info!("创建新主题: {} / Creating new topic: {}", topic, topic);
        debug!("主题配置: max_messages={:?}, message_ttl={:?}, lru_enabled={} / Topic config: max_messages={:?}, message_ttl={:?}, lru_enabled={}", 
               options.max_messages, options.message_ttl, options.lru_enabled, options.max_messages, options.message_ttl, options.lru_enabled);
        let channel = TopicChannel::new(options);
        topics.insert(topic.clone(), channel.clone());
        
        channel
    }

    pub async fn publish(&self, message: TopicMessage) -> anyhow::Result<()> {
        debug!("开始发布消息到主题: {}, 大小: {} 字节, 格式: {:?} / Start publishing message to topic: {}, size: {} bytes, format: {:?}", 
               message.topic, message.payload.len(), message.format, message.topic, message.payload.len(), message.format);
        info!("发布消息，主题: {}, {} / Publishing message, topic: {}, {}", message.topic, message.display_payload(256), message.topic, message.display_payload(256));
        
        let channel = self.get_or_create_topic(message.topic.clone()).await;
        
        channel.add_to_buffer(message.clone()).await?;
        
        // Removed async-channel logic as we unify backpressure to buffer-only
        // LRU eviction is now handled inside add_to_buffer
        
        debug!("消息已成功发布到主题: {} / Message successfully published to topic: {}", message.topic, message.topic);
        Ok(())
    }

    pub async fn publish_batch(&self, messages: Vec<TopicMessage>) -> anyhow::Result<()> {
        if messages.is_empty() { return Ok(()); }
        
        let mut groups: HashMap<String, Vec<TopicMessage>> = HashMap::new();
        for msg in messages {
            groups.entry(msg.topic.clone()).or_default().push(msg);
        }

        for (topic, msgs) in groups {
            let channel = self.get_or_create_topic(topic.clone()).await;
            
            channel.add_to_buffer_batch(msgs).await?;

            // Removed async-channel logic
            // LRU eviction handled in add_to_buffer_batch
        }
        Ok(())
    }

    pub async fn get_topic_stats(&self, topic: &str) -> Option<TopicStats> {
        let topics = self.topics.read().await;
        if let Some(channel) = topics.get(topic) {
            Some(channel.get_stats().await)
        } else {
            None
        }
    }

    pub async fn subscribe(&self, topic: String) -> anyhow::Result<Subscriber> {
        info!("创建订阅者，主题: {} / Creating subscriber, topic: {}", topic, topic);
        let channel = self.get_or_create_topic(topic.clone()).await;
        let mut subscriber = channel.add_subscriber(None, ConsumptionMode::default()).await;
        subscriber.topic_name = topic;
        
        let count = channel.subscriber_count.load(Ordering::SeqCst);
        debug!("主题订阅成功，当前订阅者数量: {} / Topic subscription successful, current subscriber count: {}", count, count);
        Ok(subscriber)
    }

    pub async fn subscribe_group(&self, topic: String, consumer_id: String, mode: ConsumptionMode) -> anyhow::Result<Subscriber> {
        info!("创建消费者组订阅者，主题: {}, ID: {}, 模式: {:?} / Creating consumer group subscriber, topic: {}, ID: {}, mode: {:?}", topic, consumer_id, mode, topic, consumer_id, mode);
        let channel = self.get_or_create_topic(topic.clone()).await;
        let mut subscriber = channel.add_subscriber(Some(consumer_id), mode).await;
        subscriber.topic_name = topic;
        
        let count = channel.subscriber_count.load(Ordering::SeqCst);
        debug!("主题订阅成功（消费者组），当前订阅者数量: {} / Topic subscription successful (consumer group), current subscriber count: {}", count, count);
        Ok(subscriber)
    }

    pub async fn subscribe_with_options(&self, topic: String, options: TopicOptions) -> anyhow::Result<Subscriber> {
        info!("创建带选项的订阅者，主题: {} / Creating subscriber with options, topic: {}", topic, topic);
        debug!("订阅选项: max_messages={:?}, message_ttl={:?}, lru_enabled={} / Subscription options: max_messages={:?}, message_ttl={:?}, lru_enabled={}", 
               options.max_messages, options.message_ttl, options.lru_enabled, options.max_messages, options.message_ttl, options.lru_enabled);
        
        let channel = self.get_or_create_topic_with_options(topic.clone(), options.clone()).await;
        let mut subscriber = channel.add_subscriber(None, ConsumptionMode::default()).await;
        subscriber.topic_name = topic;
        subscriber.options = options;
        
        let count = channel.subscriber_count.load(Ordering::SeqCst);
        debug!("主题订阅成功（带选项），当前订阅者数量: {} / Topic subscription successful (with options), current subscriber count: {}", count, count);
        Ok(subscriber)
    }

    pub async fn subscribe_group_with_options(&self, topic: String, options: TopicOptions, consumer_id: String, mode: ConsumptionMode) -> anyhow::Result<Subscriber> {
        info!("创建带选项的消费者组订阅者，主题: {}, ID: {} / Creating consumer group subscriber with options, topic: {}, ID: {}", topic, consumer_id, topic, consumer_id);
        
        let channel = self.get_or_create_topic_with_options(topic.clone(), options.clone()).await;
        let mut subscriber = channel.add_subscriber(Some(consumer_id), mode).await;
        subscriber.topic_name = topic;
        subscriber.options = options;
        
        let count = channel.subscriber_count.load(Ordering::SeqCst);
        debug!("主题订阅成功（带选项和组），当前订阅者数量: {} / Topic subscription successful (with options and group), current subscriber count: {}", count, count);
        Ok(subscriber)
    }

    pub async fn list_topics(&self) -> Vec<String> {
        let topics = self.topics.read().await;
        let topic_list: Vec<String> = topics.keys().cloned().collect();
        debug!("当前主题列表: {:?} / Current topic list: {:?}", topic_list, topic_list);
        topic_list
    }

    pub async fn get_subscriber_count(&self, topic: &str) -> Option<usize> {
        let topics = self.topics.read().await;
        if let Some(channel) = topics.get(topic) {
            let count_val = channel.subscriber_count.load(Ordering::SeqCst);
            debug!("主题 {} 的订阅者数量: {} / Subscriber count for topic {}: {}", topic, count_val, topic, count_val);
            Some(count_val)
        } else {
            warn!("请求的主题不存在: {} / Requested topic does not exist: {}", topic, topic);
            None
        }
    }

    pub async fn delete_topic(&self, topic: &str) -> bool {
        let mut topics = self.topics.write().await;
        if topics.remove(topic).is_some() {
            info!("删除主题: {} / Deleting topic: {}", topic, topic);
            true
        } else {
            warn!("尝试删除不存在的主题: {} / Attempting to delete non-existent topic: {}", topic, topic);
            false
        }
    }
}

#[async_trait::async_trait]
impl QueueManager for TopicManager {
    async fn create_publisher(&self, topic: String) -> Box<dyn crate::mq::traits::MessagePublisher> {
        Box::new(Publisher::new(self.clone(), topic))
    }

    async fn create_subscriber(&self, topic: String) -> anyhow::Result<Box<dyn crate::mq::traits::MessageSubscriber>> {
        let subscriber = self.subscribe(topic).await?;
        Ok(Box::new(subscriber))
    }

    async fn list_topics(&self) -> Vec<String> {
        let topics = self.topics.read().await;
        topics.keys().cloned().collect()
    }
}
