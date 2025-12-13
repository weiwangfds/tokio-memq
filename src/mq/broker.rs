use async_channel::{bounded, unbounded, Sender, Receiver};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex, Notify};
use log::{debug, info, warn, trace};

use crate::mq::traits::QueueManager;
use crate::mq::message::{TopicMessage, TopicOptions, TimestampedMessage, ConsumptionMode};
use crate::mq::publisher::Publisher;
use crate::mq::subscriber::Subscriber;

#[derive(Clone)]
pub struct TopicManager {
    topics: Arc<RwLock<HashMap<String, TopicChannel>>>,
}

#[derive(Clone)]
pub struct TopicChannel {
    pub sender: Sender<TopicMessage>,
    pub receiver: Arc<Mutex<Receiver<TopicMessage>>>,
    subscriber_count: Arc<Mutex<usize>>,
    options: TopicOptions,
    message_buffer: Arc<Mutex<VecDeque<TimestampedMessage>>>,
    next_offset: Arc<Mutex<usize>>,
    consumer_offsets: Arc<RwLock<HashMap<String, usize>>>,
    notify: Arc<Notify>,
}

impl TopicChannel {
    pub fn new(options: TopicOptions) -> Self {
        let (sender, receiver) = if options.max_messages.is_some() {
            let capacity = options.max_messages.unwrap();
            bounded(capacity)
        } else {
            unbounded()
        };
        
        TopicChannel {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            subscriber_count: Arc::new(Mutex::new(0)),
            options,
            message_buffer: Arc::new(Mutex::new(VecDeque::new())),
            next_offset: Arc::new(Mutex::new(0)),
            consumer_offsets: Arc::new(RwLock::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
        }
    }

    pub async fn add_subscriber(&self, consumer_id: Option<String>, mode: ConsumptionMode) -> Subscriber {
        let mut count = self.subscriber_count.lock().await;
        *count += 1;

        // 如果指定了消费者 ID，初始化偏移量 / If consumer ID is specified, initialize offset
        if let Some(ref id) = consumer_id {
            debug!("检查消费者偏移量: {} / Checking consumer offset: {}", id, id);
            let has_offset = {
                let offsets = self.consumer_offsets.read().await;
                offsets.contains_key(id)
            };

            if !has_offset {
                let mut offsets = self.consumer_offsets.write().await;
                if !offsets.contains_key(id) {
                    let buffer = self.message_buffer.lock().await;
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
                }
            } else {
                debug!("消费者 {} 已存在偏移量 / Consumer {} already has offset", id, id);
            }
        }
        
        Subscriber::new(
            String::new(),
            Arc::clone(&self.receiver),
            Arc::clone(&self.subscriber_count),
            Arc::new(Mutex::new(None)),
            Arc::clone(&self.message_buffer),
            self.options.clone(),
            consumer_id,
            mode,
            Arc::clone(&self.consumer_offsets),
            Arc::clone(&self.notify),
        )
    }

    pub async fn cleanup_expired_messages(&self) {
        if let Some(ttl) = self.options.message_ttl {
            let mut buffer = self.message_buffer.lock().await;
            while let Some(msg) = buffer.front() {
                if msg.is_expired(ttl) {
                    buffer.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    pub async fn add_to_buffer(&self, message: TopicMessage) -> anyhow::Result<()> {
        let mut offset_lock = self.next_offset.lock().await;
        let current_offset = *offset_lock;
        *offset_lock += 1;
        drop(offset_lock);
        
        let timestamped_msg = TimestampedMessage::new(message, current_offset);
        
        let mut buffer = self.message_buffer.lock().await;
        
        if let Some(ttl) = self.options.message_ttl {
            while let Some(msg) = buffer.front() {
                if msg.is_expired(ttl) {
                    buffer.pop_front();
                } else {
                    break;
                }
            }
        }
        
        if let Some(max_messages) = self.options.max_messages {
            while buffer.len() >= max_messages {
                buffer.pop_front();
            }
        }
        
        buffer.push_back(timestamped_msg);
        // 通知等待的订阅者 / Notify waiting subscribers
        self.notify.notify_waiters();
        Ok(())
    }

    pub async fn get_valid_message(&self) -> Option<TopicMessage> {
        let mut buffer = self.message_buffer.lock().await;
        
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
        let topics = Arc::new(RwLock::new(HashMap::new()));
        TopicManager { topics }
    }

    pub async fn get_or_create_topic(&self, topic: String) -> TopicChannel {
        self.get_or_create_topic_with_options(topic, TopicOptions::default()).await
    }

    pub async fn get_or_create_topic_with_options(&self, topic: String, options: TopicOptions) -> TopicChannel {
        let mut topics = self.topics.write().await;
        
        if !topics.contains_key(&topic) {
            info!("创建新主题: {} / Creating new topic: {}", topic, topic);
            debug!("主题配置: max_messages={:?}, message_ttl={:?}, lru_enabled={} / Topic config: max_messages={:?}, message_ttl={:?}, lru_enabled={}", 
                   options.max_messages, options.message_ttl, options.lru_enabled, options.max_messages, options.message_ttl, options.lru_enabled);
            let channel = TopicChannel::new(options);
            topics.insert(topic.clone(), channel);
        } else {
            debug!("使用已存在的主题: {} / Using existing topic: {}", topic, topic);
        }
        
        topics.get(&topic).unwrap().clone()
    }

    pub async fn publish(&self, message: TopicMessage) -> anyhow::Result<()> {
        debug!("开始发布消息到主题: {}, 大小: {} 字节, 格式: {:?} / Start publishing message to topic: {}, size: {} bytes, format: {:?}", 
               message.topic, message.payload.len(), message.format, message.topic, message.payload.len(), message.format);
        info!("发布消息，主题: {}, {} / Publishing message, topic: {}, {}", message.topic, message.display_payload(256), message.topic, message.display_payload(256));
        
        let channel = self.get_or_create_topic(message.topic.clone()).await;
        
        channel.add_to_buffer(message.clone()).await?;
        
        // 如果开启了 LRU 且通道已满，则尝试淘汰旧消息 / If LRU is enabled and channel is full, try to evict old messages
        if channel.options.lru_enabled {
            match channel.sender.try_send(message.clone()) {
                Ok(_) => {},
                Err(async_channel::TrySendError::Full(_)) => {
                    debug!("主题 {} 通道已满，触发 LRU 淘汰 / Topic {} channel full, triggering LRU eviction", message.topic, message.topic);
                    {
                        let receiver = channel.receiver.lock().await;
                        // 丢弃最旧的一条消息 / Discard the oldest message
                        if let Ok(_) = receiver.try_recv() {
                            debug!("成功丢弃一条旧消息 / Successfully discarded an old message");
                        }
                    }
                    // 再次尝试发送，如果还满则阻塞等待 / Try sending again, block and wait if still full
                    channel.sender.send(message.clone()).await?;
                },
                Err(e) => return Err(anyhow::anyhow!(e)),
            }
        } else {
            // 未开启 LRU，默认阻塞等待 / LRU not enabled, default blocking wait
            channel.sender.send(message.clone()).await?;
        }
        
        debug!("消息已成功发布到主题: {} / Message successfully published to topic: {}", channel.sender.receiver_count(), channel.sender.receiver_count());
        Ok(())
    }

    pub async fn subscribe(&self, topic: String) -> anyhow::Result<Subscriber> {
        info!("创建订阅者，主题: {} / Creating subscriber, topic: {}", topic, topic);
        let channel = self.get_or_create_topic(topic.clone()).await;
        let mut subscriber = channel.add_subscriber(None, ConsumptionMode::default()).await;
        subscriber.topic_name = topic;
        
        let count = channel.subscriber_count.lock().await;
        debug!("主题订阅成功，当前订阅者数量: {} / Topic subscription successful, current subscriber count: {}", *count, *count);
        Ok(subscriber)
    }

    pub async fn subscribe_group(&self, topic: String, consumer_id: String, mode: ConsumptionMode) -> anyhow::Result<Subscriber> {
        info!("创建消费者组订阅者，主题: {}, ID: {}, 模式: {:?} / Creating consumer group subscriber, topic: {}, ID: {}, mode: {:?}", topic, consumer_id, mode, topic, consumer_id, mode);
        let channel = self.get_or_create_topic(topic.clone()).await;
        let mut subscriber = channel.add_subscriber(Some(consumer_id), mode).await;
        subscriber.topic_name = topic;
        
        let count = channel.subscriber_count.lock().await;
        debug!("主题订阅成功（消费者组），当前订阅者数量: {} / Topic subscription successful (consumer group), current subscriber count: {}", *count, *count);
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
        
        let count = channel.subscriber_count.lock().await;
        debug!("主题订阅成功（带选项），当前订阅者数量: {} / Topic subscription successful (with options), current subscriber count: {}", *count, *count);
        Ok(subscriber)
    }

    pub async fn subscribe_group_with_options(&self, topic: String, options: TopicOptions, consumer_id: String, mode: ConsumptionMode) -> anyhow::Result<Subscriber> {
        info!("创建带选项的消费者组订阅者，主题: {}, ID: {} / Creating consumer group subscriber with options, topic: {}, ID: {}", topic, consumer_id, topic, consumer_id);
        
        let channel = self.get_or_create_topic_with_options(topic.clone(), options.clone()).await;
        let mut subscriber = channel.add_subscriber(Some(consumer_id), mode).await;
        subscriber.topic_name = topic;
        subscriber.options = options;
        
        let count = channel.subscriber_count.lock().await;
        debug!("主题订阅成功（带选项和组），当前订阅者数量: {} / Topic subscription successful (with options and group), current subscriber count: {}", *count, *count);
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
            let count = channel.subscriber_count.lock().await;
            let count_val = *count;
            debug!("主题 {} 的订阅者数量: {} / Subscriber count for topic {}: {}", topic, count_val, topic, count_val);
            Some(count_val)
        } else {
            warn!("请求的主题不存在: {} / Requested topic does not exist: {}", topic, topic);
            None
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
