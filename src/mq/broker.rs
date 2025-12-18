use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::hash::{Hash, Hasher};
use tokio::sync::{RwLock, Mutex, watch};
use log::{debug, info, warn, trace};
use uuid::Uuid;

use crate::mq::traits::QueueManager;
use crate::mq::message::{TopicMessage, TopicOptions, TimestampedMessage, ConsumptionMode};
use crate::mq::publisher::Publisher;
use crate::mq::subscriber::Subscriber;

/// 分区路由策略
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionRouting {
    /// 轮询分配
    RoundRobin,
    /// 基于消息键的哈希分配
    Hash(String),
    /// 随机分配
    Random,
    /// 固定分区
    Fixed(usize),
}

/// 分区统计信息
#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub partition_id: usize,
    pub message_count: usize,
    pub subscriber_count: usize,
    pub dropped_messages: usize,
    pub consumer_lags: HashMap<String, usize>,
}


#[derive(Debug, Clone)]
pub struct TopicStats {
    pub message_count: usize,
    pub subscriber_count: usize,
    pub dropped_messages: usize,
    pub consumer_lags: HashMap<String, usize>,
    /// 总负载大小（字节）
    /// Total payload size in bytes
    pub total_payload_size: usize,
}

#[derive(Clone)]
pub struct TopicManager {
    topics: Arc<RwLock<HashMap<String, TopicChannel>>>,
    // 分区主题管理器，用于处理分区主题
    partitioned_topics: Arc<RwLock<HashMap<String, PartitionedTopicChannel>>>,
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
    // 公平调度支持：消费者组活跃状态跟踪
    group_stats: Arc<RwLock<HashMap<String, GroupStats>>>,
}

#[derive(Clone, Debug, Default)]
struct GroupStats {
    active_consumers: usize,
}

/// 分区主题通道
///
/// 支持多分区的主题通道，提供更高的并发处理能力
#[derive(Clone)]
pub struct PartitionedTopicChannel {
    partition_count: usize,
    partitions: Vec<TopicChannel>,
    routing_strategy: Arc<RwLock<PartitionRouting>>,
    round_robin_counter: Arc<AtomicUsize>,
}

impl PartitionedTopicChannel {
    /// 创建新的分区主题通道
    pub fn new(options: TopicOptions, partition_count: usize) -> Self {
        info!("创建分区主题通道，分区数: {} / Creating partitioned topic channel with {} partitions", partition_count, partition_count);

        let mut partitions = Vec::with_capacity(partition_count);
        for i in 0..partition_count {
            let partition_options = TopicOptions {
                partitions: None, // 分区内部不再递归分区
                ..options.clone()
            };
            partitions.push(TopicChannel::new(partition_options));
            debug!("创建分区 {} / Created partition {}", i, i);
        }

        PartitionedTopicChannel {
            partition_count,
            partitions,
            routing_strategy: Arc::new(RwLock::new(PartitionRouting::RoundRobin)),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 设置路由策略
    pub async fn set_routing_strategy(&self, strategy: PartitionRouting) {
        let mut routing = self.routing_strategy.write().await;
        *routing = strategy.clone();
        info!("设置分区路由策略: {:?} / Set partition routing strategy: {:?}", strategy, strategy);
    }

    /// 根据路由策略选择分区
    pub async fn select_partition(&self, message: &TopicMessage) -> usize {
        let routing = self.routing_strategy.read().await;
        match &*routing {
            PartitionRouting::RoundRobin => {
                let partition = self.round_robin_counter.fetch_add(1, Ordering::SeqCst) % self.partition_count;
                debug!("轮询路由选择分区 {} / Round-robin selected partition {}", partition, partition);
                partition
            },
            PartitionRouting::Hash(key) => {
                let hash_key = if let Some(ref msg_key) = message.key {
                    if key == "message" {
                        msg_key
                    } else {
                        key
                    }
                } else {
                    key
                };

                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                hash_key.hash(&mut hasher);
                let hash = hasher.finish();
                let partition = (hash as usize) % self.partition_count;
                debug!("哈希路由选择分区 {} (key: {}) / Hash routing selected partition {} (key: {})", partition, hash_key, partition, hash_key);
                partition
            },
            PartitionRouting::Random => {
                use std::collections::hash_map::DefaultHasher;
                use std::time::SystemTime;

                let mut hasher = DefaultHasher::new();
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
                    .hash(&mut hasher);
                let hash = hasher.finish();
                let partition = (hash as usize) % self.partition_count;
                debug!("随机路由选择分区 {} / Random routing selected partition {}", partition, partition);
                partition
            },
            PartitionRouting::Fixed(partition) => {
                let partition = *partition % self.partition_count;
                debug!("固定路由选择分区 {} / Fixed routing selected partition {}", partition, partition);
                partition
            },
        }
    }

    /// 发布消息到指定分区
    pub async fn publish_to_partition(&self, message: TopicMessage, partition: usize) -> anyhow::Result<()> {
        if partition >= self.partition_count {
            return Err(anyhow::anyhow!("分区索引超出范围: {} >= {} / Partition index out of range: {} >= {}", partition, self.partition_count, partition, self.partition_count));
        }

        debug!("发布消息到分区 {}: {} / Publishing message to partition {}: {}", partition, message.display_payload(100), partition, message.display_payload(100));
        self.partitions[partition].add_to_buffer(message).await
    }

    /// 批量发布消息到指定分区
    pub async fn publish_batch_to_partition(&self, messages: Vec<TopicMessage>, partition: usize) -> anyhow::Result<()> {
        if partition >= self.partition_count {
            return Err(anyhow::anyhow!("分区索引超出范围: {} >= {} / Partition index out of range: {} >= {}", partition, self.partition_count, partition, self.partition_count));
        }

        debug!("批量发布 {} 条消息到分区 {} / Publishing {} messages to partition {}", messages.len(), partition, messages.len(), partition);
        self.partitions[partition].add_to_buffer_batch(messages).await
    }

    /// 批量发布消息（保持顺序）
    pub async fn publish_batch_ordered(&self, messages: Vec<TopicMessage>) -> anyhow::Result<()> {
        if messages.is_empty() { return Ok(()); }

        let routing = self.routing_strategy.read().await;
        match &*routing {
            // 对于固定分区和哈希分区，保持消息组内顺序
            PartitionRouting::Fixed(_) | PartitionRouting::Hash(_) => {
                let mut partition_groups: HashMap<usize, Vec<TopicMessage>> = HashMap::new();

                for msg in messages {
                    let partition = match &*routing {
                        PartitionRouting::Fixed(partition) => *partition,
                        PartitionRouting::Hash(hash_key) => {
                            // 哈希分区：相同键的消息在同一分区，保持键内顺序
                            let key_to_hash = if let Some(ref msg_key) = msg.key {
                                if hash_key == "message" {
                                    msg_key
                                } else {
                                    hash_key
                                }
                            } else {
                                hash_key
                            };

                            let mut hasher = std::collections::hash_map::DefaultHasher::new();
                            key_to_hash.hash(&mut hasher);
                            let hash = hasher.finish();
                            (hash as usize) % self.partition_count
                        },
                        _ => unreachable!(), // 已经处理过其他情况
                    };
                    partition_groups.entry(partition).or_default().push(msg);
                }

                // 按分区顺序发布，保持每个分区内消息顺序
                let mut partitions: Vec<_> = partition_groups.into_iter().collect();
                partitions.sort_by_key(|(partition, _)| *partition);

                for (partition, msgs) in partitions {
                    self.publish_batch_to_partition(msgs, partition).await?;
                }
            },
            // 对于轮询分区，为了保持全局顺序，需要特殊处理
            PartitionRouting::RoundRobin => {
                // 如果只有一个分区，直接发布到分区0
                if self.partition_count == 1 {
                    self.publish_batch_to_partition(messages, 0).await?;
                } else {
                    // 多分区轮询：为了保持顺序，每个消息单独选择分区
                    for msg in messages {
                        let partition = self.round_robin_counter.fetch_add(1, Ordering::SeqCst) % self.partition_count;
                        self.publish_to_partition(msg, partition).await?;
                    }
                }
            },
            PartitionRouting::Random => {
                // 随机分区：无法保证顺序，但我们可以尽量减少随机性
                for msg in messages {
                    let partition = {
                        use std::collections::hash_map::DefaultHasher;
                        use std::time::SystemTime;

                        let mut hasher = DefaultHasher::new();
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos()
                            .hash(&mut hasher);
                        let hash = hasher.finish();
                        (hash as usize) % self.partition_count
                    };
                    self.publish_to_partition(msg, partition).await?;
                }
            },
        }

        Ok(())
    }

    /// 获取指定分区的统计信息
    pub async fn get_partition_stats(&self, partition: usize) -> Option<PartitionStats> {
        if partition >= self.partition_count {
            return None;
        }

        let stats = self.partitions[partition].get_stats().await;
        Some(PartitionStats {
            partition_id: partition,
            message_count: stats.message_count,
            subscriber_count: stats.subscriber_count,
            dropped_messages: stats.dropped_messages,
            consumer_lags: stats.consumer_lags,
        })
    }

    /// 获取所有分区的统计信息
    pub async fn get_all_partition_stats(&self) -> Vec<PartitionStats> {
        let mut all_stats = Vec::with_capacity(self.partition_count);
        for i in 0..self.partition_count {
            if let Some(stats) = self.get_partition_stats(i).await {
                all_stats.push(stats);
            }
        }
        all_stats
    }

    /// 创建订阅者到指定分区
    pub async fn add_subscriber_to_partition(
        &self,
        topic_name: String,
        partition: usize,
        consumer_id: Option<String>,
        mode: ConsumptionMode,
    ) -> anyhow::Result<Subscriber> {
        if partition >= self.partition_count {
            return Err(anyhow::anyhow!("分区索引超出范围: {} >= {} / Partition index out of range: {} >= {}", partition, self.partition_count, partition, self.partition_count));
        }

        debug!("创建订阅者到分区 {}，消费者ID: {:?} / Creating subscriber to partition {}, consumer ID: {:?}", partition, consumer_id, partition, consumer_id);
        Ok(self.partitions[partition].add_subscriber(topic_name, consumer_id, mode).await)
    }
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
            group_stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_subscriber(&self, topic_name: String, consumer_id: Option<String>, mode: ConsumptionMode) -> Subscriber {
        self.subscriber_count.fetch_add(1, Ordering::SeqCst);

        // 如果没有提供消费者ID，自动生成UUID（现在所有消费者都有ID）
        let consumer_id = match consumer_id {
            Some(id) => id,
            None => {
                let id = Uuid::new_v4().to_string();
                debug!("自动生成消费者ID: {} / Auto-generated consumer ID: {}", id, id);
                id
            }
        };

        // 检查消费者偏移量
        debug!("检查消费者偏移量: {} / Checking consumer offset: {}", consumer_id, consumer_id);
        let start_offset_local = {
            let offsets = self.consumer_offsets.read().await;
            *offsets.get(&consumer_id).unwrap_or(&0)
        };

        // 如果是新的消费者ID，初始化偏移量并创建公平调度计数器
        let has_offset = {
            let offsets = self.consumer_offsets.read().await;
            offsets.contains_key(&consumer_id)
        };

        if !has_offset {
            let mut offsets = self.consumer_offsets.write().await;
            let buffer = self.message_buffer.read().await;
            let start_offset = match mode {
                ConsumptionMode::Earliest => {
                    buffer.front().map(|m| m.offset).unwrap_or(0)
                },
                ConsumptionMode::Latest => {
                    // Latest 应该是 buffer.back().offset + 1
                    buffer.back().map(|m| m.offset + 1).unwrap_or(0)
                },
                ConsumptionMode::Offset(offset) => offset,
                ConsumptionMode::LastOffset => {
                    // 默认行为：使用 Latest
                    buffer.back().map(|m| m.offset + 1).unwrap_or(0)
                }
            };
            debug!("初始化消费者偏移量, ID: {}, Mode: {:?}, Start Offset: {} / Initializing consumer offset, ID: {}, Mode: {:?}, Start Offset: {}", consumer_id, mode, start_offset, consumer_id, mode, start_offset);
            offsets.insert(consumer_id.clone(), start_offset);
            drop(offsets);

            // 更新消费者组统计信息
            let mut stats = self.group_stats.write().await;
            let group_stat = stats.entry(consumer_id.clone()).or_insert_with(GroupStats::default);
            group_stat.active_consumers += 1;
            debug!("更新消费者组统计: {} 活跃消费者数: {} / Updated consumer group stats: {} active consumers: {}", consumer_id, group_stat.active_consumers, consumer_id, group_stat.active_consumers);
        }

        Subscriber::new(
            topic_name,
            Arc::clone(&self.subscriber_count),
            Arc::new(Mutex::new(Some(start_offset_local))),
            Arc::clone(&self.message_buffer),
            self.options.clone(),
            Some(consumer_id),
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
        
        // 计算总负载大小
        // Calculate total payload size
        let total_payload_size: usize = buffer.iter()
            .map(|msg| msg.message.payload.len())
            .sum();

        let subscriber_count = self.subscriber_count.load(Ordering::SeqCst);
        let dropped_messages = self.dropped_count.load(Ordering::SeqCst);
        
        let mut consumer_lags = HashMap::new();
        let offsets = self.consumer_offsets.read().await;
        let next_offset = self.next_offset.load(Ordering::SeqCst);
        
        for (id, offset) in offsets.iter() {
            let lag = next_offset.saturating_sub(*offset);
            consumer_lags.insert(id.clone(), lag);
        }

        TopicStats {
            message_count,
            subscriber_count,
            dropped_messages,
            consumer_lags,
            total_payload_size,
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
        let partitioned_topics: Arc<RwLock<HashMap<String, PartitionedTopicChannel>>> = Arc::new(RwLock::new(HashMap::new()));
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
        TopicManager {
            topics,
            partitioned_topics,
        }
    }

    pub async fn get_or_create_topic(&self, topic: Arc<String>) -> TopicChannel {
        self.get_or_create_topic_with_options(topic, TopicOptions::default()).await
    }

    pub async fn get_or_create_topic_with_options(&self, topic: Arc<String>, options: TopicOptions) -> TopicChannel {
        // Fast path: try read lock first
        {
            let topics = self.topics.read().await;
            if let Some(channel) = topics.get(topic.as_str()) {
                debug!("使用已存在的主题: {} / Using existing topic: {}", topic, topic);
                return channel.clone();
            }
        }

        // Slow path: write lock
        let mut topics = self.topics.write().await;
        // Double check
        if let Some(channel) = topics.get(topic.as_str()) {
            return channel.clone();
        }

        info!("创建新主题: {} / Creating new topic: {}", topic, topic);
        debug!("主题配置: max_messages={:?}, message_ttl={:?}, lru_enabled={} / Topic config: max_messages={:?}, message_ttl={:?}, lru_enabled={}", 
               options.max_messages, options.message_ttl, options.lru_enabled, options.max_messages, options.message_ttl, options.lru_enabled);
        let channel = TopicChannel::new(options);
        topics.insert(topic.to_string(), channel.clone());
        
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
        
        let mut groups: HashMap<Arc<String>, Vec<TopicMessage>> = HashMap::new();
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
        let channel = self.get_or_create_topic(Arc::new(topic.clone())).await;
        let subscriber = channel.add_subscriber(topic.clone(), None, ConsumptionMode::default()).await;

        let count = channel.subscriber_count.load(Ordering::SeqCst);
        debug!("主题订阅成功，当前订阅者数量: {} / Topic subscription successful, current subscriber count: {}", count, count);
        Ok(subscriber)
    }

    pub async fn subscribe_group(&self, topic: String, consumer_id: String, mode: ConsumptionMode) -> anyhow::Result<Subscriber> {
        info!("创建消费者组订阅者，主题: {}, ID: {}, 模式: {:?} / Creating consumer group subscriber, topic: {}, ID: {}, mode: {:?}", topic, consumer_id, mode, topic, consumer_id, mode);
        let channel = self.get_or_create_topic(Arc::new(topic.clone())).await;
        let subscriber = channel.add_subscriber(topic.clone(), Some(consumer_id), mode).await;

        let count = channel.subscriber_count.load(Ordering::SeqCst);
        debug!("主题订阅成功（消费者组），当前订阅者数量: {} / Topic subscription successful (consumer group), current subscriber count: {}", count, count);
        Ok(subscriber)
    }

    pub async fn subscribe_with_options(&self, topic: String, options: TopicOptions) -> anyhow::Result<Subscriber> {
        info!("创建带选项的订阅者，主题: {} / Creating subscriber with options, topic: {}", topic, topic);
        debug!("订阅选项: max_messages={:?}, message_ttl={:?}, lru_enabled={} / Subscription options: max_messages={:?}, message_ttl={:?}, lru_enabled={}",
               options.max_messages, options.message_ttl, options.lru_enabled, options.max_messages, options.message_ttl, options.lru_enabled);

        let channel = self.get_or_create_topic_with_options(Arc::new(topic.clone()), options.clone()).await;
        let subscriber = channel.add_subscriber(topic.clone(), None, ConsumptionMode::default()).await;

        let count = channel.subscriber_count.load(Ordering::SeqCst);
        debug!("主题订阅成功（带选项），当前订阅者数量: {} / Topic subscription successful (with options), current subscriber count: {}", count, count);
        Ok(subscriber)
    }

    pub async fn subscribe_with_options_and_mode(&self, topic: String, options: TopicOptions, mode: ConsumptionMode) -> anyhow::Result<Subscriber> {
        info!("创建带选项的订阅者（含模式），主题: {} / Creating subscriber with options and mode, topic: {}", topic, topic);
        debug!("订阅选项: max_messages={:?}, message_ttl={:?}, lru_enabled={}, 模式: {:?} / Subscription options: max_messages={:?}, message_ttl={:?}, lru_enabled={}, mode: {:?}",
               options.max_messages, options.message_ttl, options.lru_enabled, mode, options.max_messages, options.message_ttl, options.lru_enabled, mode);

        let channel = self.get_or_create_topic_with_options(Arc::new(topic.clone()), options.clone()).await;
        let subscriber = channel.add_subscriber(topic.clone(), None, mode).await;

        let count = channel.subscriber_count.load(Ordering::SeqCst);
        debug!("主题订阅成功（带选项与模式），当前订阅者数量: {} / Topic subscription successful (with options and mode), current subscriber count: {}", count, count);
        Ok(subscriber)
    }

    pub async fn subscribe_group_with_options(&self, topic: String, options: TopicOptions, consumer_id: String, mode: ConsumptionMode) -> anyhow::Result<Subscriber> {
        info!("创建带选项的消费者组订阅者，主题: {}, ID: {} / Creating consumer group subscriber with options, topic: {}, ID: {}", topic, consumer_id, topic, consumer_id);

        let channel = self.get_or_create_topic_with_options(Arc::new(topic.clone()), options.clone()).await;
        let subscriber = channel.add_subscriber(topic.clone(), Some(consumer_id), mode).await;

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

    /// 创建或获取分区主题
    pub async fn get_or_create_partitioned_topic(&self, topic: String, options: TopicOptions, partition_count: usize) -> anyhow::Result<PartitionedTopicChannel> {
        let mut topics = self.partitioned_topics.write().await;

        if let Some(channel) = topics.get(&topic) {
            debug!("使用已存在的分区主题: {} / Using existing partitioned topic: {}", topic, topic);
            return Ok(channel.clone());
        }

        info!("创建新的分区主题: {}, 分区数: {} / Creating new partitioned topic: {}, partition count: {}", topic, partition_count, topic, partition_count);
        let channel = PartitionedTopicChannel::new(options, partition_count);
        topics.insert(topic.clone(), channel.clone());

        Ok(channel)
    }

    /// 发布消息到分区主题（自动路由）
    pub async fn publish_to_partitioned(&self, message: TopicMessage) -> anyhow::Result<()> {
        let topics = self.partitioned_topics.read().await;
        if let Some(channel) = topics.get(message.topic.as_str()) {
            let partition = channel.select_partition(&message).await;
            channel.publish_to_partition(message, partition).await
        } else {
            Err(anyhow::anyhow!("分区主题不存在: {} / Partitioned topic does not exist: {}", message.topic, message.topic))
        }
    }

    /// 批量发布消息到分区主题（自动路由）
    pub async fn publish_batch_to_partitioned(&self, messages: Vec<TopicMessage>) -> anyhow::Result<()> {
        if messages.is_empty() { return Ok(()); }

        let mut topic_groups: HashMap<Arc<String>, Vec<TopicMessage>> = HashMap::new();
        for msg in messages {
            topic_groups.entry(msg.topic.clone()).or_default().push(msg);
        }

        for (topic, msgs) in topic_groups {
            let topics = self.partitioned_topics.read().await;
            if let Some(channel) = topics.get(topic.as_str()) {
                // 使用有序批量发布，在可能的情况下保持消息顺序
                channel.publish_batch_ordered(msgs).await?;
            } else {
                return Err(anyhow::anyhow!("分区主题不存在: {} / Partitioned topic does not exist: {}", topic, topic));
            }
        }

        Ok(())
    }

    /// 创建订阅者到指定分区
    pub async fn subscribe_partition(
        &self,
        topic: String,
        partition: usize,
        consumer_id: Option<String>,
        mode: ConsumptionMode,
    ) -> anyhow::Result<Subscriber> {
        let topics = self.partitioned_topics.read().await;
        if let Some(channel) = topics.get(&topic) {
            channel.add_subscriber_to_partition(topic, partition, consumer_id, mode).await
        } else {
            Err(anyhow::anyhow!("分区主题不存在: {} / Partitioned topic does not exist: {}", topic, topic))
        }
    }

    /// 获取分区统计信息
    pub async fn get_partition_stats(&self, topic: String, partition: usize) -> Option<PartitionStats> {
        let topics = self.partitioned_topics.read().await;
        topics.get(&topic)?.get_partition_stats(partition).await
    }

    /// 获取所有分区统计信息
    pub async fn get_all_partition_stats(&self, topic: String) -> Vec<PartitionStats> {
        let topics = self.partitioned_topics.read().await;
        topics.get(&topic)
            .map(|channel| async { channel.get_all_partition_stats().await })
            .map(|fut| tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut)))
            .unwrap_or_default()
    }

    /// 设置分区路由策略
    pub async fn set_partition_routing(&self, topic: String, strategy: PartitionRouting) -> anyhow::Result<()> {
        let topics = self.partitioned_topics.read().await;
        if let Some(channel) = topics.get(&topic) {
            channel.set_routing_strategy(strategy).await;
            Ok(())
        } else {
            Err(anyhow::anyhow!("分区主题不存在: {} / Partitioned topic does not exist: {}", topic, topic))
        }
    }

    /// 删除分区主题
    pub async fn delete_partitioned_topic(&self, topic: &str) -> bool {
        let mut topics = self.partitioned_topics.write().await;
        if topics.remove(topic).is_some() {
            info!("删除分区主题: {} / Deleting partitioned topic: {}", topic, topic);
            true
        } else {
            warn!("尝试删除不存在的分区主题: {} / Attempting to delete non-existent partitioned topic: {}", topic, topic);
            false
        }
    }

    /// 列出所有分区主题
    pub async fn list_partitioned_topics(&self) -> Vec<String> {
        let topics = self.partitioned_topics.read().await;
        topics.keys().cloned().collect()
    }
}

impl Default for TopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl QueueManager for TopicManager {
    type Publisher = Publisher;
    type Subscriber = Subscriber;

    async fn create_publisher(&self, topic: String) -> Self::Publisher {
        Publisher::new(self.clone(), topic)
    }

    async fn create_subscriber(&self, topic: String) -> anyhow::Result<Self::Subscriber> {
        let subscriber = self.subscribe(topic).await?;
        Ok(subscriber)
    }

    async fn list_topics(&self) -> Vec<String> {
        let topics = self.topics.read().await;
        topics.keys().cloned().collect()
    }
}
