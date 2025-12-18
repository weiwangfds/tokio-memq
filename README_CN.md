# Tokio MemQ (Async-MQ)

[![Crates.io](https://img.shields.io/crates/v/tokio-memq.svg)](https://crates.io/crates/tokio-memq)
[![Documentation](https://docs.rs/tokio-memq/badge.svg)](https://docs.rs/tokio-memq)
[![License](https://img.shields.io/crates/l/tokio-memq.svg)](LICENSE)

[English](README.md) | [中文](README_CN.md)

**Tokio MemQ** 是一个专为 Rust 应用程序设计的高性能、功能丰富的内存异步消息队列。它基于 Tokio 运行时构建，提供极低的延迟、高吞吐量，并支持消费者组、批量处理和分区主题等高级消息传递模式。

它是高并发本地事件总线、线程间通信以及缓冲重型工作负载（如视频处理、日志聚合）的理想选择。

---

## 📦 安装

将以下内容添加到你的 `Cargo.toml`：

```toml
[dependencies]
tokio-memq = "1.0.0"
tokio = { version = "1", features = ["full"] }
anyhow = "1.0"
serde = { version = "1.0", features = ["derive"] }
```

---

## 1. 项目架构

`async-mq` 的架构设计旨在最大化并发性并最小化锁竞争。

### 1.1 高层设计

```mermaid
graph TD
    subgraph "Producer Layer"
        P1[Publisher A]
        P2[Publisher B]
        P3[Batch Publisher]
    end

    subgraph "Core Broker (MessageQueue)"
        TM[TopicManager]
        
        subgraph "Topic Channel (RWLock)"
            Buffer[Ring Buffer (VecDeque)]
            LRU[LRU Eviction Policy]
            Stats[Topic Stats & Metrics]
        end
        
        subgraph "Partitioned Topic"
            Part0[Partition 0]
            Part1[Partition 1]
            Router[Routing Strategy]
        end
    end

    subgraph "Consumer Layer"
        S1[Subscriber (Broadcast)]
        S2[Consumer Group X - Member 1]
        S3[Consumer Group X - Member 2]
    end

    P1 -->|Publish| TM
    P2 -->|Publish| TM
    P3 -->|Batch Publish| TM
    
    TM -->|Route| Buffer
    TM -->|Route| Router
    Router --> Part0
    Router --> Part1

    Buffer -->|Notify| S1
    Buffer -->|Notify| S2
    Buffer -->|Notify| S3
```

### 1.2 核心数据结构
- **消息缓冲区 (Message Buffer)**: 一个受 `RwLock` 保护的 `VecDeque<TimestampedMessage>`。这个环形缓冲区支持高效的 O(1) 推入/弹出操作。
- **通知系统 (Notification System)**: 使用 `tokio::sync::watch` 广播新消息可用性。这避免了在高扇出（Fan-out）场景下标准 MPMC 通道带来的 O(N) 内存开销。
- **消息负载 (Message Payload)**: 包装在 `Arc<Vec<u8>>` 或 `Arc<dyn Any>` (Native) 中。这种 "零拷贝" 设计意味着大负载（如 100MB 视频帧）在路由过程中永远不会被深度复制，只增加引用计数。

### 1.3 内存管理
- **LRU 淘汰**: 当达到 `max_messages` 上限时，最旧的消息会自动从队列头部丢弃，以便为新消息腾出空间。
- **TTL (生存时间)**: 超过 `message_ttl` 的消息在消费时会被跳过，并进行惰性清理。

---

## 2. 核心特性

### 🚀 核心消息传递
- **异步 API**: 完全非阻塞，基于 `async/await` 和 Tokio 构建。
- **广播与单播**: 支持发布/订阅（广播给所有人）和队列（竞争消费者）模式。
- **零拷贝消息传递**: `publish_bytes` 和 `publish_shared_bytes` 允许以零开销传递大数据块（100MB+）。

### ⚙️ 高级控制
- **消费者组**: 使用共享偏移量在多个消费者之间进行负载均衡。
- **批量处理**: `publish_batch` 和 `recv_batch` API 用于最大化吞吐量（最高 3 倍加速）。
- **分区 (Sharding)**: 使用轮询、哈希或随机路由策略进行水平扩展。
- **消息回放**: `seek(offset)` 和 `Earliest`/`Latest` 模式允许回放历史消息。
- **Stream API**: 与 `tokio_stream` 完全集成，支持惯用的流式处理。

### 🛡️ 可靠性与管理
- **流控**: 可配置的缓冲区大小 (`max_messages`) 和 TTL（生存时间）。
- **序列化管道**: 内置支持 **Bincode**、**JSON**、**MessagePack** 和 **Native**（原始字节）。支持 **Gzip/Zstd** 压缩。
- **监控**: 通过 `get_topic_stats()` 获取实时统计信息（消息深度、丢弃计数、订阅者数量）。

---

## 3. 适用场景

`async-mq` 最适合性能至关重要的 **本地、内存中** 工作负载。

### ✅ 最佳用例
1.  **高性能事件总线**: 连接单个高负载应用程序中的解耦模块（例如游戏服务器、交易引擎）。
2.  **线程数据解耦**: 在线程间安全传递数据所有权（例如从 UI 线程到后台工作线程），无需复杂的 `Mutex`/`RwLock` 管理。
3.  **数据处理流水线**: 在处理阶段之间传递大数据块（图像、视频帧、AI 张量）而不进行复制。
3.  **日志缓冲**: 在批量写入磁盘/网络之前，聚合来自数千个线程的日志/指标。
4.  **流量整形**: 在访问受速率限制的外部 API 之前，使用队列平滑突发流量。

### ❌ 不适合
- **分布式系统**: 这不是跨网络边界的 Kafka/RabbitMQ 替代品（除非封装在网络层中）。
- **磁盘持久化**: 消息是易失性的，存在于 RAM 中。如果进程崩溃，数据将丢失。

### 🆚 对比

| 特性 | `tokio::sync::broadcast` | `tokio::sync::mpsc` | `tokio-memq` |
| :--- | :--- | :--- | :--- |
| **模式** | Pub/Sub | Queue | Pub/Sub + Queue |
| **持久化** | 无 | 无 | 内存中 (可配置) |
| **回放** | 有限 (lag) | 无 | 有 (Seek/Offset) |
| **消费者组** | 无 | 有 (单个接收者) | 有 (多个消费者) |
| **序列化** | 无 | 无 | 内置 (可插拔) |
| **批量处理** | 无 | 无 | 有 |

---

## 4. 性能测试

我们在标准的 macOS 工作站（Apple Silicon）上进行了全面的性能测试。

### 4.1 测试场景
1.  **基准**: 1 生产者 / 1 消费者 (1KB 负载)。
2.  **高并发**: 10 生产者 / 10 消费者 (扇出)。
3.  **批量处理**: 批量大小 100/1000。
4.  **大对象**: 1MB - 100MB 负载 (零拷贝)。

### 4.2 基准测试结果

| 场景 | 负载 | 配置 | 吞吐量 | 消息速率 | 状态 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **基准** | 1 KB | 1P / 1C | ~780 MiB/s | **~798,000 ops/s** | ✅ |
| **负载测试** | 1 KB | 10P / 10C | ~2.0 GB/s | **~2,050,000 ops/s** | ✅ |
| **批量处理** | 1 KB | Batch 100 | ~2.2 GB/s | **~2,265,000 ops/s** | ✅ |
| **大对象** | 1 MB | 零拷贝 | **~2.9 GB/s** | ~2,900 ops/s | ✅ |
| **超大对象** | 100 MB | 零拷贝 | **~2.1 GB/s** | ~21 ops/s | ✅ |

### 4.3 优化建议
- **启用批量处理**: 对于小消息 (<1KB)，使用 `recv_batch(100)` 可以提高 300% 的性能。
- **大数据使用 Native/Bytes**: 对于大块数据，使用 `publish_bytes` 或 `SerializationFormat::Native` 避免序列化开销。
- **调整缓冲区大小**: 对于突发生产者（例如 10P/10C 测试），确保 `max_messages` 足够大（例如 1,000,000）以避免 LRU 淘汰丢弃。

---

## 5. 使用示例

### 5.1 基础 Pub/Sub
```rust
use tokio_memq::mq::MessageQueue;
use tokio_memq::{MessageSubscriber, AsyncMessagePublisher};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mq = MessageQueue::new();
    let topic = "chat";

    // 1. 创建订阅者
    let sub = mq.subscriber(topic.to_string()).await?;

    // 2. 发布消息
    let pub_instance = mq.publisher(topic.to_string());
    tokio::spawn(async move {
        pub_instance.publish("Hello World".to_string()).await.unwrap();
    });

    // 3. 接收消息
    let msg = sub.recv().await?;
    let payload: String = msg.deserialize()?;
    println!("Received: {}", payload);

    Ok(())
}
```

### 5.2 Stream API (异步迭代)
使用 Rust 的 `Stream` trait 消费消息，非常适合 `while let` 循环。

```rust
use tokio_stream::StreamExt;

let stream = sub.stream();
tokio::pin!(stream);

while let Some(msg_result) = stream.next().await {
    let msg = msg_result?;
    println!("Got msg: {:?}", msg);
}
```

### 5.3 高吞吐量批量处理
```rust
// 发布端
let messages: Vec<i32> = (0..100).collect();
publisher.publish_batch(messages).await?;

// 订阅端
let batch_size = 100;
let msgs = sub.recv_batch(batch_size).await?;
println!("Processed {} messages", msgs.len());
```

### 5.4 消费者组 (负载均衡)
```rust
use tokio_memq::mq::{ConsumptionMode, TopicOptions};

let options = TopicOptions::default();
let group_id = "worker_group_1";

// 创建两个共享相同组 ID 的消费者
let worker1 = mq.subscriber_group_with_options(
    "jobs".to_string(), 
    options.clone(), 
    group_id.to_string(), 
    ConsumptionMode::LastOffset
).await?;

let worker2 = mq.subscriber_group_with_options(
    "jobs".to_string(), 
    options, 
    group_id.to_string(), 
    ConsumptionMode::LastOffset
).await?;

// 消息将在 worker1 和 worker2 之间分发
```

### 5.5 分区主题 (分片)
```rust
use tokio_memq::mq::{TopicOptions, PartitionRouting};

// 创建一个包含 4 个分区的主题
let opts = TopicOptions { partitions: Some(4), ..Default::default() };
mq.create_partitioned_topic("events".to_string(), opts, 4).await?;

// 自动路由消息（默认为轮询）
mq.set_partition_routing("events".to_string(), PartitionRouting::RoundRobin).await?;
mq.publish_to_partitioned(msg).await?;
```

### 5.6 自定义序列化与压缩
```rust
use tokio_memq::mq::{
    SerializationFactory, SerializationFormat, SerializationConfig, 
    JsonConfig, PipelineConfig, CompressionConfig
};

let topic = "compressed_logs";

// 配置压缩管道 (Gzip Level 6)
let pipeline = PipelineConfig {
    compression: CompressionConfig::Gzip { level: Some(6) },
    pre: None, 
    post: None,
    use_magic_header: true, // 接收时自动检测格式
};

// 为主题注册默认配置
SerializationFactory::register_topic_defaults(
    topic,
    SerializationFormat::Json,
    SerializationConfig::Json(JsonConfig { pretty: false }),
    Some(pipeline),
);

// 现在所有发布到 "compressed_logs" 的消息都将自动序列化为 JSON 并进行 Gzip 压缩。
```

---

## 6. 配置参考

### TopicOptions
| 字段 | 类型 | 默认值 | 描述 |
| :--- | :--- | :--- | :--- |
| `max_messages` | `Option<usize>` | `Some(10000)` | 保留的最大消息数。超过时最旧的消息将被淘汰 (LRU)。 |
| `message_ttl` | `Option<Duration>` | `None` | 生存时间。消费时跳过比此更旧的消息。 |
| `lru_enabled` | `bool` | `true` | 是否在达到 `max_messages` 时丢弃旧消息。如果为 false，发布可能会阻塞或失败。 |
| `partitions` | `Option<usize>` | `None` | 分片主题的分区数量。 |
| `idle_timeout` | `Option<Duration>` | `None` | (预留) 主题空闲超时。 |
| `consume_idle_timeout` | `Option<Duration>` | `None` | (预留) 消费者空闲超时。 |

### ConsumptionMode
| 模式 | 描述 |
| :--- | :--- |
| `Earliest` | 从缓冲区中最早的可用消息开始消费。 |
| `Latest` | 仅消费订阅后到达的新消息。 |
| `Offset(n)` | 确切地从偏移量 `n` 开始消费。 |
| `LastOffset` | (默认) 从该消费者组上次提交的偏移量继续。 |

---

## 7. 开发与测试

### 运行测试
```bash
cargo test
```

### 运行基准测试
我们提供了一个专门的性能测试示例：
```bash
# 运行标准性能测试
cargo run --release --example perf_runner

# 运行大对象测试 (1MB - 100MB)
cargo run --release --example large_object_test
```

---

## 🤝 贡献

我们欢迎所有贡献！请按照以下步骤为 `tokio-memq` 做贡献：

1.  **Fork 仓库**: 点击 GitHub 页面右上角的 "Fork" 按钮。
2.  **创建分支**: 为你的功能或 bug 修复创建一个新分支。
    ```bash
    git checkout -b feature/amazing-feature
    ```
3.  **提交更改**: 确保你的代码遵循现有的风格并通过所有测试。
    ```bash
    git commit -m "feat: add amazing feature"
    ```
4.  **推送到分支**: 将你的更改推送到你 fork 的仓库。
    ```bash
    git push origin feature/amazing-feature
    ```
5.  **发起 Pull Request**: 转到原始仓库并点击 "New Pull Request"。清楚地描述你的更改。

### 报告问题
如果你遇到任何 bug 或有功能请求，请在我们的 [GitHub Issues](https://github.com/weiwangfds/tokio-memq/issues) 页面上提交 issue。请包含：
- 问题的清晰描述。
- 最小复现步骤或代码片段。
- 你的环境详情（操作系统，Rust 版本）。
