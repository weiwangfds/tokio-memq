# 消费者行为说明

Tokio MemQ 现在支持两种主要的消费模式：**公平竞争消费** 和 **独立消费**。

## 新特性

### ✅ 自动UUID生成
当创建订阅者时不提供消费者ID时，系统会自动生成唯一的UUID作为消费者ID。这确保每个消费者都有唯一的标识符。

### ✅ 公平调度机制
相同消费者ID的多个消费者现在通过公平调度机制竞争消息，确保负载均衡。每个消费者都有平等的机会获取消息。

## 消费者类型

### 1. 公平竞争消费者（有消费者ID）

当创建订阅者时指定了 `consumer_id`，该消费者会加入一个消费者组：

```rust
let subscriber = mq.subscriber_group("topic".to_string(), "group_id".to_string(), ConsumptionMode::Earliest).await?;
```

**行为特点：**
- 相同 `consumer_id` 的多个消费者会**公平竞争消费**消息
- 通过公平调度算法确保负载均衡，避免某个消费者垄断消息
- 每条消息只能被同一个 `consumer_id` 中的一个消费者接收
- 偏移量在相同 `consumer_id` 的消费者之间共享
- 适合工作队列、负载均衡场景

**公平调度机制：**
- 每个消费者基于其唯一标识符获得不同的延迟时间
- 相同ID的消费者通过0-5ms的随机延迟实现公平竞争
- 确保没有消费者能持续垄断消息获取

**示例：**
```rust
// 两个相同ID的消费者公平竞争消费
let worker1 = mq.subscriber_group("tasks".to_string(), "workers".to_string(), mode).await?;
let worker2 = mq.subscriber_group("tasks".to_string(), "workers".to_string(), mode).await?;

// 任务会公平分配给两个worker处理
```

### 2. 独立消费者（自动UUID生成）

当创建订阅者时不指定 `consumer_id`，系统会自动生成UUID：

```rust
let subscriber = mq.subscriber("topic".to_string()).await?;
// 或者
let subscriber = mq.subscriber_with_options_and_mode(
    "topic".to_string(),
    options,
    ConsumptionMode::Earliest
).await?;
```

**行为特点：**
- 自动生成唯一的UUID作为消费者ID
- 每个独立消费者都能**独立消费所有消息**
- 不同消费者之间完全独立，不共享偏移量
- 每个消费者都有自己的消费进度
- 适合事件通知、数据同步、实时监控场景

**自动UUID生成：**
- 使用UUID v4生成唯一标识符
- 确保不会产生ID冲突
- 每个消费者实例都有唯一身份

**示例：**
```rust
// 多个独立消费者都能收到所有消息
let logger = mq.subscriber("events".to_string()).await?;  // 自动生成UUID
let monitor = mq.subscriber("events".to_string()).await?; // 自动生成不同的UUID

// logger和monitor都会收到相同的事件消息
```

## 不同消费者ID的行为

**不同 consumer_id 的消费者也是独立消费模式：**

```rust
// 不同ID的消费者，各自独立消费所有消息
let consumer_a = mq.subscriber_group("events".to_string(), "service_a".to_string(), mode).await?;
let consumer_b = mq.subscriber_group("events".to_string(), "service_b".to_string(), mode).await?;

// service_a 和 service_b 都会收到完整的消息流
```

## 消费模式对比

| 消费者类型 | 相同ID消费者 | 不同ID消费者 | 偏移量管理 | UUID生成 | 适用场景 |
|-----------|-------------|-------------|-----------|----------|----------|
| 公平竞争消费者 | 公平竞争 | 独立消费 | 共享 | 手动指定 | 工作队列、负载均衡 |
| 独立消费者 | - | 独立消费 | 独立 | 自动生成 | 事件通知、数据同步 |

## 实际使用示例

### 工作队列（公平竞争消费）
```rust
// 多个worker处理任务队列，任务会公平分配，避免某个worker过载
for i in 1..=3 {
    let worker = mq.subscriber_group("tasks".to_string(), "task_workers".to_string(), ConsumptionMode::Earliest).await?;
    // 启动worker处理任务，确保负载均衡
}
```

### 事件分发（独立消费）
```rust
// 多个服务都监听系统事件，每个服务独立处理所有事件
let email_service = mq.subscriber_group("events".to_string(), "email_service".to_string(), mode).await?;
let notification_service = mq.subscriber_group("events".to_string(), "notification_service".to_string(), mode).await?;
let audit_service = mq.subscriber_group("events".to_string(), "audit_service".to_string(), mode).await?;

// 所有服务都会收到完整的事件流并独立处理
```

### 日志收集（自动UUID）
```rust
// 多个日志收集器都收集所有日志，无需手动管理ID
let file_logger = mq.subscriber("logs".to_string()).await?;      // 自动生成UUID
let network_logger = mq.subscriber("logs".to_string()).await?;   // 自动生成不同UUID
let alert_system = mq.subscriber("logs".to_string()).await?;    // 自动生成不同UUID

// 所有收集器都会收到相同的日志消息，完全独立
```

### 混合使用场景
```rust
// 工作队列 - 公平竞争处理
let task_worker = mq.subscriber_group("tasks".to_string(), "workers".to_string(), mode).await?;

// 监控系统 - 独立监控所有任务
let monitor = mq.subscriber("tasks".to_string()).await?; // 自动UUID

// 审计系统 - 独立记录所有任务
let auditor = mq.subscriber_group("tasks".to_string(), "audit_service".to_string(), mode).await?;

// 一个任务被worker处理，同时被monitor监控，被auditor记录
```

## 注意事项

1. **性能考虑**：独立消费模式会创建更多的消费状态，内存使用量会随着消费者数量增加
2. **公平调度延迟**：公平竞争消费会有0-5ms的小延迟以确保负载均衡
3. **消费进度**：公平竞争消费模式下要确保消费者组内的所有消费者工作正常，否则可能影响整体消费进度
4. **消息顺序**：单个消费者内部保证消息顺序，但多个消费者之间的相对顺序不保证
5. **UUID管理**：自动生成的UUID是唯一的，但建议在有业务含义的场景下使用有意义的消费者ID