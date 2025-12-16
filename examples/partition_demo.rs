use tokio_memq::mq::{MessageQueue, TopicOptions, ConsumptionMode, PartitionRouting};
use tokio::time::{sleep, Duration};
use std::sync::Arc;
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    println!("=== Tokio MemQ 分区功能演示 ===\n");

    let mq = Arc::new(MessageQueue::new());
    let topic = "partitioned_events".to_string();

    println!("--- 创建4个分区的主题 ---");
    let options = TopicOptions {
        max_messages: Some(1000),
        partitions: Some(4),
        ..Default::default()
    };
    mq.create_partitioned_topic(topic.clone(), options, 4).await?;
    println!("✅ 创建了4分区的主题: {}", topic);

    println!("\n--- 测试1: 轮询路由策略 ---");
    mq.set_partition_routing(topic.clone(), PartitionRouting::RoundRobin).await?;

    // 发布一些消息，观察轮询分布
    for i in 1..=12 {
        let message = tokio_memq::TopicMessage::new(topic.clone(), &format!("轮询消息 {}", i))?;
        mq.publish_to_partitioned(message).await?;
        println!("发布: 轮询消息 {}", i);
        sleep(Duration::from_millis(10)).await;
    }

    // 查看各分区的消息分布
    println!("\n轮询路由后的分区分布:");
    for i in 0..4 {
        if let Some(stats) = mq.get_partition_stats(topic.clone(), i).await {
            println!("  分区 {}: {} 条消息, {} 个订阅者",
                     i, stats.message_count, stats.subscriber_count);
        }
    }

    println!("\n--- 测试2: 哈希路由策略 ---");
    mq.set_partition_routing(topic.clone(), PartitionRouting::Hash("user_id".to_string())).await?;

    // 发布带键的消息，观察哈希分布
    let users = ["alice", "bob", "charlie", "david", "eve"];
    for user in users.iter() {
        for j in 1..=3 {
            let message = tokio_memq::TopicMessage::new_with_key(
                topic.clone(),
                &format!("用户 {} 的消息 {}", user, j),
                user.to_string()
            )?;
            mq.publish_to_partitioned(message).await?;
        }
        println!("发布用户 {} 的3条消息", user);
    }

    // 查看哈希路由的分区分布
    println!("\n哈希路由后的分区分布:");
    for i in 0..4 {
        if let Some(stats) = mq.get_partition_stats(topic.clone(), i).await {
            println!("  分区 {}: {} 条消息", i, stats.message_count);
        }
    }

    println!("\n--- 测试3: 并行消费不同分区 ---");

    // 创建多个消费者，每个订阅不同分区
    let mut handles = Vec::new();
    let semaphore = Arc::new(Semaphore::new(4)); // 限制并发数

    for partition_id in 0..4 {
        let mq_clone = Arc::clone(&mq);
        let topic_clone = topic.clone();
        let sem = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            let _permit = sem.acquire().await;

            let subscriber = mq_clone.subscribe_partition(
                topic_clone.clone(),
                partition_id,
                Some(format!("partition_worker_{}", partition_id)),
                ConsumptionMode::Earliest
            ).await.unwrap();

            println!("消费者-分区{} 开始消费", partition_id);
            let mut count = 0;

            for _ in 0..5 {
                match subscriber.recv_timeout(Duration::from_secs(2)).await {
                    Ok(Some(msg)) => {
                        count += 1;
                        println!("  分区{} 收到: {}", partition_id, msg.payload_str());
                    },
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            println!("消费者-分区{} 总共收到 {} 条消息", partition_id, count);
            count
        });

        handles.push(handle);
    }

    // 等待所有消费者完成
    let mut total_received = 0;
    for handle in handles {
        let count = handle.await?;
        total_received += count;
    }

    println!("\n所有分区消费者总共收到 {} 条消息", total_received);

    println!("\n--- 测试4: 性能对比 ---");

    // 测试分区的并行处理能力
    let partitioned_topic = "performance_test".to_string();
    mq.create_partitioned_topic(partitioned_topic.clone(), TopicOptions::default(), 4).await?;
    mq.set_partition_routing(partitioned_topic.clone(), PartitionRouting::RoundRobin).await?;

    let start = std::time::Instant::now();

    // 批量发布1000条消息
    let mut messages = Vec::new();
    for i in 1..=1000 {
        let msg = tokio_memq::TopicMessage::new(partitioned_topic.clone(), &format!("性能测试消息 {}", i))?;
        messages.push(msg);
    }

    mq.publish_batch_to_partitioned(messages).await?;
    let publish_time = start.elapsed();

    println!("批量发布1000条消息到4个分区耗时: {:?}", publish_time);

    // 查看消息在各分区的分布
    let all_stats = mq.get_all_partition_stats(partitioned_topic.clone()).await;
    for stats in all_stats {
        println!("  分区{}: {} 条消息", stats.partition_id, stats.message_count);
    }

    println!("\n=== 分区功能总结 ===");
    println!("✅ 支持4种路由策略: 轮询、哈希、随机、固定");
    println!("✅ 自动负载均衡和并行处理");
    println!("✅ 每个分区独立管理，支持并发消费");
    println!("✅ 显著提高消息处理吞吐量");
    println!("✅ 保持消息有序性（单分区内）");

    Ok(())
}
