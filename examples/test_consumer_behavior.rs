use tokio_memq::mq::{MessageQueue, ConsumptionMode};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    println!("=== 测试消费者行为：竞争消费 vs 广播消费 ===\n");

    let mq = MessageQueue::new();
    let topic = "test_consumer_behavior".to_string();

    // 发布一些消息
    let publisher = mq.publisher(topic.clone());
    for i in 1..=6 {
        publisher.publish(&format!("Message {}", i)).await?;
        println!("发布: Message {}", i);
        sleep(Duration::from_millis(100)).await;
    }

    println!("\n--- 测试1: 相同消费者ID的竞争消费 ---");

    // 创建两个相同ID的消费者
    let consumer1_a = mq.subscriber_group(topic.clone(), "group_a".to_string(), ConsumptionMode::Earliest).await?;
    let consumer1_b = mq.subscriber_group(topic.clone(), "group_a".to_string(), ConsumptionMode::Earliest).await?;

    // 消费者1_a 接收消息
    println!("消费者1_A 接收消息:");
    for _ in 0..3 {
        match consumer1_a.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  1_A收到: {}", msg.payload_str()),
            None => println!("  1_A超时"),
        }
        sleep(Duration::from_millis(50)).await;
    }

    // 消费者1_b 接收消息（应该从consumer1_a停止的地方继续）
    println!("消费者1_B 接收消息:");
    for _ in 0..3 {
        match consumer1_b.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  1_B收到: {}", msg.payload_str()),
            None => println!("  1_B超时"),
        }
        sleep(Duration::from_millis(50)).await;
    }

    println!("\n--- 测试2: 不同消费者ID的广播消费 ---");

    // 创建两个不同ID的消费者
    let consumer2_a = mq.subscriber_group(topic.clone(), "group_a".to_string(), ConsumptionMode::Earliest).await?;
    let consumer2_b = mq.subscriber_group(topic.clone(), "group_b".to_string(), ConsumptionMode::Earliest).await?;

    // 消费者2_a 接收前3条消息
    println!("消费者2_A 接收前3条消息:");
    for _ in 0..3 {
        match consumer2_a.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  2_A收到: {}", msg.payload_str()),
            None => println!("  2_A超时"),
        }
        sleep(Duration::from_millis(50)).await;
    }

    // 消费者2_b 接收消息（应该从头开始，能看到所有消息）
    println!("消费者2_B 从头开始接收消息:");
    for _ in 0..3 {
        match consumer2_b.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  2_B收到: {}", msg.payload_str()),
            None => println!("  2_B超时"),
        }
        sleep(Duration::from_millis(50)).await;
    }

    println!("\n--- 测试3: 广播消费者（无消费者ID）---");

    // 创建两个广播消费者（没有消费者ID）
    let consumer3_a = mq.subscriber_with_options_and_mode(
        topic.clone(),
        tokio_memq::mq::TopicOptions::default(),
        ConsumptionMode::Earliest
    ).await?;
    let consumer3_b = mq.subscriber_with_options_and_mode(
        topic.clone(),
        tokio_memq::mq::TopicOptions::default(),
        ConsumptionMode::Earliest
    ).await?;

    // 发布一些新消息
    for i in 7..=9 {
        publisher.publish(&format!("New Message {}", i)).await?;
        println!("发布新消息: New Message {}", i);
        sleep(Duration::from_millis(100)).await;
    }

    // 两个广播消费者都应该能看到所有消息
    println!("广播消费者3_A 接收3条新消息:");
    for _ in 0..3 {
        match consumer3_a.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  3_A收到: {}", msg.payload_str()),
            None => println!("  3_A超时"),
        }
        sleep(Duration::from_millis(50)).await;
    }

    println!("广播消费者3_B 接收3条新消息:");
    for _ in 0..3 {
        match consumer3_b.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  3_B收到: {}", msg.payload_str()),
            None => println!("  3_B超时"),
        }
        sleep(Duration::from_millis(50)).await;
    }

    println!("\n=== 测试完成 ===");
    Ok(())
}