use tokio_memq::mq::{MessageQueue, ConsumptionMode};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    println!("=== 简化版消费者行为测试 ===\n");

    let mq = MessageQueue::new();
    let topic = "simple_test".to_string();

    // 发布3条消息
    let publisher = mq.publisher(topic.clone());
    for i in 1..=3 {
        publisher.publish(&format!("Message {}", i)).await?;
        println!("发布: Message {}", i);
        sleep(Duration::from_millis(100)).await;
    }

    println!("\n--- 测试1: 相同ID的消费者竞争消费 ---");

    // 两个相同ID的消费者
    let consumer_a1 = mq.subscriber_group(topic.clone(), "group1".to_string(), ConsumptionMode::Earliest).await?;
    let consumer_a2 = mq.subscriber_group(topic.clone(), "group1".to_string(), ConsumptionMode::Earliest).await?;

    // 消费者a1接收1条消息
    match consumer_a1.recv_timeout(Duration::from_secs(1)).await? {
        Some(msg) => println!("消费者a1收到: {}", msg.payload_str()),
        None => println!("消费者a1超时"),
    }

    // 消费者a2接收1条消息（应该是下一条）
    match consumer_a2.recv_timeout(Duration::from_secs(1)).await? {
        Some(msg) => println!("消费者a2收到: {}", msg.payload_str()),
        None => println!("消费者a2超时"),
    }

    println!("\n--- 测试2: 不同ID的消费者独立消费 ---");

    // 两个不同ID的消费者
    let consumer_b1 = mq.subscriber_group(topic.clone(), "group2".to_string(), ConsumptionMode::Earliest).await?;
    let consumer_b2 = mq.subscriber_group(topic.clone(), "group3".to_string(), ConsumptionMode::Earliest).await?;

    // 消费者b1接收1条消息
    match consumer_b1.recv_timeout(Duration::from_secs(1)).await? {
        Some(msg) => println!("消费者b1收到: {}", msg.payload_str()),
        None => println!("消费者b1超时"),
    }

    // 消费者b2接收1条消息（也应该是第一条）
    match consumer_b2.recv_timeout(Duration::from_secs(1)).await? {
        Some(msg) => println!("消费者b2收到: {}", msg.payload_str()),
        None => println!("消费者b2超时"),
    }

    println!("\n--- 测试3: 广播消费者（无ID） ---");

    // 发布新消息
    publisher.publish(&format!("Message 4")).await?;
    println!("发布: Message 4");
    sleep(Duration::from_millis(100)).await;

    // 两个无ID的消费者
    let consumer_c1 = mq.subscriber_with_options_and_mode(
        topic.clone(),
        tokio_memq::mq::TopicOptions::default(),
        ConsumptionMode::Latest
    ).await?;
    let consumer_c2 = mq.subscriber_with_options_and_mode(
        topic.clone(),
        tokio_memq::mq::TopicOptions::default(),
        ConsumptionMode::Latest
    ).await?;

    // 再发布2条消息
    for i in 5..=6 {
        publisher.publish(&format!("Message {}", i)).await?;
        println!("发布: Message {}", i);
        sleep(Duration::from_millis(100)).await;
    }

    // 两个广播消费者都应该能收到相同的新消息
    println!("广播消费者c1接收:");
    for _ in 0..2 {
        match consumer_c1.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  c1收到: {}", msg.payload_str()),
            None => println!("  c1超时"),
        }
    }

    println!("广播消费者c2接收:");
    for _ in 0..2 {
        match consumer_c2.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  c2收到: {}", msg.payload_str()),
            None => println!("  c2超时"),
        }
    }

    println!("\n=== 测试完成 ===");
    Ok(())
}