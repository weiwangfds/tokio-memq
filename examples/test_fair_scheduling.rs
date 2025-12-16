use tokio_memq::mq::{MessageQueue, ConsumptionMode};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    println!("=== 测试公平调度和自动UUID生成 ===\n");

    let mq = MessageQueue::new();
    let topic = "fair_scheduling_test".to_string();

    // 发布一些消息
    let publisher = mq.publisher(topic.clone());
    for i in 1..=10 {
        publisher.publish(&format!("Message {}", i)).await?;
        println!("发布: Message {}", i);
        sleep(Duration::from_millis(50)).await;
    }

    println!("\n--- 测试1: 自动UUID生成 ---");

    // 创建没有指定消费者ID的订阅者，应该自动生成UUID
    let subscriber1 = mq.subscriber(topic.clone()).await?;
    let subscriber2 = mq.subscriber(topic.clone()).await?;

    println!("订阅者1 ID: {:?}", subscriber1.consumer_id);
    println!("订阅者2 ID: {:?}", subscriber2.consumer_id);

    // 两个订阅者都应该能独立消费所有消息
    println!("订阅者1接收前3条消息:");
    for _ in 0..3 {
        match subscriber1.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  订阅者1收到: {}", msg.payload_str()),
            None => println!("  订阅者1超时"),
        }
        sleep(Duration::from_millis(20)).await;
    }

    println!("订阅者2接收前3条消息:");
    for _ in 0..3 {
        match subscriber2.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  订阅者2收到: {}", msg.payload_str()),
            None => println!("  订阅者2超时"),
        }
        sleep(Duration::from_millis(20)).await;
    }

    println!("\n--- 测试2: 相同ID的公平调度 ---");

    // 发布新消息
    for i in 11..=15 {
        publisher.publish(&format!("Fair Message {}", i)).await?;
        println!("发布: Fair Message {}", i);
        sleep(Duration::from_millis(100)).await;
    }

    // 创建多个相同ID的消费者，应该公平调度
    let worker1 = mq.subscriber_group(topic.clone(), "workers".to_string(), ConsumptionMode::Earliest).await?;
    let worker2 = mq.subscriber_group(topic.clone(), "workers".to_string(), ConsumptionMode::Earliest).await?;
    let worker3 = mq.subscriber_group(topic.clone(), "workers".to_string(), ConsumptionMode::Earliest).await?;

    println!("启动3个worker消费者...");

    // 每个worker尝试获取2条消息，总共应该获取6条消息
    let mut handles = Vec::new();
    for (i, worker) in vec![worker1, worker2, worker3].into_iter().enumerate() {
        let handle = tokio::spawn(async move {
            let worker_name = format!("Worker-{}", i + 1);
            let mut received = Vec::new();

            for _ in 0..2 {
                match worker.recv_timeout(Duration::from_secs(3)).await {
                    Ok(Some(msg)) => {
                        println!("{} 收到: {}", worker_name, msg.payload_str());
                        received.push(msg.payload_str().to_string());
                    },
                    Ok(None) => println!("{} 超时", worker_name),
                    Err(e) => println!("{} 错误: {}", worker_name, e),
                }
                sleep(Duration::from_millis(50)).await;
            }

            (worker_name, received)
        });
        handles.push(handle);
    }

    // 等待所有worker完成
    for handle in handles {
        let (worker_name, messages) = handle.await?;
        println!("{} 总共收到 {} 条消息: {:?}", worker_name, messages.len(), messages);
    }

    println!("\n--- 测试3: 不同消费者ID的独立消费 ---");

    // 发布更多消息
    for i in 16..=18 {
        publisher.publish(&format!("Service Message {}", i)).await?;
        println!("发布: Service Message {}", i);
        sleep(Duration::from_millis(50)).await;
    }

    // 不同ID的服务消费者
    let service_a = mq.subscriber_group(topic.clone(), "service_a".to_string(), ConsumptionMode::Earliest).await?;
    let service_b = mq.subscriber_group(topic.clone(), "service_b".to_string(), ConsumptionMode::Earliest).await?;

    println!("服务A接收消息:");
    for _ in 0..2 {
        match service_a.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  服务A收到: {}", msg.payload_str()),
            None => println!("  服务A超时"),
        }
    }

    println!("服务B接收消息:");
    for _ in 0..2 {
        match service_b.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  服务B收到: {}", msg.payload_str()),
            None => println!("  服务B超时"),
        }
    }

    println!("\n=== 测试完成 ===");
    Ok(())
}