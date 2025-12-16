use tokio_memq::mq::{MessageQueue, ConsumptionMode};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    println!("=== 公平调度和UUID自动生成演示 ===\n");

    let mq = MessageQueue::new();
    let topic = "demo_topic".to_string();

    println!("--- 场景1: 自动UUID和独立消费 ---");

    // 创建两个没有指定ID的订阅者，会自动生成UUID
    let subscriber1 = mq.subscriber_with_options_and_mode(
        topic.clone(),
        tokio_memq::mq::TopicOptions::default(),
        ConsumptionMode::Earliest
    ).await?;
    let subscriber2 = mq.subscriber_with_options_and_mode(
        topic.clone(),
        tokio_memq::mq::TopicOptions::default(),
        ConsumptionMode::Earliest
    ).await?;

    println!("订阅者1 ID: {:?}", subscriber1.consumer_id);
    println!("订阅者2 ID: {:?}", subscriber2.consumer_id);

    // 发布消息
    let publisher = mq.publisher(topic.clone());
    for i in 1..=6 {
        publisher.publish(&format!("独立消息 {}", i)).await?;
        println!("发布: 独立消息 {}", i);
        sleep(Duration::from_millis(100)).await;
    }

    // 两个UUID订阅者都能独立收到所有消息
    println!("\n订阅者1接收消息:");
    for _ in 0..6 {
        match subscriber1.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  {}", msg.payload_str()),
            None => println!("  超时"),
        }
    }

    println!("\n订阅者2接收消息:");
    for _ in 0..6 {
        match subscriber2.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  {}", msg.payload_str()),
            None => println!("  超时"),
        }
    }

    println!("\n--- 场景2: 相同消费者ID的公平调度 ---");

    // 发布新消息
    for i in 1..=9 {
        publisher.publish(&format!("任务消息 {}", i)).await?;
        println!("发布: 任务消息 {}", i);
        sleep(Duration::from_millis(50)).await;
    }

    // 创建3个相同ID的Worker
    let mut workers = Vec::new();
    for i in 1..=3 {
        let worker = mq.subscriber_group(topic.clone(), "task_workers".to_string(), ConsumptionMode::Earliest).await?;
        workers.push((format!("Worker-{}", i), worker));
    }

    // 每个Worker尝试获取3条消息，总共9条消息被公平分配
    for (worker_name, worker) in &mut workers {
        let mut received = Vec::new();
        for _ in 0..3 {
            match worker.recv_timeout(Duration::from_secs(2)).await? {
                Some(msg) => {
                    let msg_text = msg.payload_str().to_string();
                    println!("{} 收到: {}", worker_name, msg_text);
                    received.push(msg_text);
                },
                None => println!("{} 超时", worker_name),
            }
        }
        println!("{} 总共收到 {} 条消息", worker_name, received.len());
    }

    println!("\n--- 场景3: 不同服务ID的独立消费 ---");

    // 发布服务消息
    for i in 1..=4 {
        publisher.publish(&format!("服务事件 {}", i)).await?;
        println!("发布: 服务事件 {}", i);
        sleep(Duration::from_millis(50)).await;
    }

    // 不同ID的服务消费者
    let email_service = mq.subscriber_group(topic.clone(), "email_service".to_string(), ConsumptionMode::Earliest).await?;
    let log_service = mq.subscriber_group(topic.clone(), "log_service".to_string(), ConsumptionMode::Earliest).await?;

    // 两个服务独立消费消息
    println!("\n邮件服务接收:");
    for _ in 0..4 {
        match email_service.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  {}", msg.payload_str()),
            None => println!("  超时"),
        }
    }

    println!("\n日志服务接收:");
    for _ in 0..4 {
        match log_service.recv_timeout(Duration::from_secs(1)).await? {
            Some(msg) => println!("  {}", msg.payload_str()),
            None => println!("  超时"),
        }
    }

    println!("\n=== 功能总结 ===");
    println!("✅ 没有消费者ID时自动生成UUID");
    println!("✅ 相同消费者ID的消费者公平调度消息");
    println!("✅ 不同消费者ID的消费者独立消费所有消息");
    println!("✅ 每个消费者都能正确跟踪自己的消费进度");

    Ok(())
}