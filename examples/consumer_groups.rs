use tokio_memq::mq::MessageQueue;
use tokio_memq::{AsyncMessagePublisher, MessageSubscriber};
use tokio_memq::ConsumptionMode;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mq = MessageQueue::new();
    let topic = "group_topic";
    let group_id = "my_consumer_group";

    println!("Publishing messages...");
    let pub1 = mq.publisher(topic.to_string());
    for i in 0..10 {
        pub1.publish(format!("Task {}", i)).await?;
    }

    println!("Creating two subscribers in the same group: {}", group_id);
    let sub1 = mq.subscriber_group(topic.to_string(), group_id.to_string(), ConsumptionMode::Earliest).await?;
    let sub2 = mq.subscriber_group(topic.to_string(), group_id.to_string(), ConsumptionMode::Earliest).await?;

    println!("Starting consumers...");
    
    let t1 = tokio::spawn(async move {
        for _ in 0..5 {
            let msg = sub1.recv().await.unwrap();
            let payload: String = msg.deserialize().unwrap();
            println!("Consumer 1 received: {}", payload);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    let t2 = tokio::spawn(async move {
        for _ in 0..5 {
            let msg = sub2.recv().await.unwrap();
            let payload: String = msg.deserialize().unwrap();
            println!("Consumer 2 received: {}", payload);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    let _ = tokio::join!(t1, t2);
    println!("Finished.");
    
    // Demonstrate independent subscription
    println!("\nCreating an independent subscriber (broadcast)...");
    let sub3 = mq.subscriber(topic.to_string()).await?; // Default is Latest usually? Or Earliest? 
    // Wait, standard subscriber usually starts from now or earliest depending on default.
    // If I want to read from beginning, I might need to specify Earliest or use subscriber_with_options_and_mode
    // But let's see what happens.
    
    // Actually, let's verify if sub3 gets messages if we publish MORE.
    // The previous messages might be gone for sub3 if it's "Latest" mode.
    
    pub1.publish("Broadcast Message".to_string()).await?;
    
    let msg = sub3.recv().await?;
    let payload: String = msg.deserialize()?;
    println!("Independent Subscriber received: {}", payload);

    Ok(())
}
