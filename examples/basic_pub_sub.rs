use tokio_memq::mq::MessageQueue;
use tokio_memq::{AsyncMessagePublisher, MessageSubscriber};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    env_logger::init();

    // Create a new message queue
    let mq = MessageQueue::new();
    let topic = "basic_topic";

    println!("Creating subscriber for topic: {}", topic);
    // Create a subscriber
    let sub = mq.subscriber(topic.to_string()).await?;

    // Create a publisher
    let pub1 = mq.publisher(topic.to_string());

    // Spawn a task to publish messages
    tokio::spawn(async move {
        for i in 0..5 {
            let msg = format!("Message {}", i);
            println!("Publishing: {}", msg);
            pub1.publish(msg).await.unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    });

    // Receive messages
    println!("Waiting for messages...");
    for _ in 0..5 {
        let msg = sub.recv().await?;
        let payload: String = msg.deserialize()?;
        println!("Received: {} (Offset: {:?}, Topic: {})", payload, msg.offset, msg.topic);
    }

    Ok(())
}
