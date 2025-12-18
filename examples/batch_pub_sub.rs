use tokio_memq::mq::MessageQueue;
use tokio_memq::{AsyncMessagePublisher, MessageSubscriber};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mq = MessageQueue::new();
    let topic = "batch_topic";

    println!("Creating subscriber for topic: {}", topic);
    let sub = mq.subscriber(topic.to_string()).await?;
    let pub1 = mq.publisher(topic.to_string());

    // Batch publish
    let batch_size = 10;
    println!("Batch publishing {} messages...", batch_size);
    let messages: Vec<String> = (0..batch_size).map(|i| format!("Batch Msg {}", i)).collect();
    
    // Publish the batch
    pub1.publish_batch(messages).await?;
    println!("Batch published.");

    // Batch receive
    println!("Waiting to receive batch...");
    // recv_batch_typed automatically deserializes the messages
    let received_batch: Vec<String> = sub.recv_batch_typed(batch_size).await?;
    
    println!("Received batch of {} messages:", received_batch.len());
    for msg in received_batch {
        println!(" - {}", msg);
    }

    Ok(())
}
