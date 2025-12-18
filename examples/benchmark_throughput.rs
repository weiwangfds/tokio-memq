use tokio_memq::mq::MessageQueue;
use tokio_memq::{AsyncMessagePublisher, MessageSubscriber};
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mq = MessageQueue::new();
    let topic = "throughput_bench";
    let sub = mq.subscriber(topic.to_string()).await?;
    let pub1 = mq.publisher(topic.to_string());

    let iterations = 100_000;
    let payload = "throughput payload"; 

    println!("Starting throughput benchmark with {} messages...", iterations);
    
    let start = Instant::now();

    // Spawn subscriber to drain the queue as fast as possible
    let sub_handle = tokio::spawn(async move {
        let mut count = 0;
        while count < iterations {
            if sub.recv().await.is_ok() {
                count += 1;
            }
        }
        count
    });

    // Publish messages
    for _ in 0..iterations {
        pub1.publish(payload).await?;
    }

    // Wait for subscriber to finish
    let received_count = sub_handle.await?;
    let duration = start.elapsed();

    println!("Finished processing {} messages.", received_count);
    println!("Total time: {:?}", duration);
    println!("Throughput: {:.2} msgs/sec", iterations as f64 / duration.as_secs_f64());

    Ok(())
}
