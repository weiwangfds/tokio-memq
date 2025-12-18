use tokio_memq::mq::MessageQueue;
use tokio_memq::{AsyncMessagePublisher, MessageSubscriber};
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mq = MessageQueue::new();
    let topic = "latency_bench";
    let sub = mq.subscriber(topic.to_string()).await?;
    let pub1 = mq.publisher(topic.to_string());

    let iterations = 10_000;
    let payload = "small payload";

    println!("Starting latency benchmark with {} iterations...", iterations);
    println!("Round-trip latency (Publish -> Receive)");

    let start = Instant::now();
    for _ in 0..iterations {
        pub1.publish(payload).await?;
        let _ = sub.recv().await?;
    }
    let duration = start.elapsed();

    println!("Total time: {:?}", duration);
    println!("Average latency: {:?}", duration / iterations as u32);
    println!("Ops/sec: {:.2}", iterations as f64 / duration.as_secs_f64());

    Ok(())
}
