use tokio_memq::mq::MessageQueue;
use tokio_memq::mq::serializer::SerializationFormat;
use tokio_memq::MessageSubscriber;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mq = MessageQueue::new();
    let topic = "bench_topic";
    let publisher = mq.publisher(topic.to_string());
    let subscriber = mq.subscriber(topic.to_string()).await?;

    // Enable metrics
    let stats = mq.get_topic_stats(topic.to_string()).await.unwrap();

    let payload_size = 1024; // 1KB
    let msg_count = 1_000_000;
    
    let data = vec![0u8; payload_size];
    let shared_data = Arc::new(data);
    let shared_topic = Arc::new(topic.to_string());

    println!("Starting zero-copy benchmark with {} messages, {} bytes each...", msg_count, payload_size);
    
    // Spawn consumer
    let consumer_handle = tokio::spawn(async move {
        let mut count = 0;
        let start = Instant::now();
        while count < msg_count {
            if let Ok(_msg) = subscriber.recv().await {
                count += 1;
            }
        }
        start.elapsed()
    });

    let start = Instant::now();
    
    // Publish using zero-copy API
    for _ in 0..msg_count {
        publisher.publish_shared_data(
            shared_topic.clone(), 
            shared_data.clone(), 
            SerializationFormat::Native // or Binary, but we use raw bytes mostly
        ).await?;
    }

    let publish_time = start.elapsed();
    let consume_time = consumer_handle.await?;

    println!("Published {} messages in {:?}", msg_count, publish_time);
    println!("Consumed {} messages in {:?}", msg_count, consume_time);
    
    // Check memory usage (approximate payload size in queue - should be low if consumed fast, 
    // or if we checked during accumulation. But we want to verify zero-copy effectively means 
    // we don't duplicate data in RAM if we hold references).
    // Note: Queue depth might be 0 now.
    
    println!("Final Topic Stats: {:?}", stats);
    println!("Total payload size tracked: {}", stats.total_payload_size);

    Ok(())
}
