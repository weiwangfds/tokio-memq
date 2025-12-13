use tokio_memq::mq::{
    MessageQueue, 
    TopicOptions, 
    MessageSubscriber,
    SerializationFactory,
    SerializationFormat,
    SerializationConfig,
    JsonConfig
};
use tokio::time::Duration;
use log::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    info!("Starting Advanced Filtering & Timeout Demo");

    let mq = MessageQueue::new();
    let topic = "sensor_data".to_string();

    // Setup: 100 max messages
    mq.create_topic(topic.clone(), TopicOptions {
        max_messages: Some(100),
        ..Default::default()
    }).await?;

    // Use JSON so we can inspect payload string easily in filter
    SerializationFactory::register_topic_defaults(
        &topic,
        SerializationFormat::Json,
        SerializationConfig::Json(JsonConfig { pretty: false }),
        None,
    );

    let publisher = mq.publisher(topic.clone());
    let subscriber = mq.subscriber(topic.clone()).await?;

    // 1. recv_timeout Demo
    info!("--- Testing recv_timeout ---");
    
    // Attempt to receive when empty
    info!("Attempting receive on empty queue with 1s timeout...");
    let result = subscriber.recv_timeout(Duration::from_secs(1)).await?;
    if result.is_none() {
        info!("Success: Timeout occurred as expected.");
    } else {
        warn!("Unexpected: Received message on empty queue.");
    }

    // Publish and receive with timeout
    publisher.publish(&"Data 1").await?;
    let result = subscriber.recv_timeout(Duration::from_secs(1)).await?;
    if let Some(msg) = result {
        info!("Success: Received '{}' within timeout.", msg.payload());
    } else {
        warn!("Unexpected: Timed out while message was available.");
    }


    // 2. recv_filter Demo (Server-side filtering logic simulation)
    info!("--- Testing recv_filter ---");
    
    // Publish mixed data: Low Priority, High Priority
    publisher.publish(&"Low: Log 1").await?;
    publisher.publish(&"Low: Log 2").await?;
    publisher.publish(&"High: Alert 1").await?;
    publisher.publish(&"Low: Log 3").await?;

    info!("Waiting for High Priority message...");
    
    // Filter for messages containing "High:"
    let high_prio_msg = subscriber.recv_filter(|msg| {
        msg.payload().contains("High:")
    }).await?;

    info!("Received Filtered Message: {}", high_prio_msg.payload());
    
    // Verify we skipped the first two "Low" messages.
    // Note: The standard subscriber cursor advances. 
    // `recv_filter` consumes messages from the queue until it finds a match. 
    // Messages that didn't match are effectively "skipped" (consumed but returned nothing to the caller)
    // for *this* subscriber. This is typical for a "scan until match" operation.
    
    assert!(high_prio_msg.payload().contains("High: Alert 1"));


    // 3. Metadata-Only Access
    info!("--- Testing Metadata Access ---");
    publisher.publish(&"Metadata Check").await?;
    
    // We want to peek metadata without deserializing potentially large payload
    // Note: recv() gets the whole message struct (Arc reference usually or clone), 
    // but deserialization is lazy/explicit.
    let msg = subscriber.recv().await?;
    let meta = msg.metadata();
    
    info!("Message Metadata: Offset={}, Size={} bytes, CreatedAt={:?}", 
          meta.offset, meta.payload_size, meta.created_at);

    Ok(())
}
