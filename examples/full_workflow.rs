use tokio_memq::mq::{
    MessageQueue, 
    TopicOptions, 
    ConsumptionMode, 
    SerializationFactory, 
    SerializationFormat, 
    SerializationConfig, 
    JsonConfig, 
    PipelineConfig, 
    CompressionConfig,
    MessageSubscriber // Import trait
};
use serde::{Serialize, Deserialize};
use std::time::Duration;
use log::info;
use tokio::pin; // Import pin

#[derive(Debug, Serialize, Deserialize)]
struct UserEvent {
    user_id: u32,
    action: String,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    info!("Starting Full Workflow Demo");

    let mq = MessageQueue::new();
    let topic = "user_events".to_string();

    // 1. Setup Topic with Options (TTL 5s, Max 100 messages)
    let options = TopicOptions {
        max_messages: Some(100),
        message_ttl: Some(Duration::from_secs(5)),
        lru_enabled: true,
        ..Default::default()
    };
    mq.create_topic(topic.clone(), options).await?;

    // 2. Setup Serializer Pipeline (JSON + Gzip + Magic Header)
    let pipeline = PipelineConfig {
        compression: CompressionConfig::Gzip { level: Some(1) }, // Fast compression
        pre: None,
        post: None,
        use_magic_header: true,
    };
    SerializationFactory::register_topic_defaults(
        &topic,
        SerializationFormat::Json,
        SerializationConfig::Json(JsonConfig { pretty: false }),
        Some(pipeline),
    );

    // 3. Create Publisher and Publish Batch
    let publisher = mq.publisher(topic.clone());
    let mut events = Vec::new();
    for i in 0..10 {
        events.push(UserEvent {
            user_id: i,
            action: format!("login_{}", i),
            timestamp: 1000 + i as u64,
        });
    }
    
    info!("Publishing batch of 10 events...");
    publisher.publish_batch(events).await?;

    // 4. Create Consumer Group Subscriber
    let group_id = "analytics_service".to_string();
    let subscriber = mq.subscriber_group(topic.clone(), group_id.clone(), ConsumptionMode::Earliest).await?;

    // 5. Consume with Stream API (Consume first 5)
    info!("Consuming first 5 events via Stream API...");
    use tokio_stream::StreamExt;
    let stream = subscriber.stream().take(5);
    pin!(stream);
    
    while let Some(msg_result) = stream.next().await {
        let msg = msg_result?;
        let event: UserEvent = msg.deserialize()?;
        info!("Received Event: {:?}", event);
    }

    // 6. Manual Commit (Commit offset 5)
    // Note: Stream consumes messages but doesn't auto-commit unless we implement auto-commit logic.
    // Here we manually commit. The last received message had offset 4 (since we took 5 starting from 0).
    // So we commit 5 (next expected offset).
    info!("Committing offset 5...");
    subscriber.commit(5).await?;

    // 7. Check Stats
    if let Some(stats) = mq.get_topic_stats(topic.clone()).await {
        info!("Topic Stats: Message Count: {}, Subscribers: {}, Consumer Lags: {:?}", 
              stats.message_count, stats.subscriber_count, stats.consumer_lags);
    }

    // 8. Publish more messages
    info!("Publishing one more event...");
    publisher.publish(&UserEvent { user_id: 99, action: "logout".to_string(), timestamp: 2000 }).await?;

    // 9. Seek to Latest and Receive
    info!("Seeking to Latest...");
    // Latest means we should see new messages only.
    // Current buffer has offsets 0..9 and 10.
    // Seek to 11? Or seek to latest available?
    // Let's use seek(10) to read the message we just sent.
    subscriber.seek(10).await?;
    let msg = subscriber.recv().await?;
    let event: UserEvent = msg.deserialize()?;
    info!("Received Event after seek: {:?}", event);

    // 10. Delete Topic
    info!("Deleting topic...");
    let deleted = mq.delete_topic(&topic).await;
    info!("Topic deleted: {}", deleted);

    // Verify deletion
    let stats = mq.get_topic_stats(topic.clone()).await;
    assert!(stats.is_none());
    info!("Topic stats is None as expected.");

    Ok(())
}
