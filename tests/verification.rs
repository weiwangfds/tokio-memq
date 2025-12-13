use tokio_memq::mq::{MessageQueue, TopicOptions, ConsumptionMode, MessageSubscriber};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_offset_unification_and_commit_seek() -> anyhow::Result<()> {
    // Setup
    let mq = MessageQueue::new();
    let topic = "test_offset_seek".to_string();
    let publisher = mq.publisher(topic.clone());
    
    // Publish messages
    for i in 0..10 {
        publisher.publish(&format!("msg-{}", i)).await?;
    }

    // Subscribe with group (log-based)
    let group_id = "group1".to_string();
    let subscriber = mq.subscriber_group(topic.clone(), group_id.clone(), ConsumptionMode::Earliest).await?;
    
    // Verify first message offset
    let msg1 = subscriber.recv().await?;
    assert_eq!(msg1.offset, Some(0));
    assert_eq!(msg1.deserialize::<String>()?, "msg-0");

    // Test Seek
    subscriber.seek(5).await?;
    
    // Verify next message is offset 5
    let msg2 = subscriber.recv().await?;
    assert_eq!(msg2.offset, Some(5));
    assert_eq!(msg2.deserialize::<String>()?, "msg-5");

    // Test Commit (advance offset manually)
    // Current offset should be 6 (after consuming 5)
    // Let's commit 8 (skip 6, 7)
    subscriber.commit(8).await?;
    
    let msg3 = subscriber.recv().await?;
    assert_eq!(msg3.offset, Some(8));
    assert_eq!(msg3.deserialize::<String>()?, "msg-8");

    Ok(())
}

#[tokio::test]
async fn test_backpressure_and_metrics() -> anyhow::Result<()> {
    // Setup with small buffer
    let mq = MessageQueue::new();
    let topic = "test_metrics".to_string();
    let options = TopicOptions {
        max_messages: Some(5), // Small buffer
        message_ttl: None,
        lru_enabled: true, // Should drop old messages
        ..Default::default()
    };
    
    mq.create_topic(topic.clone(), options).await?;
    let publisher = mq.publisher(topic.clone());
    
    // Publish more messages than capacity
    for i in 0..10 {
        publisher.publish(&format!("msg-{}", i)).await?;
    }
    
    // Wait a bit for async operations
    sleep(Duration::from_millis(50)).await;
    
    // Consuming from Earliest should give us messages starting from offset 5 (0-4 dropped)
    // Use a group to specify offset 0
    let subscriber_group = mq.subscriber_group(topic.clone(), "monitor".to_string(), ConsumptionMode::Offset(0)).await?;
    
    // The first message available should be offset 5
    // Because 0,1,2,3,4 were dropped.
    // subscriber.rs fetch_from_buffer should return front if target < front
    
    let msg = subscriber_group.recv().await?;
    assert_eq!(msg.offset, Some(5));
    assert_eq!(msg.deserialize::<String>()?, "msg-5");
    
    Ok(())
}
