use tokio_memq::mq::{
    MessageQueue, 
    SerializationFormat, 
    SerializationFactory, 
    SerializationConfig, 
    JsonConfig, 
    PipelineConfig, 
    CompressionConfig,
    TopicMessage,
    MessageSubscriber,
    ConsumptionMode
};
use serde::{Serialize, Deserialize};
// use std::time::Duration; // Remove unused import

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TestData {
    id: u32,
    content: String,
}

#[tokio::test]
async fn test_serialization_pipeline_integration() -> anyhow::Result<()> {
    // Initialize MQ
    let mq = MessageQueue::new();
    let topic = "pipeline_test".to_string();

    // 1. Configure Topic Defaults: JSON format, Gzip compression, Magic Header enabled
    let pipeline = PipelineConfig {
        compression: CompressionConfig::Gzip { level: Some(6) },
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

    // Create Subscriber FIRST to catch all messages
    let sub = mq.subscriber(topic.clone()).await?;

    // 2. Publisher 1: Uses Topic Defaults
    let pub1 = mq.publisher(topic.clone());
    let data1 = TestData { id: 1, content: "Compressed JSON with Magic Header".to_string() };
    pub1.publish(&data1).await?;

    // 3. Publisher 2: Uses Publisher Key override (Zstd compression, MessagePack)
    let pub2_key = "special_pub";
    let pipeline2 = PipelineConfig {
        compression: CompressionConfig::Zstd { level: 3 },
        pre: None,
        post: None,
        use_magic_header: true,
    };
    
    SerializationFactory::register_publisher_defaults(
        pub2_key,
        SerializationFormat::MessagePack,
        SerializationConfig::Default,
        Some(pipeline2),
    );

    let pub2 = mq.publisher_with_key(topic.clone(), pub2_key.to_string());
    let data2 = TestData { id: 2, content: "Compressed MsgPack with Magic Header".to_string() };
    pub2.publish(&data2).await?;

    // 4. Subscriber: Should handle both formats automatically via magic header detection
    
    // Receive message 1
    let msg1 = sub.recv().await?;
    // Verify it's JSON format (as detected/used)
    assert_eq!(msg1.format, SerializationFormat::Json);
    let received1: TestData = msg1.deserialize()?;
    assert_eq!(received1, data1);
    
    // Check raw bytes for Magic Header "TMQ"
    let raw1 = msg1.payload_bytes();
    assert_eq!(&raw1[0..3], b"TMQ");

    // Receive message 2
    let msg2 = sub.recv().await?;
    // Verify it's MessagePack format
    assert_eq!(msg2.format, SerializationFormat::MessagePack);
    let received2: TestData = msg2.deserialize()?;
    assert_eq!(received2, data2);

    // Check raw bytes for Magic Header "TMQ"
    let raw2 = msg2.payload_bytes();
    assert_eq!(&raw2[0..3], b"TMQ");

    Ok(())
}

#[tokio::test]
async fn test_auto_detect_format_without_magic() -> anyhow::Result<()> {
    let mq = MessageQueue::new();
    let topic = "auto_detect_test".to_string();
    
    // Create Subscriber FIRST
    let sub = mq.subscriber(topic.clone()).await?;
    
    // Create a raw JSON string
    let json_str = r#"{"id": 100, "content": "Raw JSON"}"#;
    let pub1 = mq.publisher(topic.clone());
    
    // Publish as pre-serialized string
    pub1.publish_serialized(json_str.to_string()).await?;

    let msg = sub.recv().await?;

    // Verify format detection (should be JSON because it starts with '{')
    assert_eq!(msg.format, SerializationFormat::Json);
    
    let received: TestData = msg.deserialize()?;
    assert_eq!(received.id, 100);
    assert_eq!(received.content, "Raw JSON");

    Ok(())
}
