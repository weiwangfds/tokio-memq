use tokio_memq::mq::{
    MessageQueue, 
    SerializationFactory, 
    SerializationFormat, 
    SerializationConfig, 
    JsonConfig, 
    MessagePackConfig, 
    PipelineConfig, 
    CompressionConfig,
    MessageSubscriber
};
use serde::{Serialize, Deserialize};
use log::info;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct DemoData {
    id: u32,
    name: String,
    data: Vec<u8>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    info!("Starting Serialization Demo");

    let mq = MessageQueue::new();
    let topic_json = "demo_json".to_string();
    let topic_msgpack = "demo_msgpack".to_string();
    let topic_pipeline = "demo_pipeline".to_string();

    // 1. JSON (Pretty) Configuration
    info!("--- Testing JSON (Pretty) ---");
    SerializationFactory::register_topic_defaults(
        &topic_json,
        SerializationFormat::Json,
        SerializationConfig::Json(JsonConfig { pretty: true }),
        None,
    );

    let pub_json = mq.publisher(topic_json.clone());
    let sub_json = mq.subscriber(topic_json.clone()).await?;

    let data = DemoData { id: 1, name: "JSON Test".into(), data: vec![1, 2, 3] };
    pub_json.publish(&data).await?;
    
    let msg = sub_json.recv().await?;
    info!("Received JSON payload: {}", msg.payload()); // Should be pretty printed
    let received: DemoData = msg.deserialize()?;
    assert_eq!(data, received);


    // 2. MessagePack Configuration
    info!("--- Testing MessagePack ---");
    SerializationFactory::register_topic_defaults(
        &topic_msgpack,
        SerializationFormat::MessagePack,
        SerializationConfig::MessagePack(MessagePackConfig { struct_map: true }),
        None,
    );

    let pub_msgpack = mq.publisher(topic_msgpack.clone());
    let sub_msgpack = mq.subscriber(topic_msgpack.clone()).await?;

    let data_mp = DemoData { id: 2, name: "MsgPack Test".into(), data: vec![4, 5, 6] };
    pub_msgpack.publish(&data_mp).await?;

    let msg = sub_msgpack.recv().await?;
    // MsgPack payload is binary, so payload() string representation might be just a summary or placeholder
    info!("Received MsgPack payload summary: {}", msg.display_payload(50));
    let received_mp: DemoData = msg.deserialize()?;
    assert_eq!(data_mp, received_mp);


    // 3. Compression Pipeline & Magic Header
    info!("--- Testing Compression Pipeline (Gzip + Magic Header) ---");
    let pipeline = PipelineConfig {
        compression: CompressionConfig::Gzip { level: Some(6) },
        pre: None,
        post: None,
        use_magic_header: true,
    };

    SerializationFactory::register_topic_defaults(
        &topic_pipeline,
        SerializationFormat::Json, // Base format is JSON, but compressed
        SerializationConfig::Default,
        Some(pipeline),
    );

    let pub_pipe = mq.publisher(topic_pipeline.clone());
    let sub_pipe = mq.subscriber(topic_pipeline.clone()).await?;

    let data_pipe = DemoData { id: 3, name: "Pipeline Test".into(), data: vec![0; 1000] }; // Large data to see compression
    pub_pipe.publish(&data_pipe).await?;

    let msg = sub_pipe.recv().await?;
    info!("Received Compressed Message. Original Size (approx): {}, Payload Size: {}", 
          serde_json::to_string(&data_pipe)?.len(), 
          msg.payload_bytes().len());
    
    // Auto-detection should work because of Magic Header
    let received_pipe: DemoData = msg.deserialize()?;
    assert_eq!(data_pipe, received_pipe);

    info!("Serialization Demo Completed Successfully");
    Ok(())
}
