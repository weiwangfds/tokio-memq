use tokio_memq::mq::MessageQueue;
use tokio_memq::{AsyncMessagePublisher, MessageSubscriber};
use tokio_memq::{SerializationFactory, SerializationConfig, JsonConfig, SerializationFormat};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct User {
    id: u32,
    name: String,
    email: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mq = MessageQueue::new();
    let topic = "json_topic";

    // Configure JSON serialization for this topic
    // This tells the publisher to use JSON format by default for this topic
    SerializationFactory::register_topic_defaults(
        topic,
        SerializationFormat::Json,
        SerializationConfig::Json(JsonConfig { pretty: true }),
        None,
    );

    let pub1 = mq.publisher(topic.to_string());
    let sub = mq.subscriber(topic.to_string()).await?;

    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    println!("Publishing user (as JSON): {:?}", user);
    pub1.publish(user).await?;

    println!("Waiting for message...");
    let msg = sub.recv().await?;
    
    // Check format
    println!("Received message format: {:?}", msg.serialization_format());
    
    // Deserialize
    let received_user: User = msg.deserialize()?;
    println!("Received user: {:?}", received_user);

    Ok(())
}
