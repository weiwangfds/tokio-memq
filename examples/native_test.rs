use tokio_memq::mq::MessageQueue;
use tokio_memq::mq::serializer::SerializationFormat;
use tokio_memq::MessageSubscriber;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let mq = MessageQueue::new();
    let topic = "native_test_topic".to_string();

    // 1. Create Subscriber
    let mut subscriber = mq.subscriber(topic.clone()).await?;

    // 2. Create Publisher (remove .await since it's not async)
    let publisher: tokio_memq::mq::publisher::Publisher = mq.publisher(topic.clone());

    // 3. Create Data
    let user = User {
        id: 12345,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    // 4. Publish (should default to Native/Zero-Copy)
    println!("Publishing message...");
    publisher.publish(user.clone()).await?;

    // 5. Receive
    println!("Receiving message...");
    let received_msg = subscriber.recv().await?;

    // 6. Verify Format
    println!("Message Format: {:?}", received_msg.format);
    assert_eq!(received_msg.format, SerializationFormat::Native, "Format should be Native");

    // 7. Verify Payload (Zero-Copy retrieval)
    let received_user: User = received_msg.deserialize()?;
    println!("Received User: {:?}", received_user);
    
    assert_eq!(user, received_user, "Data should match");

    // 8. Explicitly Native
    publisher.publish_with_format(user.clone(), SerializationFormat::Native).await?;
    let msg2 = subscriber.recv().await?;
    assert_eq!(msg2.format, SerializationFormat::Native);
    let user2: User = msg2.deserialize()?;
    assert_eq!(user, user2);

    println!("Native Zero-Copy Test Passed!");
    Ok(())
}
