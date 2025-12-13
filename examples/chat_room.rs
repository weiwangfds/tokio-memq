use tokio_memq::mq::{
    MessageQueue, 
    TopicOptions, 
    MessageSubscriber
};
use serde::{Serialize, Deserialize};
use log::info;
use tokio::time::Duration;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChatMessage {
    user: String,
    text: String,
    room: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    info!("Starting Chat Room Demo");

    let mq = MessageQueue::new();
    let topic = "chat_room_general".to_string();

    // Configure topic with short TTL for chat messages
    let options = TopicOptions {
        message_ttl: Some(Duration::from_secs(60)),
        max_messages: Some(1000),
        ..Default::default()
    };
    mq.create_topic(topic.clone(), options).await?;

    // --- Simulating Users ---

    // User A (Publisher)
    let pub_a = mq.publisher(topic.clone());
    let user_a_task = tokio::spawn(async move {
        for i in 1..=3 {
            let msg = ChatMessage {
                user: "Alice".into(),
                text: format!("Hi everyone, message {}", i),
                room: "general".into(),
            };
            pub_a.publish(&msg).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // User B (Publisher)
    let pub_b = mq.publisher(topic.clone());
    let user_b_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await; // Offset slightly
        for i in 1..=3 {
            let msg = ChatMessage {
                user: "Bob".into(),
                text: format!("Hello Alice, reply {}", i),
                room: "general".into(),
            };
            pub_b.publish(&msg).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // User C (Subscriber - Passive Observer)
    let sub_c = mq.subscriber(topic.clone()).await?;
    let user_c_task = tokio::spawn(async move {
        info!("User C (Charlie) joined the chat.");
        // Listen for 6 messages
        for _ in 0..6 {
            let msg = sub_c.recv().await.unwrap();
            let chat_msg: ChatMessage = msg.deserialize().unwrap();
            info!("[Charlie's Screen] {}: {}", chat_msg.user, chat_msg.text);
        }
        info!("User C left.");
    });

    // User D (Subscriber - Filtered, only listens to Alice)
    let sub_d = mq.subscriber(topic.clone()).await?;
    let user_d_task = tokio::spawn(async move {
        info!("User D (Dave) joined, listening only to Alice.");
        
        // Use recv_filter to only get messages from Alice
        // Note: recv_filter will discard messages that don't match for this specific call, 
        // but since we want to process a stream, we might loop. 
        // However, recv_filter is a single-shot "get next matching".
        
        for _ in 0..3 {
            let msg = sub_d.recv_filter(|m| {
                // We need to peek at content without consuming if possible, or just deserialize.
                // Since we are inside the filter, we have the TopicMessage.
                // For performance, we might check a header or metadata, but here we deserialize.
                // CAUTION: Deserializing in filter might be expensive.
                if let Ok(chat) = m.deserialize::<ChatMessage>() {
                    chat.user == "Alice"
                } else {
                    false
                }
            }).await.unwrap();
            
            let chat_msg: ChatMessage = msg.deserialize().unwrap();
            info!("[Dave's Screen - Alice Only] {}", chat_msg.text);
        }
    });

    // Wait for everyone
    let _ = tokio::join!(user_a_task, user_b_task, user_c_task, user_d_task);

    info!("Chat Room Demo Finished");
    Ok(())
}
