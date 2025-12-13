use tokio_memq::mq::MessageQueue;
use tokio_stream::StreamExt; // Need this for .next()

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logger to see internal logs
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    
    let mq = MessageQueue::new();
    let topic = "stream-demo".to_string();
    
    // Create publisher
    let publisher = mq.publisher(topic.clone());
    
    // Create subscriber
    let subscriber = mq.subscriber(topic).await?;
    
    // Spawn a task to publish messages periodically
    tokio::spawn(async move {
        for i in 0..5 {
            let msg = format!("Stream Message {}", i);
            log::info!("Publishing: {}", msg);
            publisher.publish(&msg).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        log::info!("Finished publishing");
    });
    
    log::info!("Starting stream consumption...");
    
    // 1. Get the stream
    let stream = subscriber.stream();
    
    // 2. Pin it (required because the stream is opaque `impl Stream`)
    tokio::pin!(stream);
    
    // 3. Iterate using StreamExt methods
    while let Some(msg_result) = stream.next().await {
        let msg = msg_result?;
        let content: String = msg.deserialize()?;
        log::info!("Consumed via Stream: {}", content);
        
        if content == "Stream Message 4" {
            log::info!("Received last message, exiting loop");
            break;
        }
    }
    
    Ok(())
}
