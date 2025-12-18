use tokio_memq::mq::MessageQueue;
use tokio_memq::{AsyncMessagePublisher, MessageSubscriber};
use tokio_stream::StreamExt;
use tokio::pin;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mq = MessageQueue::new();
    let topic = "stream_topic";

    let sub = mq.subscriber(topic.to_string()).await?;
    let pub1 = mq.publisher(topic.to_string());

    tokio::spawn(async move {
        for i in 0..5 {
            let msg = format!("Stream Item {}", i);
            pub1.publish(msg).await.unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    });

    let stream = sub.stream();
    pin!(stream);

    println!("Processing stream...");
    while let Some(msg_res) = stream.next().await {
        match msg_res {
            Ok(msg) => {
                let payload: String = msg.deserialize()?;
                println!("Stream received: {}", payload);
                if payload == "Stream Item 4" {
                    println!("Received last item, stopping stream processing.");
                    break;
                }
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    Ok(())
}
