use tokio_memq::mq::MessageQueue;
use tokio_memq::mq::ConsumptionMode;
use tokio::time::{sleep, Duration};

use tokio_memq::tcp::server::{TcpServer, TcpServerOptions};
use tokio_memq::tcp::client::{TcpClient, TcpClientOptions};
use tokio_memq::tcp::protocol::PayloadFormat;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mq = MessageQueue::new();
    let opts = TcpServerOptions::default();
    let server = TcpServer::new(mq.clone(), opts.clone());
    tokio::spawn(async move {
        server.start().await.unwrap();
    });
    sleep(Duration::from_millis(200)).await;

    let mut client = TcpClient::new(TcpClientOptions {
        server_addr: opts.listen_addr,
        auth_token: None,
        reconnect: true,
        heartbeat_interval: Duration::from_secs(5),
    });
    client.connect().await?;
    client.subscribe("tcp_demo_topic", None, Some(ConsumptionMode::Earliest)).await?;
    client.publish("tcp_demo_topic", b"{\"k\":\"v\"}", PayloadFormat::Json).await?;
    let (_meta, body) = client.read_message().await?;
    println!("Client received: {:?}", String::from_utf8_lossy(&body));
    Ok(())
}
