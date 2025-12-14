use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use std::net::SocketAddr;
use std::time::Duration;
use super::protocol::*;

pub struct TcpClientOptions {
    pub server_addr: SocketAddr,
    pub auth_token: Option<String>,
    pub reconnect: bool,
    pub heartbeat_interval: Duration,
}

impl Default for TcpClientOptions {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1:10083".parse().unwrap(),
            auth_token: None,
            reconnect: true,
            heartbeat_interval: Duration::from_secs(15),
        }
    }
}

pub struct TcpClient {
    opts: TcpClientOptions,
    socket: Option<TcpStream>,
}

impl TcpClient {
    pub fn new(opts: TcpClientOptions) -> Self {
        Self { opts, socket: None }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        let socket = TcpStream::connect(self.opts.server_addr).await?;
        let _ = socket.set_nodelay(true);
        self.socket = Some(socket);
        let token_opt = self.opts.auth_token.clone();
        if let Some(token) = token_opt {
            self.auth(&token).await?;
        }
        Ok(())
    }

    pub async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Some(mut s) = self.socket.take() {
            tokio::io::AsyncWriteExt::shutdown(&mut s).await?;
        }
        Ok(())
    }

    async fn send(&mut self, msg_type: MsgType, meta: &FrameMeta, body: &[u8]) -> anyhow::Result<()> {
        let frame = encode_frame(msg_type, meta, body)?;
        if let Some(s) = &mut self.socket {
            s.write_all(&frame).await?;
            Ok(())
        } else {
            anyhow::bail!("not connected")
        }
    }

    pub async fn auth(&mut self, token: &str) -> anyhow::Result<()> {
        let meta = FrameMeta {
            topic: None, consumer_id: None, mode: None,
            format: None, auth_token: Some(token.to_string()),
            offset: None, heartbeat_interval_ms: None,
        };
        self.send(MsgType::Auth, &meta, &[]).await
    }

    pub async fn heartbeat(&mut self) -> anyhow::Result<()> {
        let meta = FrameMeta { topic: None, consumer_id: None, mode: None, format: None, auth_token: None, offset: None, heartbeat_interval_ms: None };
        self.send(MsgType::Heartbeat, &meta, &[]).await
    }

    pub async fn subscribe(&mut self, topic: &str, consumer_id: Option<&str>, mode: Option<crate::mq::ConsumptionMode>) -> anyhow::Result<()> {
        let (mode_code, offset_field) = match mode {
            Some(crate::mq::ConsumptionMode::Earliest) => (0u8, None),
            Some(crate::mq::ConsumptionMode::Latest) => (1u8, None),
            Some(crate::mq::ConsumptionMode::Offset(n)) => (2u8, Some(n)),
            Some(crate::mq::ConsumptionMode::LastOffset) => (3u8, None),
            None => (3u8, None),
        };
        let meta = FrameMeta {
            topic: Some(topic.to_string()),
            consumer_id: consumer_id.map(|s| s.to_string()),
            mode: Some(mode_code),
            format: None,
            auth_token: None,
            offset: offset_field,
            heartbeat_interval_ms: None,
        };
        self.send(MsgType::Subscribe, &meta, &[]).await
    }

    pub async fn publish(&mut self, topic: &str, payload: &[u8], format: PayloadFormat) -> anyhow::Result<()> {
        let meta = FrameMeta {
            topic: Some(topic.to_string()),
            consumer_id: None, mode: None, format: Some(format),
            auth_token: None, offset: None, heartbeat_interval_ms: None,
        };
        self.send(MsgType::Publish, &meta, payload).await
    }

    pub async fn ack(&mut self, offset: usize) -> anyhow::Result<()> {
        let meta = FrameMeta { topic: None, consumer_id: None, mode: None, format: None, auth_token: None, offset: Some(offset), heartbeat_interval_ms: None };
        self.send(MsgType::Ack, &meta, &[]).await
    }

    pub async fn nack(&mut self, offset: usize) -> anyhow::Result<()> {
        let meta = FrameMeta { topic: None, consumer_id: None, mode: None, format: None, auth_token: None, offset: Some(offset), heartbeat_interval_ms: None };
        self.send(MsgType::Nack, &meta, &[]).await
    }

    pub async fn seek(&mut self, offset: usize) -> anyhow::Result<()> {
        let meta = FrameMeta { topic: None, consumer_id: None, mode: None, format: None, auth_token: None, offset: Some(offset), heartbeat_interval_ms: None };
        self.send(MsgType::Seek, &meta, &[]).await
    }

    pub async fn read_message(&mut self) -> anyhow::Result<(FrameMeta, Vec<u8>)> {
        if let Some(s) = &mut self.socket {
            loop {
                let (ty, meta, body) = read_frame(s).await?;
                match ty {
                    MsgType::Message => return Ok((meta, body)),
                    MsgType::Ack | MsgType::Heartbeat => continue,
                    _ => continue,
                }
            }
        } else {
            anyhow::bail!("not connected")
        }
    }
}
