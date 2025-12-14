use crate::mq::{MessageQueue, TopicOptions, ConsumptionMode, MessageSubscriber};
use crate::mq::serializer::SerializationFormat;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::protocol::*;

#[derive(Clone)]
pub struct TcpServerOptions {
    pub listen_addr: SocketAddr,
    pub auth_token: Option<String>,
    pub heartbeat_interval: Duration,
    pub enable_persistence: bool,
    pub persistence_dir: Option<std::path::PathBuf>,
}

impl Default for TcpServerOptions {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:10083".parse().unwrap(),
            auth_token: None,
            heartbeat_interval: Duration::from_secs(15),
            enable_persistence: false,
            persistence_dir: None,
        }
    }
}

struct ConnState {
    authenticated: bool,
    last_heartbeat: Instant,
    subscriber: Option<Arc<crate::mq::subscriber::Subscriber>>,
    push_task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct TcpServer {
    mq: MessageQueue,
    opts: TcpServerOptions,
    connections: Arc<Mutex<HashMap<SocketAddr, ConnState>>>,
}

impl TcpServer {
    pub fn new(mq: MessageQueue, opts: TcpServerOptions) -> Self {
        Self {
            mq,
            opts,
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.opts.listen_addr).await?;
        loop {
            let (socket, addr) = listener.accept().await?;
            let server = self.clone();
            tokio::spawn(async move {
                let _ = server.handle_client(socket, addr).await;
            });
        }
    }

    async fn handle_client(&self, socket: TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
        let _ = socket.set_nodelay(true);
        let (mut reader, writer) = socket.into_split();
        let writer = Arc::new(Mutex::new(writer));
        {
            let mut conns = self.connections.lock().await;
            conns.insert(addr, ConnState {
                authenticated: self.opts.auth_token.is_none(),
                last_heartbeat: Instant::now(),
                subscriber: None,
                push_task: None,
            });
        }
        loop {
            let (msg_type, meta, body) = read_frame(&mut reader).await?;
            match msg_type {
                MsgType::Auth => {
                    let ok = if let Some(expected) = &self.opts.auth_token {
                        meta.auth_token.as_deref() == Some(expected.as_str())
                    } else {
                        true
                    };
                    {
                        let mut conns = self.connections.lock().await;
                        if let Some(state) = conns.get_mut(&addr) {
                            state.authenticated = ok;
                        }
                    }
                    let response_meta = FrameMeta {
                        topic: None, consumer_id: None, mode: None, format: None,
                        auth_token: None, offset: None,
                        heartbeat_interval_ms: Some(self.opts.heartbeat_interval.as_millis() as u64),
                    };
                    let frame = encode_frame(MsgType::Ack, &response_meta, &[])?;
                    if let Err(_e) = writer.lock().await.write_all(&frame).await {
                        let mut conns = self.connections.lock().await;
                        if let Some(mut state) = conns.remove(&addr) {
                            if let Some(handle) = state.push_task.take() {
                                handle.abort();
                            }
                        }
                        return Ok(());
                    }
                }
                MsgType::Heartbeat => {
                    {
                        let mut conns = self.connections.lock().await;
                        if let Some(state) = conns.get_mut(&addr) {
                            state.last_heartbeat = Instant::now();
                        }
                    }
                    let meta = FrameMeta { topic: None, consumer_id: None, mode: None, format: None, auth_token: None, offset: None, heartbeat_interval_ms: None };
                    let frame = encode_frame(MsgType::Ack, &meta, &[])?;
                    if let Err(_e) = writer.lock().await.write_all(&frame).await {
                        let mut conns = self.connections.lock().await;
                        if let Some(mut state) = conns.remove(&addr) {
                            if let Some(handle) = state.push_task.take() {
                                handle.abort();
                            }
                        }
                        return Ok(());
                    }
                }
                MsgType::Subscribe => {
                    let topic = meta.topic.clone().ok_or_else(|| anyhow::anyhow!("missing topic"))?;
                    let mode = match meta.mode.unwrap_or(3) {
                        0 => ConsumptionMode::Earliest,
                        1 => ConsumptionMode::Latest,
                        2 => ConsumptionMode::Offset(meta.offset.unwrap_or(0)),
                        3 => ConsumptionMode::LastOffset,
                        _ => ConsumptionMode::LastOffset,
                    };
                    let subscriber = if let Some(cid) = meta.consumer_id.clone() {
                        self.mq.subscriber_group_with_options(topic.clone(), TopicOptions::default(), cid, mode).await?
                    } else {
                        self.mq.subscriber_with_options_and_mode(topic.clone(), TopicOptions::default(), mode).await?
                    };
                    let subscriber = Arc::new(subscriber);
                    {
                        let mut conns = self.connections.lock().await;
                        if let Some(state) = conns.get_mut(&addr) {
                            state.subscriber = Some(subscriber.clone());
                            if let Some(handle) = state.push_task.take() {
                                handle.abort();
                            }
                            let socket_writer = writer.clone();
                            state.push_task = Some(tokio::spawn(async move {
                                loop {
                                    let msg_res = subscriber.recv().await;
                                    let msg = match msg_res {
                                        Ok(m) => m,
                                        Err(_) => break,
                                    };
                                    let meta = FrameMeta {
                                        topic: Some(msg.topic.clone()),
                                        consumer_id: None,
                                        mode: None,
                                        format: Some(match msg.format {
                                            SerializationFormat::Json => PayloadFormat::Json,
                                            SerializationFormat::MessagePack => PayloadFormat::MessagePack,
                                            _ => PayloadFormat::Bincode,
                                        }),
                                        auth_token: None,
                                        offset: msg.offset,
                                        heartbeat_interval_ms: None,
                                    };
                                    let frame = match encode_frame(MsgType::Message, &meta, &msg.payload) {
                                        Ok(f) => f,
                                        Err(_) => break,
                                    };
                                    if socket_writer.lock().await.write_all(&frame).await.is_err() {
                                        break;
                                    }
                                }
                            }));
                        }
                    }
                    let meta = FrameMeta { topic: Some(topic), consumer_id: None, mode: None, format: None, auth_token: None, offset: None, heartbeat_interval_ms: None };
                    let frame = encode_frame(MsgType::Ack, &meta, &[])?;
                    if let Err(_e) = writer.lock().await.write_all(&frame).await {
                        let mut conns = self.connections.lock().await;
                        if let Some(mut state) = conns.remove(&addr) {
                            if let Some(handle) = state.push_task.take() {
                                handle.abort();
                            }
                        }
                        return Ok(());
                    }
                }
                MsgType::Publish => {
                    let topic = meta.topic.clone().ok_or_else(|| anyhow::anyhow!("missing topic"))?;
                    let fmt = meta.format.unwrap_or(PayloadFormat::Json);
                    let ser_fmt = match fmt {
                        PayloadFormat::Json => SerializationFormat::Json,
                        PayloadFormat::MessagePack => SerializationFormat::MessagePack,
                        PayloadFormat::Bincode => SerializationFormat::Bincode,
                    };
                    let publisher = self.mq.publisher(topic.clone());
                    publisher.publish_bytes(body.clone(), ser_fmt).await?;
                    if self.opts.enable_persistence {
                        if let Some(dir) = &self.opts.persistence_dir {
                            let path = dir.join(format!("{}.log", topic));
                            let _ = std::fs::create_dir_all(dir);
                            let _ = std::fs::OpenOptions::new().create(true).append(true).open(&path)
                                .and_then(|mut f| {
                                    use std::io::Write;
                                    f.write_all(&body)?;
                                    f.write_all(b"\n")?;
                                    Ok(())
                                });
                        }
                    }
                    let meta = FrameMeta { topic: Some(topic), consumer_id: None, mode: None, format: None, auth_token: None, offset: None, heartbeat_interval_ms: None };
                    let frame = encode_frame(MsgType::Ack, &meta, &[])?;
                    if let Err(_e) = writer.lock().await.write_all(&frame).await {
                        let mut conns = self.connections.lock().await;
                        if let Some(mut state) = conns.remove(&addr) {
                            if let Some(handle) = state.push_task.take() {
                                handle.abort();
                            }
                        }
                        return Ok(());
                    }
                }
                MsgType::Ack => {
                    if let Some(offset) = meta.offset {
                        let mut conns = self.connections.lock().await;
                        if let Some(state) = conns.get_mut(&addr) {
                            if let Some(sub) = state.subscriber.as_mut() {
                                let _ = sub.commit(offset).await;
                            }
                        }
                    }
                }
                MsgType::Seek => {
                    if let Some(offset) = meta.offset {
                        let mut conns = self.connections.lock().await;
                        if let Some(state) = conns.get_mut(&addr) {
                            if let Some(sub) = state.subscriber.as_ref() {
                                let _ = sub.seek(offset).await;
                            }
                        }
                    }
                    let meta = FrameMeta { topic: None, consumer_id: None, mode: None, format: None, auth_token: None, offset: meta.offset, heartbeat_interval_ms: None };
                    let frame = encode_frame(MsgType::Ack, &meta, &[])?;
                    if let Err(_e) = writer.lock().await.write_all(&frame).await {
                        let mut conns = self.connections.lock().await;
                        if let Some(mut state) = conns.remove(&addr) {
                            if let Some(handle) = state.push_task.take() {
                                handle.abort();
                            }
                        }
                        return Ok(());
                    }
                }
                MsgType::Commit => {
                    if let Some(offset) = meta.offset {
                        let mut conns = self.connections.lock().await;
                        if let Some(state) = conns.get_mut(&addr) {
                            if let Some(sub) = state.subscriber.as_ref() {
                                let _ = sub.commit(offset).await;
                            }
                        }
                    }
                }
                MsgType::Nack => {
                    // Simple retransmission: if offset provided, attempt to seek
                    if let Some(offset) = meta.offset {
                        let mut conns = self.connections.lock().await;
                        if let Some(state) = conns.get_mut(&addr) {
                            if let Some(sub) = state.subscriber.as_ref() {
                                let _ = sub.seek(offset).await;
                            }
                        }
                    }
                }
                MsgType::Unsubscribe => {
                    let mut conns = self.connections.lock().await;
                    if let Some(state) = conns.get_mut(&addr) {
                        state.subscriber = None;
                        if let Some(handle) = state.push_task.take() {
                            handle.abort();
                        }
                    }
                    let meta = FrameMeta { topic: None, consumer_id: None, mode: None, format: None, auth_token: None, offset: None, heartbeat_interval_ms: None };
                    let frame = encode_frame(MsgType::Ack, &meta, &[])?;
                    if let Err(_e) = writer.lock().await.write_all(&frame).await {
                        let mut conns = self.connections.lock().await;
                        if let Some(mut state) = conns.remove(&addr) {
                            if let Some(handle) = state.push_task.take() {
                                handle.abort();
                            }
                        }
                        return Ok(());
                    }
                }
                MsgType::Message => {
                    // ignore from client
                }
            }
        }
    }
}
