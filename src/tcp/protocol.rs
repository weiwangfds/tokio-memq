use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MsgType {
    Auth = 1,
    Publish = 2,
    Subscribe = 3,
    Ack = 4,
    Nack = 5,
    Heartbeat = 6,
    Message = 7,
    Unsubscribe = 8,
    Commit = 9,
    Seek = 10,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum PayloadFormat {
    Json,
    MessagePack,
    Bincode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameMeta {
    pub topic: Option<String>,
    pub consumer_id: Option<String>,
    pub mode: Option<u8>,
    pub format: Option<PayloadFormat>,
    pub auth_token: Option<String>,
    pub offset: Option<usize>,
    pub heartbeat_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct WireHeader {
    pub magic: u16,       // 'TM' 0x4D54
    pub version: u8,      // 1
    pub msg_type: u8,     // MsgType
    pub header_len: u16,  // meta length
    pub body_len: u32,    // payload length
    pub checksum: u32,    // crc32 over body
}

impl WireHeader {
    pub const MAGIC: u16 = 0x4D54;
    pub const VERSION: u8 = 1;
    pub const SIZE: usize = 14;

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..2].copy_from_slice(&self.magic.to_le_bytes());
        buf[2] = self.version;
        buf[3] = self.msg_type;
        buf[4..6].copy_from_slice(&self.header_len.to_le_bytes());
        buf[6..10].copy_from_slice(&self.body_len.to_le_bytes());
        buf[10..14].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> anyhow::Result<Self> {
        if buf.len() < Self::SIZE {
            anyhow::bail!("header too short");
        }
        let magic = u16::from_le_bytes([buf[0], buf[1]]);
        if magic != Self::MAGIC {
            anyhow::bail!("invalid magic");
        }
        Ok(Self {
            magic,
            version: buf[2],
            msg_type: buf[3],
            header_len: u16::from_le_bytes([buf[4], buf[5]]),
            body_len: u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]),
            checksum: u32::from_le_bytes([buf[10], buf[11], buf[12], buf[13]]),
        })
    }
}

pub fn encode_frame(msg_type: MsgType, meta: &FrameMeta, body: &[u8]) -> anyhow::Result<Vec<u8>> {
    let meta_bytes = bincode::serialize(meta)?;
    let checksum = crc32fast::hash(body);
    let header = WireHeader {
        magic: WireHeader::MAGIC,
        version: WireHeader::VERSION,
        msg_type: msg_type as u8,
        header_len: meta_bytes.len() as u16,
        body_len: body.len() as u32,
        checksum,
    };
    let mut buf = Vec::with_capacity(WireHeader::SIZE + meta_bytes.len() + body.len());
    buf.extend_from_slice(&header.to_bytes());
    buf.extend_from_slice(&meta_bytes);
    buf.extend_from_slice(body);
    Ok(buf)
}

pub async fn read_exact<R: AsyncRead + Unpin>(reader: &mut R, len: usize) -> anyhow::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> anyhow::Result<(MsgType, FrameMeta, Vec<u8>)> {
    let header_bytes = read_exact(reader, WireHeader::SIZE).await?;
    let header = WireHeader::from_bytes(&header_bytes)?;
    let mut meta_buf = vec![0u8; header.header_len as usize];
    reader.read_exact(&mut meta_buf).await?;
    let mut body_buf = vec![0u8; header.body_len as usize];
    reader.read_exact(&mut body_buf).await?;
    let checksum = crc32fast::hash(&body_buf);
    if checksum != header.checksum {
        anyhow::bail!("checksum mismatch");
    }
    let meta: FrameMeta = bincode::deserialize(&meta_buf)?;
    let msg_type = match header.msg_type {
        1 => MsgType::Auth,
        2 => MsgType::Publish,
        3 => MsgType::Subscribe,
        4 => MsgType::Ack,
        5 => MsgType::Nack,
        6 => MsgType::Heartbeat,
        7 => MsgType::Message,
        8 => MsgType::Unsubscribe,
        9 => MsgType::Commit,
        10 => MsgType::Seek,
        _ => anyhow::bail!("unknown msg type"),
    };
    Ok((msg_type, meta, body_buf))
}
