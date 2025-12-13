//! 泛型序列化支持 / Generic serialization support
//! 
//! 仅保留 Bincode 序列化 / Only Bincode serialization retained

use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum SerializationError {
    #[error("Bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),
    #[error("Bincode decode error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    
    #[error("Unsupported serialization format: {0}")]
    UnsupportedFormat(String),
    
    #[error("Serialization failed: {0}")]
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SerializationFormat {
    Bincode,
}

impl SerializationFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            SerializationFormat::Bincode => "bincode",
        }
    }
    
    pub fn from_str(s: &str) -> Result<Self, SerializationError> {
        match s.to_lowercase().as_str() {
            "bincode" | "binary" => Ok(SerializationFormat::Bincode),
            _ => Err(SerializationError::UnsupportedFormat(s.to_string())),
        }
    }
}

pub trait Serializer {
    fn serialize<T: Serialize + ?Sized>(data: &T) -> Result<Vec<u8>, SerializationError>;
    fn deserialize<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, SerializationError>;
    fn format() -> SerializationFormat;
}

pub struct BincodeSerializer;

impl Serializer for BincodeSerializer {
    fn serialize<T: Serialize + ?Sized>(data: &T) -> Result<Vec<u8>, SerializationError> {
        let config = bincode::config::standard();
        bincode::serde::encode_to_vec(data, config).map_err(SerializationError::BincodeEncode)
    }
    
    fn deserialize<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, SerializationError> {
        let config = bincode::config::standard();
        let (decoded, _) = bincode::serde::decode_from_slice(data, config).map_err(SerializationError::BincodeDecode)?;
        Ok(decoded)
    }
    
    fn format() -> SerializationFormat {
        SerializationFormat::Bincode
    }
}

pub struct SerializationHelper;

impl SerializationHelper {
    pub fn serialize<T: Serialize + ?Sized>(
        data: &T, 
        format: &SerializationFormat
    ) -> Result<Vec<u8>, SerializationError> {
        match format {
            SerializationFormat::Bincode => BincodeSerializer::serialize(data),
        }
    }
    
    pub fn deserialize<T: serde::de::DeserializeOwned>(
        data: &[u8], 
        format: &SerializationFormat
    ) -> Result<T, SerializationError> {
        match format {
            SerializationFormat::Bincode => BincodeSerializer::deserialize(data),
        }
    }
    
    pub fn auto_detect_format(_data: &[u8]) -> SerializationFormat {
        SerializationFormat::Bincode
    }
    
    pub fn supported_formats() -> Vec<SerializationFormat> {
        vec![SerializationFormat::Bincode]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: u32,
        name: String,
        value: f64,
    }
    
    #[test]
    fn test_bincode_serialization() {
        let data = TestData {
            id: 42,
            name: "test".to_string(),
            value: 3.14,
        };
        
        let serialized = BincodeSerializer::serialize(&data).unwrap();
        let deserialized: TestData = BincodeSerializer::deserialize(&serialized).unwrap();
        
        assert_eq!(data, deserialized);
    }
    
    #[test]
    fn test_format_parsing() {
        assert_eq!(SerializationFormat::from_str("bincode").unwrap(), SerializationFormat::Bincode);
        assert_eq!(SerializationFormat::from_str("binary").unwrap(), SerializationFormat::Bincode);
        assert!(SerializationFormat::from_str("unknown").is_err());
    }
}
