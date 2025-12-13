//! 泛型序列化支持 / Generic serialization support
//! 
//! 支持 Bincode, JSON, MessagePack 等多种格式，并允许自定义实现。
//! Supports multiple formats including Bincode, JSON, MessagePack, and allows custom implementations.

use std::sync::Arc;
use serde::Serialize;
use erased_serde::{Serialize as ErasedSerialize, Deserializer as ErasedDeserializer};
use std::collections::HashMap;
use std::sync::RwLock;
use lazy_static::lazy_static;

#[derive(Debug, thiserror::Error)]
pub enum SerializationError {
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("MessagePack encode error: {0}")]
    MessagePackEncode(#[from] rmp_serde::encode::Error),
    #[error("MessagePack decode error: {0}")]
    MessagePackDecode(#[from] rmp_serde::decode::Error),
    
    #[error("Unsupported serialization format: {0}")]
    UnsupportedFormat(String),
    
    #[error("Factory creation failed: {0}")]
    FactoryError(String),
    
    #[error("Configuration invalid: {0}")]
    ConfigInvalid(String),
    
    #[error("Thread synchronization error: {0}")]
    ThreadSyncError(String),
    
    #[error("Serialization failed: {0}")]
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SerializationFormat {
    Bincode,
    Json,
    MessagePack,
    Custom(String),
}

impl SerializationFormat {
    pub fn as_str(&self) -> &str {
        match self {
            SerializationFormat::Bincode => "bincode",
            SerializationFormat::Json => "json",
            SerializationFormat::MessagePack => "messagepack",
            SerializationFormat::Custom(s) => s,
        }
    }
    
    pub fn from_str(s: &str) -> Result<Self, SerializationError> {
        match s.to_lowercase().as_str() {
            "bincode" | "binary" => Ok(SerializationFormat::Bincode),
            "json" => Ok(SerializationFormat::Json),
            "msgpack" | "messagepack" => Ok(SerializationFormat::MessagePack),
            other => Ok(SerializationFormat::Custom(other.to_string())),
        }
    }
}

/// Configuration for Bincode serialization
#[derive(Debug, Clone)]
pub struct BincodeConfig {
    // We stick to standard options for now
}

impl Default for BincodeConfig {
    fn default() -> Self {
        Self {}
    }
}

/// Configuration for JSON serialization
#[derive(Debug, Clone)]
pub struct JsonConfig {
    pub pretty: bool,
}

impl Default for JsonConfig {
    fn default() -> Self {
        Self { pretty: false }
    }
}

/// Configuration for MessagePack serialization
#[derive(Debug, Clone)]
pub struct MessagePackConfig {
    pub struct_map: bool,
}

impl Default for MessagePackConfig {
    fn default() -> Self {
        Self { struct_map: false }
    }
}

#[derive(Debug, Clone)]
pub enum SerializationConfig {
    Bincode(BincodeConfig),
    Json(JsonConfig),
    MessagePack(MessagePackConfig),
    Custom(Arc<dyn std::any::Any + Send + Sync>),
    Default,
}

/// Trait for Serialization
/// Object-safe trait allowing dynamic dispatch
pub trait Serializer: Send + Sync {
    fn serialize(&self, data: &dyn ErasedSerialize) -> Result<Vec<u8>, SerializationError>;
    fn format(&self) -> SerializationFormat;
}

/// Trait for Deserialization
/// Uses a callback pattern to allow using stack-based deserializers (like serde_json::Deserializer)
/// which might not be easily boxed as trait objects due to lifetime or trait implementation issues.
pub trait Deserializer: Send + Sync {
    fn with_deserializer(
        &self, 
        data: &[u8], 
        f: &mut dyn FnMut(&mut dyn ErasedDeserializer) -> Result<(), SerializationError>
    ) -> Result<(), SerializationError>;
    
    fn format(&self) -> SerializationFormat;
}

// --- Implementations ---

// Bincode
#[derive(Default, Clone)]
pub struct BincodeSerializer {
    config: BincodeConfig,
}

impl BincodeSerializer {
    pub fn new(config: BincodeConfig) -> Self {
        Self { config }
    }
    
    // Backward compatibility methods
    pub fn serialize<T: Serialize>(data: &T) -> Result<Vec<u8>, SerializationError> {
        let s = BincodeSerializer::default();
        <BincodeSerializer as Serializer>::serialize(&s, data)
    }
    
    pub fn deserialize<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, SerializationError> {
        let s = BincodeSerializer::default();
        let mut obj: Option<T> = None;
        s.with_deserializer(data, &mut |erased_de| {
            let t: T = erased_serde::deserialize(erased_de).map_err(|e| SerializationError::Custom(e.to_string()))?;
            obj = Some(t);
            Ok(())
        })?;
        Ok(obj.unwrap())
    }
}

impl Serializer for BincodeSerializer {
    fn serialize(&self, data: &dyn ErasedSerialize) -> Result<Vec<u8>, SerializationError> {
        let mut buf = Vec::new();
        let _cfg = &self.config;
        let options = bincode::DefaultOptions::new();
        let mut serializer = bincode::Serializer::new(&mut buf, options);
        let mut erased = <dyn erased_serde::Serializer>::erase(&mut serializer);
        data.erased_serialize(&mut erased).map_err(|e| SerializationError::Bincode(Box::new(bincode::ErrorKind::Custom(e.to_string()))))?;
        Ok(buf)
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::Bincode
    }
}

impl Deserializer for BincodeSerializer {
    fn with_deserializer(
        &self, 
        data: &[u8], 
        f: &mut dyn FnMut(&mut dyn ErasedDeserializer) -> Result<(), SerializationError>
    ) -> Result<(), SerializationError> {
        let _cfg = &self.config;
        let options = bincode::DefaultOptions::new();
        let mut de = bincode::Deserializer::from_slice(data, options);
        let mut erased = <dyn ErasedDeserializer>::erase(&mut de);
        f(&mut erased)
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::Bincode
    }
}

// JSON
#[derive(Default, Clone)]
pub struct JsonSerializer {
    config: JsonConfig,
}

impl JsonSerializer {
    pub fn new(config: JsonConfig) -> Self {
        Self { config }
    }
}

impl Serializer for JsonSerializer {
    fn serialize(&self, data: &dyn ErasedSerialize) -> Result<Vec<u8>, SerializationError> {
        if self.config.pretty {
            serde_json::to_vec_pretty(data).map_err(SerializationError::Json)
        } else {
            serde_json::to_vec(data).map_err(SerializationError::Json)
        }
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::Json
    }
}

impl Deserializer for JsonSerializer {
    fn with_deserializer(
        &self, 
        data: &[u8], 
        f: &mut dyn FnMut(&mut dyn ErasedDeserializer) -> Result<(), SerializationError>
    ) -> Result<(), SerializationError> {
        let mut de = serde_json::Deserializer::from_slice(data);
        let mut erased = <dyn ErasedDeserializer>::erase(&mut de);
        f(&mut erased)
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::Json
    }
}

// MessagePack
#[derive(Default, Clone)]
pub struct MessagePackSerializer {
    config: MessagePackConfig,
}

impl MessagePackSerializer {
    pub fn new(config: MessagePackConfig) -> Self {
        Self { config }
    }
}

impl Serializer for MessagePackSerializer {
    fn serialize(&self, data: &dyn ErasedSerialize) -> Result<Vec<u8>, SerializationError> {
        let mut buf = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut buf);
        // Apply config if needed (rmp-serde builder pattern)
        if self.config.struct_map {
             // serializer.set_struct_map(); // rmp-serde specific
        }
        
        let mut erased = <dyn erased_serde::Serializer>::erase(&mut serializer);
        data.erased_serialize(&mut erased).map_err(|e| SerializationError::Custom(format!("MessagePack error: {}", e)))?;
        Ok(buf)
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::MessagePack
    }
}

impl Deserializer for MessagePackSerializer {
    fn with_deserializer(
        &self, 
        data: &[u8], 
        f: &mut dyn FnMut(&mut dyn ErasedDeserializer) -> Result<(), SerializationError>
    ) -> Result<(), SerializationError> {
        let mut de = rmp_serde::Deserializer::new(data);
        let mut erased = <dyn ErasedDeserializer>::erase(&mut de);
        f(&mut erased)
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::MessagePack
    }
}


// --- Factory ---

pub struct SerializationFactory;

lazy_static! {
    static ref GLOBAL_SERIALIZERS: RwLock<HashMap<String, Arc<dyn Serializer>>> = RwLock::new(HashMap::new());
    static ref GLOBAL_DESERIALIZERS: RwLock<HashMap<String, Arc<dyn Deserializer>>> = RwLock::new(HashMap::new());
}

impl SerializationFactory {
    pub fn register_serializer(format: &str, serializer: Arc<dyn Serializer>) {
        let mut map = GLOBAL_SERIALIZERS.write().unwrap();
        map.insert(format.to_lowercase(), serializer);
    }

    pub fn register_deserializer(format: &str, deserializer: Arc<dyn Deserializer>) {
        let mut map = GLOBAL_DESERIALIZERS.write().unwrap();
        map.insert(format.to_lowercase(), deserializer);
    }
    
    pub fn get_serializer(format: &str) -> Option<Arc<dyn Serializer>> {
        // First check registered
        {
            let map = GLOBAL_SERIALIZERS.read().unwrap();
            if let Some(s) = map.get(&format.to_lowercase()) {
                return Some(s.clone());
            }
        }
        
        // Then check built-ins
        match format.to_lowercase().as_str() {
            "bincode" => Some(Arc::new(BincodeSerializer::default())),
            "json" => Some(Arc::new(JsonSerializer::default())),
            "msgpack" | "messagepack" => Some(Arc::new(MessagePackSerializer::default())),
            _ => None,
        }
    }
    
    pub fn get_deserializer(format: &str) -> Option<Arc<dyn Deserializer>> {
        {
            let map = GLOBAL_DESERIALIZERS.read().unwrap();
            if let Some(d) = map.get(&format.to_lowercase()) {
                return Some(d.clone());
            }
        }
        
        match format.to_lowercase().as_str() {
            "bincode" => Some(Arc::new(BincodeSerializer::default())),
            "json" => Some(Arc::new(JsonSerializer::default())),
            "msgpack" | "messagepack" => Some(Arc::new(MessagePackSerializer::default())),
            _ => None,
        }
    }

    pub fn create_serializer(format: &str, config: SerializationConfig) -> Result<Arc<dyn Serializer>, SerializationError> {
        match (format.to_lowercase().as_str(), config) {
            ("bincode", SerializationConfig::Bincode(c)) => Ok(Arc::new(BincodeSerializer::new(c))),
            ("bincode", SerializationConfig::Default) => Ok(Arc::new(BincodeSerializer::default())),
            ("json", SerializationConfig::Json(c)) => Ok(Arc::new(JsonSerializer::new(c))),
            ("json", SerializationConfig::Default) => Ok(Arc::new(JsonSerializer::default())),
            ("msgpack" | "messagepack", SerializationConfig::MessagePack(c)) => Ok(Arc::new(MessagePackSerializer::new(c))),
            ("msgpack" | "messagepack", SerializationConfig::Default) => Ok(Arc::new(MessagePackSerializer::default())),
            _ => Err(SerializationError::ConfigInvalid(format!("Format {} does not match config or not supported for creation", format))),
        }
    }
}

// --- Helper ---

pub struct SerializationHelper;

impl SerializationHelper {
    pub fn serialize<T: Serialize>(
        data: &T, 
        format: &SerializationFormat
    ) -> Result<Vec<u8>, SerializationError> {
        let fmt_str = format.as_str();
        let serializer = SerializationFactory::get_serializer(fmt_str)
            .ok_or_else(|| SerializationError::UnsupportedFormat(fmt_str.to_string()))?;
            
        serializer.serialize(data)
    }
    
    pub fn deserialize<T: serde::de::DeserializeOwned>(
        data: &[u8], 
        format: &SerializationFormat
    ) -> Result<T, SerializationError> {
        let fmt_str = format.as_str();
        let deserializer = SerializationFactory::get_deserializer(fmt_str)
            .ok_or_else(|| SerializationError::UnsupportedFormat(fmt_str.to_string()))?;
            
        let mut obj: Option<T> = None;
        deserializer.with_deserializer(data, &mut |erased_de| {
            let t: T = erased_serde::deserialize(erased_de).map_err(|e| SerializationError::Custom(e.to_string()))?;
            obj = Some(t);
            Ok(())
        })?;
        
        Ok(obj.expect("Deserialization closure was not called or failed silently"))
    }
    
    pub fn auto_detect_format(_data: &[u8]) -> SerializationFormat {
        // In a real system we might check magic bytes
        // For now default to Bincode
        SerializationFormat::Bincode
    }
    
    pub fn supported_formats() -> Vec<SerializationFormat> {
        vec![
            SerializationFormat::Bincode,
            SerializationFormat::Json,
            SerializationFormat::MessagePack,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::thread;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: u32,
        name: String,
        value: f64,
    }

    #[test]
    fn test_bincode_serialization() {
        let data = TestData { id: 42, name: "test".to_string(), value: 3.14 };
        let serialized = SerializationHelper::serialize(&data, &SerializationFormat::Bincode).unwrap();
        let deserialized: TestData = SerializationHelper::deserialize(&serialized, &SerializationFormat::Bincode).unwrap();
        assert_eq!(data, deserialized);
    }
    
    #[test]
    fn test_json_serialization() {
        let data = TestData { id: 42, name: "test".to_string(), value: 3.14 };
        let serialized = SerializationHelper::serialize(&data, &SerializationFormat::Json).unwrap();
        // Check if it looks like JSON
        assert!(std::str::from_utf8(&serialized).is_ok());
        let deserialized: TestData = SerializationHelper::deserialize(&serialized, &SerializationFormat::Json).unwrap();
        assert_eq!(data, deserialized);
    }
    
    #[test]
    fn test_messagepack_serialization() {
        let data = TestData { id: 42, name: "test".to_string(), value: 3.14 };
        let serialized = SerializationHelper::serialize(&data, &SerializationFormat::MessagePack).unwrap();
        let deserialized: TestData = SerializationHelper::deserialize(&serialized, &SerializationFormat::MessagePack).unwrap();
        assert_eq!(data, deserialized);
    }
    
    #[test]
    fn test_factory_custom_creation() {
        let config = JsonConfig { pretty: true };
        let s = SerializationFactory::create_serializer("json", SerializationConfig::Json(config)).unwrap();
        let data = TestData { id: 1, name: "a".to_string(), value: 1.0 };
        let bytes = s.serialize(&data).unwrap();
        let json_str = String::from_utf8(bytes).unwrap();
        assert!(json_str.contains("\n")); // Pretty print has newlines
    }

    #[test]
    fn test_thread_safety() {
        let data = TestData { id: 1, name: "thread".to_string(), value: 1.0 };
        let serializer = SerializationFactory::get_serializer("json").unwrap();
        let serializer_clone = serializer.clone();
        
        let handle = thread::spawn(move || {
            serializer_clone.serialize(&data).unwrap()
        });
        
        let bytes = handle.join().unwrap();
        let deserializer = SerializationFactory::get_deserializer("json").unwrap();
        
        let mut obj: Option<TestData> = None;
        deserializer.with_deserializer(&bytes, &mut |erased_de| {
            let t: TestData = erased_serde::deserialize(erased_de).unwrap();
            obj = Some(t);
            Ok(())
        }).unwrap();
        
        assert_eq!(obj.unwrap().id, 1);
    }
}
