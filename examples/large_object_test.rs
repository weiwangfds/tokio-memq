use tokio_memq::mq::MessageQueue;
use tokio_memq::{TopicOptions, AsyncMessagePublisher, MessageSubscriber};
use tokio_memq::mq::serializer::SerializationFormat;
use std::time::Instant;
use std::sync::Arc;
use sysinfo::{Pid, System, ProcessesToUpdate, ProcessRefreshKind};

struct LargeObjectTestConfig {
    size_name: String,
    size_bytes: usize,
    count: usize,
    max_messages: Option<usize>,
}

async fn run_large_object_test(config: LargeObjectTestConfig) -> anyhow::Result<()> {
    println!("----------------------------------------------------------------");
    println!("Testing Large Object: {} ({} bytes) x {} messages", 
             config.size_name, config.size_bytes, config.count);

    // Warmup system info
    let mut sys = System::new();
    let pid = Pid::from(std::process::id() as usize);
    
    let refresh_sys = |sys: &mut System| {
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            ProcessRefreshKind::everything(),
        );
    };
    
    refresh_sys(&mut sys);
    let mem_start = sys.process(pid).map(|p| p.memory()).unwrap_or(0);
    println!("  Initial Memory: {:.2} MB", mem_start as f64 / 1024.0 / 1024.0);

    let mq = MessageQueue::new();
    let topic = format!("topic_{}", config.size_name);

    // Ensure we have enough buffer but not too much to blow up RAM
    // For 100MB messages, 100 messages = 10GB RAM! We must be careful.
    // We'll set a small buffer for very large messages to force backpressure/flow control if we were using it,
    // but here we just want to test throughput.
    let opts = TopicOptions {
        max_messages: config.max_messages.or(Some(1000)), 
        ..Default::default()
    };
    
    // Create subscriber
    let sub = mq.subscriber_with_options(topic.clone(), opts).await?;
    let pub_instance = mq.publisher(topic.clone());

    // Create payload once to save allocation time during test setup
    println!("  Allocating payload...");
    let payload = vec![0u8; config.size_bytes];
    let payload_arc = Arc::new(payload);
    println!("  Payload allocated.");

    let start = Instant::now();

    // Spawn consumer
    let consumer_handle = tokio::spawn(async move {
        let mut received = 0;
        let mut total_bytes: u64 = 0;
        while received < config.count {
            match sub.recv().await {
                Ok(msg) => {
                    received += 1;
                    total_bytes += msg.payload.len() as u64;
                }
                Err(e) => {
                    eprintln!("Consumer error: {}", e);
                    break;
                }
            }
        }
        (received, total_bytes)
    });

    // Run producer
    let p_count = config.count;
    let p_payload = payload_arc.clone();
    
    // We send sequentially to measure pure pipeline speed without producer-side concurrency noise
    for _ in 0..p_count {
        // Use publish_bytes to ensure we are sending raw bytes and not wrapping in Native format (which has 0 length for payload)
        // We use Bincode as a placeholder format for raw bytes
        pub_instance.publish_bytes(p_payload.to_vec(), SerializationFormat::Bincode).await?;
    }

    // Wait for consumer
    let (received, bytes_received) = consumer_handle.await?;
    let duration = start.elapsed();

    refresh_sys(&mut sys);
    let mem_end = sys.process(pid).map(|p| p.memory()).unwrap_or(0);
    
    let total_mb = (bytes_received as f64) / 1024.0 / 1024.0;
    // Prevent division by zero if test is too fast
    let secs = duration.as_secs_f64();
    let (throughput_mib_s, msgs_sec) = if secs > 0.0 {
        (total_mb / secs, (received as f64) / secs)
    } else {
        (0.0, 0.0)
    };

    println!("Results for {}:", config.size_name);
    println!("  Duration: {:?}", duration);
    println!("  Message Rate: {:.2} msgs/sec", msgs_sec);
    println!("  Throughput: {:.2} MiB/s", throughput_mib_s);
    println!("  Memory Usage: Start {:.2} MB -> End {:.2} MB (Diff: {:.2} MB)", 
             mem_start as f64 / 1024.0 / 1024.0, 
             mem_end as f64 / 1024.0 / 1024.0,
             (mem_end as i64 - mem_start as i64) as f64 / 1024.0 / 1024.0);
    
    if received != config.count {
        println!("  !! FAILURE: Expected {} messages, got {}", config.count, received);
    } else {
        println!("  Status: SUCCESS");
    }
    println!("----------------------------------------------------------------\n");

    Ok(())
}

async fn run_large_object_batch_test(config: LargeObjectTestConfig, batch_size: usize) -> anyhow::Result<()> {
    println!("----------------------------------------------------------------");
    println!("Testing Large Object Batch: {} ({} bytes) x {} messages, Batch Size: {}", 
             config.size_name, config.size_bytes, config.count, batch_size);

    // Warmup system info
    let mut sys = System::new();
    let pid = Pid::from(std::process::id() as usize);
    
    let refresh_sys = |sys: &mut System| {
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            ProcessRefreshKind::everything(),
        );
    };
    
    refresh_sys(&mut sys);
    let mem_start = sys.process(pid).map(|p| p.memory()).unwrap_or(0);
    println!("  Initial Memory: {:.2} MB", mem_start as f64 / 1024.0 / 1024.0);

    let mq = MessageQueue::new();
    let topic = format!("topic_batch_{}", config.size_name);

    let opts = TopicOptions {
        max_messages: Some(1000), 
        ..Default::default()
    };
    
    // Create subscriber
    let sub = mq.subscriber_with_options(topic.clone(), opts).await?;
    let pub_instance = mq.publisher(topic.clone());

    // Create payload
    println!("  Allocating payload...");
    let payload = vec![0u8; config.size_bytes];
    let payload_arc = Arc::new(payload);
    println!("  Payload allocated.");

    let start = Instant::now();

    // Spawn consumer
    let consumer_handle = tokio::spawn(async move {
        let mut received = 0;
        let mut total_bytes: u64 = 0;
        while received < config.count {
            match sub.recv_batch(batch_size).await {
                Ok(msgs) => {
                    for msg in msgs {
                        received += 1;
                        total_bytes += msg.payload.len() as u64;
                    }
                }
                Err(e) => {
                    eprintln!("Consumer error: {}", e);
                    break;
                }
            }
        }
        (received, total_bytes)
    });

    // Run producer
    let p_count = config.count;
    let p_payload = payload_arc.clone();
    
    let chunks = p_count / batch_size;
    let remainder = p_count % batch_size;

    for _ in 0..chunks {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            batch.push(p_payload.to_vec());
        }
        // Use Bincode to ensure payload length is preserved (serialized)
        pub_instance.publish_batch_with_format(batch, SerializationFormat::Bincode).await?;
    }
    
    if remainder > 0 {
        let mut batch = Vec::with_capacity(remainder);
        for _ in 0..remainder {
            batch.push(p_payload.to_vec());
        }
        pub_instance.publish_batch_with_format(batch, SerializationFormat::Bincode).await?;
    }

    // Wait for consumer
    let (received, bytes_received) = consumer_handle.await?;
    let duration = start.elapsed();

    refresh_sys(&mut sys);
    let mem_end = sys.process(pid).map(|p| p.memory()).unwrap_or(0);

    let total_mb = (bytes_received as f64) / 1024.0 / 1024.0;
    let secs = duration.as_secs_f64();
    let (throughput_mib_s, msgs_sec) = if secs > 0.0 {
        (total_mb / secs, (received as f64) / secs)
    } else {
        (0.0, 0.0)
    };

    println!("Results for Batch {}:", config.size_name);
    println!("  Duration: {:?}", duration);
    println!("  Message Rate: {:.2} msgs/sec", msgs_sec);
    println!("  Throughput: {:.2} MiB/s", throughput_mib_s);
    println!("  Memory Usage: Start {:.2} MB -> End {:.2} MB (Diff: {:.2} MB)", 
             mem_start as f64 / 1024.0 / 1024.0, 
             mem_end as f64 / 1024.0 / 1024.0,
             (mem_end as i64 - mem_start as i64) as f64 / 1024.0 / 1024.0);
    
    if received != config.count {
        println!("  !! FAILURE: Expected {} messages, got {}", config.count, received);
    } else {
        println!("  Status: SUCCESS");
    }
    println!("----------------------------------------------------------------\n");

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 1KB x 1,000,000 - High QPS
    run_large_object_test(LargeObjectTestConfig {
        size_name: "1KB".to_string(),
        size_bytes: 1024,
        count: 1_000_000,
        max_messages: Some(2_000_000), // Ensure buffer is large enough
    }).await?;

    // 2. 1MB - The "standard" large object
    run_large_object_test(LargeObjectTestConfig {
        size_name: "1MB".to_string(),
        size_bytes: 1024 * 1024,
        count: 1000, // 1GB Total
        max_messages: None,
    }).await?;

    // 3. 10MB - Heavier
    run_large_object_test(LargeObjectTestConfig {
        size_name: "10MB".to_string(),
        size_bytes: 10 * 1024 * 1024,
        count: 100, // 1GB Total
        max_messages: None,
    }).await?;

    // 4. 50MB - Very Large
    run_large_object_test(LargeObjectTestConfig {
        size_name: "50MB".to_string(),
        size_bytes: 50 * 1024 * 1024,
        count: 20, // 1GB Total
        max_messages: None,
    }).await?;

    // 5. 100MB - Extreme
    // Note: memory usage will spike here as we hold messages in the queue
    run_large_object_test(LargeObjectTestConfig {
        size_name: "100MB".to_string(),
        size_bytes: 100 * 1024 * 1024,
        count: 10, // 1GB Total
        max_messages: None,
    }).await?;

    // 6. Batch Test - 1MB x 1000 messages, Batch Size 10
    // Tests batch processing overhead/efficiency
    run_large_object_batch_test(LargeObjectTestConfig {
        size_name: "1MB".to_string(),
        size_bytes: 1024 * 1024,
        count: 1000,
        max_messages: None,
    }, 10).await?;

    // 7. Small Batch Test - 1KB x 1,000,000 messages, Batch Size 1000
    run_large_object_batch_test(LargeObjectTestConfig {
        size_name: "1KB".to_string(),
        size_bytes: 1024,
        count: 1_000_000,
        max_messages: Some(2_000_000),
    }, 1000).await?;

    Ok(())
}
