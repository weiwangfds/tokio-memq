use tokio_memq::mq::MessageQueue;
use tokio_memq::{AsyncMessagePublisher, MessageSubscriber, TopicOptions};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
use sysinfo::{Pid, System, ProcessesToUpdate, ProcessRefreshKind};

#[derive(Clone)]
struct TestConfig {
    name: String,
    producers: usize,
    consumers: usize,
    msg_size: usize,
    msg_count_per_producer: usize,
    batch_size: usize,
    topic_options: Option<TopicOptions>,
}

struct TestResult {
    config: TestConfig,
    duration: Duration,
    throughput: f64,
    avg_latency_us: f64,
    cpu_usage_start: f32,
    cpu_usage_end: f32,
    memory_usage_start: u64,
    memory_usage_end: u64,
    dropped_messages: usize,
    success: bool,
}

async fn run_test(config: TestConfig) -> anyhow::Result<TestResult> {
    println!("----------------------------------------------------------------");
    println!("Running Test: {}", config.name);
    println!("Producers: {}, Consumers: {}, Msg Size: {} B, Count/Prod: {}, Batch: {}", 
             config.producers, config.consumers, config.msg_size, config.msg_count_per_producer, config.batch_size);

    let mq = MessageQueue::new();
    let topic = "perf_topic";
    
    // Apply topic options if provided, otherwise default
    if let Some(opts) = &config.topic_options {
        // We need to initialize the topic with options before subscribers connect
        let _ = mq.subscriber_with_options(topic.to_string(), opts.clone()).await?;
    }
    
    // Warmup system info
    let mut sys = System::new();
    let pid = Pid::from(std::process::id() as usize);
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::everything(),
    );
    let cpu_start = sys.process(pid).map(|p| p.cpu_usage()).unwrap_or(0.0);
    let mem_start = sys.process(pid).map(|p| p.memory()).unwrap_or(0);

    // Setup consumers
    let mut consumer_handles = Vec::new();
    let total_messages = config.producers * config.msg_count_per_producer;
    let messages_received = Arc::new(AtomicUsize::new(0));
    
    // Pre-create subscribers to ensure they are ready
    let mut subscribers = Vec::new();
    for _ in 0..config.consumers {
        subscribers.push(mq.subscriber(topic.to_string()).await?);
    }

    let start_time = Instant::now();

    for sub in subscribers {
        let counter = messages_received.clone();
        let target = if config.consumers > 0 { total_messages } else { 0 }; 
        
        consumer_handles.push(tokio::spawn(async move {
            let mut count = 0;
            loop {
                // Use timeout to prevent hanging forever if messages are dropped
                match tokio::time::timeout(Duration::from_secs(5), sub.recv()).await {
                    Ok(Ok(_msg)) => {
                        count += 1;
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(_)) => break, // Channel closed
                    Err(_) => {
                        // Timeout waiting for message
                         break;
                    }
                }
                if count >= target {
                    break;
                }
            }
        }));
    }

    // Setup producers
    let mut producer_handles = Vec::new();
    let payload = vec![0u8; config.msg_size]; 
    let payload_arc = Arc::new(payload);
    
    for _ in 0..config.producers {
        let pub_instance = mq.publisher(topic.to_string());
        let p_count = config.msg_count_per_producer;
        let p_batch = config.batch_size;
        let p_payload = payload_arc.clone();
        
        producer_handles.push(tokio::spawn(async move {
            if p_batch > 1 {
                let mut batch = Vec::with_capacity(p_batch);
                for _ in 0..p_count {
                    batch.push(p_payload.to_vec()); 
                    if batch.len() >= p_batch {
                        pub_instance.publish_batch(batch.clone()).await.unwrap();
                        batch.clear();
                    }
                }
                if !batch.is_empty() {
                    pub_instance.publish_batch(batch).await.unwrap();
                }
            } else {
                for _ in 0..p_count {
                    pub_instance.publish(p_payload.to_vec()).await.unwrap();
                }
            }
        }));
    }

    // Wait for producers
    for h in producer_handles {
        h.await?;
    }
    
    // Wait for consumers with global timeout
    let timeout_duration = Duration::from_secs(60);
    let mut success = true;

    let consumer_wait = async {
        for h in consumer_handles {
            h.await.unwrap();
        }
    };

    if let Err(_) = tokio::time::timeout(timeout_duration, consumer_wait).await {
        println!("!! Test Timed Out !!");
        success = false;
    }

    let duration = start_time.elapsed();
    
    // Metrics
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::everything(),
    );
    let cpu_end = sys.process(pid).map(|p| p.cpu_usage()).unwrap_or(0.0);
    let mem_end = sys.process(pid).map(|p| p.memory()).unwrap_or(0);

    let total_ops = messages_received.load(Ordering::Relaxed);
    let throughput = if duration.as_secs_f64() > 0.0 { total_ops as f64 / duration.as_secs_f64() } else { 0.0 };
    let avg_latency_us = if total_ops > 0 { (duration.as_micros() as f64) / (total_ops as f64) } else { 0.0 };

    // Get stats to check drops
    let stats = mq.get_topic_stats(topic.to_string()).await;
    let dropped = stats.map(|s| s.dropped_messages).unwrap_or(0);

    Ok(TestResult {
        config,
        duration,
        throughput,
        avg_latency_us,
        cpu_usage_start: cpu_start,
        cpu_usage_end: cpu_end,
        memory_usage_start: mem_start,
        memory_usage_end: mem_end,
        dropped_messages: dropped,
        success,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Baseline: 1 Pub, 1 Sub, Small Msg
    let base_options = TopicOptions {
        max_messages: Some(200_000), // Sufficient for 100k messages
        ..TopicOptions::default()
    };
    let base_config = TestConfig {
        name: "Baseline (1P/1C, 1KB)".to_string(),
        producers: 1,
        consumers: 1,
        msg_size: 1024,
        msg_count_per_producer: 100_000,
        batch_size: 1,
        topic_options: Some(base_options),
    };
    let res_base = run_test(base_config).await?;
    print_result(&res_base);

    // 2. Load Test: High Concurrency (10P, 10C)
    // IMPORTANT: Increase buffer size to avoid eviction during high load burst
    let load_options = TopicOptions {
        max_messages: Some(1_000_000), // Large buffer to prevent drops
        ..TopicOptions::default()
    };
    let load_config = TestConfig {
        name: "Load Test (10P/10C, 1KB)".to_string(),
        producers: 10,
        consumers: 10,
        msg_size: 1024,
        msg_count_per_producer: 10_000, // Total 100k published, 1M received
        batch_size: 1,
        topic_options: Some(load_options),
    };
    let res_load = run_test(load_config).await?;
    print_result(&res_load);

    // 3. Batching Test: High Throughput
    // Increase buffer to prevent drops during high throughput batching
    let batch_options = TopicOptions {
        max_messages: Some(2_000_000), 
        ..TopicOptions::default()
    };
    let batch_config = TestConfig {
        name: "Batching (1P/1C, 1KB, Batch 100)".to_string(),
        producers: 1,
        consumers: 1,
        msg_size: 1024,
        msg_count_per_producer: 200_000,
        batch_size: 100,
        topic_options: Some(batch_options),
    };
    let res_batch = run_test(batch_config).await?;
    print_result(&res_batch);

    // 4. Stress Test: Large Messages (1MB)
    let stress_config = TestConfig {
        name: "Stress Test (1MB Msg)".to_string(),
        producers: 1,
        consumers: 1,
        msg_size: 1024 * 1024, // 1MB
        msg_count_per_producer: 1_000,
        batch_size: 1,
        topic_options: None,
    };
    let res_stress = run_test(stress_config).await?;
    print_result(&res_stress);

    // 5. Endurance Test: Sustained Load (simulated by high count)
    // Increase buffer to prevent drops over long run
    let endurance_options = TopicOptions {
        max_messages: Some(2_000_000), 
        ..TopicOptions::default()
    };
    let endurance_config = TestConfig {
        name: "Endurance Test (High Count)".to_string(),
        producers: 2,
        consumers: 2,
        msg_size: 1024,
        msg_count_per_producer: 200_000, // Total 400k published
        batch_size: 50,
        topic_options: Some(endurance_options),
    };
    let res_endurance = run_test(endurance_config).await?;
    print_result(&res_endurance);

    Ok(())
}

fn print_result(res: &TestResult) {
    println!("Results for '{}':", res.config.name);
    println!("  Status: {}", if res.success { "SUCCESS" } else { "TIMEOUT/FAILURE" });
    println!("  Duration: {:?}", res.duration);
    println!("  Throughput: {:.2} msgs/sec", res.throughput);
    println!("  Avg Latency (Amortized): {:.2} µs/msg", res.avg_latency_us);
    println!("  CPU Usage: Start {:.1}% -> End {:.1}%", res.cpu_usage_start, res.cpu_usage_end);
    println!("  Mem Usage: Start {} MB -> End {} MB", res.memory_usage_start / 1024 / 1024, res.memory_usage_end / 1024 / 1024);
    println!("  Dropped Messages: {}", res.dropped_messages);
    if res.dropped_messages > 0 {
        println!("  WARNING: Messages were dropped! Buffer might be too small or consumers too slow.");
    }
    println!("----------------------------------------------------------------\n");
}
