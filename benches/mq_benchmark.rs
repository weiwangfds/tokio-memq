use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use tokio::runtime::Runtime;
use tokio_memq::mq::MessageQueue;
use tokio_memq::MessageSubscriber;
use tokio_stream::StreamExt;
use sysinfo::{System, Pid};

fn get_memory_usage() -> u64 {
    let mut system = System::new();
    system.refresh_all();
    let pid = Pid::from_u32(std::process::id());
    if let Some(process) = system.process(pid) {
        process.memory() // Returns memory usage in KB
    } else {
        0
    }
}

async fn publish_individual(mq: &MessageQueue, topic: &str, count: usize) {
    let publisher = mq.publisher(topic.to_string());
    for i in 0..count {
        publisher.publish(&i).await.unwrap();
    }
}

async fn publish_batch(mq: &MessageQueue, topic: &str, count: usize) {
    let publisher = mq.publisher(topic.to_string());
    let messages: Vec<usize> = (0..count).collect();
    publisher.publish_batch(messages).await.unwrap();
}

async fn consume_individual(mq: &MessageQueue, topic: &str, count: usize) {
    // Ensure messages are published first
    let publisher = mq.publisher(topic.to_string());
    let msgs: Vec<usize> = (0..count).collect();
    publisher.publish_batch(msgs).await.unwrap();

    let subscriber = mq.subscriber(topic.to_string()).await.unwrap();
    // Seek to the beginning to consume pre-published messages
    subscriber.seek(0).await.unwrap();
    
    for _ in 0..count {
        let _ = subscriber.recv().await.unwrap();
    }
}

async fn consume_stream(mq: &MessageQueue, topic: &str, count: usize) {
    // Ensure messages are published first
    let publisher = mq.publisher(topic.to_string());
    let msgs: Vec<usize> = (0..count).collect();
    publisher.publish_batch(msgs).await.unwrap();

    let subscriber = mq.subscriber(topic.to_string()).await.unwrap();
    // Seek to the beginning to consume pre-published messages
    subscriber.seek(0).await.unwrap();
    
    let stream = subscriber.stream().take(count);
    tokio::pin!(stream);
    
    while let Some(_) = stream.next().await {}
}

async fn publish_concurrent(mq: &MessageQueue, topic: &str, num_tasks: usize, msgs_per_task: usize) {
    let mut handles = Vec::new();
    for _ in 0..num_tasks {
        let publisher = mq.publisher(topic.to_string());
        handles.push(tokio::spawn(async move {
            for i in 0..msgs_per_task {
                publisher.publish(&i).await.unwrap();
            }
        }));
    }
    
    for h in handles {
        h.await.unwrap();
    }
}

async fn consume_concurrent_fanout(mq: &MessageQueue, topic: &str, num_subs: usize, count: usize) {
    // Publish messages once
    let publisher = mq.publisher(topic.to_string());
    let msgs: Vec<usize> = (0..count).collect();
    publisher.publish_batch(msgs).await.unwrap();

    // Create subscribers
    let mut subscribers = Vec::new();
    for _ in 0..num_subs {
        let sub = mq.subscriber(topic.to_string()).await.unwrap();
        sub.seek(0).await.unwrap();
        subscribers.push(sub);
    }

    let mut handles = Vec::new();
    for sub in subscribers {
        handles.push(tokio::spawn(async move {
            for _ in 0..count {
                let _ = sub.recv().await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

async fn run_stream_backpressure_test(mq: MessageQueue, topic: String, count: usize) {
    // Start measuring memory
    let start_mem = get_memory_usage();
    println!("Start memory: {} KB", start_mem);

    // 1. Publish messages rapidly
    let publisher = mq.publisher(topic.clone());
    
    // Spawn publisher
    let pub_handle = tokio::spawn(async move {
        for i in 0..count {
            publisher.publish(&i).await.unwrap();
        }
    });

    // 2. Consume slowly using stream
    let mq_sub = mq.clone();
    let topic_sub = topic.clone();
    let sub_handle = tokio::spawn(async move {
        let subscriber = mq_sub.subscriber(topic_sub).await.unwrap();
        let stream = subscriber.stream(); 
        tokio::pin!(stream);

        let mut consumed = 0;
        while let Some(_) = stream.next().await {
            consumed += 1;
            if consumed % 1000 == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await; // Slow down
            }
            if consumed >= count {
                break;
            }
        }
    });

    // Monitor memory during execution
    let monitor_handle = tokio::spawn(async move {
        for _ in 0..10 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            // let current_mem = get_memory_usage();
            // println!("Current memory: {} KB", current_mem); 
        }
    });

    let _ = tokio::join!(pub_handle, sub_handle, monitor_handle);
    
    let end_mem = get_memory_usage();
    println!("End memory: {} KB, Diff: {} KB", end_mem, end_mem as i64 - start_mem as i64);
}

fn bench_concurrent(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mq = MessageQueue::new();
    let count = 1000;
    let concurrency = 4;

    let mut group = c.benchmark_group("Concurrent");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_secs(1));

    group.bench_function(BenchmarkId::new("Publish_4x250k", count), |b| {
        b.to_async(&rt).iter(|| async {
            let topic = format!("con_pub_{}", rand::random::<u32>());
            publish_concurrent(&mq, &topic, concurrency, count / concurrency).await;
        })
    });

    group.bench_function(BenchmarkId::new("Fanout_4x1M", count), |b| {
        b.to_async(&rt).iter(|| async {
            let topic = format!("con_sub_{}", rand::random::<u32>());
            consume_concurrent_fanout(&mq, &topic, concurrency, count).await;
        })
    });

    group.finish();
}

fn bench_publish(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mq = MessageQueue::new();
    let count = 1000;

    let mut group = c.benchmark_group("Publish");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_secs(1));
    
    group.bench_function(BenchmarkId::new("Individual", count), |b| {
        b.to_async(&rt).iter(|| async {
            let topic = format!("pub_ind_{}", rand::random::<u32>());
            publish_individual(&mq, &topic, count).await;
        })
    });

    group.bench_function(BenchmarkId::new("Batch", count), |b| {
        b.to_async(&rt).iter(|| async {
            let topic = format!("pub_batch_{}", rand::random::<u32>());
            publish_batch(&mq, &topic, count).await;
        })
    });
    
    group.finish();
}

fn bench_consume(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mq = MessageQueue::new();
    let count = 1_000;

    let mut group = c.benchmark_group("Consume");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_secs(1));

    group.bench_function(BenchmarkId::new("Individual", count), |b| {
        b.to_async(&rt).iter(|| async {
            let topic = format!("sub_ind_{}", rand::random::<u32>());
            consume_individual(&mq, &topic, count).await;
        })
    });

    group.bench_function(BenchmarkId::new("Stream", count), |b| {
        b.to_async(&rt).iter(|| async {
            let topic = format!("sub_stream_{}", rand::random::<u32>());
            consume_stream(&mq, &topic, count).await;
        })
    });

    group.finish();
}

fn bench_backpressure(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mq = MessageQueue::new();
    let count = 1000; // Reduce count for repetitive benchmark

    let mut group = c.benchmark_group("Backpressure");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_secs(1));

    group.bench_function(BenchmarkId::new("Stream_SlowConsumer", count), |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let mq = mq.clone();
            async move {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let topic = format!("bp_stream_{}", rand::random::<u32>());
                    run_stream_backpressure_test(mq.clone(), topic, count).await;
                }
                start.elapsed()
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_publish, bench_consume, bench_concurrent, bench_backpressure);
criterion_main!(benches);
