use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use tokio::runtime::Runtime;
use tokio_memq::mq::MessageQueue;
use tokio_memq::MessageSubscriber;
use tokio_stream::StreamExt;

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

fn bench_concurrent(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    // We need an Arc<MessageQueue> to share across potential spawns if needed, but here we spawn with moved components.
    // MessageQueue itself:
    // pub struct MessageQueue { topic_manager: TopicManager }
    // TopicManager is Clone (Arc internally).
    // So MessageQueue should be Clone-able if we derive it? 
    // Let's assume we can't Clone MessageQueue easily without checking.
    // But `publish_concurrent` takes `&MessageQueue` and creates publishers.
    
    let mq = MessageQueue::new();
    let count = 1000;
    let concurrency = 4;

    let mut group = c.benchmark_group("Concurrent");

    group.bench_function(BenchmarkId::new("Publish_4x250", count), |b| {
        b.to_async(&rt).iter(|| async {
            let topic = format!("con_pub_{}", rand::random::<u32>());
            // 4 tasks, each 250 messages = 1000 total
            publish_concurrent(&mq, &topic, concurrency, count / concurrency).await;
        })
    });

    group.bench_function(BenchmarkId::new("Fanout_4x1000", count), |b| {
        b.to_async(&rt).iter(|| async {
            let topic = format!("con_sub_{}", rand::random::<u32>());
            // 4 subscribers, each consumes ALL 1000 messages (Fanout)
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
    let count = 1000;

    let mut group = c.benchmark_group("Consume");

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

criterion_group!(benches, bench_publish, bench_consume, bench_concurrent);
criterion_main!(benches);
