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
    let stream = subscriber.stream().take(count);
    tokio::pin!(stream);
    
    while let Some(_) = stream.next().await {}
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

criterion_group!(benches, bench_publish, bench_consume);
criterion_main!(benches);
