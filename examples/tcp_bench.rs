use tokio_memq::mq::MessageQueue;
use tokio_memq::mq::ConsumptionMode;
use tokio::time::{sleep, Duration, Instant};
use tokio::task::JoinSet;
use tokio::sync::mpsc;
use serde::{Serialize, Deserialize};
use log::{info, warn, debug};
use std::sync::Arc;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio_memq::tcp::server::{TcpServer, TcpServerOptions};
use tokio_memq::tcp::client::{TcpClient, TcpClientOptions};
use tokio_memq::tcp::protocol::PayloadFormat;

#[derive(Serialize, Deserialize, Debug)]
struct BenchPayload {
    i: u64,
    client: u32,
    ts_ms: u128,
}

fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let mq = MessageQueue::new();
    let opts = TcpServerOptions::default();
    let server = TcpServer::new(mq.clone(), opts.clone());
    tokio::spawn(async move {
        server.start().await.unwrap();
    });
    sleep(Duration::from_millis(200)).await;

    let topic = "tcp_bench_topic";
    let publishers: usize = std::env::var("PUBLISHERS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let messages_per_pub: usize = std::env::var("MESSAGES").ok().and_then(|v| v.parse().ok()).unwrap_or(100);
    let format_str = std::env::var("FORMAT").unwrap_or_else(|_| "json".to_string());
    let use_format = match format_str.as_str() {
        "bincode" => PayloadFormat::Bincode,
        "msgpack" => PayloadFormat::MessagePack,
        _ => PayloadFormat::Json,
    };
    let total_msgs = (publishers * messages_per_pub) as u64;
    info!("bench start addr={} topic={} publishers={} messages_per_pub={} format={:?}", opts.listen_addr, topic, publishers, messages_per_pub, use_format);

    let mut sub = TcpClient::new(TcpClientOptions {
        server_addr: opts.listen_addr,
        auth_token: None,
        reconnect: true,
        heartbeat_interval: Duration::from_secs(5),
    });
    sub.connect().await?;
    info!("subscriber connected addr={}", opts.listen_addr);
    sub.subscribe(topic, None, Some(ConsumptionMode::Earliest)).await?;
    info!("subscriber subscribed topic={}", topic);

    let (lat_tx, mut lat_rx) = mpsc::channel::<u128>(1024);
    let (done_tx, mut done_rx) = mpsc::channel::<()>(1);

    let consumer_handle = tokio::spawn(async move {
        let mut received: u64 = 0;
        let progress_every: u64 = std::env::var("PROGRESS").ok().and_then(|v| v.parse().ok()).unwrap_or(500);
        let lat_interval_ms: u64 = std::env::var("LAT_INTERVAL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
        let lat_window_size: usize = std::env::var("LAT_WINDOW").ok().and_then(|v| v.parse().ok()).unwrap_or(2000);
        let mut lat_window: VecDeque<u128> = VecDeque::with_capacity(lat_window_size);
        let mut last_report = Instant::now();
        while received < total_msgs {
            let (meta, body) = sub.read_message().await.unwrap();
            let payload: BenchPayload = match meta.format.unwrap_or(PayloadFormat::Json) {
                PayloadFormat::Json => serde_json::from_slice(&body).unwrap(),
                PayloadFormat::MessagePack => rmp_serde::from_slice(&body).unwrap(),
                PayloadFormat::Bincode => bincode::deserialize(&body).unwrap(),
            };
            let latency = now_ms().saturating_sub(payload.ts_ms);
            let _ = lat_tx.try_send(latency);
            if lat_window.len() == lat_window_size { let _ = lat_window.pop_front(); }
            lat_window.push_back(latency);
            debug!("subscriber recv seq={} client={} latency={}ms", payload.i, payload.client, latency);
            received += 1;
            if received % progress_every == 0 {
                info!("subscriber progress received={}/{}", received, total_msgs);
            }
            if last_report.elapsed() >= Duration::from_millis(lat_interval_ms) {
                let mut buf: Vec<u128> = lat_window.iter().cloned().collect();
                buf.sort_unstable();
                let count = buf.len();
                let p50 = buf.get((count as f64 * 0.5) as usize).cloned().unwrap_or(0);
                let p90 = buf.get((count as f64 * 0.9) as usize).cloned().unwrap_or(0);
                let p99 = buf.get((count as f64 * 0.99) as usize).cloned().unwrap_or(0);
                let avg = if count > 0 { (buf.iter().sum::<u128>() as f64) / (count as f64) } else { 0.0 };
                info!("latency window count={} avg={:.2}ms p50/p90/p99={} / {} / {} ms", count, avg, p50, p90, p99);
                last_report = Instant::now();
            }
        }
        let _ = done_tx.send(()).await;
        info!("subscriber completed total_received={}", received);
    });

    let start = Instant::now();
    let mut set = JoinSet::new();
    let addr = opts.listen_addr;
    for client_id in 0..publishers {
        set.spawn(async move {
            let buf_size: usize = std::env::var("PUB_BUFFER").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
            let report_ms: u64 = std::env::var("PUB_REPORT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
            let (tx, mut rx) = mpsc::channel::<Vec<u8>>(buf_size);
            let depth = Arc::new(AtomicUsize::new(0));
            let d1 = depth.clone();
            let prod = tokio::spawn(async move {
                let mut last = Instant::now();
                for i in 0..messages_per_pub {
                    let payload = BenchPayload { i: i as u64, client: client_id as u32, ts_ms: now_ms() };
                    let bytes = match use_format {
                        PayloadFormat::Json => serde_json::to_vec(&payload).unwrap(),
                        PayloadFormat::MessagePack => rmp_serde::to_vec(&payload).unwrap(),
                        PayloadFormat::Bincode => bincode::serialize(&payload).unwrap(),
                    };
                    d1.fetch_add(1, Ordering::SeqCst);
                    let _ = tx.send(bytes).await;
                    if last.elapsed() >= Duration::from_millis(report_ms) {
                        info!("publisher queue depth id={} depth={}", client_id, d1.load(Ordering::SeqCst));
                        last = Instant::now();
                    }
                    if i % 100 == 0 {
                        debug!("publisher progress id={} sent={}/{}", client_id, i, messages_per_pub);
                    }
                }
            });
            let mut client = TcpClient::new(TcpClientOptions {
                server_addr: addr,
                auth_token: None,
                reconnect: true,
                heartbeat_interval: Duration::from_secs(5),
            });
            client.connect().await.unwrap();
            info!("publisher connected id={} addr={}", client_id, addr);
            let cons = tokio::spawn(async move {
                while let Some(bytes) = rx.recv().await {
                    let before = Instant::now();
                    client.publish(topic, &bytes, use_format).await.unwrap();
                    let elapsed = before.elapsed();
                    depth.fetch_sub(1, Ordering::SeqCst);
                    debug!("publisher write id={} write_ms={:.3}", client_id, elapsed.as_secs_f64() * 1000.0);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                client.disconnect().await.unwrap();
                info!("publisher finished id={} sent={}", client_id, messages_per_pub);
            });
            let _ = prod.await;
            let _ = cons.await;
        });
    }

    while let Some(res) = set.join_next().await {
        res.unwrap();
    }
    info!("all publishers completed");

    let bench_timeout_ms: u64 = std::env::var("BENCH_TIMEOUT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(30000);
    match tokio::time::timeout(Duration::from_millis(bench_timeout_ms), done_rx.recv()).await {
        Ok(_) => {
            info!("subscriber signaled completion");
        }
        Err(_) => {
            warn!("bench timed out waiting for subscriber, aborting consumer task after {} ms", bench_timeout_ms);
            consumer_handle.abort();
        }
    }
    let elapsed = start.elapsed();
    let _ = consumer_handle.await;

    let mut count: u64 = 0;
    let mut sum_latency: u128 = 0;
    let mut latencies: Vec<u128> = Vec::with_capacity(total_msgs as usize);
    while let Ok(lat) = lat_rx.try_recv() {
        latencies.push(lat);
        sum_latency += lat;
        count += 1;
    }
    latencies.sort_unstable();
    let p50 = latencies.get((count as f64 * 0.5) as usize).cloned().unwrap_or(0);
    let p90 = latencies.get((count as f64 * 0.9) as usize).cloned().unwrap_or(0);
    let p99 = latencies.get((count as f64 * 0.99) as usize).cloned().unwrap_or(0);

    let qps = (total_msgs as f64) / elapsed.as_secs_f64();
    println!("TCP Bench Results:");
    println!("  Publishers: {}", publishers);
    println!("  Messages per publisher: {}", messages_per_pub);
    println!("  Total messages: {}", total_msgs);
    println!("  Elapsed: {:.3}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.0} msgs/sec", qps);
    if count > 0 {
        println!("  Latency avg: {:.2} ms", (sum_latency as f64) / (count as f64));
        println!("  Latency p50/p90/p99: {} / {} / {} ms", p50, p90, p99);
    } else {
        println!("  No latency samples collected");
    }

    Ok(())
}
