mod common;

use std::thread::sleep;
use std::time::Duration;

use common::{KEY_LEN, Latencies, VALUE_LEN, bench_options, put_through_stalls, random_key};
use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use database_engine::{KVEngine, SyncConfig};
use tempfile::tempdir;

use crate::common::mix;

const FILL_RECORDS: u64 = 200_000;

const FILL_LARGE_VALUE_RECORDS: u64 = 10_000;

fn bench_fill(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    records: u64,
    key: fn(u64) -> [u8; KEY_LEN],
    value_len: usize,
) {
    let value = vec![b'v'; value_len];
    let mut latencies = Latencies::new();
    let mut stalls = 0;
    let mut loads = 0;

    group.throughput(Throughput::Elements(records));
    group.bench_function(name, |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let dir = tempdir().unwrap();
                let mut db = KVEngine::open(dir.path(), bench_options(SyncConfig::None)).unwrap();
                for n in 0..records {
                    let key = key(n);
                    total +=
                        latencies.time_op(|| stalls += put_through_stalls(&mut db, &key, &value));
                }

                db.close().unwrap();
                loads += 1;
            }
            total
        })
    });
    latencies.print_data(
        &format!("fill/{name}"),
        &format!("stalls={stalls} loads={loads}"),
    );
}

fn fill(c: &mut Criterion) {
    let mut group = c.benchmark_group("fill");

    group
        .sample_size(10)
        .sampling_mode(SamplingMode::Flat)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(15));

    bench_fill(&mut group, "random", FILL_RECORDS, random_key, VALUE_LEN);
    bench_fill(
        &mut group,
        "random_4KiB_values",
        FILL_LARGE_VALUE_RECORDS,
        random_key,
        4096,
    );
    group.finish();
}

const MIXED_RECORDS: u64 = 200_000;

fn mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed");
    group.throughput(Throughput::Elements(1));
    let value = [b'v'; VALUE_LEN];

    for (name, read_percent) in [("read95_write5", 95), ("read50_write50", 50)] {
        let dir = tempdir().unwrap();
        let mut db = KVEngine::open(dir.path(), bench_options(SyncConfig::None)).unwrap();
        for n in 0..MIXED_RECORDS {
            put_through_stalls(&mut db, &random_key(n), &value);
        }
        while !db.is_idle() {
            db.maintenance().unwrap();
            sleep(Duration::from_millis(20));
        }

        let mut all = Latencies::new();
        let mut gets = Latencies::new();
        let mut puts = Latencies::new();
        let mut stalls = 0;
        let mut op: u64 = 0;
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let r = mix(op);
                    op += 1;
                    // low bits pick the key, high bits pick get or put
                    let key = random_key(r % MIXED_RECORDS);
                    let elapsed = if (r >> 32) % 100 < read_percent {
                        gets.time_op(|| db.get(&key).unwrap())
                    } else {
                        puts.time_op(|| stalls += put_through_stalls(&mut db, &key, &value))
                    };
                    all.record(elapsed);
                    total += elapsed;
                }
                total
            })
        });

        all.print_data(&format!("mixed/{name}"), &format!("stalls={stalls}"));
        gets.print_data(&format!("mixed/{name}/get"), "");
        puts.print_data(&format!("mixed/{name}/put"), "");
        db.close().unwrap();
    }
    group.finish();
}

criterion_group!(benches, fill, mixed);
criterion_main!(benches);
