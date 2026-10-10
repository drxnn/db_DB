mod common;

use std::{thread::sleep, time::Duration};

use common::{Latencies, VALUE_LEN, bench_options, put_through_stalls, random_key};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use database_engine::{KVEngine, KeyLocation};

use tempfile::{TempDir, tempdir};

use crate::common::KEY_LEN;

const BENCHMARK_RECORDS_NUM: u64 = 500_000;
const BENCHMARK_MEMTABLE_RECORDS: u64 = 5_000;
const BENCHMARK_L0_RECORDS_NUM: u64 = 5_000;

struct TestingEngine {
    db: KVEngine,
    keys_by_location: Vec<(&'static str, Vec<[u8; KEY_LEN]>)>,
    _dir: TempDir,
}

impl TestingEngine {
    fn build_engine_for_benchmarks() -> Self {
        let dir = tempdir().unwrap();

        let value = [b'v'; VALUE_LEN];
        let mut written: u64 = 0;
        let mut put = |db: &mut KVEngine| {
            put_through_stalls(db, &random_key(written), &value);
            written += 1;
        };
        let mut db =
            KVEngine::open(dir.path(), bench_options(database_engine::SyncConfig::None)).unwrap();

        for _ in 0..BENCHMARK_RECORDS_NUM {
            put(&mut db)
        }

        while !db.is_idle() {
            db.maintenance().unwrap();
            sleep(Duration::from_millis(20));
        }

        for _ in
            0..BENCHMARK_L0_RECORDS_NUM.saturating_sub(db.stats().active_memtable_number_of_records)
        {
            // close will put these 5K records into L0
            put(&mut db)
        }
        db.close().unwrap();

        let mut db =
            KVEngine::open(dir.path(), bench_options(database_engine::SyncConfig::None)).unwrap();
        for _ in 0..BENCHMARK_MEMTABLE_RECORDS {
            put(&mut db)
        }

        let mut keys_by_location: Vec<(&'static str, Vec<[u8; KEY_LEN]>)> = Vec::new();
        let mut groups = [
            ("memtable", KeyLocation::Memtable, Vec::new()),
            ("L0", KeyLocation::Level(0), Vec::new()),
            ("L1", KeyLocation::Level(1), Vec::new()),
            ("L2", KeyLocation::Level(2), Vec::new()),
            ("L3", KeyLocation::Level(3), Vec::new()),
        ];

        for n in 0..written {
            let k = random_key(n);
            let location = db.locate(k.as_slice()).unwrap().unwrap();

            let (_, _, group) = groups.iter_mut().find(|(_, l, _)| *l == location).unwrap();
            group.push(k);
        }

        for (name, _, keys) in groups {
            keys_by_location.push((name, keys));
        }
        Self {
            db,
            keys_by_location,
            _dir: dir,
        }
    }
}

fn get_by_location(c: &mut Criterion) {
    let engine = TestingEngine::build_engine_for_benchmarks();

    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1));
    for (name, keys) in &engine.keys_by_location {
        let mut latencies = Latencies::new();
        let mut next_key = keys.iter().cycle();
        group.bench_function(*name, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let key = next_key.next().unwrap();
                    total += latencies.time_op(|| engine.db.get(key).unwrap());
                }
                total
            })
        });
        latencies.print_data(&format!("get/{name}"), "");
    }
    group.finish();

    engine.db.close().unwrap();
}

criterion_group!(benches, get_by_location);
criterion_main!(benches);
