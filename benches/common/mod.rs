use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use database_engine::{DbError, KVEngine, KVEngineOptions, SyncConfig};
use hdrhistogram::Histogram;
pub const KEY_LEN: usize = 16;
pub const VALUE_LEN: usize = 100;
pub fn bench_options(sync: SyncConfig) -> KVEngineOptions {
    KVEngineOptions {
        sync,
        memtable_threshold: 1024 * 1024,
        max_sst_size: 1024 * 1024,
        max_bytes_for_level_base: 4 * 1024 * 1024,
        level_multiplier: 4,
        l0_compaction_trigger: 4,
        ..KVEngineOptions::default()
    }
}

pub fn mix(n: u64) -> u64 {
    let mut x = n.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

pub fn random_key(n: u64) -> [u8; KEY_LEN] {
    let mut key: [u8; KEY_LEN] = [0u8; KEY_LEN];

    key[..8].copy_from_slice(&mix(n).to_be_bytes());
    key[8..].copy_from_slice(&n.to_be_bytes());
    key
}

pub fn put_through_stalls(db: &mut KVEngine, key: &[u8], value: &[u8]) -> u64 {
    let mut stalls = 0;
    loop {
        match db.put(key, value) {
            Ok(()) => return stalls,
            Err(DbError::WritesStalled { .. }) => stalls += 1,
            Err(e) => panic!("put failed: {e}"),
        }
    }
}

pub struct Latencies {
    histogram: Histogram<u64>,
    total: Duration,
}

impl Latencies {
    pub fn new() -> Self {
        Self {
            histogram: Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap(), // 60 second upper bound,
            total: Duration::ZERO,
        }
    }
    pub fn time_op<T>(&mut self, op: impl FnOnce() -> T) -> Duration {
        let start = Instant::now();
        black_box(op());
        let elapsed = start.elapsed();
        self.record(elapsed);
        elapsed
    }

    pub fn record(&mut self, elapsed: Duration) {
        self.histogram.saturating_record(elapsed.as_nanos() as u64);
        self.total += elapsed;
    }

    pub fn print_data(&self, op_name: &str, extra_info: &str) {
        let hist = &self.histogram;
        if hist.is_empty() {
            return;
        }

        let ops_per_sec = hist.len() as f64 / self.total.as_secs_f64();
        println!(
            "{op_name} n={} ops/s={} mean={} p50={} p90={} p99={} p99.9={} p99.99={} max={}. {extra_info}",
            hist.len(),
            ops_per_sec,
            format_ns(hist.mean() as u64),
            format_ns(hist.value_at_quantile(0.50)),
            format_ns(hist.value_at_quantile(0.90)),
            format_ns(hist.value_at_quantile(0.99)),
            format_ns(hist.value_at_quantile(0.999)),
            format_ns(hist.value_at_quantile(0.9999)),
            format_ns(hist.max()),
        );
    }
}

fn format_ns(ns: u64) -> String {
    match ns {
        0..1_000 => format!("{ns}ns"),
        1_000..1_000_000 => format!("{:.2}µs", ns as f64 / 1e3),
        1_000_000..1_000_000_000 => format!("{:.2}ms", ns as f64 / 1e6),
        _ => format!("{:.2}s", ns as f64 / 1e9),
    }
}
