#[allow(dead_code)]
mod common;

use std::thread::sleep;
use std::time::{Duration, Instant};

use common::{KEY_LEN, Latencies, VALUE_LEN, put_through_stalls, random_key};
use database_engine::{KVEngine, KVEngineOptions, SyncConfig};

const RECORD_BYTES: u64 = (KEY_LEN + VALUE_LEN + 25) as u64;
const REPORT_EVERY: Duration = Duration::from_secs(5);

fn options() -> KVEngineOptions {
    KVEngineOptions {
        memtable_threshold: 64 * 1024 * 1024,
        max_sst_size: 100 * 1024 * 1024,
        sync: SyncConfig::None,
        ..KVEngineOptions::default()
    }
}

fn levels(db: &KVEngine) -> String {
    let stats = db.stats();
    let files: Vec<String> = stats
        .files_per_level
        .iter()
        .enumerate()
        .map(|(level, files)| format!("L{level} {files:>3}"))
        .collect();
    let mib = stats.bytes_per_level.iter().sum::<u64>() as f64 / (1024.0 * 1024.0);
    format!(
        "{}   {mib:>6.0} MiB in SSTs   {} frozen",
        files.join("  "),
        stats.frozen_memtables
    )
}

fn main() {
    if !std::env::args().any(|arg| arg == "--bench") {
        println!("large_write skipped: run it with `cargo bench --bench large_write`");
        return;
    }

    let gb: f64 = std::env::var("GB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2.0);

    let dir = tempfile::tempdir().unwrap();
    let mut db = KVEngine::open(dir.path(), options()).unwrap();

    let num_records = (gb * (1_u64 << 30) as f64 / RECORD_BYTES as f64) as u64;

    let mut latencies = Latencies::new();
    let mut stalls = 0;
    let start = Instant::now();
    let mut last_report = start;
    let value = [b'v'; VALUE_LEN];
    let mut num_of_records_written_in_last_report: u64 = 0;

    for n in 0..num_records {
        let k = random_key(n);

        latencies.time_op(|| stalls += put_through_stalls(&mut db, &k, &value));
        if last_report.elapsed() >= REPORT_EVERY {
            // print a format like: wrote (num of records) in (num of seconds), (num_of_MiB/per )
            let ops_per_sec = (n - num_of_records_written_in_last_report) as f64
                / last_report.elapsed().as_secs_f64();

            println!(
                "{:>5.0}s {:>11} written {:>8.0} ops/s   {}   stalls {stalls}",
                start.elapsed().as_secs_f64(),
                n + 1,
                ops_per_sec,
                levels(&db)
            );

            last_report = Instant::now();
            num_of_records_written_in_last_report = n + 1;
        }
    }
    let load_time = start.elapsed().as_secs_f64();
    println!(
        "\nwrote {num_records} records in {load_time:.1}s: {:.0} ops/s on average, {stalls} stalls",
        num_records as f64 / load_time
    );
    latencies.print_data("put", "");

    let settle_start = Instant::now();
    while !db.is_idle() {
        db.maintenance().unwrap();
        sleep(Duration::from_millis(20));
    }
    println!(
        "background work finished {:.1}s after the last put: {}",
        settle_start.elapsed().as_secs_f64(),
        levels(&db)
    );
    db.close().unwrap();
}
