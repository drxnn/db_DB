mod common;

use common::{Latencies, VALUE_LEN, bench_options, put_through_stalls, random_key};

use database_engine::KVEngine;

use tempfile::tempdir;

const BENCHMARK_RECORDS_NUM: u64 = 500_000;
const BENCHMARK_MEMTABLE_RECORDS: u64 = 5_000;

struct TestingEngine {
    db: KVEngine,
    keys_by_location: u64, // placeholder this will be a vector of keys by location(memtable,L0-L3) // also keys that dont exist response time
}

impl TestingEngine {
    fn build_engine_for_benchmarks() {
        let dir = tempdir().unwrap();
        // todo: build an environemt that tests reads
        // this means we will write about 60 MB of data in a running instance of the engine
        // after we have done this, we will test the time it takes to get a key in each level by using Latencies.time()
        // after we populate the SSTable of the engine, we should run necessary compactions until the data tree is static.
        // after this we close and reopen the engine and populate the memtable with 5K records.(to test get time with fresh records).
        // then we need use the locate function to order keys by location.
        // then we test the time it takes to get keys for all locations for all the keys in those locations
        // Each location will run its own Latencies instance for this

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
    }
}
