use std::hint::black_box;

use crc::{CRC_32_ISO_HDLC, Crc, Table};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const BLOCK_LEN: usize = 8 * 1024;

fn crc_implementations(c: &mut Criterion) {
    let block: Vec<u8> = (0..BLOCK_LEN).map(|i| (i * 31 % 251) as u8).collect();
    let bytewise = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    let slice16 = Crc::<u32, Table<16>>::new(&CRC_32_ISO_HDLC);
    assert_eq!(bytewise.checksum(&block), slice16.checksum(&block));

    let mut group = c.benchmark_group("crc32");
    group.throughput(Throughput::Bytes(BLOCK_LEN as u64));
    group.bench_function("bytewise", |b| {
        b.iter(|| bytewise.checksum(black_box(&block)))
    });
    group.bench_function("slice16", |b| {
        b.iter(|| slice16.checksum(black_box(&block)))
    });
    group.finish();
}

criterion_group!(benches, crc_implementations);
criterion_main!(benches);
