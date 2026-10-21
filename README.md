# db_DB

A crash-safe K/V storage engine. It uses an AVL tree as a memtable, has a WAL for durability (configurable) and a Manifest for atomic operations regarding compaction or flushing. A Hybrid Logical Clock is used for versioning and ordering data.

- [Demo](#demo)
- [Quick Start](#quick-start)
- [How it works](#how-it-works)
- [Hybrid Logical Clock](#hybrid-logical-clock)
- [SSTables](#sstables)
- [Levels](#levels)
- [Compaction](#compaction)
- [WAL](#wal)
- [Manifest](#manifest)
- [Data integrity](#data-integrity-and-failure-handling)
- [Tests](#tests)
- [Benchmarks](#benchmarks)
- [Limitations](#limitations)

## Demo

![demo](docs/demo.gif)

`python3 scripts/demo.py` runs this demo against the release build, in a throwaway directory: a write load that reaches L3, every key checked, recovery after kill -9, a flipped bit and a torn WAL caught, and tombstones dropped at the bottom level. `PAUSE=1` waits for Enter between steps.

The demo uses small sizes for the config (1 MiB memtable and SSTs, 4 MiB L1, the same as the benchmarks) so the data reaches every level very fast. Every memtable rotation, flush and compaction has fixed costs (mostly fsyncs), so throughput rises with bigger sizes: the same load runs at ~240k writes/s with 4 MiB files instead of ~100k.

## Quick Start

```bash
cargo run --release -- --dir data
```

This will run a REPL where you can interact with the engine.

To see all the arguments the engine can be started with, run:

```bash
cargo run --release -- --help
```

REPL commands:

- `put <key> <value>` (the value is the rest of the line and may contain spaces)
- `get <key>`
- `del <key>` or `delete <key>`
- `stats` (current state: number of SSTs, number of records etc.)
- `exit` or `quit`

The `>` prompt is only printed when stdin is a terminal, so commands can also be piped in.

## How it works

All writes go to the WAL first and then to the in-memory memtable. When the memtable is full it is frozen (made read-only), a new memtable and WAL take its place, and a background thread writes the frozen memtable to disk as a sorted SSTable in L0. Compaction later merges SSTs down through L1, L2 and L3, keeping only the newest version of each key. The manifest records every change to the set of live SSTs, so the engine can always rebuild a consistent view of its files after a crash.

## Hybrid Logical Clock

Every record and every file is stamped with a 64-bit HLC value.

Format:

```text
 63                                             12 11            0
┌─────────────────────────────────────────────────┬───────────────┐
│ physical time: microseconds since UNIX epoch    │ logical       │
│ (52 bits)                                       │ counter (12)  │
└─────────────────────────────────────────────────┴───────────────┘
```

The HLC is used to record timestamps, as an id for files (.wal | .sst), or an id for the frozen memtables. The HLC is used to determine recency in records/files/frozen memtables. It is also the structure that would be used if the system uses replicas in the future.

## SSTables

The Key/Value records are ordered lexicographically in Sorted String Tables.

SSTable format:

```text
┌──────────────────────────────┐ offset 0
│ data block 0                 │  records + CRC32
│ data block 1                 │
│ ...                          │
│ data block N-1               │
├──────────────────────────────┤ sparse_index_offset
│ sparse index                 │  sparse_index_len bytes
│ bloom filter                 │  bloom_len bytes
│ min key                      │  min_key_len bytes
│ max key                      │  max_key_len bytes
├──────────────────────────────┤ file_len - 57
│ footer (41 bytes)            │  offsets, lengths, level
│ 4 CRC32s (16 bytes)          │  sparse index, bloom, min/max, footer
└──────────────────────────────┘ EOF
```

Data Block format:

```text
┌──────────┬──────────┬─────┬──────────┬────────────────────────┐
│ record 0 │ record 1 │ ... │ record n │ CRC32 of the records   │
└──────────┴──────────┴─────┴──────────┴────────────────────────┘
```

where Record has format:

```text
[ timestamp (HLC, u64) | key_len (u64) | value_len (u64) | tombstone (u8) | key | value ]
```

The SSTables' metadata is loaded in memory and have three optimizations that make lookups faster / avoid seeking to disk:

1. Holds a min/max key of the file in the metadata. If the `key_we_are_looking_for` falls out of this range, we skip the SST.
2. Has a Bloom Filter structure that can answer whether the `key_we_are_looking_for` is definitely not in the file. Has a false positive rate of about 0.8% (default setting, 10 bits per key).
3. Has a Sparse Index structure that holds metadata about the data blocks and their offsets. We can binary search the sparse index to identify the data block that `key_we_are_looking_for` would fall into, then we would read only that one block to locate the key.

## Levels

The SSTables are ordered in 4 levels (L0-L3). Records in L0 are from recently flushed memtables and they may overlap. L1-L3 have no overlap between keys.

Each level contains more data than the previous one, with the last level also dropping deleted records during compaction.

The no overlapping keys between L1-L3 files invariant is taken care of by the compaction procedure. When compacting we pick a file from Ln and find every file in Ln+1 that overlaps with it to merge it into a single sorted stream.

## Compaction

Compaction runs on one background thread, one job at a time. It's considered whenever a flush lands in L0 or a compaction finishes; both are committed in `maintenance()`, which runs at the start of every `put` and `delete`.

**Choosing a level.** Each level gets a score, and the level with the highest score of at least 1 is compacted into the next one:

```text
L0   files in L0 / l0_compaction_trigger                          (default 10 files)
L1   bytes in L1 / max_bytes_for_level_base                       (default 1000 MiB)
L2   bytes in L2 / (max_bytes_for_level_base × level_multiplier)  (default × 10)
L3   bottom level, never compacted further
```

**Choosing files.**

- L0 -> L1: all L0 files, since they can overlap each other, plus every L1 file that overlaps their combined key range.
- L1 -> L2 and L2 -> L3: one file from the level, plus every file in the next level that overlaps it. The chosen file is the one with the fewest overlapping bytes per byte of its own, so the least data has to be rewritten(Less write amplification).

**Merging.** The inputs are merged into one sorted stream using a min-heap. When several files hold the same key, only the newest version (highest HLC) is written. The output is split into new SSTs of up to `max_sst_size`. When the output level is L3, tombstones are dropped together with the older values they delete. A corrupt input block aborts the job (see [Read-only mode](#read-only-mode)).

**Committing.** The new SSTs and the removal of the inputs go into the MANIFEST as one edit, ensuring state consistency. Inputs are deleted only after the commit, and any SST the MANIFEST doesn't list is removed on the next open.

**When compaction falls behind**, writes slow down instead of letting L0 grow without limit. Once L0 reaches `l0_stop_writes_trigger` files (default 30), or `max_frozen_memtables` memtables (default 4) are waiting to be flushed, a write waits up to `stalled_writes_retry_max_time_in_ms` (default 512 ms) and then fails with `DbError::WritesStalled`.

## WAL

The write-ahead log ensures operations are recorded on disk before being acknowledged to the caller (although this can be configured to not sync to disk). If a crash happens, we can retrieve every acknowledged write back to memory.

```text
              put / delete
                   │
        1. append  ▼
   ┌──────────────────────────┐
   │ WAL   <hlc>.wal          │
   └──────────────────────────┘
        2. insert  ▼
   ┌──────────────────────────┐
   │ active memtable (AVL)    │
   └──────────────────────────┘
```

```text
[ tag: 0x04 (TAG_INSERTION) | timestamp (HLC, u64) | key_len (u64) | value_len (u64) | key | value | crc ]
[ tag: 0x02 (TAG_DELETION) | timestamp (HLC, u64) | key_len (u64) | key | crc ]
```

Configuration:

```text
--sync always|none|every:<ms>
--wal-recovery-mode point-in-time|absolute-consistency
```

## Manifest

The MANIFEST is the single source of truth for which SSTs are live, at which level, and which WALs still have to be replayed. It is an append-only log of edits. Replaying all of them gives the current state:

```
levels: [BTreeSet<sst_id>; 4]   // live SSTs per level
min_live_wal: u64               // WALs with a smaller id are already in SSTs
```

MANIFEST format:

```text
┌───────────────────┬────────────────────────────────┬──────────────────────┐
│ payload_len (u64) │ payload: a sequence of entries │ CRC32 of the payload │
└───────────────────┴────────────────────────────────┴──────────────────────┘
```

entries:

```text
ADD_FILE      0x01 │ level (u8) │ sst_id (u64)     10 bytes
DELETE_FILE   0x02 │ level (u8) │ sst_id (u64)     10 bytes
MIN_LIVE_WAL  0x03 │ wal_id (u64)                   9 bytes
```

## Data integrity and failure handling

Every byte the engine reads back from disk is covered by a CRC32:

```text
each WAL record ----------------------------------------------------- checked on replay
each SST data block ------------------------------------------------- checked on every lookup, and on every block that compaction reads
SST sparse index, bloom filter, min/max keys, footer ---------------- checked when the SST is loaded
each manifest record ------------------------------------------------ checked on replay
```

Length fields read from disk are checked against their maximums before anything is allocated or sliced, and decoding goes through bounds-checked helpers (`read_range`, `read_u64`, …).

### Read-only mode

Some failures mean the engine can no longer make writes durable or publish background work safely:

- a WAL append or fsync fails;
- a flush still fails after `max_flush_retries` retries;
- a manifest commit fails;
- a compaction hits a corrupt input file.

When one of these happens, the engine stores the reason and disables writes. After that, `put`, `delete` and `maintenance` return `DbError::ReadOnly(reason)`, while `get` keeps working.

## Tests

```bash
cargo test
```

**Unit tests** (inside each module) check every component on its own, down to single bytes on disk:

- **Memtable:** puts, overwrites, deletes and lookups, plus AVL balance and ordering after 1,000 ascending, descending and zigzag inserts.
- **WAL:** write/replay round trips, and replay stopping at the right record after a flipped bit, an absurd length field, or a last record cut off at any byte.
- **SSTables:** every key's latest state after a flush, sparse index and bloom/min-max lookups, and a flipped bit in each checksummed section (footer or sparse index: load fails, bloom filter: dropped, min/max keys: rebuilt, data block: read fails).
- **Compaction:** the newest version of each key wins, output is split into non-overlapping files, tombstones are dropped at the bottom level, and a corrupt input aborts the job.
- **HLC:** To be added.
- **MANIFEST:** To be added.

**Integration tests** (`tests/`) go through the public `KVEngine` API:

- put/get/overwrite/delete, oversized writes and the directory lock.
- A crash test that runs the real binary with tiny demo sizes and `--sync none`, streams writes and deletes into it, and kills it with `SIGKILL` at three different points. It then reopens the directory and checks that every acknowledged write survived.

## Benchmarks

```bash
cargo bench --bench reads
cargo bench --bench writes
cargo bench --bench crc
```

Measured with criterion, plus an HDR histogram of every operation for the percentiles, on an Apple M4 Pro: a single thread, 16-byte keys, 100-byte values, WAL sync off (`SyncConfig::None`), data in the OS page cache. The engine sizes are scaled down (1 MiB memtable and SSTs, 4 MiB L1) so the data reaches every level (see `benches/common/mod.rs`).

**reads:** 500k records (~70 MB) are loaded into L0–L3, then `get` is timed separately for keys found in each place:

| found in | throughput  | p50     | p99     |
| -------- | ----------- | ------- | ------- |
| memtable | 6.74M ops/s | 125 ns  | 209 ns  |
| L0       | 378k ops/s  | 2.58 µs | 3.12 µs |
| L1       | 365k ops/s  | 2.62 µs | 5.04 µs |
| L2       | 344k ops/s  | 2.75 µs | 5.54 µs |
| L3       | 296k ops/s  | 3.21 µs | 6.46 µs |

Every level costs about the same: the min/max keys and bloom filters skip almost every SST that can't hold the key, so a `get` reads about one data block wherever it ends.

**writes:** `fill` writes random keys into an empty engine while flushes and compactions run, and `mixed` runs gets and overwrites on a preloaded engine:

| workload                        | throughput | p50     | p99     |
| ------------------------------- | ---------- | ------- | ------- |
| fill, 200k puts of 100 B values | 131k ops/s | 1.92 µs | 6.88 µs |
| fill, 10k puts of 4 KiB values  | 8k ops/s   | 15.7 µs | 3.94 ms |
| mixed, 95% gets and 5% puts     | 290k ops/s | 3.00 µs | 6.00 µs |

**Note:** With files this small, flushes and compactions run constantly, so write throughput is lower than it would be with larger Config sizes.

**crc:** the CRC32 of one 8 KiB block, computed one byte at a time vs 16 bytes per step: 13.8 µs vs 1.50 µs. Profiling SST reads with samply showed the checksum taking most of each read, so the engine now uses the 16-bytes-per-step version.

## Limitations

This is a portfolio/learning project. It focuses on the core functionality of an LSM engine and it therefore has a lot of limitations:

1. No group commits: As of this comment, there are no group commits for the engine, so if `SyncConfig` is set to `Always`, it will be much slower in comparison to `SyncConfig::None`. This is to be added.
2. No range scans: Only per-key `get()`. This is to be added.
3. No compression of data
4. Uses an AVL tree instead of a SkipList. To be changed.
5. Only one compaction is running at a time
6. Only 2 WalRecovery modes. (`WalRecoveryMode::AbsoluteConsistency` refuses to open on any corruption error, `WalRecoveryMode::PointInTime` recovers to the last non-corrupt record and throws away all newer writes)
7. Corrupt sparse indexes can’t be repaired.
8. One corrupt file stops all writes
9. Not distributed.

and many more.
