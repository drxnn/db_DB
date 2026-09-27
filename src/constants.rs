pub const MAX_FILE_SIZE: u64 = 4 * 1024 * 1024; // SUBJECT TO CHANGE
pub const MEMTABLE_THRESHOLD: u64 = 8 * 1024 * 1024; // SUBJECT TO CHANGE
// this means L0 sstables are 8 mb, so we usually compact all L0 sstablse with all L1 sstables to a single L1 sstable.
pub const DATA_BLOCK: usize = 8 * 1024; // Data block in SSTable
pub const DATA_BLOCK_MAX_BYTES_SIZE: u64 =
    DATA_BLOCK as u64 + KEY_MAX_BYTES_SIZE + VALUE_MAX_BYTES_SIZE + RECORD_HEADER_LEN as u64; // 8192(max db_size) + KEY_MAX_BYTES_SIZE + VALUE_MAX_BYTES_SIZE + 25 bytes for metadata(timestamp, ksz,vsz,tmbstone); // if we had a db_size of 8191, we could end up with adding a max val and max key

pub const MAX_SST_SIZE: u64 = 1024 * 1024 * 100;

pub const COMPACTION_READ_BUFFER_LEN: usize = 64 * 1024;
pub const TAG_DELETION: u8 = 2;
pub const TAG_INSERTION: u8 = 4;
pub const SST_LEVEL_COUNT: usize = 4;
pub const KEY_MAX_BYTES_SIZE: u64 = 16 * 1024;
pub const VALUE_MAX_BYTES_SIZE: u64 = 128 * 1024;
pub const NUM_OF_BITS_FOR_TSTAMP: u8 = 52;
pub const NUM_OF_BITS_FOR_COUNTER: u8 = 12;
pub const MASK_FOR_COUNTER: u64 = (u64::MAX) >> NUM_OF_BITS_FOR_TSTAMP;
pub const MASK_FOR_TSTAMP: u64 = (u64::MAX) << NUM_OF_BITS_FOR_COUNTER;
pub const LEVEL_MULTIPLIER: usize = 10; // 

pub const NUM_OF_L0_FILES_TO_TRIGGER_COMPACTION: usize = 10;
pub const NUM_OF_BYTES_NEEDED_TO_TRIGGER_L1_COMPACTION: u64 = MAX_SST_SIZE * 10;
pub const NUM_OF_BYTES_NEEDED_TO_TRIGGER_L2_COMPACTION: u64 = MAX_SST_SIZE * 100;
pub const NUM_OF_BYTES_NEEDED_TO_TRIGGER_L3_COMPACTION: u64 = MAX_SST_SIZE * 1000;
pub const NUM_OF_BYTES_NEEDED_TO_TRIGGER_L4_COMPACTION: u64 = MAX_SST_SIZE * 10000;
pub const DEFAULT_DATA_DIR: &str = "data";
pub const MAX_FLUSH_ATTEMPTS: u8 = 5;

pub const U64_LEN: usize = size_of::<u64>(); // 8
pub const U32_LEN: usize = size_of::<u32>();
pub const U8_LEN: usize = size_of::<u8>();

pub const MANIFEST_LEN_PREFIX: usize = U64_LEN; // 8
pub const MANIFEST_RECORD_OVERHEAD: usize = MANIFEST_LEN_PREFIX + CRC_LEN;

pub const TOMBSTONE_LEN: usize = 1;
pub const RECORD_KSZ_OFFSET: usize = U64_LEN; // 8
pub const RECORD_VSZ_OFFSET: usize = 2 * U64_LEN; // 16
pub const RECORD_TOMBSTONE_OFFSET: usize = 3 * U64_LEN; // 24
pub const RECORD_HEADER_LEN: usize = 3 * U64_LEN + TOMBSTONE_LEN; // 25

pub const TOMBSTONE_DELETED: u8 = 0xFF;
pub const TOMBSTONE_LIVE: u8 = 0x00;

pub const BLOOM_BITS_PER_KEY: usize = 10;

pub const CRC_LEN: usize = 4;
pub const LEVEL_LEN: usize = 1;
pub const TAG_LEN: usize = 1;

// below is to read footer from
pub const FOOTER_SPARSE_OFFSET_START: usize = 0;
pub const FOOTER_SPARSE_LEN_START: usize = FOOTER_SPARSE_OFFSET_START + U64_LEN; // 8
pub const FOOTER_BLOOM_LEN_START: usize = FOOTER_SPARSE_LEN_START + U64_LEN; // 16
pub const FOOTER_MIN_KEY_LEN_START: usize = FOOTER_BLOOM_LEN_START + U64_LEN; // 24
pub const FOOTER_MAX_KEY_LEN_START: usize = FOOTER_MIN_KEY_LEN_START + U64_LEN; // 32
pub const FOOTER_LEVEL_START: usize = FOOTER_MAX_KEY_LEN_START + U64_LEN; // 40
pub const FOOTER_FIXED_LEN: usize = FOOTER_LEVEL_START + LEVEL_LEN; // 41

pub const FOOTER_SPARSE_CRC_START: usize = FOOTER_FIXED_LEN; // 41
pub const FOOTER_BLOOM_CRC_START: usize = FOOTER_SPARSE_CRC_START + CRC_LEN; // 45
pub const FOOTER_MIN_MAX_CRC_START: usize = FOOTER_BLOOM_CRC_START + CRC_LEN; // 49
pub const FOOTER_FIELDS_CRC_START: usize = FOOTER_MIN_MAX_CRC_START + CRC_LEN; // 53
pub const FOOTER_LEN: usize = FOOTER_FIELDS_CRC_START + CRC_LEN;

pub const SST_EXT: &str = "sst";
pub const WAL_EXT: &str = "wal";
pub const TMP_EXT: &str = "tmp";

pub const BLOOM_WORD_BITS: usize = 64;
pub const MAX_MANIFEST_SIZE: u64 = 4 * 1024 * 1024;
pub const TAG_ADD_FILE: u8 = 1;
pub const TAG_DELETE_FILE: u8 = 2;
pub const TAG_MIN_LIVE_WAL: u8 = 3;
pub const MANIFEST_FILE_NAME: &str = "MANIFEST";
pub const MANIFEST_TMP_FILE_NAME: &str = "MANIFEST.tmp";

pub const MAX_FROZEN_MEMTABLES_LIMIT: u8 = 24;
pub const MAX_MEMTABLE_THRESHOLD: u64 = 256 * 1024 * 1024;
