use std::os::unix::fs::FileExt;

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions, remove_file, rename};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::thread::{JoinHandle, spawn};
use std::time::{Duration, Instant};
use std::{format, todo, unreachable};

use crate::compact::{CompactionJob, CompactionManager, CompactionOutcome, CompactionSstSlice};
use crate::constants::{
    BLOOM_BITS_PER_KEY, BLOOM_WORD_BITS, CRC_LEN, DATA_BLOCK, DATA_BLOCK_MAX_BYTES_SIZE,
    FOOTER_BLOOM_LEN_START, FOOTER_FIXED_LEN, FOOTER_LEN, FOOTER_LEVEL_START,
    FOOTER_MAX_KEY_LEN_START, FOOTER_MIN_KEY_LEN_START, FOOTER_SPARSE_LEN_START,
    FOOTER_SPARSE_OFFSET_START, KEY_MAX_BYTES_SIZE, MASK_FOR_COUNTER, MASK_FOR_TSTAMP,
    MAX_FLUSH_ATTEMPTS, MAX_FROZEN_MEMTABLES_LIMIT, MAX_MEMTABLE_THRESHOLD, MAX_SST_SIZE,
    MEMTABLE_THRESHOLD, NUM_OF_BITS_FOR_COUNTER, NUM_OF_L0_FILES_TO_TRIGGER_COMPACTION,
    RECORD_HEADER_LEN, RECORD_KSZ_OFFSET, RECORD_TOMBSTONE_OFFSET, RECORD_VSZ_OFFSET, SST_EXT,
    SST_LEVEL_COUNT, TAG_DELETION, TAG_INSERTION, TAG_LEN, TMP_EXT, TOMBSTONE_DELETED,
    TOMBSTONE_LEN, TOMBSTONE_LIVE, U64_LEN, VALUE_MAX_BYTES_SIZE, WAL_EXT,
};

use crate::errors::CorruptionType::{CrcMismatch, Other, SstLevelMalformed, TruncatedRecord};

use crate::errors::{
    CorruptionType, CrcType, DataCorruptedErr, DbError, InvalidMemtableInput, InvalidOptions,
    Result,
};
use crate::helpers::{CRC32, get_hlc_from_valid_pathbuf, read_u8, read_u64};
use crate::helpers::{
    NUM_HASHES, check_crc, create_new_data_file, get_hashed_key_positions, new_timestamp,
    read_exact_or_truncated, read_range,
};
use crate::hlc::Hlc;
use crate::lsm::Lookup::{Absent, Deleted, Found};
use crate::lsm::SyncConfig::{Always, Every};
use crate::lsm::WalRecoveryMode::{AbsoluteConsistency, PointInTime};
use crate::lsm::WalReplayState::Clean;
use crate::manifest::{Manifest, ManifestEdit};
use crate::memtable::AVL;

use std::cmp::{Ordering as CmpOrdering, Reverse, max};

// WAL config for flush

#[derive(Copy, Clone)]
enum SyncConfig {
    None,       // fast, data can be lost
    Every(u64), // in ms
    Always,     // Ddurable
}

pub enum Lookup {
    Found(Vec<u8>),
    Deleted,
    Absent,
}

pub struct BloomFilter {
    pub bits: Vec<u64>,
    pub num_bits: u64,
}

enum WalRecordType<'a> {
    Deletion(&'a [u8]),            // ( key )
    Insertion(&'a [u8], &'a [u8]), // (key, value)
}

pub struct SparseIndex {
    pub index_entries: Vec<u8>,
    pub size: u64,
}

// SOURCE OF TRUTH FOR STATE CHANGES IN THE DB
// Manifest should accept different types of records
// RECORD TYPE | RECORD | CRC

// what different record types can we have ?
// ADD_FILE_RECORD -> when a new sst is added either from flush or compaction
// DELETE_FILE_RECORD -> file deletion, e.g after a successful compaction, we remove the input files or after we are done with a wal
// DELETE_WAL RECORD -> after a successful flush
// LATEST_WAL_RECORD -> name_of_wal | crc
/*
so it can look like:
RECORD_LEN | (RECORD_TYPE | RECORD)* | CRC
 |
*/

impl SparseIndex {
    pub fn new() -> Self {
        Self {
            index_entries: Vec::new(),
            size: 0,
        }
    }
    pub fn add_entry(&mut self, starting_key: &[u8], data_len: u64, offset: u64) {
        // data_len = block length WITHOUT 4 Byte CRC
        let first_keysz = (starting_key.len() as u64).to_le_bytes();
        let data_block_sz = data_len.to_le_bytes();

        // sparse index: sizeof(k), k, offset, datablock_size);
        self.index_entries.extend_from_slice(&first_keysz);
        self.index_entries.extend_from_slice(starting_key);
        self.index_entries.extend_from_slice(&offset.to_le_bytes());
        self.index_entries.extend_from_slice(&data_block_sz);
        self.size += 1;
    }

    fn parse_sparse_index(b: &[u8], path: PathBuf) -> Result<Vec<(Vec<u8>, u64, u64)>> {
        let mut out = Vec::new();
        // I am parsing this layout: ksz(8) | key(of size: ksz) | offset(8) | datablock_sz(8)
        // to essentially => key | offset | datablock_sz (this lives in memory, the sparseIndex needs key to binary search. meanwhile the sparseIndex in the metadatafooter does need the key size)
        // TODO:
        // Have caller
        let mut current = 0;
        while current < b.len() {
            let ksz = read_u64(b, current)?;
            if ksz > KEY_MAX_BYTES_SIZE {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: current as u64,
                    file_path: path,
                    reason: CorruptionType::KeyValueRecordExceedsMaxLength {
                        max: KEY_MAX_BYTES_SIZE,
                        found: ksz,
                    },
                }));
            }
            current += U64_LEN;
            let key = read_range(b, current, current + (ksz as usize))?.to_vec();

            current += ksz as usize;

            let offset = read_u64(b, current)?;

            current += U64_LEN;

            let data_block_size = read_u64(b, current)?;
            if data_block_size > DATA_BLOCK_MAX_BYTES_SIZE {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: current as u64,
                    file_path: path,
                    reason: CorruptionType::BufferExceedsMaxLength {
                        size: data_block_size,
                        max_size: DATA_BLOCK_MAX_BYTES_SIZE,
                    },
                }));
            }
            current += U64_LEN;
            out.push((key, offset, data_block_size));
        }
        Ok(out)
    }
}

impl BloomFilter {
    pub fn new(num_bits: usize) -> Self {
        let words_for_bits = num_bits.div_ceil(BLOOM_WORD_BITS);

        Self {
            bits: vec![0u64; words_for_bits],
            num_bits: (words_for_bits * BLOOM_WORD_BITS) as u64,
        }
    }

    pub fn set_bits(&mut self, positons: [usize; NUM_HASHES]) {
        for position in positons {
            let word_idx = position / BLOOM_WORD_BITS;
            let bit_idx = position % BLOOM_WORD_BITS;

            self.bits[word_idx] |= 1u64 << bit_idx; // shift the bit to the left by bit_idx positions and thats our mask. mask OR curr_u64 = done
        }
    }

    pub fn check_bits(&self, positons: [usize; NUM_HASHES]) -> bool {
        for position in positons {
            let word_idx = position / BLOOM_WORD_BITS;
            let bit_idx = position % BLOOM_WORD_BITS;

            if ((self.bits[word_idx] >> bit_idx) & 1u64) == 0 {
                return false;
            }
        }

        true
    }
}

struct WAL {
    id: u64,
    wal_writer: Option<BufWriter<File>>,
    sync_c: SyncConfig,
    record_buffer: Vec<u8>,
    threshold: u64,
    path: PathBuf,
    last_sync: Instant,
}

impl WAL {
    fn new(
        threshold: u64,
        sync_c: SyncConfig,
        parent_dir: &Path,
        curr_hlc: u64,
    ) -> io::Result<WAL> {
        let wal_path = parent_dir.join(format!("{}.wal", curr_hlc));

        let wal_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(&wal_path)?;
        File::open(parent_dir)?.sync_all()?;
        Ok(Self {
            id: curr_hlc,
            wal_writer: Some(BufWriter::new(wal_file)),
            threshold,
            record_buffer: Vec::new(),
            sync_c,
            path: wal_path,
            last_sync: Instant::now(),
        })
    }
    fn destruct(mut self) -> Result<()> {
        self.wal_writer = None;
        let _ = remove_file(&self.path);
        Ok(())
    }
    fn sync(&mut self) -> Result<()> {
        let writer = self.wal_writer.as_mut().ok_or(DbError::WalNotFound)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        Ok(())
    }

    fn record_to_wal<'a>(&mut self, record: WalRecordType<'a>, timestamp: u64) -> Result<()> {
        let record_buffer = &mut self.record_buffer;
        record_buffer.clear();

        match record {
            WalRecordType::Deletion(k) => {
                record_buffer.extend_from_slice(&TAG_DELETION.to_le_bytes());
                record_buffer.extend_from_slice(&timestamp.to_le_bytes());
                record_buffer.extend_from_slice(&(k.len() as u64).to_le_bytes());
                record_buffer.extend_from_slice(k);
            }
            WalRecordType::Insertion(k, v) => {
                record_buffer.extend_from_slice(&TAG_INSERTION.to_le_bytes());
                record_buffer.extend_from_slice(&timestamp.to_le_bytes());
                record_buffer.extend_from_slice(&(k.len() as u64).to_le_bytes());
                record_buffer.extend_from_slice(&(v.len() as u64).to_le_bytes());
                record_buffer.extend_from_slice(k);
                record_buffer.extend_from_slice(v);
            }
        }

        let crc = CRC32.compute_crc_data_block(record_buffer);
        record_buffer.extend_from_slice(&crc.to_le_bytes());

        match self.wal_writer.as_mut() {
            Some(writer) => {
                writer.write_all(record_buffer)?;
                writer.flush()?;
                match self.sync_c {
                    Always => {
                        writer.get_ref().sync_all()?;
                    }
                    Every(ms) => {
                        // TODO: this here isnt really accurate because if we have no record_to_wal call, more time can elapse than the specified ms
                        // we assume that theres always writes happening.

                        if self.last_sync.elapsed() >= Duration::from_millis(ms) {
                            writer.get_ref().sync_all()?;
                            self.last_sync = Instant::now()
                        }
                    }
                    SyncConfig::None => {
                        // yuhu
                    }
                }

                Ok(())
            }
            None => Err(DbError::WalNotFound),
        }
    }
}

pub struct SsTableDataBlock {
    pub bytes: Cursor<Vec<u8>>, //[ tstamp(8) | ksz(8) | value_sz(8) | tombstone | key | value |  ] ... crc(4) (crc for the entire datablock);
    pub size: usize,
    pub starting_key: Vec<u8>,
}

impl SsTableDataBlock {
    pub fn new(s_key: &[u8]) -> Self {
        // creates SsTableDataBlock

        Self {
            bytes: Cursor::new(Vec::new()),
            size: 0,
            starting_key: s_key.to_vec(),
        }
    }
    pub fn append_to_block(&mut self, entry: &[u8]) {
        self.bytes.get_mut().extend_from_slice(entry);
        // self.bytes.extend_from_slice(entry);
        self.size += entry.len();
    }

    pub fn is_finished(&self) -> bool {
        self.size > DATA_BLOCK as usize
    }

    pub fn full_data_block(mut self) -> Self {
        let crc = CRC32.compute_crc_data_block(self.bytes.get_ref());
        self.bytes.get_mut().extend_from_slice(&crc.to_le_bytes());
        self
    }

    pub fn grab_min_key_from_data_block(&mut self) -> Result<Vec<u8>> {
        let ksz = read_u64(self.bytes.get_ref(), RECORD_KSZ_OFFSET)?;

        let key = read_range(
            self.bytes.get_ref(),
            RECORD_HEADER_LEN,
            RECORD_HEADER_LEN + ksz as usize,
        )?;

        Ok(key.to_vec())
    }
    pub fn grab_max_key_from_data_block(&mut self) -> Result<Vec<u8>> {
        let mut pos = 0;

        let mut curr_max: Option<&[u8]> = None; // put in some then unwrap at the end or err

        while pos < self.size {
            // when it throws eof, we have reached the end

            let k_size = read_u64(self.bytes.get_ref(), RECORD_KSZ_OFFSET + pos)? as usize;

            let v_size = read_u64(self.bytes.get_ref(), pos + RECORD_VSZ_OFFSET)? as usize;

            let key_start = pos + RECORD_HEADER_LEN;

            let key = read_range(self.bytes.get_ref(), key_start, key_start + k_size as usize)?;
            pos = key_start + k_size + v_size;
            curr_max = Some(key);
        }

        if pos != self.size {
            return Err(DbError::MalformedDataBlock(
                "last record runs past end of block".to_string(),
            ));
        }
        let Some(max_k) = curr_max else {
            return Err(DbError::MalformedDataBlock(
                "Max key is missing from SsTableDataBlock".to_string(),
            ));
        };

        Ok(max_k.to_vec())
    }
}

// put the cold data into a SStable cold data vector(sparse index, etc)* //
pub struct SSTable {
    id: u64,
    file: File,
    file_path: PathBuf,
    file_size: u64,
    min_max_keys: Option<(Vec<u8>, Vec<u8>)>, // min_key is index 0, max_key is index 1
    sparse_index: Arc<Vec<(Vec<u8>, u64, u64)>>, // key | offset | datablock block length ( before CRC, which means you need to read the next 4 bytes and compute the crc)
    bloom_filter: Option<BloomFilter>,
    corrupted: bool,
    level: u8,
    currently_picked_for_compaction: bool,
}

impl SSTable {
    pub fn load(path: &Path) -> Result<Self> {
        let mut f = File::open(path)?;
        let file_metadata = f.metadata()?;

        let file_len = file_metadata.len();
        if file_len <= FOOTER_LEN as u64 {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: 0,
                file_path: path.to_path_buf(),
                reason: CorruptionType::FileTooSmall {
                    min_size: FOOTER_LEN as u64 + 1, // the check is `<=`, so FOOTER_LEN itself is rejected
                    found: file_len,
                },
            }));
        }
        let stem = path
            .file_stem()
            .and_then(|x| x.to_str())
            .ok_or_else(|| DbError::InvalidSstableFileName(path.to_path_buf()))?; // skip if this happens

        let id = stem
            .parse::<u64>()
            .ok()
            .ok_or_else(|| DbError::NonNumericFileIdOnSstable(path.to_path_buf()))?; // Have the caller skip file if this happens

        f.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        let mut footer = [0u8; FOOTER_FIXED_LEN];
        f.read_exact(&mut footer)?;

        let mut sparse_index_crc = [0u8; CRC_LEN];
        let mut bloom_filter_crc = [0u8; CRC_LEN];
        let mut metadata_crc = [0u8; CRC_LEN];
        let mut min_max_crc = [0u8; CRC_LEN];

        f.read_exact(&mut sparse_index_crc)?;
        f.read_exact(&mut bloom_filter_crc)?;
        f.read_exact(&mut min_max_crc)?;
        f.read_exact(&mut metadata_crc)?;
        let min_max_crc_in_file = u32::from_le_bytes(min_max_crc);
        let metadata_crc_in_file = u32::from_le_bytes(metadata_crc);
        let sparse_index_crc_in_file = u32::from_le_bytes(sparse_index_crc);
        let bloom_filter_crc_in_file = u32::from_le_bytes(bloom_filter_crc);

        let footer_metadata_crc_check = CRC32.compute_crc_data_block(&footer);

        check_crc(
            footer_metadata_crc_check,
            metadata_crc_in_file,
            f.stream_position()?,
            path,
            CrcType::SstFooterMetadata,
        )?;

        let level = read_u8(&footer, FOOTER_LEVEL_START)?;

        if level as usize >= SST_LEVEL_COUNT {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: file_metadata.len() - (FOOTER_LEN - FOOTER_LEVEL_START) as u64, // - 17
                file_path: path.to_path_buf(),
                reason: SstLevelMalformed(level as usize),
            }));
        }

        let sparse_index_offset = read_u64(&footer, FOOTER_SPARSE_OFFSET_START)?;

        let size_of_sparse_index = read_u64(&footer, FOOTER_SPARSE_LEN_START)?;

        let size_of_bloom_filter = read_u64(&footer, FOOTER_BLOOM_LEN_START)?; // byte count of vector
        let size_of_min_key = read_u64(&footer, FOOTER_MIN_KEY_LEN_START)?;

        let size_of_max_key = read_u64(&footer, FOOTER_MAX_KEY_LEN_START)?;

        let full_data_length = size_of_sparse_index
            .checked_add(size_of_bloom_filter)
            .and_then(|x| x.checked_add(size_of_min_key))
            .and_then(|x| x.checked_add(size_of_max_key))
            .ok_or({
                DbError::DataCorrupted(DataCorruptedErr {
                    offset: sparse_index_offset,
                    file_path: path.to_path_buf(),
                    reason: CorruptionType::MetadataSizeOverflow {
                        sizes: [
                            size_of_sparse_index,
                            size_of_bloom_filter,
                            size_of_min_key,
                            size_of_max_key,
                        ],
                    },
                })
            })?;

        // check_key_value_record_does_not_exceed_max(size, max_size, offset, file_path)
        // TODO: have the helper function above work with different kinds of data_corruption // not just k/v record check
        if full_data_length > file_len {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: sparse_index_offset,
                file_path: path.to_path_buf(),
                reason: CorruptionType::MetaDataSizeExceedsFileSize {
                    file_size: file_len,
                    metadata_size: full_data_length,
                },
            }));
        }
        let full_data_length = full_data_length as usize;

        f.seek(SeekFrom::Start(sparse_index_offset))?;
        let mut full_sst_data = vec![0u8; full_data_length];
        f.read_exact(&mut full_sst_data)?;
        let bloom_filter_start = size_of_sparse_index;
        let bloom_filter_end = bloom_filter_start + size_of_bloom_filter;
        let min_k_start = bloom_filter_end;
        let min_k_end = min_k_start + size_of_min_key;
        let max_k_start = min_k_end;
        let max_k_end = max_k_start + size_of_max_key;

        // let sparse_index: &[u8] = &full_sst_data[0..(size_of_sparse_index as usize)];
        let sparse_index: &[u8] = read_range(&full_sst_data, 0, size_of_sparse_index as usize)?;
        let sparse_index_crc_check = CRC32.compute_crc_data_block(sparse_index);

        check_crc(
            sparse_index_crc_check,
            sparse_index_crc_in_file,
            sparse_index_offset,
            path,
            CrcType::SparseIndex,
        )?;

        let bloom_filter: &[u8] = read_range(
            &full_sst_data,
            bloom_filter_start as usize,
            bloom_filter_end as usize as usize,
        )?;

        let bloom_filter_crc_check = CRC32.compute_crc_data_block(bloom_filter);

        // &full_sst_data[(bloom_filter_start as usize)..(bloom_filter_end as usize)];
        let min_key = read_range(
            &full_sst_data,
            min_k_start as usize,
            min_k_end as usize as usize,
        )?;
        // let min_key = &full_sst_data[(min_k_start as usize)..(min_k_end as usize)];
        // let max_k = &full_sst_data[(max_k_start as usize)..(max_k_end as usize)];
        let max_k = read_range(&full_sst_data, max_k_start as usize, max_k_end as usize)?;

        let min_max_key_crc_to_check = CRC32.compute_crc_data_block(read_range(
            &full_sst_data,
            min_k_start as usize,
            max_k_end as usize,
        )?);

        let bloomf_filter_64: Vec<u64> = bloom_filter
            .chunks_exact(U64_LEN)
            .map(|chunk| {
                u64::from_le_bytes(
                    chunk
                        .try_into()
                        .expect("bloom_filter not divided in 64 bit chunks, data corrupted"),
                )
            })
            .collect();

        let num_bits = (bloomf_filter_64.len() * BLOOM_WORD_BITS) as u64;

        // IF THE BLOOM_FILTER BITS ARE CORRUPTED, WE JUST DON'T USE IT. NO ERR
        let bloom_filter = if bloom_filter_crc_check == bloom_filter_crc_in_file {
            Some(BloomFilter {
                bits: bloomf_filter_64,
                num_bits,
            })
        } else {
            None
        };

        let min_max = if min_max_key_crc_to_check == min_max_crc_in_file {
            Some((min_key.to_vec(), max_k.to_vec()))
        } else {
            None
        };

        let parsed_sparse_index =
            SparseIndex::parse_sparse_index(sparse_index, path.to_path_buf())?; // catch err from caller
        let mut sstable = SSTable {
            id,
            file: f,
            file_path: path.to_path_buf(),
            file_size: file_len,
            min_max_keys: min_max,
            sparse_index: Arc::new(parsed_sparse_index),
            bloom_filter,
            corrupted: false,
            level,
            currently_picked_for_compaction: false,
        };
        if sstable.min_max_keys.is_none() {
            sstable.rebuild_min_max_key_from_sparse_index()?;
        }
        Ok(sstable)
    }

    fn binary_search_sparse_index(&self, key: &[u8]) -> Option<(u64, u64)> {
        // first u64 is the offset, the second is the datablock size
        if self.sparse_index.is_empty() {
            return None;
        }

        let mut lo: i64 = 0;
        let mut hi: i64 = (self.sparse_index.len() - 1) as i64;

        let mut best_candidate: Option<(u64, u64)> = None;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            match self.sparse_index.get(mid as usize) {
                Some(entry) => {
                    let key_in_index = entry.0.as_slice();
                    if key_in_index < key {
                        best_candidate = Some((entry.1, entry.2));
                        lo = mid + 1;
                    } else if key_in_index > key {
                        hi = mid - 1;
                    } else {
                        return Some((entry.1, entry.2));
                    }
                }
                None => unreachable!(),
            }
        }

        best_candidate
    }

    fn rebuild_min_max_key_from_sparse_index(&mut self) -> Result<()> {
        //[ tstamp(8) | ksz(8) | value_sz(8) | tombstone | key | value |  ] ... crc(4) (crc for the entire datablock);

        let (Some((min_k, _, _)), Some((_, last_sparse_offset, last_data_block_length))) =
            (self.sparse_index.first(), self.sparse_index.last())
        else {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: 0,
                file_path: self.file_path.to_path_buf(),
                reason: Other("empty sparse index, cannot rebuild min/max keys".to_string()),
            }));
        };

        let f = &mut self.file;
        let block_len = *last_data_block_length as usize;

        let mut data_block_buffer_and_crc = vec![0u8; block_len + CRC_LEN];
        f.read_exact_at(&mut data_block_buffer_and_crc, *last_sparse_offset)?;
        let data_block_buffer = &data_block_buffer_and_crc[..block_len];

        let crc = &data_block_buffer_and_crc[block_len..];

        check_crc(
            CRC32.compute_crc_data_block(data_block_buffer),
            u32::from_le_bytes(crc.try_into().unwrap()),
            *last_sparse_offset,
            &self.file_path,
            CrcType::DataBlock,
        )?;

        let mut max_k: Option<&[u8]> = None;
        let mut pos = 0;

        while pos < block_len {
            //[ tstamp(8) | ksz(8) | value_sz(8) | tombstone | key | value |  ] ... crc(4) (crc for the entire datablock);

            let ksz = read_u64(data_block_buffer, pos + RECORD_KSZ_OFFSET)? as usize;

            let vsz = read_u64(data_block_buffer, pos + RECORD_VSZ_OFFSET)? as usize;

            max_k = Some(read_range(
                data_block_buffer,
                pos + RECORD_HEADER_LEN,
                (pos + RECORD_HEADER_LEN + ksz) as usize,
            )?);

            pos += ksz + vsz + RECORD_HEADER_LEN; // skip val
        }

        if pos != data_block_buffer.len() {
            return Err(DbError::MalformedDataBlock(
                "last record runs past end of block".to_string(),
            ));
        }
        let Some(max_k) = max_k else {
            return Err(DbError::MalformedDataBlock(
                "SsTableDataBlock is empty".to_string(),
            ));
        };
        self.min_max_keys = Some((min_k.to_vec(), max_k.to_vec()));

        Ok(())
    }
}

pub enum FlushingThreadResponse {
    Success { id: u64, sstable: SSTable },
    Error { id: u64, error: DbError },
}

struct FlushingManager {
    tx: Sender<FlushingThreadResponse>,
    rx: Receiver<FlushingThreadResponse>,
    in_flight: Vec<JoinHandle<Result<()>>>,
}

pub enum WalReplayState {
    Clean,                 // replayed everything to mem
    PartialError(DbError), // either truncation or corruption
}
pub struct WalToMemtableReplay {
    memtable: AVL,
    records_recovered: u64,
    valid_bytes: u64,
    most_recent_hlc: Option<u64>,
    replay_state: WalReplayState,
}

impl FlushingManager {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel::<FlushingThreadResponse>();
        Self {
            tx,
            rx,
            in_flight: Vec::new(),
        }
    }

    // main will poll and on success, will add the SST to active memory and delete old_wal from directory
    fn background_flush_memtable(
        &mut self,
        frozen_instance: FrozenMemtableInstance,
        dir: PathBuf,
    ) -> Result<()> {
        let tx: Sender<FlushingThreadResponse> = self.tx.clone();
        let id = frozen_instance.id;
        self.in_flight.retain(|handle| !handle.is_finished());
        let handle = spawn(move || -> Result<()> {
            let result = (|| -> Result<SSTable> {
                let (_, ss_path_final) = frozen_instance
                    .memtable
                    .sync_avl(&dir, frozen_instance.id)?
                    .ok_or_else(|| {
                        DbError::MemTableSyncError("The memtable returned None".to_string())
                    })?;
                File::open(dir)?.sync_all()?;
                SSTable::load(&ss_path_final)
            })();

            let msg = match result {
                Ok(sstable) => FlushingThreadResponse::Success { id, sstable },
                Err(error) => FlushingThreadResponse::Error { id, error },
            };
            let _ = tx.send(msg);

            Ok(())
        });

        self.in_flight.push(handle);
        Ok(())
    }

    fn build_avl_from_wal(
        &self,
        path: &PathBuf,
        memtable_threshold: u64,
    ) -> Result<WalToMemtableReplay> {
        let mut memtable = AVL::new(memtable_threshold);
        let mut curr_offset: u64 = 0;
        let mut records_recovered = 0;
        let mut most_recent_hlc: Option<u64> = None;

        let wal_f = File::open(path)?;

        let file_len = wal_f.metadata()?.len();

        let mut reader = BufReader::new(&wal_f);

        let mut type_of_record: [u8; TAG_LEN] = [0u8; TAG_LEN];

        let mut ksz = [0u8; U64_LEN];
        let mut tstamp = [0u8; U64_LEN];
        let mut vsz = [0u8; U64_LEN];
        let mut crc = [0u8; CRC_LEN];
        let mut pos: u64 = 0;

        let outcome = (|| -> Result<()> {
            while pos < file_len {
                // We should read records up until a truncated record or a corrupted record, then we stop
                read_exact_or_truncated(&mut reader, &mut type_of_record, curr_offset, path)?;
                curr_offset += TAG_LEN as u64;
                let type_tag = type_of_record[0];

                match type_tag {
                    TAG_DELETION => {
                        //  TAG_DELETION handle  [ tstamp(8) | ksz(8) | key(sizeof ksz ) |crc (4 bytes) ]
                        read_exact_or_truncated(&mut reader, &mut tstamp, curr_offset, path)?;
                        curr_offset += U64_LEN as u64;
                        read_exact_or_truncated(&mut reader, &mut ksz, curr_offset, path)?;
                        curr_offset += U64_LEN as u64;

                        let key_size = u64::from_le_bytes(ksz);

                        if key_size > KEY_MAX_BYTES_SIZE {
                            return Err(DbError::DataCorrupted(DataCorruptedErr {
                                offset: pos,
                                file_path: path.to_path_buf(),
                                reason: CorruptionType::Other(format!(
                                    "record size overflow: ksz={key_size}"
                                )),
                            }));
                        }
                        let mut key_buffer = vec![0u8; key_size as usize];

                        read_exact_or_truncated(&mut reader, &mut key_buffer, curr_offset, path)?;
                        curr_offset += key_size;

                        let crc_data_block =
                            [type_of_record.as_slice(), &tstamp, &ksz, &key_buffer].concat();
                        let crc_to_check = CRC32.compute_crc_data_block(&crc_data_block);

                        read_exact_or_truncated(&mut reader, &mut crc, curr_offset, path)?;
                        curr_offset += CRC_LEN as u64;

                        let crc_from_buff = u32::from_le_bytes(crc);

                        check_crc(crc_to_check, crc_from_buff, pos, path, CrcType::WalRecord)?;
                        let ts = u64::from_le_bytes(tstamp);
                        most_recent_hlc = Some(most_recent_hlc.unwrap_or(0).max(ts));

                        pos = reader.stream_position()?;
                        memtable.delete(&key_buffer, ts);
                        records_recovered += 1;
                    }
                    TAG_INSERTION => {
                        read_exact_or_truncated(&mut reader, &mut tstamp, curr_offset, path)?;
                        curr_offset += U64_LEN as u64;
                        read_exact_or_truncated(&mut reader, &mut ksz, curr_offset, path)?;
                        curr_offset += U64_LEN as u64;
                        read_exact_or_truncated(&mut reader, &mut vsz, curr_offset, path)?;
                        curr_offset += U64_LEN as u64;

                        let key_size = u64::from_le_bytes(ksz);
                        let val_size = u64::from_le_bytes(vsz);

                        if key_size > KEY_MAX_BYTES_SIZE || val_size > VALUE_MAX_BYTES_SIZE {
                            return Err(DbError::DataCorrupted(DataCorruptedErr {
                                offset: pos,
                                file_path: path.to_path_buf(),
                                reason: CorruptionType::Other(format!(
                                    "record size overflow: ksz={key_size} vsz={val_size}"
                                )),
                            }));
                        }
                        let mut key_buffer = vec![0u8; key_size as usize];
                        let mut val_buffer = vec![0u8; val_size as usize];

                        read_exact_or_truncated(&mut reader, &mut key_buffer, curr_offset, path)?;
                        curr_offset += key_size;

                        read_exact_or_truncated(&mut reader, &mut val_buffer, curr_offset, path)?;
                        curr_offset += val_size;

                        let crc_data_block = [
                            type_of_record.as_slice(),
                            &tstamp,
                            &ksz,
                            &vsz,
                            &key_buffer,
                            &val_buffer,
                        ]
                        .concat();
                        let crc_to_check = CRC32.compute_crc_data_block(&crc_data_block);

                        read_exact_or_truncated(&mut reader, &mut crc, curr_offset, path)?;
                        curr_offset += CRC_LEN as u64;

                        let crc_from_buff = u32::from_le_bytes(crc);

                        check_crc(crc_to_check, crc_from_buff, pos, path, CrcType::WalRecord)?;

                        let ts = u64::from_le_bytes(tstamp);
                        most_recent_hlc = Some(most_recent_hlc.unwrap_or(0).max(ts));

                        pos = reader.stream_position()?;

                        memtable.put(&key_buffer, &val_buffer, ts);
                        records_recovered += 1;

                        // TAG_INSERTION handle tstamp | ksz | vsz | key | value |crc (4 bytes)
                    }
                    _ => {
                        // corrupt
                        return Err(DbError::DataCorrupted(DataCorruptedErr {
                            reason: CorruptionType::RecordTypeCorrupted { found: type_tag },
                            offset: curr_offset - TAG_LEN as u64,
                            file_path: path.to_path_buf(),
                        }));
                    }
                }
            }
            Ok(())
        })();

        let replay_state = match outcome {
            Ok(()) => WalReplayState::Clean,
            Err(err @ DbError::DataCorrupted(_)) => WalReplayState::PartialError(err),
            Err(e) => return Err(e),
        };

        Ok(WalToMemtableReplay {
            memtable,
            records_recovered,
            valid_bytes: pos, // pos is only updated when we read a valid record
            most_recent_hlc,
            replay_state,
        })
    }

    fn join_all_handles(&mut self) {
        for handle in self.in_flight.drain(..) {
            let _ = handle.join();
        }
    }
}

struct FrozenMemtableInstance {
    memtable: Arc<AVL>,
    sstable: Option<SSTable>, // it should hold sstables until it is safe for it to be retired(when after every older memtable is gone from the BtreeMap)
    id: u64,                  // hlc
    wal_id: u64,
    flush_attempts: u8,
}

#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub enum WalRecoveryMode {
    PointInTime, // stop at the first damage, skip every newer WAL, open at that moment(loses data but recovers)
    AbsoluteConsistency, // any damage at all, even a torn tail, is an error
}
pub struct KVEngineOptions {
    sync: SyncConfig,
    wal_recovery_mode: WalRecoveryMode,
    memtable_threshold: u64,
    max_flush_retries: u8,
    max_frozen_memtables: u8, // whem max is reached, pause writes until we finish at least 1
}
impl Default for KVEngineOptions {
    fn default() -> Self {
        Self {
            sync: SyncConfig::Always,
            wal_recovery_mode: WalRecoveryMode::PointInTime,
            memtable_threshold: 8 * 1024 * 1024,
            max_flush_retries: 5,
            max_frozen_memtables: 4,
        }
    }
}

impl KVEngineOptions {
    fn validate(&self) -> Result<()> {
        let min = VALUE_MAX_BYTES_SIZE + KEY_MAX_BYTES_SIZE + RECORD_HEADER_LEN as u64;
        if !(min..=MAX_MEMTABLE_THRESHOLD).contains(&self.memtable_threshold) {
            return Err(InvalidOptions::MemtableThresholdOutOfRange {
                min,
                max: MAX_MEMTABLE_THRESHOLD,
                found: self.memtable_threshold,
            }
            .into());
        }
        if !(1..=MAX_FROZEN_MEMTABLES_LIMIT).contains(&self.max_frozen_memtables) {
            return Err(InvalidOptions::MaxFrozenMemtablesOutOfRange {
                min: 1,
                max: MAX_FROZEN_MEMTABLES_LIMIT,
                found: self.max_frozen_memtables,
            }
            .into());
        }

        if let Every(ms) = self.sync
            && ms == 0
        {
            return Err(InvalidOptions::SyncIntervalIsZero.into());
        }

        Ok(())
    }
}

struct KVEngine {
    // node_id: have a unique ID here
    data_directory: PathBuf, // data_directory now holds all .sst and .wal files
    sstables: Option<Arc<RwLock<[Vec<SSTable>; SST_LEVEL_COUNT]>>>,
    wal: WAL,
    memtable: AVL,
    frozen_memtables: BTreeMap<u64, FrozenMemtableInstance>, // ordered. id(hlc) -> mem
    corrupted_files: HashSet<PathBuf>,
    flushing_manager: FlushingManager,
    hlc: Arc<Hlc>, // first 52 bits are the time stamp, 12 last bits are the counter
    compaction_manager: CompactionManager,
    db_failed: Option<String>, //
    manifest: Manifest,
    options: KVEngineOptions,
}

impl KVEngine {
    fn open(dir_name: &Path, options: KVEngineOptions) -> Result<KVEngine> {
        let path = PathBuf::from(dir_name);

        //TODO: make sure we use options now
        options.validate()?;

        let mut sstables: [Vec<SSTable>; SST_LEVEL_COUNT] = [const { Vec::new() }; SST_LEVEL_COUNT];

        let memtable = AVL::new(options.memtable_threshold);

        let mut sst_vec: Vec<(u64, PathBuf)> = Vec::new();
        let mut wal_vec: Vec<(u64, PathBuf)> = Vec::new();

        // sort by
        for entry in fs::read_dir(dir_name)? {
            //
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            match path.extension().and_then(|x| x.to_str()) {
                Some(SST_EXT) => {
                    if let Ok(id) = get_hlc_from_valid_pathbuf(&path) {
                        sst_vec.push((id, path));
                    }
                }
                Some(WAL_EXT) => {
                    if let Ok(id) = get_hlc_from_valid_pathbuf(&path) {
                        wal_vec.push((id, path));
                    }
                }
                Some(TMP_EXT) => {
                    // incomplete manifest
                    let _ = remove_file(path);
                }
                _ => {}
            };
        }

        let manifest = match Manifest::open(dir_name)? {
            Some(m) => m,
            None if !sst_vec.is_empty() => {
                return Err(DbError::ManifestError(
                    "no MANIFEST, but .sst files exist".into(),
                ));
            }
            None => Manifest::new_manifest(dir_name)?,
        };

        for level in &manifest.state.levels {
            level.iter().try_for_each(|file_id|-> Result<()>{
                if !sst_vec.iter().any(|(id, _)|  id == file_id) {
                    // error, file in manifest but not in the director
                    return Err(DbError::ManifestError(format!("The manifest contains a file that is not present in the data directory. File id missing: {file_id}")));
                }
                Ok(())
            })?;
        }

        let max_hlc = sst_vec
            .iter()
            .map(|(id, _)| *id)
            .chain(wal_vec.iter().map(|(id, _)| *id))
            .max()
            .unwrap_or(0);

        let (live_ssts, obsolete_ssts): (Vec<(u64, PathBuf)>, Vec<(u64, PathBuf)>) =
            sst_vec.into_iter().partition(|(id, _)| {
                for level in &manifest.state.levels {
                    if level.contains(id) {
                        return true;
                    }
                }
                false
            });

        let hlc = Hlc::new();

        hlc.recover_to(max_hlc);
        let wal = WAL::new(options.memtable_threshold, options.sync, &path, hlc.tick())?;
        // IMPORTANT: The new wal is created after we check the actual directory for wal files.

        let mut self_instance = Self {
            data_directory: path,
            memtable,
            sstables: None,
            manifest,
            wal,
            frozen_memtables: BTreeMap::new(),
            flushing_manager: FlushingManager::new(),
            corrupted_files: HashSet::new(),
            hlc: Arc::new(hlc),
            compaction_manager: CompactionManager::new(),
            db_failed: None,
            options,
        };

        for (_, path) in live_ssts {
            match SSTable::load(&path) {
                Ok(sst) => {
                    sstables[sst.level as usize].push(sst);
                }
                Err(
                    e @ DbError::DataCorrupted(DataCorruptedErr {
                        reason:
                            CorruptionType::CrcMismatch {
                                mismatch_type: CrcType::SparseIndex,
                                ..
                            }
                            | CorruptionType::MetaDataSizeExceedsFileSize { .. }
                            | CorruptionType::MetadataSizeOverflow { .. },
                        ..
                    }),
                ) => {
                    // rebuild all the metadata(sparse, bloom, min max etc)
                    // what needs to be done here? we have a sstable with presumably some correct data in there but
                    // the metadata is corrupt, do I read the sstable front to back and create the metadata as we go?
                    // Read the sstable in order(needs the change below), write a new sstable with all the valid data_records
                    // while also building the bloom_filter, sparse_index etc, attach all metadata at the end
                    // then name the rebuilt sstable the same as the sstable with the corrupt metadata so we preserve key recency order

                    // Note: For now I'm just going to treat the sstable as corrupt and the data 3lost because I need to change
                    // the sparse_index format to be firstkey: offset and have the offset point to the data_block_length in the front of a
                    // datea_block, so we jump to that offset, the first 8 bytes tell us the data_block_length, then we read the data_block
                    // compared to what I have now: firstkey: (offset, data_block_length) which means I need the sparse_index to know where
                    // and how long the data_block_length is, meaning if there is a corrupt sparse_index, I cannot rebuild it because
                    // I dont know where data_blocks start or end
                    // LEAVING THIS HERE BECAUSE I WILL IMPLEMENT THIS LATER
                    return Err(e); // for now reject
                }
                Err(
                    DbError::InvalidSstableFileName(_p) | DbError::NonNumericFileIdOnSstable(_p),
                ) => {
                    continue;
                }
                Err(dberr) => {
                    return Err(dberr); // reject here. 
                }
            }
        }

        let (mut wals_to_replay, obsolete_wals): (Vec<_>, Vec<_>) = wal_vec
            .into_iter()
            .partition(|(id, _)| *id >= self_instance.manifest.state.min_live_wal);

        //TODO probbably have the manifest do this instead
        for (_, path) in obsolete_ssts.into_iter().chain(obsolete_wals) {
            let _ = remove_file(path);
        }

        wals_to_replay.sort_by_key(|(id, _)| *id);

        let recovered = self_instance.retrieve__wal_records(&wals_to_replay)?;
        sstables[0].extend(recovered);

        // TODO MAYBE: Other than L0, all the other levels have no overlapping keys in the sstables
        // meaning that they could be ordered by min_k, that way a binary search can be done on them instead of linearly checking every sst for the record
        // good enough for now
        sstables.iter_mut().for_each(|ss_vec| {
            ss_vec.sort_by_key(|p| Reverse(p.id)); // Descending order
        });

        self_instance.sstables = Some(Arc::new(RwLock::new(sstables)));
        Ok(self_instance)
    }
    fn retrieve__wal_records(&mut self, wals: &[(u64, PathBuf)]) -> Result<Vec<SSTable>> {
        let mut recovered = Vec::new();
        for (index, (id, path)) in wals.iter().enumerate() {
            let replay = self
                .flushing_manager
                .build_avl_from_wal(path, self.options.memtable_threshold)?;

            let should_stop_replaying = match (replay.replay_state, self.options.wal_recovery_mode)
            {
                (WalReplayState::Clean, _) => false,
                (WalReplayState::PartialError(_), WalRecoveryMode::PointInTime) => true,
                (WalReplayState::PartialError(e), WalRecoveryMode::AbsoluteConsistency) => {
                    return Err(e);
                }
            };

            if should_stop_replaying {
                self.skip_files_from_index(&wals[index + 1..])?;
            }

            let mut new_files = Vec::new();

            if let Some(max_hlc) = replay.most_recent_hlc {
                self.hlc.recover_to(max_hlc);

                if let Some((_, ss_final_path)) =
                    replay.memtable.sync_avl(&self.data_directory, max_hlc)?
                {
                    File::open(&self.data_directory)?.sync_all()?;
                    let sst = SSTable::load(&ss_final_path)?;

                    new_files.push((0, sst.id));
                    recovered.push(sst);
                }
            }
            self.manifest.edit_and_append(&ManifestEdit {
                new_files,
                deleted_files: vec![],
                min_live_wal: Some(id + 1),
            })?;

            if should_stop_replaying {
                fs::rename(path, path.with_extension("wal.corrupt"))?;
                break;
            }
            let _ = fs::remove_file(path);
        }

        Ok(recovered)
    }

    fn should_search_sstable_file(key: &[u8], sstable: &SSTable) -> bool {
        if let Some((min, max)) = &sstable.min_max_keys
            && (key > max.as_slice() || key < min.as_slice())
        {
            return false;
        }

        if let Some(bloom_filter) = &sstable.bloom_filter {
            let bf_bit_positions = get_hashed_key_positions(key, bloom_filter.num_bits as usize);
            bloom_filter.check_bits(bf_bit_positions)
        } else {
            true // If we do not have a bloom filter, we just search the file without bloom filter optimization
        }
    }

    fn search_kv_in_sstable(sstable: &SSTable, key: &[u8]) -> Result<Lookup> {
        let Some((offset, data_len)) = sstable.binary_search_sparse_index(key) else {
            return Ok(Absent);
        };

        if data_len > DATA_BLOCK_MAX_BYTES_SIZE {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset,
                file_path: sstable.file_path.clone(),
                reason: CorruptionType::BufferExceedsMaxLength {
                    size: data_len,
                    max_size: DATA_BLOCK_MAX_BYTES_SIZE,
                },
            }));
        }
        let mut data_buffer = vec![0u8; data_len as usize];

        let mut crc = [0u8; CRC_LEN];

        let mut reader = BufReader::new(&sstable.file);

        reader.seek(SeekFrom::Start(offset))?;

        reader.read_exact(&mut data_buffer)?;

        //
        // we read CRC here because data_len above doesnt take into account the 4 bytes for crc
        reader.read_exact(&mut crc)?;
        let crc_from_buff = u32::from_le_bytes(crc);

        let fresh_crc = CRC32.compute_crc_data_block(&data_buffer);

        check_crc(
            fresh_crc,
            crc_from_buff,
            offset,
            &sstable.file_path,
            CrcType::DataBlock,
        )?;

        let mut pos = 0;
        while pos < data_buffer.len() {
            if pos + RECORD_HEADER_LEN > data_buffer.len() {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: offset + pos as u64,
                    file_path: sstable.file_path.clone(),
                    reason: CorruptionType::Other(format!(
                        "truncated record header at buffer position {} (buffer len {})",
                        pos,
                        data_buffer.len(),
                    )),
                }));
            }

            // its actually: [ tstamp(8) | ksz(8) | value_sz(8) |tombstone| key | value |  ]
            let ksz = read_u64(&data_buffer, pos + RECORD_KSZ_OFFSET)? as usize;

            let vsz = read_u64(&data_buffer, pos + RECORD_VSZ_OFFSET)? as usize;

            let deleted = read_range(
                &data_buffer,
                pos + RECORD_TOMBSTONE_OFFSET,
                pos + RECORD_HEADER_LEN,
            )?[0];
            // [ tstamp(8) | ksz(8) | value_sz(8) | deletedflag(1) | key | value ]
            // check ksz and vsz doesnt overflow
            let key_start = pos + RECORD_HEADER_LEN;

            let val_end = key_start
                .checked_add(ksz)
                .and_then(|v| v.checked_add(vsz))
                .ok_or_else(|| {
                    DbError::DataCorrupted(DataCorruptedErr {
                        offset: offset + pos as u64,
                        file_path: sstable.file_path.clone(),
                        reason: CorruptionType::Other(format!(
                            "record size overflow: ksz={ksz}, vsz={vsz}"
                        )),
                    })
                })?;

            if val_end > data_buffer.len() {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: offset + pos as u64,
                    file_path: sstable.file_path.clone(),
                    reason: CorruptionType::LengthMismatch {
                        expected: val_end,
                        found: data_buffer.len(),
                    },
                }));
            }

            let val_start = key_start + ksz; // if val_end is safe then this is safe(no overflow)
            let curr_key = read_range(&data_buffer, key_start, val_start)?;
            // let curr_key = &data_buffer[key_start..val_start];
            let value = read_range(&data_buffer, val_start, val_end)?;
            // let value: &[u8] = &data_buffer[val_start..val_end];

            match curr_key.cmp(key) {
                CmpOrdering::Less => {
                    pos = val_end;
                    continue;
                }
                CmpOrdering::Equal => {
                    if deleted == TOMBSTONE_DELETED {
                        return Ok(Deleted);
                    }
                    return Ok(Found(value.to_vec()));
                }
                CmpOrdering::Greater => break,
            }
        }
        Ok(Absent)
    }
    fn search_for_kv_in_sstables(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(sstables) = &self.sstables {
            // Lock here is held for the entirety of the loop. Ok for now, mostly reads, rare writes
            for level in sstables.read().unwrap().iter() {
                for element in level.iter() {
                    match Self::should_search_sstable_file(key, element) {
                        true => match Self::search_kv_in_sstable(element, key)? {
                            Found(k) => return Ok(Some(k)),
                            Deleted => return Ok(None),
                            Absent => {
                                continue;
                            }
                        },
                        false => continue,
                    }
                }
            }
        }
        Ok(None)
    }

    fn skip_files_from_index(&self, files: &[(u64, PathBuf)]) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        for (_, path) in files {
            rename(path, path.with_added_extension("skipped"))?;
        }
        File::open(&self.data_directory)?.sync_all()?;
        Ok(())
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.get(key) {
            Found(bytes) => return Ok(Some(bytes.to_vec())),
            Deleted => return Ok(None),
            Absent => {} // fall through
        }

        for (id, mem_table_instance) in self.frozen_memtables.iter().rev() {
            // rev() because we search newer memtables first which have a higher id(hlc)

            match mem_table_instance.memtable.get(key) {
                Found(bytes) => return Ok(Some(bytes.to_vec())),
                Deleted => return Ok(None),
                Absent => {}
            }
        }

        self.search_for_kv_in_sstables(key) // if we get here, 
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(err_msg) = &self.db_failed {
            return Err(DbError::ReadOnly(err_msg.to_string()));
        }
        self.memtable
            .exceeds_max(key.len() as u64, value.len() as u64)?;

        if (key.len() as u64 + value.len() as u64 + self.memtable.size_in_bytes)
            > self.memtable.threshold
        {
            self.rotate_memtable_and_wal()?; // if it returns DbError::WritesStalled, have caller handle(maybe retry again in a couple ms)
        }

        let hlc = self.hlc.tick();
        match self
            .wal
            .record_to_wal(WalRecordType::Insertion(key, value), hlc)
        {
            Ok(()) => {}
            Err(e) => {
                // can fail because of hardware failure, disk full etc.
                // in that case subsequent writes will also fail so we stop accepting writes
                // how to handle? check what other dbs do but no need to go too far into it

                self.fail(format!(
                    "WAL append failed, so writes can no longer be made durable. \
     Reopen the database to recover: {e}"
                ));

                return Err(e);
            }
        }

        self.memtable.put(key, value, hlc);

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if let Some(err_str) = &self.db_failed {
            return Err(DbError::ReadOnly(err_str.to_string()));
        }
        let k_len = key.len() as u64;
        self.memtable.exceeds_max(k_len, 0)?;
        if (k_len + self.memtable.size_in_bytes) > self.memtable.threshold {
            self.rotate_memtable_and_wal()?;
        }
        // let tstamp = new_timestamp();
        let hlc = self.hlc.tick();
        match self.wal.record_to_wal(WalRecordType::Deletion(key), hlc) {
            Ok(()) => {}
            Err(e) => {
                self.fail(format!(
    "WAL append failed while recording a delete, so writes can no longer be made durable. \
     Reopen the database to recover: {e}"
));
                return Err(e);
            }
        }
        self.memtable.delete(key, hlc);

        Ok(())
    }

    fn rotate_memtable_and_wal(&mut self) -> Result<()> {
        let max = self.options.max_frozen_memtables as usize;
        if self.frozen_memtables.len() >= max {
            // block writes for a little(caller handles when it receives WritesStalled )
            while let Ok(msg) = self.flushing_manager.rx.try_recv() {
                self.handle_flushing_message(msg)?;
            }
            self.retire_frozen_memtables()?;
            if self.frozen_memtables.len() >= max {
                return Err(DbError::WritesStalled {
                    frozen_memtables: self.frozen_memtables.len(),
                });
            }
        }
        let old_wal = std::mem::replace(
            &mut self.wal,
            WAL::new(
                self.options.memtable_threshold,
                self.options.sync,
                &self.data_directory,
                self.hlc.tick(),
            )?,
        );
        let wal_id = old_wal.id;
        drop(old_wal);

        // WHEN MAIN(whoever polls it) RECEIVES A SUCCESSFUL FLUSH, REMOVE THE OLD WAL ASSOCIATED WITH THAT FLUSH
        let frozen = Arc::new(std::mem::replace(
            &mut self.memtable,
            AVL::new(self.options.memtable_threshold),
        ));

        let tick = self.hlc.tick();

        let frozen_mems = &mut self.frozen_memtables;
        frozen_mems.insert(
            tick,
            FrozenMemtableInstance {
                sstable: None,
                wal_id, // remove after its done
                memtable: Arc::clone(&frozen),
                id: tick,
                flush_attempts: 0,
            },
        );

        self.flushing_manager.background_flush_memtable(
            FrozenMemtableInstance {
                sstable: None,
                wal_id,
                memtable: Arc::clone(&frozen),
                id: tick,
                flush_attempts: 0,
            },
            self.data_directory.clone(),
        )?;

        Ok(())
    }

    fn does_overlap(sstable: &SSTable, min_k: &[u8], max_k: &[u8]) -> bool {
        if let Some((other_ss_min_k, other_ss_max_k)) = sstable.min_max_keys.as_ref() {
            return other_ss_min_k.as_slice() <= max_k && min_k <= other_ss_max_k.as_slice();
        }
        false
    }

    fn get_min_max_key_range_of_entire_level(
        &self,
        sstables: &[SSTable],
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut curr_min_max: Option<(&[u8], &[u8])> = None;

        for sstable in sstables {
            let Some((curr_ss_min, curr_ss_max)) = sstable.min_max_keys.as_ref() else {
                return Err(DbError::MissingKey(
                    "Min-Max keys missing from SStables metadata".to_string(),
                )); // When we load an SST, if min max is missing, we rebuild it so it will always be there at this point
            };

            curr_min_max = match curr_min_max {
                Some((curr_min, curr_max)) => {
                    Some((curr_min.min(curr_ss_min), curr_max.max(curr_ss_max)))
                }
                None => Some((curr_ss_min, curr_ss_max)),
            }
        }

        Ok(curr_min_max.map(|(min, max)| (min.to_vec(), max.to_vec())))
    }

    fn select_files_for_l0_compaction(
        &self,
        levels: &RwLockReadGuard<'_, [Vec<SSTable>; SST_LEVEL_COUNT]>,
    ) -> Result<Option<Vec<CompactionSstSlice>>> {
        let mut vec_of_overlapping_pathbufs: Vec<&SSTable> = Vec::new();
        let ssts_in_level = &levels[0];
        if let Some((min_k, max_k)) = self.get_min_max_key_range_of_entire_level(ssts_in_level)? {
            let _ = &levels[0]
                .iter()
                .for_each(|ss| vec_of_overlapping_pathbufs.push(ss));

            if let Some(vector_of_sstables_one_level_up) = levels.get(1_usize) {
                vector_of_sstables_one_level_up.iter().for_each(|sst| {
                    KVEngine::does_overlap(sst, min_k.as_slice(), max_k.as_slice()).then(|| {
                        vec_of_overlapping_pathbufs.push(sst);
                    });
                });
            }
        }

        Ok(Some(
            vec_of_overlapping_pathbufs
                .iter()
                .map(|x| {
                    CompactionSstSlice::new(
                        x.file_path.clone(),
                        x.id,
                        Arc::clone(&x.sparse_index),
                        x.level,
                    )
                })
                .collect::<Vec<CompactionSstSlice>>(),
        ))
    }

    fn select_level_for_compaction(
        &self,
        levels: &RwLockReadGuard<'_, [Vec<SSTable>; SST_LEVEL_COUNT]>,
    ) -> Option<(f64, u8)> {
        let mut best_ratio_candidate: Option<(f64, u8)> = None; // first number is ratio, second is what level

        let l0 = &levels[0];
        let ratio = l0.len() as f64 / NUM_OF_L0_FILES_TO_TRIGGER_COMPACTION as f64;
        if ratio >= 1_f64 {
            best_ratio_candidate = Some((ratio, 0))
        }

        for level in 1..levels.len() - 1 {
            let sstables_in_level = &levels[level];

            let bytes_in_entire_level: u64 =
                sstables_in_level.iter().map(|sst| sst.file_size).sum();

            let target_to_trigger: u64 = MAX_SST_SIZE * 10_u64.pow(level as u32); // corresponds to NUM_OF_BYTES_NEEDED_TO_TRIGGER_L1_COMPACTION et al

            if bytes_in_entire_level < target_to_trigger {
                continue;
            };

            let ratio_for_level = bytes_in_entire_level as f64 / target_to_trigger as f64;

            best_ratio_candidate = match best_ratio_candidate {
                None => {
                    if ratio_for_level >= 1_f64 {
                        Some((ratio_for_level, level as u8))
                    } else {
                        None
                    }
                }
                curr @ Some((curr_ratio, _)) => {
                    if curr_ratio < ratio_for_level {
                        Some((ratio_for_level, level as u8))
                    } else {
                        curr
                    }
                }
            }
        }

        best_ratio_candidate
    }

    fn select_files_for_compaction(
        &self,
        levels: &RwLockReadGuard<'_, [Vec<SSTable>; SST_LEVEL_COUNT]>,
        level: u8,
    ) -> Result<Option<Vec<CompactionSstSlice>>> {
        let mut best_candidate: Option<(u64, &SSTable)> = None; // will be made into a struct later
        let mut files_to_compact: Option<Vec<&SSTable>> = None;
        // first  value is the size of the sum of bytes of all files(a level up) that overlap with SSTable
        // TODO: Remember to mark files picked for compaction

        if level == 0 {
            return self.select_files_for_l0_compaction(levels);
        }

        let Some(curr_level_sstables) = levels.get(level as usize) else {
            return Ok(None);
        };

        let Some(vector_of_sstables_one_level_up) = levels.get((level + 1) as usize) else {
            return Ok(None);
        };
        for sstable in curr_level_sstables.iter() {
            let mut temp_curr_sum_of_file_sizes: u64 = 0;
            let (min_k, max_k) = sstable.min_max_keys.as_ref().unwrap(); // it will always be here, we make sure in load()

            // keep curr sstable, and go up a level, find every single file that overlaps, do the math, if better than curr best_candidate, switch

            let mut vec_of_overlapping_pathbufs: Vec<&SSTable> = Vec::new();
            vector_of_sstables_one_level_up.iter().for_each(|sst| {
                KVEngine::does_overlap(sst, min_k, max_k).then(|| {
                    // we should also push to the vector of Pathbufs here
                    vec_of_overlapping_pathbufs.push(sst);
                    temp_curr_sum_of_file_sizes += sst.file_size;
                });
            });

            match best_candidate {
                Some((bytes, sst)) => {
                    if (bytes as f64 / sst.file_size as f64) // RATIO used to determine candidate, we want the lower ration because theres less write ampl
                            > (temp_curr_sum_of_file_sizes as f64 / sstable.file_size as f64)
                    {
                        best_candidate = Some((temp_curr_sum_of_file_sizes, sstable));
                        vec_of_overlapping_pathbufs.push(sstable);
                        files_to_compact = Some(vec_of_overlapping_pathbufs)
                    }
                }
                None => {
                    best_candidate = Some((temp_curr_sum_of_file_sizes, sstable));
                    vec_of_overlapping_pathbufs.push(sstable);
                    files_to_compact = Some(vec_of_overlapping_pathbufs)
                }
            }
        }

        if let Some(files) = files_to_compact {
            Ok(Some(
                files
                    .iter()
                    .map(|x| {
                        CompactionSstSlice::new(
                            x.file_path.clone(),
                            x.id,
                            Arc::clone(&x.sparse_index),
                            x.level,
                        )
                    })
                    .collect::<Vec<CompactionSstSlice>>(),
            ))
        } else {
            Ok(None)
        }
    }

    fn compact(&mut self) -> Result<Option<bool>> {
        if self.compaction_manager.is_busy() {
            return Ok(None);
        }

        if let Some(levels) = &self.sstables {
            let levels = levels.read().unwrap();

            let level = match self.select_level_for_compaction(&levels) {
                Some((_, level)) => level,
                None => return Ok(None),
            };

            let files = match self.select_files_for_compaction(&levels, level) {
                Ok(Some(files)) => files,
                Err(e) => return Err(e),
                Ok(None) => return Ok(None),
            };
            let compaction_job = CompactionJob::new(
                files,
                level + 1,
                self.data_directory.clone(),
                Arc::clone(&self.hlc),
            );

            // if this returns an error, because one or more of the files are corrupt, trying again will just error again
            // mark them

            self.compaction_manager.start(compaction_job)?; // if this throws an error, theres another compaction running so just throw away, compact will be called again
        } // 

        Ok(Some(true))
    }

    fn add_sstable_to_l0(&mut self, sstable: SSTable) {
        if let Some(levels) = &self.sstables {
            let mut levels = levels.write().unwrap();
            levels[0].push(sstable);
            levels[0].sort_by_key(|s| Reverse(s.id));
        }
    }

    fn handle_flushing_message(&mut self, msg: FlushingThreadResponse) -> Result<()> {
        match msg {
            FlushingThreadResponse::Success { id, sstable } => {
                if let Some(frozen_instance) = self.frozen_memtables.get_mut(&id) {
                    frozen_instance.sstable = Some(sstable);
                }
            }
            FlushingThreadResponse::Error { id, error } => {
                if let Some(instance) = self.frozen_memtables.get_mut(&id) {
                    instance.flush_attempts += 1;
                    if instance.flush_attempts <= self.options.max_flush_retries {
                        let _ = fs::remove_file(self.data_directory.join(format!("{id}.sst")));
                        self.flushing_manager.background_flush_memtable(
                            FrozenMemtableInstance {
                                memtable: instance.memtable.clone(),
                                sstable: instance.sstable.take(),
                                id: instance.id,
                                wal_id: instance.wal_id,
                                flush_attempts: instance.flush_attempts,
                            },
                            self.data_directory.clone(),
                        )?;
                    } else {
                        self.fail(format!("memtable {id} failed to flush after {MAX_FLUSH_ATTEMPTS} attempts. \
                                            No later memtable can retire behind it, so memory will keep growing. \
                                            Its data is still in its WAL and will be recovered on reopen: {error}"));
                        return Err(error);
                    }
                }
            } // use self.fail here but retry first
        };
        Ok(())
    }

    fn retire_frozen_memtables(&mut self) -> Result<bool> {
        let mut should_check_for_compaction = false;
        while let Some(first) = self.frozen_memtables.first_entry() {
            let Some(sst_id) = first.get().sstable.as_ref().map(|sst| sst.id) else {
                // sstable there = finished
                break;
            };

            let instance = first.remove();
            let wal_id = instance.wal_id;
            if let Err(e) = self.manifest.edit_and_append(&ManifestEdit {
                new_files: vec![(0, sst_id)],
                deleted_files: vec![],
                min_live_wal: Some(wal_id + 1),
            }) {
                self.frozen_memtables.insert(instance.id, instance); // put it back on failure or we lose access to it from first.remove() ^
                self.fail(format!(
                    "manifest commit failed for the flush of sst {sst_id} (wal {wal_id}), \
                 so no flush can be published and memtables cannot retire. \
                  The WAL is intact and replays on reopen: {e}"
                ));
                return Err(e);
            }

            self.add_sstable_to_l0(instance.sstable.unwrap()); // safe unwrap
            should_check_for_compaction = true;
            let _ = fs::remove_file(self.data_directory.join(format!("{}.wal", wal_id)));
        }

        Ok(should_check_for_compaction)
    }

    fn finalize_compaction(&mut self, result: Result<CompactionOutcome>) -> Result<bool> {
        let mut should_check_for_compaction = false;
        match result {
            Ok(compaction_outcome) => {
                let edit = ManifestEdit {
                    new_files: compaction_outcome
                        .final_sst_files
                        .iter()
                        .map(|x| (compaction_outcome.level_for_output_sst, *x))
                        .collect(),
                    deleted_files: compaction_outcome.consumed_sst_files.clone(),
                    min_live_wal: None,
                };

                let mut outputs = Vec::with_capacity(compaction_outcome.final_sst_files.len());
                for file_id in compaction_outcome.final_sst_files {
                    // fs::rename(tmp, &final_path)?;
                    outputs.push(SSTable::load(
                        &self.data_directory.join(format!("{file_id}.sst")),
                    )?);
                }
                File::open(&self.data_directory)?.sync_all()?;
                if let Err(e) = self.manifest.edit_and_append(&edit) {
                    self.fail(format!( "manifest commit failed for the compaction into L{}, so its outputs cannot be published. \
                        The inputs are still live and the outputs are cleaned up on reopen: {e}", compaction_outcome.level_for_output_sst ));
                    return Err(e);
                }

                if let Some(levels) = &self.sstables {
                    let mut levels = levels.write().unwrap();
                    for level in levels.iter_mut() {
                        level.retain(|ss| !edit.deleted_files.contains(&(ss.level, ss.id)));
                    }

                    for sst in outputs {
                        levels[sst.level as usize].push(sst);
                    }

                    for level in levels.iter_mut() {
                        level.sort_by_key(|s| Reverse(s.id));
                    }
                }
                should_check_for_compaction = true;

                for (_, sst_id) in compaction_outcome.consumed_sst_files {
                    let _ = remove_file(self.data_directory.join(format!("{sst_id}.sst")));
                }

                Ok(should_check_for_compaction)
            }
            Err(DbError::DataCorrupted(e)) => {
                // propagated to here by a compaction failure on a corrupt file. There could be more files that are actually corrupt
                // but this is what set it off.
                self.corrupted_files.insert(e.file_path.clone());
                self.fail(format!(
        "compaction stopped on a corrupt input file; writes disabled, reads still served: {e}"
    ));
                Err(DbError::DataCorrupted(e))
            }
            Err(e) => Err(e),
        }
    }
    fn maintenance(&mut self) -> Result<()> {
        //
        // check flushing thread first

        if let Some(error_str) = &self.db_failed {
            return Err(DbError::ReadOnly(error_str.to_string()));
        }

        while let Ok(msg) = self.flushing_manager.rx.try_recv() {
            self.handle_flushing_message(msg)?;
        }

        let mut should_check_for_compaction = self.retire_frozen_memtables()?; // true means we added to L0, successful retirement

        if let Some(result) = self.compaction_manager.poll() {
            should_check_for_compaction |= self.finalize_compaction(result)?; // overwriting the value
        }

        if should_check_for_compaction {
            self.compact()?;
        }
        Ok(())
    }
    fn fail(&mut self, msg: String) {
        if self.db_failed.is_none() {
            self.db_failed = Some(msg);
        }
    }

    fn close(mut self) -> Result<()> {
        //

        // we have to track the first error that goes wrong, for logging reasons and because we cant stop the close just because there was an error

        let mut error: Option<DbError> = None;
        if let Err(e) = self.wal.sync() {
            self.fail(format!("WAL sync failed during close: {e}"));
            error.get_or_insert(e);
        }

        loop {
            // looping because we might have an error on flushing, so we retry which means we push another handle to the flushing manager
            // so we have to make sure we drain all of them
            self.flushing_manager.join_all_handles();
            while let Ok(msg) = self.flushing_manager.rx.try_recv() {
                if let Err(e) = self.handle_flushing_message(msg) {
                    error.get_or_insert(e);
                }
            }
            if self.flushing_manager.in_flight.is_empty() {
                break;
            }
        }
        let compaction = self.compaction_manager.wait_for_handle_finish();
        if self.db_failed.is_none() // wals are on disk so if db has failed we skip
            && let Err(e) = self.retire_frozen_memtables()
        {
            error.get_or_insert(e);
        }
        if self.db_failed.is_none()
            && let Some(result) = compaction
            && let Err(e) = self.finalize_compaction(result)
        {
            error.get_or_insert(e);
        }

        if let Some(e) = error {
            return Err(e);
        }

        if let Some(msg) = self.db_failed.take() {
            return Err(DbError::ReadOnly(msg)); // when we called close, the engine had failed already so if we dont return that here
            // we might lose the original reason of the fail
        }
        Ok(())
    }
}

/*Notes:
 // footer is : sparse_index | bloom_filter | min key | max key |  sparse_index_offset| sizeof(sparse_index) | sizeof(bloom_filter) | sizeof(minkey) | sizeof(maxkey) | LevelofSST(1 byte) | sparse_crc(4 bytes) | bloom_crc(4 bytes) | min_max_key_crc(4) | metadata_crc(4 bytes) |

DataBlocks:  [ tstamp(8) | ksz(8) | value_sz(8) | tombstone | key | value |  ] ... crc(4) (crc for the entire datablock);
    pub size: usize,
SSTable: Datablock1 | DataBlock2 ... Datablock N | Footer
Bloom filter: k-hash bit array per SSTable to skip files on negative lookups. Use 10 bits per key. Built during flush of AVL.
*/

// SparseIndex => [ firskey:[offset, datablock_length] ]

// TODO: have SparseIndex be -> [firstkey: [offsert(points to start of data_block)] ]
// then have the data_block be datablock_length | record1| record2 ... crc
// this way we can reconstruct the sparse_index in the case of a sparse_index corruption because we can just read the records front to back without the need for the sparse_index

// wal record looks like: ksz, vsz, k, v, crc(4 bytes)
// When you read a data block in the sparse index, remember to account for the 4 crc bytes yourself, they are not accounted forin the length
/*







 For WAL records, we have deletion and insertion types so far. Will use one byte to define type. 00000100(4) = INSERTION. 00000010(2) = DELETION.
 serialized should look like this: TYPE | RECORD
 WAL RECORD can be tstamp | ksz | key |crc (4 bytes) OR it can be  | tstamp | ksz |vsz | key | value | crc(4 bytes)

 PROBLEM/UPDATE: make Bufreaders with capacity instead
 TODO: Modularize the components into their own files
 TODO.1: Document how things work


Atomics u64

// TODO: when main receives a successful sync from mebtable -> sst, check if curr number of L0 ssts >= NUM_OF_L0_FILES_TO_TRIGGER_COMPACTION, true -> trigger

TODO: USE read_exact_at from FileExt trait in place of every read_exact call()
// chekc static vs dynamic level sizing(rocksdb)
// TODO: make sure to document the different parsers and how records are written in different formats in some places. One change somewhere can break things in other palces
// use consts no magic ns
// TODO: WRITE TESTS
 //

*/
