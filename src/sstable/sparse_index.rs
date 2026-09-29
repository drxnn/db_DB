use std::path::PathBuf;

use crate::constants::{DATA_BLOCK_MAX_BYTES_SIZE, KEY_MAX_BYTES_SIZE, U64_LEN};
use crate::errors::{CorruptionType, DataCorruptedErr, DbError, Result};
use crate::helpers::{read_range, read_u64};

pub struct SparseIndex {
    pub index_entries: Vec<u8>,
    pub size: u64,
}
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

    pub(crate) fn parse_sparse_index(b: &[u8], path: PathBuf) -> Result<Vec<(Vec<u8>, u64, u64)>> {
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
