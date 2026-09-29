mod bloom_filter;
mod data_block;
mod sparse_index;

pub use bloom_filter::BloomFilter;
pub use data_block::SsTableDataBlock;
pub use sparse_index::SparseIndex;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::constants::{
    BLOOM_WORD_BITS, CRC_LEN, FOOTER_BLOOM_LEN_START, FOOTER_FIXED_LEN, FOOTER_LEN,
    FOOTER_LEVEL_START, FOOTER_MAX_KEY_LEN_START, FOOTER_MIN_KEY_LEN_START,
    FOOTER_SPARSE_LEN_START, FOOTER_SPARSE_OFFSET_START, RECORD_HEADER_LEN, RECORD_KSZ_OFFSET,
    RECORD_VSZ_OFFSET, SST_LEVEL_COUNT, U64_LEN,
};
use crate::errors::CorruptionType::{self, Other, SstLevelMalformed};
use crate::errors::{CrcType, DataCorruptedErr, DbError, Result};
use crate::helpers::{CRC32, check_crc, read_range, read_u8, read_u64};

pub struct SSTable {
    pub(crate) id: u64,
    pub(crate) file: File,
    pub(crate) file_path: PathBuf,
    pub(crate) file_size: u64,
    pub(crate) min_max_keys: Option<(Vec<u8>, Vec<u8>)>, // min_key is index 0, max_key is index 1
    pub(crate) sparse_index: Arc<Vec<(Vec<u8>, u64, u64)>>, // key | offset | datablock block length ( before CRC, which means you need to read the next 4 bytes and compute the crc)
    pub(crate) bloom_filter: Option<BloomFilter>,
    pub(crate) corrupted: bool,
    pub(crate) level: u8,
    pub(crate) currently_picked_for_compaction: bool,
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

    pub(crate) fn binary_search_sparse_index(&self, key: &[u8]) -> Option<(u64, u64)> {
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
