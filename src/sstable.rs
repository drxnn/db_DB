mod bloom_filter;
mod data_block;
mod sparse_index;

pub use bloom_filter::BloomFilter;
pub use data_block::SsTableDataBlock;
pub use sparse_index::SparseIndex;

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::constants::{
    BLOOM_WORD_BITS, CRC_LEN, DATA_BLOCK_MAX_BYTES_SIZE, FOOTER_BLOOM_LEN_START, FOOTER_FIXED_LEN,
    FOOTER_LEN, FOOTER_LEVEL_START, FOOTER_MAX_KEY_LEN_START, FOOTER_MIN_KEY_LEN_START,
    FOOTER_SPARSE_LEN_START, FOOTER_SPARSE_OFFSET_START, LEVEL_LEN, RECORD_HEADER_LEN,
    RECORD_KSZ_OFFSET, RECORD_TOMBSTONE_OFFSET, RECORD_VSZ_OFFSET, SST_LEVEL_COUNT,
    TOMBSTONE_DELETED, U64_LEN,
};
use crate::errors::CorruptionType::{self, Other, SstLevelMalformed};
use crate::errors::{CrcType, DataCorruptedErr, DbError, Result};
use crate::helpers::{CRC32, check_crc, get_hashed_key_positions, read_range, read_u8, read_u64};
use crate::lsm::Lookup::{self, Absent, Deleted, Found};
use std::cmp::Ordering as CmpOrdering;

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

        let footer = Footer::parse(&footer)?;

        if footer.level as usize >= SST_LEVEL_COUNT {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: file_metadata.len() - (FOOTER_LEN - FOOTER_LEVEL_START) as u64, // - 17
                file_path: path.to_path_buf(),
                reason: SstLevelMalformed(footer.level as usize),
            }));
        }

        let full_data_length = footer
            .sparse_index_len
            .checked_add(footer.bloom_len)
            .and_then(|x| x.checked_add(footer.min_key_len))
            .and_then(|x| x.checked_add(footer.max_key_len))
            .ok_or({
                DbError::DataCorrupted(DataCorruptedErr {
                    offset: footer.sparse_index_offset,
                    file_path: path.to_path_buf(),
                    reason: CorruptionType::MetadataSizeOverflow {
                        sizes: [
                            footer.sparse_index_len,
                            footer.bloom_len,
                            footer.min_key_len,
                            footer.max_key_len,
                        ],
                    },
                })
            })?;

        // check_key_value_record_does_not_exceed_max(size, max_size, offset, file_path)
        // TODO: have the helper function above work with different kinds of data_corruption // not just k/v record check
        if full_data_length > file_len {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset: footer.sparse_index_offset,
                file_path: path.to_path_buf(),
                reason: CorruptionType::MetaDataSizeExceedsFileSize {
                    file_size: file_len,
                    metadata_size: full_data_length,
                },
            }));
        }
        let full_data_length = full_data_length as usize;

        f.seek(SeekFrom::Start(footer.sparse_index_offset))?;
        let mut full_sst_data = vec![0u8; full_data_length];
        f.read_exact(&mut full_sst_data)?;
        let bloom_filter_start = footer.sparse_index_len;
        let bloom_filter_end = bloom_filter_start + footer.bloom_len;
        let min_k_start = bloom_filter_end;
        let min_k_end = min_k_start + footer.min_key_len;
        let max_k_start = min_k_end;
        let max_k_end = max_k_start + footer.max_key_len;

        // let sparse_index: &[u8] = &full_sst_data[0..(size_of_sparse_index as usize)];
        let sparse_index: &[u8] = read_range(&full_sst_data, 0, footer.sparse_index_len as usize)?;
        let sparse_index_crc_check = CRC32.compute_crc_data_block(sparse_index);

        check_crc(
            sparse_index_crc_check,
            sparse_index_crc_in_file,
            footer.sparse_index_offset,
            path,
            CrcType::SparseIndex,
        )?;

        let bloom_filter: &[u8] = read_range(
            &full_sst_data,
            bloom_filter_start as usize,
            bloom_filter_end as usize,
        )?;

        let bloom_filter_crc_check = CRC32.compute_crc_data_block(bloom_filter);

        // &full_sst_data[(bloom_filter_start as usize)..(bloom_filter_end as usize)];
        let min_key = read_range(&full_sst_data, min_k_start as usize, min_k_end as usize)?;
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
            level: footer.level,
            currently_picked_for_compaction: false,
        };
        if sstable.min_max_keys.is_none() {
            sstable.rebuild_min_max_key_from_sparse_index()?;
        }
        Ok(sstable)
    }

    pub(crate) fn should_search_sstable_file(&self, key: &[u8]) -> bool {
        if let Some((min, max)) = &self.min_max_keys
            && (key > max.as_slice() || key < min.as_slice())
        {
            return false;
        }

        if let Some(bloom_filter) = &self.bloom_filter {
            let bf_bit_positions = get_hashed_key_positions(key, bloom_filter.num_bits as usize);
            bloom_filter.check_bits(bf_bit_positions)
        } else {
            true // If we do not have a bloom filter, we just search the file without bloom filter optimization
        }
    }
    pub(crate) fn search_kv_in_sstable(&self, key: &[u8]) -> Result<Lookup> {
        let Some((offset, data_len)) = self.binary_search_sparse_index(key) else {
            return Ok(Absent);
        };

        if data_len > DATA_BLOCK_MAX_BYTES_SIZE {
            return Err(DbError::DataCorrupted(DataCorruptedErr {
                offset,
                file_path: self.file_path.clone(),
                reason: CorruptionType::BufferExceedsMaxLength {
                    size: data_len,
                    max_size: DATA_BLOCK_MAX_BYTES_SIZE,
                },
            }));
        }

        let mut data_block_buffer_and_crc = vec![0u8; data_len as usize + CRC_LEN];
        self.file
            .read_exact_at(&mut data_block_buffer_and_crc, offset)?;
        let (data_buffer, crc) = data_block_buffer_and_crc.split_at(data_len as usize);
        let crc_from_buff = u32::from_le_bytes(crc.try_into().unwrap());

        //
        // we read CRC here because data_len above doesnt take into account the 4 bytes for crc

        let fresh_crc = CRC32.compute_crc_data_block(data_buffer);

        check_crc(
            fresh_crc,
            crc_from_buff,
            offset,
            &self.file_path,
            CrcType::DataBlock,
        )?;

        let mut pos = 0;
        while pos < data_buffer.len() {
            if pos + RECORD_HEADER_LEN > data_buffer.len() {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: offset + pos as u64,
                    file_path: self.file_path.clone(),
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
                        file_path: self.file_path.clone(),
                        reason: CorruptionType::Other(format!(
                            "record size overflow: ksz={ksz}, vsz={vsz}"
                        )),
                    })
                })?;

            if val_end > data_buffer.len() {
                return Err(DbError::DataCorrupted(DataCorruptedErr {
                    offset: offset + pos as u64,
                    file_path: self.file_path.clone(),
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

pub(crate) struct Footer {
    pub(crate) sparse_index_offset: u64,
    pub(crate) sparse_index_len: u64,
    pub(crate) bloom_len: u64,
    pub(crate) min_key_len: u64,
    pub(crate) max_key_len: u64,
    pub(crate) level: u8,
}

impl Footer {
    pub(crate) fn serialize(&self) -> [u8; FOOTER_FIXED_LEN] {
        let mut footer: [u8; FOOTER_FIXED_LEN] = [0u8; FOOTER_FIXED_LEN];

        footer[FOOTER_SPARSE_OFFSET_START..FOOTER_SPARSE_LEN_START]
            .copy_from_slice(&self.sparse_index_offset.to_le_bytes());

        footer[FOOTER_SPARSE_LEN_START..FOOTER_BLOOM_LEN_START]
            .copy_from_slice(&self.sparse_index_len.to_le_bytes());

        footer[FOOTER_BLOOM_LEN_START..FOOTER_MIN_KEY_LEN_START]
            .copy_from_slice(&self.bloom_len.to_le_bytes());

        footer[FOOTER_MIN_KEY_LEN_START..FOOTER_MAX_KEY_LEN_START]
            .copy_from_slice(&self.min_key_len.to_le_bytes());

        footer[FOOTER_MAX_KEY_LEN_START..FOOTER_LEVEL_START]
            .copy_from_slice(&self.max_key_len.to_le_bytes());

        footer[FOOTER_LEVEL_START..FOOTER_FIXED_LEN].copy_from_slice(&self.level.to_le_bytes());

        footer
    }

    pub(crate) fn parse(bytes: &[u8; FOOTER_FIXED_LEN]) -> Result<Footer> {
        Ok(Footer {
            sparse_index_offset: read_u64(bytes, FOOTER_SPARSE_OFFSET_START)?,
            sparse_index_len: read_u64(bytes, FOOTER_SPARSE_LEN_START)?,
            bloom_len: read_u64(bytes, FOOTER_BLOOM_LEN_START)?,
            min_key_len: read_u64(bytes, FOOTER_MIN_KEY_LEN_START)?,
            max_key_len: read_u64(bytes, FOOTER_MAX_KEY_LEN_START)?,
            level: read_u8(bytes, FOOTER_LEVEL_START)?,
        })
    }
}

#[cfg(test)]
mod tests {

    use std::{assert_eq, format, matches};

    use super::*;

    // test sstables methods, then make create a valid sst file and load it, then test the fields are correct
    // then flip a bit in crcs to make sure we throw err
    use std::fs::{self, OpenOptions};
    use std::path::PathBuf;

    use tempfile::{TempDir, tempdir};

    use crate::constants::{
        FOOTER_BLOOM_CRC_START, FOOTER_FIELDS_CRC_START, FOOTER_LEN, FOOTER_MIN_MAX_CRC_START,
        FOOTER_SPARSE_CRC_START, MAX_MEMTABLE_THRESHOLD,
    };

    use crate::memtable::AVL;
    use crate::test_utils::flip_bit_at;
    const RECORDS: &[(&[u8], Option<&[u8]>)] = &[
        (b"mango", Some(b"yellow")),
        (b"apple", Some(b"red")),
        (b"cherry", Some(b"dark red")),
        (b"banana", Some(b"yellow")),
        (b"apple", Some(b"green")), // overwrite newer value wins
        (b"cherry", None),          // delete of an existing key
        (b"kiwi", None),            // delete of a key we never put
        (b"0-never-put", None),     // a tombstone that is also the smallest key()
        (b"a", Some(b"prefix of apple")),
        (b"app", Some(b"also a prefix")),
        (b"empty-value", Some(b"")), // empty value, which is not a delete
        (
            b"zebra-this-key-is-deliberately-longer-than-the-57-byte-sst-trailer",
            Some(b"long"),
        ),
        (b"banana", None),
        (b"banana", Some(b"back again")),
    ];

    fn write_sst() -> (TempDir, PathBuf) {
        let dir = tempdir().unwrap();
        let mut memtable = AVL::new(MAX_MEMTABLE_THRESHOLD);

        for (ts, (key, value)) in RECORDS.iter().enumerate() {
            match value {
                Some(v) => memtable.put(key, v, ts as u64),
                None => memtable.delete(key, ts as u64),
            }
        }
        let (_, path) = memtable.sync_avl(dir.path(), 10).unwrap().unwrap();
        (dir, path)
    }

    fn write_sst_with_multiple_data_blocks() -> (TempDir, PathBuf) {
        let dir = tempdir().unwrap();
        let mut memtable = AVL::new(MAX_MEMTABLE_THRESHOLD);

        for i in 0..1000_u64 {
            memtable.put(
                format!("key{i:05}").as_bytes(),
                format!("value{i:0100}").as_bytes(),
                i,
            );
        }
        let (_, path) = memtable.sync_avl(dir.path(), 10).unwrap().unwrap();
        (dir, path)
    }

    #[test]
    fn loads_sst() {
        let (dir, path_to_sst) = write_sst();
        let loaded_ss = SSTable::load(&path_to_sst).unwrap();
        let (min_k, max_k) = loaded_ss.min_max_keys.unwrap();
        assert_eq!(
            &max_k,
            b"zebra-this-key-is-deliberately-longer-than-the-57-byte-sst-trailer"
        );
        assert_eq!(&min_k, b"0-never-put");
        assert_eq!(loaded_ss.level, 0);
        assert_eq!(
            loaded_ss.file_size,
            fs::metadata(&path_to_sst).unwrap().len()
        );
        assert!(loaded_ss.bloom_filter.is_some());
        assert_eq!(loaded_ss.sparse_index.len(), 1); // everything we put is in 1 data block
        assert_eq!(loaded_ss.id, 10);
    }

    #[test]
    // flip for all metadata, then flip random records
    // TODO: use helpers here
    fn flipping_bit_in_sst_footer_crc_fails_to_load_sst() {
        let (dir, path_to_sst) = write_sst();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path_to_sst)
            .unwrap();
        let file_len = f.metadata().unwrap().len();

        flip_bit_at(
            &path_to_sst,
            file_len - (FOOTER_LEN - FOOTER_FIELDS_CRC_START) as u64,
        );

        assert!(matches!(
            SSTable::load(&path_to_sst),
            Err(DbError::DataCorrupted(DataCorruptedErr {
                reason: CorruptionType::CrcMismatch {
                    mismatch_type: CrcType::SstFooterMetadata,
                    ..
                },
                ..
            }))
        ));
    }
    #[test]
    fn flipping_bit_in_sst_bloom_crc_drops_the_bloom() {
        let (dir, path_to_sst) = write_sst();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path_to_sst)
            .unwrap();
        let file_len = f.metadata().unwrap().len();

        flip_bit_at(
            &path_to_sst,
            file_len - (FOOTER_LEN - FOOTER_BLOOM_CRC_START) as u64,
        );

        let loaded_ss = SSTable::load(&path_to_sst).unwrap();
        assert!(loaded_ss.bloom_filter.is_none()) // if bloom crc is bad, we just skip it
    }
    #[test]
    fn flipping_bit_in_sst_minmax_crc_rebuilds_minmax() {
        let (dir, path_to_sst) = write_sst();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path_to_sst)
            .unwrap();
        let file_len = f.metadata().unwrap().len();

        flip_bit_at(
            &path_to_sst,
            file_len - (FOOTER_LEN - FOOTER_MIN_MAX_CRC_START) as u64,
        );

        let loaded_ss = SSTable::load(&path_to_sst).unwrap();
        assert!(loaded_ss.min_max_keys.is_some());
        assert_eq!(
            loaded_ss.min_max_keys,
            Some((
                b"0-never-put".to_vec(),
                b"zebra-this-key-is-deliberately-longer-than-the-57-byte-sst-trailer".to_vec(),
            ))
        );
    }
    #[test]
    fn flipping_bit_in_sst_sparse_crc_fails_to_load_sst() {
        let (dir, path_to_sst) = write_sst();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path_to_sst)
            .unwrap();
        let file_len = f.metadata().unwrap().len();

        // let mut footer_crc = [0u8; 4];
        // f.read_exact_at(
        //     &mut footer_crc,
        //     file_len - (FOOTER_LEN - FOOTER_SPARSE_CRC_START) as u64,
        // )
        // .unwrap();
        // footer_crc[2] ^= 0x01;
        // f.write_all_at(
        //     &footer_crc,
        //     file_len - (FOOTER_LEN - FOOTER_SPARSE_CRC_START) as u64,
        // )
        // .unwrap();
        flip_bit_at(
            &path_to_sst,
            file_len - (FOOTER_LEN - FOOTER_SPARSE_CRC_START) as u64,
        );

        assert!(matches!(
            SSTable::load(&path_to_sst),
            Err(DbError::DataCorrupted(DataCorruptedErr {
                reason: CorruptionType::CrcMismatch {
                    mismatch_type: CrcType::SparseIndex,
                    ..
                },
                ..
            }))
        ));
    }

    #[test]

    fn flipping_bit_on_a_record_returns_crc_mismatch_when_search() {
        let (dir, path_to_sst) = write_sst();
        let sstable = SSTable::load(&path_to_sst).unwrap();
        // flipping a random bit in any of the records will fail here since all the test records are in one data block
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path_to_sst)
            .unwrap();

        let mut byte = [0u8; 1];
        f.read_exact_at(&mut byte, 15).unwrap();
        byte[0] ^= 0x01;
        f.write_all_at(&byte, 15).unwrap();
        assert!(matches!(
            sstable.search_kv_in_sstable(b"banana"),
            Err(DbError::DataCorrupted(DataCorruptedErr {
                reason: CorruptionType::CrcMismatch {
                    mismatch_type: CrcType::DataBlock,
                    ..
                },
                ..
            }))
        ))
    }
    #[test]
    fn every_key_returns_its_latest_state() {
        let (_dir, path_to_sst) = write_sst();
        let sst = SSTable::load(&path_to_sst).unwrap();

        let expected: &[(&[u8], Lookup)] = &[
            (b"0-never-put", Deleted),
            (b"a", Found(b"prefix of apple".to_vec())),
            (b"app", Found(b"also a prefix".to_vec())),
            (b"apple", Found(b"green".to_vec())),
            (b"banana", Found(b"back again".to_vec())),
            (b"cherry", Deleted),
            (b"empty-value", Found(Vec::new())),
            (b"kiwi", Deleted),
            (b"mango", Found(b"yellow".to_vec())),
            (
                b"zebra-this-key-is-deliberately-longer-than-the-57-byte-sst-trailer",
                Found(b"long".to_vec()),
            ),
            (b"", Absent),
            (b"aa", Absent),
            (b"b", Absent),
            (b"zzz", Absent),
        ];
        for (key, expected_result) in expected {
            assert_eq!(&sst.search_kv_in_sstable(key).unwrap(), expected_result);
        }
    }

    #[test]
    fn searching_sparse_index_return_correct_data_block_for_key() {
        let (_dir, path_to_sst) = write_sst_with_multiple_data_blocks();
        let sst = SSTable::load(&path_to_sst).unwrap();
        let blocks = &sst.sparse_index;
        // for each block, first_key comes first
        assert_eq!(sst.binary_search_sparse_index(b"a"), None); // doesnt exist

        // (1, 20, 40, 60)
        // 25 -> index 1
        // starting backwards, can always check if key_we_are_looking_for >= first_key_in_block
        for i in 0..1000_u64 {
            let key = &format!("key{i:05}").into_bytes();
            let (offset, length) = blocks
                .iter()
                .rev()
                .find(|(first_key, _, _)| key >= first_key)
                .map(|(_, offset, length)| (*offset, *length))
                .unwrap();

            assert_eq!(
                sst.binary_search_sparse_index(key).unwrap(),
                (offset, length)
            )
        }
    }

    #[test]
    fn should_search_sstable_returns_correct_answers() {
        let (_dir, path_to_sst) = write_sst_with_multiple_data_blocks();
        let sst = SSTable::load(&path_to_sst).unwrap();

        for i in 0..1000_u64 {
            assert!(
                sst.should_search_sstable_file(format!("key{i:05}").as_bytes()),
                "key{i:05}"
            );
        }
        assert!(!sst.should_search_sstable_file("key10001".as_bytes())); // more than max
        assert!(!sst.should_search_sstable_file("jey00001".as_bytes())); // less than mn
    }
}
