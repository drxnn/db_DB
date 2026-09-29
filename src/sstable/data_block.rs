use std::io::Cursor;

use crate::constants::{DATA_BLOCK, RECORD_HEADER_LEN, RECORD_KSZ_OFFSET, RECORD_VSZ_OFFSET};
use crate::errors::{DbError, Result};
use crate::helpers::{CRC32, read_range, read_u64};

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
