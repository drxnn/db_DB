use std::fs::{File, OpenOptions, remove_file};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::constants::{TAG_DELETION, TAG_INSERTION};
use crate::errors::{DbError, Result};
use crate::helpers::CRC32;
use crate::lsm::SyncConfig::{self, Always, Every};

use crate::memtable::AVL;

pub(crate) struct WAL {
    pub(crate) id: u64,
    wal_writer: Option<BufWriter<File>>,
    sync_c: SyncConfig,
    record_buffer: Vec<u8>,
    threshold: u64,
    pub(crate) path: PathBuf,
    last_sync: Instant,
}

impl WAL {
    pub(crate) fn new(
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
    pub(crate) fn sync(&mut self) -> Result<()> {
        let writer = self.wal_writer.as_mut().ok_or(DbError::WalNotFound)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        Ok(())
    }

    pub(crate) fn record_to_wal<'a>(
        &mut self,
        record: WalRecordType<'a>,
        timestamp: u64,
    ) -> Result<()> {
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

pub enum WalReplayState {
    Clean,                 // replayed everything to mem
    PartialError(DbError), // either truncation or corruption
}
pub struct WalToMemtableReplay {
    pub(crate) memtable: AVL,
    pub(crate) records_recovered: u64,
    pub(crate) valid_bytes: u64,
    pub(crate) most_recent_hlc: Option<u64>,
    pub(crate) replay_state: WalReplayState,
}

#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub enum WalRecoveryMode {
    PointInTime, // stop at the first damage, skip every newer WAL, open at that moment(loses data but recovers)
    AbsoluteConsistency, // any damage at all, even a torn tail, is an error
}

pub(crate) enum WalRecordType<'a> {
    Deletion(&'a [u8]),            // ( key )
    Insertion(&'a [u8], &'a [u8]), // (key, value)
}
