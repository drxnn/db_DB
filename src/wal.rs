use std::fs::{File, OpenOptions, remove_file};
use std::io::{self, BufReader, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::constants::{
    CRC_LEN, KEY_MAX_BYTES_SIZE, TAG_DELETION, TAG_INSERTION, TAG_LEN, U64_LEN,
    VALUE_MAX_BYTES_SIZE,
};
use crate::errors::{CorruptionType, CrcType, DataCorruptedErr, DbError, Result};
use crate::helpers::{CRC32, check_crc, read_exact_or_truncated};
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
    pub(crate) fn build_avl_from_wal(
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

#[cfg(test)]
mod tests {

    use crate::{
        constants::MAX_MEMTABLE_THRESHOLD,
        lsm::Lookup::{Deleted, Found},
        test_utils::flip_bit_at,
    };
    use std::{assert_eq, collections::HashMap, fs, matches};

    use super::*;
    use tempfile::{TempDir, tempdir};
    const WAL_ID: u64 = 1;

    const BIG_VALUE: [u8; 20_000] = [b'x'; 20_000];

    const WAL_RECORDS: &[(&[u8], Option<&[u8]>)] = &[
        (b"user:1", Some(b"alice")),
        (b"user:2", Some(b"bob")),
        (b"user:1", Some(b"alice-updated")),
        (b"user:2", None),
        (b"user:3", None),
        (b"empty", Some(b"")),
        (b"bin\x00key", Some(b"\x00\x02\x04\xff")), // tag bytes
        (b"big", Some(&BIG_VALUE)),
        (b"user:4", Some(b"dave")), // a record after the big one
    ];
    fn create_wal() -> (TempDir, WAL) {
        let dir = tempdir().unwrap();
        let wal = WAL::new(MAX_MEMTABLE_THRESHOLD, SyncConfig::None, dir.path(), WAL_ID).unwrap();
        (dir, wal)
    }

    // write a bunch of records, replay wal

    #[test]
    fn writes_and_replays_records_from_wal() {
        let mut latest_records: HashMap<&[u8], Option<&[u8]>> = HashMap::new();
        let (_dir, mut wal) = create_wal();
        for (ts, (key, value)) in WAL_RECORDS.iter().enumerate() {
            let record = match value {
                Some(v) => WalRecordType::Insertion(key, v),
                None => WalRecordType::Deletion(key),
            };
            latest_records.insert(key, *value); // for the test below
            wal.record_to_wal(record, ts as u64).unwrap();
        }

        let replay = WAL::build_avl_from_wal(&wal.path, MAX_MEMTABLE_THRESHOLD).unwrap();
        assert!(matches!(replay.replay_state, WalReplayState::Clean));

        for (k, v) in latest_records {
            let expected = match v {
                Some(v) => Found(v.to_vec()),
                None => Deleted,
            };
            assert_eq!(replay.memtable.get(k), expected,);
        }

        assert_eq!(replay.records_recovered, WAL_RECORDS.len() as u64);
        assert_eq!(replay.most_recent_hlc, Some(WAL_RECORDS.len() as u64 - 1));
        assert_eq!(replay.valid_bytes, fs::metadata(&wal.path).unwrap().len());
    }

    #[test]

    fn record_with_bad_type_tag_stops_replay() {
        let (_dir, mut wal) = create_wal();
        for (ts, (key, value)) in WAL_RECORDS.iter().enumerate() {
            let record = match value {
                Some(v) => WalRecordType::Insertion(key, v),
                None => WalRecordType::Deletion(key),
            };
            wal.record_to_wal(record, ts as u64).unwrap();
        }
        wal.sync().unwrap(); // to pass the test
        flip_bit_at(&wal.path, 0); // bit of first tag is flipped
        let replay = WAL::build_avl_from_wal(&wal.path, MAX_MEMTABLE_THRESHOLD).unwrap();
        assert!(matches!(
            replay.replay_state,
            WalReplayState::PartialError(DbError::DataCorrupted(DataCorruptedErr {
                reason: CorruptionType::RecordTypeCorrupted { found: _ },
                ..
            }))
        ));
        assert_eq!(replay.records_recovered, 0); // corrupted the first record
    }
}
