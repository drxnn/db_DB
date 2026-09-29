use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{JoinHandle, spawn};

use crate::constants::{
    CRC_LEN, KEY_MAX_BYTES_SIZE, TAG_DELETION, TAG_INSERTION, TAG_LEN, U64_LEN,
    VALUE_MAX_BYTES_SIZE,
};
use crate::errors::{CorruptionType, CrcType, DataCorruptedErr, DbError, Result};
use crate::helpers::{CRC32, check_crc, read_exact_or_truncated};
use crate::memtable::AVL;
use crate::sstable::SSTable;
use crate::wal::{WalReplayState, WalToMemtableReplay};

pub enum FlushingThreadResponse {
    Success { id: u64, sstable: SSTable },
    Error { id: u64, error: DbError },
}

pub(crate) struct FlushingManager {
    tx: Sender<FlushingThreadResponse>,
    pub(crate) rx: Receiver<FlushingThreadResponse>,
    pub(crate) in_flight: Vec<JoinHandle<Result<()>>>,
}

impl FlushingManager {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel::<FlushingThreadResponse>();
        Self {
            tx,
            rx,
            in_flight: Vec::new(),
        }
    }

    // main will poll and on success, will add the SST to active memory and delete old_wal from directory
    pub(crate) fn background_flush_memtable(
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

    pub(crate) fn build_avl_from_wal(
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

    pub(crate) fn join_all_handles(&mut self) {
        for handle in self.in_flight.drain(..) {
            let _ = handle.join();
        }
    }
}

pub(crate) struct FrozenMemtableInstance {
    pub(crate) memtable: Arc<AVL>,
    pub(crate) sstable: Option<SSTable>, // it should hold sstables until it is safe for it to be retired(when after every older memtable is gone from the BtreeMap)
    pub(crate) id: u64,                  // hlc
    pub(crate) wal_id: u64,
    pub(crate) flush_attempts: u8,
}
