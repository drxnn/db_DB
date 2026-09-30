use std::fs::File;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{JoinHandle, spawn};

use crate::errors::{DbError, Result};
use crate::memtable::AVL;
use crate::sstable::SSTable;

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
