use std::cmp::Reverse;
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions, TryLockError, remove_file, rename};

use std::path::{Path, PathBuf};

use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::todo;

use crate::compact::{CompactionJob, CompactionManager, CompactionOutcome, CompactionSstSlice};
use crate::constants::{
    DATA_BLOCK_MAX_BYTES_SIZE, KEY_MAX_BYTES_SIZE, MAX_FLUSH_ATTEMPTS, MAX_FROZEN_MEMTABLES_LIMIT,
    MAX_L0_COMPACTION_TRIGGER, MAX_LEVEL_MULTIPLIER, MAX_MEMTABLE_THRESHOLD,
    MAX_WAIT_TIME_FOR_WRITE_IF_STALLED_IN_MS, MIN_L0_COMPACTION_TRIGGER, MIN_LEVEL_MULTIPLIER,
    RECORD_HEADER_LEN, SST_EXT, SST_LEVEL_COUNT, TMP_EXT, VALUE_MAX_BYTES_SIZE, WAL_EXT,
};

use crate::errors::{CorruptionType, CrcType, DataCorruptedErr, DbError, InvalidOptions, Result};
use crate::flush::{FlushingManager, FlushingThreadResponse, FrozenMemtableInstance};
use crate::helpers::get_hlc_from_valid_pathbuf;

use crate::hlc::Hlc;
use crate::lsm::Lookup::{Absent, Deleted, Found};
use crate::lsm::SyncConfig::Every;
use crate::manifest::{Manifest, ManifestEdit};
use crate::memtable::AVL;
use crate::sstable::SSTable;
use crate::wal::{WAL, WalRecordType, WalRecoveryMode, WalReplayState};

// WAL config for flush

#[derive(Copy, Clone)]
pub enum SyncConfig {
    None,       // fast, data can be lost
    Every(u64), // in ms
    Always,     // Ddurable
}

#[derive(PartialEq, Debug)]
pub enum Lookup {
    Found(Vec<u8>),
    Deleted,
    Absent,
}

pub struct KVEngineOptions {
    pub sync: SyncConfig,
    pub wal_recovery_mode: WalRecoveryMode,
    pub memtable_threshold: u64,
    pub max_flush_retries: u8,
    pub max_frozen_memtables: u8, // whem max is reached, pause writes until we finish at least 1
    pub max_sst_size: u64,        // default 100MB
    pub l0_compaction_trigger: usize, //default 10, cap it between 2-50
    pub max_bytes_for_level_base: u64,
    pub level_multiplier: u64, // needs to be more than 2
    pub l0_stop_writes_trigger: usize,
    pub stalled_writes_retry_max_time_in_ms: u64,
}
impl Default for KVEngineOptions {
    fn default() -> Self {
        Self {
            sync: SyncConfig::Always,
            wal_recovery_mode: WalRecoveryMode::PointInTime,
            memtable_threshold: 8 * 1024 * 1024,
            max_flush_retries: 5,
            max_frozen_memtables: 4,
            max_sst_size: 100 * 1024 * 1024,
            l0_compaction_trigger: 10,
            max_bytes_for_level_base: 10 * 100 * 1024 * 1024, // base for L1
            level_multiplier: 10,
            l0_stop_writes_trigger: 30,
            stalled_writes_retry_max_time_in_ms: 512,
        }
    }
}

impl KVEngineOptions {
    pub fn demo() -> Self {
        Self {
            memtable_threshold: 256 * 1024,
            max_sst_size: 256 * 1024,
            max_bytes_for_level_base: 512 * 1024,
            level_multiplier: 4,
            l0_compaction_trigger: 4,
            ..Default::default()
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
        if self.max_flush_retries > MAX_FLUSH_ATTEMPTS {
            return Err(InvalidOptions::MaxFlushRetriesTooLarge {
                max: MAX_FLUSH_ATTEMPTS,
                found: self.max_flush_retries,
            }
            .into());
        }

        if let Every(ms) = self.sync
            && ms == 0
        {
            return Err(InvalidOptions::SyncIntervalIsZero.into());
        }
        if self.max_sst_size < DATA_BLOCK_MAX_BYTES_SIZE {
            return Err(InvalidOptions::MaxSstSizeTooSmall {
                min: DATA_BLOCK_MAX_BYTES_SIZE,
                found: self.max_sst_size,
            }
            .into());
        }

        if !(MIN_L0_COMPACTION_TRIGGER..=MAX_L0_COMPACTION_TRIGGER)
            .contains(&self.l0_compaction_trigger)
        {
            return Err(InvalidOptions::L0CompactionTriggerOutOfRange {
                min: MIN_L0_COMPACTION_TRIGGER,
                max: MAX_L0_COMPACTION_TRIGGER,
                found: self.l0_compaction_trigger,
            }
            .into());
        }
        if !(MIN_LEVEL_MULTIPLIER..=MAX_LEVEL_MULTIPLIER).contains(&self.level_multiplier) {
            return Err(InvalidOptions::LevelMultiplierOutOfRange {
                min: MIN_LEVEL_MULTIPLIER,
                max: MAX_LEVEL_MULTIPLIER,
                found: self.level_multiplier,
            }
            .into());
        }

        if self.l0_stop_writes_trigger <= self.l0_compaction_trigger {
            return Err(
                InvalidOptions::L0WritesStopTriggerSmallerThanL0CompactionTrigger {
                    found: self.l0_stop_writes_trigger,
                    min: self.l0_compaction_trigger,
                }
                .into(),
            );
        }
        if self.max_bytes_for_level_base < self.max_sst_size {
            return Err(InvalidOptions::MaxBytesForLevelSmallerThanSstSize {
                max_bytes_for_level_base: self.max_bytes_for_level_base,
                max_sst_size: self.max_sst_size,
            }
            .into());
        }

        if self.stalled_writes_retry_max_time_in_ms > MAX_WAIT_TIME_FOR_WRITE_IF_STALLED_IN_MS {
            return Err(InvalidOptions::MaxWaitTimeForWriteIfStalledInMsTooLarge {
                found: self.stalled_writes_retry_max_time_in_ms,
            }
            .into());
        }

        Ok(())
    }
}

pub struct KVEngine {
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
    _lock: File,
}
#[derive(Debug)]
pub struct EngineStats {
    pub files_per_level: Vec<usize>,
    pub bytes_per_level: Vec<u64>,
    pub frozen_memtables: usize,
    pub active_memtable_number_of_records: u64,
}

impl KVEngine {
    pub fn stats(&self) -> EngineStats {
        let levels = self.sstables.as_ref().unwrap().read().unwrap();
        EngineStats {
            files_per_level: levels.iter().map(|l| l.len()).collect(),
            bytes_per_level: levels
                .iter()
                .map(|l| l.iter().map(|s| s.file_size).sum())
                .collect(),
            frozen_memtables: self.frozen_memtables.len(),
            active_memtable_number_of_records: self.memtable.size,
        }
    }
}

impl KVEngine {
    pub fn open(dir_name: &Path, options: KVEngineOptions) -> Result<KVEngine> {
        let path = PathBuf::from(dir_name);

        //TODO: make sure we use options now
        options.validate()?;
        let lock = OpenOptions::new()
            .create(true)
            .write(true)
            .open(dir_name.join("LOCK"))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(DbError::DirectoryLocked(dir_name.to_path_buf()));
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }

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
            _lock: lock,
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

        let recovered = self_instance.retrieve_wal_records(&wals_to_replay)?;
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
    fn retrieve_wal_records(&mut self, wals: &[(u64, PathBuf)]) -> Result<Vec<SSTable>> {
        let mut recovered = Vec::new();
        let mut wal_files_replayed = 0;
        let mut records_recovered = 0;
        for (index, (id, path)) in wals.iter().enumerate() {
            let replay = WAL::build_avl_from_wal(path, self.options.memtable_threshold)?;

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
            wal_files_replayed += 1;
            records_recovered += replay.records_recovered;

            if should_stop_replaying {
                fs::rename(path, path.with_extension("wal.corrupt"))?;
                break;
            }
            let _ = fs::remove_file(path);
        }

        if wal_files_replayed > 0 {
            println!("Recovered {records_recovered} records from {wal_files_replayed} WAL files");
        }
        Ok(recovered)
    }

    fn search_for_kv_in_sstables(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(sstables) = &self.sstables {
            // Lock here is held for the entirety of the loop. Ok for now, mostly reads, rare writes
            for level in sstables.read().unwrap().iter() {
                for element in level.iter() {
                    match element.should_search_sstable_file(key) {
                        true => match element.search_kv_in_sstable(key)? {
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
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.get(key) {
            Found(bytes) => return Ok(Some(bytes.to_vec())),
            Deleted => return Ok(None),
            Absent => {} // fall through
        }

        for (_, mem_table_instance) in self.frozen_memtables.iter().rev() {
            // rev() because we search newer memtables first which have a higher id(hlc)

            match mem_table_instance.memtable.get(key) {
                Found(bytes) => return Ok(Some(bytes.to_vec())),
                Deleted => return Ok(None),
                Absent => {}
            }
        }

        self.search_for_kv_in_sstables(key) // if we get here, 
    }

    pub(crate) fn retry_write_if_stalled(&mut self, record_len: u64) -> Result<()> {
        // let sleep_time

        if (record_len + self.memtable.size_in_bytes) <= self.memtable.threshold
            && !self.wal.is_full()
        {
            return Ok(());
        }
        let deadline = Instant::now()
            + Duration::from_millis(self.options.stalled_writes_retry_max_time_in_ms);
        let mut ms_time: u64 = 1;
        loop {
            match self.rotate_memtable_and_wal() {
                err @ Err(DbError::WritesStalled { .. }) => {
                    if Instant::now() > deadline {
                        return err;
                    }

                    sleep(Duration::from_millis(ms_time));
                    self.maintenance()?;
                }
                result => return result,
            }
            ms_time = (ms_time * 2).min(40) // check every 40 ms until we hit our max
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(err_msg) = &self.db_failed {
            return Err(DbError::ReadOnly(err_msg.to_string()));
        }

        self.maintenance()?;
        self.memtable
            .exceeds_max(key.len() as u64, value.len() as u64)?;

        self.retry_write_if_stalled(key.len() as u64 + value.len() as u64)?;

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

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        if let Some(err_str) = &self.db_failed {
            return Err(DbError::ReadOnly(err_str.to_string()));
        }
        self.maintenance()?;
        let k_len = key.len() as u64;
        self.memtable.exceeds_max(k_len, 0)?;
        self.retry_write_if_stalled(key.len() as u64)?;
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
                    l0_files: self.l0_file_count(),
                });
            }
        }

        let l0_files = self.sstables.as_ref().unwrap().read().unwrap()[0].len();
        if l0_files >= self.options.l0_stop_writes_trigger {
            self.compact()?;
            return Err(DbError::WritesStalled {
                frozen_memtables: self.frozen_memtables.len(),
                l0_files,
            });
        }
        if let Err(e) = self.wal.sync() {
            self.fail(format!("WAL sync failed while rotating: {e}"));
            return Err(e);
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
    fn l0_file_count(&self) -> usize {
        self.sstables
            .as_ref()
            .map_or(0, |levels| levels.read().unwrap()[0].len())
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
        let ratio = l0.len() as f64 / self.options.l0_compaction_trigger as f64;
        if ratio >= 1_f64 {
            best_ratio_candidate = Some((ratio, 0))
        }

        for level in 1..levels.len() - 1 {
            let sstables_in_level = &levels[level];

            let bytes_in_entire_level: u64 =
                sstables_in_level.iter().map(|sst| sst.file_size).sum();

            let target_to_trigger = self
                .options
                .level_multiplier
                .saturating_pow(level as u32 - 1)
                .saturating_mul(self.options.max_bytes_for_level_base); // we use L1s cap to compute it

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
                self.options.max_sst_size,
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
                        let max_flush_retries = self.options.max_flush_retries;
                        self.fail(format!("memtable {id} failed to flush after {max_flush_retries} attempts. \
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
    pub fn maintenance(&mut self) -> Result<()> {
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

    pub fn close(mut self) -> Result<()> {
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
