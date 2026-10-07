use std::path::Path;

use database_engine::{DbError, KVEngine, KVEngineOptions, Result, SyncConfig};
pub fn open(dir: &Path) -> KVEngine {
    KVEngine::open(
        dir,
        KVEngineOptions {
            sync: SyncConfig::None,
            ..KVEngineOptions::demo()
        },
    )
    .unwrap()
}

pub fn crash_db(db: &mut KVEngine) {}
