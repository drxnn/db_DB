use std::{format, path::PathBuf};

use clap::{Parser, ValueEnum};
use database_engine::{KVEngine, KVEngineOptions, SyncConfig, WalRecoveryMode};

use crate::repl::run_repl;
mod repl;
#[derive(Parser)]
#[command(version, about = "An LSM-tree key-value store")]
struct Args {
    /// Directory for the WAL, SST files and MANIFEST
    #[arg(long, default_value = "data")]
    dir: PathBuf,

    /// Start from tiny sizes so flushes and compactions happen after a few hundred KB of writes
    #[arg(long)]
    demo: bool,

    // every option below overrides the defaults (or the --demo preset) only when it's given
    /// When to fsync the WAL: always, none, or every:<ms>
    #[arg(long, value_parser = parse_sync, help_heading = "Durability")]
    sync: Option<SyncConfig>,

    /// What to do when a WAL is damaged on startup
    #[arg(long, value_enum, help_heading = "Durability")]
    wal_recovery_mode: Option<RecoveryMode>,

    /// Memtable size before it's flushed to an SST, e.g. 8MiB
    #[arg(long, value_parser = parse_size, help_heading = "Memtable")]
    memtable_size: Option<u64>,

    /// Flushed memtables allowed to wait for disk before writes stall
    #[arg(long, help_heading = "Memtable")]
    max_frozen_memtables: Option<u8>,

    /// Times a failed flush is retried before writes are disabled
    #[arg(long, help_heading = "Memtable")]
    max_flush_retries: Option<u8>,

    /// Size at which compaction starts a new output file, e.g. 100MiB
    #[arg(long, value_parser = parse_size, help_heading = "Compaction")]
    max_sst_size: Option<u64>,

    /// Number of L0 files that triggers a compaction
    #[arg(long, help_heading = "Compaction")]
    l0_compaction_trigger: Option<usize>,

    /// Size limit of L1; each deeper level is level_multiplier times bigger
    #[arg(long, value_parser = parse_size, help_heading = "Compaction")]
    max_bytes_for_level_base: Option<u64>,

    /// How much bigger each level is than the one above it
    #[arg(long, help_heading = "Compaction")]
    level_multiplier: Option<u64>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
// using this becaues I didnt want to use ValueEnum on WalRecoverMode
enum RecoveryMode {
    /// Stop at the first damaged record and open with everything before it
    PointInTime,
    /// Refuses to open if any WAL is damaged
    AbsoluteConsistency,
}

impl From<RecoveryMode> for WalRecoveryMode {
    fn from(mode: RecoveryMode) -> Self {
        match mode {
            RecoveryMode::PointInTime => WalRecoveryMode::PointInTime,
            RecoveryMode::AbsoluteConsistency => WalRecoveryMode::AbsoluteConsistency,
        }
    }
}

impl Args {
    /// only overrides what is passed as an argument
    fn options(&self) -> KVEngineOptions {
        let mut options = if self.demo {
            KVEngineOptions::demo()
        } else {
            KVEngineOptions::default()
        };

        if let Some(v) = self.sync {
            options.sync = v;
        }
        if let Some(v) = self.wal_recovery_mode {
            options.wal_recovery_mode = v.into();
        }
        if let Some(v) = self.memtable_size {
            options.memtable_threshold = v;
        }
        if let Some(v) = self.max_frozen_memtables {
            options.max_frozen_memtables = v;
        }
        if let Some(v) = self.max_flush_retries {
            options.max_flush_retries = v;
        }
        if let Some(v) = self.max_sst_size {
            options.max_sst_size = v;
        }
        if let Some(v) = self.l0_compaction_trigger {
            options.l0_compaction_trigger = v;
        }
        if let Some(v) = self.max_bytes_for_level_base {
            options.max_bytes_for_level_base = v;
        }
        if let Some(v) = self.level_multiplier {
            options.level_multiplier = v;
        }

        options
    }
}
fn parse_sync(s: &str) -> Result<SyncConfig, String> {
    match s {
        "always" => Ok(SyncConfig::Always),
        "none" => Ok(SyncConfig::None),
        _ => s
            .strip_prefix("every:")
            .and_then(|ms| ms.parse::<u64>().ok())
            .map(SyncConfig::Every)
            .ok_or_else(|| format!("expected always, none or every:<ms>, got {s}")),
    }
}

fn parse_size(size: &str) -> Result<u64, String> {
    let where_to_split = size
        .find(|x: char| !x.is_ascii_digit())
        .unwrap_or(size.len());

    let (number, unit) = size.split_at(where_to_split);

    let number: u64 = number
        .parse()
        .map_err(|_| format!("Expected a valid size like 4096, 8MiB etc, instead found {size}"))?;

    let unit = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        _ => {
            return Err(format!(
                "Expected valid unit like kb, gib etc, intead found unit: {unit} in size: {size}"
            ));
        }
    };
    number
        .checked_mul(unit)
        .ok_or_else(|| format!("size is too large. Size used: {size}"))
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.dir)?;

    let mut db = KVEngine::open(&args.dir, args.options())?;
    run_repl(&mut db)?;

    db.close()?;
    Ok(())
}
