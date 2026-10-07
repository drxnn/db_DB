mod common;

use common::open;
use database_engine::{DbError, KEY_MAX_BYTES_SIZE, VALUE_MAX_BYTES_SIZE};
use tempfile::tempdir;
#[test]
fn put_get_overwrite_delete() {
    let dir = tempdir().unwrap();
    let mut db = open(dir.path());

    db.put(b"apple", b"red").unwrap();
    assert_eq!(db.get(b"apple").unwrap(), Some(b"red".to_vec()));

    db.put(b"apple", b"green").unwrap();
    assert_eq!(db.get(b"apple").unwrap(), Some(b"green".to_vec()));

    db.delete(b"apple").unwrap();
    assert_eq!(db.get(b"apple").unwrap(), None);

    db.put(b"apple", b"back again").unwrap();
    assert_eq!(db.get(b"apple").unwrap(), Some(b"back again".to_vec()));

    assert_eq!(db.get(b"never-written").unwrap(), None);
    db.close().unwrap();
}

#[test]
fn empty_value_is_not_a_delete() {
    let dir = tempdir().unwrap();
    let mut db = open(dir.path());
    db.put(b"empty", b"").unwrap();
    assert_eq!(db.get(b"empty").unwrap(), Some(Vec::new()));
    db.close().unwrap();
}

#[test]
fn oversized_writes_are_rejected_and_the_db_keeps_working() {
    let dir = tempdir().unwrap();
    let mut db = open(dir.path());

    let huge_key = vec![b'k'; KEY_MAX_BYTES_SIZE as usize + 1];
    let huge_value = vec![b'v'; VALUE_MAX_BYTES_SIZE as usize + 1];
    assert!(matches!(
        db.put(&huge_key, b"v"),
        Err(DbError::InvalidMemtableInput(_))
    ));
    assert!(matches!(
        db.put(b"k", &huge_value),
        Err(DbError::InvalidMemtableInput(_))
    ));

    db.put(b"k", b"still works").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"still works".to_vec()));
    db.close().unwrap();
}
