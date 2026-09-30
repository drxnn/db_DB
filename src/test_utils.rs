use std::{fs::OpenOptions, os::unix::fs::FileExt, path::Path};

pub(crate) fn flip_bit_at(path: &Path, offset: u64) {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut byte = [0u8; 1];
    f.read_exact_at(&mut byte, offset).unwrap();
    byte[0] ^= 0x01;
    f.write_all_at(&byte, offset).unwrap();
}
