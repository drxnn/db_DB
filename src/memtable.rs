use std::{
    cmp::max,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use crate::{
    constants::{
        BLOOM_BITS_PER_KEY, KEY_MAX_BYTES_SIZE, RECORD_HEADER_LEN, TOMBSTONE_DELETED,
        TOMBSTONE_LEN, TOMBSTONE_LIVE, U64_LEN, VALUE_MAX_BYTES_SIZE,
    },
    errors::{DbError, InvalidMemtableInput, Result},
    helpers::{CRC32, create_new_data_file, get_hashed_key_positions},
    lsm::Lookup::{self, Absent, Deleted, Found},
    sstable::{BloomFilter, Footer, SparseIndex, SsTableDataBlock},
};

pub struct AVL {
    root: Option<Box<Node>>,
    pub threshold: u64,
    size: u64,
    pub size_in_bytes: u64,
}
#[derive(PartialEq, Clone, Debug)]
struct AvlEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    deleted: bool,
    timestamp: u64,
}
#[derive(PartialEq, Clone, Debug)]
pub struct Node {
    // (Done): Node should actually carry timestamp, exactly at the time a Node is created
    // Right now we get the timestamp when we serialize the kv which is basically called for everynode as we are flushing
    entry: AvlEntry,
    height: u64,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

impl Node {
    fn serialize_kv(&self) -> Vec<u8> {
        // return [ tstamp(8) | ksz(8) | value_sz(8) | tombstone | key | value |  ]
        let tstamp = self.entry.timestamp.to_le_bytes();
        let ksz = (self.entry.key.len() as u64).to_le_bytes();
        let vsz = (self.entry.value.len() as u64).to_le_bytes();
        let tombstone_in_byte: [u8; TOMBSTONE_LEN] = [if self.entry.deleted {
            TOMBSTONE_DELETED
        } else {
            TOMBSTONE_LIVE
        }];

        [
            &tstamp,
            &ksz,
            &vsz,
            tombstone_in_byte.as_slice(),
            self.entry.key.as_slice(),
            &self.entry.value,
        ]
        .concat()
    }
}

impl AVL {
    pub fn new(threshold: u64) -> Self {
        Self {
            root: None,
            threshold,
            size: 0,
            size_in_bytes: 0,
        }
    }

    pub fn get(&self, key: &[u8]) -> Lookup {
        let mut current = self.root.as_ref();
        while let Some(curr) = current {
            if curr.entry.key == key {
                if !curr.entry.deleted {
                    return Found(curr.entry.value.to_vec());
                } else {
                    return Deleted;
                }
            }
            if curr.entry.key.as_slice() > key {
                current = curr.left.as_ref();
            } else {
                current = curr.right.as_ref();
            }
        }
        Absent
    }

    fn update_height(node: &mut Box<Node>) {
        let left_height = if let Some(x) = node.left.as_ref() {
            x.height as i64
        } else {
            -1
        };

        let right_height = if let Some(x) = node.right.as_ref() {
            x.height as i64
        } else {
            -1
        };
        node.height = (1 + max(left_height, right_height)) as u64;
    }
    fn insert(&mut self, curr: Option<Box<Node>>, n: Node) -> Option<Box<Node>> {
        if let Some(mut node) = curr {
            if n.entry.key == node.entry.key {
                let old_len = node.entry.value.len() as u64;
                node.entry.value = n.entry.value;
                // We do this here because we when we delete something, we dont delete the node, we just replace the value with an empty vector
                // and we mark it as deleted so when it gets flushed to memory, the deleted flag maps to a tombstone
                node.entry.deleted = n.entry.deleted;
                node.entry.timestamp = n.entry.timestamp; // most recent of deletion
                self.size_in_bytes = self.size_in_bytes - old_len + node.entry.value.len() as u64;

                return Some(node);
            }
            if n.entry.key < node.entry.key {
                node.left = self.insert(node.left.take(), n);
            } else {
                node.right = self.insert(node.right.take(), n);
            }

            node = Self::balance(node);
            Some(node)
        } else {
            self.size_in_bytes +=
                n.entry.value.len() as u64 + n.entry.key.len() as u64 + RECORD_HEADER_LEN as u64;
            self.size += 1;
            Some(Box::new(n))
        }
    }
    /*

    */
    pub fn exceeds_max(
        &self,
        key_size: u64,
        value_size: u64,
    ) -> std::result::Result<(), InvalidMemtableInput> {
        if key_size > KEY_MAX_BYTES_SIZE {
            return Err(InvalidMemtableInput::KeySizeTooLarge {
                max: KEY_MAX_BYTES_SIZE,
                found: key_size,
            });
        }
        if value_size > VALUE_MAX_BYTES_SIZE {
            return Err(InvalidMemtableInput::ValueSizeTooLarge {
                max: VALUE_MAX_BYTES_SIZE,
                found: value_size,
            });
        }
        Ok(())
    }
    pub fn put(&mut self, key: &[u8], value: &[u8], timestamp: u64) {
        let n = Node {
            entry: AvlEntry {
                key: key.to_vec(),
                value: value.to_vec(),
                deleted: false,
                timestamp,
            },
            height: 0,
            left: None,
            right: None,
        };
        let root = self.root.take();
        self.root = self.insert(root, n);
    }

    fn balance(mut node: Box<Node>) -> Box<Node> {
        Self::update_height(&mut node);
        let bf = Self::compute_balance_factor_of_node(&node);

        if bf > 1 {
            // left heavy

            let left_node = node.left.as_mut().unwrap();
            match Self::compute_balance_factor_of_node(left_node) {
                bf if bf >= 0 => {
                    let left = node.left.take().unwrap();

                    node = Self::right_rotation(node, left);
                }
                _ => {
                    let mut left_child = node.left.take().unwrap();
                    let right_of_left = left_child.right.take().unwrap();
                    left_child = Self::left_rotation(left_child, right_of_left);
                    node = Self::right_rotation(node, left_child)
                }
            }
        } else if bf < -1 {
            // right heavy

            let right_node = node.right.as_mut().unwrap();
            match Self::compute_balance_factor_of_node(right_node) {
                bf if bf <= 0 => {
                    let right = node.right.take().unwrap();
                    node = Self::left_rotation(node, right);
                }
                _ => {
                    let mut right_child = node.right.take().unwrap();
                    let left_of_right = right_child.left.take().unwrap();
                    right_child = Self::right_rotation(right_child, left_of_right);
                    node = Self::left_rotation(node, right_child);
                }
            }
        }

        node
    }

    fn left_rotation(mut parent: Box<Node>, mut child: Box<Node>) -> Box<Node> {
        // parent and child.right
        parent.right = child.left.take();

        child.left = Some(parent);

        if let Some(left) = child.left.as_mut() {
            Self::update_height(left);
        }
        Self::update_height(&mut child);
        child
    }
    fn right_rotation(mut parent: Box<Node>, mut child: Box<Node>) -> Box<Node> {
        // parent and child.left
        parent.left = child.right.take();
        child.right = Some(parent);
        if let Some(right) = child.right.as_mut() {
            Self::update_height(right);
        }
        Self::update_height(&mut child);
        child
    }

    fn compute_balance_factor_of_node(node: &Node) -> i32 {
        let bf_l = if let Some(x) = node.left.as_ref() {
            x.height as i32
        } else {
            -1
        };
        let bf_r = if let Some(x) = node.right.as_ref() {
            x.height as i32
        } else {
            -1
        };
        bf_l - bf_r
    }
    fn take_min(mut curr: Box<Node>) -> (Option<Box<Node>>, Option<Box<Node>>) {
        // in order successor.
        // we have passed the right child here
        // go left till the end

        // None
        if curr.left.is_none() {
            let right = curr.right.take();
            return (Some(curr), right);
        }

        let (min_node, left_node) = Self::take_min(curr.left.take().unwrap());
        curr.left = left_node;
        (min_node, Some(Self::balance(curr)))
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u64) {
        let node = Node {
            entry: AvlEntry {
                key: key.to_vec(),
                value: Vec::new(),
                deleted: true,
                timestamp,
            },
            height: 0,
            left: None,
            right: None,
        };

        let root = self.root.take();
        self.root = self.insert(root, node);
    }

    pub fn get_min_node(node: &Option<Box<Node>>) -> Option<&Node> {
        let mut curr = node.as_ref()?;
        while let Some(n) = curr.left.as_ref() {
            curr = n
        }

        Some(curr.as_ref())
    }
    fn get_max_node(node: &Option<Box<Node>>) -> Option<&Node> {
        let mut curr = node.as_ref()?;

        while let Some(n) = curr.right.as_ref() {
            curr = n
        }

        Some(curr.as_ref()) // 
    }

    fn build_sstable_recursive(
        &self,
        writer: &mut BufWriter<File>,
        n: &Option<Box<Node>>,
        bf: &mut BloomFilter,
        data_block: &mut Option<SsTableDataBlock>,
        sparse_index: &mut SparseIndex,
        offset: &mut u64,
    ) -> Result<()> {
        if let Some(x) = n {
            self.build_sstable_recursive(writer, &x.left, bf, data_block, sparse_index, offset)?;
            if let Some(ss_data_block) = data_block {
                match ss_data_block.is_finished() {
                    true => {
                        let owned_ss_data_block =
                            data_block.take().expect("Expected a SsTableDataBlock");
                        let data_len = owned_ss_data_block.bytes.get_ref().len() as u64; // before 4 byte crc
                        let full = owned_ss_data_block.full_data_block();

                        writer.write_all(full.bytes.get_ref())?; // including 4 byte crc

                        sparse_index.add_entry(&full.starting_key, data_len, *offset);
                        *offset += full.bytes.get_ref().len() as u64;

                        let mut new_ss_db = SsTableDataBlock::new(&x.entry.key);
                        new_ss_db.append_to_block(&x.serialize_kv());
                        *data_block = Some(new_ss_db);
                    }
                    false => {
                        ss_data_block.append_to_block(&x.serialize_kv());
                    }
                }
            } else {
                let mut new_ss_db = SsTableDataBlock::new(&x.entry.key);
                new_ss_db.append_to_block(&x.serialize_kv());
                *data_block = Some(new_ss_db);
            }
            let positions = get_hashed_key_positions(&x.entry.key, bf.num_bits as usize);
            bf.set_bits(positions);
            self.build_sstable_recursive(writer, &x.right, bf, data_block, sparse_index, offset)?;
        }
        Ok(())
    }

    pub fn sync_avl(&self, dir: &Path, hlc: u64) -> Result<Option<(File, PathBuf)>> {
        let min_k = match Self::get_min_node(&self.root) {
            Some(k) => &k.entry.key,
            None => return Ok(None),
        };

        let max_k = match Self::get_max_node(&self.root) {
            Some(k) => &k.entry.key,
            None => return Ok(None),
        };

        let (file, ss_path_final) = create_new_data_file(dir, hlc)?;
        let tmp_path_for_err_case = ss_path_final.clone();

        (|| -> Result<Option<(File, PathBuf)>> {
            // TODO: Can also put in a function
            let mut writer = BufWriter::new(file);
            //
            let mut data_block: Option<SsTableDataBlock> = None;

            // Also TODO: see if you can use SstFinalizer here

            // sizeof(key) | key | offset | datablock block length ( before CRC )
            let mut sparse_index = SparseIndex::new();
            let mut bloom_filter = BloomFilter::new(self.size as usize * BLOOM_BITS_PER_KEY);

            let mut file_offset: u64 = 0;
            self.build_sstable_recursive(
                &mut writer,
                &self.root,
                &mut bloom_filter,
                &mut data_block,
                &mut sparse_index,
                &mut file_offset,
            )?;

            if let Some(last_db) = data_block {
                let len = last_db.bytes.get_ref().len() as u64;

                let full = last_db.full_data_block();
                writer.write_all(full.bytes.get_ref())?;

                sparse_index.add_entry(&full.starting_key, len, file_offset);

                file_offset += full.bytes.get_ref().len() as u64; // length here is the start of sparse_index // 
            }

            let footer = Footer {
                sparse_index_offset: file_offset,
                sparse_index_len: sparse_index.index_entries.len() as u64,
                bloom_len: (bloom_filter.bits.len() * U64_LEN) as u64,
                min_key_len: min_k.len() as u64,
                max_key_len: max_k.len() as u64,
                level: 0 as u8,
            };
            let footer = footer.serialize();

            let footer_crc = CRC32.compute_crc_data_block(&footer);
            let mut min_max_digest = CRC32.digest();
            min_max_digest.update(min_k);
            min_max_digest.update(max_k);
            let min_max_crc = min_max_digest.finalize();

            let sparse_crc = CRC32.compute_crc_data_block(&sparse_index.index_entries);

            writer.write_all(&sparse_index.index_entries)?;

            let mut bloom_digest = CRC32.digest();

            for word in &bloom_filter.bits {
                bloom_digest.update(&word.to_le_bytes());
                writer.write_all(&word.to_le_bytes())?;
            }
            let bloom_crc = bloom_digest.finalize();

            writer.write_all(min_k)?;
            writer.write_all(max_k)?;
            writer.write_all(&footer)?;
            writer.write_all(&sparse_crc.to_le_bytes())?;
            writer.write_all(&bloom_crc.to_le_bytes())?;
            writer.write_all(&min_max_crc.to_le_bytes())?;

            writer.write_all(&footer_crc.to_le_bytes())?;

            let f = writer.into_inner().map_err(|e| {
                DbError::FileError(
                    format!("Failed to extract File from BufWriter: {}", e.error()),
                    ss_path_final.to_path_buf(),
                )
            })?;
            f.sync_all()?;

            Ok(Some((f, ss_path_final)))
        })()
        .map_err(|err| {
            let _ = fs::remove_file(&tmp_path_for_err_case);
            DbError::SyncFail(Box::new(err), tmp_path_for_err_case)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use super::*;

    // use crate::hlc::Hlc;
    fn populated_memtable() -> AVL {
        let records_to_add = [("a", "a1"), ("ab", "ab2"), ("longest", "longest123")];

        let mut memtable = AVL::new(1024);
        for (i, (k, v)) in records_to_add.iter().enumerate() {
            memtable.put(k.as_bytes(), v.as_bytes(), i as u64);
        }

        memtable
    }

    fn populated_numbers_memtable() -> AVL {
        let records_to_add = [
            (1_u64.to_le_bytes(), 100_u64.to_le_bytes()),
            (2_u64.to_le_bytes(), 200_u64.to_le_bytes()),
            (3_u64.to_le_bytes(), 300_u64.to_le_bytes()),
        ];
        let mut memtable = AVL::new(1024);
        for (i, (k, v)) in records_to_add.iter().enumerate() {
            memtable.put(k, v, i as u64);
        }
        memtable
    }

    #[test]
    pub fn puts_records() {
        let memtable = populated_memtable();

        assert_eq!(memtable.get(b"a"), Found("a1".into()));
        assert_eq!(memtable.get(b"ab"), Found("ab2".into()));
        assert_eq!(memtable.get(b"longest"), Found("longest123".into()));
        assert_eq!(memtable.size, 3);
    }

    #[test]
    fn deletes_record() {
        let mut populated_mem = populated_memtable();
        populated_mem.delete(b"a", 4);
        assert_eq!(populated_mem.get(b"a"), Lookup::Deleted)
    }
    #[test]
    fn gets_min_node() {
        let mut memtable = populated_memtable();
        let root = memtable.root.take();
        let min = AVL::get_min_node(&root);
        assert_eq!(min.unwrap().entry.key, b"a");
    }

    #[test]
    fn gets_max_node() {
        let mut memtable = populated_memtable();
        let root = memtable.root.take();
        let min = AVL::get_max_node(&root);
        assert_eq!(min.unwrap().entry.key, b"longest");
    }

    #[test]
    fn overwrites_key() {
        let mut memtable = populated_memtable();
        assert_eq!(memtable.get(b"a"), Found("a1".into()));
        memtable.put(b"a", b"a1_overwrite", 5);
        assert_eq!(memtable.get(b"a"), Found("a1_overwrite".into()));
    }

    #[test]
    fn get_key_that_does_not_exist_returns_absent() {
        let memtable = populated_memtable();
        assert_eq!(memtable.get(b"nothere"), Absent)
    }
    #[test]
    fn deleting_key_that_does_not_exist_inserts_record() {
        let mut memtable = populated_memtable();
        memtable.delete(b"notherebutinserts", 6);
        assert_eq!(memtable.size, 4);
        assert_eq!(memtable.get(b"notherebutinserts"), Deleted)
    }

    #[test]
    fn empty_tree_is_empty() {
        let mut memtable = AVL::new(1024);
        assert_eq!(memtable.size, 0);
        assert_eq!(memtable.size_in_bytes, 0);
        assert_eq!(memtable.root.take(), None);
    }

    #[test]
    fn each_rotation_case_works() {
        let memtable = populated_numbers_memtable();
        // 1,2,3
        // need a helper that returns nodes in order
    }
}
