use std::{
    cmp::max,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use crate::{
    constants::{
        BLOOM_BITS_PER_KEY, FOOTER_FIXED_LEN, KEY_MAX_BYTES_SIZE, RECORD_HEADER_LEN,
        TOMBSTONE_DELETED, TOMBSTONE_LEN, TOMBSTONE_LIVE, U64_LEN, VALUE_MAX_BYTES_SIZE,
    },
    errors::{DbError, InvalidMemtableInput, Result},
    helpers::{CRC32, create_new_data_file, get_hashed_key_positions},
    lsm::Lookup::{self, Absent, Deleted, Found},
    sstable::{BloomFilter, SparseIndex, SsTableDataBlock},
};

pub struct AVL {
    pub root: Option<Box<Node>>,
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
struct Node {
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

    fn get_min_node(node: &Option<Box<Node>>) -> Option<&Node> {
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

    pub fn serialize_sstable_footer(
        offset: u64,
        min_key: &[u8],
        max_key: &[u8],
        sizeof_si: u64,
        sizeof_bf: u64,
        level: u8,
    ) -> Vec<u8> {
        let mut footer: Vec<u8> = Vec::new();

        footer.extend_from_slice(min_key);
        footer.extend_from_slice(max_key);

        footer.extend_from_slice(&offset.to_le_bytes());
        footer.extend_from_slice(&sizeof_si.to_le_bytes());
        footer.extend_from_slice(&sizeof_bf.to_le_bytes());
        footer.extend_from_slice(&(min_key.len() as u64).to_le_bytes());
        footer.extend_from_slice(&(max_key.len() as u64).to_le_bytes());
        footer.extend_from_slice(&level.to_le_bytes()); // Level of sstable, starts at L0
        // entire footer: | sparse_index | bloom_filter | min key | max key |  sparse_index_offset| sizeof(sparse_index) | sizeof(bloom_filter) | sizeof(minkey) |
        // | sizeof(maxkey) | level | sparse_crc(4 bytes) | bloom_crc(4 bytes) | min_max_key_crc | metadata_crc(4 bytes) |

        footer
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
            let footer = Self::serialize_sstable_footer(
                file_offset,
                min_k,
                max_k,
                sparse_index.index_entries.len() as u64,
                (bloom_filter.bits.len() * U64_LEN) as u64, // multiply by 8, needed for reading the u8s during load
                0_u8,                                       // LEVEL 0
            );

            let footer_len = footer.len();

            let footer_crc =
                CRC32.compute_crc_data_block(&footer[footer_len - FOOTER_FIXED_LEN..footer_len]);
            let min_max_crc =
                CRC32.compute_crc_data_block(&footer[..footer_len - FOOTER_FIXED_LEN]);
            let sparse_crc = CRC32.compute_crc_data_block(&sparse_index.index_entries);

            writer.write_all(&sparse_index.index_entries)?;

            let mut bloom_digest = CRC32.digest();

            for word in &bloom_filter.bits {
                bloom_digest.update(&word.to_le_bytes());
                writer.write_all(&word.to_le_bytes())?;
            }
            let bloom_crc = bloom_digest.finalize();

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
