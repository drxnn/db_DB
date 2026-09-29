use crate::constants::BLOOM_WORD_BITS;
use crate::helpers::NUM_HASHES;

pub struct BloomFilter {
    pub bits: Vec<u64>,
    pub num_bits: u64,
}
impl BloomFilter {
    pub fn new(num_bits: usize) -> Self {
        let words_for_bits = num_bits.div_ceil(BLOOM_WORD_BITS);

        Self {
            bits: vec![0u64; words_for_bits],
            num_bits: (words_for_bits * BLOOM_WORD_BITS) as u64,
        }
    }

    pub fn set_bits(&mut self, positons: [usize; NUM_HASHES]) {
        for position in positons {
            let word_idx = position / BLOOM_WORD_BITS;
            let bit_idx = position % BLOOM_WORD_BITS;

            self.bits[word_idx] |= 1u64 << bit_idx; // shift the bit to the left by bit_idx positions and thats our mask. mask OR curr_u64 = done
        }
    }

    pub fn check_bits(&self, positons: [usize; NUM_HASHES]) -> bool {
        for position in positons {
            let word_idx = position / BLOOM_WORD_BITS;
            let bit_idx = position % BLOOM_WORD_BITS;

            if ((self.bits[word_idx] >> bit_idx) & 1u64) == 0 {
                return false;
            }
        }

        true
    }
}
