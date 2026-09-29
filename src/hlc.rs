use std::sync::atomic::{
    AtomicU64,
    Ordering::{self, Relaxed},
};

use crate::{
    constants::{MASK_FOR_COUNTER, MASK_FOR_TSTAMP, NUM_OF_BITS_FOR_COUNTER},
    helpers::new_timestamp,
};

pub struct Hlc {
    hlc: AtomicU64,
}

impl Hlc {
    pub fn new() -> Self {
        // we always create a new one for now, but in reality, we only create a new one on first boot, then we check the last most recent
        // timestmap in the database and we check if we need a new timestamp or we use the last one + counter goes up
        Self {
            hlc: AtomicU64::new(new_timestamp() << NUM_OF_BITS_FOR_COUNTER),
        } // counter starts at 0 here
    }

    fn curr_timestamp(&self) -> u64 {
        new_timestamp() << NUM_OF_BITS_FOR_COUNTER
    }

    pub fn sync_with_remote(&self, remote_hlc: u64) -> u64 {
        self.advance(remote_hlc)
    }

    pub fn recover_to(&self, hlc_from_disk: u64) -> u64 {
        self.advance(hlc_from_disk)
    }

    pub fn tick(&self) -> u64 {
        self.advance(0)
    }
    fn advance(&self, floor: u64) -> u64 {
        let curr_tstamp = self.curr_timestamp();
        let mut prev = self.hlc.load(Relaxed);
        loop {
            let max = prev.max(floor);
            let hlc_tstamp = max & MASK_FOR_TSTAMP;

            let new = if hlc_tstamp >= curr_tstamp {
                let new_counter = (max & MASK_FOR_COUNTER) + 1;
                if new_counter > MASK_FOR_COUNTER {
                    // if counter overflows, we add 1 to the timestamp, so we are advancing the physical clock and resetting counter to 0
                    // physical clock starts 12 bits to the left so thats why we add the operation below
                    hlc_tstamp + (MASK_FOR_COUNTER + 1) // Mask is 12 ones, add 1 to get 4096
                } else {
                    hlc_tstamp | new_counter // new counter here is at most 4095 no need to use mask
                }
            } else {
                curr_tstamp
            };

            match self
                .hlc
                .compare_exchange_weak(prev, new, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return new,
                Err(x) => prev = x,
            }
        }
    }

    pub fn deserialize_hlc(hlc: u64) -> (u64, u64) {
        // returns (timestmap, counter)
        ((hlc >> NUM_OF_BITS_FOR_COUNTER), (hlc & MASK_FOR_COUNTER)) // timestamp okay to move down to lower bits, counter cant move up
    }
}
