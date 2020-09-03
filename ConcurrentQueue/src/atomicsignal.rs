use std::sync::atomic::{AtomicUsize, Ordering};
const UPDATE_EPOCH: usize = 1 << 0;
const NO_READER: usize = 1 << 1;
pub struct AtomicSignal {
    flags: AtomicUsize,
}
pub struct LoadedSignal {
    flags: usize,
}
impl AtomicSignal {
    pub fn new() -> AtomicSignal {
        AtomicSignal { flags: AtomicUsize::new(0) }
    }

    #[inline(always)]
    pub fn load(&self, ord: Ordering) -> LoadedSignal {
        LoadedSignal { flags: self.flags.load(ord) }
    }

    #[inline(always)]
    pub fn setEpoch(&self, ord: Ordering) -> bool {
        let prev = self.flags.fetch_or(UPDATE_EPOCH, ord);
        (prev & UPDATE_EPOCH) != 0
    }

    #[inline(always)]
    pub fn clearEpoch(&self, ord: Ordering) -> bool {
        let prev = self.flags.fetch_and(!UPDATE_EPOCH, ord);
        (prev & UPDATE_EPOCH) != 0
    }

    #[inline(always)]
    pub fn setReader(&self, ord: Ordering) -> bool {
        let prev = self.flags.fetch_or(NO_READER, ord);
        (prev & NO_READER) != 0
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn clearReader(&self, ord: Ordering) -> bool {
        let prev = self.flags.fetch_and(!NO_READER, ord);
        (prev & NO_READER) != 0
    }
}

impl LoadedSignal {
    #[inline(always)]
    pub fn hasAction(&self) -> bool {
        self.flags != 0
    }
    #[inline(always)]
    pub fn getEpoch(&self) -> bool {
        (self.flags & UPDATE_EPOCH) != 0
    }
    #[inline(always)]
    pub fn getReader(&self) -> bool {
        (self.flags & NO_READER) != 0
    }
}
