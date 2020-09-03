use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering, fence};

use alloc;
use consume::CONSUME;
use countedindex::{CountedIndex, Index, past, Transaction};
use maybe_acquire::{MAYBE_ACQUIRE, maybe_acquire_fence};
use memory::MemoryManager;

#[derive(Clone, Copy, PartialEq)]
enum ReaderState {
    Single,
    Multi,
}

struct ReaderPos {
    pos_data: CountedIndex,
}

struct ReaderMeta {
    num_consumers: AtomicUsize,
}

#[derive(Clone)]
pub struct Reader {
    state: Cell<ReaderState>,
    pos: *const ReaderPos,
    meta: *const ReaderMeta,
}

pub struct ReadAttempt<'a> {
    linked: Transaction<'a>,
    state: ReaderState,
}

struct ReaderGroup {
    readers: Vec<*const ReaderPos>,
}

#[repr(C)]
pub struct ReadCursor {
    readers: AtomicPtr<ReaderGroup>,
    pub last_pos: Cell<usize>,
}

impl<'a> ReadAttempt<'a> {
    #[inline(always)]
    pub fn get(&self) -> (isize, usize) {
        self.linked.get()
    }

    #[inline(always)]
    pub fn commit_attempt(self, by: Index, ord: Ordering) -> Option<ReadAttempt<'a>> {
        match self.state {
            ReaderState::Single => {
                self.linked.commit_direct(by, ord);
                None
            }
            ReaderState::Multi => {
                match self.linked.commit(by, ord) {
                    Some(transaction) => {
                        Some(ReadAttempt {
                                 linked: transaction,
                                 state: ReaderState::Multi,
                             })
                    }
                    None => None,
                }
            }
        }
    }

    #[inline(always)]
    pub fn reload(self) -> ReadAttempt<'a> {
        ReadAttempt {
            linked: self.linked.reload(),
            state: self.state,
        }
    }

    #[inline(always)]
    pub fn commit_direct(self, by: Index, ord: Ordering) {
        self.linked.commit_direct(by, ord);
    }
}

impl Reader {
    #[inline(always)]
    pub fn load_attempt(&self, ord: Ordering) -> ReadAttempt {
        if self.state.get() == ReaderState::Multi {
            if unsafe { (*self.meta).num_consumers.load(Ordering::Relaxed) } == 1 {
                fence(Ordering::Acquire);
                self.state.set(ReaderState::Single);
            }
        }
        unsafe {
            ReadAttempt {
                linked: (*self.pos).pos_data.load_transaction(ord),
                state: self.state.get(),
            }
        }
    }

    #[inline(always)]
    pub fn load_count(&self, ord: Ordering) -> usize {
        unsafe { (*self.pos).pos_data.load_count(ord) }
    }

    pub fn dupConsumer(&self) {
        unsafe {
            (*self.meta).num_consumers.fetch_add(1, Ordering::SeqCst);
        }
        self.state.set(ReaderState::Multi);
    }

    pub fn removeConsumer(&self) -> usize {
        unsafe { (*self.meta).num_consumers.fetch_sub(1, Ordering::SeqCst) }
    }

    #[inline(always)]
    pub fn get_consumers(&self) -> usize {
        unsafe { (*self.meta).num_consumers.load(Ordering::Relaxed) }
    }

    #[inline(always)]
    pub fn is_single(&self) -> bool {
        self.get_consumers() == 1
    }
}

impl ReaderGroup {
    pub fn new() -> ReaderGroup {
        ReaderGroup { readers: Vec::new() }
    }

    pub unsafe fn addStream(&self, raw: usize, wrap: Index) -> (*mut ReaderGroup, Reader) {
        let new_meta = alloc::allocate(1);
        let new_group = alloc::allocate(1);
        let new_pos = alloc::allocate(1);
        ptr::write(new_pos,
                   ReaderPos { pos_data: CountedIndex::from_usize(raw, wrap) });
        ptr::write(new_meta, ReaderMeta { num_consumers: AtomicUsize::new(1) });
        let new_reader = Reader {
            state: Cell::new(ReaderState::Single),
            pos: new_pos,
            meta: new_meta as *const ReaderMeta,
        };
        let mut new_readers = self.readers.clone();
        new_readers.push(new_pos as *const ReaderPos);
        ptr::write(new_group, ReaderGroup { readers: new_readers });
        (new_group, new_reader)
    }

    pub unsafe fn removeReader(&self, reader: *const ReaderPos) -> *mut ReaderGroup {
        let new_group = alloc::allocate(1);
        let mut new_readers = self.readers.clone();
        new_readers.retain(|pt| *pt != reader);
        ptr::write(new_group, ReaderGroup { readers: new_readers });
        new_group
    }

    pub fn get_max_diff(&self, cur_writer: usize) -> Option<Index> {
        let mut max_diff: usize = 0;
        unsafe {
            for reader_ptr in &self.readers {
                let rpos = (**reader_ptr).pos_data.load_count(MAYBE_ACQUIRE);
                let (diff, tofar) = past(cur_writer, rpos);
                if tofar {
                    return None;
                }
                max_diff = if diff > max_diff { diff } else { max_diff };
            }
        }
        maybe_acquire_fence();

        Some(max_diff as Index)
    }
}

impl ReadCursor {
    pub fn new(wrap: Index) -> (ReadCursor, Reader) {
        let rg = ReaderGroup::new();
        unsafe {
            let (real_group, reader) = rg.addStream(0, wrap);
            (ReadCursor {
                 readers: AtomicPtr::new(real_group),
                 last_pos: Cell::new(0),
             },
             reader)
        }
    }

    pub fn get_max_diff(&self, cur_writer: usize) -> Option<Index> {
        loop {
            unsafe {
                let first_ptr = self.readers.load(CONSUME);
                let rg = &*first_ptr;
                let rval = rg.get_max_diff(cur_writer);

                let second_ptr = self.readers.load(Ordering::Relaxed);
                if second_ptr == first_ptr {
                    return rval;
                }
            }
        }
    }

    pub fn addStream(&self, reader: &Reader, manager: &MemoryManager) -> Reader {
        let mut current_ptr = self.readers.load(CONSUME);
        loop {
            unsafe {
                let current_group = &*current_ptr;
                let raw = (*reader.pos).pos_data.load_raw(Ordering::Relaxed);
                let wrap = (*reader.pos).pos_data.wrap_at();
                let (new_group, new_reader) = current_group.addStream(raw, wrap);
                fence(Ordering::SeqCst);
                match self.readers.compare_exchange(current_ptr,
                                                    new_group,
                                                    Ordering::Relaxed,
                                                    Ordering::Relaxed) {
                    Ok(_) => {
                        fence(Ordering::SeqCst);
                        manager.free(current_ptr, 1);
                        return new_reader;
                    }
                    Err(val) => {
                        current_ptr = val;
                        fence(Ordering::Acquire);
                        ptr::read(new_group);
                        alloc::deallocate(new_reader.meta as *mut ReaderMeta, 1);
                        alloc::deallocate(new_reader.pos as *mut ReaderPos, 1);
                        alloc::deallocate(new_group, 1);
                    }
                }
            }
        }
    }

    pub fn removeReader(&self, reader: &Reader, mem: &MemoryManager) -> bool {
        let mut current_group = self.readers.load(CONSUME);
        loop {
            unsafe {
                let new_group = (*current_group).removeReader(reader.pos);
                match self.readers.compare_exchange(current_group,
                                                    new_group,
                                                    Ordering::SeqCst,
                                                    Ordering::SeqCst) {
                    Ok(_) => {
                        fence(Ordering::SeqCst);
                        if (*current_group).readers.len() == 1 {
                            self.last_pos.set(reader.load_count(Ordering::Relaxed));
                        }
                        mem.free(current_group, 1);
                        mem.free(reader.pos as *mut ReaderPos, 1);
                        alloc::deallocate(reader.meta as *mut ReaderMeta, 1);
                        return self.hasReaders();
                    }
                    Err(val) => {
                        current_group = val;
                        ptr::read(new_group);
                        alloc::deallocate(new_group, 1);
                    }
                }
            }
        }
    }

    pub fn hasReaders(&self) -> bool {
        unsafe {
            let current_group = &*self.readers.load(CONSUME);
            current_group.readers.is_empty()
        }
    }
}