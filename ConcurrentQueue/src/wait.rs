use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::AtomicUsize;
use std::thread::yield_now;

use countedindex::{past, rm_tag};

extern crate parking_lot;

pub const DEFAULT_YIELD_SPINS: usize = 50;
pub const DEFAULT_TRY_SPINS: usize = 1000;

#[inline(always)]
pub fn load_tagless(val: &AtomicUsize) -> usize {
    rm_tag(val.load(Relaxed))
}

#[inline(always)]
pub fn check(seq: usize, at: &AtomicUsize, wc: &AtomicUsize) -> bool {
    let cur_count = load_tagless(at);
    wc.load(Relaxed) == 0 || seq == cur_count || past(seq, cur_count).1
}

pub trait Wait {
    fn wait(&self, usize, &AtomicUsize, &AtomicUsize);

    fn notify(&self);

    fn needs_notify(&self) -> bool;
}

#[derive(Copy, Clone)]
pub struct BusyWait {}

#[derive(Copy, Clone)]
pub struct YieldingWait {
    spins_first: usize,
    spins_yield: usize,
}

pub struct BlockingWait {
    spins_first: usize,
    spins_yield: usize,
    lock: parking_lot::Mutex<bool>,
    condvar: parking_lot::Condvar,
}

unsafe impl Sync for BusyWait {}
unsafe impl Sync for YieldingWait {}
unsafe impl Sync for BlockingWait {}
unsafe impl Send for BusyWait {}
unsafe impl Send for YieldingWait {}
unsafe impl Send for BlockingWait {}

impl BusyWait {
    pub fn new() -> BusyWait {
        BusyWait {}
    }
}

impl YieldingWait {
    pub fn new() -> YieldingWait {
        YieldingWait::withSpins(DEFAULT_TRY_SPINS, DEFAULT_YIELD_SPINS)
    }

    pub fn withSpins(spins_first: usize, spins_yield: usize) -> YieldingWait {
        YieldingWait {
            spins_first: spins_first,
            spins_yield: spins_yield,
        }
    }
}

impl BlockingWait {
    pub fn new() -> BlockingWait {
        BlockingWait::withSpins(DEFAULT_TRY_SPINS, DEFAULT_YIELD_SPINS)
    }

    pub fn withSpins(spins_first: usize, spins_yield: usize) -> BlockingWait {
        BlockingWait {
            spins_first: spins_first,
            spins_yield: spins_yield,
            lock: parking_lot::Mutex::new(false),
            condvar: parking_lot::Condvar::new(),
        }
    }
}

impl Wait for BusyWait {
    #[cold]
    fn wait(&self, seq: usize, w_pos: &AtomicUsize, wc: &AtomicUsize) {
        loop {
            if check(seq, w_pos, wc) {
                return;
            }
        }
    }

    fn notify(&self) {
	
    }

    fn needs_notify(&self) -> bool {
        false
    }
}

impl Wait for YieldingWait {
    #[cold]
    fn wait(&self, seq: usize, w_pos: &AtomicUsize, wc: &AtomicUsize) {
        for _ in 0..self.spins_first {
            if check(seq, w_pos, wc) {
                return;
            }
        }
        loop {
            yield_now();
            for _ in 0..self.spins_yield {
                if check(seq, w_pos, wc) {
                    return;
                }
            }
        }
    }

    fn notify(&self) {

    }

    fn needs_notify(&self) -> bool {
        false
    }
}


impl Wait for BlockingWait {
    #[cold]
    fn wait(&self, seq: usize, w_pos: &AtomicUsize, wc: &AtomicUsize) {
        for _ in 0..self.spins_first {
            if check(seq, w_pos, wc) {
                return;
            }
        }
        for _ in 0..self.spins_yield {
            yield_now();
            if check(seq, w_pos, wc) {
                return;
            }
        }

        loop {
            {
                let mut lock = self.lock.lock();
                if check(seq, w_pos, wc) {
                    return;
                }
                self.condvar.wait(&mut lock);
            }
            if check(seq, w_pos, wc) {
                return;
            }

        }
    }

    fn notify(&self) {
        let _lock = self.lock.lock();
        self.condvar.notify_all();
    }

    fn needs_notify(&self) -> bool {
        true
    }
}

impl Clone for BlockingWait {
    fn clone(&self) -> BlockingWait {
        BlockingWait::withSpins(self.spins_first, self.spins_yield)
    }
}

#[cfg(test)]
mod test {

    use std::sync::atomic::{AtomicUsize, fence, Ordering};
    use std::thread::yield_now;

    use super::*;
    use broadcast::broadcast_queue_with;

    extern crate crossbeam;
    use self::crossbeam::scope;

    #[inline(never)]
    fn waste_some_us(n_us: usize, to: &AtomicUsize) {
        for _ in 0..20 * n_us {
            to.store(0, Ordering::Relaxed);
            fence(Ordering::SeqCst);
        }
    }

    fn mpsc_broadcast<W: Wait + 'static>(senders: usize, receivers: usize, waiter: W) {
        let (writer, reader) = broadcast_queue_with(4, waiter);
        let num_loop = 1000;
        scope(|scope| {
            for q in 0..senders {
                let cur_writer = writer.clone();
                scope.spawn(move || {
                    let v = AtomicUsize::new(0);
                    'outer: for i in 0..num_loop {
                        for _ in 0..100000000 {
                            if cur_writer.try_send((q, i)).is_ok() {
                                waste_some_us(1, &v); 
                                continue 'outer;
                            }
                            yield_now();
                        }
                        assert!(false, "Writer could not write");
                    }
                });
            }
            writer.unsubscribe();
            for _ in 0..receivers {
                let this_reader = reader.add_stream().into_single().unwrap();
                scope.spawn(move || {
                    let mut myv = Vec::new();
                    for _ in 0..senders {
                        myv.push(0);
                    }
                    for _ in 0..num_loop * senders {
                        if let Ok(val) = this_reader.recv() {
                            assert_eq!(myv[val.0], val.1);
                            myv[val.0] += 1;
                        } else {
                            panic!("Writer got disconnected early");
                        }
                    }
                    for val in myv {
                        if val != num_loop {
                            panic!("Wrong number of values obtained for this");
                        }
                    }
                    assert!(this_reader.try_recv().is_err());
                });
            }
            reader.unsubscribe();
        });
    }

    fn testWaiter<T: Wait + Clone + 'static>(waiter: T) {
        mpsc_broadcast(2, 2, waiter.clone());
    }

    #[test]
    fn test_busywait() {
        test_waiter(BusyWait::new());
    }

    #[test]
    fn test_yieldwait() {
        test_waiter(YieldingWait::new());
    }

    #[test]
    fn test_blockingwait() {
        test_waiter(BlockingWait::new());
    }

    #[test]
    fn test_blockingwait_nospin() {
        test_waiter(BlockingWait::with_spins(0, 0));
    }
}