use countedindex::Index;
use multiqueue::{InnerSend, InnerRecv, FutInnerSend, FutInnerRecv, FutInnerUniRecv, BCast,
                 MultiQueue, SendError, futures_multiqueue};
use wait::Wait;

use std::sync::mpsc::{TrySendError, TryRecvError, RecvError};

extern crate futures;
use self::futures::{Async, Poll, Sink, Stream, StartSend};


#[derive(Clone)]
pub struct BroadcastSender<T: Clone> {
    sender: InnerSend<BCast<T>, T>,
}

#[derive(Clone, Debug)]
pub struct BroadcastReceiver<T: Clone> {
    receiver: InnerRecv<BCast<T>, T>,
}

pub struct BroadcastUniReceiver<T: Clone + Sync> {
    receiver: InnerRecv<BCast<T>, T>,
}

#[derive(Clone)]
pub struct BroadcastFutSender<T: Clone> {
    sender: FutInnerSend<BCast<T>, T>,
}

#[derive(Clone)]
pub struct BroadcastFutReceiver<T: Clone> {
    receiver: FutInnerRecv<BCast<T>, T>,
}

pub struct BroadcastFutUniReceiver<R, F: FnMut(&T) -> R, T: Clone + Sync> {
    receiver: FutInnerUniRecv<BCast<T>, R, F, T>,
}

impl<T: Clone> BroadcastSender<T> {
    #[inline(always)]
    pub fn try_send(&self, val: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(val)
    }

    
    pub fn unsubscribe(self) {
        self.sender.unsubscribe();
    }
}

impl<T: Clone> BroadcastReceiver<T> {
    #[inline(always)]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline(always)]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    pub fn addStream(&self) -> BroadcastReceiver<T> {
        BroadcastReceiver { receiver: self.receiver.add_stream() }
    }

    pub fn unsubscribe(self) -> bool {
        self.receiver.unsubscribe()
    }

    pub fn tryIter<'a>(&'a self) -> BroadcastRefIter<'a, T> {
        BroadcastRefIter { recv: self }
    }
}

impl<T: Clone + Sync> BroadcastReceiver<T> {
    pub fn into_single(self) -> Result<BroadcastUniReceiver<T>, BroadcastReceiver<T>> {
        if self.receiver.is_single() {
            Ok(BroadcastUniReceiver { receiver: self.receiver })
        } else {
            Err(self)
        }
    }
}

impl<T: Clone + Sync> BroadcastUniReceiver<T> {
    #[inline(always)]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline(always)]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    #[inline(always)]
    pub fn try_recv_view<R, F: FnOnce(&T) -> R>(&self, op: F) -> Result<R, (F, TryRecvError)> {
        self.receiver.try_recv_view(op)
    }

    #[inline(always)]
    pub fn recv_view<R, F: FnOnce(&T) -> R>(&self, op: F) -> Result<R, (F, RecvError)> {
        self.receiver.recv_view(op)
    }

    pub fn unsubscribe(self) {
        self.receiver.unsubscribe();
    }

    pub fn intoMulti(self) -> BroadcastReceiver<T> {
        BroadcastReceiver { receiver: self.receiver }
    }

    pub fn iterWith<R, F: FnMut(&T) -> R>(self, op: F) -> BroadcastUniIter<R, F, T> {
        BroadcastUniIter {
            recv: self,
            op: op,
        }
    }

    pub fn tryIterWith<'a, R, F: FnMut(&T) -> R>(&'a self,
                                                   op: F)
                                                   -> BroadcastUniRefIter<'a, R, F, T> {
        BroadcastUniRefIter {
            recv: self,
            op: op,
        }
    }
}

impl<T: Clone> BroadcastFutSender<T> {
    #[inline(always)]
    pub fn try_send(&self, val: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(val)
    }

    pub fn unsubscribe(self) {
        self.sender.unsubscribe()
    }
}

impl<T: Clone> BroadcastFutReceiver<T> {
    #[inline(always)]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline(always)]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    pub fn addStream(&self) -> BroadcastFutReceiver<T> {
        BroadcastFutReceiver { receiver: self.receiver.add_stream() }
    }

    pub fn unsubscribe(self) -> bool {
        self.receiver.unsubscribe()
    }
}

impl<T: Clone + Sync> BroadcastFutReceiver<T> {
    pub fn intoSingle<R, F: FnMut(&T) -> R>
        (self,
         op: F)
         -> Result<BroadcastFutUniReceiver<R, F, T>, (F, BroadcastFutReceiver<T>)> {
        match self.receiver.into_single(op) {
            Ok(sreceiver) => Ok(BroadcastFutUniReceiver { receiver: sreceiver }),
            Err((o, receiver)) => Err((o, BroadcastFutReceiver { receiver: receiver })),
        }
    }
}

impl<R, F: FnMut(&T) -> R, T: Clone + Sync> BroadcastFutUniReceiver<R, F, T> {
    #[inline(always)]
    pub fn try_recv(&mut self) -> Result<R, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline(always)]
    pub fn recv(&mut self) -> Result<R, RecvError> {
        self.receiver.recv()
    }

    pub fn addStreamWith<RQ, FQ: FnMut(&T) -> RQ>(&self,
                                                    op: FQ)
                                                    -> BroadcastFutUniReceiver<RQ, FQ, T> {
        BroadcastFutUniReceiver { receiver: self.receiver.add_stream_with(op) }
    }

    pub fn transformOperation<RQ, FQ: FnMut(&T) -> RQ>(self,
                                                        op: FQ)
                                                        -> BroadcastFutUniReceiver<RQ, FQ, T> {
        BroadcastFutUniReceiver { receiver: self.receiver.add_stream_with(op) }
    }

    pub fn unsubscribe(self) -> bool {
        self.receiver.unsubscribe()
    }

    pub fn intoMulti(self) -> BroadcastFutReceiver<T> {
        BroadcastFutReceiver { receiver: self.receiver.into_multi() }
    }
}

impl<T: Clone> Sink for BroadcastFutSender<T> {
    type SinkItem = T;
    type SinkError = SendError<T>;

    #[inline(always)]
    fn start_send(&mut self, msg: T) -> StartSend<T, SendError<T>> {
        self.sender.start_send(msg)
    }

    #[inline(always)]
    fn poll_complete(&mut self) -> Poll<(), SendError<T>> {
        Ok(Async::Ready(()))
    }
}

impl<T: Clone> Stream for BroadcastFutReceiver<T> {
    type Item = T;
    type Error = ();

    #[inline(always)]
    fn poll(&mut self) -> Poll<Option<T>, ()> {
        self.receiver.poll()
    }
}

impl<R, F: FnMut(&T) -> R, T: Clone + Sync> Stream for BroadcastFutUniReceiver<R, F, T> {
    type Item = R;
    type Error = ();

    #[inline(always)]
    fn poll(&mut self) -> Poll<Option<R>, ()> {
        self.receiver.poll()
    }
}

pub struct BroadcastIter<T: Clone> {
    recv: BroadcastReceiver<T>,
}

impl<T: Clone> Iterator for BroadcastIter<T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        match self.recv.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

impl<T: Clone> IntoIterator for BroadcastReceiver<T> {
    type Item = T;

    type IntoIter = BroadcastIter<T>;

    fn into_iter(self) -> BroadcastIter<T> {
        BroadcastIter { recv: self }
    }
}

pub struct BroadcastSCIter<T: Clone + Sync> {
    recv: BroadcastUniReceiver<T>,
}

impl<T: Clone + Sync> Iterator for BroadcastSCIter<T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        match self.recv.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

impl<T: Clone + Sync> IntoIterator for BroadcastUniReceiver<T> {
    type Item = T;

    type IntoIter = BroadcastSCIter<T>;

    fn into_iter(self) -> BroadcastSCIter<T> {
        BroadcastSCIter { recv: self }
    }
}


pub struct BroadcastRefIter<'a, T: Clone + 'a> {
    recv: &'a BroadcastReceiver<T>,
}

impl<'a, T: Clone + 'a> Iterator for BroadcastRefIter<'a, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        match self.recv.try_recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

impl<'a, T: Clone + 'a> IntoIterator for &'a BroadcastReceiver<T> {
    type Item = T;

    type IntoIter = BroadcastRefIter<'a, T>;

    fn into_iter(self) -> BroadcastRefIter<'a, T> {
        BroadcastRefIter { recv: self }
    }
}

pub struct BroadcastSCRefIter<'a, T: Clone + Sync + 'a> {
    recv: &'a BroadcastUniReceiver<T>,
}

impl<'a, T: Clone + Sync + 'a> Iterator for BroadcastSCRefIter<'a, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        match self.recv.try_recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

impl<'a, T: Clone + Sync + 'a> IntoIterator for &'a BroadcastUniReceiver<T> {
    type Item = T;

    type IntoIter = BroadcastSCRefIter<'a, T>;

    fn into_iter(self) -> BroadcastSCRefIter<'a, T> {
        BroadcastSCRefIter { recv: self }
    }
}


pub struct BroadcastUniIter<R, F: FnMut(&T) -> R, T: Clone + Sync> {
    recv: BroadcastUniReceiver<T>,
    op: F,
}

impl<R, F: FnMut(&T) -> R, T: Clone + Sync> Iterator for BroadcastUniIter<R, F, T> {
    type Item = R;

    #[inline(always)]
    fn next(&mut self) -> Option<R> {
        let opref = &mut self.op;
        match self.recv.recv_view(|v| opref(v)) {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

pub struct BroadcastUniRefIter<'a, R, F: FnMut(&T) -> R, T: Clone + Sync + 'a> {
    recv: &'a BroadcastUniReceiver<T>,
    op: F,
}

impl<'a, R, F: FnMut(&T) -> R, T: Clone + Sync + 'a> Iterator for BroadcastUniRefIter<'a, R, F, T> {
    type Item = R;

    #[inline(always)]
    fn next(&mut self) -> Option<R> {
        let opref = &mut self.op;
        match self.recv.try_recv_view(|v| opref(v)) {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

pub fn broadcast_queue<T: Clone>(capacity: Index) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    let (send, recv) = MultiQueue::<BCast<T>, T>::new(capacity);
    (BroadcastSender { sender: send }, BroadcastReceiver { receiver: recv })
}

pub fn broadcast_queue_with<T: Clone, W: Wait + 'static>
    (capacity: Index,
     wait: W)
     -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    let (send, recv) = MultiQueue::<BCast<T>, T>::new_with(capacity, wait);
    (BroadcastSender { sender: send }, BroadcastReceiver { receiver: recv })
}

pub fn broadcast_fut_queue<T: Clone>(capacity: Index)
                                     -> (BroadcastFutSender<T>, BroadcastFutReceiver<T>) {
    let (isend, irecv) = futures_multiqueue::<BCast<T>, T>(capacity);
    (BroadcastFutSender { sender: isend }, BroadcastFutReceiver { receiver: irecv })
}

unsafe impl<T: Send + Sync + Clone> Send for BroadcastSender<T> {}
unsafe impl<T: Send + Sync + Clone> Send for BroadcastReceiver<T> {}
unsafe impl<T: Send + Sync + Clone> Send for BroadcastUniReceiver<T> {}

#[cfg(test)]
mod test {

    use super::broadcast_queue;

    extern crate crossbeam;
    use self::crossbeam::scope;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::sync::mpsc::TryRecvError;
    use std::thread::yield_now;

    #[test]
    fn build_queue() {
        let _ = broadcast_queue::<usize>(10);
    }

    #[test]
    fn push_pop_test() {
        let (writer, reader) = broadcast_queue(1);
        for _ in 0..100 {
            assert!(reader.try_recv().is_err());
            writer.try_send(1 as usize).expect("Push should succeed");
            assert!(writer.try_send(1).is_err());
            assert_eq!(1, reader.try_recv().unwrap());
        }
    }

    fn mpsc_broadcast(senders: usize, receivers: usize) {
        let (writer, reader) = broadcast_queue(4);
        let myb = Barrier::new(receivers + senders);
        let bref = &myb;
        let num_loop = 100000;
        scope(|scope| {
            for q in 0..senders {
                let cur_writer = writer.clone();
                scope.spawn(move || {
                    bref.wait();
                    'outer: for i in 0..num_loop {
                        for _ in 0..100000000 {
                            if cur_writer.try_send((q, i)).is_ok() {
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
                    bref.wait();
                    for _ in 0..num_loop * senders {
                        loop {
                            if let Ok(val) = this_reader.try_recv_view(|x| *x) {
                                assert_eq!(myv[val.0], val.1);
                                myv[val.0] += 1;
                                break;
                            }
                            yield_now();
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

    #[test]
    fn test_spsc_this() {
        mpsc_broadcast(1, 1);
    }

    #[test]
    fn test_spsc_broadcast() {
        mpsc_broadcast(1, 3);
    }

    #[test]
    fn test_mpsc_single() {
        mpsc_broadcast(2, 1);
    }

    #[test]
    fn test_mpsc_broadcast() {
        mpsc_broadcast(2, 3);
    }

    #[test]
    fn test_remove_reader() {
        let (writer, reader) = broadcast_queue(1);
        assert!(writer.try_send(1).is_ok());
        let reader_2 = reader.add_stream();
        assert!(writer.try_send(1).is_err());
        assert_eq!(1, reader.try_recv().unwrap());
        assert!(reader.try_recv().is_err());
        assert_eq!(1, reader_2.try_recv().unwrap());
        assert!(reader_2.try_recv().is_err());
        assert!(writer.try_send(1).is_ok());
        assert!(writer.try_send(1).is_err());
        assert_eq!(1, reader.try_recv().unwrap());
        assert!(reader.try_recv().is_err());
        reader_2.unsubscribe();
        assert!(writer.try_send(2).is_ok());
        assert_eq!(2, reader.try_recv().unwrap());
    }

    fn mpmc_broadcast(senders: usize, receivers: usize, nclone: usize) {
        let (writer, reader) = broadcast_queue(10);
        let myb = Barrier::new((receivers * nclone) + senders);
        let bref = &myb;
        let num_loop = 1000000;
        let counter = AtomicUsize::new(0);
        let _do_panic = AtomicUsize::new(0);
        let do_panic = &_do_panic;
        let cref = &counter;
        scope(|scope| {
            for _ in 0..senders {
                let cur_writer = writer.clone();
                scope.spawn(move || {
                    bref.wait();
                    'outer: for i in 0..num_loop {
                        for _ in 0..100000000 {
                            if do_panic.load(Ordering::Relaxed) > 0 {
                                panic!("Somebody left");
                            }
                            if cur_writer.try_send(i).is_ok() {
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
                let _this_reader = reader.add_stream();
                for _ in 0..nclone {
                    let this_reader = _this_reader.clone();
                    scope.spawn(move || {
                        let mut cur = 0;
                        bref.wait();
                        loop {
                            match this_reader.try_recv() {
                                Ok(val) => {
                                    if (senders == 1) && (val != 0) && (val <= cur) {
                                        do_panic.fetch_add(1, Ordering::Relaxed);
                                        panic!("Non-increasing values read {} last, val was {}",
                                               cur,
                                               val);
                                    }
                                    cur = val;
                                    cref.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(TryRecvError::Disconnected) => break,
                                _ => yield_now(),
                            }
                        }
                    });
                }
            }
            reader.unsubscribe();
        });
        assert_eq!(senders * receivers * num_loop,
                   counter.load(Ordering::SeqCst));
    }

    #[test]
    fn test_spmc() {
        mpmc_broadcast(1, 1, 2);
    }

    #[test]
    fn test_spmc_broadcast() {
        mpmc_broadcast(1, 2, 2);
    }

    #[test]
    fn test_mpmc() {
        mpmc_broadcast(2, 1, 2);
    }

    #[test]
    fn test_mpmc_broadcast() {
        mpmc_broadcast(2, 2, 2);
    }

    #[test]
    fn test_baddrop() {
        let (writer, reader) = broadcast_queue(1);
        for _ in 0..10 {
            writer.try_send(Arc::new(10)).unwrap();
            reader.recv().unwrap();
        }
    }


    struct Dropper<'a> {
        aref: &'a AtomicUsize,
    }

    impl<'a> Dropper<'a> {
        pub fn new(a: &AtomicUsize) -> Dropper {
            a.fetch_add(1, Ordering::Relaxed);
            Dropper { aref: a }
        }
    }

    impl<'a> Drop for Dropper<'a> {
        fn drop(&mut self) {
            self.aref.fetch_sub(1, Ordering::Relaxed);
        }
    }

    impl<'a> Clone for Dropper<'a> {
        fn clone(&self) -> Dropper<'a> {
            self.aref.fetch_add(1, Ordering::Relaxed);
            Dropper { aref: self.aref }
        }
    }

    #[test]
    fn test_gooddrop() {
        let count = AtomicUsize::new(0);
        {
            let (writer, reader) = broadcast_queue(1);
            for _ in 0..10 {
                writer.try_send(Dropper::new(&count)).unwrap();
                reader.recv().unwrap();
            }
        }
        assert_eq!(count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_iterator_comp() {
        let (writer, reader) = broadcast_queue::<usize>(10);
        drop(writer);
        for _ in reader {}
    }

    #[test]
    fn test_single_leave_multi() {
        let (writer, reader) = broadcast_queue::<usize>(10);
        let reader2 = reader.clone();
        writer.try_send(1).unwrap();
        writer.try_send(1).unwrap();
        assert_eq!(reader2.recv().unwrap(), 1);
        drop(reader2);
        let reader_s = reader.into_single().unwrap();
        assert!(reader_s.recv_view(|x| *x).is_ok());
    }
}