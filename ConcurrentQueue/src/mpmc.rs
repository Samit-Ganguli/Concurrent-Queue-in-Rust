use countedindex::Index;
use multiqueue::{InnerSend, InnerRecv, MPMC, MultiQueue, FutInnerSend, FutInnerRecv,
                 FutInnerUniRecv, SendError, futures_multiqueue};
use wait::Wait;

use std::sync::mpsc::{TrySendError, TryRecvError, RecvError};

extern crate futures;
use self::futures::{Async, Poll, Sink, Stream, StartSend};

#[derive(Clone)]
pub struct MPMCSender<T> {
    sender: InnerSend<MPMC<T>, T>,
}

#[derive(Debug)]
pub struct MPMCReceiver<T> {
    receiver: InnerRecv<MPMC<T>, T>,
}

impl<T> Clone for MPMCReceiver<T> {
    fn clone(&self) -> Self {
        MPMCReceiver { receiver: self.receiver.clone() }
    }
}

pub struct MPMCUniReceiver<T> {
    receiver: InnerRecv<MPMC<T>, T>,
}

pub struct MPMCFutSender<T> {
    sender: FutInnerSend<MPMC<T>, T>,
}

pub struct MPMCFutReceiver<T> {
    receiver: FutInnerRecv<MPMC<T>, T>,
}

pub struct MPMCFutUniReceiver<R, F: FnMut(&T) -> R, T> {
    receiver: FutInnerUniRecv<MPMC<T>, R, F, T>,
}

impl<T> MPMCSender<T> {
    pub fn trySend(&self, val: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(val)
    }

    pub fn unsubscribe(self) {
        self.sender.unsubscribe()
    }
}

impl<T> MPMCReceiver<T> {

    #[inline(always)]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline(always)]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    pub fn unsubscribe(self) -> bool {
        self.receiver.unsubscribe()
    }

    pub fn intoSingle(self) -> Result<MPMCUniReceiver<T>, MPMCReceiver<T>> {
        if self.receiver.is_single() {
            Ok(MPMCUniReceiver { receiver: self.receiver })
        } else {
            Err(self)
        }
    }

    pub fn try_iter<'a>(&'a self) -> MPMCRefIter<'a, T> {
        MPMCRefIter { recv: self }
    }
}

impl<T> MPMCUniReceiver<T> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }


    #[inline(always)]
    pub fn try_recv_view<R, F: FnOnce(&T) -> R>(&self, op: F) -> Result<R, (F, TryRecvError)> {
        self.receiver.try_recv_view(op)
    }

    pub fn recvView<R, F: FnOnce(&T) -> R>(&self, op: F) -> Result<R, (F, RecvError)> {
        self.receiver.recv_view(op)
    }

    pub fn unsubscribe(self) -> bool {
        self.receiver.unsubscribe()
    }

    pub fn into_multi(self) -> MPMCReceiver<T> {
        MPMCReceiver { receiver: self.receiver }
    }

    pub fn iter_with<R, F: FnMut(&T) -> R>(self, op: F) -> MPMCUniIter<R, F, T> {
        MPMCUniIter {
            recv: self,
            op: op,
        }
    }

    pub fn try_iter_with<'a, R, F: FnMut(&T) -> R>(&'a self, op: F) -> MPMCUniRefIter<'a, R, F, T> {
        MPMCUniRefIter {
            recv: self,
            op: op,
        }
    }
}

impl<T> MPMCFutSender<T> {

    #[inline(always)]
    pub fn try_send(&self, val: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(val)
    }

    pub fn unsubscribe(self) {
        self.sender.unsubscribe()
    }
}

impl<T> MPMCFutReceiver<T> {

    #[inline(always)]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline(always)]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    pub fn unsubscribe(self) -> bool {
        self.receiver.unsubscribe()
    }

    pub fn intoSingle<R, F: FnMut(&T) -> R>
        (self,
         op: F)
         -> Result<MPMCFutUniReceiver<R, F, T>, (F, MPMCFutReceiver<T>)> {
        match self.receiver.into_single(op) {
            Ok(sreceiver) => Ok(MPMCFutUniReceiver { receiver: sreceiver }),
            Err((o, receiver)) => Err((o, MPMCFutReceiver { receiver: receiver })),
        }
    }
}

impl<R, F: FnMut(&T) -> R, T> MPMCFutUniReceiver<R, F, T> {

    #[inline(always)]
    pub fn try_recv(&mut self) -> Result<R, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline(always)]
    pub fn recv(&mut self) -> Result<R, RecvError> {
        self.receiver.recv()
    }

    pub fn add_stream_with<RQ, FQ: FnMut(&T) -> RQ>(&self,
                                                    op: FQ)
                                                    -> MPMCFutUniReceiver<RQ, FQ, T> {
        MPMCFutUniReceiver { receiver: self.receiver.add_stream_with(op) }
    }

    pub fn transform_operation<RQ, FQ: FnMut(&T) -> RQ>(self,
                                                        op: FQ)
                                                        -> MPMCFutUniReceiver<RQ, FQ, T> {
        MPMCFutUniReceiver { receiver: self.receiver.add_stream_with(op) }
    }

    pub fn unsubscribe(self) -> bool {
        self.receiver.unsubscribe()
    }

    pub fn into_multi(self) -> MPMCFutReceiver<T> {
        MPMCFutReceiver { receiver: self.receiver.into_multi() }
    }
}

impl<T> Clone for MPMCFutSender<T> {
    fn clone(&self) -> Self {
        MPMCFutSender { sender: self.sender.clone() }
    }
}

impl<T> Sink for MPMCFutSender<T> {
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

impl<T> Clone for MPMCFutReceiver<T> {
    fn clone(&self) -> Self {
        MPMCFutReceiver { receiver: self.receiver.clone() }
    }
}

impl<T> Stream for MPMCFutReceiver<T> {
    type Item = T;
    type Error = ();

    #[inline(always)]
    fn poll(&mut self) -> Poll<Option<T>, ()> {
        self.receiver.poll()
    }
}

impl<R, F: FnMut(&T) -> R, T> Stream for MPMCFutUniReceiver<R, F, T> {
    type Item = R;
    type Error = ();

    #[inline(always)]
    fn poll(&mut self) -> Poll<Option<R>, ()> {
        self.receiver.poll()
    }
}


pub struct MPMCIter<T> {
    recv: MPMCReceiver<T>,
}

impl<T> Iterator for MPMCIter<T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        match self.recv.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

impl<T> IntoIterator for MPMCReceiver<T> {
    type Item = T;

    type IntoIter = MPMCIter<T>;

    fn into_iter(self) -> MPMCIter<T> {
        MPMCIter { recv: self }
    }
}

pub struct MPSCIter<T> {
    recv: MPMCUniReceiver<T>,
}

impl<T> Iterator for MPSCIter<T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        match self.recv.recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

impl<T> IntoIterator for MPMCUniReceiver<T> {
    type Item = T;

    type IntoIter = MPSCIter<T>;

    fn into_iter(self) -> MPSCIter<T> {
        MPSCIter { recv: self }
    }
}

pub struct MPMCRefIter<'a, T: 'a> {
    recv: &'a MPMCReceiver<T>,
}

impl<'a, T> Iterator for MPMCRefIter<'a, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        match self.recv.try_recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

impl<'a, T: 'a> IntoIterator for &'a MPMCReceiver<T> {
    type Item = T;

    type IntoIter = MPMCRefIter<'a, T>;

    fn into_iter(self) -> MPMCRefIter<'a, T> {
        MPMCRefIter { recv: self }
    }
}

pub struct MPSCRefIter<'a, T: 'a> {
    recv: &'a MPMCUniReceiver<T>,
}

impl<'a, T> Iterator for MPSCRefIter<'a, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        match self.recv.try_recv() {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

impl<'a, T: 'a> IntoIterator for &'a MPMCUniReceiver<T> {
    type Item = T;

    type IntoIter = MPSCRefIter<'a, T>;

    fn into_iter(self) -> MPSCRefIter<'a, T> {
        MPSCRefIter { recv: self }
    }
}

pub struct MPMCUniIter<R, F: FnMut(&T) -> R, T> {
    recv: MPMCUniReceiver<T>,
    op: F,
}

impl<R, F: FnMut(&T) -> R, T> Iterator for MPMCUniIter<R, F, T> {
    type Item = R;

    #[inline(always)]
    fn next(&mut self) -> Option<R> {
        let opref = &mut self.op;
        match self.recv.recvView(|v| opref(v)) {
            Ok(val) => Some(val),
            Err(_) => None,
        }
    }
}

pub struct MPMCUniRefIter<'a, R, F: FnMut(&T) -> R, T: 'a> {
    recv: &'a MPMCUniReceiver<T>,
    op: F,
}

impl<'a, R, F: FnMut(&T) -> R, T: 'a> Iterator for MPMCUniRefIter<'a, R, F, T> {
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


pub fn mpmc_queue<T>(capacity: Index) -> (MPMCSender<T>, MPMCReceiver<T>) {
    let (send, recv) = MultiQueue::<MPMC<T>, T>::new(capacity);
    (MPMCSender { sender: send }, MPMCReceiver { receiver: recv })
}

pub fn mpmc_queue_with<T, W: Wait + 'static>(capacity: Index,
                                             w: W)
                                             -> (MPMCSender<T>, MPMCReceiver<T>) {
    let (send, recv) = MultiQueue::<MPMC<T>, T>::new_with(capacity, w);
    (MPMCSender { sender: send }, MPMCReceiver { receiver: recv })
}

pub fn mpmc_fut_queue<T>(capacity: Index) -> (MPMCFutSender<T>, MPMCFutReceiver<T>) {
    let (isend, irecv) = futures_multiqueue::<MPMC<T>, T>(capacity);
    (MPMCFutSender { sender: isend }, MPMCFutReceiver { receiver: irecv })
}

unsafe impl<T: Send> Send for MPMCSender<T> {}
unsafe impl<T: Send> Send for MPMCReceiver<T> {}
unsafe impl<T: Send> Send for MPMCUniReceiver<T> {}

#[cfg(test)]
mod test {

    use super::mpmc_queue;

    extern crate crossbeam;
    use self::crossbeam::scope;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::sync::mpsc::TryRecvError;
    use std::thread::yield_now;

    #[test]
    fn build_queue() {
        let _ = mpmc_queue::<usize>(10);
    }

    #[test]
    fn push_pop_test() {
        let (writer, reader) = mpmc_queue(1);
        for _ in 0..100 {
            assert!(reader.try_recv().is_err());
            writer.try_send(1 as usize).expect("Push should succeed");
            assert!(writer.try_send(1).is_err());
            assert_eq!(1, reader.try_recv().unwrap());
        }
    }

    fn mpsc(senders: usize, receivers: usize) {
        let (writer, reader) = mpmc_queue(4);
        let sreader = reader.into_single().unwrap();
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
            scope.spawn(move || {
                let mut myv = Vec::new();
                for _ in 0..senders {
                    myv.push(0);
                }
                bref.wait();
                for _ in 0..num_loop * senders {
                    loop {
                        if let Ok(val) = sreader.try_recv_view(|x| *x) {
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
                assert!(sreader.try_recv().is_err());
            });
        });
    }

    #[test]
    fn test_spsc() {
        mpsc(1, 1);
    }

    #[test]
    fn test_mpsc() {
        mpsc(2, 1);
    }

    fn mpmc(senders: usize, receivers: usize) {
        let (writer, reader) = mpmc_queue(10);
        let myb = Barrier::new(receivers + senders);
        let bref = &myb;
        let num_loop = 1000000;
        let counter = AtomicUsize::new(0);
        let cref = &counter;
        scope(|scope| {
            for _ in 0..senders {
                let cur_writer = writer.clone();
                scope.spawn(move || {
                    bref.wait();
                    'outer: for _ in 0..num_loop {
                        for _ in 0..100000000 {
                            if cur_writer.try_send(1).is_ok() {
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
                let this_reader = reader.clone();
                scope.spawn(move || {
                    bref.wait();
                    loop {
                        match this_reader.try_recv() {
                            Ok(_) => {
                                cref.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(TryRecvError::Disconnected) => break,
                            _ => yield_now(),
                        }
                    }
                });
            }
            reader.unsubscribe();
        });
        assert_eq!(senders * num_loop, counter.load(Ordering::SeqCst));
    }

    #[test]
    fn test_spmc() {
        mpmc(1, 2);
    }

    #[test]
    fn test_mpmc() {
        mpmc(2, 2);
    }

    #[test]
    fn test_baddrop() {
        let (writer, reader) = mpmc_queue(1);
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
            let (writer, reader) = mpmc_queue(1);
            for _ in 0..10 {
                writer.try_send(Dropper::new(&count)).unwrap();
                reader.recv().unwrap();
            }
        }
        assert_eq!(count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_iterator_comp() {
        let (writer, reader) = mpmc_queue::<usize>(10);
        drop(writer);
        for _ in reader {}
    }

    #[test]
    fn test_single_leave_multi() {
        let (writer, reader) = mpmc_queue::<usize>(10);
        let reader2 = reader.clone();
        writer.try_send(1).unwrap();
        writer.try_send(1).unwrap();
        assert_eq!(reader2.recv().unwrap(), 1);
        drop(reader2);
        let reader_s = reader.into_single().unwrap();
        assert!(reader_s.recv_view(|x| *x).is_ok());
    }

    #[test]
    fn test_recv_clone_item_noclone() {
        struct NoClone;

        let (_, reader) = mpmc_queue::<NoClone>(10);
        reader.clone();
    }
}
