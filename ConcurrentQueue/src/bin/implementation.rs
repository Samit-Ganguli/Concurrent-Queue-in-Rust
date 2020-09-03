extern crate crossbeam;
extern crate multiqueue;
extern crate time;

use multiqueue::{BroadcastReceiver, BroadcastSender, broadcast_queue_with, wait};

use time::precise_time_ns;

use crossbeam::scope;
use crossbeam::ScopedJoinHandle;

use std::sync::Barrier;
use std::thread;
use std::sync::atomic::{AtomicUsize, Ordering};

#[inline(never)]
fn recv(bar: &Barrier, mreader: BroadcastReceiver<u64>, sum: &AtomicUsize, check: bool) {
    let reader = mreader.into_single().unwrap();
    bar.wait();
    let start = precise_time_ns();
    let mut cur = 0;
    loop {
        match reader.recv() {
            Ok(pushed) => {
                if cur != pushed {
                    if check {
                        panic!("Got {}, expected {}", pushed, cur);
                    }
                }
                cur += 1;
            }
            Err(_) => break,
        }
    }

    sum.fetch_add((precise_time_ns() - start) as usize, Ordering::SeqCst);
}

fn send(bar: &Barrier, writer: BroadcastSender<u64>, num_push: usize) {
    bar.wait();
    for i in 0..num_push as u64 {
        loop {
            let topush = i;
            if let Ok(_) = writer.try_send(topush) {
                break;
            }
        }
    }
}

fn executeQueue(name: &str, enqueuers: usize, dequeuers: usize) {
    let num_do = 100000;
    let (enqueuer, dequeuer) = broadcast_queue_with(200, wait::BusyWait::new());
    let bar = Barrier::new(1 + enqueuers + dequeuers);
    let bref = &bar;
    let ns_atomic = AtomicUsize::new(0);
    scope(|scope| {
        for _ in 0..enqueuers {
            let w = enqueuer.clone();
            scope.spawn(move || {		
            send(bref, w, num_do); });
        }
        enqueuer.unsubscribe();
        for _ in 0..dequeuers {
            let aref = &ns_atomic;
            let r = dequeuer.addStream();
            let check = enqueuers == 1;
            scope.spawn(move || {            		
            recv(bref, r, aref, check); });
        }
        dequeuer.unsubscribe();
        bar.wait();
    });
    let ns_spent = (ns_atomic.load(Ordering::Relaxed) as f64) / dequeuers as f64;
    let ns_per_item = ns_spent / (num_do as f64);
    println!("Time spent doing {} operations was {} ns per item",
             num_do,
             ns_per_item);
}

fn main() {
    executeQueue("1p::1c", 1, 8);
}
