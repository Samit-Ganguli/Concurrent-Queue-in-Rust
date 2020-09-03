#![cfg_attr(feature = "cargo-clippy", allow(inline_always))]

mod alloc;
mod atomicsignal;
mod consume;
mod countedindex;
mod maybe_acquire;
mod memory;
mod mpmc;
mod multiqueue;
mod broadcast;
mod read_cursor;
pub mod wait;

pub use broadcast::{BroadcastSender, BroadcastReceiver, BroadcastUniReceiver, BroadcastFutSender,
                    BroadcastFutReceiver, BroadcastFutUniReceiver, broadcast_queue,
                    broadcast_queue_with, broadcast_fut_queue};

pub use mpmc::{MPMCSender, MPMCReceiver, MPMCUniReceiver, MPMCFutSender, MPMCFutReceiver,
               MPMCFutUniReceiver, mpmc_queue, mpmc_queue_with, mpmc_fut_queue};
