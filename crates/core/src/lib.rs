//! Gateway and storage-node logic as deterministic state machines.
//!
//! The core does no I/O, reads no clocks and starts no threads. Its owner,
//! the server or the simulator, hands it requests, responses and the time,
//! carries out the actions it returns, and moves the bytes.

#![forbid(unsafe_code)]

pub mod doorkeeper;
pub mod gateway;
pub mod layout;
pub mod membership;
pub mod node;
pub mod placement;
pub mod s3;
pub mod store;

/// Milliseconds on the owner's clock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(pub u64);
