//! Gateway and storage-node logic as deterministic state machines.
//!
//! The core does no I/O, reads no clocks and starts no threads. Its owner,
//! the server or the simulator, hands it requests and responses, carries out
//! the actions it returns, and moves the bytes.

#![forbid(unsafe_code)]

pub mod gateway;
pub mod node;
pub mod placement;
pub mod s3;
