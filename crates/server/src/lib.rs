//! The storage node and gateway: serves S3 over plaintext HTTP/1.1, with the
//! gateway and a storage node in one process.

pub mod config;
pub mod engine;
pub mod http;
pub mod origin;
pub mod server;
pub mod sigv4;
