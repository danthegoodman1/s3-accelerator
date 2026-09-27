//! The gateway and storage node: a gateway serves S3 over plaintext
//! HTTP/1.1, and nodes serve gateways over the cluster protocol. A process
//! runs either or both.

pub mod config;
pub mod disk;
pub mod gateway_engine;
pub mod http;
pub mod membership_engine;
pub mod node_engine;
pub mod origin;
pub mod passthrough;
pub mod peers;
pub mod protocol;
pub mod server;
pub mod sigv4;
pub mod zero_copy;
