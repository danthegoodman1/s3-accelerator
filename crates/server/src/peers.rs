//! The client side of the cluster protocol, which gateways and storage
//! nodes both speak to storage nodes: connections kept idle between
//! requests, and answers whose bodies stay in their connections until read.

use crate::http::{Connection, header};
use crate::protocol::{self, NodeAnswer, NodeRequest};
use bytes::Bytes;
use s3_accelerator_core::placement::NodeId;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::time::Duration;
use tokio::net::TcpStream;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Peers {
    /// Where each node listens, as the config and rings tell, and the
    /// secret the cluster shares.
    addresses: RefCell<BTreeMap<NodeId, String>>,
    secret: Rc<str>,
    idle: RefCell<BTreeMap<NodeId, Vec<Connection>>>,
}

/// A node's answer whose body is still in its connection.
pub struct NodeBody {
    node: NodeId,
    connection: Connection,
    /// Body bytes still unread.
    len: u64,
}

impl NodeBody {
    pub fn stream(&self) -> &TcpStream {
        self.connection.stream()
    }

    /// Body bytes still unread.
    pub fn unread(&self) -> u64 {
        self.len
    }

    /// Marks `copied` more bytes of the body read.
    pub fn consumed(&mut self, copied: u64) {
        self.len = self.len.saturating_sub(copied);
    }
}

/// A node's answer, the version of its ring, and its body.
pub struct Exchanged {
    pub answer: NodeAnswer,
    pub ring: Option<u64>,
    pub body: NodeBody,
}

impl Peers {
    pub fn new(addresses: BTreeMap<NodeId, String>, secret: Rc<str>) -> Rc<Peers> {
        Rc::new(Peers {
            addresses: RefCell::new(addresses),
            secret,
            idle: RefCell::new(BTreeMap::new()),
        })
    }

    /// Learns where nodes are reached, such as nodes that joined after the
    /// config was written.
    pub fn learn(&self, addresses: &BTreeMap<NodeId, String>) {
        self.addresses
            .borrow_mut()
            .extend(addresses.iter().map(|(&id, address)| (id, address.clone())));
    }

    /// Every node whose address this process knows.
    pub fn nodes(&self) -> Vec<NodeId> {
        self.addresses.borrow().keys().copied().collect()
    }

    /// Sends one request to `node` and reads the answer's head, leaving its
    /// body in the connection. An idle connection the node has since closed
    /// gets one retry on a new one.
    pub async fn exchange(&self, node: NodeId, request: &NodeRequest) -> io::Result<Exchanged> {
        let address = self.addresses.borrow().get(&node).cloned();
        let address =
            address.ok_or_else(|| io::Error::other(format!("no address for node {}", node.0)))?;
        let address = address.as_str();
        let idle = self.idle.borrow_mut().get_mut(&node).and_then(Vec::pop);
        let (answer, ring, len, connection) = match idle {
            Some(connection) => match exchange_on(connection, request, &self.secret).await {
                Ok(exchanged) => exchanged,
                Err(_) => exchange_on(connect(address).await?, request, &self.secret).await?,
            },
            None => exchange_on(connect(address).await?, request, &self.secret).await?,
        };
        let body = NodeBody {
            node,
            connection,
            len,
        };
        Ok(Exchanged { answer, ring, body })
    }

    /// Keeps a connection for the next request if its last answer was read
    /// in full, and otherwise closes it.
    pub fn idle(&self, body: NodeBody) {
        if body.len == 0 {
            self.idle
                .borrow_mut()
                .entry(body.node)
                .or_default()
                .push(body.connection);
        }
    }

    /// Reads a body whole, such as a previous owner's blocks, and keeps its
    /// connection.
    pub async fn read_body(&self, mut body: NodeBody) -> io::Result<Bytes> {
        let len = body.len;
        let bytes = body.connection.read_body(len).await?;
        body.len = 0;
        self.idle(body);
        Ok(bytes.into())
    }
}

async fn connect(address: &str) -> io::Result<Connection> {
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(address))
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    stream.set_nodelay(true)?;
    Ok(Connection::new(stream))
}

async fn exchange_on(
    mut connection: Connection,
    request: &NodeRequest,
    secret: &str,
) -> io::Result<(NodeAnswer, Option<u64>, u64, Connection)> {
    let (method, target, headers) = protocol::encode_request(request, secret);
    connection
        .write_request(method, &target, &headers, &[])
        .await?;
    let (status, headers) = connection.read_response_head().await?;
    let answer = protocol::decode_answer(status, &headers).map_err(io::Error::other)?;
    let ring = protocol::ring_version(&headers);
    let len = header(&headers, "content-length")
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0);
    Ok((answer, ring, len, connection))
}
