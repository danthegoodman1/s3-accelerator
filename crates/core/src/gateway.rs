//! The gateway: routes each client request to the storage node that owns it.

use crate::placement::{NodeId, Placement, Ring};
use crate::s3::{Request, ResponseHead};
use std::collections::BTreeMap;

/// A client request, numbered by the gateway's owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientRequestId(pub u64);

/// A request the gateway sent to a storage node, numbered by the gateway.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeRequestId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Send `request` to `node`.
    Send {
        node: NodeId,
        id: NodeRequestId,
        request: Request,
    },
    /// Answer the client with `head`, then the body of the node's response
    /// to `from`.
    Relay {
        request: ClientRequestId,
        head: ResponseHead,
        from: NodeRequestId,
    },
    /// Answer the client with `head` and no body.
    Respond {
        request: ClientRequestId,
        head: ResponseHead,
    },
}

pub struct Gateway {
    ring: Ring,
    next_node_request: u64,
    pending: BTreeMap<NodeRequestId, (ClientRequestId, Request)>,
    actions: Vec<Action>,
}

impl Gateway {
    pub fn new(ring: Ring) -> Gateway {
        Gateway {
            ring,
            next_node_request: 0,
            pending: BTreeMap::new(),
            actions: Vec::new(),
        }
    }

    pub fn on_request(&mut self, id: ClientRequestId, request: Request) {
        let Some(node) = self.ring.owner(Placement::Home(&request.key).hash()) else {
            let head = ResponseHead::status(503);
            self.actions.push(Action::Respond { request: id, head });
            return;
        };
        let node_request = NodeRequestId(self.next_node_request);
        self.next_node_request += 1;
        self.pending.insert(node_request, (id, request.clone()));
        self.actions.push(Action::Send {
            node,
            id: node_request,
            request,
        });
    }

    pub fn on_node_response(&mut self, from: NodeRequestId, head: ResponseHead) {
        if let Some((request, _)) = self.pending.remove(&from) {
            self.actions.push(Action::Relay {
                request,
                head,
                from,
            });
        }
    }

    /// The object changed while the node served the request: send it again.
    pub fn on_node_stale(&mut self, from: NodeRequestId) {
        if let Some((id, request)) = self.pending.remove(&from) {
            self.on_request(id, request);
        }
    }

    /// The actions since the last drain, in the order the gateway took them.
    pub fn drain(&mut self) -> Vec<Action> {
        std::mem::take(&mut self.actions)
    }
}
