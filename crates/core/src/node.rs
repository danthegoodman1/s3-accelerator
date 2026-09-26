//! The storage node: serves the blocks it owns and fills misses from S3.

use crate::s3::{GetObject, ResponseHead};
use std::collections::BTreeMap;

/// A request from a gateway, numbered by the node's owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GatewayRequestId(pub u64);

/// A request the node sent to S3, numbered by the node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OriginRequestId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Send `get` to S3.
    Fetch {
        request: OriginRequestId,
        get: GetObject,
    },
    /// Answer the gateway with `head`, then the body of S3's response to
    /// `from`.
    Relay {
        request: GatewayRequestId,
        head: ResponseHead,
        from: OriginRequestId,
    },
}

#[derive(Default)]
pub struct Node {
    next_origin_request: u64,
    pending: BTreeMap<OriginRequestId, GatewayRequestId>,
    actions: Vec<Action>,
}

impl Node {
    pub fn new() -> Node {
        Node::default()
    }

    pub fn on_get(&mut self, request: GatewayRequestId, get: GetObject) {
        let origin_request = OriginRequestId(self.next_origin_request);
        self.next_origin_request += 1;
        self.pending.insert(origin_request, request);
        self.actions.push(Action::Fetch {
            request: origin_request,
            get,
        });
    }

    pub fn on_origin_response(&mut self, from: OriginRequestId, head: ResponseHead) {
        if let Some(request) = self.pending.remove(&from) {
            self.actions.push(Action::Relay {
                request,
                head,
                from,
            });
        }
    }

    /// The actions since the last drain, in the order the node took them.
    pub fn drain(&mut self) -> Vec<Action> {
        std::mem::take(&mut self.actions)
    }
}
