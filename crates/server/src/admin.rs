//! The admin listener: metrics, health and readiness over plaintext
//! HTTP/1.1, on an address of its own.

use crate::gateway_engine::{GatewayEngine, SharedGateway};
use crate::http::{Connection, Response};
use crate::metrics::{Metrics, View};
use crate::node_engine::{NodeEngine, SharedNode};
use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use tokio::net::TcpListener;

/// What the admin listener reports on: the process's roles as they start,
/// serve and stop.
pub struct Admin {
    pub metrics: Rc<Metrics>,
    /// Whether the process runs a node, and the node once its store has
    /// recovered.
    runs_node: bool,
    node: RefCell<Option<SharedNode>>,
    /// The node took a ring from a seed, or found none answering.
    joined: Cell<bool>,
    gateway: RefCell<Option<SharedGateway>>,
    stopping: Cell<bool>,
    leaving: Cell<bool>,
}

impl Admin {
    pub fn new(runs_node: bool) -> Rc<Admin> {
        Rc::new(Admin {
            metrics: Rc::new(Metrics::default()),
            runs_node,
            node: RefCell::new(None),
            joined: Cell::new(false),
            gateway: RefCell::new(None),
            stopping: Cell::new(false),
            leaving: Cell::new(false),
        })
    }

    /// The node recovered its store.
    pub fn recovered(&self, node: SharedNode) {
        *self.node.borrow_mut() = Some(node);
    }

    /// The node joined the cluster.
    pub fn joined(&self) {
        self.joined.set(true);
    }

    /// The gateway serves clients.
    pub fn serving(&self, gateway: SharedGateway) {
        *self.gateway.borrow_mut() = Some(gateway);
    }

    pub fn stopping(&self) {
        self.stopping.set(true);
    }

    pub fn leaving(&self) {
        self.leaving.set(true);
    }

    /// What the process waits for before it is ready, or why it no longer
    /// is; empty once ready.
    pub fn waiting_for(&self) -> Vec<&'static str> {
        let progress = Progress {
            node: self
                .runs_node
                .then(|| (self.node.borrow().is_some(), self.joined.get())),
            gateway_heard: self
                .gateway
                .borrow()
                .as_ref()
                .map(|gateway| GatewayEngine::observe(gateway).1),
            stopping: self.stopping.get(),
            leaving: self.leaving.get(),
        };
        progress.waiting_for()
    }

    fn view(&self) -> View {
        let gateway = self.gateway.borrow().as_ref().map(GatewayEngine::observe);
        let node = self.node.borrow().as_ref().map(NodeEngine::observe);
        // A node's ring is the one its membership holds; a gateway's is
        // the latest a node sent it.
        let ring = match (&node, &gateway) {
            (Some((_, _, ring)), _) => Some(*ring),
            (None, Some((ring, _))) => Some(*ring),
            (None, None) => None,
        };
        View {
            gateway: gateway.is_some(),
            node: node.map(|(stats, usage, _)| (stats, usage)),
            ring,
        }
    }

    fn answer(&self, path: &str) -> Response {
        let text = |status, body: String| Response {
            status,
            headers: vec![(
                "Content-Type".to_string(),
                "text/plain; charset=utf-8".to_string(),
            )],
            content_length: body.len() as u64,
            body: Bytes::from(body),
        };
        match path {
            "/metrics" => {
                let mut response = text(200, self.metrics.render(&self.view()));
                response.headers[0].1 = "text/plain; version=0.0.4; charset=utf-8".to_string();
                response
            }
            "/healthz" => text(200, "ok\n".to_string()),
            "/readyz" => match self.waiting_for().as_slice() {
                [] => text(200, "ready\n".to_string()),
                waiting => text(503, format!("{}\n", waiting.join("\n"))),
            },
            _ => text(404, "not found\n".to_string()),
        }
    }
}

/// How far a process has come toward serving.
struct Progress {
    /// For a node, whether it has recovered its store and joined.
    node: Option<(bool, bool)>,
    /// For a gateway serving clients, whether a node has answered it.
    gateway_heard: Option<bool>,
    stopping: bool,
    leaving: bool,
}

impl Progress {
    fn waiting_for(&self) -> Vec<&'static str> {
        let mut waiting = Vec::new();
        if self.stopping {
            waiting.push("the process is stopping");
        }
        if self.leaving {
            waiting.push("the node is leaving the cluster");
        }
        match self.node {
            Some((false, _)) => waiting.push("the node is recovering its store"),
            Some((true, false)) => waiting.push("the node is joining the cluster"),
            _ => {}
        }
        if self.gateway_heard == Some(false) {
            waiting.push("no node has answered the gateway");
        }
        waiting
    }
}

/// Serves the admin listener for as long as the process runs.
pub async fn serve(listener: TcpListener, admin: Rc<Admin>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let admin = admin.clone();
        tokio::task::spawn_local(async move {
            let mut connection = Connection::new(stream);
            while let Ok(Some(head)) = connection.read_head().await {
                let mut response = match head.method.as_str() {
                    "GET" | "HEAD" => admin.answer(&head.path),
                    _ => admin.answer(""),
                };
                if head.method == "HEAD" {
                    response.body = Bytes::new();
                }
                if connection
                    .write_response(&response, head.keep_alive)
                    .await
                    .is_err()
                    || !head.keep_alive
                {
                    return;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_process_is_ready_once_each_role_is() {
        let progress = |node, gateway_heard, stopping, leaving| {
            let progress = Progress {
                node,
                gateway_heard,
                stopping,
                leaving,
            };
            progress.waiting_for()
        };
        let recovering = ["the node is recovering its store"];
        assert_eq!(
            progress(Some((false, false)), None, false, false),
            recovering
        );
        let joining = ["the node is joining the cluster"];
        assert_eq!(progress(Some((true, false)), None, false, false), joining);
        let unheard = ["no node has answered the gateway"];
        assert_eq!(
            progress(Some((true, true)), Some(false), false, false),
            unheard
        );
        assert!(progress(Some((true, true)), Some(true), false, false).is_empty());
        assert!(progress(None, Some(true), false, false).is_empty());
        let leaving = ["the node is leaving the cluster"];
        assert_eq!(progress(Some((true, true)), None, false, true), leaving);
        let stopping = ["the process is stopping"];
        assert_eq!(progress(None, Some(true), true, false), stopping);
    }
}
