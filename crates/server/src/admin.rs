//! The admin listener: metrics, health, readiness and the metadata
//! service's invalidations over plaintext HTTP/1.1, on an address of its
//! own.

use crate::http::{self, Answering, RequestHead, Response};
use crate::lookups::{self, Invalidated, Kind};
use crate::metrics::{Metrics, View};
use crate::node_engine::{NodeEngine, SharedNode};
use crate::origins::Origins;
use crate::sigv4;
use bytes::Bytes;
use percent_encoding::percent_decode_str;
use std::cell::{Cell, RefCell};
use std::future::{Future, ready};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

/// What the admin listener reports on: the process's roles as they start,
/// serve and stop.
pub struct Admin {
    /// What this thread's event loop measured: the node's, and the admin
    /// listener's own.
    pub metrics: Arc<Metrics>,
    /// Whether the process runs a node, and the node once its store has
    /// recovered.
    runs_node: bool,
    node: RefCell<Option<SharedNode>>,
    /// The node's origins, which invalidations reach.
    origins: RefCell<Option<Rc<Origins>>>,
    /// The gateway's event loops once they serve, and the metadata
    /// service's token, which signs invalidations of their clients.
    gateways: RefCell<Vec<GatewayLoop>>,
    client_token: RefCell<Option<String>>,
    /// The node took a ring from a seed, or found none answering.
    joined: Cell<bool>,
    stopping: Cell<bool>,
    leaving: Cell<bool>,
}

/// A gateway event loop, as the admin listener reaches it from another
/// thread.
#[derive(Clone)]
pub struct GatewayLoop {
    pub metrics: Arc<Metrics>,
    pub observed: Arc<Mutex<Observed>>,
    /// Takes the access key IDs of clients the loop looks up again, and
    /// answers on the sender once it has dropped each.
    pub forget: mpsc::UnboundedSender<(String, oneshot::Sender<()>)>,
}

/// A gateway loop's ring, with its nodes up and down, and whether a node
/// has answered it, as of the loop's last tick.
#[derive(Clone, Copy, Default)]
pub struct Observed {
    pub ring: (u64, usize, usize),
    pub heard: bool,
}

impl GatewayLoop {
    fn observed(&self) -> Observed {
        *self.observed.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Admin {
    pub fn new(runs_node: bool) -> Rc<Admin> {
        Rc::new(Admin {
            metrics: Arc::new(Metrics::default()),
            runs_node,
            node: RefCell::new(None),
            origins: RefCell::new(None),
            gateways: RefCell::new(Vec::new()),
            client_token: RefCell::new(None),
            joined: Cell::new(false),
            stopping: Cell::new(false),
            leaving: Cell::new(false),
        })
    }

    /// The node recovered its store.
    pub fn recovered(&self, node: SharedNode) {
        *self.node.borrow_mut() = Some(node);
    }

    /// The node's origins, which take invalidations from its start.
    pub fn origins(&self, origins: Rc<Origins>) {
        *self.origins.borrow_mut() = Some(origins);
    }

    /// The node joined the cluster.
    pub fn joined(&self) {
        self.joined.set(true);
    }

    /// The gateway's loops serve clients, whom the metadata service holding
    /// `client_token`, if any, names.
    pub fn serving(&self, loops: Vec<GatewayLoop>, client_token: Option<String>) {
        *self.gateways.borrow_mut() = loops;
        *self.client_token.borrow_mut() = client_token;
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
            gateway_heard: self.gateway_heard(),
            stopping: self.stopping.get(),
            leaving: self.leaving.get(),
        };
        progress.waiting_for()
    }

    /// Whether a node has answered every gateway loop, for a process whose
    /// gateway serves.
    fn gateway_heard(&self) -> Option<bool> {
        let gateways = self.gateways.borrow();
        (!gateways.is_empty()).then(|| gateways.iter().all(|gateway| gateway.observed().heard))
    }

    fn view(&self) -> View {
        let gateways = self.gateways.borrow();
        let node = self.node.borrow().as_ref().map(NodeEngine::observe);
        // A node's ring is the one its membership holds; a gateway's, the
        // one most of its loops hold, the first loop's among equals.
        let rings: Vec<(u64, usize, usize)> = gateways
            .iter()
            .map(|gateway| gateway.observed().ring)
            .collect();
        let held = |version: u64| rings.iter().filter(|ring| ring.0 == version).count();
        let gateway_ring = rings.iter().rev().max_by_key(|ring| held(ring.0)).copied();
        let ring = match &node {
            Some((_, _, ring)) => Some(*ring),
            None => gateway_ring,
        };
        View {
            gateway: !gateways.is_empty(),
            node: node.map(|(stats, usage, _)| (stats, usage)),
            ring,
        }
    }

    /// Checks the service's invalidation of the client `access_key_id`,
    /// and has every gateway loop drop it; completes once each has.
    fn forget_client(
        &self,
        access_key_id: &str,
        path: &str,
        time: &str,
        signature: &str,
    ) -> Pin<Box<dyn Future<Output = Invalidated>>> {
        let Some(token) = self.client_token.borrow().clone() else {
            return Box::pin(ready(Invalidated::NoService));
        };
        if !lookups::signed_invalidation(&token, path, time, signature, sigv4::unix_now()) {
            return Box::pin(ready(Invalidated::Refused));
        }
        let mut dropped = Vec::new();
        for gateway in self.gateways.borrow().iter() {
            let (done, waiting) = oneshot::channel();
            if gateway
                .forget
                .send((access_key_id.to_string(), done))
                .is_ok()
            {
                dropped.push(waiting);
            }
        }
        lookups::took_invalidation(&self.metrics, Kind::Client, access_key_id);
        Box::pin(async move {
            for waiting in dropped {
                // A loop that stopped serves no requests to keep fresh.
                let _ = waiting.await;
            }
            Invalidated::Dropped
        })
    }

    fn answer(&self, head: &RequestHead) -> Answering {
        match head.method.as_str() {
            "GET" | "HEAD" => Box::pin(ready(self.report(&head.path))),
            "POST" => self.invalidate(head),
            _ => Box::pin(ready(Response::text(405, "method not allowed\n"))),
        }
    }

    /// `POST /origins/<bucket>/invalidate` or `POST /clients/<access key
    /// ID>/invalidate`, from the metadata service. A process answers once
    /// it has dropped the entry, from every gateway loop for a client.
    fn invalidate(&self, head: &RequestHead) -> Answering {
        let Some(named) = head.path.strip_suffix("/invalidate") else {
            return Box::pin(ready(Response::text(404, "not found\n")));
        };
        let time = head.header("x-accel-time").unwrap_or_default();
        let signature = head.header("x-accel-signature").unwrap_or_default();
        let (path, now) = (head.path.as_str(), sigv4::unix_now());
        let origins = self.origins.borrow().clone();
        // The name comes percent-encoded, as the service signed it.
        let decoded = |segment: &str| {
            let name = percent_decode_str(segment).decode_utf8().ok()?;
            (!name.is_empty() && !name.contains('/')).then(|| name.into_owned())
        };
        let origin = named.strip_prefix("/origins/").and_then(decoded);
        let client = named.strip_prefix("/clients/").and_then(decoded);
        let invalidated: Pin<Box<dyn Future<Output = Invalidated>>> = match (origin, client) {
            (Some(bucket), _) => Box::pin(ready(match origins {
                Some(origins) => origins.invalidate(&bucket, path, time, signature, now),
                None => Invalidated::NoService,
            })),
            (_, Some(id)) => self.forget_client(&id, path, time, signature),
            _ => return Box::pin(ready(Response::text(404, "not found\n"))),
        };
        Box::pin(async move {
            match invalidated.await {
                Invalidated::Dropped => Response {
                    status: 204,
                    headers: Vec::new(),
                    content_length: 0,
                    body: Bytes::new(),
                },
                Invalidated::Refused => Response::text(403, "forbidden\n"),
                Invalidated::NoService => {
                    Response::text(404, "the process takes these from its config\n")
                }
            }
        })
    }

    fn report(&self, path: &str) -> Response {
        match path {
            "/metrics" => {
                let gateways = self.gateways.borrow();
                let mut loops = vec![self.metrics.as_ref()];
                loops.extend(gateways.iter().map(|gateway| gateway.metrics.as_ref()));
                let mut response = Response::text(200, Metrics::render(&loops, &self.view()));
                response.headers[0].1 = "text/plain; version=0.0.4; charset=utf-8".to_string();
                response
            }
            "/healthz" => Response::text(200, "ok\n"),
            "/readyz" => match self.waiting_for().as_slice() {
                [] => Response::text(200, "ready\n"),
                waiting => Response::text(503, format!("{}\n", waiting.join("\n"))),
            },
            _ => Response::text(404, "not found\n"),
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
    http::serve_heads(listener, "admin", Rc::new(move |head| admin.answer(head))).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scrape names the ring most of the gateway's loops hold.
    #[test]
    fn a_gateway_reports_the_ring_most_loops_hold() {
        let admin = Admin::new(false);
        let gateway = |version| GatewayLoop {
            metrics: Arc::new(Metrics::default()),
            observed: Arc::new(Mutex::new(Observed {
                ring: (version, 3, 0),
                heard: true,
            })),
            forget: mpsc::unbounded_channel().0,
        };
        admin.serving(vec![gateway(9), gateway(4), gateway(4)], None);
        assert_eq!(admin.view().ring, Some((4, 3, 0)));
        admin.serving(vec![gateway(9), gateway(4)], None);
        assert_eq!(admin.view().ring, Some((9, 3, 0)));
    }

    /// A gateway is ready once a node has answered every one of its loops.
    #[test]
    fn a_gateway_waits_for_every_loop() {
        let admin = Admin::new(false);
        let gateway = |heard| {
            let observed = Observed {
                ring: (1, 3, 0),
                heard,
            };
            GatewayLoop {
                metrics: Arc::new(Metrics::default()),
                observed: Arc::new(Mutex::new(observed)),
                forget: mpsc::unbounded_channel().0,
            }
        };
        admin.serving(vec![gateway(true), gateway(false)], None);
        assert_eq!(admin.waiting_for(), ["no node has answered the gateway"]);
        admin.serving(vec![gateway(true), gateway(true)], None);
        assert!(admin.waiting_for().is_empty());
    }

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
