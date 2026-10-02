//! How gateways and storage nodes talk, and nodes to each other: HTTP/1.1,
//! one request per read. The request's path names the object, and headers
//! say what to read; the node's answer is a response with its body, the
//! object's metadata, word that the object changed, or its ring. A request
//! the cache doesn't serve travels whole, its target, headers and body as
//! the client sent them, and S3's answer comes back the same way. Every
//! request carries the cluster's secret, and every answer the version of
//! the node's ring.
//!
//! `Content-Length` always counts the body bytes that follow, or a chunked
//! body follows. A response's own length, which a HEAD read answers
//! without a body, travels in `x-accel-content-length`.

use crate::http::{
    etag_condition, format_content_range, format_range, header, parse_content_range, parse_range,
};
use crate::origin::is_object_header;
use crate::sigv4;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use s3_accelerator_core::node::{ObjectMeta, RangeRead, Read};
use s3_accelerator_core::placement::{Member, NodeId, PlacementHash, Ring};
use s3_accelerator_core::s3::{ETag, Method, ObjectKey, Request, ResponseHead};
use std::collections::BTreeMap;

pub const SECRET: &str = "x-accel-secret";
/// Names the client request a node request serves, and a gateway's
/// response to it.
pub const REQUEST_ID: &str = "x-accel-request-id";

/// A client request's ID, which follows it to nodes and into logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestId(pub u64);

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// A request ID for a log line, or `none` for work no client asked for.
pub fn logged(id: Option<RequestId>) -> String {
    id.map_or_else(|| "none".to_string(), |id| id.to_string())
}
const KIND: &str = "x-accel-read";
const METHOD: &str = "x-accel-method";
const STALE: &str = "x-accel-stale";
const DIRECT: &str = "x-accel-direct";
const ETAG: &str = "x-accel-etag";
const SIZE: &str = "x-accel-size";
const FIRST: &str = "x-accel-first";
const LAST: &str = "x-accel-last";
const ANSWER: &str = "x-accel-answer";
const LENGTH: &str = "x-accel-content-length";
const S3_ERROR: &str = "x-accel-s3-error";
const META_ETAG: &str = "x-accel-meta-etag";
const META_SIZE: &str = "x-accel-meta-size";
const META_AGE: &str = "x-accel-meta-age";
const META_HEADER: &str = "x-accel-meta-header";
const RING: &str = "x-accel-ring";
const DOWN: &str = "x-accel-down";
const RING_DOWN: &str = "x-accel-ring-down";
const PASSED_ON: &str = "x-accel-passed-on";
const RING_MEMBERS: &str = "x-accel-ring-members";
const PAYLOAD: &str = "x-accel-payload";
const HOT: &str = "x-accel-hot";
const PLACEMENT: &str = "x-accel-placement";
const OWNER: &str = "x-accel-owner";
const LEFT: &str = "x-accel-left";
const READS: &str = "x-accel-reads";
/// Prefixes a forwarded request's or response's own headers.
const FORWARDED: &str = "x-accel-h-";

/// What a gateway asks of a node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeRequest {
    Read(Read),
    /// A write to the key passed through a gateway and succeeded. A node
    /// passes on a write it heard of, and `passed_on` stops it there.
    Written {
        key: ObjectKey,
        passed_on: bool,
    },
    /// Purge `key`; a node that coordinates the purge sends it with
    /// `passed_on` set.
    Purge {
        key: ObjectKey,
        passed_on: bool,
    },
    /// S3's event that `key` changed to `etag`, or went away for `None`,
    /// which another node took from the queue.
    Event {
        key: ObjectKey,
        etag: Option<ETag>,
    },
    /// `owner` leases a hot placement to the node for `left` more
    /// milliseconds.
    Lease {
        placement: PlacementHash,
        owner: NodeId,
        left: u64,
    },
    /// A replica served `reads` reads of a placement under its lease.
    LeaseReport {
        placement: PlacementHash,
        reads: u64,
    },
    /// The node's ring.
    Ring,
    /// A client's request the node passes to S3 under the cluster's
    /// signature. The request's body follows.
    Forward(Forward),
}

/// A client's request, as the gateway authenticated and authorized it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Forward {
    pub method: String,
    /// The path and query as the client sent them.
    pub path: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    /// The payload hash the client signed, which S3 checks the body against.
    pub payload_hash: String,
    /// The body's length.
    pub len: u64,
}

/// A hot placement's owner and replicas, which gateways spread its reads
/// across for `left` more milliseconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hint {
    pub placement: PlacementHash,
    pub nodes: Vec<NodeId>,
    pub left: u64,
}

/// A node's answer to a gateway. Answers to reads carry hints for the hot
/// placements they read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeAnswer {
    /// `s3_error` marks a 5xx that S3 gave, which the gateway passes to
    /// its client rather than asking another node.
    Respond {
        head: ResponseHead,
        meta: Option<ObjectMeta>,
        hot: Vec<Hint>,
        s3_error: bool,
    },
    Metadata(ObjectMeta, Vec<Hint>),
    Stale,
    /// The node took a write or event notice.
    Written,
    /// The node's ring, where each of its nodes is reached, and the nodes
    /// its membership holds down.
    Ring {
        ring: Ring,
        addresses: BTreeMap<NodeId, String>,
        down: Vec<NodeId>,
    },
    /// S3's answer to a forwarded request. Its body follows, `length` bytes
    /// long, or chunked when S3 sent no length; a HEAD's `length` is what a
    /// GET's would be.
    Forwarded {
        status: u16,
        headers: Vec<(String, String)>,
        length: Option<u64>,
    },
}

/// A request's method, target and headers.
pub fn encode_request(
    request: &NodeRequest,
    secret: &str,
) -> (&'static str, String, Vec<(String, String)>) {
    let mut headers = vec![(SECRET.to_string(), secret.to_string())];
    let mut add = |name: &str, value: String| headers.push((name.to_string(), value));
    let key = match request {
        NodeRequest::Ring => {
            add(KIND, "ring".into());
            return ("GET", "/".into(), headers);
        }
        NodeRequest::Lease {
            placement,
            owner,
            left,
        } => {
            add(KIND, "lease".into());
            add(PLACEMENT, format!("{:016x}", placement.0));
            add(OWNER, owner.0.to_string());
            add(LEFT, left.to_string());
            return ("POST", "/".into(), headers);
        }
        NodeRequest::LeaseReport { placement, reads } => {
            add(KIND, "lease-report".into());
            add(PLACEMENT, format!("{:016x}", placement.0));
            add(READS, reads.to_string());
            return ("POST", "/".into(), headers);
        }
        NodeRequest::Forward(forward) => {
            add(KIND, "forward".into());
            add(METHOD, forward.method.clone());
            add(PAYLOAD, forward.payload_hash.clone());
            for (name, value) in &forward.headers {
                add(&format!("{FORWARDED}{name}"), value.clone());
            }
            let target = match forward.query.as_str() {
                "" => forward.path.clone(),
                query => format!("{}?{query}", forward.path),
            };
            return ("POST", target, headers);
        }
        NodeRequest::Written { key, passed_on } => {
            add(KIND, "written".into());
            if *passed_on {
                add(PASSED_ON, "1".into());
            }
            key
        }
        NodeRequest::Purge { key, passed_on } => {
            add(KIND, "purge".into());
            if *passed_on {
                add(PASSED_ON, "1".into());
            }
            key
        }
        NodeRequest::Event { key, etag } => {
            add(KIND, "event".into());
            if let Some(etag) = etag {
                add(ETAG, etag.0.clone());
            }
            key
        }
        NodeRequest::Read(Read::Object {
            request,
            stale,
            direct,
        }) => {
            add(KIND, "object".into());
            let method = match request.method {
                Method::Get => "GET",
                Method::Head => "HEAD",
            };
            add(METHOD, method.into());
            if let Some(range) = request.range {
                add("range", format_range(range));
            }
            if let Some(etag) = &request.if_match {
                add("if-match", etag.0.clone());
            }
            if let Some(etag) = &request.if_none_match {
                add("if-none-match", etag.0.clone());
            }
            if let Some(etag) = stale {
                add(STALE, etag.0.clone());
            }
            if *direct {
                add(DIRECT, "1".into());
            }
            &request.key
        }
        NodeRequest::Read(Read::Range(range)) | NodeRequest::Read(Read::Stored(range)) => {
            let kind = match request {
                NodeRequest::Read(Read::Stored(_)) => "stored",
                _ => "range",
            };
            add(KIND, kind.into());
            add(ETAG, range.etag.0.clone());
            add(SIZE, range.size.to_string());
            add(FIRST, range.first.to_string());
            add(LAST, range.last.to_string());
            &range.key
        }
        NodeRequest::Read(Read::Known(key)) => {
            add(KIND, "known".into());
            key
        }
    };
    let method = match request {
        NodeRequest::Written { .. } | NodeRequest::Event { .. } | NodeRequest::Purge { .. } => {
            "POST"
        }
        NodeRequest::Read(_)
        | NodeRequest::Ring
        | NodeRequest::Forward(_)
        | NodeRequest::Lease { .. }
        | NodeRequest::LeaseReport { .. } => "GET",
    };
    (method, path(key), headers)
}

/// The request a gateway sent, from its path, query and headers, and for
/// a forwarded request, the length of the body that follows.
pub fn decode_request(
    path: &str,
    query: &str,
    headers: &[(String, String)],
    len: u64,
) -> Result<NodeRequest, String> {
    let field = |name: &str| header(headers, name).ok_or_else(|| format!("no {name}"));
    let number = |name: &str| -> Result<u64, String> {
        field(name)?
            .parse()
            .map_err(|_| format!("{name} is no number"))
    };
    let placement = || {
        u64::from_str_radix(field(PLACEMENT)?, 16)
            .map(PlacementHash)
            .map_err(|_| format!("{PLACEMENT} is no hash"))
    };
    match field(KIND)? {
        "ring" => return Ok(NodeRequest::Ring),
        "lease" => {
            return Ok(NodeRequest::Lease {
                placement: placement()?,
                owner: NodeId(number(OWNER)?),
                left: number(LEFT)?,
            });
        }
        "lease-report" => {
            return Ok(NodeRequest::LeaseReport {
                placement: placement()?,
                reads: number(READS)?,
            });
        }
        "forward" => {
            return Ok(NodeRequest::Forward(Forward {
                method: field(METHOD)?.to_string(),
                path: path.to_string(),
                query: query.to_string(),
                headers: unprefixed(headers),
                payload_hash: field(PAYLOAD)?.to_string(),
                len,
            }));
        }
        _ => {}
    }
    let key = key(path)?;
    let condition = |name: &str| {
        etag_condition(header(headers, name)).ok_or_else(|| format!("{name} names no one ETag"))
    };
    let range = |key: ObjectKey| -> Result<RangeRead, String> {
        Ok(RangeRead {
            key,
            etag: ETag(field(ETAG)?.to_string()),
            size: number(SIZE)?,
            first: number(FIRST)?,
            last: number(LAST)?,
        })
    };
    match field(KIND)? {
        "written" => Ok(NodeRequest::Written {
            key,
            passed_on: header(headers, PASSED_ON).is_some(),
        }),
        "purge" => Ok(NodeRequest::Purge {
            key,
            passed_on: header(headers, PASSED_ON).is_some(),
        }),
        "event" => Ok(NodeRequest::Event {
            key,
            etag: header(headers, ETAG).map(|etag| ETag(etag.to_string())),
        }),
        "range" => Ok(NodeRequest::Read(Read::Range(range(key)?))),
        "stored" => Ok(NodeRequest::Read(Read::Stored(range(key)?))),
        "known" => Ok(NodeRequest::Read(Read::Known(key))),
        "object" => {
            let method = match field(METHOD)? {
                "GET" => Method::Get,
                "HEAD" => Method::Head,
                other => return Err(format!("method {other}")),
            };
            let request = Request {
                method,
                key,
                range: header(headers, "range").and_then(parse_range),
                if_match: condition("if-match")?,
                if_none_match: condition("if-none-match")?,
            };
            Ok(NodeRequest::Read(Read::Object {
                request,
                stale: header(headers, STALE).map(|etag| ETag(etag.to_string())),
                direct: header(headers, DIRECT).is_some(),
            }))
        }
        other => Err(format!("{KIND} {other}")),
    }
}

/// An answer's status and headers, which name the version of the node's
/// ring. A `Respond` answer's body follows it.
/// A node's answer, stamped with its `versions`: its ring's and that of
/// the nodes its membership holds down.
pub fn encode_answer(answer: &NodeAnswer, versions: Versions) -> (u16, Vec<(String, String)>) {
    let mut headers = vec![
        (RING.to_string(), format!("{:016x}", versions.ring)),
        (DOWN.to_string(), format!("{:016x}", versions.down)),
    ];
    let status = match answer {
        NodeAnswer::Respond {
            head,
            meta,
            hot,
            s3_error,
        } => {
            encode_hints(hot, &mut headers);
            headers.push((ANSWER.to_string(), "respond".into()));
            headers.push((LENGTH.to_string(), head.content_length.to_string()));
            if *s3_error {
                headers.push((S3_ERROR.to_string(), "1".into()));
            }
            if let Some(etag) = &head.etag {
                headers.push(("etag".to_string(), etag.0.clone()));
            }
            if let Some(range) = head.content_range {
                headers.push(("content-range".to_string(), format_content_range(range)));
            }
            headers.extend(head.headers.iter().cloned());
            if let Some(meta) = meta {
                encode_meta(meta, &mut headers);
            }
            head.status
        }
        NodeAnswer::Metadata(meta, hot) => {
            encode_hints(hot, &mut headers);
            headers.push((ANSWER.to_string(), "metadata".into()));
            encode_meta(meta, &mut headers);
            200
        }
        NodeAnswer::Stale => {
            headers.push((ANSWER.to_string(), "stale".into()));
            200
        }
        NodeAnswer::Written => {
            headers.push((ANSWER.to_string(), "written".into()));
            200
        }
        NodeAnswer::Forwarded {
            status,
            headers: forwarded,
            length,
        } => {
            headers.push((ANSWER.to_string(), "forwarded".into()));
            if let Some(length) = length {
                headers.push((LENGTH.to_string(), length.to_string()));
            }
            for (name, value) in forwarded {
                headers.push((format!("{FORWARDED}{name}"), value.clone()));
            }
            *status
        }
        NodeAnswer::Ring {
            ring,
            addresses,
            down,
        } => {
            headers.push((ANSWER.to_string(), "ring".into()));
            let down: Vec<String> = down.iter().map(|node| node.0.to_string()).collect();
            headers.push((RING_DOWN.to_string(), down.join(",")));
            let members: Vec<String> = ring
                .members()
                .iter()
                .map(|member| {
                    let address = addresses.get(&member.id).map_or("", String::as_str);
                    format!("{}:{}@{address}", member.id.0, member.weight)
                })
                .collect();
            headers.push((RING_MEMBERS.to_string(), members.join(",")));
            200
        }
    };
    (status, headers)
}

/// The headers a forwarded request or response carries for S3 or the
/// client, without their prefix.
fn unprefixed(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.to_ascii_lowercase();
            let name = name.strip_prefix(FORWARDED)?;
            Some((name.to_string(), value.clone()))
        })
        .collect()
}

/// The versions an answering node stamps its answers with: of its ring,
/// and of the nodes its membership holds down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Versions {
    pub ring: u64,
    pub down: u64,
}

/// The versions of the node that answered. A node that names no down
/// nodes holds none down.
pub fn versions(headers: &[(String, String)]) -> Option<Versions> {
    let hex = |name| u64::from_str_radix(header(headers, name)?, 16).ok();
    Some(Versions {
        ring: hex(RING)?,
        down: hex(DOWN).unwrap_or(0),
    })
}

pub fn decode_answer(status: u16, headers: &[(String, String)]) -> Result<NodeAnswer, String> {
    let field = |name: &str| header(headers, name).ok_or_else(|| format!("no {name}"));
    match field(ANSWER)? {
        "respond" => {
            let head = ResponseHead {
                status,
                etag: header(headers, "etag").map(|etag| ETag(etag.to_string())),
                content_range: header(headers, "content-range").and_then(parse_content_range),
                content_length: field(LENGTH)?
                    .parse()
                    .map_err(|_| format!("{LENGTH} is no number"))?,
                headers: headers
                    .iter()
                    .filter(|(name, _)| is_object_header(name))
                    .cloned()
                    .collect(),
            };
            let meta = match header(headers, META_ETAG) {
                Some(_) => Some(decode_meta(headers)?),
                None => None,
            };
            let hot = decode_hints(headers)?;
            let s3_error = header(headers, S3_ERROR).is_some();
            Ok(NodeAnswer::Respond {
                head,
                meta,
                hot,
                s3_error,
            })
        }
        "metadata" => Ok(NodeAnswer::Metadata(
            decode_meta(headers)?,
            decode_hints(headers)?,
        )),
        "stale" => Ok(NodeAnswer::Stale),
        "written" => Ok(NodeAnswer::Written),
        "forwarded" => {
            let length = match header(headers, LENGTH) {
                Some(length) => Some(
                    length
                        .parse()
                        .map_err(|_| format!("{LENGTH} is no number"))?,
                ),
                None => None,
            };
            Ok(NodeAnswer::Forwarded {
                status,
                headers: unprefixed(headers),
                length,
            })
        }
        "ring" => {
            let version = versions(headers).ok_or_else(|| format!("no {RING}"))?.ring;
            let mut addresses = BTreeMap::new();
            let members = field(RING_MEMBERS)?
                .split(',')
                .filter(|member| !member.is_empty())
                .map(|entry| {
                    let parsed = entry.split_once('@').and_then(|(member, address)| {
                        let (id, weight) = member.split_once(':')?;
                        let member = Member {
                            id: NodeId(id.parse().ok()?),
                            weight: weight.parse().ok()?,
                        };
                        if !address.is_empty() {
                            addresses.insert(member.id, address.to_string());
                        }
                        Some(member)
                    });
                    parsed.ok_or_else(|| format!("{RING_MEMBERS} {entry}"))
                })
                .collect::<Result<Vec<_>, String>>()?;
            let ring = Ring::new(version, members);
            let down = field(RING_DOWN)?
                .split(',')
                .filter(|node| !node.is_empty())
                .map(|node| node.parse().map(NodeId))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| format!("{RING_DOWN} is no list of nodes"))?;
            Ok(NodeAnswer::Ring {
                ring,
                addresses,
                down,
            })
        }
        other => Err(format!("{ANSWER} {other}")),
    }
}

/// Each hint as `placement:left:node,node`.
fn encode_hints(hints: &[Hint], headers: &mut Vec<(String, String)>) {
    for hint in hints {
        let nodes: Vec<String> = hint.nodes.iter().map(|node| node.0.to_string()).collect();
        let value = format!(
            "{:016x}:{}:{}",
            hint.placement.0,
            hint.left,
            nodes.join(",")
        );
        headers.push((HOT.to_string(), value));
    }
}

fn decode_hints(headers: &[(String, String)]) -> Result<Vec<Hint>, String> {
    headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case(HOT))
        .map(|(_, value)| {
            let parsed = (|| {
                let mut parts = value.splitn(3, ':');
                let placement = u64::from_str_radix(parts.next()?, 16).ok()?;
                let left = parts.next()?.parse().ok()?;
                let nodes = parts
                    .next()?
                    .split(',')
                    .map(|node| node.parse().ok().map(NodeId))
                    .collect::<Option<Vec<NodeId>>>()?;
                Some(Hint {
                    placement: PlacementHash(placement),
                    nodes,
                    left,
                })
            })();
            parsed.ok_or_else(|| format!("{HOT} {value}"))
        })
        .collect()
}

fn encode_meta(meta: &ObjectMeta, headers: &mut Vec<(String, String)>) {
    headers.push((META_ETAG.to_string(), meta.etag.0.clone()));
    headers.push((META_SIZE.to_string(), meta.size.to_string()));
    headers.push((META_AGE.to_string(), meta.age.to_string()));
    for (name, value) in &meta.headers {
        let encoded = format!(
            "{}:{}",
            utf8_percent_encode(name, NON_ALPHANUMERIC),
            utf8_percent_encode(value, NON_ALPHANUMERIC)
        );
        headers.push((META_HEADER.to_string(), encoded));
    }
}

fn decode_meta(headers: &[(String, String)]) -> Result<ObjectMeta, String> {
    let field = |name: &str| header(headers, name).ok_or_else(|| format!("no {name}"));
    let number = |name: &str| -> Result<u64, String> {
        field(name)?
            .parse()
            .map_err(|_| format!("{name} is no number"))
    };
    let decode = |text: &str| percent_decode_str(text).decode_utf8_lossy().into_owned();
    let object_headers = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case(META_HEADER))
        .map(|(_, value)| {
            let (name, value) = value
                .split_once(':')
                .ok_or_else(|| format!("{META_HEADER} {value}"))?;
            Ok((decode(name), decode(value)))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(ObjectMeta {
        etag: ETag(field(META_ETAG)?.to_string()),
        size: number(META_SIZE)?,
        headers: object_headers,
        age: number(META_AGE)?,
    })
}

/// `/bucket/key`, each segment percent-encoded.
fn path(key: &ObjectKey) -> String {
    let segments: Vec<String> = key.key.split('/').map(sigv4::encode).collect();
    format!("/{}/{}", sigv4::encode(&key.bucket), segments.join("/"))
}

fn key(path: &str) -> Result<ObjectKey, String> {
    let path = path.strip_prefix('/').ok_or("a relative path")?;
    let (bucket, key) = path.split_once('/').ok_or("no key")?;
    let decode = |part: &str| percent_decode_str(part).decode_utf8_lossy().into_owned();
    Ok(ObjectKey {
        bucket: decode(bucket),
        key: decode(key),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3_accelerator_core::s3::{ByteRange, ContentRange};

    fn key() -> ObjectKey {
        ObjectKey {
            bucket: "bucket".into(),
            key: "a b/../ü?c".into(),
        }
    }

    fn round_trip(request: NodeRequest) {
        let (_, target, headers) = encode_request(&request, "secret");
        assert_eq!(header(&headers, SECRET), Some("secret"));
        let (path, query) = target.split_once('?').unwrap_or((&target, ""));
        let len = match &request {
            NodeRequest::Forward(forward) => forward.len,
            _ => 0,
        };
        assert_eq!(decode_request(path, query, &headers, len), Ok(request));
    }

    #[test]
    fn requests_round_trip() {
        let request = Request {
            method: Method::Head,
            key: key(),
            range: Some(ByteRange::Suffix { length: 5 }),
            if_match: Some(ETag("\"a\"".into())),
            if_none_match: None,
        };
        round_trip(NodeRequest::Read(Read::Object {
            request: request.clone(),
            stale: Some(ETag("\"b\"".into())),
            direct: true,
        }));
        round_trip(NodeRequest::Read(Read::Object {
            request: Request::get(key()),
            stale: None,
            direct: false,
        }));
        round_trip(NodeRequest::Read(Read::Range(RangeRead {
            key: key(),
            etag: ETag("\"c\"".into()),
            size: 100,
            first: 10,
            last: 20,
        })));
        round_trip(NodeRequest::Read(Read::Stored(RangeRead {
            key: key(),
            etag: ETag("\"d\"".into()),
            size: 50,
            first: 0,
            last: 49,
        })));
        round_trip(NodeRequest::Read(Read::Known(key())));
        round_trip(NodeRequest::Written {
            key: key(),
            passed_on: false,
        });
        round_trip(NodeRequest::Written {
            key: key(),
            passed_on: true,
        });
        round_trip(NodeRequest::Purge {
            key: key(),
            passed_on: true,
        });
        round_trip(NodeRequest::Event {
            key: key(),
            etag: Some(ETag("\"v2\"".into())),
        });
        round_trip(NodeRequest::Event {
            key: key(),
            etag: None,
        });
        round_trip(NodeRequest::Ring);
        round_trip(NodeRequest::Lease {
            placement: PlacementHash(0x0123_4567_89ab_cdef),
            owner: NodeId(4),
            left: 10_000,
        });
        round_trip(NodeRequest::LeaseReport {
            placement: PlacementHash(7),
            reads: 312,
        });
        round_trip(NodeRequest::Forward(Forward {
            method: "PUT".into(),
            path: "/bucket/a%20b/../c".into(),
            query: "x-id=PutObject".into(),
            headers: vec![
                ("content-type".into(), "text/plain".into()),
                ("x-amz-meta-note".into(), "hi".into()),
            ],
            payload_hash: "UNSIGNED-PAYLOAD".into(),
            len: 5,
        }));
    }

    #[test]
    fn answers_round_trip() {
        let meta = ObjectMeta {
            etag: ETag("\"e\"".into()),
            size: 100,
            headers: vec![("x-amz-meta-note".into(), "a: b, ü".into())],
            age: 7,
        };
        let head = ResponseHead {
            status: 206,
            etag: Some(ETag("\"e\"".into())),
            content_range: Some(ContentRange {
                first: 10,
                last: 19,
                size: 100,
            }),
            content_length: 10,
            headers: vec![("content-type".into(), "text/plain".into())],
        };
        let member = |id, weight| Member {
            id: NodeId(id),
            weight: std::num::NonZeroU32::new(weight).unwrap(),
        };
        let ring = Ring::new(0xfeed, vec![member(1, 2), member(7, 1)]);
        let addresses = BTreeMap::from([
            (NodeId(1), "10.0.0.1:9100".to_string()),
            (NodeId(7), "cache-7.internal:9100".to_string()),
        ]);
        for answer in [
            NodeAnswer::Respond {
                head: head.clone(),
                meta: Some(meta.clone()),
                hot: Vec::new(),
                s3_error: false,
            },
            NodeAnswer::Respond {
                head: ResponseHead::status(503),
                meta: None,
                hot: Vec::new(),
                s3_error: true,
            },
            NodeAnswer::Respond {
                head,
                meta: None,
                hot: vec![
                    Hint {
                        placement: PlacementHash(0xabcdef),
                        nodes: vec![NodeId(3), NodeId(1), NodeId(12)],
                        left: 9_000,
                    },
                    Hint {
                        placement: PlacementHash(u64::MAX),
                        nodes: vec![NodeId(0)],
                        left: 1,
                    },
                ],
                s3_error: false,
            },
            NodeAnswer::Metadata(meta, Vec::new()),
            NodeAnswer::Stale,
            NodeAnswer::Written,
            NodeAnswer::Ring {
                ring,
                addresses,
                down: vec![NodeId(2), NodeId(5)],
            },
            NodeAnswer::Forwarded {
                status: 404,
                headers: vec![("content-type".into(), "application/xml".into())],
                length: Some(120),
            },
            NodeAnswer::Forwarded {
                status: 200,
                headers: Vec::new(),
                length: None,
            },
        ] {
            // A node's answer names its ring, which a ring answer carries.
            let ring = match &answer {
                NodeAnswer::Ring { ring, .. } => ring.version(),
                _ => 0xabc,
            };
            let stamp = Versions { ring, down: 0xdef };
            let (status, headers) = encode_answer(&answer, stamp);
            assert_eq!(versions(&headers), Some(stamp));
            assert_eq!(decode_answer(status, &headers), Ok(answer));
        }
    }
}
