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
use s3_accelerator_core::placement::{Member, NodeId, Ring};
use s3_accelerator_core::s3::{ETag, Method, ObjectKey, Request, ResponseHead};
use std::collections::BTreeMap;

pub const SECRET: &str = "x-accel-secret";
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
const META_ETAG: &str = "x-accel-meta-etag";
const META_SIZE: &str = "x-accel-meta-size";
const META_AGE: &str = "x-accel-meta-age";
const META_HEADER: &str = "x-accel-meta-header";
const RING: &str = "x-accel-ring";
const PASSED_ON: &str = "x-accel-passed-on";
const RING_MEMBERS: &str = "x-accel-ring-members";
const PAYLOAD: &str = "x-accel-payload";
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

/// A node's answer to a gateway.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeAnswer {
    Respond {
        head: ResponseHead,
        meta: Option<ObjectMeta>,
    },
    Metadata(ObjectMeta),
    Stale,
    Written,
    /// The node's ring, and where each of its nodes is reached.
    Ring {
        ring: Ring,
        addresses: BTreeMap<NodeId, String>,
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
        NodeRequest::Written { .. } => "POST",
        NodeRequest::Read(_) | NodeRequest::Ring | NodeRequest::Forward(_) => "GET",
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
    match field(KIND)? {
        "ring" => return Ok(NodeRequest::Ring),
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
    let number = |name: &str| -> Result<u64, String> {
        field(name)?
            .parse()
            .map_err(|_| format!("{name} is no number"))
    };
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
pub fn encode_answer(answer: &NodeAnswer, ring: u64) -> (u16, Vec<(String, String)>) {
    let mut headers = vec![(RING.to_string(), format!("{ring:016x}"))];
    let status = match answer {
        NodeAnswer::Respond { head, meta } => {
            headers.push((ANSWER.to_string(), "respond".into()));
            headers.push((LENGTH.to_string(), head.content_length.to_string()));
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
        NodeAnswer::Metadata(meta) => {
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
        NodeAnswer::Ring { ring, addresses } => {
            headers.push((ANSWER.to_string(), "ring".into()));
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

/// The version of the ring of the node that answered.
pub fn ring_version(headers: &[(String, String)]) -> Option<u64> {
    u64::from_str_radix(header(headers, RING)?, 16).ok()
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
            Ok(NodeAnswer::Respond { head, meta })
        }
        "metadata" => Ok(NodeAnswer::Metadata(decode_meta(headers)?)),
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
            let version = ring_version(headers).ok_or_else(|| format!("no {RING}"))?;
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
            Ok(NodeAnswer::Ring { ring, addresses })
        }
        other => Err(format!("{ANSWER} {other}")),
    }
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
        round_trip(NodeRequest::Ring);
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
            },
            NodeAnswer::Respond { head, meta: None },
            NodeAnswer::Metadata(meta),
            NodeAnswer::Stale,
            NodeAnswer::Written,
            NodeAnswer::Ring { ring, addresses },
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
            let version = match &answer {
                NodeAnswer::Ring { ring, .. } => ring.version(),
                _ => 0xabc,
            };
            let (status, headers) = encode_answer(&answer, version);
            assert_eq!(ring_version(&headers), Some(version));
            assert_eq!(decode_answer(status, &headers), Ok(answer));
        }
    }
}
