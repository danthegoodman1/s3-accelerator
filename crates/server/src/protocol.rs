//! How gateways and storage nodes talk: HTTP/1.1, one request per read.
//! The request's path names the object, and headers say what to read; the
//! node's answer is a response with its body, the object's metadata, or word
//! that the object changed. Every request carries the cluster's secret.
//!
//! `Content-Length` always counts the body bytes that follow. A response's
//! own length, which a HEAD read answers without a body, travels in
//! `x-accel-content-length`.

use crate::http::{
    etag_condition, format_content_range, format_range, header, parse_content_range, parse_range,
};
use crate::origin::is_object_header;
use crate::sigv4;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use s3_accelerator_core::node::{ObjectMeta, RangeRead, Read};
use s3_accelerator_core::s3::{ETag, Method, ObjectKey, Request, ResponseHead};

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

/// What a gateway asks of a node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeRequest {
    Read(Read),
    /// A write to the key passed through the gateway and succeeded.
    Written(ObjectKey),
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
}

/// A request's method, target and headers.
pub fn encode_request(
    request: &NodeRequest,
    secret: &str,
) -> (&'static str, String, Vec<(String, String)>) {
    let mut headers = vec![(SECRET.to_string(), secret.to_string())];
    let mut add = |name: &str, value: String| headers.push((name.to_string(), value));
    let key = match request {
        NodeRequest::Written(key) => {
            add(KIND, "written".into());
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
        NodeRequest::Read(Read::Range(range)) => {
            add(KIND, "range".into());
            add(ETAG, range.etag.0.clone());
            add(SIZE, range.size.to_string());
            add(FIRST, range.first.to_string());
            add(LAST, range.last.to_string());
            &range.key
        }
    };
    let method = match request {
        NodeRequest::Written(_) => "POST",
        NodeRequest::Read(_) => "GET",
    };
    (method, path(key), headers)
}

/// The request a gateway sent, from its path and headers.
pub fn decode_request(path: &str, headers: &[(String, String)]) -> Result<NodeRequest, String> {
    let key = key(path)?;
    let field = |name: &str| header(headers, name).ok_or_else(|| format!("no {name}"));
    let number = |name: &str| -> Result<u64, String> {
        field(name)?
            .parse()
            .map_err(|_| format!("{name} is no number"))
    };
    let condition = |name: &str| {
        etag_condition(header(headers, name)).ok_or_else(|| format!("{name} names no one ETag"))
    };
    match field(KIND)? {
        "written" => Ok(NodeRequest::Written(key)),
        "range" => Ok(NodeRequest::Read(Read::Range(RangeRead {
            key,
            etag: ETag(field(ETAG)?.to_string()),
            size: number(SIZE)?,
            first: number(FIRST)?,
            last: number(LAST)?,
        }))),
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

/// An answer's status and headers. A `Respond` answer's body follows it.
pub fn encode_answer(answer: &NodeAnswer) -> (u16, Vec<(String, String)>) {
    let mut headers = Vec::new();
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
    };
    (status, headers)
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
        assert_eq!(decode_request(&target, &headers), Ok(request));
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
        round_trip(NodeRequest::Written(key()));
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
        for answer in [
            NodeAnswer::Respond {
                head: head.clone(),
                meta: Some(meta.clone()),
            },
            NodeAnswer::Respond { head, meta: None },
            NodeAnswer::Metadata(meta),
            NodeAnswer::Stale,
            NodeAnswer::Written,
        ] {
            let (status, headers) = encode_answer(&answer);
            assert_eq!(decode_answer(status, &headers), Ok(answer));
        }
    }
}
