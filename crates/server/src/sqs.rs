//! S3's event notifications reach storage nodes through an SQS queue,
//! which each node long-polls over SQS's JSON protocol.

use crate::origin::{self, RequestBody};
use crate::sigv4::{self, Credentials, Signer};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use percent_encoding::percent_decode_str;
use s3_accelerator_core::s3::{ETag, ObjectKey};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::io;
use std::time::Duration;

/// The most bytes an answer may hold: ten messages of up to 256 KiB each.
const ANSWER_LIMIT: u64 = 4 << 20;

pub struct Queue {
    client: Client<HttpsConnector<HttpConnector>, RequestBody>,
    /// The queue's URL, and the scheme and authority that serve it.
    url: String,
    endpoint: String,
    authority: String,
    signer: Signer,
}

/// A message the queue offered, which leaves the queue once deleted with
/// its receipt.
pub struct Offered {
    pub receipt: String,
    pub body: String,
}

impl Queue {
    pub fn new(url: &str, region: &str, credentials: Credentials) -> io::Result<Queue> {
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| io::Error::other(format!("{url} names no scheme")))?;
        let authority = rest.split('/').next().unwrap_or_default().to_string();
        Ok(Queue {
            client: origin::client(),
            url: url.to_string(),
            endpoint: format!("{scheme}://{authority}"),
            authority,
            signer: Signer {
                credentials,
                region: region.to_string(),
                service: "sqs",
            },
        })
    }

    /// Waits up to `wait` for messages, which stay hidden from other nodes
    /// for `visibility` once offered.
    pub async fn receive(&self, wait: Duration, visibility: Duration) -> io::Result<Vec<Offered>> {
        let request = json!({
            "QueueUrl": self.url,
            "MaxNumberOfMessages": 10,
            "WaitTimeSeconds": wait.as_secs(),
            "VisibilityTimeout": visibility.as_secs(),
        });
        let answer = self.call("ReceiveMessage", &request, wait).await?;
        let messages = answer["Messages"].as_array().cloned().unwrap_or_default();
        messages
            .iter()
            .map(|message| {
                let field = |name: &str| {
                    message[name]
                        .as_str()
                        .map(str::to_string)
                        .ok_or_else(|| io::Error::other(format!("a message without {name}")))
                };
                Ok(Offered {
                    receipt: field("ReceiptHandle")?,
                    body: field("Body")?,
                })
            })
            .collect()
    }

    pub async fn delete(&self, receipt: &str) -> io::Result<()> {
        let request = json!({ "QueueUrl": self.url, "ReceiptHandle": receipt });
        self.call("DeleteMessage", &request, Duration::ZERO)
            .await
            .map(drop)
    }

    /// Sends one action and returns SQS's answer. A long poll may take
    /// `wait` before SQS answers.
    async fn call(&self, action: &str, request: &Value, wait: Duration) -> io::Result<Value> {
        let body = serde_json::to_vec(request).map_err(io::Error::other)?;
        let payload_hash = hex::encode(Sha256::digest(&body));
        let mut headers = vec![
            ("host".to_string(), self.authority.clone()),
            (
                "content-type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            ),
            ("x-amz-target".to_string(), format!("AmazonSQS.{action}")),
        ];
        let now = sigv4::unix_now();
        self.signer
            .sign("POST", "/", "", &mut headers, &payload_hash, now);
        let mut builder = http::Request::post(format!("{}/", self.endpoint));
        for (name, value) in &headers {
            builder = builder.header(name, value);
        }
        let body = Full::new(Bytes::from(body))
            .map_err(|never| match never {})
            .boxed();
        let request = builder.body(body).map_err(io::Error::other)?;
        let sent = self.client.request(request);
        let response = tokio::time::timeout(wait + origin::READ_TIMEOUT, sent)
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
            .map_err(io::Error::other)?;
        let status = response.status();
        let answer = origin::collect(response.into_body(), ANSWER_LIMIT).await?;
        if !status.is_success() {
            let answer = String::from_utf8_lossy(&answer);
            return Err(io::Error::other(format!("SQS answered {status}: {answer}")));
        }
        serde_json::from_slice(&answer).map_err(io::Error::other)
    }
}

/// The changes an S3 event notification names: each object, and its new
/// ETag or `None` once it is gone. A message holds S3's event, or SNS's
/// envelope around it; S3's test event names none.
pub fn changes(body: &str) -> Result<Vec<(ObjectKey, Option<ETag>)>, String> {
    let value: Value = serde_json::from_str(body).map_err(|error| error.to_string())?;
    if let Some(Value::String(event)) = value.get("Message") {
        return changes(event);
    }
    let Some(records) = value.get("Records").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    records.iter().map(change).collect()
}

fn change(record: &Value) -> Result<(ObjectKey, Option<ETag>), String> {
    let object = &record["s3"]["object"];
    let bucket = record["s3"]["bucket"]["name"]
        .as_str()
        .ok_or("a record without a bucket")?;
    let key = object["key"].as_str().ok_or("a record without a key")?;
    // S3 encodes keys as a URL's query does, with `+` for a space.
    let key = percent_decode_str(&key.replace('+', " "))
        .decode_utf8()
        .map_err(|error| error.to_string())?
        .into_owned();
    let removed = record["eventName"]
        .as_str()
        .is_some_and(|name| name.starts_with("ObjectRemoved"));
    let etag = object["eTag"]
        .as_str()
        .filter(|_| !removed)
        .map(|etag| ETag(format!("\"{}\"", etag.trim_matches('"'))));
    let key = ObjectKey {
        bucket: bucket.to_string(),
        key,
    };
    Ok((key, etag))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> ObjectKey {
        ObjectKey {
            bucket: "logs".into(),
            key: name.into(),
        }
    }

    #[test]
    fn reads_s3_events() {
        let body = r#"{"Records":[
            {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"logs"},
             "object":{"key":"a+b%2Bc/d","eTag":"0123abcd","sequencer":"01"}}},
            {"eventName":"ObjectRemoved:Delete","s3":{"bucket":{"name":"logs"},
             "object":{"key":"gone","sequencer":"02"}}}]}"#;
        assert_eq!(
            changes(body).unwrap(),
            [
                (key("a b+c/d"), Some(ETag("\"0123abcd\"".into()))),
                (key("gone"), None),
            ]
        );
    }

    #[test]
    fn reads_events_inside_sns_envelopes() {
        let event = r#"{"Records":[{"eventName":"ObjectCreated:Copy","s3":{"bucket":{"name":"logs"},"object":{"key":"k","eTag":"ff"}}}]}"#;
        let body = json!({ "Type": "Notification", "Message": event }).to_string();
        assert_eq!(
            changes(&body).unwrap(),
            [(key("k"), Some(ETag("\"ff\"".into())))]
        );
    }

    #[test]
    fn a_test_event_names_no_change() {
        let body = r#"{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"logs"}"#;
        assert_eq!(changes(body).unwrap(), []);
    }
}
