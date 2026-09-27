//! Connects the conformance suite to the S3 endpoint under test.
//!
//! The suite runs against s3proxy directly and through the accelerator, and
//! the accelerator must pass everything s3proxy passes. Three variables pick
//! the endpoint; they default to the s3proxy that `scripts/s3proxy start`
//! runs:
//!
//! - `CONFORMANCE_ENDPOINT` (`http://localhost:8080`)
//! - `CONFORMANCE_ACCESS_KEY_ID` (`local-identity`)
//! - `CONFORMANCE_SECRET_ACCESS_KEY` (`local-credential`)

use aws_sdk_s3::Client;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_http_client::tls;
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use std::net::{IpAddr, Ipv4Addr};

pub fn client() -> Client {
    let credentials = Credentials::new(
        var("CONFORMANCE_ACCESS_KEY_ID", "local-identity"),
        var("CONFORMANCE_SECRET_ACCESS_KEY", "local-credential"),
        None,
        None,
        "conformance",
    );
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(var("CONFORMANCE_ENDPOINT", "http://localhost:8080"))
        .region(Region::new("us-east-1"))
        .credentials_provider(credentials)
        .http_client(loopback_http())
        .force_path_style(true)
        .build();
    Client::from_conf(config)
}

/// An HTTP client whose `localhost` names resolve to 127.0.0.1.
fn loopback_http() -> aws_smithy_runtime_api::client::http::SharedHttpClient {
    aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::AwsLc,
        ))
        .build_with_resolver(Loopback)
}

/// A client that names buckets in the host, virtual-hosted-style: at the
/// endpoint's port on `localhost`, whose subdomains resolve to loopback.
pub fn virtual_hosted_client() -> Client {
    let credentials = Credentials::new(
        var("CONFORMANCE_ACCESS_KEY_ID", "local-identity"),
        var("CONFORMANCE_SECRET_ACCESS_KEY", "local-credential"),
        None,
        None,
        "conformance",
    );
    let endpoint =
        var("CONFORMANCE_ENDPOINT", "http://localhost:8080").replace("127.0.0.1", "localhost");
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(credentials)
        .http_client(loopback_http())
        .build();
    Client::from_conf(config)
}

/// Resolves `localhost` and its subdomains to 127.0.0.1, where the
/// endpoint under test listens, and other names as the system does.
#[derive(Clone, Debug)]
struct Loopback;

impl ResolveDns for Loopback {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        if name == "localhost" || name.ends_with(".localhost") {
            return DnsFuture::ready(Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]));
        }
        DnsFuture::new(async move {
            let addresses = tokio::net::lookup_host((name, 0))
                .await
                .map_err(ResolveDnsError::new)?;
            Ok(addresses.map(|address| address.ip()).collect())
        })
    }
}

fn var(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Creates `name` unless it exists. Each test uses its own bucket, so tests
/// run in parallel and rerun against the same endpoint.
pub async fn bucket(client: &Client, name: &str) -> String {
    let result = client.create_bucket().bucket(name).send().await;
    if let Err(error) = result {
        let service_error = error.into_service_error();
        assert!(
            service_error.is_bucket_already_owned_by_you(),
            "create bucket {name}: {service_error:?}"
        );
    }
    name.to_string()
}

/// Puts `body` at `key` and returns its ETag.
pub async fn put(client: &Client, bucket: &str, key: &str, body: &[u8]) -> String {
    let output = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .expect("put object");
    output.e_tag.expect("put returns an ETag")
}

/// `length` bytes with no short period, so a misplaced range reads wrong
/// bytes. Tests pass different salts to tell versions apart.
pub fn pattern(length: usize, salt: u8) -> Vec<u8> {
    (0..length as u32)
        .map(|index| (index.wrapping_mul(0x9e37_79b9) >> 24) as u8 ^ salt)
        .collect()
}

/// The HTTP status of a failed request.
pub fn status<E>(error: &SdkError<E, HttpResponse>) -> Option<u16> {
    error
        .raw_response()
        .map(|response| response.status().as_u16())
}
