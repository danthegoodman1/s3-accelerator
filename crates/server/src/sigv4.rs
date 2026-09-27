//! AWS Signature Version 4 for S3: verifying clients' requests and signing
//! requests to the origin. Both build the same canonical request.

use crate::http::header;
use hmac::{Hmac, KeyInit, Mac};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use sha2::{Digest, Sha256};
use std::fmt;

/// Clients may sign a request without hashing its body.
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
/// The payload hash of an empty body.
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// How far a request's signing time may be from now, in seconds.
const MAX_SKEW: i64 = 15 * 60;
/// How long a presigned URL may last, in seconds: seven days.
pub const MAX_EXPIRES: i64 = 7 * 24 * 60 * 60;
/// The query parameters that carry a presigned URL's signature.
pub const PRESIGN_PARAMETERS: [&str; 7] = [
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
];

/// Everything but the unreserved characters of RFC 3986.
const ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// The parts of a request a signature covers.
pub struct Signable<'a> {
    pub method: &'a str,
    /// The path as sent.
    pub path: &'a str,
    /// The query string as sent, without the `?`.
    pub query: &'a str,
    /// Every header; names in any case.
    pub headers: &'a [(String, String)],
    pub payload_hash: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthError {
    Missing,
    Malformed(&'static str),
    UnknownAccessKey,
    Expired,
    SignatureMismatch,
    /// An `x-amz-*` header the signature doesn't cover, which the node
    /// would sign on the client's behalf.
    UnsignedHeader(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AuthError::Missing => write!(f, "the request is not signed"),
            AuthError::Malformed(what) => write!(f, "malformed {what}"),
            AuthError::UnknownAccessKey => write!(f, "unknown access key"),
            AuthError::Expired => write!(f, "the request was signed too long ago"),
            AuthError::SignatureMismatch => write!(f, "the signature does not match"),
            AuthError::UnsignedHeader(name) => write!(f, "the header {name} is not signed"),
        }
    }
}

/// A parsed `Authorization` header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Authorization {
    pub access_key_id: String,
    pub date: String,
    pub region: String,
    pub service: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

impl Authorization {
    pub fn parse(value: &str) -> Result<Authorization, AuthError> {
        let fields = value
            .strip_prefix("AWS4-HMAC-SHA256 ")
            .ok_or(AuthError::Malformed("algorithm"))?;
        let mut credential = None;
        let mut signed_headers = None;
        let mut signature = None;
        for field in fields.split(',') {
            let (name, value) = field
                .trim()
                .split_once('=')
                .ok_or(AuthError::Malformed("Authorization"))?;
            match name {
                "Credential" => credential = Some(value),
                "SignedHeaders" => signed_headers = Some(value),
                "Signature" => signature = Some(value),
                _ => return Err(AuthError::Malformed("Authorization")),
            }
        }
        let credential = credential.ok_or(AuthError::Malformed("Credential"))?;
        let signed_headers = signed_headers.ok_or(AuthError::Malformed("SignedHeaders"))?;
        let signature = signature.ok_or(AuthError::Malformed("Signature"))?;
        Authorization::of(credential, signed_headers, signature)
    }

    /// A presigned URL's signature, from its query parameters.
    pub fn from_query(query: &str) -> Result<Authorization, AuthError> {
        let parameters = decoded_query(query);
        let get = |name: &'static str| {
            parameters
                .iter()
                .find(|(parameter, _)| parameter == name)
                .map(|(_, value)| value.as_str())
                .ok_or(AuthError::Malformed(name))
        };
        if get("X-Amz-Algorithm")? != "AWS4-HMAC-SHA256" {
            return Err(AuthError::Malformed("X-Amz-Algorithm"));
        }
        if get("X-Amz-Security-Token").is_ok() {
            // The gateway issues no session credentials.
            return Err(AuthError::Malformed("X-Amz-Security-Token"));
        }
        Authorization::of(
            get("X-Amz-Credential")?,
            get("X-Amz-SignedHeaders")?,
            get("X-Amz-Signature")?,
        )
    }

    fn of(
        credential: &str,
        signed_headers: &str,
        signature: &str,
    ) -> Result<Authorization, AuthError> {
        let mut scope = credential.split('/');
        let mut part = || scope.next().ok_or(AuthError::Malformed("Credential"));
        let (access_key_id, date, region, service, terminator) =
            (part()?, part()?, part()?, part()?, part()?);
        if terminator != "aws4_request" {
            return Err(AuthError::Malformed("Credential"));
        }
        Ok(Authorization {
            access_key_id: access_key_id.to_string(),
            date: date.to_string(),
            region: region.to_string(),
            service: service.to_string(),
            signed_headers: signed_headers.split(';').map(str::to_string).collect(),
            signature: signature.to_string(),
        })
    }
}

/// Whether a request carries its signature in its query: a presigned URL.
pub fn is_presigned(query: &str) -> bool {
    query
        .split('&')
        .any(|pair| pair.split('=').next() == Some("X-Amz-Algorithm"))
}

/// A query's parameters, each name and value decoded.
fn decoded_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            let decode = |part: &str| percent_decode_str(part).decode_utf8_lossy().into_owned();
            (decode(name), decode(value))
        })
        .collect()
}

/// Checks a client's signed request: its `Authorization` header, or a
/// presigned URL's query. `lookup` finds the client with an access key,
/// and its secret; `now` is Unix seconds.
pub fn verify<'c, C: ?Sized>(
    request: &Signable,
    now: i64,
    lookup: impl Fn(&str) -> Option<(&'c C, &'c str)>,
) -> Result<&'c C, AuthError> {
    if let Some(value) = header(request.headers, "authorization") {
        let authorization = Authorization::parse(value)?;
        let amz_date =
            header(request.headers, "x-amz-date").ok_or(AuthError::Malformed("x-amz-date"))?;
        let signed_at = parse_amz_date(amz_date).ok_or(AuthError::Malformed("x-amz-date"))?;
        if (now - signed_at).abs() > MAX_SKEW {
            return Err(AuthError::Expired);
        }
        return check(request, &authorization, amz_date, lookup);
    }
    if !is_presigned(request.query) {
        return Err(AuthError::Missing);
    }
    let authorization = Authorization::from_query(request.query)?;
    let parameters = decoded_query(request.query);
    let get = |name: &str| {
        parameters
            .iter()
            .find(|(parameter, _)| parameter == name)
            .map(|(_, value)| value.as_str())
    };
    let amz_date = get("X-Amz-Date").ok_or(AuthError::Malformed("X-Amz-Date"))?;
    let signed_at = parse_amz_date(amz_date).ok_or(AuthError::Malformed("X-Amz-Date"))?;
    let expires = get("X-Amz-Expires")
        .and_then(|expires| expires.parse::<i64>().ok())
        .filter(|expires| (1..=MAX_EXPIRES).contains(expires))
        .ok_or(AuthError::Malformed("X-Amz-Expires"))?;
    // Valid from its signing, give or take the skew, until it expires.
    if now + MAX_SKEW < signed_at || now > signed_at + expires {
        return Err(AuthError::Expired);
    }
    // The signature covers every parameter but itself, and not the body.
    let query: Vec<&str> = request
        .query
        .split('&')
        .filter(|pair| pair.split('=').next() != Some("X-Amz-Signature"))
        .collect();
    let unsigned = Signable {
        query: &query.join("&"),
        payload_hash: UNSIGNED_PAYLOAD,
        ..*request
    };
    check(&unsigned, &authorization, amz_date, lookup)
}

/// Checks `request`'s signature against the one `authorization` names.
fn check<'c, C: ?Sized>(
    request: &Signable,
    authorization: &Authorization,
    amz_date: &str,
    lookup: impl Fn(&str) -> Option<(&'c C, &'c str)>,
) -> Result<&'c C, AuthError> {
    if !amz_date.starts_with(&authorization.date) || authorization.service != "s3" {
        return Err(AuthError::Malformed("Credential"));
    }
    if !authorization
        .signed_headers
        .iter()
        .any(|name| name == "host")
    {
        return Err(AuthError::Malformed("SignedHeaders"));
    }
    // S3 takes no x-amz-* header its signature leaves out.
    if let Some((name, _)) = request.headers.iter().find(|(name, _)| {
        let name = name.to_ascii_lowercase();
        name.starts_with("x-amz-") && !authorization.signed_headers.contains(&name)
    }) {
        return Err(AuthError::UnsignedHeader(name.to_ascii_lowercase()));
    }
    let (client, secret) =
        lookup(&authorization.access_key_id).ok_or(AuthError::UnknownAccessKey)?;
    let signed_headers: Vec<&str> = authorization
        .signed_headers
        .iter()
        .map(String::as_str)
        .collect();
    let expected = signature(
        request,
        &signed_headers,
        amz_date,
        (&authorization.region, "s3"),
        secret,
    );
    if !constant_time_eq(expected.as_bytes(), authorization.signature.as_bytes()) {
        return Err(AuthError::SignatureMismatch);
    }
    Ok(client)
}

/// Signs requests to one AWS service, such as `s3`, in one region with one
/// credential.
pub struct Signer {
    pub credentials: Credentials,
    pub region: String,
    pub service: &'static str,
}

impl Signer {
    /// Adds `x-amz-date` and `x-amz-content-sha256`, then an
    /// `Authorization` header covering every header. `now` is Unix seconds.
    pub fn sign(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &mut Vec<(String, String)>,
        payload_hash: &str,
        now: i64,
    ) {
        sign(self, method, path, query, headers, payload_hash, now);
    }

    /// The query of a presigned URL for a request to `host`, valid for
    /// `expires` seconds from `now`: `query` followed by the parameters of
    /// a signature that covers the host and not the body.
    pub fn presign(
        &self,
        (method, path, query): (&str, &str, &str),
        host: &str,
        expires: i64,
        now: i64,
    ) -> String {
        let amz_date = format_amz_date(now);
        let credential = format!(
            "{}/{}/{}/{}/aws4_request",
            self.credentials.access_key_id,
            &amz_date[..8],
            self.region,
            self.service
        );
        let mut parameters: Vec<String> = query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(str::to_string)
            .collect();
        parameters.extend([
            "X-Amz-Algorithm=AWS4-HMAC-SHA256".to_string(),
            format!("X-Amz-Credential={}", encode(&credential)),
            format!("X-Amz-Date={amz_date}"),
            format!("X-Amz-Expires={expires}"),
            "X-Amz-SignedHeaders=host".to_string(),
        ]);
        let query = parameters.join("&");
        let headers = vec![("host".to_string(), host.to_string())];
        let request = Signable {
            method,
            path,
            query: &query,
            headers: &headers,
            payload_hash: UNSIGNED_PAYLOAD,
        };
        let secret = &self.credentials.secret_access_key;
        let scope = (self.region.as_str(), self.service);
        let signature = signature(&request, &["host"], &amz_date, scope, secret);
        format!("{query}&X-Amz-Signature={signature}")
    }
}

fn sign(
    signer: &Signer,
    method: &str,
    path: &str,
    query: &str,
    headers: &mut Vec<(String, String)>,
    payload_hash: &str,
    now: i64,
) {
    let (credentials, region) = (&signer.credentials, signer.region.as_str());
    let service = signer.service;
    headers.retain(|(name, _)| {
        !["authorization", "x-amz-date", "x-amz-content-sha256"]
            .contains(&name.to_ascii_lowercase().as_str())
    });
    let amz_date = format_amz_date(now);
    headers.push(("x-amz-date".into(), amz_date.clone()));
    headers.push(("x-amz-content-sha256".into(), payload_hash.into()));
    let mut signed: Vec<String> = headers
        .iter()
        .map(|(name, _)| name.to_ascii_lowercase())
        .collect();
    signed.sort();
    signed.dedup();
    let signed: Vec<&str> = signed.iter().map(String::as_str).collect();
    let request = Signable {
        method,
        path,
        query,
        headers,
        payload_hash,
    };
    let signature = signature(
        &request,
        &signed,
        &amz_date,
        (region, service),
        &credentials.secret_access_key,
    );
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}/{region}/{service}/aws4_request, SignedHeaders={}, Signature={signature}",
        credentials.access_key_id,
        &amz_date[..8],
        signed.join(";"),
    );
    headers.push(("authorization".into(), authorization));
}

/// The signature of `request` in the scope of a region and a service.
fn signature(
    request: &Signable,
    signed_headers: &[&str],
    amz_date: &str,
    (region, service): (&str, &str),
    secret: &str,
) -> String {
    let canonical = canonical_request(request, signed_headers);
    let date = &amz_date[..amz_date.len().min(8)];
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let mut key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    for part in [region, service, "aws4_request"] {
        key = hmac(&key, part.as_bytes());
    }
    hex::encode(hmac(&key, string_to_sign.as_bytes()))
}

fn canonical_request(request: &Signable, signed_headers: &[&str]) -> String {
    let mut canonical = format!(
        "{}\n{}\n{}\n",
        request.method,
        canonical_path(request.path),
        canonical_query(request.query)
    );
    for name in signed_headers {
        let values: Vec<String> = request
            .headers
            .iter()
            .filter(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect();
        canonical.push_str(&format!("{name}:{}\n", values.join(",")));
    }
    canonical.push_str(&format!(
        "\n{}\n{}",
        signed_headers.join(";"),
        request.payload_hash
    ));
    canonical
}

/// S3 encodes each path segment once.
fn canonical_path(path: &str) -> String {
    path.split('/')
        .map(|segment| encode(&percent_decode_str(segment).decode_utf8_lossy()))
        .collect::<Vec<_>>()
        .join("/")
}

fn canonical_query(query: &str) -> String {
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            let decode = |part: &str| percent_decode_str(part).decode_utf8_lossy().into_owned();
            (encode(&decode(name)), encode(&decode(value)))
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-encodes everything but RFC 3986's unreserved characters.
pub fn encode(text: &str) -> String {
    utf8_percent_encode(text, ENCODE).to_string()
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Unix seconds now.
pub fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64)
}

/// Whether `a` and `b` hold the same bytes, taking the same time wherever
/// they differ.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// `YYYYMMDDTHHMMSSZ` for Unix seconds `now`.
pub fn format_amz_date(now: i64) -> String {
    let (days, seconds) = (now.div_euclid(86_400), now.rem_euclid(86_400));
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        seconds / 3_600,
        seconds / 60 % 60,
        seconds % 60
    )
}

/// Unix seconds for a `YYYYMMDDTHHMMSSZ` timestamp.
pub fn parse_amz_date(text: &str) -> Option<i64> {
    let digits = |range: std::ops::Range<usize>| -> Option<i64> { text.get(range)?.parse().ok() };
    if !text.is_ascii() || text.len() != 16 || &text[8..9] != "T" || &text[15..] != "Z" {
        return None;
    }
    let days = days_from_civil(digits(0..4)?, digits(4..6)?, digits(6..8)?);
    Some(days * 86_400 + digits(9..11)? * 3_600 + digits(11..13)? * 60 + digits(13..15)?)
}

/// Howard Hinnant's date algorithms.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    /// AWS's "GET Object" example for header-based authentication.
    #[test]
    fn signs_the_get_object_example() {
        let headers = headers(&[
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("Range", "bytes=0-9"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", "20130524T000000Z"),
        ]);
        let request = Signable {
            method: "GET",
            path: "/test.txt",
            query: "",
            headers: &headers,
            payload_hash: EMPTY_SHA256,
        };
        let signed = ["host", "range", "x-amz-content-sha256", "x-amz-date"];
        assert_eq!(
            signature(
                &request,
                &signed,
                "20130524T000000Z",
                ("us-east-1", "s3"),
                SECRET
            ),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// AWS's presigned URL example: a GET of `test.txt` valid for a day.
    const PRESIGNED: &str = "X-Amz-Algorithm=AWS4-HMAC-SHA256\
        &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request\
        &X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\
        &X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404";

    fn verify_presigned(query: &str, now: i64) -> Result<(), AuthError> {
        let headers = headers(&[("Host", "examplebucket.s3.amazonaws.com")]);
        let request = Signable {
            method: "GET",
            path: "/test.txt",
            query,
            headers: &headers,
            payload_hash: UNSIGNED_PAYLOAD,
        };
        let lookup = |id: &str| (id == "AKIAIOSFODNN7EXAMPLE").then_some((&(), SECRET));
        verify(&request, now, lookup).map(|_| ())
    }

    #[test]
    fn presigns_the_presigned_url_example() {
        let signer = Signer {
            credentials: Credentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
                secret_access_key: SECRET.into(),
            },
            region: "us-east-1".into(),
            service: "s3",
        };
        let signed = parse_amz_date("20130524T000000Z").unwrap();
        let query = signer.presign(
            ("GET", "/test.txt", ""),
            "examplebucket.s3.amazonaws.com",
            86_400,
            signed,
        );
        assert_eq!(query, PRESIGNED);
    }

    /// A presigned URL's holder can add no `x-amz-*` header it didn't sign.
    #[test]
    fn a_presigned_url_takes_no_unsigned_amz_header() {
        let headers = headers(&[
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-copy-source", "examplebucket/private"),
        ]);
        let request = Signable {
            method: "GET",
            path: "/test.txt",
            query: PRESIGNED,
            headers: &headers,
            payload_hash: UNSIGNED_PAYLOAD,
        };
        let lookup = |id: &str| (id == "AKIAIOSFODNN7EXAMPLE").then_some((&(), SECRET));
        let signed = parse_amz_date("20130524T000000Z").unwrap();
        assert_eq!(
            verify(&request, signed + 60, lookup).map(|_| ()),
            Err(AuthError::UnsignedHeader("x-amz-copy-source".into()))
        );
    }

    #[test]
    fn verifies_the_presigned_url_example_until_it_expires() {
        let signed = parse_amz_date("20130524T000000Z").unwrap();
        assert_eq!(verify_presigned(PRESIGNED, signed + 3_600), Ok(()));
        assert_eq!(verify_presigned(PRESIGNED, signed + 86_400), Ok(()));
        assert_eq!(
            verify_presigned(PRESIGNED, signed + 86_401),
            Err(AuthError::Expired)
        );
        assert_eq!(
            verify_presigned(PRESIGNED, signed - 3_600),
            Err(AuthError::Expired)
        );
        let altered = PRESIGNED.replace("Expires=86400", "Expires=86401");
        assert_eq!(
            verify_presigned(&altered, signed + 3_600),
            Err(AuthError::SignatureMismatch)
        );
        let too_long = PRESIGNED.replace("Expires=86400", "Expires=604801");
        assert_eq!(
            verify_presigned(&too_long, signed + 3_600),
            Err(AuthError::Malformed("X-Amz-Expires"))
        );
    }

    /// AWS's "GET Bucket (List Objects)" example, which has a query.
    #[test]
    fn signs_the_list_objects_example() {
        let headers = headers(&[
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", "20130524T000000Z"),
        ]);
        let request = Signable {
            method: "GET",
            path: "/",
            query: "max-keys=2&prefix=J",
            headers: &headers,
            payload_hash: EMPTY_SHA256,
        };
        let signed = ["host", "x-amz-content-sha256", "x-amz-date"];
        assert_eq!(
            signature(
                &request,
                &signed,
                "20130524T000000Z",
                ("us-east-1", "s3"),
                SECRET
            ),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    #[test]
    fn verifies_what_it_signs() {
        let credentials = Credentials {
            access_key_id: "AKID".into(),
            secret_access_key: SECRET.into(),
        };
        let now = parse_amz_date("20260926T120000Z").unwrap();
        let mut headers = headers(&[("Host", "localhost:9000"), ("Range", "bytes=0-9")]);
        let signer = Signer {
            credentials,
            region: "us-east-1".into(),
            service: "s3",
        };
        signer.sign(
            "GET",
            "/bucket/a%20key",
            "versionId=3",
            &mut headers,
            UNSIGNED_PAYLOAD,
            now,
        );
        let request = Signable {
            method: "GET",
            path: "/bucket/a%20key",
            query: "versionId=3",
            headers: &headers,
            payload_hash: UNSIGNED_PAYLOAD,
        };
        let lookup = |key: &str| (key == "AKID").then_some(("AKID", SECRET));
        assert_eq!(verify(&request, now, lookup), Ok("AKID"));
        assert_eq!(
            verify(&request, now + 16 * 60, lookup),
            Err(AuthError::Expired)
        );
        let tampered = Signable {
            query: "versionId=4",
            ..request
        };
        assert_eq!(
            verify(&tampered, now, lookup),
            Err(AuthError::SignatureMismatch)
        );
        assert_eq!(
            verify(&request, now, |_| None::<(&str, &str)>),
            Err(AuthError::UnknownAccessKey)
        );
    }

    #[test]
    fn dates_round_trip() {
        for text in [
            "19700101T000000Z",
            "20130524T000000Z",
            "20240229T235959Z",
            "21000301T010203Z",
        ] {
            let seconds = parse_amz_date(text).unwrap();
            assert_eq!(format_amz_date(seconds), text);
        }
        assert_eq!(parse_amz_date("20130524T000000Z"), Some(1_369_353_600));
        assert_eq!(parse_amz_date("2013-05-24"), None);
        assert_eq!(parse_amz_date("1234567é9012345Z"), None);
    }
}
