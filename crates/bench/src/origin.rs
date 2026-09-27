//! A stand-in for S3 fast enough never to limit the cache: every object's
//! bytes are a window of one shared buffer, sent without copying. A key's
//! last segment ends in its size, as `obj-17-1048576`. It counts the
//! requests and body bytes it answers with, by the key's first segment
//! after the bucket.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use xxhash_rust::xxh3::xxh3_64;

/// The bytes every object is cut from.
const DATA_LEN: usize = 64 << 20;
static DATA: LazyLock<Vec<u8>> = LazyLock::new(|| {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    (0..DATA_LEN / 8)
        .flat_map(|_| {
            // xorshift64*
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes()
        })
        .collect()
});

/// Where in `DATA` the object at `path` starts, and its size.
pub fn object(path: &str) -> Option<(usize, u64)> {
    let size = path.rsplit('-').next()?.parse().ok()?;
    Some((xxh3_64(path.as_bytes()) as usize % DATA_LEN, size))
}

/// The object's byte at `offset`.
pub fn byte(start: usize, offset: u64) -> u8 {
    DATA[(start + offset as usize) % DATA_LEN]
}

#[derive(Default)]
pub struct Origin {
    /// Waited before each answer, as S3's time to first byte.
    pub latency: Duration,
    pub requests: AtomicU64,
    pub bytes: AtomicU64,
    /// Requests and body bytes by the key's class.
    classes: Mutex<BTreeMap<String, (u64, u64)>>,
}

impl Origin {
    pub fn new(latency: Duration) -> Arc<Origin> {
        LazyLock::force(&DATA);
        Arc::new(Origin {
            latency,
            ..Origin::default()
        })
    }

    /// Requests and body bytes for keys of `class` so far.
    pub fn class(&self, class: &str) -> (u64, u64) {
        let classes = self.classes.lock().unwrap();
        classes.get(class).copied().unwrap_or_default()
    }

    fn count(&self, path: &str, bytes: u64) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        let class = path.split('/').nth(2).unwrap_or_default().to_string();
        let mut classes = self.classes.lock().unwrap();
        let counts = classes.entry(class).or_default();
        counts.0 += 1;
        counts.1 += bytes;
    }
}

pub async fn serve(listener: TcpListener, origin: Arc<Origin>) {
    while let Ok((stream, _)) = listener.accept().await {
        let _ = stream.set_nodelay(true);
        tokio::spawn(connection(stream, origin.clone()));
    }
}

async fn connection(mut stream: TcpStream, origin: Arc<Origin>) -> io::Result<()> {
    let mut buffer = Vec::new();
    loop {
        let Some((method, path, headers, consumed)) = read_head(&mut stream, &mut buffer).await?
        else {
            return Ok(());
        };
        buffer.drain(..consumed);
        // Bodies of uploads and other requests are read and dropped.
        let mut body = header(&headers, "content-length")
            .and_then(|len| len.parse::<usize>().ok())
            .unwrap_or(0);
        let held = body.min(buffer.len());
        buffer.drain(..held);
        body -= held;
        while body > 0 {
            let mut sink = vec![0; body.min(1 << 20)];
            let read = stream.read(&mut sink).await?;
            if read == 0 {
                return Ok(());
            }
            body -= read;
        }
        tokio::time::sleep(origin.latency).await;
        let Some((start, size)) = object(&path).filter(|_| method == "GET" || method == "HEAD")
        else {
            origin.count(&path, 0);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await?;
            continue;
        };
        let etag = format!("\"{start:016x}\"");
        if header(&headers, "if-match").is_some_and(|expected| expected != etag) {
            origin.count(&path, 0);
            stream
                .write_all(b"HTTP/1.1 412 Precondition Failed\r\ncontent-length: 0\r\n\r\n")
                .await?;
            continue;
        }
        let range = header(&headers, "range").and_then(|range| parse_range(range, size));
        let (status, first, last) = match range {
            Some((first, last)) => ("206 Partial Content", first, last),
            None => ("200 OK", 0, size.saturating_sub(1)),
        };
        let len = if size == 0 { 0 } else { last - first + 1 };
        let mut head = format!(
            "HTTP/1.1 {status}\r\netag: {etag}\r\ncontent-length: {len}\r\n\
             last-modified: Mon, 01 Jan 2024 00:00:00 GMT\r\n"
        );
        if range.is_some() {
            head.push_str(&format!("content-range: bytes {first}-{last}/{size}\r\n"));
        }
        head.push_str("\r\n");
        stream.write_all(head.as_bytes()).await?;
        let sent = if method == "HEAD" { 0 } else { len };
        origin.count(&path, sent);
        let mut offset = first;
        while offset < first + sent {
            let at = (start + offset as usize) % DATA_LEN;
            let piece = ((first + sent - offset) as usize)
                .min(DATA_LEN - at)
                .min(1 << 20);
            stream.write_all(&DATA[at..at + piece]).await?;
            offset += piece as u64;
        }
    }
}

type Head = (String, String, Vec<(String, String)>, usize);

/// The next request's method, path, headers, and the bytes its head took
/// in `buffer`, or `None` once the connection closes.
async fn read_head(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> io::Result<Option<Head>> {
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut request = httparse::Request::new(&mut headers);
        if let httparse::Status::Complete(consumed) =
            request.parse(buffer).map_err(io::Error::other)?
        {
            let headers = request
                .headers
                .iter()
                .map(|header| {
                    let value = String::from_utf8_lossy(header.value).into_owned();
                    (header.name.to_ascii_lowercase(), value)
                })
                .collect();
            let method = request.method.unwrap_or_default().to_string();
            let target = request.path.unwrap_or_default();
            let path = target.split('?').next().unwrap_or_default().to_string();
            return Ok(Some((method, path, headers, consumed)));
        }
        let mut chunk = [0; 8192];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(None);
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

/// The first and last byte a `Range` header asks for.
fn parse_range(range: &str, size: u64) -> Option<(u64, u64)> {
    let (first, last) = range.strip_prefix("bytes=")?.split_once('-')?;
    if first.is_empty() {
        let suffix: u64 = last.parse().ok()?;
        return Some((size.saturating_sub(suffix), size.checked_sub(1)?));
    }
    let first: u64 = first.parse().ok()?;
    let last = match last {
        "" => size.checked_sub(1)?,
        last => last.parse::<u64>().ok()?.min(size.checked_sub(1)?),
    };
    (first <= last).then_some((first, last))
}
