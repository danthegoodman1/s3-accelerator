//! HTTP/1.1 on one connection: a server reads requests and writes
//! responses, and a client writes requests and reads responses. Also the
//! header formats S3 and the cluster share.

use bytes::Bytes;
use s3_accelerator_core::s3::{ByteRange, ContentRange, ETag};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_HEAD: usize = 64 * 1024;
const MAX_HEADERS: usize = 100;
/// The most bytes one read of a body takes.
const READ_CHUNK: usize = 256 * 1024;
/// How long a peer may leave a write waiting.
const WRITE_IDLE: std::time::Duration = std::time::Duration::from_secs(60);
/// How long a closing connection drains a body it never read.
const LINGER: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestHead {
    pub method: String,
    /// The path as sent, still percent-encoded.
    pub path: String,
    /// The query string as sent, without the `?`.
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub keep_alive: bool,
}

/// The first value of the header `name`, in any case.
pub fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

impl RequestHead {
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }

    /// The body's length. HTTP/1.1 bodies here always carry
    /// `Content-Length`; S3 clients send it on every upload.
    pub fn content_length(&self) -> Result<u64, &'static str> {
        if self.header("transfer-encoding").is_some() {
            return Err("chunked transfer encoding");
        }
        match self.header("content-length") {
            None => Ok(0),
            Some(value) => value.trim().parse().map_err(|_| "Content-Length"),
        }
    }
}

pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    /// For a HEAD, the length a GET's body would have; otherwise the body's.
    pub content_length: u64,
    pub body: Bytes,
}

/// How a response's body is framed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    /// `Content-Length`: the body has this many bytes.
    Length(u64),
    /// Chunked transfer encoding, for a body of unknown length.
    Chunked,
}

pub struct Connection {
    stream: TcpStream,
    /// Bytes read from the stream and not yet consumed.
    buffer: Vec<u8>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buffer: Vec::new(),
        }
    }

    /// The socket, for moving body bytes inside the kernel. Bytes already
    /// read ahead stay in the connection's buffer.
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    /// The next request's head, or `None` once the client closes the
    /// connection between requests.
    pub async fn read_head(&mut self) -> io::Result<Option<RequestHead>> {
        loop {
            if let Some((head, consumed)) = parse_head(&self.buffer)? {
                self.buffer.drain(..consumed);
                return Ok(Some(head));
            }
            if self.buffer.len() > MAX_HEAD {
                return Err(invalid("request head too large"));
            }
            let mut chunk = [0; 8 * 1024];
            let read = self.stream.read(&mut chunk).await?;
            if read == 0 {
                return match self.buffer.is_empty() {
                    true => Ok(None),
                    false => Err(io::ErrorKind::UnexpectedEof.into()),
                };
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }

    /// Up to `max` bytes of a body, as they arrive: bytes read ahead first,
    /// then the next read. Fails at the end of the stream.
    pub async fn read_some(&mut self, max: usize) -> io::Result<Bytes> {
        if !self.buffer.is_empty() {
            let take = self.buffer.len().min(max);
            return Ok(self.buffer.drain(..take).collect::<Vec<u8>>().into());
        }
        let mut chunk = vec![0; max.min(READ_CHUNK)];
        let read = self.stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        chunk.truncate(read);
        Ok(chunk.into())
    }

    /// Reads a body of `len` bytes as they arrive, so memory grows with
    /// what the client sends rather than with what it claims.
    pub async fn read_body(&mut self, len: u64) -> io::Result<Vec<u8>> {
        let buffered = self
            .buffer
            .len()
            .min(usize::try_from(len).unwrap_or(usize::MAX));
        let mut body: Vec<u8> = self.buffer.drain(..buffered).collect();
        let remaining = len - body.len() as u64;
        (&mut self.stream)
            .take(remaining)
            .read_to_end(&mut body)
            .await?;
        if (body.len() as u64) < len {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(body)
    }

    /// Sends a request with a body of `body.len()` bytes.
    pub async fn write_request(
        &mut self,
        method: &str,
        target: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> io::Result<()> {
        let mut head = format!("{method} {target} HTTP/1.1\r\n");
        for (name, value) in headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
        self.stream.write_all(head.as_bytes()).await?;
        self.stream.write_all(body).await?;
        self.stream.flush().await
    }

    /// The next response's status and headers. Reads no byte past the
    /// head, so the body stays in the socket for `splice`.
    pub async fn read_response_head(&mut self) -> io::Result<(u16, Vec<(String, String)>)> {
        debug_assert!(self.buffer.is_empty(), "a response head read ahead");
        // Bytes of the head consumed so far.
        let mut head = Vec::new();
        loop {
            let mut chunk = [0; 8 * 1024];
            let peeked = self.stream.peek(&mut chunk).await?;
            if peeked == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let start = head.len();
            head.extend_from_slice(&chunk[..peeked]);
            let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
            let mut response = httparse::Response::new(&mut headers);
            let consumed = match response.parse(&head) {
                Ok(httparse::Status::Complete(consumed)) => consumed,
                Ok(httparse::Status::Partial) => {
                    if head.len() > MAX_HEAD {
                        return Err(invalid("response head too large"));
                    }
                    // Every byte peeked belongs to the head.
                    self.stream.read_exact(&mut chunk[..peeked]).await?;
                    continue;
                }
                Err(error) => return Err(invalid(&error.to_string())),
            };
            let status = response.code.ok_or_else(|| invalid("no status"))?;
            let headers = response
                .headers
                .iter()
                .map(|header| {
                    let value = String::from_utf8_lossy(header.value).into_owned();
                    (header.name.to_string(), value)
                })
                .collect();
            self.stream
                .read_exact(&mut chunk[..consumed - start])
                .await?;
            return Ok((status, headers));
        }
    }

    pub async fn write_continue(&mut self) -> io::Result<()> {
        self.stream
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .await
    }

    pub async fn write_response(
        &mut self,
        response: &Response,
        keep_alive: bool,
    ) -> io::Result<()> {
        let framing = Framing::Length(response.content_length);
        self.write_response_head(response.status, &response.headers, framing, keep_alive)
            .await?;
        self.write_all(&response.body).await
    }

    /// Writes a response's status line and headers; the body follows.
    pub async fn write_response_head(
        &mut self,
        status: u16,
        headers: &[(String, String)],
        framing: Framing,
        keep_alive: bool,
    ) -> io::Result<()> {
        let mut head = format!("HTTP/1.1 {status} {}\r\n", reason(status));
        for (name, value) in headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        match framing {
            Framing::Length(len) => head.push_str(&format!("Content-Length: {len}\r\n")),
            Framing::Chunked => head.push_str("Transfer-Encoding: chunked\r\n"),
        }
        if !keep_alive {
            head.push_str("Connection: close\r\n");
        }
        head.push_str("\r\n");
        self.write_all(head.as_bytes()).await
    }

    /// Writes all of `bytes`, failing if the peer takes none of them for
    /// `WRITE_IDLE`.
    pub async fn write_all(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            let written = tokio::time::timeout(WRITE_IDLE, self.stream.write(bytes))
                .await
                .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()))?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            bytes = &bytes[written..];
        }
        Ok(())
    }

    /// Writes one chunk of a chunked body.
    pub async fn write_chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let mut chunk = format!("{:x}\r\n", bytes.len()).into_bytes();
        chunk.extend_from_slice(bytes);
        chunk.extend_from_slice(b"\r\n");
        self.write_all(&chunk).await
    }

    /// Stops writing, then reads and drops what the peer still sends,
    /// for up to `LINGER`. A server that answers before reading a request's
    /// body closes this way: closing with unread bytes would reset the
    /// connection, and the client could lose the answer.
    pub async fn linger(&mut self) {
        let _ = self.stream.shutdown().await;
        let deadline = tokio::time::Instant::now() + LINGER;
        let mut sink = vec![0; READ_CHUNK];
        while let Ok(Ok(read)) =
            tokio::time::timeout_at(deadline, self.stream.read(&mut sink)).await
            && read > 0
        {}
    }

    /// Ends a chunked body.
    pub async fn finish_chunks(&mut self) -> io::Result<()> {
        self.write_all(b"0\r\n\r\n").await
    }
}

/// A complete request head at the start of `buffer`, and its length.
fn parse_head(buffer: &[u8]) -> io::Result<Option<(RequestHead, usize)>> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut request = httparse::Request::new(&mut headers);
    let consumed = match request.parse(buffer) {
        Ok(httparse::Status::Complete(consumed)) => consumed,
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(error) => return Err(invalid(&error.to_string())),
    };
    let target = request.path.ok_or_else(|| invalid("no request target"))?;
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let headers: Vec<(String, String)> = request
        .headers
        .iter()
        .map(|header| {
            let value = String::from_utf8_lossy(header.value).into_owned();
            (header.name.to_string(), value)
        })
        .collect();
    let connection = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("connection"))
        .map(|(_, value)| value.to_ascii_lowercase());
    let keep_alive = match request.version {
        Some(1) => connection.as_deref() != Some("close"),
        _ => connection.as_deref() == Some("keep-alive"),
    };
    let head = RequestHead {
        method: request.method.unwrap_or_default().to_string(),
        path: path.to_string(),
        query: query.to_string(),
        headers,
        keep_alive,
    };
    Ok(Some((head, consumed)))
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        206 => "Partial Content",
        304 => "Not Modified",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        411 => "Length Required",
        412 => "Precondition Failed",
        416 => "Range Not Satisfiable",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "",
    }
}

/// A `Range` header's single byte range, or `None` for anything else.
pub fn parse_range(value: &str) -> Option<ByteRange> {
    let spec = value.trim().strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (first, last) = spec.split_once('-')?;
    let number = |text: &str| text.trim().parse::<u64>().ok();
    match (first.trim(), last.trim()) {
        ("", length) => Some(ByteRange::Suffix {
            length: number(length)?,
        }),
        (first, "") => Some(ByteRange::From {
            first: number(first)?,
        }),
        (first, last) => Some(ByteRange::Inclusive {
            first: number(first)?,
            last: number(last)?,
        }),
    }
}

pub fn format_range(range: ByteRange) -> String {
    match range {
        ByteRange::Inclusive { first, last } => format!("bytes={first}-{last}"),
        ByteRange::From { first } => format!("bytes={first}-"),
        ByteRange::Suffix { length } => format!("bytes=-{length}"),
    }
}

/// `bytes first-last/size`.
pub fn parse_content_range(value: &str) -> Option<ContentRange> {
    let (span, size) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (first, last) = span.split_once('-')?;
    Some(ContentRange {
        first: first.parse().ok()?,
        last: last.parse().ok()?,
        size: size.parse().ok()?,
    })
}

pub fn format_content_range(range: ContentRange) -> String {
    format!("bytes {}-{}/{}", range.first, range.last, range.size)
}

/// An `If-Match` or `If-None-Match` header's one strong ETag: `Some(None)`
/// without the header, and `None` for a list, a wildcard or a weak ETag.
pub fn etag_condition(value: Option<&str>) -> Option<Option<ETag>> {
    let Some(value) = value.map(str::trim) else {
        return Some(None);
    };
    if value == "*" || value.contains(',') || value.starts_with("W/") {
        return None;
    }
    let quoted = match value.starts_with('"') {
        true => value.to_string(),
        false => format!("\"{value}\""),
    };
    Some(Some(ETag(quoted)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ranges_as_s3_does() {
        assert_eq!(
            parse_range("bytes=0-9"),
            Some(ByteRange::Inclusive { first: 0, last: 9 })
        );
        assert_eq!(parse_range("bytes=5-"), Some(ByteRange::From { first: 5 }));
        assert_eq!(
            parse_range("bytes=-5"),
            Some(ByteRange::Suffix { length: 5 })
        );
        assert_eq!(parse_range("bytes=0-1,4-5"), None);
        assert_eq!(parse_range("items=0-1"), None);
    }

    #[test]
    fn passes_complex_preconditions_to_s3() {
        assert_eq!(etag_condition(None), Some(None));
        assert_eq!(
            etag_condition(Some("\"a\"")),
            Some(Some(ETag("\"a\"".into())))
        );
        assert_eq!(etag_condition(Some("a")), Some(Some(ETag("\"a\"".into()))));
        assert_eq!(etag_condition(Some("*")), None);
        assert_eq!(etag_condition(Some("\"a\", \"b\"")), None);
    }

    #[test]
    fn parses_content_range() {
        let range = parse_content_range("bytes 5-9/100").unwrap();
        assert_eq!((range.first, range.last, range.size), (5, 9, 100));
        assert_eq!(parse_content_range("bytes */100"), None);
    }

    #[test]
    fn formats_ranges() {
        assert_eq!(
            format_range(ByteRange::Inclusive { first: 1, last: 2 }),
            "bytes=1-2"
        );
        assert_eq!(format_range(ByteRange::From { first: 7 }), "bytes=7-");
        assert_eq!(format_range(ByteRange::Suffix { length: 3 }), "bytes=-3");
    }

    #[test]
    fn parses_a_request_head() {
        let raw = b"GET /bucket/a%20key?x-id=GetObject HTTP/1.1\r\nHost: localhost\r\nRange: bytes=0-9\r\n\r\nrest";
        let (head, consumed) = parse_head(raw).unwrap().unwrap();
        assert_eq!(consumed, raw.len() - 4);
        assert_eq!(head.method, "GET");
        assert_eq!(head.path, "/bucket/a%20key");
        assert_eq!(head.query, "x-id=GetObject");
        assert_eq!(head.header("range"), Some("bytes=0-9"));
        assert!(head.keep_alive);
        assert_eq!(head.content_length(), Ok(0));
    }

    #[test]
    fn waits_for_a_complete_head() {
        assert_eq!(parse_head(b"GET / HTTP/1.1\r\nHost: x\r\n").unwrap(), None);
    }

    #[test]
    fn honors_connection_close() {
        let raw = b"PUT /b/k HTTP/1.1\r\nConnection: close\r\nContent-Length: 3\r\n\r\n";
        let (head, _) = parse_head(raw).unwrap().unwrap();
        assert!(!head.keep_alive);
        assert_eq!(head.content_length(), Ok(3));
    }
}
