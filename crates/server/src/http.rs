//! HTTP/1.1 on one connection: a server reads requests and writes
//! responses, and a client writes requests and reads responses. Also the
//! header formats S3 and the cluster share.

use crate::protocol::{REQUEST_ID, RequestId};
use crate::tls::Session;
use bytes::Bytes;
use percent_encoding::percent_decode_str;
use rustix::net::SendFlags;
use s3_accelerator_core::s3::{ByteRange, ContentRange, ETag};
use std::fmt::Write as _;
use std::io;
use std::pin::Pin;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

const MAX_HEAD: usize = 64 * 1024;
const MAX_HEADERS: usize = 100;
/// The most bytes one read of a body takes.
const READ_CHUNK: usize = 256 * 1024;
/// The most of a node's answer read with its head: a small body, as of a
/// 4 KiB range, arrives whole, and a larger one's first bytes go on from
/// memory before `splice` moves the rest.
pub const READ_AHEAD: usize = 16 * 1024;
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

impl Response {
    /// A plain-text response.
    pub fn text(status: u16, body: impl Into<String>) -> Response {
        let body = body.into();
        Response {
            status,
            headers: vec![(
                "Content-Type".to_string(),
                "text/plain; charset=utf-8".to_string(),
            )],
            content_length: body.len() as u64,
            body: Bytes::from(body),
        }
    }
}

/// How many connections wait in a listener's queue to be accepted; the
/// kernel caps it at `net.core.somaxconn`.
const BACKLOG: u32 = 4_096;
/// How long a listener waits after failing to accept.
const ACCEPT_PAUSE: std::time::Duration = std::time::Duration::from_millis(100);

/// Listens on the first of `address`'s addresses that binds. The queue of
/// connections waiting to be accepted holds `BACKLOG` of them: with the
/// standard library's 128, a burst of new connections loses handshakes, and
/// each client sends its handshake again a second later.
pub async fn listen(address: &str) -> io::Result<TcpListener> {
    let mut failure = io::Error::new(io::ErrorKind::InvalidInput, "no address to listen on");
    for address in tokio::net::lookup_host(address).await? {
        let socket = match address {
            std::net::SocketAddr::V4(_) => TcpSocket::new_v4()?,
            std::net::SocketAddr::V6(_) => TcpSocket::new_v6()?,
        };
        socket.set_reuseaddr(true)?;
        match socket.bind(address) {
            Ok(()) => return socket.listen(BACKLOG),
            Err(error) => failure = error,
        }
    }
    Err(failure)
}

/// The next connection `listener`, named `name` in logs, takes. An error
/// such as running out of descriptors lasts a while: the listener logs it
/// and tries again after a pause, which keeps the event loop free for the
/// connections it has, and serves again once descriptors free up.
pub async fn accept(listener: &TcpListener, name: &'static str) -> TcpStream {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => return stream,
            Err(error) => {
                crate::log!(
                    Warn,
                    "a listener failed to accept",
                    listener = name,
                    error = error
                );
                tokio::time::sleep(ACCEPT_PAUSE).await;
            }
        }
    }
}

/// A response a listener waits for.
pub type Answering = Pin<Box<dyn Future<Output = Response>>>;

/// Serves `listener`, named `name` in logs, over plaintext HTTP/1.1 for as
/// long as the process runs, answering each request from its head alone.
/// It reads no bodies, so a request that sends one ends its connection
/// after the answer, and a `HEAD`'s answer carries none.
pub async fn serve_heads(
    listener: TcpListener,
    name: &'static str,
    answer: std::rc::Rc<dyn Fn(&RequestHead) -> Answering>,
) {
    loop {
        let stream = accept(&listener, name).await;
        let answer = answer.clone();
        tokio::task::spawn_local(async move {
            let mut connection = Connection::new(stream);
            while let Ok(Some(head)) = connection.read_head().await {
                let mut response = answer(&head).await;
                if head.method == "HEAD" {
                    response.body = Bytes::new();
                }
                let bodiless = head
                    .header("content-length")
                    .is_none_or(|length| length.trim() == "0")
                    && head.header("transfer-encoding").is_none();
                let keep_alive = head.keep_alive && bodiless;
                if connection
                    .write_response(&response, keep_alive)
                    .await
                    .is_err()
                    || !keep_alive
                {
                    return;
                }
            }
        });
    }
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
    /// The kernel holds the connection's TLS session.
    kernel_tls: bool,
    /// The session still owes the client a `close_notify`.
    owes_close_notify: bool,
    /// The status of the last response head written and when it went, and
    /// the body bytes sent since.
    answered: Option<(u16, Instant)>,
    body_sent: u64,
    /// The ID of the client request being answered, which its response
    /// names.
    request_id: Option<RequestId>,
}

/// A response as it went: its status, when its head was written, and the
/// body bytes sent.
pub struct Answered {
    pub status: u16,
    pub head_sent: Instant,
    pub body_sent: u64,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buffer: Vec::new(),
            kernel_tls: false,
            owes_close_notify: false,
            answered: None,
            body_sent: 0,
            request_id: None,
        }
    }

    /// A connection after its TLS handshake, starting with the plaintext
    /// the handshake read past its end.
    pub fn tls(session: Session) -> Connection {
        Connection {
            stream: session.stream,
            buffer: session.read_ahead,
            kernel_tls: session.kernel,
            owes_close_notify: session.kernel,
            answered: None,
            body_sent: 0,
            request_id: None,
        }
    }

    /// The socket, for moving body bytes inside the kernel. Bytes already
    /// read ahead stay in the connection's buffer.
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    /// Whether the kernel holds the connection's TLS session.
    pub fn kernel_tls(&self) -> bool {
        self.kernel_tls
    }

    /// Names the client request the next responses answer.
    pub fn set_request_id(&mut self, id: RequestId) {
        self.request_id = Some(id);
    }

    pub fn request_id(&self) -> Option<RequestId> {
        self.request_id
    }

    /// The response written since the last call, if one was.
    pub fn take_answered(&mut self) -> Option<Answered> {
        let (status, head_sent) = self.answered.take()?;
        Some(Answered {
            status,
            head_sent,
            body_sent: std::mem::take(&mut self.body_sent),
        })
    }

    /// Counts body bytes sent around the connection's own writes, such as
    /// with `splice`.
    pub fn sent_body(&mut self, len: u64) {
        self.body_sent += len;
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
            let read = self.read_stream(&mut chunk).await?;
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
        let read = self.read_stream(&mut chunk).await?;
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

    /// The next piece of a chunked body, or `None` at its end. `left`
    /// carries the bytes left in the current chunk from call to call, and
    /// starts at 0.
    pub async fn read_chunked(&mut self, left: &mut u64) -> io::Result<Option<Bytes>> {
        if *left == 0 {
            let line = self.read_line().await?;
            let size = line.split(';').next().unwrap_or_default().trim();
            let size = u64::from_str_radix(size, 16).map_err(|_| invalid("bad chunk size"))?;
            if size == 0 {
                // Trailers, if any, end at an empty line.
                while !self.read_line().await?.is_empty() {}
                return Ok(None);
            }
            *left = size;
        }
        let piece = self
            .read_some(usize::try_from(*left).unwrap_or(usize::MAX))
            .await?;
        *left -= piece.len() as u64;
        if *left == 0 && !self.read_line().await?.is_empty() {
            return Err(invalid("a chunk runs past its size"));
        }
        Ok(Some(piece))
    }

    /// The next line, without its CRLF.
    async fn read_line(&mut self) -> io::Result<String> {
        loop {
            if let Some(end) = self.buffer.windows(2).position(|pair| pair == b"\r\n") {
                let line = String::from_utf8_lossy(&self.buffer[..end]).into_owned();
                self.buffer.drain(..end + 2);
                return Ok(line);
            }
            if self.buffer.len() > MAX_HEAD {
                return Err(invalid("line too long"));
            }
            let mut chunk = [0; 8 * 1024];
            let read = self.read_stream(&mut chunk).await?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
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
            let Some((consumed, status, headers)) = parse_response_head(&head)? else {
                if head.len() > MAX_HEAD {
                    return Err(invalid("response head too large"));
                }
                // Every byte peeked belongs to the head.
                self.stream.read_exact(&mut chunk[..peeked]).await?;
                continue;
            };
            self.stream
                .read_exact(&mut chunk[..consumed - start])
                .await?;
            return Ok((status, headers));
        }
    }

    /// Reads a node's answer head, and with it up to `READ_AHEAD` bytes of
    /// whatever of its body has arrived, which wait in the buffer.
    pub async fn read_answer_head(&mut self) -> io::Result<(u16, Vec<(String, String)>)> {
        debug_assert!(self.buffer.is_empty(), "an answer read ahead");
        loop {
            // Every byte of earlier reads belongs to the head, so capping
            // each read caps the body bytes read with it.
            self.buffer.reserve(READ_AHEAD);
            let mut limited = (&mut self.stream).take(READ_AHEAD as u64);
            if limited.read_buf(&mut self.buffer).await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let Some((consumed, status, headers)) = parse_response_head(&self.buffer)? else {
                if self.buffer.len() > MAX_HEAD {
                    return Err(invalid("response head too large"));
                }
                continue;
            };
            self.buffer.drain(..consumed);
            return Ok((status, headers));
        }
    }

    /// Bytes read from the stream and not yet consumed: a node's answer's
    /// first body bytes.
    pub fn buffered(&self) -> &[u8] {
        &self.buffer
    }

    /// Marks the first `len` buffered bytes consumed.
    pub fn consume_buffered(&mut self, len: usize) {
        self.buffer.drain(..len);
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
        let (status, headers) = (response.status, &response.headers);
        self.write_head_and_body(status, headers, framing, keep_alive, &response.body)
            .await
    }

    /// Writes a response's head and the start of its body, in one write
    /// when the body is small, since each write costs a system call.
    pub async fn write_head_and_body(
        &mut self,
        status: u16,
        headers: &[(impl AsRef<str>, impl AsRef<str>)],
        framing: Framing,
        keep_alive: bool,
        body: &[u8],
    ) -> io::Result<()> {
        let head = response_head(status, headers, framing, keep_alive, self.request_id);
        let mut head = head.into_bytes();
        if body.len() > COALESCED_BODY {
            self.write_all(&head).await?;
            self.answered = Some((status, Instant::now()));
            self.body_sent = 0;
            return self.write_body(body).await;
        }
        head.extend_from_slice(body);
        self.write_all(&head).await?;
        self.answered = Some((status, Instant::now()));
        self.body_sent = body.len() as u64;
        Ok(())
    }

    /// Writes a response's status line and headers; the body follows.
    pub async fn write_response_head(
        &mut self,
        status: u16,
        headers: &[(impl AsRef<str>, impl AsRef<str>)],
        framing: Framing,
        keep_alive: bool,
    ) -> io::Result<()> {
        let flags = SendFlags::empty();
        self.write_head(status, headers, framing, keep_alive, flags)
            .await
    }

    /// As `write_response_head`, for a head whose body follows at once: over
    /// plaintext, the kernel holds the head back to send it in one packet
    /// with the body's first bytes.
    pub async fn write_response_head_more(
        &mut self,
        status: u16,
        headers: &[(impl AsRef<str>, impl AsRef<str>)],
        framing: Framing,
        keep_alive: bool,
    ) -> io::Result<()> {
        let flags = match self.kernel_tls {
            true => SendFlags::empty(),
            false => SendFlags::MORE,
        };
        self.write_head(status, headers, framing, keep_alive, flags)
            .await
    }

    async fn write_head(
        &mut self,
        status: u16,
        headers: &[(impl AsRef<str>, impl AsRef<str>)],
        framing: Framing,
        keep_alive: bool,
        flags: SendFlags,
    ) -> io::Result<()> {
        let head = response_head(status, headers, framing, keep_alive, self.request_id);
        self.send_all(head.as_bytes(), flags).await?;
        self.answered = Some((status, Instant::now()));
        self.body_sent = 0;
        Ok(())
    }

    /// Writes body bytes, which count toward the response's.
    pub async fn write_body(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_all(bytes).await?;
        self.body_sent += bytes.len() as u64;
        Ok(())
    }

    /// Writes all of `bytes`, failing if the peer takes none of them for
    /// `WRITE_IDLE`.
    pub async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.send_all(bytes, SendFlags::empty()).await
    }

    /// Sends all of `bytes` with `flags`, failing if the peer takes none of
    /// them for `WRITE_IDLE`.
    async fn send_all(&self, mut bytes: &[u8], flags: SendFlags) -> io::Result<()> {
        while !bytes.is_empty() {
            let send = || Ok(rustix::net::send(&self.stream, bytes, flags)?);
            let sent = self.stream.async_io(Interest::WRITABLE, send);
            let written = tokio::time::timeout(WRITE_IDLE, sent)
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
        self.write_all(&chunk).await?;
        self.body_sent += bytes.len() as u64;
        Ok(())
    }

    /// Reads from the socket. A kernel TLS session fails the read with EIO
    /// on any record but application data: the client's `close_notify`, or
    /// a KeyUpdate. Either ends the connection.
    async fn read_stream(&mut self, chunk: &mut [u8]) -> io::Result<usize> {
        match self.stream.read(chunk).await {
            Err(error) if self.kernel_tls && error.raw_os_error() == Some(libc::EIO) => Ok(0),
            read => read,
        }
    }

    /// Stops writing, then reads and drops what the peer still sends,
    /// for up to `LINGER`. A server that answers before reading a request's
    /// body closes this way: closing with unread bytes would reset the
    /// connection, and the client could lose the answer.
    pub async fn linger(&mut self) {
        self.close_notify();
        let _ = self.stream.shutdown().await;
        let deadline = tokio::time::Instant::now() + LINGER;
        let mut sink = vec![0; READ_CHUNK];
        while let Ok(Ok(read)) =
            tokio::time::timeout_at(deadline, self.stream.read(&mut sink)).await
            && read > 0
        {}
    }

    /// Ends a kernel TLS session with a `close_notify` alert, so the client
    /// knows the connection closed cleanly rather than was cut off.
    fn close_notify(&mut self) {
        if std::mem::take(&mut self.owes_close_notify) {
            crate::tls::send_close_notify(&self.stream);
        }
    }

    /// Ends a chunked body.
    pub async fn finish_chunks(&mut self) -> io::Result<()> {
        self.write_all(b"0\r\n\r\n").await
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close_notify();
    }
}

/// Writes all of `bytes` to a shared socket, which another task may read
/// meanwhile, failing if the peer takes none of them for `WRITE_IDLE`.
pub async fn write_shared(stream: &TcpStream, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        tokio::time::timeout(WRITE_IDLE, stream.writable())
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
        match stream.try_write(bytes) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => bytes = &bytes[written..],
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// A path-style request's bucket and key, decoded.
pub fn split_path(path: &str) -> (String, String) {
    let path = path.strip_prefix('/').unwrap_or(path);
    let (bucket, key) = path.split_once('/').unwrap_or((path, ""));
    let decode = |part: &str| percent_decode_str(part).decode_utf8_lossy().into_owned();
    (decode(bucket), decode(key))
}

/// Bodies at most this long go out in the same write as their head.
const COALESCED_BODY: usize = 64 << 10;

/// A response's status line and headers, ending with the blank line. A
/// response to the client request `request_id` names it, and when S3 gave
/// the response no ID of its own, names it as S3's too.
fn response_head(
    status: u16,
    headers: &[(impl AsRef<str>, impl AsRef<str>)],
    framing: Framing,
    keep_alive: bool,
    request_id: Option<RequestId>,
) -> String {
    let named: usize = headers
        .iter()
        .map(|(name, value)| name.as_ref().len() + value.as_ref().len() + 4)
        .sum();
    let mut head = String::with_capacity(named + 192);
    let _ = write!(head, "HTTP/1.1 {status} {}\r\n", reason(status));
    for (name, value) in headers {
        head.push_str(name.as_ref());
        head.push_str(": ");
        head.push_str(value.as_ref());
        head.push_str("\r\n");
    }
    if let Some(id) = request_id {
        let _ = write!(head, "{REQUEST_ID}: {id}\r\n");
        let names_id = headers
            .iter()
            .any(|(name, _)| name.as_ref().eq_ignore_ascii_case("x-amz-request-id"));
        if !names_id {
            let _ = write!(head, "x-amz-request-id: {id}\r\n");
        }
    }
    match framing {
        Framing::Length(len) => {
            let _ = write!(head, "Content-Length: {len}\r\n");
        }
        Framing::Chunked => head.push_str("Transfer-Encoding: chunked\r\n"),
    }
    if !keep_alive {
        head.push_str("Connection: close\r\n");
    }
    head.push_str("\r\n");
    head
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

/// A response head's length, status and headers, or `None` while it is
/// incomplete.
type ParsedHead = (usize, u16, Vec<(String, String)>);

fn parse_response_head(buffer: &[u8]) -> io::Result<Option<ParsedHead>> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut response = httparse::Response::new(&mut headers);
    let consumed = match response.parse(buffer) {
        Ok(httparse::Status::Complete(consumed)) => consumed,
        Ok(httparse::Status::Partial) => return Ok(None),
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
    Ok(Some((consumed, status, headers)))
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
