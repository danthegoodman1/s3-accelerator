//! An S3 client over HTTP/1.1 with one keep-alive connection, plaintext or
//! TLS, that signs each request with SigV4 and times each response's first
//! byte and last.

use rustls::ClientConfig;
use rustls_pki_types::ServerName;
use s3_accelerator::sigv4::{self, Credentials, Signer, UNSIGNED_PAYLOAD};
use serde::{Deserialize, Serialize};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Bytes a client reads or writes at a time.
const CHUNK: usize = 1 << 20;

/// Where requests go: `http://host:port` or `https://host[:port]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub tls: bool,
    /// The host TLS checks the certificate for.
    pub name: String,
    /// The `Host` header, as the URL wrote it.
    pub host: String,
    /// Where to connect.
    pub address: String,
}

impl Endpoint {
    pub fn parse(url: &str) -> Result<Endpoint, String> {
        let (tls, authority) = match (url.strip_prefix("https://"), url.strip_prefix("http://")) {
            (Some(rest), _) => (true, rest),
            (_, Some(rest)) => (false, rest),
            _ => return Err(format!("{url} is no http:// or https:// URL")),
        };
        let authority = authority.trim_end_matches('/');
        if authority.is_empty() || authority.contains('/') {
            return Err(format!(
                "{url} names a path; an endpoint is a scheme and a host"
            ));
        }
        // An IPv6 address comes in brackets, with its port after them.
        let (name, port) = match authority.strip_prefix('[') {
            Some(rest) => {
                let (name, port) = rest
                    .split_once(']')
                    .ok_or(format!("{url} has an open bracket"))?;
                (name, port.strip_prefix(':'))
            }
            None => match authority.rsplit_once(':') {
                Some((name, port)) => (name, Some(port)),
                None => (authority, None),
            },
        };
        let port: u16 = match port {
            Some(port) => port.parse().map_err(|_| format!("{url} has a bad port"))?,
            None if tls => 443,
            None => 80,
        };
        let address = match name.contains(':') {
            true => format!("[{name}]:{port}"),
            false => format!("{name}:{port}"),
        };
        Ok(Endpoint {
            tls,
            name: name.to_string(),
            host: authority.to_string(),
            address,
        })
    }
}

/// Why a request got no answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Failure {
    /// The connection or its TLS handshake failed.
    Connect,
    /// The request ran past its timeout.
    Timeout,
    /// The connection failed, or the body ended early.
    Broken,
    /// The answer wasn't HTTP this client reads, or had the wrong length.
    Protocol,
    /// The body's bytes differed from the object's.
    Corrupt,
}

/// An answered request.
#[derive(Clone, Copy, Debug)]
pub struct Answer {
    pub status: u16,
    /// From sending the request to the response's first byte, and to its
    /// last.
    pub first_byte: Duration,
    pub total: Duration,
    pub bytes: u64,
}

/// What a GET asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Range {
    Whole,
    /// Bytes `first` to `last`, inclusive.
    Span(u64, u64),
    /// The last this many bytes.
    Suffix(u64),
}

/// Takes a response's status and each piece of its body, with its offset.
type Sink<'a> = &'a mut (dyn FnMut(u16, u64, &[u8]) + Send);
/// Writes an upload's bytes from an offset into a buffer.
type Fill<'a> = &'a mut (dyn FnMut(u64, &mut [u8]) + Send);

trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

pub struct Client {
    endpoint: Arc<Endpoint>,
    tls: Option<Arc<ClientConfig>>,
    signer: Arc<Signer>,
    timeout: Duration,
    stream: Option<Pin<Box<dyn Io>>>,
    buffer: Vec<u8>,
}

pub fn signer(access_key_id: &str, secret_access_key: &str, region: &str) -> Signer {
    Signer {
        credentials: Credentials {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        },
        region: region.into(),
        service: "s3",
    }
}

impl Client {
    pub fn new(
        endpoint: Arc<Endpoint>,
        tls: Option<Arc<ClientConfig>>,
        signer: Arc<Signer>,
        timeout: Duration,
    ) -> Client {
        Client {
            endpoint,
            tls,
            signer,
            timeout,
            stream: None,
            buffer: vec![0; CHUNK],
        }
    }

    /// GETs `path`, handing `body` the status and each piece of the body
    /// with its offset from the body's start. A failure closes the
    /// connection.
    pub async fn get(
        &mut self,
        path: &str,
        range: Range,
        body: Sink<'_>,
    ) -> Result<Answer, Failure> {
        let mut headers = Vec::new();
        match range {
            Range::Whole => {}
            Range::Span(first, last) => {
                headers.push(("range".into(), format!("bytes={first}-{last}")))
            }
            Range::Suffix(len) => headers.push(("range".into(), format!("bytes=-{len}"))),
        }
        let (sent, timeout) = (Instant::now(), self.timeout);
        let exchange = self.exchange("GET", path, headers, None, body, sent);
        let exchanged = tokio::time::timeout(timeout, exchange).await;
        self.finish(exchanged, sent)
    }

    /// HEADs `path`: the answer's `bytes` is the object's length.
    pub async fn head(&mut self, path: &str) -> Result<Answer, Failure> {
        let (sent, timeout) = (Instant::now(), self.timeout);
        let mut ignore = |_: u16, _: u64, _: &[u8]| {};
        let exchange = self.exchange("HEAD", path, Vec::new(), None, &mut ignore, sent);
        let exchanged = tokio::time::timeout(timeout, exchange).await;
        self.finish(exchanged, sent)
    }

    /// PUTs `len` bytes to `path`, which `fill` writes from each offset.
    pub async fn put(&mut self, path: &str, len: u64, fill: Fill<'_>) -> Result<Answer, Failure> {
        let headers = vec![
            ("content-length".into(), len.to_string()),
            ("content-type".into(), "application/octet-stream".into()),
        ];
        let (sent, timeout) = (Instant::now(), self.timeout);
        let mut ignore = |_: u16, _: u64, _: &[u8]| {};
        let exchange = self.exchange("PUT", path, headers, Some((len, fill)), &mut ignore, sent);
        let exchanged = tokio::time::timeout(timeout, exchange).await;
        self.finish(exchanged, sent)
    }

    fn finish(
        &mut self,
        exchanged: Result<Result<(u16, Duration, u64), Failure>, tokio::time::error::Elapsed>,
        sent: Instant,
    ) -> Result<Answer, Failure> {
        match exchanged {
            Ok(Ok((status, first_byte, bytes))) => Ok(Answer {
                status,
                first_byte,
                total: sent.elapsed(),
                bytes,
            }),
            Ok(Err(failure)) => {
                self.stream = None;
                Err(failure)
            }
            Err(_) => {
                self.stream = None;
                Err(Failure::Timeout)
            }
        }
    }

    /// Sends one request and reads its answer: the status, when its first
    /// byte came, and the body's length.
    async fn exchange(
        &mut self,
        method: &str,
        path: &str,
        mut headers: Vec<(String, String)>,
        upload: Option<(u64, Fill<'_>)>,
        body: Sink<'_>,
        sent: Instant,
    ) -> Result<(u16, Duration, u64), Failure> {
        headers.push(("host".into(), self.endpoint.host.clone()));
        self.signer.sign(
            method,
            path,
            "",
            &mut headers,
            UNSIGNED_PAYLOAD,
            sigv4::unix_now(),
        );
        let mut head = format!("{method} {path} HTTP/1.1\r\n");
        for (name, value) in &headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        let mut upload = upload;
        let reused = self.stream.is_some();
        let mut stream = match self.stream.take() {
            Some(stream) => stream,
            None => connect(self.endpoint.clone(), self.tls.clone()).await?,
        };
        let mut answered = send(
            &mut stream,
            head.as_bytes(),
            &mut upload,
            &mut self.buffer,
            sent,
        )
        .await;
        // A connection kept open may have closed while idle: once, try a
        // new one.
        if reused && matches!(answered, Err(Stop::Unanswered)) {
            stream = connect(self.endpoint.clone(), self.tls.clone()).await?;
            answered = send(
                &mut stream,
                head.as_bytes(),
                &mut upload,
                &mut self.buffer,
                sent,
            )
            .await;
        }
        let (status, length, keep_alive, first_byte, mut held) =
            answered.map_err(|stop| match stop {
                Stop::Unanswered => Failure::Broken,
                Stop::Failed(failure) => failure,
            })?;
        let len = match length {
            // A HEAD's length is the GET's, and no body follows.
            Length::Fixed(len) if method == "HEAD" => len,
            Length::Chunked if method == "HEAD" => 0,
            Length::Fixed(len) => {
                let mut offset = 0;
                while offset < len {
                    if held == 0 {
                        let want = ((len - offset) as usize).min(self.buffer.len());
                        held = stream
                            .read(&mut self.buffer[..want])
                            .await
                            .map_err(|_| Failure::Broken)?;
                        if held == 0 {
                            return Err(Failure::Broken);
                        }
                    }
                    let take = held.min((len - offset) as usize);
                    body(status, offset, &self.buffer[..take]);
                    offset += take as u64;
                    held = 0;
                }
                len
            }
            Length::Chunked => chunked(&mut stream, &mut self.buffer, held, status, body).await?,
        };
        if keep_alive {
            self.stream = Some(stream);
        }
        Ok((status, first_byte, len))
    }
}

/// How a response's body ends.
#[derive(Clone, Copy, Debug)]
enum Length {
    /// After this many bytes.
    Fixed(u64),
    /// At a chunk of length zero, as S3 sends its errors.
    Chunked,
}

/// Why a request got no response head.
enum Stop {
    /// The connection failed before the response's first byte.
    Unanswered,
    Failed(Failure),
}

/// Sends a request's head and body, and reads the response's head.
async fn send(
    stream: &mut Pin<Box<dyn Io>>,
    head: &[u8],
    upload: &mut Option<(u64, Fill<'_>)>,
    buffer: &mut [u8],
    sent: Instant,
) -> Result<(u16, Length, bool, Duration, usize), Stop> {
    stream.write_all(head).await.map_err(|_| Stop::Unanswered)?;
    if let Some((len, fill)) = upload {
        let mut offset = 0;
        while offset < *len {
            let piece = ((*len - offset) as usize).min(buffer.len());
            fill(offset, &mut buffer[..piece]);
            stream
                .write_all(&buffer[..piece])
                .await
                .map_err(|_| Stop::Unanswered)?;
            offset += piece as u64;
        }
    }
    stream.flush().await.map_err(|_| Stop::Unanswered)?;
    read_head(stream, buffer, sent).await
}

async fn connect(
    endpoint: Arc<Endpoint>,
    tls: Option<Arc<ClientConfig>>,
) -> Result<Pin<Box<dyn Io>>, Failure> {
    let stream = TcpStream::connect(&endpoint.address)
        .await
        .map_err(|_| Failure::Connect)?;
    stream.set_nodelay(true).map_err(|_| Failure::Connect)?;
    Ok(match tls {
        Some(config) => {
            let name = ServerName::try_from(endpoint.name.clone()).map_err(|_| Failure::Connect)?;
            let session = TlsConnector::from(config)
                .connect(name, stream)
                .await
                .map_err(|_| Failure::Connect)?;
            Box::pin(session)
        }
        None => Box::pin(stream),
    })
}

/// Reads a response's head into `buffer`: its status, how its body ends,
/// whether the connection stays open, when its first byte came, and how
/// many body bytes came with it, moved to the buffer's start.
async fn read_head(
    stream: &mut Pin<Box<dyn Io>>,
    buffer: &mut [u8],
    sent: Instant,
) -> Result<(u16, Length, bool, Duration, usize), Stop> {
    let mut filled = 0;
    let mut first_byte = None;
    loop {
        let read = stream.read(&mut buffer[filled..]).await;
        let read = match (read, filled) {
            (Ok(0) | Err(_), 0) => return Err(Stop::Unanswered),
            (Ok(0) | Err(_), _) => return Err(Stop::Failed(Failure::Broken)),
            (Ok(read), _) => read,
        };
        let first_byte = *first_byte.get_or_insert_with(|| sent.elapsed());
        filled += read;
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut response = httparse::Response::new(&mut headers);
        let parsed = response
            .parse(&buffer[..filled])
            .map_err(|_| Stop::Failed(Failure::Protocol))?;
        if let httparse::Status::Complete(consumed) = parsed {
            let status = response.code.unwrap_or_default();
            let value = |name: &str| {
                response
                    .headers
                    .iter()
                    .find(|header| header.name.eq_ignore_ascii_case(name))
                    .and_then(|header| std::str::from_utf8(header.value).ok())
            };
            let len = match (value("transfer-encoding"), value("content-length")) {
                (Some(coding), _) if coding.trim().eq_ignore_ascii_case("chunked") => {
                    Length::Chunked
                }
                (Some(_), _) => return Err(Stop::Failed(Failure::Protocol)),
                (None, Some(len)) => Length::Fixed(
                    len.trim()
                        .parse()
                        .map_err(|_| Stop::Failed(Failure::Protocol))?,
                ),
                (None, None) => Length::Fixed(0),
            };
            let keep_alive =
                !value("connection").is_some_and(|value| value.eq_ignore_ascii_case("close"));
            buffer.copy_within(consumed..filled, 0);
            return Ok((status, len, keep_alive, first_byte, filled - consumed));
        }
        if filled == buffer.len() {
            return Err(Stop::Failed(Failure::Protocol));
        }
    }
}

/// Reads a chunked body, handing `body` each piece with its offset, and
/// returns its length. Its first `held` bytes sit at the buffer's start.
async fn chunked(
    stream: &mut Pin<Box<dyn Io>>,
    buffer: &mut [u8],
    held: usize,
    status: u16,
    body: Sink<'_>,
) -> Result<u64, Failure> {
    let mut reader = Buffered {
        stream,
        buffer,
        start: 0,
        end: held,
    };
    let mut len = 0;
    loop {
        let line = reader.line().await?;
        let size = line.split(';').next().unwrap_or_default().trim();
        let mut left = u64::from_str_radix(size, 16).map_err(|_| Failure::Protocol)?;
        if left == 0 {
            // Trailers, if any, end at an empty line.
            while !reader.line().await?.is_empty() {}
            return Ok(len);
        }
        while left > 0 {
            let piece = reader.take(left).await?;
            body(status, len, piece);
            len += piece.len() as u64;
            left -= piece.len() as u64;
        }
        if !reader.line().await?.is_empty() {
            return Err(Failure::Protocol);
        }
    }
}

/// A stream and the bytes read from it but not yet taken:
/// `buffer[start..end]`.
struct Buffered<'a> {
    stream: &'a mut Pin<Box<dyn Io>>,
    buffer: &'a mut [u8],
    start: usize,
    end: usize,
}

impl Buffered<'_> {
    /// Moves the untaken bytes to the buffer's start and reads more after
    /// them.
    async fn fill(&mut self) -> Result<(), Failure> {
        self.buffer.copy_within(self.start..self.end, 0);
        self.end -= self.start;
        self.start = 0;
        if self.end == self.buffer.len() {
            return Err(Failure::Protocol);
        }
        match self.stream.read(&mut self.buffer[self.end..]).await {
            Ok(0) | Err(_) => Err(Failure::Broken),
            Ok(read) => {
                self.end += read;
                Ok(())
            }
        }
    }

    /// The next line, without its CRLF.
    async fn line(&mut self) -> Result<String, Failure> {
        loop {
            let held = &self.buffer[self.start..self.end];
            if let Some(at) = held.windows(2).position(|pair| pair == b"\r\n") {
                let line = String::from_utf8(held[..at].to_vec()).map_err(|_| Failure::Protocol)?;
                self.start += at + 2;
                return Ok(line);
            }
            self.fill().await?;
        }
    }

    /// Up to `most` bytes, reading more only when none are held.
    async fn take(&mut self, most: u64) -> Result<&[u8], Failure> {
        if self.start == self.end {
            self.fill().await?;
        }
        let from = self.start;
        self.start += (self.end - from).min(most as usize);
        Ok(&self.buffer[from..self.start])
    }
}

/// TLS that trusts the system's certificate authorities, or only the
/// certificates in `ca` when given, as for a gateway's own CA.
pub fn tls_config(ca: Option<&str>) -> io::Result<Arc<ClientConfig>> {
    use rustls_pki_types::CertificateDer;
    use rustls_pki_types::pem::PemObject;
    let config = match ca {
        Some(path) => {
            let mut roots = rustls::RootCertStore::empty();
            for certificate in CertificateDer::pem_file_iter(path).map_err(io::Error::other)? {
                roots
                    .add(certificate.map_err(io::Error::other)?)
                    .map_err(io::Error::other)?;
            }
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
        None => {
            use rustls_platform_verifier::ConfigVerifierExt;
            ClientConfig::with_platform_verifier().map_err(io::Error::other)?
        }
    };
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads one request's head and body from `stream`: its request line,
    /// and its body.
    async fn request(stream: &mut TcpStream) -> Option<(String, Vec<u8>)> {
        let mut head = Vec::new();
        let mut byte = [0; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if stream.read(&mut byte).await.ok()? == 0 {
                return None;
            }
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap();
        let len: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .map_or(0, |len| len.parse().unwrap());
        let mut body = vec![0; len];
        stream.read_exact(&mut body).await.ok()?;
        Some((head.lines().next().unwrap().to_string(), body))
    }

    /// A server that answers each connection's first request, then closes
    /// it as if it sat idle too long: the client's next request on it gets
    /// no answer, and goes again on a new connection.
    #[tokio::test]
    async fn a_connection_closed_while_idle_is_replaced() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let mut seen = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (line, body) = request(&mut stream).await.unwrap();
                let answer: &[u8] = match line.starts_with("PUT") {
                    true => b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n",
                    false => b"HTTP/1.1 206 Partial Content\r\ncontent-length: 5\r\n\r\nhello",
                };
                stream.write_all(answer).await.unwrap();
                seen.push((line, body));
            }
            seen
        });
        let endpoint = Arc::new(Endpoint::parse(&format!("http://127.0.0.1:{port}")).unwrap());
        let signer = Arc::new(signer("key", "secret", "us-east-1"));
        let mut client = Client::new(endpoint, None, signer, Duration::from_secs(5));
        for _ in 0..2 {
            let mut body = Vec::new();
            let mut sink = |status: u16, offset: u64, bytes: &[u8]| {
                assert_eq!((status, offset as usize), (206, body.len()));
                body.extend_from_slice(bytes);
            };
            let answer = client
                .get("/b/k", Range::Span(0, 4), &mut sink)
                .await
                .unwrap();
            assert_eq!((answer.status, answer.bytes), (206, 5));
            assert_eq!(body, b"hello");
        }
        let mut fill = |offset: u64, buffer: &mut [u8]| buffer.fill(offset as u8 + 7);
        let answer = client.put("/b/new", 3, &mut fill).await.unwrap();
        assert_eq!(answer.status, 200);
        let seen = server.await.unwrap();
        assert_eq!(seen[0].0, "GET /b/k HTTP/1.1");
        assert_eq!(seen[2], ("PUT /b/new HTTP/1.1".to_string(), vec![7, 7, 7]));
    }

    /// S3 sends its errors chunked: the client reads the whole body, and
    /// the connection serves the next request.
    #[tokio::test]
    async fn chunked_bodies_are_read_whole() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let answers: [&[u8]; 2] = [
                b"HTTP/1.1 503 Slow Down\r\ntransfer-encoding: chunked\r\n\r\n\
                  5\r\n<Err>\r\n6;name=value\r\nSlowDo\r\n0\r\nx-trailer: 1\r\n\r\n",
                b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n",
            ];
            for answer in answers {
                request(&mut stream).await.unwrap();
                stream.write_all(answer).await.unwrap();
            }
        });
        let endpoint = Arc::new(Endpoint::parse(&format!("http://127.0.0.1:{port}")).unwrap());
        let signer = Arc::new(signer("key", "secret", "us-east-1"));
        let mut client = Client::new(endpoint, None, signer, Duration::from_secs(5));
        let mut body = Vec::new();
        let mut sink = |status: u16, offset: u64, bytes: &[u8]| {
            assert_eq!((status, offset as usize), (503, body.len()));
            body.extend_from_slice(bytes);
        };
        let answer = client.get("/b/k", Range::Whole, &mut sink).await.unwrap();
        assert_eq!((answer.status, answer.bytes), (503, 11));
        assert_eq!(body, b"<Err>SlowDo");
        // The server accepts one connection, so this PUT reuses it.
        let mut fill = |_: u64, buffer: &mut [u8]| buffer.fill(1);
        let answer = client.put("/b/k", 3, &mut fill).await.unwrap();
        assert_eq!(answer.status, 200);
        server.await.unwrap();
    }

    #[test]
    fn endpoints_name_their_host_and_port() {
        let gateway = Endpoint::parse("http://10.0.1.5:9000").unwrap();
        assert_eq!(
            (gateway.tls, gateway.host.as_str()),
            (false, "10.0.1.5:9000")
        );
        assert_eq!(gateway.address, "10.0.1.5:9000");
        let s3 = Endpoint::parse("https://s3.us-east-1.amazonaws.com/").unwrap();
        assert_eq!(
            (s3.tls, s3.name.as_str()),
            (true, "s3.us-east-1.amazonaws.com")
        );
        assert_eq!(s3.address, "s3.us-east-1.amazonaws.com:443");
        let v6 = Endpoint::parse("https://[2600:1f18::5]").unwrap();
        assert_eq!(
            (v6.name.as_str(), v6.address.as_str()),
            ("2600:1f18::5", "[2600:1f18::5]:443")
        );
        let v6 = Endpoint::parse("http://[::1]:9000").unwrap();
        assert_eq!((v6.name.as_str(), v6.host.as_str()), ("::1", "[::1]:9000"));
        assert!(Endpoint::parse("s3.amazonaws.com").is_err());
        assert!(Endpoint::parse("http://host/bucket").is_err());
    }
}
