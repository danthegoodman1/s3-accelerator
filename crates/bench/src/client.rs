//! The load: clients that each keep one connection to the gateway and send
//! signed GETs, over plaintext or TLS. Each checks every body's length and
//! its first and last bytes against the stand-in's pattern, and times the
//! response's first byte.

use crate::origin;
use rustls::ClientConfig;
use rustls_pki_types::ServerName;
use s3_accelerator::sigv4::{self, Credentials, Signer, UNSIGNED_PAYLOAD};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

pub const ACCESS_KEY_ID: &str = "bench";
pub const SECRET_ACCESS_KEY: &str = "bench-secret";

trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

/// A GET of `path`, whole or of the bytes from `first` to `last`.
#[derive(Clone)]
pub struct Get {
    pub path: String,
    pub range: Option<(u64, u64)>,
}

/// One answered GET.
pub struct Fetched {
    pub bytes: u64,
    pub first_byte: Duration,
}

/// Where the clients connect, and the TLS they speak.
#[derive(Clone)]
pub struct Target {
    pub port: u16,
    pub tls: Option<Arc<ClientConfig>>,
}

struct Client {
    target: Target,
    signer: Signer,
    stream: Option<Pin<Box<dyn Io>>>,
    buffer: Vec<u8>,
}

impl Client {
    fn new(target: Target) -> Client {
        Client {
            target,
            signer: Signer {
                credentials: Credentials {
                    access_key_id: ACCESS_KEY_ID.into(),
                    secret_access_key: SECRET_ACCESS_KEY.into(),
                },
                region: "us-east-1".into(),
                service: "s3",
            },
            stream: None,
            buffer: vec![0; 4 << 20],
        }
    }

    async fn get(&mut self, get: &Get) -> io::Result<Fetched> {
        let host = format!("127.0.0.1:{}", self.target.port);
        let mut headers = vec![("host".to_string(), host)];
        if let Some((first, last)) = get.range {
            headers.push(("range".to_string(), format!("bytes={first}-{last}")));
        }
        self.signer.sign(
            "GET",
            &get.path,
            "",
            &mut headers,
            UNSIGNED_PAYLOAD,
            sigv4::unix_now(),
        );
        let mut request = format!("GET {} HTTP/1.1\r\n", get.path);
        for (name, value) in &headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");
        let mut stream = match self.stream.take() {
            Some(stream) => stream,
            None => connect(self.target.clone()).await?,
        };
        let sent = Instant::now();
        stream.write_all(request.as_bytes()).await?;
        let (status, len, first_byte, mut held) =
            read_head(&mut stream, &mut self.buffer, sent).await?;
        let (start, size) = origin::object(&get.path).expect("a benchmark key");
        let (first, last) = get.range.unwrap_or((0, size - 1));
        let expected = last - first + 1;
        if status != 200 && status != 206 || len != expected {
            return Err(io::Error::other(format!(
                "{}: status {status}, {len} of {expected} bytes",
                get.path
            )));
        }
        // The body's first and last bytes must match the object's.
        let mut offset = 0;
        let check = |bytes: &[u8], offset: u64| {
            let head = bytes.iter().take(16).enumerate();
            let tail_from = bytes.len().saturating_sub(16);
            let tail = bytes.iter().enumerate().skip(tail_from);
            head.chain(tail)
                .all(|(index, &byte)| byte == origin::byte(start, first + offset + index as u64))
        };
        let mut correct = true;
        while offset < len {
            if held == 0 {
                let want = ((len - offset) as usize).min(self.buffer.len());
                held = stream.read(&mut self.buffer[..want]).await?;
                if held == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
            let piece = &self.buffer[..held];
            if offset == 0 || offset + held as u64 == len {
                correct &= check(piece, offset);
            }
            offset += held as u64;
            held = 0;
        }
        if !correct {
            return Err(io::Error::other(format!("{}: wrong bytes", get.path)));
        }
        self.stream = Some(stream);
        Ok(Fetched {
            bytes: len,
            first_byte,
        })
    }
}

async fn connect(target: Target) -> io::Result<Pin<Box<dyn Io>>> {
    let stream = TcpStream::connect(("127.0.0.1", target.port)).await?;
    stream.set_nodelay(true)?;
    Ok(match target.tls {
        Some(config) => {
            let name = ServerName::try_from("127.0.0.1").unwrap();
            let session = TlsConnector::from(config).connect(name, stream).await?;
            Box::pin(session)
        }
        None => Box::pin(stream),
    })
}

/// Reads a response's head into `buffer`: the status, the body's length,
/// when its first byte came, and the body bytes read with it, moved to the
/// buffer's start.
async fn read_head(
    stream: &mut Pin<Box<dyn Io>>,
    buffer: &mut [u8],
    sent: Instant,
) -> io::Result<(u16, u64, Duration, usize)> {
    let mut filled = 0;
    let mut first_byte = None;
    loop {
        let read = stream.read(&mut buffer[filled..]).await?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let first_byte = *first_byte.get_or_insert_with(|| sent.elapsed());
        filled += read;
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut response = httparse::Response::new(&mut headers);
        if let httparse::Status::Complete(consumed) = response
            .parse(&buffer[..filled])
            .map_err(io::Error::other)?
        {
            let status = response.code.unwrap_or_default();
            let len = response
                .headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case("content-length"))
                .and_then(|header| std::str::from_utf8(header.value).ok()?.parse().ok())
                .unwrap_or(0);
            buffer.copy_within(consumed..filled, 0);
            return Ok((status, len, first_byte, filled - consumed));
        }
        if filled == buffer.len() {
            return Err(io::Error::other("response head too large"));
        }
    }
}

/// What a run of GETs took.
pub struct Outcome {
    pub requests: u64,
    pub bytes: u64,
    pub elapsed: Duration,
    pub first_bytes: Vec<Duration>,
}

impl Outcome {
    /// The time to first byte below which `share` of the requests fall.
    pub fn first_byte(&self, share: f64) -> Duration {
        let mut sorted = self.first_bytes.clone();
        sorted.sort();
        let index = ((sorted.len() as f64 * share) as usize).min(sorted.len().saturating_sub(1));
        sorted.get(index).copied().unwrap_or_default()
    }
}

/// Sends `gets` in order from `clients` clients, each taking the next GET
/// once its last is answered.
pub async fn run(target: &Target, clients: usize, gets: Vec<Get>) -> io::Result<Outcome> {
    let gets = Arc::new(gets);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let tasks: Vec<_> = (0..clients)
        .map(|_| {
            let (gets, next) = (gets.clone(), next.clone());
            let mut client = Client::new(target.clone());
            tokio::spawn(async move {
                let mut fetched = Vec::new();
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(get) = gets.get(index) else {
                        return Ok::<_, io::Error>(fetched);
                    };
                    fetched.push(client.get(get).await?);
                }
            })
        })
        .collect();
    let mut outcome = Outcome {
        requests: 0,
        bytes: 0,
        elapsed: Duration::ZERO,
        first_bytes: Vec::new(),
    };
    for task in tasks {
        for fetched in task.await.map_err(io::Error::other)?? {
            outcome.requests += 1;
            outcome.bytes += fetched.bytes;
            outcome.first_bytes.push(fetched.first_byte);
        }
    }
    outcome.elapsed = started.elapsed();
    Ok(outcome)
}
