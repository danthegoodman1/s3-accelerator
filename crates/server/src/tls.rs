//! TLS for S3 clients and among cluster members. rustls runs the handshake,
//! and the session then moves into the kernel (kTLS), which encrypts what
//! the server writes, sends with `sendfile` or moves with `splice`, and
//! decrypts what it reads: to the rest of the server the connection stays a
//! plain TCP socket. Where the kernel cannot take sessions, or the config
//! asks for userspace TLS, a task relays between the rustls stream and a
//! loopback socket that the server uses instead.

use crate::config::{ClusterTlsConfig, TlsConfig};
use crate::http::Connection;
use crate::log;
use crate::metrics::{Link, Metrics};
use ktls::{CompatibleCiphers, CorkStream};
use rustls::client::Resumption;
use rustls::crypto::CryptoProvider;
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// The server side: a gateway's listener for S3 clients, or a node's for
/// members.
pub struct Tls {
    acceptor: TlsAcceptor,
    /// Whether sessions move into the kernel.
    kernel: bool,
}

/// The client side, with which members reach nodes.
pub struct Connector {
    connector: TlsConnector,
    kernel: bool,
}

/// A connection once its handshake is done: the socket the server reads and
/// writes, the plaintext rustls read past the handshake, and whether the
/// kernel holds the session.
pub struct Session {
    pub stream: TcpStream,
    pub read_ahead: Vec<u8>,
    pub kernel: bool,
}

/// How long a client has to finish its handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The ciphers the kernel takes, when the config asks for kernel sessions
/// and the kernel takes a TLS 1.3 cipher rustls offers.
pub async fn kernel(wanted: bool) -> Option<CompatibleCiphers> {
    if !wanted {
        return None;
    }
    let ciphers = CompatibleCiphers::new().await.ok().filter(|ciphers| {
        let tls13 = &ciphers.tls13;
        tls13.aes_gcm_128 || tls13.aes_gcm_256 || tls13.chacha20_poly1305
    });
    if ciphers.is_none() {
        log!(
            Warn,
            "the kernel cannot take TLS sessions, so TLS runs in userspace; is the tls module loaded?"
        );
    }
    ciphers
}

/// rustls's ciphers, less those the kernel cannot take when sessions move
/// into it, so no handshake agrees on a cipher the kernel refuses.
fn provider(kernel: Option<&CompatibleCiphers>) -> Arc<CryptoProvider> {
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    if let Some(ciphers) = kernel {
        provider
            .cipher_suites
            .retain(|&suite| ciphers.is_compatible(suite));
    }
    Arc::new(provider)
}

impl Tls {
    /// Serves S3 clients with the certificate `config` names.
    pub fn clients(config: &TlsConfig, kernel: Option<&CompatibleCiphers>) -> io::Result<Tls> {
        let server = ServerConfig::builder_with_provider(provider(kernel))
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_no_client_auth()
            .with_single_cert(chain(&config.cert)?, key(&config.key)?)
            .map_err(io::Error::other)?;
        Ok(Tls::new(server, kernel))
    }

    /// Serves cluster members, each with a certificate the cluster's CA
    /// signed.
    pub fn members(
        config: &ClusterTlsConfig,
        kernel: Option<&CompatibleCiphers>,
    ) -> io::Result<Tls> {
        let provider = provider(kernel);
        let roots = Arc::new(roots(&config.ca)?);
        let verifier = WebPkiClientVerifier::builder_with_provider(roots, provider.clone())
            .build()
            .map_err(io::Error::other)?;
        let server = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain(&config.cert)?, key(&config.key)?)
            .map_err(io::Error::other)?;
        Ok(Tls::new(server, kernel))
    }

    fn new(mut server: ServerConfig, kernel: Option<&CompatibleCiphers>) -> Tls {
        // The kernel takes the session's secrets once the handshake ends,
        // and with no tickets to send, rustls has nothing left to write.
        server.enable_secret_extraction = true;
        server.send_tls13_tickets = 0;
        server.alpn_protocols = vec![b"http/1.1".to_vec()];
        Tls {
            acceptor: TlsAcceptor::from(Arc::new(server)),
            kernel: kernel.is_some(),
        }
    }

    /// Runs the handshake on a connection a client opened.
    pub async fn accept(&self, stream: TcpStream) -> io::Result<Session> {
        if self.kernel {
            let session = self.acceptor.accept(CorkStream::new(stream)).await?;
            let ktls = ktls::config_ktls_server(session)
                .await
                .map_err(io::Error::other)?;
            let (read_ahead, stream) = ktls.into_raw();
            return Ok(Session {
                stream,
                read_ahead: read_ahead.unwrap_or_default(),
                kernel: true,
            });
        }
        relayed(self.acceptor.accept(stream).await?).await
    }
}

impl Connector {
    /// Reaches nodes whose certificates the cluster's CA signed, with this
    /// process's own certificate.
    pub fn new(
        config: &ClusterTlsConfig,
        kernel: Option<&CompatibleCiphers>,
    ) -> io::Result<Connector> {
        let mut client = ClientConfig::builder_with_provider(provider(kernel))
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_root_certificates(roots(&config.ca)?)
            .with_client_auth_cert(chain(&config.cert)?, key(&config.key)?)
            .map_err(io::Error::other)?;
        client.enable_secret_extraction = true;
        client.resumption = Resumption::disabled();
        client.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Connector {
            connector: TlsConnector::from(Arc::new(client)),
            kernel: kernel.is_some(),
        })
    }

    /// Runs the handshake with the node at `address`, whose certificate
    /// must name the address's host.
    pub async fn connect(&self, stream: TcpStream, address: &str) -> io::Result<Session> {
        let host = address
            .rsplit_once(':')
            .map_or(address, |(host, _)| host)
            .trim_start_matches('[')
            .trim_end_matches(']');
        let name = ServerName::try_from(host.to_string()).map_err(io::Error::other)?;
        if self.kernel {
            let session = self
                .connector
                .connect(name, CorkStream::new(stream))
                .await?;
            let ktls = ktls::config_ktls_client(session)
                .await
                .map_err(io::Error::other)?;
            let (read_ahead, stream) = ktls.into_raw();
            return Ok(Session {
                stream,
                read_ahead: read_ahead.unwrap_or_default(),
                kernel: true,
            });
        }
        relayed(self.connector.connect(name, stream).await?).await
    }
}

/// A connection a client opened on `link`, once its handshake is done, or
/// `None` if the handshake failed.
pub async fn accept(
    tls: Option<&Tls>,
    stream: TcpStream,
    metrics: &Metrics,
    link: Link,
) -> Option<Connection> {
    let Some(tls) = tls else {
        return Some(Connection::new(stream));
    };
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.accept(stream)).await {
        Ok(Ok(session)) => {
            metrics.tls_session(link, session.kernel);
            Some(Connection::tls(session))
        }
        // A client that closes before its handshake, such as a TCP health
        // check, is no failure.
        Ok(Err(error)) if error.kind() == io::ErrorKind::UnexpectedEof => None,
        Ok(Err(error)) => {
            metrics.tls_failure(link);
            log!(Warn, "a TLS handshake failed", error = error);
            None
        }
        Err(_) => {
            metrics.tls_failure(link);
            log!(Warn, "a TLS handshake timed out");
            None
        }
    }
}

/// A userspace session, relayed to a loopback socket the server uses in
/// its place.
async fn relayed<S>(mut session: S) -> io::Result<Session>
where
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (inner, mut outer) = loopback_pair().await?;
    tokio::task::spawn_local(async move {
        let _ = tokio::io::copy_bidirectional(&mut session, &mut outer).await;
        let _ = session.shutdown().await;
    });
    Ok(Session {
        stream: inner,
        read_ahead: Vec::new(),
        kernel: false,
    })
}

fn chain(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(path)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|error| io::Error::other(format!("{path}: {error}")))
}

fn key(path: &str) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(|error| io::Error::other(format!("{path}: {error}")))
}

fn roots(path: &str) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for certificate in chain(path)? {
        roots
            .add(certificate)
            .map_err(|error| io::Error::other(format!("{path}: {error}")))?;
    }
    Ok(roots)
}

/// A connected pair of loopback sockets. Another local process could reach
/// the listener first, so it takes only the connection this one opened.
async fn loopback_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let connected = TcpStream::connect(listener.local_addr()?).await?;
    let accepting = accept_from(&listener, connected.local_addr()?);
    let accepted = tokio::time::timeout(HANDSHAKE_TIMEOUT, accepting)
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    connected.set_nodelay(true)?;
    accepted.set_nodelay(true)?;
    Ok((connected, accepted))
}

/// The next connection `listener` takes from `peer`. Any other closes.
async fn accept_from(listener: &TcpListener, peer: SocketAddr) -> io::Result<TcpStream> {
    loop {
        let (stream, from) = listener.accept().await?;
        if from == peer {
            return Ok(stream);
        }
    }
}

/// Sends a `close_notify` alert on a kernel TLS session.
pub fn send_close_notify(socket: &TcpStream) {
    let _ = close_notify(socket.as_raw_fd());
}

fn close_notify(fd: RawFd) -> io::Result<()> {
    const SOL_TLS: libc::c_int = 282;
    const TLS_SET_RECORD_TYPE: libc::c_int = 1;
    const ALERT: u8 = 21;
    // A warning-level close_notify.
    let mut alert = [1u8, 0];
    let mut iov = libc::iovec {
        iov_base: alert.as_mut_ptr().cast(),
        iov_len: alert.len(),
    };
    // SAFETY: CMSG_SPACE is a pure computation on a constant length.
    let space = unsafe { libc::CMSG_SPACE(1) } as usize;
    let mut control = vec![0u8; space];
    // SAFETY: an all-zero msghdr is valid, and the fields set below point
    // at buffers that outlive the call.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = space as _;
    // SAFETY: the control buffer holds one header with a one-byte payload,
    // as CMSG_SPACE(1) sized it.
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        (*header).cmsg_level = SOL_TLS;
        (*header).cmsg_type = TLS_SET_RECORD_TYPE;
        (*header).cmsg_len = libc::CMSG_LEN(1) as _;
        *libc::CMSG_DATA(header) = ALERT;
    }
    // SAFETY: `message` is fully initialized and its buffers are live.
    let sent = unsafe { libc::sendmsg(fd, &message, libc::MSG_DONTWAIT) };
    match sent {
        -1 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_loopback_pair_takes_only_its_own_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let _stranger = TcpStream::connect(address).await.unwrap();
        let ours = TcpStream::connect(address).await.unwrap();
        let accepted = accept_from(&listener, ours.local_addr().unwrap())
            .await
            .unwrap();
        assert_eq!(accepted.peer_addr().unwrap(), ours.local_addr().unwrap());
    }
}
