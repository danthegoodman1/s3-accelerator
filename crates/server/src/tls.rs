//! TLS for S3 clients. rustls runs the handshake, and the session then
//! moves into the kernel (kTLS), which encrypts what the server writes,
//! sends with `sendfile` or moves with `splice`, and decrypts what it
//! reads: to the rest of the server the connection stays a plain TCP
//! socket. Where the kernel cannot take sessions, or the config asks for
//! userspace TLS, a task relays between the rustls stream and a loopback
//! socket that the server uses instead.

use crate::config::TlsConfig;
use ktls::CorkStream;
use rustls::ServerConfig;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

pub struct Tls {
    acceptor: TlsAcceptor,
    /// Whether sessions move into the kernel.
    kernel: bool,
}

/// A client's connection once the handshake is done: the socket the server
/// reads and writes, the plaintext rustls read past the handshake, and
/// whether the kernel holds the session.
pub struct Accepted {
    pub stream: TcpStream,
    pub read_ahead: Vec<u8>,
    pub kernel: bool,
}

impl Tls {
    /// Loads the certificate chain and key the config names. Sessions move
    /// into the kernel if the config allows it and the kernel takes them.
    pub async fn new(config: &TlsConfig) -> io::Result<Tls> {
        let chain = CertificateDer::pem_file_iter(&config.cert)
            .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
            .map_err(|error| io::Error::other(format!("{}: {error}", config.cert)))?;
        let key = PrivateKeyDer::from_pem_file(&config.key)
            .map_err(|error| io::Error::other(format!("{}: {error}", config.key)))?;
        let mut server = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .map_err(io::Error::other)?;
        // The kernel takes the session's secrets once the handshake ends,
        // and with no tickets to send, rustls has nothing left to write.
        server.enable_secret_extraction = true;
        server.send_tls13_tickets = 0;
        server.alpn_protocols = vec![b"http/1.1".to_vec()];
        let kernel = config.kernel && kernel_takes_sessions().await;
        if config.kernel && !kernel {
            eprintln!(
                "the kernel cannot take TLS sessions (is the tls module loaded?); using userspace TLS"
            );
        }
        Ok(Tls {
            acceptor: TlsAcceptor::from(Arc::new(server)),
            kernel,
        })
    }

    pub fn kernel(&self) -> bool {
        self.kernel
    }

    /// Runs the handshake on a client's connection.
    pub async fn accept(&self, stream: TcpStream) -> io::Result<Accepted> {
        if self.kernel {
            let session = self.acceptor.accept(CorkStream::new(stream)).await?;
            let ktls = ktls::config_ktls_server(session)
                .await
                .map_err(io::Error::other)?;
            let (read_ahead, stream) = ktls.into_raw();
            return Ok(Accepted {
                stream,
                read_ahead: read_ahead.unwrap_or_default(),
                kernel: true,
            });
        }
        let mut session = self.acceptor.accept(stream).await?;
        let (inner, mut outer) = loopback_pair().await?;
        tokio::task::spawn_local(async move {
            let _ = tokio::io::copy_bidirectional(&mut session, &mut outer).await;
            let _ = session.shutdown().await;
        });
        Ok(Accepted {
            stream: inner,
            read_ahead: Vec::new(),
            kernel: false,
        })
    }
}

/// Whether the kernel takes TLS sessions: the `tls` module is loaded and
/// takes at least one of the TLS 1.3 ciphers rustls offers.
async fn kernel_takes_sessions() -> bool {
    match ktls::CompatibleCiphers::new().await {
        Ok(ciphers) => {
            let tls13 = &ciphers.tls13;
            tls13.aes_gcm_128 || tls13.aes_gcm_256 || tls13.chacha20_poly1305
        }
        Err(_) => false,
    }
}

/// A connected pair of loopback sockets.
async fn loopback_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let connecting = TcpStream::connect(listener.local_addr()?);
    let (connected, accepted) = tokio::join!(connecting, listener.accept());
    let (connected, (accepted, _)) = (connected?, accepted?);
    connected.set_nodelay(true)?;
    accepted.set_nodelay(true)?;
    Ok((connected, accepted))
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
