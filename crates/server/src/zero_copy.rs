//! Moving body bytes inside the kernel: `sendfile` from the slab file to a
//! socket, and `splice` from one socket to another through a pipe. The
//! bytes never enter this process's memory.
//!
//! Both pass references to pages rather than copies, so a page stays in use
//! until every socket and pipe holding it lets go: after the peer
//! acknowledges it, or, over loopback, after the reader reads it. A slot's
//! pages are overwritten only once they are free (see [`PageCache`]).

use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fs::{Advice, fadvise};
use rustix::io::Errno;
use rustix::mm::{MapFlags, ProtFlags, mmap, munmap};
use rustix::pipe::{
    PipeFlags, SpliceFlags, fcntl_getpipe_size, fcntl_setpipe_size, pipe_with, splice,
};
use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::num::NonZeroU64;
use std::ops::Range;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::Interest;
use tokio::net::TcpStream;

/// How long a transfer waits for a socket to take or give more bytes.
pub const IDLE: Duration = Duration::from_secs(60);

/// Sends `len` bytes of `file` from `offset` to `socket`, a non-blocking
/// socket, waiting whenever it is full. Blocks while the kernel reads pages
/// the page cache lacks, so it runs on a worker thread.
pub fn send_file(socket: &OwnedFd, file: &File, offset: u64, len: u64) -> io::Result<()> {
    let (mut offset, end) = (offset, offset + len);
    while offset < end {
        let count = usize::try_from(end - offset).unwrap_or(usize::MAX);
        match rustix::fs::sendfile(socket, file, Some(&mut offset), count) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(_) | Err(Errno::INTR) => {}
            Err(Errno::AGAIN) => wait(socket, PollFlags::OUT)?,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Waits until `socket` is ready for `flags`, for up to `IDLE`.
fn wait(socket: &OwnedFd, flags: PollFlags) -> io::Result<()> {
    let mut fds = [PollFd::new(socket, flags)];
    let timeout = Timespec {
        tv_sec: IDLE.as_secs() as i64,
        tv_nsec: 0,
    };
    match poll(&mut fds, Some(&timeout)) {
        Ok(0) => Err(io::ErrorKind::TimedOut.into()),
        // An error or hangup shows in the next call on the socket.
        Ok(_) | Err(Errno::INTR) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Why a relay stopped short of its length.
#[derive(Debug)]
pub enum Short {
    /// The source ended or failed.
    Source(io::Error),
    /// The destination failed.
    Destination(io::Error),
}

/// Moves `len` bytes from `from` to `to` through a pipe. Returns the bytes
/// that reached `to`, and why the relay stopped short if it did.
///
/// Sockets never block `splice`, so the relay runs on this thread's event
/// loop, unless `kernel_tls` says either socket carries a kernel TLS
/// session: then each `splice` encrypts or decrypts, and the relay runs on
/// the relay runtime's threads so the crypto leaves the event loop free.
pub async fn relay(
    from: &TcpStream,
    to: &TcpStream,
    len: u64,
    kernel_tls: bool,
) -> (u64, Result<(), Short>) {
    if !kernel_tls {
        return relay_here(from, to, len).await;
    }
    let from = match from.as_fd().try_clone_to_owned() {
        Ok(from) => std::net::TcpStream::from(from),
        Err(error) => return (0, Err(Short::Source(error))),
    };
    let to = match to.as_fd().try_clone_to_owned() {
        Ok(to) => std::net::TcpStream::from(to),
        Err(error) => return (0, Err(Short::Destination(error))),
    };
    let relayed = relays().spawn(async move {
        let from = match TcpStream::from_std(from) {
            Ok(from) => from,
            Err(error) => return (0, Err(Short::Source(error))),
        };
        let to = match TcpStream::from_std(to) {
            Ok(to) => to,
            Err(error) => return (0, Err(Short::Destination(error))),
        };
        relay_here(&from, &to, len).await
    });
    relayed
        .await
        .unwrap_or_else(|error| (0, Err(Short::Source(io::Error::other(error)))))
}

/// The runtime that relays over kernel TLS, one thread per core: its
/// threads take the crypto off the event loop, and a slow client holds no
/// thread while it waits.
fn relays() -> &'static tokio::runtime::Runtime {
    static RELAYS: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RELAYS.get_or_init(|| {
        let threads = std::thread::available_parallelism().map_or(1, |threads| threads.get());
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("relay")
            .enable_all()
            .build()
            .expect("the relay runtime starts")
    })
}

/// A relay on this thread's event loop.
async fn relay_here(from: &TcpStream, to: &TcpStream, len: u64) -> (u64, Result<(), Short>) {
    let pipe = match Pipe::take() {
        Ok(pipe) => pipe,
        Err(error) => return (0, Err(Short::Source(error))),
    };
    let flags = SpliceFlags::MOVE | SpliceFlags::NONBLOCK;
    let mut copied = 0;
    while copied < len {
        // The pipe is empty here, so `splice` would block only on the
        // socket, which is what the readiness wait expects.
        let want = usize::try_from(len - copied)
            .unwrap_or(usize::MAX)
            .min(pipe.capacity);
        let fill = || Ok(splice(from, None, &pipe.write, None, want, flags)?);
        let filled = match idle(from.async_io(Interest::READABLE, fill)).await {
            Ok(0) => {
                pipe.put_back();
                return (
                    copied,
                    Err(Short::Source(io::ErrorKind::UnexpectedEof.into())),
                );
            }
            Ok(filled) => filled,
            Err(error) => {
                pipe.put_back();
                return (copied, Err(Short::Source(error)));
            }
        };
        let mut drained = 0;
        while drained < filled {
            let drain = || Ok(splice(&pipe.read, None, to, None, filled - drained, flags)?);
            match idle(to.async_io(Interest::WRITABLE, drain)).await {
                Ok(0) => {
                    let error = io::ErrorKind::WriteZero.into();
                    return (copied + drained as u64, Err(Short::Destination(error)));
                }
                Ok(moved) => drained += moved,
                Err(error) => return (copied + drained as u64, Err(Short::Destination(error))),
            }
        }
        copied += filled as u64;
    }
    pipe.put_back();
    (copied, Ok(()))
}

async fn idle<T>(operation: impl Future<Output = io::Result<T>>) -> io::Result<T> {
    tokio::time::timeout(IDLE, operation)
        .await
        .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()))
}

/// A pipe for relays, reused while empty.
struct Pipe {
    read: OwnedFd,
    write: OwnedFd,
    capacity: usize,
}

/// Empty pipes each thread keeps for its next relays.
const POOLED_PIPES: usize = 64;
/// The capacity a pipe asks for. The kernel may give less: past the
/// user's `pipe-user-pages-soft`, it gives two pages.
const PIPE_SIZE: usize = 256 << 10;

thread_local! {
    static PIPES: RefCell<Vec<Pipe>> = const { RefCell::new(Vec::new()) };
}

impl Pipe {
    fn take() -> io::Result<Pipe> {
        if let Some(pipe) = PIPES.with_borrow_mut(Vec::pop) {
            return Ok(pipe);
        }
        let (read, write) = pipe_with(PipeFlags::CLOEXEC | PipeFlags::NONBLOCK)?;
        let _ = fcntl_setpipe_size(&read, PIPE_SIZE);
        let capacity = fcntl_getpipe_size(&read)?;
        Ok(Pipe {
            read,
            write,
            capacity,
        })
    }

    /// Keeps an empty pipe for reuse.
    fn put_back(self) {
        PIPES.with_borrow_mut(|pipes| {
            if pipes.len() < POOLED_PIPES {
                pipes.push(self);
            }
        });
    }
}

/// Which of a file's pages the page cache still holds. The kernel drops a
/// clean page on request unless something else references it, so a page
/// that stays after the request is still in a socket or pipe.
pub struct PageCache {
    address: *mut std::ffi::c_void,
    len: usize,
}

// SAFETY: the mapping is only passed to `mincore`, which reads no memory
// through it, and `munmap` on drop.
unsafe impl Send for PageCache {}
unsafe impl Sync for PageCache {}

impl PageCache {
    /// Maps `len` bytes of `file` so `mincore` can report on them. Pages
    /// are never touched through the mapping.
    pub fn new(file: &File, len: u64) -> io::Result<PageCache> {
        let len = usize::try_from(len).map_err(io::Error::other)?;
        // SAFETY: a fresh read-only shared mapping, which aliases nothing
        // this process uses.
        let address = unsafe {
            mmap(
                std::ptr::null_mut(),
                len,
                ProtFlags::READ,
                MapFlags::SHARED,
                file,
                0,
            )?
        };
        Ok(PageCache { address, len })
    }

    /// Drops the pages of `span` that nothing references, then reports
    /// whether any page of `range`, which lies within it, is still cached.
    pub fn in_use(&self, file: &File, span: Range<u64>, range: Range<u64>) -> io::Result<bool> {
        let Some(span_len) = NonZeroU64::new(span.end - span.start) else {
            return Ok(false);
        };
        fadvise(file, span.start, Some(span_len), Advice::DontNeed)?;
        let page = rustix::param::page_size() as u64;
        let first = range.start / page * page;
        let end = range.end.div_ceil(page) * page;
        if first >= end || end as usize > self.len {
            return Ok(first < end);
        }
        let mut pages = vec![0u8; ((end - first) / page) as usize];
        // SAFETY: `first..end` lies within the mapping, and `pages` holds a
        // byte for each of its pages.
        let status = unsafe {
            libc::mincore(
                self.address.add(first as usize),
                (end - first) as usize,
                pages.as_mut_ptr(),
            )
        };
        if status != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(pages.iter().any(|page| page & 1 == 1))
    }
}

impl Drop for PageCache {
    fn drop(&mut self) {
        // SAFETY: the mapping `new` made, unmapped once.
        let _ = unsafe { munmap(self.address, self.len) };
    }
}
