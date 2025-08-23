use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use std::io;

use socket2::{Domain, Protocol, Socket, Type};
use mio::{Events, Interest, Poll, Token};
use mio::net::TcpStream as MioTcpStream;

use libc;

use crate::{PingOptions, PingResult, Pinger};

pub struct TcpPinger {
    options: PingOptions,
}

impl Pinger for TcpPinger {
    fn from_options(options: PingOptions) -> Result<Self, crate::PingCreationError> {
        Ok(TcpPinger { options })
    }

    fn parse_fn(&self) -> fn(String) -> Option<PingResult> {
        |_| None
    }

    fn ping_args(&self) -> (&str, Vec<String>) {
        ("tcp", vec![])
    }

    fn start(&self) -> Result<mpsc::Receiver<PingResult>, crate::PingCreationError> {
        let (tx, rx) = mpsc::channel();
        let options = self.options.clone();

        thread::spawn(move || {
            for _ in 0.. {
                let port = options.port.unwrap_or(80);
                let socket_str = format!("{}:{}", options.target, port);

                let addr: SocketAddr = match socket_str.to_socket_addrs() {
                    Ok(mut addrs) => match addrs.next() {
                        Some(a) => a,
                        None => {
                            let _ = tx.send(PingResult::Unknown("Unable to resolve address".into()));
                            continue;
                        }
                    },
                    Err(e) => {
                        let _ = tx.send(PingResult::Unknown(format!("Resolve error: {}", e)));
                        continue;
                    }
                };

                let result = tcp_probe(&addr, options.interval, options.allow_rst);

                match result {
                    Ok(Some(duration)) => {
                        let _ = tx.send(PingResult::Pong(duration, addr.to_string()));
                    }
                    Ok(None) => {
                        let _ = tx.send(PingResult::Timeout(addr.to_string()));
                    }
                    Err(e) => {
                        let _ = tx.send(PingResult::Unknown(format!("Probe error: {}", e)));
                    }
                }

                thread::sleep(options.interval);
            }
        });

        Ok(rx)
    }
}

/// Cross-platform TCP probe using non-blocking socket + mio for readiness.
/// Returns:
/// - Ok(Some(duration)) → SYN+ACK (connected) or SYN+RST (if allow_rst = true)
/// - Ok(None) → timeout or RST (when allow_rst = false)
/// - Err(e) → unexpected error
fn tcp_probe(addr: &SocketAddr, timeout: Duration, allow_rst: bool) -> io::Result<Option<Duration>> {
    let socket = Socket::new(Domain::for_address(*addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_nonblocking(true)?;

    // Initiate nonblocking connect
    match socket.connect(&(*addr).into()) {
        Ok(_) => {
            // Immediately connected
            return Ok(Some(Instant::now().elapsed()));
        }
        Err(ref e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {
            // normal non-blocking connect in progress
        }
        Err(ref e) if e.kind() == io::ErrorKind::ConnectionRefused => {
            return if allow_rst { Ok(Some(Instant::now().elapsed())) } else { Ok(None) };
        }
        Err(e) => return Err(e),
    }

    // Wrap socket for mio polling
    let mut stream = MioTcpStream::from_std(socket.into());
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(1);
    poll.registry()
        .register(&mut stream, Token(0), Interest::WRITABLE)?;

    let start = Instant::now();

    // Wait for WRITABLE event (handshake complete or error)
    poll.poll(&mut events, Some(timeout))?;
    if events.is_empty() {
        return Ok(None); // timeout
    }

    let err = stream.take_error()?;
    drop(stream); // close socket

    match err {
        None => Ok(Some(start.elapsed())), // handshake success
        Some(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
            if allow_rst { Ok(Some(start.elapsed())) } else { Ok(None) }
        }
        Some(e) => Err(e),
    }
}
