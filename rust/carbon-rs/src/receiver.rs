use crate::app::App;
use crate::config::Receiver;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

fn line(app: &App, bytes: &[u8], limit: usize, tcp: bool) {
    if bytes.len() > limit {
        app.invalid.fetch_add(1, Ordering::Relaxed);
        parse_failed(tcp, bytes, &"line exceeds max-line-bytes");
        return;
    }
    match crate::plaintext::parse_line(bytes) {
        Ok((name, point)) => {
            if tcp
                && let Some(counter) = app
                    .prometheus
                    .as_ref()
                    .and_then(|m| m.tcp_received.as_ref())
            {
                counter.inc();
            }
            if let Err(e) = app.ingest(name, point)
                && e.kind() == io::ErrorKind::InvalidInput
            {
                app.invalid.fetch_add(1, Ordering::Relaxed);
                parse_failed(tcp, bytes, &e);
            }
        }
        Err(error) => {
            app.invalid.fetch_add(1, Ordering::Relaxed);
            parse_failed(tcp, bytes, &error);
        }
    }
}

fn parse_failed(tcp: bool, bytes: &[u8], error: &dyn std::fmt::Display) {
    // Bound debug payloads; JSON encoding escapes untrusted line content.
    let bytes = &bytes[..bytes.len().min(4096)];
    if tcp {
        tracing::debug!(target: "tcp", error = %error, line = %String::from_utf8_lossy(bytes), "parse failed");
    } else {
        tracing::debug!(target: "udp", error = %error, line = %String::from_utf8_lossy(bytes), "parse failed");
    }
}

pub async fn tcp(
    listener: TcpListener,
    app: Arc<App>,
    config: Receiver,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let slots = Arc::new(Semaphore::new(config.max_connections.max(1)));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result { tracing::error!(target: "tcp", error = %error, "connection task failed"); }
            },
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                match slots.clone().try_acquire_owned() {
                    Ok(permit) => {
                        let app = app.clone(); let config = config.clone(); let stop = stop.clone();
                        connections.spawn(async move {
                            let _permit = permit;
                            if let Err(error) = connection(stream, app, config, stop).await {
                                tracing::error!(target: "tcp", peer = %peer, error = %error, "read error");
                            }
                        });
                    }
                    Err(_) => tracing::warn!(target: "tcp", peer = %peer, max_connections = config.max_connections, "failed to accept connection: connection limit reached"),
                }
            }
        }
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn connection(
    mut stream: TcpStream,
    app: Arc<App>,
    config: Receiver,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut buffer = [0u8; 16_384];
    let mut pending = Vec::new();
    let mut oversized = false;
    loop {
        let n = tokio::select! {
            biased;
            _ = stop.changed() => return Ok(()),
            result = tokio::time::timeout(config.read_timeout, stream.read(&mut buffer)) => match result { Ok(result) => result?, Err(_) => return Ok(()) },
        };
        if n == 0 {
            if !pending.is_empty() {
                tracing::warn!(target: "tcp", line = %String::from_utf8_lossy(&pending[..pending.len().min(4096)]), "unfinished line");
            }
            return Ok(());
        } // Go discards a partial line at EOF.
        for &byte in &buffer[..n] {
            if byte == b'\n' {
                if !oversized {
                    line(&app, &pending, config.max_line_bytes, true);
                }
                pending.clear();
                oversized = false;
            } else if !oversized {
                if pending.len() >= config.max_line_bytes {
                    pending.clear();
                    oversized = true;
                    app.invalid.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(target: "tcp", max_line_bytes = config.max_line_bytes, "line exceeds max-line-bytes");
                } else {
                    pending.push(byte);
                }
            }
        }
    }
}

pub async fn udp(
    socket: UdpSocket,
    app: Arc<App>,
    config: Receiver,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut buffer = vec![0u8; 65_535];
    loop {
        let n = tokio::select! { biased; _ = stop.changed() => return Ok(()), result = socket.recv(&mut buffer) => result? };
        for bytes in buffer[..n].split(|b| *b == b'\n') {
            if !bytes.is_empty() {
                line(&app, bytes, config.max_line_bytes, false);
            }
        }
    }
}
