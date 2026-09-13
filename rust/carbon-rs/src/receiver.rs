use crate::app::App;
use crate::config::Receiver;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

fn line(app: &App, bytes: &[u8], limit: usize) {
    if bytes.len() > limit {
        app.invalid.fetch_add(1, Ordering::Relaxed);
        return;
    }
    match crate::plaintext::parse_line(bytes) {
        Ok((name, point)) => {
            if let Err(e) = app.ingest(name, point)
                && e.kind() == io::ErrorKind::InvalidInput
            {
                app.invalid.fetch_add(1, Ordering::Relaxed);
            }
        }
        Err(_) => {
            app.invalid.fetch_add(1, Ordering::Relaxed);
        }
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
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                match slots.clone().try_acquire_owned() {
                    Ok(permit) => {
                        let app = app.clone(); let config = config.clone(); let stop = stop.clone();
                        connections.spawn(async move { let _permit = permit; connection(stream, app, config, stop).await });
                    }
                    Err(_) => eprintln!("TCP connection refused: max_connections={} reached", config.max_connections),
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
            return Ok(());
        } // Go discards a partial line at EOF.
        for &byte in &buffer[..n] {
            if byte == b'\n' {
                if !oversized {
                    line(&app, &pending, config.max_line_bytes);
                }
                pending.clear();
                oversized = false;
            } else if !oversized {
                if pending.len() >= config.max_line_bytes {
                    pending.clear();
                    oversized = true;
                    app.invalid.fetch_add(1, Ordering::Relaxed);
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
                line(&app, bytes, config.max_line_bytes);
            }
        }
    }
}
