use std::net::Ipv4Addr;

use bytes::{Buf, BytesMut};
use log::{debug, error, info, warn};
use prost::Message;
use tokio::io::{AsyncWriteExt, Interest};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{broadcast, mpsc};

use crate::Filesystem;
use crate::handler::Handler;
use crate::pb::{Request, Response, response};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub(super) struct Socket {
    stop_tx: Sender<()>,
}

impl Socket {
    pub(super) async fn start<FS>(handler: Handler<FS>, server_port: u16) -> Result<Self>
    where
        FS: Filesystem + Send + Sync + Clone + 'static,
    {
        let (start_tx, mut start_rx) = mpsc::channel::<bool>(1);
        let (stop_tx, stop_rx) = mpsc::channel::<()>(1);

        tokio::spawn(async move {
            if spawn_loop(&start_tx, stop_rx, handler, server_port)
                .await
                .is_err()
            {
                let _ = start_tx.send(false).await;
            }
        });

        if !start_rx.recv().await.unwrap_or(false) {
            return Err(Error::StartFailed);
        }

        Ok(Self { stop_tx })
    }

    pub(super) async fn stop(&self) {
        let _ = self.stop_tx.send(()).await;
    }
}

async fn spawn_loop<FS>(
    start_tx: &Sender<bool>,
    mut stop_rx: Receiver<()>,
    handler: Handler<FS>,
    server_port: u16,
) -> Result<()>
where
    FS: Filesystem + Send + Sync + Clone + 'static,
{
    let addr = format!("{}:{}", Ipv4Addr::LOCALHOST, server_port);

    let listener = TcpListener::bind(&addr).await?;
    info!("listening on {addr}");

    let _ = start_tx.send(true).await;

    let (shutdown_tx, _) = broadcast::channel::<()>(2);

    loop {
        select! {
            _ = stop_rx.recv() => {
                info!("stop listening");
                let _ = shutdown_tx.send(());
                break;
            }

            Ok((stream, peer)) = listener.accept() => {
                info!("accepted connection from {peer}");
                let handler = handler.clone();
                let shutdown_rx = shutdown_tx.subscribe();
                tokio::spawn(async move {
                    if let Err(err) = handle_stream(stream, handler, shutdown_rx).await {
                        error!("{err}");
                    }
                });
            }
        }
    }

    Ok(())
}

async fn handle_stream<FS>(
    mut stream: TcpStream,
    mut handler: Handler<FS>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()>
where
    FS: Filesystem + Send + Sync + Clone + 'static,
{
    let mut buf = BytesMut::with_capacity(4096);
    loop {
        select! {
            _ = shutdown_rx.recv() => {
                let _ = stream.shutdown().await;
                info!("connection closed by shutdown: {stream:?}");
                return Ok(());
            }

            r = stream.ready(Interest::READABLE) => {
                r?;
                match stream.try_read_buf(&mut buf) {
                    Ok(0) => {
                        info!("connection closed by peer: {stream:?}");
                        return Ok(());
                    }
                    Ok(_) => {
                        while buf.has_remaining() {
                            let mut frozen = buf.clone().freeze();
                            match Request::decode_length_delimited(&mut frozen) {
                                Ok(request) => {
                                    debug!("received message: id={}", request.id);
                                    buf.advance(buf.len() - frozen.remaining());

                                    let content = match request.content {
                                        Some(content) => match handler.handle(content).await {
                                            Ok(content) => Some(content),
                                            Err(err) => {
                                                error!("handler error: {err}");
                                                None
                                            }
                                        },
                                        None => {
                                            warn!("received request without content: {}", request.id);
                                            Some(response::Content::PosixError(libc::EINVAL))
                                        }
                                    };

                                    let response = Response { request_id: request.id, content };

                                    let mut out = Vec::with_capacity(4096);
                                    response.encode_length_delimited(&mut out).unwrap();

                                    stream.ready(Interest::WRITABLE).await?;
                                    if let Err(err) = stream.write_all(&out).await {
                                        error!("write error: {err}");
                                        return Err(err.into());
                                    }
                                }
                                Err(err) => {
                                    let s = err.to_string();
                                    if !s.contains("failed to decode length prefix")
                                        && !s.contains("buffer underflow")
                                    {
                                        error!("decode error: {err}");
                                        return Err(err.into());
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        continue;
                    }
                    Err(err) => {
                        error!("read error: {err}");
                        return Err(err.into());
                    }
                }
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    DecodeError(#[from] prost::DecodeError),

    #[error("socket failed to start")]
    StartFailed,
}
