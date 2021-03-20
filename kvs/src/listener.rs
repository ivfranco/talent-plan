use futures::{channel::oneshot, FutureExt, Stream};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::net::{TcpListener, ToSocketAddrs};

/// The orders a [Listener](Listener) can give to a [KvsServer](KvsServer).
pub enum ServerOrder {
    /// A TCP stream from a client.
    Stream(std::net::TcpStream),
    /// An order to shutdown the server.
    Shutdown,
}

/// The shutdown switch of a [Listener](Listener).
pub struct ShutdownSwitch {
    shutdown_tx: oneshot::Sender<()>,
}

impl ShutdownSwitch {
    /// Shutdown the corresponding [Listener](Listener). Block until the listener thread is
    /// terminated.
    pub async fn shutdown(self) {
        if self.shutdown_tx.send(()).is_err() {
            warn!("Listener already terminated");
        }
    }
}

/// A TcpListener that can be remotely shutdown without resorting to SIGTERM or SIGKILL.
pub struct Listener {
    listener: TcpListener,
    shutdown_rx: oneshot::Receiver<()>,
}

impl Listener {
    /// Spawn the TcpListener as a task, return the shutdown switch.
    pub async fn wire<A: ToSocketAddrs>(addr: A) -> io::Result<(Self, ShutdownSwitch)> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let this = Self {
            listener: TcpListener::bind(addr).await?,
            shutdown_rx,
        };
        let switch = ShutdownSwitch { shutdown_tx };

        Ok((this, switch))
    }
}

impl Stream for Listener {
    type Item = io::Result<(tokio::net::TcpStream, SocketAddr)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.shutdown_rx.poll_unpin(cx).is_ready() {
            // either the switch is pushed or dropped
            return Poll::Ready(None);
        }

        self.listener.poll_accept(cx).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{protocol::Error, server};
    use futures::stream::StreamExt;
    use std::time::Duration;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn listener_shutdown() -> Result<(), Error> {
        let mut addr = server::default_addr();
        addr.set_port(4005);
        let (mut listener, switch) = Listener::wire(addr).await?;
        let join = tokio::spawn(async move {
            listener.next().await;
        });

        switch.shutdown().await;
        assert!(join.await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn listener_recv_stream() -> Result<(), Error> {
        let mut addr = server::default_addr();
        addr.set_port(4006);
        let (mut listener, switch) = Listener::wire(addr).await?;

        for _ in 0u8..10 {
            tokio::spawn(async move {
                let s = TcpStream::connect(addr).await.unwrap();
                tokio::time::sleep(Duration::from_secs(5)).await;
                drop(s);
            });
        }

        for _ in 0u8..10 {
            assert!(listener.next().await.is_some());
        }

        switch.shutdown().await;
        assert!(listener.next().await.is_none());

        Ok(())
    }
}
