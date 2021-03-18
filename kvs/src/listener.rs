use futures::{
    channel::oneshot,
    future::{select, Either},
};
use std::{io, net::SocketAddr, thread::JoinHandle};
use std::{sync::mpsc::Sender, thread};
use tokio::{net::TcpListener, runtime};

/// The orders a [Listener](Listener) can give to a [KvsServer](KvsServer).
pub enum ServerOrder {
    /// A TCP stream from a client.
    Stream(std::net::TcpStream),
    /// An order to shutdown the server.
    Shutdown,
}

/// The shutdown switch of the a [Listener](Listener).
pub struct ShutdownSwitch {
    shutdown_tx: oneshot::Sender<()>,
}

impl ShutdownSwitch {
    /// Shutdown the corresponding [Listener](Listener).
    pub fn shutdown(self) {
        self.shutdown_tx.send(()).unwrap();
    }
}

/// A TcpListener that can be remotely shutdown without resorting to SIGTERM or
/// SIGKILL.
pub struct Listener {
    listener: TcpListener,
    shutdown_rx: oneshot::Receiver<()>,
    order_tx: Sender<ServerOrder>,
}

impl Listener {
    /// Spawn the TcpListener in another thread, return a handle to that thread
    /// and the shutdown switch.
    pub fn spawn(
        addr: SocketAddr,
        order_tx: Sender<ServerOrder>,
    ) -> (JoinHandle<()>, ShutdownSwitch) {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = thread::spawn(move || {
            let rt = runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .unwrap();

            rt.block_on(Listener::bind(addr, order_tx, shutdown_rx))
                .unwrap();
        });

        (handle, ShutdownSwitch { shutdown_tx })
    }

    async fn bind(
        addr: SocketAddr,
        order_tx: Sender<ServerOrder>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> io::Result<()> {
        let listener = TcpListener::bind(addr).await?;

        let this = Self {
            listener,
            shutdown_rx,
            order_tx,
        };

        this.poll_loop().await
    }

    async fn poll_loop(self) -> io::Result<()> {
        let Self {
            listener,
            mut shutdown_rx,
            order_tx,
        } = self;

        loop {
            let stream = listener.accept();
            tokio::pin!(stream);

            match select(&mut shutdown_rx, stream).await {
                Either::Left((..)) => {
                    // either the switch is pushed, or the switch is dropped
                    let _ = order_tx.send(ServerOrder::Shutdown);
                    break;
                }
                Either::Right((item, _)) => match item {
                    Ok((s, _)) => {
                        let _ = order_tx.send(ServerOrder::Stream(s.into_std()?));
                    }
                    Err(e) => {
                        eprintln!("On accepting TCP stream: {}", e);
                    }
                },
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server;
    use std::{net::TcpStream, sync::mpsc::channel, time::Duration};

    #[test]
    fn listener_shutdown() {
        let mut addr = server::default_addr();
        addr.set_port(4001);
        let (order_tx, _order_rx) = channel();
        let (handle, switch) = Listener::spawn(addr, order_tx);

        switch.shutdown();
        handle.join().unwrap();
    }

    #[test]
    fn listener_send_stream() {
        let mut addr = server::default_addr();
        addr.set_port(4002);
        let (order_tx, order_rx) = channel();
        let (handle, switch) = Listener::spawn(addr, order_tx);

        for _ in 0..10 {
            thread::spawn(move || {
                TcpStream::connect(addr).unwrap();
                thread::sleep(Duration::from_millis(5000));
            });
        }

        for _ in 0..10 {
            order_rx.recv().unwrap();
        }

        switch.shutdown();
        handle.join().unwrap();
    }
}
