use crate::Result;
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::thread;

/// A thread pool used to execute jobs on a fixed number of threads. When the
/// ThreadPool goes out of scope every worker thread is shut down.
pub trait ThreadPool: Sized {
    /// Creates a new thread pool, immediately spawning the specified number of
    /// threads.
    /// # Errors
    ///
    /// Returns an error if any thread fails to spawn. All previously-spawned
    /// threads are terminated.
    fn new(threads: u32) -> Result<Self>;

    /// Spawn a function into the threadpool.
    ///
    /// Spawning always succeeds, but if the function panics the threadpool
    /// continues to operate with the same number of threads — the thread count
    /// is not reduced nor is the thread pool destroyed, corrupted or
    /// invalidated.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

/// A naive implementation of ThreadPool. ot really even a thread pool since this
/// implementation is not going to reuse threads between jobs.
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(Self)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(move || job());
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;

/// A thread pool that distributes jobs with a shared queue.
pub struct SharedQueueThreadPool {
    sender: Sender<Job>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (tx, rx) = unbounded();
        for _ in 0..threads {
            spawn_worker(&rx);
        }

        Ok(Self { sender: tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Err(..) = self.sender.send(Box::new(job)) {
            panic!("All receiver terminated");
        }
    }
}

fn spawn_worker(rx: &Receiver<Job>) {
    let rx = rx.clone();
    thread::spawn(move || {
        let mut watcher = Watcher::new(&rx);

        #[allow(clippy::clippy::while_let_loop)]
        loop {
            match rx.recv() {
                Ok(job) => job(),
                Err(..) => break,
            }
        }

        watcher.cancel();
    });
}

/// Machanism similar to Sentinel in
/// [threadpool](https://docs.rs/threadpool/1.8.1/src/threadpool/lib.rs.html#106)
struct Watcher<'a> {
    receiver: &'a Receiver<Job>,
    active: bool,
}

impl<'a> Watcher<'a> {
    fn new(receiver: &'a Receiver<Job>) -> Self {
        Self {
            receiver,
            active: true,
        }
    }

    fn cancel(&mut self) {
        self.active = false;
    }
}

impl<'a> Drop for Watcher<'a> {
    fn drop(&mut self) {
        // When a job panics the resources captured / allocated by the closure
        // may be leaked (memory leak is not part of the memory safety Rust
        // guarantees), instead of catch_unwind the panicking thread is let go
        // so the system may have a chance to recycle the resources.
        if self.active && thread::panicking() {
            warn!("Job panicked, spawn another worker");
            spawn_worker(self.receiver)
        }
    }
}

/// A wrapper of [rayon_core::ThreadPool](https://docs.rs/rayon-core/1.9.0/rayon_core/struct.ThreadPool.html).
pub struct RayonThreadPool {
    pool: rayon_core::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let pool = rayon_core::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()?;

        Ok(Self { pool })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job)
    }
}
