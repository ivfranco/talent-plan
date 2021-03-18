use std::{
    sync::Arc,
    sync::{
        mpsc::{channel, Sender},
        Mutex,
    },
    thread::{self, JoinHandle},
};

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Command {
    Job(Job),
    Terminate,
}

pub struct ThreadPool {
    sender: Sender<Command>,
    handles: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    pub fn new(threads: u32) -> Self {
        let (sender, receiver) = channel::<Command>();
        let receiver = Arc::new(Mutex::new(receiver));
        let handles = {
            (0..threads)
                .map(|_| {
                    let rx = receiver.clone();
                    thread::spawn(move || loop {
                        // https://users.rust-lang.org/t/30903
                        // in a `while let PAT = EXPR {..}` block, all temporary
                        // value in EXPR has lifetime over the entire loop body,
                        // the code below will never release the lock, all jobs
                        // will be executed on a single thread.
                        // ```
                        // while let Command::Job(job) = rx.lock().unwrap().recv().unwrap() {
                        //     job()
                        // }
                        // ```
                        let command = {
                            let lock = rx.lock().unwrap();
                            lock.recv().unwrap()
                        };

                        match command {
                            Command::Job(job) => job(),
                            Command::Terminate => break,
                        }
                    })
                })
                .collect()
        };

        Self { sender, handles }
    }

    pub fn spawn<F: FnOnce() + Send + 'static>(&self, job: F) {
        self.sender.send(Command::Job(Box::new(job))).unwrap();
    }

    pub fn join(self) {
        for _ in 0..self.handles.len() {
            self.sender.send(Command::Terminate).unwrap();
        }

        for handle in self.handles {
            // Nothing to do here, whatever caused a panic in child threads has
            // already been printed to stderr, the structure of the error is
            // unrecoverable from Box<dyn Any>.
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU32, Ordering},
        time::Duration,
    };

    use super::*;

    #[test]
    fn atomic_read_write() {
        let cnt = Arc::new(AtomicU32::new(0));
        let pool = ThreadPool::new(4);

        for _ in 0..16 {
            let thread_cnt = cnt.clone();
            pool.spawn(move || {
                thread::sleep(Duration::from_millis(1000));
                // with only x86 computers around there's no way to test
                // differences between memory orders
                let i = thread_cnt.fetch_add(1, Ordering::SeqCst);
                println!("Get {} at {:?}", i, thread::current().id());
            });
        }

        pool.join();

        assert_eq!(cnt.load(Ordering::SeqCst), 16);
    }
}
