use criterion::{BenchmarkId, Criterion, Throughput};
use kvs::thread_pool::ThreadPool;
use kvs::{client::KvsClient, server::KvsServer, thread_pool::SharedQueueThreadPool, KvsEngine};
use rand::{distributions::Alphanumeric, prelude::Distribution, rngs::SmallRng, Rng, SeedableRng};
use std::{
    fs::create_dir,
    path::Path,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
};
use tempfile::tempdir;

fn main() {}

struct StringLength {
    len: usize,
}

impl StringLength {
    fn new(len: usize) -> Self {
        Self { len }
    }
}

impl Distribution<String> for StringLength {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> String {
        rng.sample_iter(Alphanumeric)
            .map(char::from)
            .take(self.len)
            .collect()
    }
}

struct Counter {
    cnt: AtomicI32,
    lock: Mutex<()>,
    cvar: Condvar,
}

impl Counter {
    fn new(n: i32) -> Self {
        Self {
            cnt: AtomicI32::new(n),
            lock: Mutex::new(()),
            cvar: Condvar::new(),
        }
    }

    fn block(&self) {
        let mut guard = self.lock.lock().unwrap();
        while self.cnt.load(Ordering::Acquire) > 0 {
            guard = self.cvar.wait(guard).unwrap();
        }
    }

    fn dec_notify_one(&self) {
        if self.cnt.fetch_sub(1, Ordering::AcqRel) - 1 <= 0 {
            self.cvar.notify_one();
        }
    }
}

fn set_job(key: String, value: String) -> Result<(), kvs::server::Error> {
    let client = KvsClient::connect(kvs::server::default_addr())?;
    client.set(key, value)?;
    Ok(())
}

fn write_benchmark<S, F, P>(name: &str, constr: F, c: &mut Criterion)
where
    S: KvsEngine,
    F: Fn(&Path) -> S,
    P: ThreadPool + Send + 'static,
{
    let mut group = c.benchmark_group(name);

    let threads = (0..)
        .map(|n| 2u32.pow(n))
        .take_while(|&n| (n as usize) <= 2 * num_cpus::get());

    const KEY_SIZE: usize = 1000;
    const SAMPLE_SIZE: usize = 1000;

    let mut rng = SmallRng::seed_from_u64(0x1234);
    let dist = StringLength::new(KEY_SIZE);
    let keys: Vec<_> = (&mut rng).sample_iter(&dist).take(SAMPLE_SIZE).collect();
    let value = dist.sample(&mut rng);

    for n in threads {
        let client_pool = SharedQueueThreadPool::new(n).unwrap();
        let temp = tempdir().unwrap();
        let mut iter = 0;
        group.throughput(Throughput::Elements(u64::from(n)));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_with_setup(
                || {
                    let path = temp.path().join(format!("{}", iter));
                    create_dir(path).unwrap();
                    let engine = constr(&temp.path().join(format!("{}", n)));
                    let server = KvsServer::open(engine, P::new(n).unwrap());
                    let handle = thread::spawn(move || server.listen(None).unwrap());
                    iter += 1;
                    (keys.clone(), handle)
                },
                |(keys, handle)| {
                    let cnt = Arc::new(Counter::new(SAMPLE_SIZE as i32));
                    for key in keys {
                        let value = value.clone();
                        let cnt = cnt.clone();
                        client_pool.spawn(move || {
                            set_job(key, value).unwrap();
                            cnt.dec_notify_one();
                        });
                    }

                    cnt.block();
                },
            )
        });
    }
}
