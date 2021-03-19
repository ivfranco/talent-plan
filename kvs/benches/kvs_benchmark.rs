use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use kvs::{
    client::KvsClient,
    server::KvsServer,
    sled_engine::SledKvsEngine,
    thread_pool::{RayonThreadPool, SharedQueueThreadPool},
    KvsEngine,
};
use kvs::{thread_pool::ThreadPool, LogKvsEngine};
use rand::{distributions::Alphanumeric, prelude::Distribution, rngs::SmallRng, Rng, SeedableRng};
use std::{
    fs::create_dir,
    path::Path,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc, Condvar, Mutex,
    },
};
use tempfile::tempdir;

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

#[inline]
fn set_job(key: String, value: String) -> Result<(), kvs::server::Error> {
    let client = KvsClient::connect(kvs::server::default_addr())?;
    client.set(key, value)?;
    Ok(())
}

#[inline]
fn get_job(key: String) -> Result<(), kvs::server::Error> {
    let client = KvsClient::connect(kvs::server::default_addr())?;
    client.get(key)?;
    Ok(())
}

const KEY_SIZE: usize = 1000;
const SAMPLE_SIZE: usize = 1000;

fn write_benchmark<S, F, P>(name: &str, constr: F, c: &mut Criterion)
where
    S: KvsEngine,
    F: Fn(&Path) -> S,
    P: ThreadPool,
{
    let mut group = c.benchmark_group(name);
    group.sample_size(50);

    let num_cpus = num_cpus::get();
    let threads = (0..)
        .map(|n| 2u32.pow(n))
        .take_while(|&n| (n as usize) <= 2 * num_cpus);

    // All tests inserts the same set of "random" keys and values
    let mut rng = SmallRng::seed_from_u64(0x1234);
    let dist = StringLength::new(KEY_SIZE);
    let keys: Vec<_> = (&mut rng).sample_iter(&dist).take(SAMPLE_SIZE).collect();
    let value = dist.sample(&mut rng);

    for n in threads {
        // too much threads in the client thread pool significantly hinders the performance
        let client_pool = RayonThreadPool::new(num_cpus::get_physical() as u32 * 2).unwrap();
        // the temporary directory is cleaned after each iteration
        let temp = tempdir().unwrap();
        let mut iter = 0;

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_with_setup(
                || {
                    // engines impose different on-disk file structures, the only way to isolate
                    // stores in the benchmark loop is to give them directories of their own
                    let path = temp.path().join(format!("{}", iter));
                    create_dir(&path).unwrap();
                    iter += 1;

                    let engine = constr(&path);
                    let server = KvsServer::open(engine, P::new(n).unwrap());
                    let switch = server.spawn(None);

                    (keys.clone(), switch)
                },
                |(keys, switch)| {
                    let cnt = Arc::new(Counter::new(SAMPLE_SIZE as i32));
                    for key in keys {
                        let value = value.clone();
                        let cnt = cnt.clone();
                        client_pool.spawn(move || {
                            // panic_control::disable_hook_in_current_thread();
                            set_job(key, value).unwrap();
                            cnt.dec_notify_one();
                        });
                    }

                    cnt.block();
                    switch.shutdown_non_blocking();
                },
            )
        });
    }

    group.finish();
}

fn write_queued_kv_store(c: &mut Criterion) {
    write_benchmark::<_, _, SharedQueueThreadPool>(
        "write_queued_kv_store",
        |path| LogKvsEngine::open(path).unwrap(),
        c,
    );
}

fn write_rayon_kv_store(c: &mut Criterion) {
    write_benchmark::<_, _, RayonThreadPool>(
        "write_rayon_kv_store",
        |path| LogKvsEngine::open(path).unwrap(),
        c,
    );
}

fn write_rayon_sled(c: &mut Criterion) {
    write_benchmark::<_, _, RayonThreadPool>(
        "write_rayon_sled",
        |path| SledKvsEngine::open(path).unwrap(),
        c,
    );
}

fn read_benchmark<S, F, P>(name: &str, constr: F, c: &mut Criterion)
where
    S: KvsEngine,
    F: Fn(&Path) -> S,
    P: ThreadPool,
{
    let mut group = c.benchmark_group(name);
    group.sample_size(50);

    let num_cpus = num_cpus::get();
    let threads = (0..)
        .map(|n| 2u32.pow(n))
        .take_while(|&n| (n as usize) <= 2 * num_cpus);

    // All tests inserts the same set of "random" keys and values
    let mut rng = SmallRng::seed_from_u64(0x1234);
    let dist = StringLength::new(KEY_SIZE);
    let keys: Vec<_> = (&mut rng).sample_iter(&dist).take(SAMPLE_SIZE).collect();
    let value = dist.sample(&mut rng);

    for n in threads {
        // too much threads in the client thread pool significantly hinders the performance
        let client_pool = RayonThreadPool::new(num_cpus::get_physical() as u32 * 2).unwrap();
        // the temporary directory is cleaned after each iteration
        let temp = tempdir().unwrap();
        let mut iter = 0;

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_with_setup(
                || {
                    // engines impose different on-disk file structures, the only way to isolate
                    // stores in the benchmark loop is to give them directories of their own
                    let path = temp.path().join(format!("{}", iter));
                    create_dir(&path).unwrap();
                    iter += 1;

                    let engine = constr(&path);
                    for key in &keys {
                        engine.set(key.clone(), value.clone()).unwrap();
                    }
                    let server = KvsServer::open(engine, P::new(n).unwrap());
                    let switch = server.spawn(None);

                    (keys.clone(), switch)
                },
                |(keys, switch)| {
                    let cnt = Arc::new(Counter::new(SAMPLE_SIZE as i32));
                    for key in keys {
                        let cnt = cnt.clone();
                        client_pool.spawn(move || {
                            // panic_control::disable_hook_in_current_thread();
                            get_job(key).unwrap();
                            cnt.dec_notify_one();
                        });
                    }

                    cnt.block();
                    switch.shutdown_non_blocking();
                },
            )
        });
    }

    group.finish();
}

fn read_queued_kv_store(c: &mut Criterion) {
    read_benchmark::<_, _, SharedQueueThreadPool>(
        "read_queued_kv_store",
        |path| LogKvsEngine::open(path).unwrap(),
        c,
    )
}

fn read_rayon_kv_store(c: &mut Criterion) {
    read_benchmark::<_, _, RayonThreadPool>(
        "read_rayon_kv_store",
        |path| LogKvsEngine::open(path).unwrap(),
        c,
    )
}

fn read_rayon_sled(c: &mut Criterion) {
    read_benchmark::<_, _, RayonThreadPool>(
        "read_rayon_sled",
        |path| SledKvsEngine::open(path).unwrap(),
        c,
    )
}

criterion_group!(
    write,
    write_queued_kv_store,
    write_rayon_kv_store,
    write_rayon_sled
);
criterion_group!(
    read,
    read_queued_kv_store,
    read_rayon_kv_store,
    read_rayon_sled
);

criterion_main!(write, read);
