use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use futures::{stream, FutureExt, StreamExt};
use kvs::{log_engine::LogKvsEngine, sled_engine::SledKvsEngine, KvsEngine};
use rand::{
    distributions::{Alphanumeric, Distribution},
    prelude::SliceRandom,
    rngs::StdRng,
    Rng, SeedableRng,
};
use std::{path::Path, sync::Arc};
use tempfile::tempdir;
use tokio::{
    runtime::{self, Runtime},
    sync::Notify,
};

struct StringLength {
    len: usize,
}

impl Distribution<String> for StringLength {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> String {
        rng.sample_iter(Alphanumeric)
            .map(char::from)
            .take(self.len)
            .collect()
    }
}

async fn write_jobs<E: KvsEngine>(keys: Vec<String>, value: String, engine: &E) {
    let tasks = keys.into_iter().map(|key| {
        let engine = engine.clone();
        let value = value.clone();
        async move {
            let set = engine.set(key, value);
            assert!(set.await.is_ok());
        }
    });
    stream::iter(tasks)
        .for_each_concurrent(None, |fut| async {
            let _ = fut.await;
        })
        .await;
}

async fn read_jobs<E: KvsEngine>(keys: Vec<String>, engine: E, notify: Arc<Notify>) {
    let tasks = keys
        .into_iter()
        .map(|key| {
            let engine = engine.clone();
            let notify = notify.clone();
            tokio::spawn(async move {
                notify.notified().await;
                let get = engine.get(key);
                let value = get.await.unwrap();
                assert!(value.is_some());
            })
        })
        .collect::<Vec<_>>();

    stream::iter(tasks)
        .for_each_concurrent(None, |fut| fut.map(|_| ()))
        .await;
}

fn init_inputs(seed: u64) -> (Vec<String>, String) {
    const SAMPLE_SIZE: usize = 100;
    const REPEAT: usize = 10;
    const KEY_LEN: usize = 1000;
    const VALUE_LEN: usize = 100;

    let mut rng = StdRng::seed_from_u64(seed);
    let keys = (&mut rng)
        .sample_iter(&StringLength { len: KEY_LEN })
        .take(SAMPLE_SIZE)
        .collect::<Vec<_>>();
    let value = (&mut rng).sample(&StringLength { len: VALUE_LEN });

    let mut keys = {
        let mut vec = vec![];
        for _ in 0..REPEAT {
            vec.extend_from_slice(&keys);
        }
        vec
    };

    keys.shuffle(&mut rng);

    (keys, value)
}

fn read_benchmark<E, F>(name: &str, constr: F, c: &mut Criterion)
where
    E: KvsEngine,
    F: Fn(&Path) -> E,
{
    let (keys, value) = init_inputs(0x1234);
    let temp = tempdir().unwrap();
    let engine = constr(temp.path());
    {
        Runtime::new()
            .unwrap()
            .block_on(write_jobs(keys.clone(), value, &engine));
    }

    let mut group = c.benchmark_group(name);

    for n in (0..)
        .map(|n| 2usize.pow(n))
        .take_while(|&n| n < 2 * num_cpus::get_physical())
    {
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(n)
            .build()
            .unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _n| {
            b.iter_batched(
                || {
                    let notify = Arc::new(Notify::new());
                    let handle =
                        runtime.spawn(read_jobs(keys.clone(), engine.clone(), notify.clone()));
                    (notify, handle)
                },
                |(notify, handle)| {
                    notify.notify_waiters();
                    runtime.block_on(handle).unwrap();
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

pub fn read_kvs(c: &mut Criterion) {
    read_benchmark("read_kvs", |path| LogKvsEngine::open(path).unwrap(), c)
}

pub fn read_sled(c: &mut Criterion) {
    read_benchmark("read_sled", |path| SledKvsEngine::open(path).unwrap(), c)
}

criterion_group!(read, read_sled);
criterion_main!(read);
