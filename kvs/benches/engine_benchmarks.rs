use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use futures::{stream, StreamExt};
use kvs::{log_engine::LogKvsEngine, sled_engine::SledKvsEngine, KvsEngine};
use rand::{
    distributions::{Alphanumeric, Distribution},
    rngs::StdRng,
    Rng, SeedableRng,
};
use std::path::Path;
use tempfile::tempdir;
use tokio::runtime::{self, Runtime};

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
        tokio::spawn(async move {
            let set = engine.set(key, value);
            assert!(set.await.is_ok());
        })
    });
    stream::iter(tasks)
        .for_each_concurrent(None, |fut| async {
            let _ = fut.await;
        })
        .await;
}

async fn read_jobs<E: KvsEngine>(keys: Vec<String>, engine: &E) {
    let tasks = keys.into_iter().map(|key| {
        let engine = engine.clone();
        tokio::spawn(async move {
            let get = engine.get(key);
            assert!(get.await.unwrap().is_some());
        })
    });
    stream::iter(tasks)
        .for_each_concurrent(None, |fut| async {
            let _ = fut.await;
        })
        .await;
}

fn read_benchmark<E, F>(name: &str, constr: F, c: &mut Criterion)
where
    E: KvsEngine,
    F: Fn(&Path) -> E,
{
    const SAMPLE_SIZE: usize = 1000;
    const SAMPLE_LEN: usize = 1000;

    let mut rng = StdRng::seed_from_u64(0x1234);
    let dist = StringLength { len: SAMPLE_LEN };
    let keys = (&mut rng)
        .sample_iter(&dist)
        .take(SAMPLE_SIZE)
        .collect::<Vec<_>>();
    let value = (&mut rng).sample(dist);

    let temp = tempdir().unwrap();
    let engine = constr(temp.path());
    Runtime::new()
        .unwrap()
        .block_on(write_jobs(keys.clone(), value, &engine));

    let mut group = c.benchmark_group(name);

    for n in (1..2 * num_cpus::get_physical()).filter(|n| n.is_power_of_two()) {
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(n)
            .build()
            .unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _n| {
            b.iter_batched(
                || keys.clone(),
                |keys| {
                    runtime.block_on(read_jobs(keys, &engine));
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

criterion_group!(read, read_kvs, read_sled);
criterion_main!(read);
