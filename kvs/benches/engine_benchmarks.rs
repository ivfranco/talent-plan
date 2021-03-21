use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::{stream, Future, FutureExt, StreamExt};
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
    sync::Barrier,
    task::JoinHandle,
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

fn spawn_paused_tasks<I, F, Fut>(rt: &Runtime, input: I, f: F) -> (Arc<Barrier>, JoinHandle<()>)
where
    I: IntoIterator,
    I::IntoIter: ExactSizeIterator,
    F: Fn(I::Item) -> Fut,
    Fut: Future + Send + 'static,
{
    let iter = input.into_iter();
    let spawn_barrier = Arc::new(Barrier::new(iter.len() + 1));
    let exec_barrier = Arc::new(Barrier::new(iter.len() + 1));

    let tasks = iter
        .map(|i| {
            let spawn_barrier = spawn_barrier.clone();
            let exec_barrier = exec_barrier.clone();
            let fut = f(i);
            rt.spawn(async move {
                spawn_barrier.wait().await;
                exec_barrier.wait().await;
                fut.await;
            })
        })
        .collect::<Vec<_>>();

    let handle = rt.spawn(stream::iter(tasks).for_each_concurrent(None, |fut| fut.map(|_| ())));
    rt.block_on(spawn_barrier.wait());
    // at this point all tasks are spawned in the runtime

    (exec_barrier, handle)
}

fn init_inputs(seed: u64, sample_size: usize, repeat: usize) -> (Vec<String>, String) {
    const KEY_LEN: usize = 1000;
    const VALUE_LEN: usize = 100;

    let mut rng = StdRng::seed_from_u64(seed);
    let keys = (&mut rng)
        .sample_iter(&StringLength { len: KEY_LEN })
        .take(sample_size)
        .collect::<Vec<_>>();
    let value = (&mut rng).sample(&StringLength { len: VALUE_LEN });

    let mut keys = {
        let mut vec = vec![];
        for _ in 0..repeat {
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
    const SAMPLE_SIZE: usize = 100;
    const REPEAT: usize = 10;

    let (keys, value) = init_inputs(0x1234, SAMPLE_SIZE, REPEAT);
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
            b.iter_with_setup(
                || {
                    spawn_paused_tasks(&runtime, keys.clone(), |key| {
                        let engine = engine.clone();
                        async move {
                            let get = engine.get(key);
                            get.await.unwrap();
                        }
                    })
                },
                |(barrier, handle)| {
                    runtime.block_on(barrier.wait());
                    runtime.block_on(handle).unwrap();
                },
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
