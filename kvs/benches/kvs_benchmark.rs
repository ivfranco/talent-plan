use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::KvStore;
use kvs::{sled_engine::SledKvsEngine, KvsEngine};
use rand::{distributions::Alphanumeric, prelude::SliceRandom, rngs::SmallRng, Rng, SeedableRng};
use std::{fs::create_dir, iter::from_fn, path::Path};
use tempfile::tempdir;

fn generate_random_strings<R: Rng>(rng: &mut R, max_len: usize, cnt: usize) -> Vec<String> {
    from_fn(|| {
        let len = rng.gen_range(1..=max_len);
        let str = rng
            .sample_iter(Alphanumeric)
            .take(len)
            .map(char::from)
            .collect();
        Some(str)
    })
    .take(cnt)
    .collect()
}

fn write_benchmark<S, F>(id: &str, constr: F, c: &mut Criterion)
where
    S: KvsEngine,
    F: Fn(&Path) -> S,
{
    // this temporary directory cannot be created in the benchmark setup
    // function, when TempDir went out of scope the directory will be deleted
    let temp = tempdir().unwrap();
    let mut rng = SmallRng::seed_from_u64(0x1234);
    let (keys, values) = (
        generate_random_strings(&mut rng, 100_000, 100),
        generate_random_strings(&mut rng, 100_000, 100),
    );
    let mut iter = 0;

    c.bench_function(id, |b| {
        b.iter_batched(
            || {
                let dir = temp.path().join(iter.to_string());
                create_dir(&dir).unwrap();
                let store = constr(&dir);
                iter += 1;
                (store, keys.clone(), values.clone())
            },
            |(mut store, keys, values)| {
                for (key, value) in keys.into_iter().zip(values) {
                    assert!(store.set(key, value).is_ok());
                }
            },
            BatchSize::LargeInput,
        )
    });
}

pub fn kvs_write(c: &mut Criterion) {
    write_benchmark("kvs_write", |path| KvStore::open(path).unwrap(), c)
}

pub fn sled_write(c: &mut Criterion) {
    write_benchmark("sled_write", |path| SledKvsEngine::open(path).unwrap(), c)
}

fn read_benchmark<S, F>(id: &str, constr: F, c: &mut Criterion)
where
    S: KvsEngine,
    F: Fn(&Path) -> S,
{
    let mut rng = SmallRng::seed_from_u64(0x1234);

    let (keys, values) = (
        generate_random_strings(&mut rng, 1000, 100),
        generate_random_strings(&mut rng, 1000, 100),
    );

    let temp = tempdir().unwrap();
    let mut store = constr(temp.path());

    for (key, value) in keys.iter().zip(values.iter()) {
        store.set(key.clone(), value.clone()).unwrap();
    }

    let mut chosen = vec![];
    for _ in 0..1000 {
        chosen.extend(keys.choose(&mut rng).cloned());
    }

    c.bench_function(id, |b| {
        b.iter_batched(
            || chosen.clone(),
            |chosen| {
                for key in chosen {
                    assert!(store.get(key).is_ok());
                }
            },
            BatchSize::LargeInput,
        )
    });
}

pub fn kvs_read(c: &mut Criterion) {
    read_benchmark("kvs_read", |path| KvStore::open(path).unwrap(), c)
}

pub fn sled_read(c: &mut Criterion) {
    read_benchmark("sled_read", |path| SledKvsEngine::open(path).unwrap(), c)
}

criterion_group!(write, kvs_write, sled_write);
criterion_group!(read, kvs_read, sled_read);
criterion_main!(write, read);
