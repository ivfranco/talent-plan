use futures::{stream, FutureExt, StreamExt};
use kvs::log_engine::LogKvsEngine as KvStore;
use kvs::{KvsEngine, Result};
use tempfile::TempDir;

// Should get previously stored value
#[tokio::test]
async fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned()).await?;
    store.set("key2".to_owned(), "value2".to_owned()).await?;

    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value1".to_owned())
    );
    assert_eq!(
        store.get("key2".to_owned()).await?,
        Some("value2".to_owned())
    );

    // Open from disk again and check persistent data
    drop(store);
    let store = KvStore::open(temp_dir.path())?;
    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value1".to_owned())
    );
    assert_eq!(
        store.get("key2".to_owned()).await?,
        Some("value2".to_owned())
    );

    Ok(())
}

// KvStore::<$pool>::open($path, $thread) ==>> KvStore::open($path)

// Should overwrite existent value
#[tokio::test]
async fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned()).await?;
    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value1".to_owned())
    );
    store.set("key1".to_owned(), "value2".to_owned()).await?;
    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value2".to_owned())
    );

    // Open from disk again and check persistent data
    drop(store);
    let store = KvStore::open(temp_dir.path())?;
    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value2".to_owned())
    );
    store.set("key1".to_owned(), "value3".to_owned()).await?;
    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value3".to_owned())
    );

    Ok(())
}

// Should get `None` when getting a non-existent key
#[tokio::test]
async fn get_non_existent_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned()).await?;
    assert_eq!(store.get("key2".to_owned()).await?, None);

    // Open from disk again and check persistent data
    drop(store);
    let store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key2".to_owned()).await?, None);

    Ok(())
}

#[tokio::test]
async fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;
    assert!(store.remove("key1".to_owned()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned()).await?;
    assert!(store.remove("key1".to_owned()).await.is_ok());
    assert_eq!(store.get("key1".to_owned()).await?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[tokio::test(flavor = "multi_thread")]
async fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;

    let mut current_size = store.on_disk_size();
    for iter in 0u32..1000 {
        for key_id in 0u32..1000 {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            store.set(key, value).await?;
        }

        let new_size = store.on_disk_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        }
        // Compaction triggered

        drop(store);
        // reopen and check content
        let store = KvStore::open(temp_dir.path())?;
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            assert_eq!(store.get(key).await?, Some(format!("{}", iter)));
        }
        return Ok(());
    }

    panic!("No compaction detected");
}

#[tokio::test]
async fn concurrent_set() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let store = KvStore::open(temp_dir.path())?;

    // concurrent set in 8 threads
    stream::iter((0..10000).map(|i| store.set(format!("key{}", i), format!("value{}", i))))
        .for_each_concurrent(Some(8), |fut| fut.map(|_| ()))
        .await;

    // We only check concurrent set in this test, so we check sequentially here
    let store = KvStore::open(temp_dir.path())?;
    for i in 0..10000 {
        assert_eq!(
            store.get(format!("key{}", i)).await?,
            Some(format!("value{}", i))
        );
    }

    Ok(())
}

#[tokio::test]
async fn concurrent_get() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;
    // We only check concurrent get in this test, so we set sequentially here
    for i in 0u32..100 {
        store
            .set(format!("key{}", i), format!("value{}", i))
            .await
            .unwrap();
    }

    let gets = (0u32..100)
        .flat_map(|thread_id| (0u32..100).map(move |i| (thread_id + i) % 100))
        .map(|key_id| {
            store.get(format!("key{}", key_id)).map(move |res| {
                if let Ok(Some(value)) = res {
                    value == format!("value{}", key_id)
                } else {
                    false
                }
            })
        });

    assert!(
        stream::iter(gets)
            .buffer_unordered(8)
            .fold(true, |all, got| async move { got || all })
            .await
    );

    // reload from disk and test again
    let store = KvStore::open(temp_dir.path())?;

    let gets = (0u32..100)
        .flat_map(|thread_id| (0u32..100).map(move |i| (thread_id + i) % 100))
        .map(|key_id| {
            store.get(format!("key{}", key_id)).map(move |res| {
                if let Ok(Some(value)) = res {
                    value == format!("value{}", key_id)
                } else {
                    false
                }
            })
        });

    assert!(
        stream::iter(gets)
            .buffer_unordered(8)
            .fold(true, |all, got| async move { got || all })
            .await
    );

    Ok(())
}
