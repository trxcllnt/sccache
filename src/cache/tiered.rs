// Copyright 2016 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{
    cache::{Cache, CacheMode, PreprocessorCacheModeConfig, Storage},
    errors::*,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryFutureExt;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub struct TieredCache(
    // primary cache
    pub Arc<dyn Storage>,
    // secondary cache
    pub Arc<dyn Storage>,
);

impl TieredCache {
    pub fn create(primary: Arc<dyn Storage>, secondary: Arc<dyn Storage>) -> Arc<dyn Storage> {
        Arc::new(Self(primary, secondary)) as Arc<dyn Storage>
    }
}

#[async_trait]
impl Storage for TieredCache {
    async fn get(&self, key: &str) -> Result<Cache<Bytes>> {
        match self.0.get(key).await {
            Ok(Cache::Hit(entry)) => {
                if !self.1.has(key).await {
                    let _ = self.1.put(key, entry.clone()).await;
                }
                Ok(Cache::Hit(entry))
            }
            _ => match self.1.get(key).await {
                Ok(Cache::Hit(entry)) => {
                    let _ = self.0.put(key, entry.clone()).await;
                    Ok(Cache::Hit(entry))
                }
                res => res,
            },
        }
    }

    async fn del(&self, key: &str) -> Result<()> {
        tokio::join!(self.0.del(key), self.1.del(key)).0
    }

    async fn has(&self, key: &str) -> bool {
        if !self.0.has(key).await {
            self.1.has(key).await
        } else {
            true
        }
    }

    async fn put(&self, key: &str, entry: Bytes) -> Result<Duration> {
        let start = Instant::now();
        let res1 = self.0.put(key, entry.clone()).await;
        let res2 = self.1.put(key, entry.clone()).await;
        match (res1, res2) {
            (Err(err1), Err(err2)) => Err(anyhow!(
                "Failed to put key {key:?} (err1={err1}, err2={err2})"
            )),
            _ => Ok(Instant::now() - start),
        }
    }

    async fn size(&self, key: &str) -> Result<u64> {
        self.0.size(key).or_else(|_| self.1.size(key)).await
    }

    /// Check the cache capability.
    async fn check(&self) -> Result<CacheMode> {
        // Return ReadWrite if at least one of them is ReadWrite
        match (self.0.check().await?, self.1.check().await?) {
            (CacheMode::ReadWrite, _) => Ok(CacheMode::ReadWrite),
            (_, CacheMode::ReadWrite) => Ok(CacheMode::ReadWrite),
            _ => Ok(CacheMode::ReadOnly),
        }
    }

    /// Get the storage location.
    async fn location(&self) -> String {
        [self.0.location().await, self.1.location().await].join("\n")
    }

    /// Get the current storage usage, if applicable.
    async fn current_size(&self) -> Result<Option<u64>> {
        self.0.current_size().await
    }

    /// Get the maximum storage size, if applicable.
    async fn max_size(&self) -> Result<Option<u64>> {
        self.0.max_size().await
    }

    /// Return the config for preprocessor cache mode if applicable
    fn preprocessor_cache_mode_config(&self) -> PreprocessorCacheModeConfig {
        self.0.preprocessor_cache_mode_config()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::DiskCacheConfig;

    async fn make_disk_caches() -> Result<(
        Arc<dyn Storage>,
        Arc<dyn Storage>,
        Arc<dyn Storage>,
        Arc<dyn Storage>,
        Arc<dyn Storage>,
    )> {
        // Use disk cache.
        let tempdir = tempfile::Builder::new()
            .prefix("sccache_test_tiered_cache")
            .tempdir()
            .context("Failed to create tempdir")
            .unwrap();

        let cache1 = DiskCacheConfig {
            dir: tempdir.path().join("cache-1"),
            ..DiskCacheConfig::default()
        };
        let cache2 = DiskCacheConfig {
            dir: tempdir.path().join("cache-2"),
            ..DiskCacheConfig::default()
        };
        let cache3 = DiskCacheConfig {
            dir: tempdir.path().join("cache-3"),
            ..DiskCacheConfig::default()
        };

        tokio::try_join!(
            tokio::fs::create_dir(&cache1.dir),
            tokio::fs::create_dir(&cache2.dir),
            tokio::fs::create_dir(&cache3.dir),
        )?;

        let cache1 = Arc::<dyn Storage>::from(&cache1);
        let cache2 = Arc::<dyn Storage>::from(&cache2);
        let cache3 = Arc::<dyn Storage>::from(&cache3);
        let cache4 = TieredCache::create(cache2.clone(), cache1.clone());
        let cache5 = TieredCache::create(cache4.clone(), cache3.clone());
        Ok((cache1, cache2, cache3, cache4, cache5))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_write_read() -> Result<()> {
        let (cache1, cache2, cache3, cache4, cache5) = make_disk_caches().await?;
        // Test writing to cache5 writes to the other 4 caches
        cache5.put("key", "val".into()).await?;

        // Verify we can read "key" from each cache
        for cache in [&cache1, &cache2, &cache3, &cache4, &cache5] {
            let e = cache.get("key").await?;
            assert!(matches!(e, Cache::Hit(_)));
            if let Cache::Hit(e) = e {
                assert_eq!(e.to_vec(), "val".as_bytes());
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_write_del() -> Result<()> {
        let (cache1, cache2, cache3, cache4, cache5) = make_disk_caches().await?;

        // Test deleting from cache5 deletes from the other 4 caches
        cache5.put("key", "val".into()).await?;

        for cache in [&cache1, &cache2, &cache3, &cache4, &cache5] {
            let e = cache.get("key").await?;
            assert!(matches!(e, Cache::Hit(_)));
        }

        cache5.del("key").await?;

        for cache in [&cache1, &cache2, &cache3, &cache4, &cache5] {
            let e = cache.get("key").await?;
            assert!(matches!(e, Cache::Miss));
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_read_secondary() -> Result<()> {
        let (cache1, cache2, cache3, cache4, cache5) = make_disk_caches().await?;

        // Test reading from cache5 falls back to reading from secondary caches
        cache1.put("key", "val".into()).await?;

        for cache in [&cache5, &cache4, &cache3, &cache2, &cache1] {
            let e = cache.get("key").await?;
            assert!(matches!(e, Cache::Hit(_)));
        }

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_read_primary_propagates_to_secondary() -> Result<()> {
        let (cache1, cache2, cache3, cache4, cache5) = make_disk_caches().await?;

        // Test reading from cache5 falls back to reading from secondary caches
        cache5.put("key", "val".into()).await?;

        cache2.del("key").await?;
        cache3.del("key").await?;
        assert!(matches!(cache2.get("key").await?, Cache::Miss));
        assert!(matches!(cache3.get("key").await?, Cache::Miss));

        for cache in [&cache5, &cache4, &cache3, &cache2, &cache1] {
            let e = cache.get("key").await?;
            assert!(matches!(e, Cache::Hit(_)));
        }

        Ok(())
    }
}
