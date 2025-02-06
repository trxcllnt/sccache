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

use crate::cache::{Cache, CacheMode, CacheRead, CacheWrite, Storage};
use crate::compiler::PreprocessorCacheEntry;
use crate::lru_disk_cache::LruDiskCache;
use crate::lru_disk_cache::{Error as LruError, ReadSeek};
use async_trait::async_trait;
use futures::AsyncReadExt;
use futures::lock::Mutex;
use std::ffi::{OsStr, OsString};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::errors::*;

use super::{normalize_key, PreprocessorCacheModeConfig};

enum LazyDiskCache {
    Uninit { root: OsString, max_size: u64 },
    Init(LruDiskCache),
}

impl LazyDiskCache {
    fn get_or_init(&mut self) -> Result<&mut LruDiskCache> {
        match self {
            LazyDiskCache::Uninit { root, max_size } => {
                *self = LazyDiskCache::Init(LruDiskCache::new(&root, *max_size)?);
                self.get_or_init()
            }
            LazyDiskCache::Init(d) => Ok(d),
        }
    }

    fn get(&mut self) -> Option<&mut LruDiskCache> {
        match self {
            LazyDiskCache::Uninit { .. } => None,
            LazyDiskCache::Init(d) => Some(d),
        }
    }

    fn capacity(&self) -> u64 {
        match self {
            LazyDiskCache::Uninit { max_size, .. } => *max_size,
            LazyDiskCache::Init(d) => d.capacity(),
        }
    }

    fn path(&self) -> &Path {
        match self {
            LazyDiskCache::Uninit { root, .. } => root.as_ref(),
            LazyDiskCache::Init(d) => d.path(),
        }
    }
}

/// A cache that stores entries at local disk paths.
pub struct DiskCache {
    /// `LruDiskCache` does all the real work here.
    lru: Arc<Mutex<LazyDiskCache>>,
    preprocessor_cache_mode_config: PreprocessorCacheModeConfig,
    preprocessor_cache: Arc<Mutex<LazyDiskCache>>,
    rw_mode: CacheMode,
    root: PathBuf,
}

impl DiskCache {
    /// Create a new `DiskCache` rooted at `root`, with `max_size` as the maximum cache size on-disk, in bytes.
    pub fn new<T: AsRef<OsStr>>(
        root: T,
        max_size: u64,
        preprocessor_cache_mode_config: PreprocessorCacheModeConfig,
        rw_mode: CacheMode,
    ) -> DiskCache {
        DiskCache {
            lru: Arc::new(Mutex::new(LazyDiskCache::Uninit {
                root: root.as_ref().to_os_string(),
                max_size,
            })),
            preprocessor_cache_mode_config,
            preprocessor_cache: Arc::new(Mutex::new(LazyDiskCache::Uninit {
                root: Path::new(root.as_ref())
                    .join("preprocessor")
                    .into_os_string(),
                max_size,
            })),
            rw_mode,
            root: PathBuf::from(root.as_ref()),
        }
    }
}

/// Make a path to the cache entry with key `key`.
fn make_key_path(key: &str) -> PathBuf {
    Path::new(&key[0..1]).join(&key[1..2]).join(key)
}

#[async_trait]
impl Storage for DiskCache {
    async fn get(&self, key: &str) -> Result<Cache> {
        trace!("DiskCache::get({})", key);
        let file = match self.lru.lock().await.get_or_init()?.get(key) {
            Ok(file) => file,
            Err(LruError::FileNotInCache) => {
                trace!("DiskCache::get({}): FileNotInCache", key.to_owned());
                return Ok(Cache::Miss);
            }
            Err(LruError::Io(e)) => {
                trace!("DiskCache::get({}): IoError: {:?}", key.to_owned(), e);
                return Err(e.into());
            }
            Err(_) => unreachable!(),
        };
        Ok(Cache::Hit(CacheRead::from(file)?))
    }

    async fn get_stream(&self, key: &str) -> Result<Box<dyn futures::AsyncRead + Send + Unpin>> {
        // HACK: Ignore the LRU and assume the file exists
        trace!("DiskCache::get_stream({})", key);
        let path = self.root.join(make_key_path(key));
        let file = tokio::fs::File::open(path).await?;
        Ok(Box::new(file.compat()) as Box<dyn futures::AsyncRead + Unpin + Send>)
    }

    async fn del(&self, key: &str) -> Result<()> {
        match self.lru.lock().await.get_or_init() {
            Err(err) => Err(err),
            Ok(lru) => lru.remove(key).await.map_err(|e| e.into()),
        }
    }

    async fn has(&self, key: &str) -> bool {
        self.size(key).await.is_ok()
    }

    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration> {
        // We should probably do this on a background thread if we're going to buffer
        // everything in memory...
        trace!("DiskCache::finish_put({})", key);

        if self.rw_mode == CacheMode::ReadOnly {
            return Err(anyhow!("Cannot write to a read-only cache"));
        }

        let start = Instant::now();
        let v = entry.finish()?;
        let mut f = self
            .lru
            .lock()
            .await
            .get_or_init()?
            .prepare_add(key, v.len() as u64)?;

        futures::io::copy(
            &mut futures::io::AllowStdIo::new(v.reader()),
            &mut futures::io::AllowStdIo::new(f.as_file_mut()),
        )
        .await?;

        self.lru.lock().await.get_or_init()?.commit(f)?;

        Ok(start.elapsed())
    }

    async fn put_stream(
        &self,
        key: &str,
        size: u64,
        mut source: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<()> {
        if self.rw_mode == CacheMode::ReadOnly {
            return Err(anyhow!("Cannot write to a read-only cache"));
        }

        let mut f = self
            .lru
            .lock()
            .await
            .get_or_init()?
            .prepare_add(key, size)?;

        futures::io::copy(
            &mut source,
            &mut futures::io::AllowStdIo::new(f.as_file_mut()),
        )
        .await?;

        self.lru.lock().await.get_or_init()?.commit(f)?;

        Ok(())
    }

    }

    async fn check(&self) -> Result<CacheMode> {
        Ok(self.rw_mode)
    }

    async fn location(&self) -> String {
        format!("Local disk: {:?}", self.lru.lock().await.path())
    }

    async fn current_size(&self) -> Result<Option<u64>> {
        Ok(self.lru.lock().await.get().map(|l| l.size()))
    }

    async fn max_size(&self) -> Result<Option<u64>> {
        Ok(Some(self.lru.lock().await.capacity()))
    }
    fn preprocessor_cache_mode_config(&self) -> PreprocessorCacheModeConfig {
        self.preprocessor_cache_mode_config
    }

    async fn get_preprocessor_cache_entry(&self, key: &str) -> Result<Option<Box<dyn ReadSeek>>> {
        let key = normalize_key(key);
        Ok(self
            .preprocessor_cache
            .lock()
            .unwrap()
            .get_or_init()?
            .get(key)
            .ok())
    }
    async fn put_preprocessor_cache_entry(
        &self,
        key: &str,
        preprocessor_cache_entry: PreprocessorCacheEntry,
    ) -> Result<()> {
        if self.rw_mode == CacheMode::ReadOnly {
            return Err(anyhow!("Cannot write to a read-only cache"));
        }

        let key = normalize_key(key);
        let mut f = self
            .preprocessor_cache
            .lock()
            .unwrap()
            .get_or_init()?
            .prepare_add(key, 0)?;
        preprocessor_cache_entry.serialize_to(BufWriter::new(f.as_file_mut()))?;
        Ok(self
            .preprocessor_cache
            .lock()
            .unwrap()
            .get()
            .unwrap()
            .commit(f)?)
    }
}
