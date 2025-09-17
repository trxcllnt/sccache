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
use crate::lru_disk_cache::Error as LruError;
use crate::lru_disk_cache::LruDiskCache;
use async_trait::async_trait;
use bytes::Buf;
use futures::{lock::Mutex, AsyncWriteExt, StreamExt};
use futures::{SinkExt, TryStreamExt};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::errors::*;

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
    rw_mode: CacheMode,
    preprocessor_cache: Arc<Mutex<LazyDiskCache>>,
}

impl DiskCache {
    /// Create a new `DiskCache` rooted at `root`, with `max_size` as the maximum cache size on-disk, in bytes.
    pub fn new<T: AsRef<OsStr>>(root: T, max_size: u64, rw_mode: CacheMode) -> DiskCache {
        DiskCache {
            lru: Arc::new(Mutex::new(LazyDiskCache::Uninit {
                root: root.as_ref().to_os_string(),
                max_size,
            })),
            preprocessor_cache: Arc::new(Mutex::new(LazyDiskCache::Uninit {
                root: Path::new(root.as_ref())
                    .join("preprocessor")
                    .into_os_string(),
                max_size,
            })),
            rw_mode,
        }
    }

    async fn reader(&self, key: &str) -> Result<Box<dyn crate::cache::ReadSeek>> {
        trace!("DiskCache::read({})", key);
        match self.lru.lock().await.get_or_init()?.get(key) {
            Ok(reader) => Ok(reader),
            Err(LruError::Io(err)) => {
                trace!("DiskCache::read({key}): {err}");
                Err(err.into())
            }
            Err(err) => {
                trace!("DiskCache::read({key}): {err}");
                Err(err.into())
            }
        }
    }
}

impl DiskCache {
    pub async fn entry(&self, key: &str) -> Result<(PathBuf, u64)> {
        self.lru.lock().await.get_or_init().and_then(|lru| {
            let path = lru.key_to_abs_path(key);
            lru.get_size(key)
                .with_context(|| format!("[DiskCache::entry({key})]: {path:?}"))
                .map(|size| (path, size))
        })
    }

    pub async fn insert_with<F, Fut>(
        &self,
        key: &str,
        size: u64,
        with_fn: F,
    ) -> Result<(PathBuf, u64)>
    where
        F: FnOnce(&Path) -> Fut,
        Fut: std::future::Future<Output = std::io::Result<u64>>,
    {
        // Separate lru locks so concurrent insertions aren't serialized

        let tmp = self
            .lru
            .lock()
            .await
            .get_or_init()?
            .prepare_dir(key, size)
            .with_context(|| format!("[DiskCache::insert_with({key}, {size})]"))?;

        let tmp_path = tmp.as_path();

        // Do the actual I/O now that the LRU lock dropped
        let res = with_fn(tmp_path).await.map_err(anyhow::Error::new);

        // Clean up and commit the dir
        self.lru
            .lock()
            .await
            .get_or_init()?
            .commit_dir(res.as_ref().map(|_| &tmp).map_err(|_| &tmp))
            .await
            .or_else(|err| {
                if let LruError::FileNotInCache = err {
                    // Usual case: return the original (possibly I/O) error.
                    // Map successful results to this function's return signature,
                    // even though  `FileNotInCache` means `res` will always be Err
                    res.map(|_| (PathBuf::new(), size))
                } else {
                    // Unusual case: encountered an IO error committing the directory
                    Err(err.into())
                }
            })
            .with_context(|| format!("[DiskCache::insert_with({key}, {size})]: {tmp_path:?}"))
    }
}

#[async_trait]
impl Storage for DiskCache {
    async fn get(&self, key: &str) -> Result<Cache> {
        match self.reader(key).await {
            Ok(read) => Ok(Cache::Hit(CacheRead::from(read)?)),
            Err(err) => match err.downcast_ref::<LruError>() {
                Some(LruError::FileNotInCache) => Ok(Cache::Miss),
                _ => Err(err),
            },
        }
    }

    async fn get_async_reader(
        &self,
        key: &str,
    ) -> Result<Box<dyn futures::AsyncRead + Send + Unpin>> {
        let reader = futures::io::AllowStdIo::new(
            self.reader(key)
                .await
                .with_context(|| format!("[DiskCache::get_async_reader({key})]"))?,
        );
        Ok(Box::new(reader))
    }

    async fn get_byte_stream(
        &self,
        key: &str,
    ) -> Result<Box<dyn futures::Stream<Item = Result<bytes::Bytes>> + Send + Unpin>> {
        use tokio_util::compat::FuturesAsyncReadCompatExt;
        let reader = futures::io::AllowStdIo::new(
            self.reader(key)
                .await
                .with_context(|| format!("[DiskCache::get_byte_stream({key})]"))?,
        );
        let stream = tokio_util::io::ReaderStream::new(reader.compat());
        Ok(Box::new(stream.map_err(|e| e.into())))
    }

    async fn del(&self, key: &str) -> Result<()> {
        match self.lru.lock().await.get_or_init() {
            Err(err) => Err(err),
            Ok(lru) => lru
                .remove(key)
                .await
                .with_context(|| format!("[DiskCache::del({key})]")),
        }
    }

    async fn has(&self, key: &str) -> bool {
        self.size(key).await.is_ok()
    }

    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration> {
        // We should probably do this on a background thread if we're going to buffer
        // everything in memory...
        trace!("DiskCache::put({})", key);

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
            .prepare_add(key, v.len() as u64)
            .with_context(|| format!("[DiskCache::put({key})]"))?;

        futures::io::copy(
            &mut futures::io::AllowStdIo::new(v.reader()),
            &mut futures::io::AllowStdIo::new(f.as_file_mut()),
        )
        .await
        .with_context(|| format!("[DiskCache::put({key})]"))?;

        self.lru
            .lock()
            .await
            .get_or_init()?
            .commit(f)
            .with_context(|| format!("[DiskCache::put({key})]"))?;

        Ok(start.elapsed())
    }

    async fn put_async_reader(
        &self,
        key: &str,
        size: u64,
        source: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<()> {
        if self.rw_mode == CacheMode::ReadOnly {
            return Err(anyhow!("Cannot write to a read-only cache"));
        }

        let mut f = self
            .lru
            .lock()
            .await
            .get_or_init()?
            .prepare_add(key, size)
            .with_context(|| format!("[DiskCache::put_async_reader({key})]"))?;

        futures::io::copy(source, &mut futures::io::AllowStdIo::new(f.as_file_mut()))
            .await
            .with_context(|| format!("[DiskCache::put_async_reader({key})]"))?;

        self.lru
            .lock()
            .await
            .get_or_init()?
            .commit(f)
            .with_context(|| format!("[DiskCache::put_async_reader({key})]"))?;

        Ok(())
    }

    async fn put_byte_stream(
        &self,
        key: &str,
        size: u64,
        source: Pin<&mut (dyn futures::Stream<Item = Result<bytes::Bytes>> + Send)>,
    ) -> Result<()> {
        if self.rw_mode == CacheMode::ReadOnly {
            return Err(anyhow!("Cannot write to a read-only cache"));
        }

        let mut f = self
            .lru
            .lock()
            .await
            .get_or_init()?
            .prepare_add(key, size)
            .with_context(|| format!("[DiskCache::put_byte_stream({key})]"))?;

        source
            .forward(
                futures::io::AllowStdIo::new(f.as_file_mut())
                    .into_sink()
                    .sink_err_into(),
            )
            .await
            .with_context(|| format!("[DiskCache::put_byte_stream({key})]"))?;

        self.lru
            .lock()
            .await
            .get_or_init()?
            .commit(f)
            .with_context(|| format!("[DiskCache::put_byte_stream({key})]"))?;

        Ok(())
    }

    async fn size(&self, key: &str) -> Result<u64> {
        self.lru.lock().await.get_or_init().and_then(|lru| {
            lru.get_size(key)
                .with_context(|| format!("[DiskCache::size({key})]"))
        })
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

    /// Return the preprocessor cache entry for a given preprocessor key,
    /// if it exists.
    /// Only applicable when using preprocessor cache mode.
    async fn get_preprocessor_cache_entry(&self, key: &str) -> Option<PreprocessorCacheEntry> {
        if let Some(mut file) = self
            .preprocessor_cache
            .lock()
            .await
            .get_or_init()
            .ok()
            .and_then(|lru| lru.get(key).ok())
        {
            let mut buf = vec![];
            file.read_to_end(&mut buf).ok()?;
            PreprocessorCacheEntry::read(&buf).ok()
        } else {
            None
        }
    }

    /// Insert a preprocessor cache entry at the given preprocessor key,
    /// overwriting the entry if it exists.
    /// Only applicable when using preprocessor cache mode.
    async fn put_preprocessor_cache_entry(
        &self,
        key: &str,
        preprocessor_cache_entry: PreprocessorCacheEntry,
    ) -> Result<()> {
        if self.rw_mode == CacheMode::ReadOnly {
            return Err(anyhow!("Cannot write to a read-only cache"));
        }

        let mut f = self
            .preprocessor_cache
            .lock()
            .await
            .get_or_init()?
            .prepare_add(key, 0)
            .with_context(|| format!("[DiskCache::put_preprocessor_cache_entry({key})]"))?;

        preprocessor_cache_entry
            .serialize_to(std::io::BufWriter::new(f.as_file_mut()))
            .with_context(|| format!("[DiskCache::put_preprocessor_cache_entry({key})]"))?;

        self.preprocessor_cache
            .lock()
            .await
            .get_or_init()?
            .commit(f)
            .with_context(|| format!("[DiskCache::put_preprocessor_cache_entry({key})]"))
    }
}
