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
    cache::{disk::DiskCache, readonly::ReadOnlyStorage, tiered::TieredCache},
    config::{
        AzureCacheConfig, CacheType, DEFAULT_REDIS_DB, DiskCacheConfig, GCSCacheConfig,
        GHACacheConfig, MemcachedCacheConfig, OSSCacheConfig, RedisCacheConfig, S3CacheConfig,
        WebdavCacheConfig,
    },
    errors::*,
};

use async_trait::async_trait;
use fs_err as fs;
use futures::{AsyncReadExt, FutureExt, StreamExt, TryFutureExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{self, Cursor, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use zip::write::FileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

#[cfg(feature = "azure")]
use crate::cache::azure::AzureBlobCache;
#[cfg(feature = "gcs")]
use crate::cache::gcs::GCSCache;
#[cfg(feature = "gha")]
use crate::cache::gha::GHACache;
#[cfg(feature = "memcached")]
use crate::cache::memcached::MemcachedCache;
#[cfg(feature = "oss")]
use crate::cache::oss::OSSCache;
#[cfg(feature = "redis")]
use crate::cache::redis::RedisCache;
#[cfg(feature = "s3")]
use crate::cache::s3::S3Cache;
#[cfg(feature = "watcher")]
use crate::cache::watch::WatchStorage;
#[cfg(feature = "webdav")]
use crate::cache::webdav::WebdavCache;
#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss"
))]
use {crate::util::retry_with_jitter, futures::AsyncWriteExt, tokio_retry2::RetryError};

#[cfg(unix)]
fn get_file_mode(file: &fs::File) -> Result<Option<u32>> {
    use std::os::unix::fs::MetadataExt;
    Ok(Some(file.metadata()?.mode()))
}

#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)]
fn get_file_mode(_file: &fs::File) -> Result<Option<u32>> {
    Ok(None)
}

#[cfg(unix)]
fn set_file_mode(path: &Path, mode: u32) -> Result<()> {
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    let p = Permissions::from_mode(mode);
    fs::set_permissions(path, p)?;
    Ok(())
}

#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)]
fn set_file_mode(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

/// Cache object sourced by a file.
#[derive(Clone)]
pub struct FileObjectSource {
    /// Identifier for this object. Should be unique within a compilation unit.
    /// Note that a compilation unit is a single source file in C/C++ and a crate in Rust.
    pub key: String,
    /// Absolute path to the file.
    pub path: PathBuf,
    /// Whether the file must be present on disk and is essential for the compilation.
    pub optional: bool,
    /// Whether the file size must be greater than 0 bytes.
    pub must_be_non_empty: bool,
}

/// Result of a cache lookup.
#[derive(Eq, PartialEq)]
pub enum Cache<T> {
    /// Result was found in cache.
    Hit(T),
    /// Result was not found in cache.
    Miss,
}

impl<T> fmt::Debug for Cache<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Cache::Hit(_) => write!(f, "Cache::Hit(...)"),
            Cache::Miss => write!(f, "Cache::Miss"),
        }
    }
}

/// CacheMode is used to represent which mode we are using.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CacheMode {
    /// Only read cache from storage.
    ReadOnly,
    /// Full support of cache storage: read and write.
    ReadWrite,
}

/// Trait objects can't be bounded by more than one non-builtin trait.
pub trait ReadSeek: Read + Seek + Send {}
pub trait AsyncReadSeek: futures::AsyncRead + futures::AsyncSeek + Send {}

impl<T: Read + Seek + Send> ReadSeek for T {}
impl<T: futures::AsyncRead + futures::AsyncSeek + Send> AsyncReadSeek for T {}

/// Data stored in the compiler cache.
pub struct CacheRead {
    zip: ZipArchive<Box<dyn ReadSeek>>,
}

/// Represents a failure to decompress stored object data.
#[derive(Debug)]
pub struct DecompressionFailure {
    message: String,
}

impl std::fmt::Display for DecompressionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for DecompressionFailure {}

/// Represents a failure to decompress stored object data.
#[derive(Debug)]
pub struct UnexpectedFileSize(pub u64, pub u64);

impl std::fmt::Display for UnexpectedFileSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Expected {} bytes, but unpacked {}", self.0, self.1)
    }
}

impl std::error::Error for UnexpectedFileSize {}

/// Error that indicates we expected a file to be larger than 0 bytes.
#[derive(Debug)]
pub struct UnexpectedEmptyFile(pub PathBuf);

impl std::fmt::Display for UnexpectedEmptyFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Expected {:?} to be larger than 0 bytes", self.0)
    }
}

impl std::error::Error for UnexpectedEmptyFile {}

impl CacheRead {
    /// Create a cache entry from `reader`.
    pub fn from<R>(reader: R) -> Result<CacheRead>
    where
        R: ReadSeek + 'static,
    {
        let z = ZipArchive::new(Box::new(reader) as Box<dyn ReadSeek>)
            .context("Failed to parse cache entry")?;
        Ok(CacheRead { zip: z })
    }

    /// Get an object from this cache entry at `name` and write it to `to`.
    /// If the file has stored permissions, return them.
    pub fn get_object<T>(&mut self, name: &str, to: &mut T) -> Result<Option<u32>>
    where
        T: Write,
    {
        let file = self.zip.by_name(name).or(Err(DecompressionFailure {
            message: format!("Failed to decompress {name:?}"),
        }))?;
        if file.compression() != CompressionMethod::Stored {
            bail!(DecompressionFailure {
                message: format!(
                    "Compression method must be {:?}, received {:?}",
                    CompressionMethod::Stored,
                    file.compression()
                )
            });
        }
        let mode = file.unix_mode();
        zstd::stream::copy_decode(file, to).or(Err(DecompressionFailure {
            message: format!("Failed to decompress {name:?}"),
        }))?;
        Ok(mode)
    }

    /// Get the stdout from this cache entry, if it exists.
    pub fn get_stdout(&mut self) -> Vec<u8> {
        self.get_bytes("stdout")
    }

    /// Get the stderr from this cache entry, if it exists.
    pub fn get_stderr(&mut self) -> Vec<u8> {
        self.get_bytes("stderr")
    }

    pub fn get_bytes(&mut self, name: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        drop(self.get_object(name, &mut bytes));
        bytes
    }

    pub fn into_inner(self) -> Box<dyn ReadSeek> {
        self.zip.into_inner()
    }

    pub async fn extract_objects<T>(
        mut self,
        objects: T,
        pool: &tokio::runtime::Handle,
    ) -> Result<()>
    where
        T: IntoIterator<Item = FileObjectSource> + Send + Sync + 'static,
    {
        pool.spawn_blocking(move || {
            for FileObjectSource {
                key,
                path,
                optional,
                must_be_non_empty,
            } in objects
            {
                let dir = match path.parent() {
                    Some(d) => d,
                    None => bail!("Output file without a parent directory!"),
                };
                // Write the cache entry to a tempfile and then atomically
                // move it to its final location so that other rustc invocations
                // happening in parallel don't see a partially-written file.
                let mut tmp = NamedTempFile::new_in(dir)?;
                match (self.get_object(&key, &mut tmp), optional) {
                    (Ok(mode), _) => {
                        if must_be_non_empty {
                            let size_on_disk = tmp
                                .flush()
                                .and_then(|_| tmp.as_file_mut().metadata())?
                                .len();
                            if size_on_disk == 0 {
                                bail!(anyhow!(UnexpectedEmptyFile(path)));
                            }
                        }

                        let file = match tmp.persist(&path) {
                            Ok(file) => {
                                if let Some(mode) = mode {
                                    set_file_mode(&path, mode)?;
                                }
                                // Sync immediately so other readers see the file.
                                // This is important for nvcc's .module_id files,
                                // which are read during many pararllel compilations.
                                file.sync_all().map(|_| file)
                            }
                            Err(err) => Err(err.error),
                        }?;

                        if must_be_non_empty {
                            let size_on_disk = file.metadata()?.len();
                            if size_on_disk == 0 {
                                bail!(anyhow!(UnexpectedEmptyFile(path)));
                            }
                        }
                    }
                    (Err(e), false) => return Err(e),
                    // skip if no object found and it's optional
                    (Err(_), true) => continue,
                }
            }
            Ok(())
        })
        .await?
    }
}

/// Data to be stored in the compiler cache.
pub struct CacheWrite {
    zip: ZipWriter<io::Cursor<Vec<u8>>>,
}

impl CacheWrite {
    /// Create a new, empty cache entry.
    pub fn new() -> CacheWrite {
        CacheWrite {
            zip: ZipWriter::new(io::Cursor::new(vec![])),
        }
    }

    /// Create a new cache entry populated with the contents of `objects`.
    pub async fn from_objects<T>(objects: T, pool: &tokio::runtime::Handle) -> Result<CacheWrite>
    where
        T: IntoIterator<Item = FileObjectSource> + Send + Sync + 'static,
    {
        pool.spawn_blocking(move || {
            let mut entry = CacheWrite::new();
            for FileObjectSource {
                key,
                path,
                optional,
                must_be_non_empty,
            } in objects
            {
                let file = fs::File::open(&path)
                    .with_context(|| format!("failed to open file `{path:?}`"));
                match (file, optional) {
                    (Ok(mut file), _) => {
                        if must_be_non_empty {
                            let size_on_disk = file.metadata()?.len();
                            if size_on_disk == 0 {
                                bail!(anyhow!(UnexpectedEmptyFile(path)));
                            }
                        }
                        let mode = get_file_mode(&file)?;
                        entry.put_object(&key, &mut file, mode).with_context(|| {
                            format!("failed to put object `{path:?}` in cache entry")
                        })?;
                    }
                    (Err(e), false) => return Err(e),
                    (Err(_), true) => continue,
                }
            }
            Ok(entry)
        })
        .await?
    }

    /// Add an object containing the contents of `from` to this cache entry at `name`.
    /// If `mode` is `Some`, store the file entry with that mode.
    pub fn put_object<T>(&mut self, name: &str, from: &mut T, mode: Option<u32>) -> Result<()>
    where
        T: Read,
    {
        // We're going to declare the compression method as "stored",
        // but we're actually going to store zstd-compressed blobs.
        let opts = FileOptions::default().compression_method(CompressionMethod::Stored);
        let opts = if let Some(mode) = mode {
            opts.unix_permissions(mode)
        } else {
            opts
        };
        self.zip
            .start_file(name, opts)
            .context("Failed to start cache entry object")?;

        let compression_level = std::env::var("SCCACHE_CACHE_ZSTD_LEVEL")
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(3);
        zstd::stream::copy_encode(from, &mut self.zip, compression_level)?;
        Ok(())
    }

    pub fn put_stdout(&mut self, bytes: &[u8]) -> Result<()> {
        self.put_bytes("stdout", bytes)
    }

    pub fn put_stderr(&mut self, bytes: &[u8]) -> Result<()> {
        self.put_bytes("stderr", bytes)
    }

    fn put_bytes(&mut self, name: &str, bytes: &[u8]) -> Result<()> {
        if !bytes.is_empty() {
            let mut cursor = Cursor::new(bytes);
            return self.put_object(name, &mut cursor, None);
        }
        Ok(())
    }

    /// Finish writing data to the cache entry writer, and return the data.
    pub fn finish(self) -> Result<Vec<u8>> {
        let CacheWrite { mut zip } = self;
        let cur = zip.finish().context("Failed to finish cache entry zip")?;
        Ok(cur.into_inner())
    }
}

impl Default for CacheWrite {
    fn default() -> Self {
        Self::new()
    }
}

/// An interface to cache storage.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Get a cache entry by `key`.
    ///
    /// If an error occurs, this method should return a `Cache::Error`.
    /// If nothing fails but the entry is not found in the cache,
    /// it should return a `Cache::Miss`.
    /// If the entry is successfully found in the cache, it should
    /// return a `Cache::Hit`.
    async fn get(&self, key: &str) -> Result<Cache<Box<dyn ReadSeek>>>;

    async fn get_async_reader(&self, key: &str) -> Result<Cache<Box<dyn AsyncReadSeek + Unpin>>>;

    /// Delete the cache entry for `key`.
    async fn del(&self, key: &str) -> Result<()>;

    /// Check if the cache has an entry for `key`.
    ///
    /// If the entry is successfully found in the cache, return true.
    /// If an error occurs, or the entry is not found in the cache, return false.
    async fn has(&self, key: &str) -> bool;

    /// Put `entry` in the cache under `key`.
    ///
    /// Returns a `Future` that will provide the result or error when the put is
    /// finished.
    async fn put(&self, key: &str, entry: &mut dyn ReadSeek) -> Result<Duration>;

    async fn put_async_reader(
        &self,
        key: &str,
        size: u64,
        source: &mut (dyn AsyncReadSeek + Unpin),
    ) -> Result<()>;

    async fn size(&self, key: &str) -> Result<u64>;

    /// Check the cache capability.
    ///
    /// - `Ok(CacheMode::ReadOnly)` means cache can only be used to `get`
    ///   cache.
    /// - `Ok(CacheMode::ReadWrite)` means cache can do both `get` and `put`.
    /// - `Err(err)` means cache is not setup correctly or not match with
    ///   users input (for example, user try to use `ReadWrite` but cache
    ///   is `ReadOnly`).
    ///
    /// We will provide a default implementation which returns
    /// `Ok(CacheMode::ReadWrite)` for service that doesn't
    /// support check yet.
    async fn check(&self) -> Result<CacheMode> {
        Ok(CacheMode::ReadWrite)
    }

    /// Get the storage location.
    async fn location(&self) -> String;

    /// Get the current storage usage, if applicable.
    async fn current_size(&self) -> Result<Option<u64>>;

    /// Get the maximum storage size, if applicable.
    async fn max_size(&self) -> Result<Option<u64>>;

    /// Return the config for preprocessor cache mode if applicable
    fn preprocessor_cache_mode_config(&self) -> PreprocessorCacheModeConfig {
        PreprocessorCacheModeConfig::default()
    }
}

/// Configuration switches for preprocessor cache mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(default)]
pub struct PreprocessorCacheModeConfig {
    /// Whether to use preprocessor cache mode entirely
    pub use_preprocessor_cache_mode: bool,
    /// If false (default), only compare header files by hashing their contents.
    /// If true, will use size + ctime + mtime to check whether a file has changed.
    /// See other flags below for more control over this behavior.
    pub file_stat_matches: bool,
    /// If true (default), uses the ctime (file status change on UNIX,
    /// creation time on Windows) to check that a file has/hasn't changed.
    /// Can be useful to disable when backdating modification times
    /// in a controlled manner.
    pub use_ctime_for_stat: bool,
    /// If true, ignore `__DATE__`, `__TIME__` and `__TIMESTAMP__` being present
    /// in the source code. Will speed up preprocessor cache mode,
    /// but can result in false positives.
    pub ignore_time_macros: bool,
    /// If true, preprocessor cache mode will not cache system headers, only
    /// add them to the hash.
    pub skip_system_headers: bool,
    /// The prefix in cache storage where the preprocessor cache files are stored.
    /// For example, the disk preprocessor cache will be `$SCCACHE_DIR/{key_prefix}`,
    /// the S3 preprocessor cache files will be under `$SCCACHE_BUCKET/{key_prefix}`,
    /// etc.
    pub key_prefix: String,
}

impl Default for PreprocessorCacheModeConfig {
    fn default() -> Self {
        Self {
            use_preprocessor_cache_mode: false,
            file_stat_matches: false,
            use_ctime_for_stat: true,
            ignore_time_macros: false,
            skip_system_headers: false,
            key_prefix: "preprocessor".into(),
        }
    }
}

impl PreprocessorCacheModeConfig {
    /// Return a default [`Self`], but with the cache active.
    pub fn activated() -> Self {
        Self {
            use_preprocessor_cache_mode: true,
            ..Default::default()
        }
    }
}

#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss"
))]
mod operator {
    use super::*;

    fn to_retry_err(kind: &str, err: opendal::Error) -> RetryError<opendal::Error> {
        match err.kind() {
            opendal::ErrorKind::NotFound => {
                trace!("cache {kind} error (permanent): {err}");
                RetryError::permanent(err)
            }
            opendal::ErrorKind::PermissionDenied => {
                trace!("cache {kind} error (permanent): {err}");
                RetryError::permanent(err)
            }
            opendal::ErrorKind::RateLimited => {
                trace!("cache {kind} error (transient): {err}");
                RetryError::transient(err)
            }
            opendal::ErrorKind::Unsupported => {
                trace!("cache {kind} error (permanent): {err}");
                RetryError::permanent(err)
            }
            opendal::ErrorKind::Unexpected => {
                if err.is_temporary() {
                    debug!("cache {kind} error (transient): {err}");
                    RetryError::transient(err)
                } else {
                    warn!("cache {kind} error (permanent): {err:?}");
                    RetryError::permanent(err)
                }
            }
            _ => {
                warn!("cache {kind} error (permanent): {err:?}");
                RetryError::permanent(err)
            }
        }
    }

    pub async fn read_with_retry(
        storage: &opendal::Operator,
        key: &str,
    ) -> std::result::Result<opendal::Buffer, opendal::Error> {
        // TODO: Allow configuring the number of retries
        retry_with_jitter(usize::MAX, || async {
            storage
                .read(&normalize_key(key))
                .await
                .map_err(|e| to_retry_err("lookup", e))
        })
        .await
    }

    pub async fn async_reader_with_retry(
        storage: &opendal::Operator,
        key: &str,
    ) -> std::result::Result<opendal::Reader, opendal::Error> {
        // TODO: Allow configuring the number of retries
        retry_with_jitter(usize::MAX, || async {
            storage
                .reader(&normalize_key(key))
                .await
                .map_err(|e| to_retry_err("lookup", e))
        })
        .await
    }

    pub async fn write_with_retry(
        storage: &opendal::Operator,
        key: &str,
        buf: bytes::Bytes,
    ) -> std::result::Result<opendal::Metadata, opendal::Error> {
        // TODO: Allow configuring the number of retries
        retry_with_jitter(usize::MAX, || async {
            storage
                .write(&normalize_key(key), buf.clone())
                .await
                .map_err(|e| to_retry_err("write", e))
        })
        .await
    }

    pub async fn async_writer_with_retry(
        storage: &opendal::Operator,
        key: &str,
    ) -> std::result::Result<opendal::Writer, opendal::Error> {
        // TODO: Allow configuring the number of retries
        retry_with_jitter(usize::MAX, || async {
            storage
                .writer(&normalize_key(key))
                .await
                .map_err(|e| to_retry_err("write", e))
        })
        .await
    }
}

/// Implement storage for operator.
#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss"
))]
#[async_trait]
impl Storage for opendal::Operator {
    async fn get(&self, key: &str) -> Result<Cache<Box<dyn ReadSeek>>> {
        match operator::read_with_retry(self, key).await {
            Ok(res) => Ok(Cache::Hit(Box::new(io::Cursor::new(res.to_bytes())))),
            Err(err) => match err.kind() {
                opendal::ErrorKind::NotFound => Ok(Cache::Miss),
                _ => Err(err.into()),
            },
        }
    }

    async fn get_async_reader(&self, key: &str) -> Result<Cache<Box<dyn AsyncReadSeek + Unpin>>> {
        let reader = match operator::async_reader_with_retry(self, key).await {
            Ok(reader) => reader,
            Err(err) => match err.kind() {
                opendal::ErrorKind::NotFound => return Ok(Cache::Miss),
                _ => return Err(err.into()),
            },
        };
        let mut buffer = vec![];
        reader
            .into_futures_async_read(..)
            .await?
            .read_to_end(&mut buffer)
            .await?;
        Ok(Cache::Hit(Box::new(futures::io::Cursor::new(buffer))))
    }

    async fn del(&self, key: &str) -> Result<()> {
        self.delete(&normalize_key(key))
            .await
            .map_err(anyhow::Error::new)
    }

    async fn has(&self, key: &str) -> bool {
        self.stat(&normalize_key(key)).await.is_ok()
    }

    async fn put(&self, key: &str, entry: &mut dyn ReadSeek) -> Result<Duration> {
        let start = std::time::Instant::now();

        let mut b = vec![];
        entry.read_to_end(&mut b)?;
        let b = bytes::Bytes::from(b);
        operator::write_with_retry(self, key, b).await?;

        Ok(start.elapsed())
    }

    async fn put_async_reader(
        &self,
        key: &str,
        _size: u64,
        source: &mut (dyn AsyncReadSeek + Unpin),
    ) -> Result<()> {
        let mut sink = operator::async_writer_with_retry(self, key)
            .await?
            .into_futures_async_write();
        if let Err(err) = futures::io::copy(source, &mut sink).await {
            sink.close().await?;
            Err(err.into())
        } else {
            sink.close().await?;
            Ok(())
        }
    }

    async fn size(&self, key: &str) -> Result<u64> {
        Ok(self.stat_with(&normalize_key(key)).await?.content_length())
    }

    async fn check(&self) -> Result<CacheMode> {
        use opendal::ErrorKind;

        let path = ".sccache_check";

        // Read is required, return error directly if we can't read .
        match self.read(path).await {
            Ok(_) => (),
            // Read not exist file with not found is ok.
            Err(err) if err.kind() == ErrorKind::NotFound => (),
            // Tricky Part.
            //
            // We tolerate rate limited here to make sccache keep running.
            // For the worse case, we will miss all the cache.
            //
            // In some super rare cases, user could configure storage in wrong
            // and hitting other services rate limit. There are few things we
            // can do, so we will print our the error here to make users know
            // about it.
            Err(err) if err.kind() == ErrorKind::RateLimited => {
                eprintln!("cache storage read check: {err:?}, but we decide to keep running")
            }
            Err(err) => bail!("cache storage failed to read: {:?}", err),
        };

        let can_write = match self.write(path, "Hello, World!").await {
            Ok(_) => true,
            Err(err) if err.kind() == ErrorKind::AlreadyExists => true,
            // Tolerate all other write errors because we can do read at least.
            Err(err) => {
                eprintln!("storage write check failed: {err:?}");
                false
            }
        };

        let mode = if can_write {
            CacheMode::ReadWrite
        } else {
            CacheMode::ReadOnly
        };

        debug!("storage check result: {mode:?}");

        Ok(mode)
    }

    async fn location(&self) -> String {
        let meta = self.info();
        format!(
            "{}, name: {}, prefix: {}",
            meta.scheme(),
            meta.name(),
            meta.root()
        )
    }

    async fn current_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn max_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }
}

#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss",
    test
))]
/// Normalize key `abcdef` into `a/b/c/abcdef`
pub(in crate::cache) fn normalize_key(key: &str) -> String {
    format!("{}/{}/{}/{}", &key[0..1], &key[1..2], &key[2..3], &key)
}

struct QueuedRequests {
    inner: Arc<dyn Storage>,
    queue: Arc<tokio::sync::Semaphore>,
}

// Opendal memcached and redis use connection pools with
// non-configurable max sizes and connection timeouts of 30s.
// Limit the number of concurrent requests when using those storage backends.
impl QueuedRequests {
    pub fn create(inner: Arc<dyn Storage>, max_size: usize) -> Arc<dyn Storage> {
        Arc::new(Self {
            inner,
            queue: Arc::new(tokio::sync::Semaphore::new(max_size)),
        })
    }
}

#[async_trait]
impl Storage for QueuedRequests {
    async fn get(&self, key: &str) -> Result<Cache<Box<dyn ReadSeek>>> {
        let _permit = self.queue.acquire().await?;
        self.inner.get(key).await
    }

    async fn get_async_reader(&self, key: &str) -> Result<Cache<Box<dyn AsyncReadSeek + Unpin>>> {
        let _permit = self.queue.acquire().await?;
        self.inner.get_async_reader(key).await
    }

    async fn del(&self, key: &str) -> Result<()> {
        let _permit = self.queue.acquire().await?;
        self.inner.del(key).await
    }

    async fn has(&self, key: &str) -> bool {
        let permit = self.queue.acquire().await;
        if permit.is_ok() {
            self.inner.has(key).await
        } else {
            false
        }
    }

    async fn put(&self, key: &str, entry: &mut dyn ReadSeek) -> Result<Duration> {
        let _permit = self.queue.acquire().await?;
        self.inner.put(key, entry).await
    }

    async fn put_async_reader(
        &self,
        key: &str,
        size: u64,
        source: &mut (dyn AsyncReadSeek + Unpin),
    ) -> Result<()> {
        let _permit = self.queue.acquire().await?;
        self.inner.put_async_reader(key, size, source).await
    }

    async fn size(&self, key: &str) -> Result<u64> {
        let _permit = self.queue.acquire().await?;
        self.inner.size(key).await
    }

    /// Check the cache capability.
    async fn check(&self) -> Result<CacheMode> {
        self.inner.check().await
    }

    /// Get the storage location.
    async fn location(&self) -> String {
        self.inner.location().await
    }

    /// Get the current storage usage, if applicable.
    async fn current_size(&self) -> Result<Option<u64>> {
        self.inner.current_size().await
    }

    /// Get the maximum storage size, if applicable.
    async fn max_size(&self) -> Result<Option<u64>> {
        self.inner.max_size().await
    }

    /// Return the config for preprocessor cache mode if applicable
    fn preprocessor_cache_mode_config(&self) -> PreprocessorCacheModeConfig {
        self.inner.preprocessor_cache_mode_config()
    }
}

pub struct PreprocessorCache(pub Arc<dyn Storage>, pub PreprocessorCacheModeConfig);

impl PreprocessorCache {
    pub fn create(storage: Arc<dyn Storage>, cfg: PreprocessorCacheModeConfig) -> Arc<dyn Storage> {
        Arc::new(PreprocessorCache(storage, cfg))
    }
}

#[async_trait]
impl Storage for PreprocessorCache {
    async fn get(&self, key: &str) -> Result<Cache<Box<dyn ReadSeek>>> {
        self.0.get(key).await
    }

    async fn get_async_reader(&self, key: &str) -> Result<Cache<Box<dyn AsyncReadSeek + Unpin>>> {
        self.0.get_async_reader(key).await
    }

    async fn del(&self, key: &str) -> Result<()> {
        self.0.del(key).await
    }

    async fn has(&self, key: &str) -> bool {
        self.0.has(key).await
    }

    async fn put(&self, key: &str, entry: &mut dyn ReadSeek) -> Result<Duration> {
        self.0.put(key, entry).await
    }

    async fn put_async_reader(
        &self,
        key: &str,
        size: u64,
        source: &mut (dyn AsyncReadSeek + Unpin),
    ) -> Result<()> {
        self.0.put_async_reader(key, size, source).await
    }

    async fn size(&self, key: &str) -> Result<u64> {
        self.0.size(key).await
    }

    /// Check the cache capability.
    async fn check(&self) -> Result<CacheMode> {
        self.0.check().await
    }

    /// Get the storage location.
    async fn location(&self) -> String {
        self.0.location().await
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
        self.1.clone()
    }
}

#[derive(Default)]
struct StorageBuilder {
    create_storage: Option<Box<dyn Fn() -> Result<Arc<dyn Storage>> + Send>>,
    max_concurrent_requests: Option<usize>,
    preprocessor_cache_mode: Option<PreprocessorCacheModeConfig>,
    watch_paths: Vec<PathBuf>,
}

impl StorageBuilder {
    pub fn create_storage<F: Fn() -> Result<Arc<dyn Storage>> + Send + 'static>(
        self,
        factory: F,
    ) -> Self {
        Self {
            create_storage: Some(Box::new(factory)),
            ..self
        }
    }

    pub fn preprocessor_cache_mode<P: Into<Option<PreprocessorCacheModeConfig>>>(
        self,
        config: P,
    ) -> Self {
        Self {
            preprocessor_cache_mode: config.into(),
            ..self
        }
    }

    pub fn max_concurrent_requests<P: Into<Option<usize>>>(self, limit: P) -> Self {
        Self {
            max_concurrent_requests: limit.into(),
            ..self
        }
    }

    pub fn watch_paths<P: ToOwned<Owned = Vec<PathBuf>>>(self, paths: P) -> Self {
        Self {
            watch_paths: paths.to_owned(),
            ..self
        }
    }

    pub async fn build(self) -> Result<Arc<dyn Storage>> {
        let Self {
            create_storage,
            max_concurrent_requests,
            preprocessor_cache_mode,
            #[allow(unused_variables)]
            watch_paths,
        } = self;

        let create_storage = create_storage.expect("create_storage should exist");

        let create_storage = move || {
            let storage = create_storage()?;
            Ok(if let Some(limit) = max_concurrent_requests {
                QueuedRequests::create(storage, limit)
            } else {
                storage
            })
        };

        let create_storage = move || {
            use futures::{FutureExt, TryFutureExt, future};
            future::ready(create_storage())
                .and_then(|storage| async {
                    storage.check().await.map(|mode| match mode {
                        CacheMode::ReadWrite => storage,
                        CacheMode::ReadOnly => ReadOnlyStorage::create(storage),
                    })
                })
                .boxed()
        };

        #[cfg(not(feature = "watcher"))]
        let storage = create_storage().await?;
        #[cfg(feature = "watcher")]
        let storage = WatchStorage::from(create_storage, &watch_paths).await?;

        Ok(if let Some(cfg) = preprocessor_cache_mode {
            PreprocessorCache::create(storage.clone(), cfg)
        } else {
            storage
        })
    }
}

impl From<(StorageKind, DiskCacheConfig)> for StorageBuilder {
    fn from((storage_kind, config): (StorageKind, DiskCacheConfig)) -> Self {
        let DiskCacheConfig {
            dir,
            size,
            rw_mode,
            preprocessor_cache_mode,
            ..
        } = config;

        let rw_mode = rw_mode.into();
        let dir = dir.join(storage_kind.key_prefix(String::new(), Some(&preprocessor_cache_mode)));

        Self::default()
            .create_storage(
                move || {
                    debug!("Init disk {storage_kind} cache with dir={dir:?}, size={size}, rw_mode={rw_mode:?})");
                    Ok(Arc::new(DiskCache::new(&dir, size, rw_mode)))
                }
            )
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "azure")]
impl From<(StorageKind, AzureCacheConfig)> for StorageBuilder {
    fn from((storage_kind, config): (StorageKind, AzureCacheConfig)) -> Self {
        let AzureCacheConfig {
            connection_string,
            container,
            key_prefix,
            preprocessor_cache_mode,
            ..
        } = config;

        let key_prefix = storage_kind.key_prefix(key_prefix, preprocessor_cache_mode.as_ref());

        Self::default()
            .create_storage(move || {
                debug!("Init azure {storage_kind} cache with container {container}, key_prefix {key_prefix}");

                AzureBlobCache::build(&connection_string, &container, &key_prefix)
                    .map(|storage| Arc::new(storage) as Arc<dyn Storage>)
                    .map_err(|err| anyhow!("create azure cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "gcs")]
impl From<(StorageKind, GCSCacheConfig)> for StorageBuilder {
    fn from((storage_kind, config): (StorageKind, GCSCacheConfig)) -> Self {
        let GCSCacheConfig {
            bucket,
            key_prefix,
            cred_path,
            rw_mode,
            service_account,
            credential_url,
            preprocessor_cache_mode,
            ..
        } = config;

        let key_prefix = storage_kind.key_prefix(key_prefix, preprocessor_cache_mode.as_ref());

        Self::default()
            .create_storage(move || {
                debug!(
                    "Init gcs {storage_kind} cache with bucket {bucket}, key_prefix {key_prefix}"
                );

                GCSCache::build(
                    &bucket,
                    &key_prefix,
                    cred_path.as_deref(),
                    service_account.as_deref(),
                    rw_mode.into(),
                    credential_url.as_deref(),
                )
                .map(|storage| Arc::new(storage) as Arc<dyn Storage>)
                .map_err(|err| anyhow!("create gcs cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "gha")]
impl From<(StorageKind, GHACacheConfig)> for StorageBuilder {
    fn from((storage_kind, config): (StorageKind, GHACacheConfig)) -> Self {
        let GHACacheConfig {
            version,
            preprocessor_cache_mode,
            ..
        } = config;

        Self::default()
            .create_storage(move || {
                debug!("Init gha {storage_kind} cache with version {version}");

                GHACache::build(&version)
                    .map(|storage| Arc::new(storage) as Arc<dyn Storage>)
                    .map_err(|err| anyhow!("create gha cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "memcached")]
impl From<(StorageKind, MemcachedCacheConfig)> for StorageBuilder {
    fn from((storage_kind, config): (StorageKind, MemcachedCacheConfig)) -> Self {
        let MemcachedCacheConfig {
            url,
            username,
            password,
            expiration,
            connection_pool_max_size,
            key_prefix,
            preprocessor_cache_mode,
            ..
        } = config;

        let connection_pool_max_size = connection_pool_max_size.unwrap_or(10);
        let key_prefix = storage_kind.key_prefix(key_prefix, preprocessor_cache_mode.as_ref());

        Self::default()
            .create_storage(move || {
                debug!("Init memcached {storage_kind} cache with url {url}");

                MemcachedCache::build(
                    &url,
                    username.as_deref(),
                    password.as_deref(),
                    &key_prefix,
                    expiration,
                    connection_pool_max_size,
                )
                .map(|storage| Arc::new(storage) as Arc<dyn Storage>)
                .map_err(|err| anyhow!("create memcached cache failed: {err:?}"))
            })
            .max_concurrent_requests(connection_pool_max_size.saturating_sub(1).max(1) as usize)
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "redis")]
impl From<(StorageKind, RedisCacheConfig)> for StorageBuilder {
    fn from((storage_kind, config): (StorageKind, RedisCacheConfig)) -> Self {
        use crate::cache::simplex::SimplexCache;

        let RedisCacheConfig {
            endpoint,
            cluster_endpoints,
            reader_endpoints,
            username,
            password,
            db,
            url,
            ttl,
            connection_pool_max_size,
            key_prefix,
            preprocessor_cache_mode,
            ..
        } = config;

        let connection_pool_max_size = connection_pool_max_size.unwrap_or(10);
        let connection_limit = connection_pool_max_size.saturating_sub(1).max(1) as usize;
        let key_prefix = storage_kind.key_prefix(key_prefix, preprocessor_cache_mode.as_ref());

        Self::default()
            .create_storage(move || {
                match (&endpoint, &cluster_endpoints, &url) {
                    (Some(url), None, None) => {
                        debug!("Init redis single-node {storage_kind} cache with url {url}");
                        RedisCache::build_single(
                            url,
                            username.as_deref(),
                            password.as_deref(),
                            db,
                            &key_prefix,
                            ttl,
                            connection_pool_max_size,
                        )
                    }
                    (None, Some(urls), None) => {
                        debug!("Init redis cluster {storage_kind} cache with urls {urls}");
                        RedisCache::build_cluster(
                            urls,
                            username.as_deref(),
                            password.as_deref(),
                            db,
                            &key_prefix,
                            ttl,
                            connection_pool_max_size,
                        )
                    }
                    (None, None, Some(url)) => {
                        warn!("Init redis single-node {storage_kind} cache from deprecated API with url {url}");
                        if username.is_some() || password.is_some() || db != DEFAULT_REDIS_DB {
                            bail!("`username`, `password` and `db` has no effect when `url` is set. Please use `endpoint` or `cluster_endpoints` for new API accessing");
                        }

                        RedisCache::build_from_url(url, &key_prefix, ttl, connection_pool_max_size)
                    }
                    _ => bail!("Only one of `endpoint`, `cluster_endpoints`, `url` must be set"),
                }
                .map(|storage| QueuedRequests::create(Arc::new(storage), connection_limit))
                .and_then(|storage| {
                    if let Some(reader_endpoints) = &reader_endpoints {
                        debug!("Init redis cluster {storage_kind} cache with reader endpoints {reader_endpoints}");
                        let reader = if reader_endpoints.contains(",") {
                            RedisCache::build_cluster(
                                reader_endpoints,
                                username.as_deref(),
                                password.as_deref(),
                                db,
                                &key_prefix,
                                ttl,
                                connection_pool_max_size,
                            )?
                        } else {
                            RedisCache::build_single(
                                reader_endpoints,
                                username.as_deref(),
                                password.as_deref(),
                                db,
                                &key_prefix,
                                ttl,
                                connection_pool_max_size,
                            )?
                        };
                        Ok(SimplexCache::create(QueuedRequests::create(Arc::new(reader), connection_limit), storage))
                    } else {
                        Ok(storage)
                    }
                })
                .map_err(|err| anyhow!("create redis cache failed: {err:?}"))
        })
        .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "s3")]
impl From<(StorageKind, S3CacheConfig)> for StorageBuilder {
    fn from((storage_kind, config): (StorageKind, S3CacheConfig)) -> Self {
        let S3CacheConfig {
            bucket,
            enable_virtual_host_style,
            endpoint,
            key_prefix,
            no_credentials,
            region,
            server_side_encryption,
            use_ssl,
            preprocessor_cache_mode,
            ..
        } = config;

        let key_prefix = storage_kind.key_prefix(key_prefix, preprocessor_cache_mode.as_ref());

        Self::default()
            .create_storage(move || {
            debug!("Init s3 {storage_kind} cache with endpoint {endpoint:?}, bucket {bucket:?}, and key prefix: {key_prefix:?}");

            S3Cache::new(bucket.clone(), key_prefix.clone(), no_credentials)
                .with_region(region.clone())
                .with_endpoint(endpoint.clone())
                .with_use_ssl(use_ssl)
                .with_server_side_encryption(server_side_encryption)
                .with_enable_virtual_host_style(enable_virtual_host_style)
                .build()
                .map(|storage| Arc::new(storage) as Arc<dyn Storage>)
                .map_err(|err| anyhow!("create s3 cache failed: {err:?}"))
        })
        .preprocessor_cache_mode(preprocessor_cache_mode)
        .watch_paths({
            let ctx = reqsign::default_context();
            [
                ctx.env_var("AWS_CONFIG_FILE").unwrap_or_else(|| "~/.aws/config".into()),
                ctx.env_var("AWS_SHARED_CREDENTIALS_FILE").unwrap_or_else(|| "~/.aws/credentials".into()),
            ]
            .iter()
            .filter_map(|s| ctx.expand_home_dir(s))
            .map(PathBuf::from)
            .collect::<Vec<_>>()
        })
    }
}

#[cfg(feature = "webdav")]
impl From<(StorageKind, WebdavCacheConfig)> for StorageBuilder {
    fn from((storage_kind, config): (StorageKind, WebdavCacheConfig)) -> Self {
        let WebdavCacheConfig {
            endpoint,
            key_prefix,
            password,
            token,
            username,
            preprocessor_cache_mode,
            ..
        } = config;

        let key_prefix = storage_kind.key_prefix(key_prefix, preprocessor_cache_mode.as_ref());

        Self::default()
            .create_storage(move || {
                debug!("Init webdav {storage_kind} cache with endpoint {endpoint}");

                WebdavCache::build(
                    &endpoint,
                    &key_prefix,
                    username.as_deref(),
                    password.as_deref(),
                    token.as_deref(),
                )
                .map(|storage| Arc::new(storage) as Arc<dyn Storage>)
                .map_err(|err| anyhow!("create webdav cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "oss")]
impl From<(StorageKind, OSSCacheConfig)> for StorageBuilder {
    fn from((storage_kind, config): (StorageKind, OSSCacheConfig)) -> Self {
        let OSSCacheConfig {
            bucket,
            endpoint,
            key_prefix,
            no_credentials,
            preprocessor_cache_mode,
            ..
        } = config;

        let key_prefix = storage_kind.key_prefix(key_prefix, preprocessor_cache_mode.as_ref());

        Self::default()
            .create_storage(move || {
                debug!("Init oss {storage_kind} cache with bucket {bucket}, endpoint {endpoint:?}");

                OSSCache::build(&bucket, &key_prefix, endpoint.as_deref(), no_credentials)
                    .map(|storage| Arc::new(storage) as Arc<dyn Storage>)
                    .map_err(|err| anyhow!("create oss cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

impl From<(StorageKind, CacheType)> for StorageBuilder {
    fn from((storage_kind, cache_type): (StorageKind, CacheType)) -> Self {
        #[allow(unreachable_patterns)]
        match cache_type {
            #[cfg(feature = "azure")]
            CacheType::Azure(cfg) => (storage_kind, cfg).into(),
            #[cfg(feature = "gcs")]
            CacheType::GCS(cfg) => (storage_kind, cfg).into(),
            #[cfg(feature = "gha")]
            CacheType::GHA(cfg) => (storage_kind, cfg).into(),
            #[cfg(feature = "memcached")]
            CacheType::Memcached(cfg) => (storage_kind, cfg).into(),
            #[cfg(feature = "redis")]
            CacheType::Redis(cfg) => (storage_kind, cfg).into(),
            #[cfg(feature = "s3")]
            CacheType::S3(cfg) => (storage_kind, cfg).into(),
            #[cfg(feature = "webdav")]
            CacheType::Webdav(cfg) => (storage_kind, cfg).into(),
            #[cfg(feature = "oss")]
            CacheType::OSS(cfg) => (storage_kind, cfg).into(),
            CacheType::Disk(cfg) => (storage_kind, cfg).into(),
            _ => Self::default(),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum StorageKind {
    Compilations,
    Preprocessor,
}

impl fmt::Display for StorageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compilations => write!(f, "object"),
            Self::Preprocessor => write!(f, "preprocessor"),
        }
    }
}

impl StorageKind {
    /// Create suitable `Storage` implementations from a list of storage configurations.
    pub async fn create(&self, cache_types: &[CacheType]) -> Result<Arc<dyn Storage>> {
        let kind = *self;
        futures::stream::iter(
            cache_types
                .iter()
                .sorted()
                .rev()
                .filter({
                    let use_preprocessor_cache_mode = matches!(kind, StorageKind::Preprocessor);
                    move |&config| match config {
                        CacheType::Disk(_) => true,
                        _ => config
                            .preprocessor_cache_mode()
                            .map(|cache| cache.use_preprocessor_cache_mode)
                            .filter(|&f| f == use_preprocessor_cache_mode)
                            .unwrap_or(!use_preprocessor_cache_mode),
                    }
                })
                .map(|config| StorageBuilder::from((kind, config.clone()))),
        )
        .fold(None, |prev, curr| async move {
            match prev {
                Some(Err(err)) => Some(Err(err)),
                None => Some(curr.build().await),
                Some(Ok(secondary)) => Some(
                    curr.build()
                        .await
                        .map(|primary| TieredCache::create(primary, secondary)),
                ),
            }
        })
        .then(|res| async move {
            if let Some(storage) = res {
                storage
            } else {
                // If we build only with `cargo build --no-default-features`
                // only use sccache with a local cache (no remote storage)
                StorageBuilder::from((kind, DiskCacheConfig::default()))
                    .build()
                    .await
            }
        })
        .inspect_err(|err| error!("storage init failed for {kind} cache: {err:?}"))
        .and_then(|storage| async {
            storage
                .check()
                .await
                .context("storage check failed")
                .map(|mode| {
                    info!("server configured with {kind} cache: {mode:?}");
                    storage
                })
        })
        .await
    }

    fn key_prefix(
        &self,
        key_prefix: String,
        preprocessor_cache_mode: Option<&PreprocessorCacheModeConfig>,
    ) -> String {
        preprocessor_cache_mode
            .filter(|&p| p.use_preprocessor_cache_mode)
            .filter(|_| matches!(self, StorageKind::Preprocessor))
            .map(|p| p.key_prefix.clone())
            .unwrap_or(key_prefix)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::{CacheModeConfig, Config};

    #[test]
    fn test_normalize_key() {
        assert_eq!(
            normalize_key("0123456789abcdef0123456789abcdef"),
            "0/1/2/0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn test_read_write_mode_local() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .unwrap();

        // Use disk cache.
        let tempdir = tempfile::Builder::new()
            .prefix("sccache_test_rust_cargo")
            .tempdir()
            .context("Failed to create tempdir")
            .unwrap();

        let cache_dir = tempdir.path().join("cache");
        fs::create_dir(&cache_dir).unwrap();

        let make_config = |rw_mode| Config {
            caches: vec![CacheType::Disk(DiskCacheConfig {
                dir: cache_dir.clone(),
                rw_mode,
                ..DiskCacheConfig::default()
            })],
            ..Default::default()
        };

        // Test Read Write
        {
            let caches = make_config(CacheModeConfig::ReadWrite).caches;
            runtime.block_on(async {
                let storage = StorageKind::Compilations.create(&caches).await.unwrap();
                storage
                    .put("test1", &mut std::io::Cursor::new("entry".as_bytes()))
                    .await
                    .unwrap();
            });
        }

        // Test Read-only
        {
            let caches = make_config(CacheModeConfig::ReadOnly).caches;
            runtime.block_on(async {
                let storage = StorageKind::Compilations.create(&caches).await.unwrap();
                assert_eq!(
                    storage
                        .put("test1", &mut std::io::Cursor::new("entry".as_bytes()))
                        .await
                        .unwrap_err()
                        .to_string(),
                    "Cannot write to read-only storage"
                );
            });
        }
    }
}
