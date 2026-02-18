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
    config::{CacheType, DiskCacheConfig},
    errors::*,
};
use async_trait::async_trait;
use bytes::Bytes;
use fs_err as fs;
use futures::{FutureExt, StreamExt, TryFutureExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    io::{self, Cursor, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::FileOptions};

#[cfg(feature = "watcher")]
use crate::cache::watch::WatchStorage;
#[cfg(feature = "azure")]
use crate::{cache::azure::AzureBlobCache, config::AzureCacheConfig};
#[cfg(feature = "cos")]
use crate::{cache::cos::COSCache, config::COSCacheConfig};
#[cfg(feature = "gcs")]
use crate::{cache::gcs::GCSCache, config::GCSCacheConfig};
#[cfg(feature = "gha")]
use crate::{cache::gha::GHACache, config::GHACacheConfig};
#[cfg(feature = "memcached")]
use crate::{cache::memcached::MemcachedCache, config::MemcachedCacheConfig};
#[cfg(feature = "oss")]
use crate::{cache::oss::OSSCache, config::OSSCacheConfig};
#[cfg(feature = "redis")]
use crate::{
    cache::redis::RedisCache,
    config::{DEFAULT_REDIS_DB, RedisCacheConfig},
};
#[cfg(feature = "s3")]
use crate::{cache::s3::S3Cache, config::S3CacheConfig};
#[cfg(feature = "webdav")]
use crate::{cache::webdav::WebdavCache, config::WebdavCacheConfig};
#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss",
    feature = "cos"
))]
use {crate::util::retry_with_jitter, tokio_retry2::RetryError};

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
pub trait BufReadSeek: std::io::BufRead + Seek + Send {}

impl<T: std::io::BufRead + Seek + Send> BufReadSeek for T {}

/// Data stored in the compiler cache.
pub struct CacheRead {
    zip: ZipArchive<Box<dyn BufReadSeek>>,
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
        R: BufReadSeek + 'static,
    {
        let z = ZipArchive::new(Box::new(reader) as Box<dyn BufReadSeek>)
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

    pub fn into_inner(self) -> Box<dyn BufReadSeek> {
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
                let mut tmp = crate::util::tempfile_in(dir)?;
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
    async fn get(&self, key: &str) -> Result<Cache<Bytes>>;

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
    async fn put(&self, key: &str, entry: Bytes) -> Result<Duration>;

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

    /// Get the cache backend type name (e.g., "disk", "redis", "s3").
    /// Used for statistics and display purposes.
    fn cache_type_name(&self) -> &'static str {
        "unknown"
    }

    /// Get the current storage usage, if applicable.
    async fn current_size(&self) -> Result<Option<u64>>;

    /// Get the maximum storage size, if applicable.
    async fn max_size(&self) -> Result<Option<u64>>;

    /// Return the config for preprocessor cache mode if applicable
    fn preprocessor_cache_mode_config(&self) -> PreprocessorCacheModeConfig {
        PreprocessorCacheModeConfig::default()
    }
    /// Return the base directories for path normalization if configured
    fn basedirs(&self) -> &[Vec<u8>] {
        &[]
    }
}

/// Configuration switches for preprocessor cache mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(default)]
pub struct PreprocessorCacheModeConfig {
    /// Whether to use preprocessor cache mode entirely
    pub use_preprocessor_cache_mode: bool,
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

    pub fn default_key_prefix() -> String {
        "preprocessor".into()
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
    feature = "cos"
))]
mod operator {
    use super::*;

    fn to_retry_err<E: Into<opendal::Error>>(kind: &str, err: E) -> RetryError<opendal::Error> {
        let err = err.into();
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
    ) -> std::result::Result<Bytes, opendal::Error> {
        // TODO: Allow configuring the number of retries
        retry_with_jitter(usize::MAX, || async {
            storage
                .read(&normalize_key(key))
                .await
                .map(|buf| buf.to_bytes())
                .map_err(|e| to_retry_err("lookup", e))
        })
        .await
    }

    pub async fn write_with_retry(
        storage: &opendal::Operator,
        key: &str,
        buf: Bytes,
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
}

/// Wrapper for opendal::Operator that adds basedirs support
#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss",
    feature = "cos"
))]
pub struct RemoteStorage {
    operator: opendal::Operator,
    basedirs: Vec<Vec<u8>>,
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
    feature = "cos"
))]
impl RemoteStorage {
    pub fn new(operator: opendal::Operator, basedirs: Vec<Vec<u8>>) -> Self {
        Self { operator, basedirs }
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
    feature = "oss",
    feature = "cos"
))]
#[async_trait]
impl Storage for RemoteStorage {
    async fn get(&self, key: &str) -> Result<Cache<Bytes>> {
        match operator::read_with_retry(&self.operator, key).await {
            Ok(data) => Ok(Cache::Hit(data)),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(Cache::Miss),
            Err(e) => Err(e.into()),
        }
    }

    async fn del(&self, key: &str) -> Result<()> {
        self.operator
            .delete(&normalize_key(key))
            .await
            .map_err(anyhow::Error::new)
    }

    async fn has(&self, key: &str) -> bool {
        self.operator.stat(&normalize_key(key)).await.is_ok()
    }

    async fn put(&self, key: &str, entry: Bytes) -> Result<Duration> {
        let start = std::time::Instant::now();
        operator::write_with_retry(&self.operator, key, entry).await?;
        Ok(start.elapsed())
    }

    async fn size(&self, key: &str) -> Result<u64> {
        Ok(self
            .operator
            .stat_with(&normalize_key(key))
            .await?
            .content_length())
    }

    async fn check(&self) -> Result<CacheMode> {
        use opendal::ErrorKind;

        let path = ".sccache_check";

        // Read is required, return error directly if we can't read .
        match self.operator.read(path).await {
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
                eprintln!("cache storage read check: {err:?}, but we decide to keep running");
            }
            Err(err) => bail!("cache storage failed to read: {:?}", err),
        }

        let can_write = match self.operator.write(path, "Hello, World!").await {
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
        let meta = self.operator.info();
        format!(
            "{}, name: {}, prefix: {}",
            meta.scheme(),
            meta.name(),
            meta.root()
        )
    }

    fn cache_type_name(&self) -> &'static str {
        // Use opendal's scheme as the cache type name
        // This returns "s3", "redis", "azure", "gcs", etc.
        self.operator.info().scheme()
    }

    async fn current_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn max_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    fn basedirs(&self) -> &[Vec<u8>] {
        &self.basedirs
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
    feature = "cos",
    test
))]
/// Normalize key `abcdef` into `a/b/c/abcdef`
pub(in crate::cache) fn normalize_key(key: &str) -> String {
    format!("{}/{}/{}/{}", &key[0..1], &key[1..2], &key[2..3], &key)
}

pub struct PreprocessorCache(pub Arc<dyn Storage>, pub PreprocessorCacheModeConfig);

impl PreprocessorCache {
    pub fn create(storage: Arc<dyn Storage>, cfg: PreprocessorCacheModeConfig) -> Arc<dyn Storage> {
        Arc::new(PreprocessorCache(storage, cfg))
    }
}

#[async_trait]
impl Storage for PreprocessorCache {
    async fn get(&self, key: &str) -> Result<Cache<Bytes>> {
        self.0.get(key).await
    }

    async fn del(&self, key: &str) -> Result<()> {
        self.0.del(key).await
    }

    async fn has(&self, key: &str) -> bool {
        self.0.has(key).await
    }

    async fn put(&self, key: &str, entry: Bytes) -> Result<Duration> {
        self.0.put(key, entry).await
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

    pub fn watch_paths<P: ToOwned<Owned = Vec<PathBuf>>>(self, paths: P) -> Self {
        Self {
            watch_paths: paths.to_owned(),
            ..self
        }
    }

    pub async fn build(self) -> Result<Arc<dyn Storage>> {
        let Self {
            create_storage,
            preprocessor_cache_mode,
            #[allow(unused_variables)]
            watch_paths,
        } = self;

        let create_storage = create_storage.expect("create_storage should exist");

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

impl From<(StorageKind, Vec<Vec<u8>>, DiskCacheConfig)> for StorageBuilder {
    fn from(
        (storage_kind, basedirs, config): (StorageKind, Vec<Vec<u8>>, DiskCacheConfig),
    ) -> Self {
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
                    Ok(Arc::new(DiskCache::new(&dir, size, rw_mode, basedirs.clone())))
                }
            )
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "azure")]
impl From<(StorageKind, Vec<Vec<u8>>, AzureCacheConfig)> for StorageBuilder {
    fn from(
        (storage_kind, basedirs, config): (StorageKind, Vec<Vec<u8>>, AzureCacheConfig),
    ) -> Self {
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
                    .map(|storage| Arc::new(RemoteStorage::new(storage, basedirs.clone())) as Arc<dyn Storage>)
                    .map_err(|err| anyhow!("create azure cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "gcs")]
impl From<(StorageKind, Vec<Vec<u8>>, GCSCacheConfig)> for StorageBuilder {
    fn from((storage_kind, basedirs, config): (StorageKind, Vec<Vec<u8>>, GCSCacheConfig)) -> Self {
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
                .map(|storage| {
                    Arc::new(RemoteStorage::new(storage, basedirs.clone())) as Arc<dyn Storage>
                })
                .map_err(|err| anyhow!("create gcs cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "gha")]
impl From<(StorageKind, Vec<Vec<u8>>, GHACacheConfig)> for StorageBuilder {
    fn from((storage_kind, basedirs, config): (StorageKind, Vec<Vec<u8>>, GHACacheConfig)) -> Self {
        let GHACacheConfig {
            version,
            preprocessor_cache_mode,
            key_prefix,
            ..
        } = config;

        let key_prefix = storage_kind.key_prefix(key_prefix, preprocessor_cache_mode.as_ref());

        Self::default()
            .create_storage(move || {
                debug!("Init gha {storage_kind} cache with version {version}");

                GHACache::build(&version, key_prefix.as_str())
                    .map(|storage| {
                        Arc::new(RemoteStorage::new(storage, basedirs.clone())) as Arc<dyn Storage>
                    })
                    .map_err(|err| anyhow!("create gha cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "memcached")]
impl From<(StorageKind, Vec<Vec<u8>>, MemcachedCacheConfig)> for StorageBuilder {
    fn from(
        (storage_kind, basedirs, config): (StorageKind, Vec<Vec<u8>>, MemcachedCacheConfig),
    ) -> Self {
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
                .map(|storage| {
                    Arc::new(RemoteStorage::new(storage, basedirs.clone())) as Arc<dyn Storage>
                })
                .map_err(|err| anyhow!("create memcached cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "redis")]
impl From<(StorageKind, Vec<Vec<u8>>, RedisCacheConfig)> for StorageBuilder {
    fn from(
        (storage_kind, basedirs, config): (StorageKind, Vec<Vec<u8>>, RedisCacheConfig),
    ) -> Self {
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
                        let reader = Arc::new(RemoteStorage::new(reader, basedirs.clone()));
                        let storage = Arc::new(RemoteStorage::new(storage, basedirs.clone()));
                        Ok(SimplexCache::create(reader, storage))
                    } else {
                        Ok(Arc::new(RemoteStorage::new(storage, basedirs.clone())))
                    }
                })
                .map_err(|err| anyhow!("create redis cache failed: {err:?}"))
        })
        .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "s3")]
impl From<(StorageKind, Vec<Vec<u8>>, S3CacheConfig)> for StorageBuilder {
    fn from((storage_kind, basedirs, config): (StorageKind, Vec<Vec<u8>>, S3CacheConfig)) -> Self {
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
                .map(|storage| Arc::new(RemoteStorage::new(storage, basedirs.clone())) as Arc<dyn Storage>)
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
impl From<(StorageKind, Vec<Vec<u8>>, WebdavCacheConfig)> for StorageBuilder {
    fn from(
        (storage_kind, basedirs, config): (StorageKind, Vec<Vec<u8>>, WebdavCacheConfig),
    ) -> Self {
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
                .map(|storage| {
                    Arc::new(RemoteStorage::new(storage, basedirs.clone())) as Arc<dyn Storage>
                })
                .map_err(|err| anyhow!("create webdav cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "oss")]
impl From<(StorageKind, Vec<Vec<u8>>, OSSCacheConfig)> for StorageBuilder {
    fn from((storage_kind, basedirs, config): (StorageKind, Vec<Vec<u8>>, OSSCacheConfig)) -> Self {
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
                    .map(|storage| {
                        Arc::new(RemoteStorage::new(storage, basedirs.clone())) as Arc<dyn Storage>
                    })
                    .map_err(|err| anyhow!("create oss cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

#[cfg(feature = "cos")]
impl From<(StorageKind, Vec<Vec<u8>>, COSCacheConfig)> for StorageBuilder {
    fn from((storage_kind, basedirs, config): (StorageKind, Vec<Vec<u8>>, COSCacheConfig)) -> Self {
        let COSCacheConfig {
            bucket,
            endpoint,
            key_prefix,
            preprocessor_cache_mode,
            ..
        } = config;

        let key_prefix = storage_kind.key_prefix(key_prefix, preprocessor_cache_mode.as_ref());

        Self::default()
            .create_storage(move || {
                debug!("Init cos {storage_kind} cache with bucket {bucket}, endpoint {endpoint:?}");

                COSCache::build(&bucket, &key_prefix, endpoint.as_deref())
                    .map(|storage| {
                        Arc::new(RemoteStorage::new(storage, basedirs.clone())) as Arc<dyn Storage>
                    })
                    .map_err(|err| anyhow!("create oss cache failed: {err:?}"))
            })
            .preprocessor_cache_mode(preprocessor_cache_mode)
    }
}

impl From<(StorageKind, Vec<Vec<u8>>, CacheType)> for StorageBuilder {
    fn from((storage_kind, basedirs, cache_type): (StorageKind, Vec<Vec<u8>>, CacheType)) -> Self {
        #[allow(unreachable_patterns)]
        match cache_type {
            #[cfg(feature = "azure")]
            CacheType::Azure(cfg) => (storage_kind, basedirs, cfg).into(),
            #[cfg(feature = "gcs")]
            CacheType::GCS(cfg) => (storage_kind, basedirs, cfg).into(),
            #[cfg(feature = "gha")]
            CacheType::GHA(cfg) => (storage_kind, basedirs, cfg).into(),
            #[cfg(feature = "memcached")]
            CacheType::Memcached(cfg) => (storage_kind, basedirs, cfg).into(),
            #[cfg(feature = "redis")]
            CacheType::Redis(cfg) => (storage_kind, basedirs, cfg).into(),
            #[cfg(feature = "s3")]
            CacheType::S3(cfg) => (storage_kind, basedirs, cfg).into(),
            #[cfg(feature = "webdav")]
            CacheType::Webdav(cfg) => (storage_kind, basedirs, cfg).into(),
            #[cfg(feature = "oss")]
            CacheType::OSS(cfg) => (storage_kind, basedirs, cfg).into(),
            #[cfg(feature = "cos")]
            CacheType::COS(cfg) => (storage_kind, basedirs, cfg).into(),
            CacheType::Disk(cfg) => (storage_kind, basedirs, cfg).into(),
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
    pub async fn create(
        &self,
        cache_types: &[CacheType],
        basedirs: &[Vec<u8>],
    ) -> Result<Arc<dyn Storage>> {
        let kind = *self;
        futures::stream::iter(
            cache_types
                .iter()
                .sorted()
                .rev()
                .filter({
                    let use_preprocessor_cache_mode = matches!(kind, StorageKind::Preprocessor);
                    move |&cache| {
                        if use_preprocessor_cache_mode {
                            match cache {
                                CacheType::Disk(_) => true,
                                _ => cache
                                    .preprocessor_cache_mode()
                                    .map(|cache| cache.use_preprocessor_cache_mode)
                                    .filter(|&f| f == use_preprocessor_cache_mode)
                                    .unwrap_or_default(),
                            }
                        } else {
                            true
                        }
                    }
                })
                .map(|cache| StorageBuilder::from((kind, basedirs.to_vec(), cache.clone()))),
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
                StorageBuilder::from((kind, basedirs.to_vec(), DiskCacheConfig::default()))
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
        let tempdir = crate::util::normal_tempdir()
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
                let storage = StorageKind::Compilations
                    .create(&caches, &[])
                    .await
                    .unwrap();
                storage.put("test1", "entry".into()).await.unwrap();
            });
        }

        // Test Read-only
        {
            let caches = make_config(CacheModeConfig::ReadOnly).caches;
            runtime.block_on(async {
                let storage = StorageKind::Compilations
                    .create(&caches, &[])
                    .await
                    .unwrap();
                assert_eq!(
                    storage
                        .put("test1", "entry".into())
                        .await
                        .unwrap_err()
                        .to_string(),
                    "Cannot write to read-only storage"
                );
            });
        }
    }

    #[test]
    #[cfg(feature = "s3")]
    fn test_operator_storage_s3_with_basedirs() {
        // Create S3 operator (doesn't need real credentials for this test)
        let operator = crate::cache::s3::S3Cache::new(
            "test-bucket".to_string(),
            "test-prefix".to_string(),
            true, // no_credentials = true
        )
        .with_region(Some("us-east-1".to_string()))
        .build()
        .expect("Failed to create S3 cache operator");

        let basedirs = vec![b"/home/user/project".to_vec(), b"/opt/build".to_vec()];

        // Wrap with OperatorStorage
        let storage = RemoteStorage::new(operator, basedirs.clone());

        // Verify basedirs are stored and retrieved correctly
        assert_eq!(storage.basedirs(), basedirs.as_slice());
        assert_eq!(storage.basedirs().len(), 2);
        assert_eq!(storage.basedirs()[0], b"/home/user/project".to_vec());
        assert_eq!(storage.basedirs()[1], b"/opt/build".to_vec());
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_operator_storage_redis_with_basedirs() {
        // Create Redis operator
        let operator = crate::cache::redis::RedisCache::build_single(
            "redis://localhost:6379",
            None,
            None,
            0,
            "test-prefix",
            0,
            10,
        )
        .expect("Failed to create Redis cache operator");

        let basedirs = vec![b"/workspace".to_vec()];

        // Wrap with OperatorStorage
        let storage = RemoteStorage::new(operator, basedirs.clone());

        // Verify basedirs work
        assert_eq!(storage.basedirs(), basedirs.as_slice());
        assert_eq!(storage.basedirs().len(), 1);
    }
}
