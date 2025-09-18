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

#[cfg(feature = "azure")]
use crate::cache::azure::AzureBlobCache;
use crate::cache::disk::DiskCache;
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
#[cfg(feature = "webdav")]
use crate::cache::webdav::WebdavCache;
use crate::compiler::PreprocessorCacheEntry;
use crate::config::{CacheType, DiskCacheConfig};
use async_trait::async_trait;
use fs_err as fs;
use futures::AsyncReadExt;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{self, Cursor, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use zip::write::FileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};
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
use {
    crate::{config, util::retry_with_jitter},
    futures::AsyncWriteExt,
    tokio_retry2::RetryError,
};

use crate::errors::*;

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
pub enum Cache {
    /// Result was found in cache.
    Hit(CacheRead),
    /// Result was not found in cache.
    Miss,
    /// Do not cache the results of the compilation.
    None,
    /// Cache entry should be ignored, force compilation.
    Recache,
}

impl fmt::Debug for Cache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Cache::Hit(_) => write!(f, "Cache::Hit(...)"),
            Cache::Miss => write!(f, "Cache::Miss"),
            Cache::None => write!(f, "Cache::None"),
            Cache::Recache => write!(f, "Cache::Recache"),
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

impl<T: Read + Seek + Send> ReadSeek for T {}

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
    async fn get(&self, key: &str) -> Result<Cache>;

    async fn get_async_reader(
        &self,
        key: &str,
    ) -> Result<Box<dyn futures::AsyncRead + Send + Unpin>>;

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
    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration>;

    async fn put_async_reader(
        &self,
        key: &str,
        size: u64,
        source: Pin<&mut (dyn futures::AsyncRead + Send)>,
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

    /// Return the preprocessor cache entry for a given preprocessor key,
    /// if it exists.
    /// Only applicable when using preprocessor cache mode.
    async fn get_preprocessor_cache_entry(&self, key: &str) -> Option<PreprocessorCacheEntry> {
        let mut reader = self.get_async_reader(key).await.ok()?;
        let mut buf = vec![];
        reader.read_to_end(&mut buf).await.ok()?;
        PreprocessorCacheEntry::read(&buf).ok()
    }

    /// Insert a preprocessor cache entry at the given preprocessor key,
    /// overwriting the entry if it exists.
    /// Only applicable when using preprocessor cache mode.
    async fn put_preprocessor_cache_entry(
        &self,
        key: &str,
        preprocessor_cache_entry: PreprocessorCacheEntry,
    ) -> Result<()> {
        use futures::TryStreamExt;
        let mut buf = vec![];
        preprocessor_cache_entry.serialize_to(&mut buf)?;
        let size = buf.len();
        let stream = futures::stream::iter([Ok(buf)]);
        let stream = stream.into_async_read();
        let stream = std::pin::pin!(stream);
        self.put_async_reader(key, size as u64, stream).await
    }
}

/// Configuration switches for preprocessor cache mode.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

impl Default for PreprocessorCacheModeConfig {
    fn default() -> Self {
        Self {
            use_preprocessor_cache_mode: false,
            file_stat_matches: false,
            use_ctime_for_stat: true,
            ignore_time_macros: false,
            skip_system_headers: false,
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
                debug!("cache {kind} error (permanent): {err}");
                RetryError::permanent(err)
            }
            opendal::ErrorKind::PermissionDenied => {
                debug!("cache {kind} error (permanent): {err}");
                RetryError::permanent(err)
            }
            opendal::ErrorKind::RateLimited => {
                debug!("cache {kind} error (transient): {err}");
                RetryError::transient(err)
            }
            opendal::ErrorKind::Unsupported => {
                debug!("cache {kind} error (permanent): {err}");
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
    async fn get(&self, key: &str) -> Result<Cache> {
        match operator::read_with_retry(self, key).await {
            Ok(res) => CacheRead::from(io::Cursor::new(res.to_bytes())).map(Cache::Hit),
            Err(err) => match err.kind() {
                opendal::ErrorKind::NotFound => Ok(Cache::Miss),
                _ => Err(err.into()),
            },
        }
    }

    async fn get_async_reader(
        &self,
        key: &str,
    ) -> Result<Box<dyn futures::AsyncRead + Send + Unpin>> {
        let reader = operator::async_reader_with_retry(self, key).await?;
        Ok(Box::new(reader.into_futures_async_read(..).await?)
            as Box<dyn futures::AsyncRead + Send + Unpin>)
    }

    async fn del(&self, key: &str) -> Result<()> {
        self.delete(&normalize_key(key))
            .await
            .map_err(anyhow::Error::new)
    }

    async fn has(&self, key: &str) -> bool {
        self.stat(&normalize_key(key)).await.is_ok()
    }

    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration> {
        let start = std::time::Instant::now();

        operator::write_with_retry(self, key, bytes::Bytes::from(entry.finish()?)).await?;

        Ok(start.elapsed())
    }

    async fn put_async_reader(
        &self,
        key: &str,
        _size: u64,
        source: Pin<&mut (dyn futures::AsyncRead + Send)>,
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

pub struct PreprocessorCache(pub Arc<dyn Storage>, pub PreprocessorCacheModeConfig);

#[async_trait]
impl Storage for PreprocessorCache {
    async fn get(&self, key: &str) -> Result<Cache> {
        self.0.get(key).await
    }

    async fn get_async_reader(
        &self,
        key: &str,
    ) -> Result<Box<dyn futures::AsyncRead + Send + Unpin>> {
        self.0.get_async_reader(key).await
    }

    async fn del(&self, _key: &str) -> Result<()> {
        Err(anyhow!("Cannot write to read-only storage"))
    }

    async fn has(&self, key: &str) -> bool {
        self.0.has(key).await
    }

    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration> {
        self.0.put(key, entry).await
    }

    async fn put_async_reader(
        &self,
        key: &str,
        size: u64,
        source: Pin<&mut (dyn futures::AsyncRead + Send)>,
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
        self.1
    }

    /// Return the preprocessor cache entry for a given preprocessor key,
    /// if it exists.
    /// Only applicable when using preprocessor cache mode.
    async fn get_preprocessor_cache_entry(&self, key: &str) -> Option<PreprocessorCacheEntry> {
        self.0.get_preprocessor_cache_entry(key).await
    }

    /// Insert a preprocessor cache entry at the given preprocessor key,
    /// overwriting the entry if it exists.
    /// Only applicable when using preprocessor cache mode.
    async fn put_preprocessor_cache_entry(
        &self,
        key: &str,
        preprocessor_cache_entry: PreprocessorCacheEntry,
    ) -> Result<()> {
        self.0
            .put_preprocessor_cache_entry(key, preprocessor_cache_entry)
            .await
    }
}

/// Get a suitable `Storage` implementation from configuration.
#[allow(clippy::cognitive_complexity)] // TODO simplify!
pub fn storage_from_config(
    cache: &Option<CacheType>,
    fallback_cache: &DiskCacheConfig,
) -> Result<(Arc<dyn Storage>, Arc<dyn Storage>)> {
    let fallback_storage = || -> (Arc<dyn Storage>, _) {
        let dir = &fallback_cache.dir;
        let size = fallback_cache.size;
        let rw_mode = fallback_cache.rw_mode.into();
        let preprocessor_cache_mode = fallback_cache.preprocessor_cache_mode;
        debug!("Init disk cache with dir={dir:?}, size={size}, rw_mode={rw_mode:?}, preprocessor_cache_mode={preprocessor_cache_mode:?})");
        let storage = DiskCache::new(dir, size, rw_mode);
        (Arc::new(storage), Some(preprocessor_cache_mode))
    };

    let (storage, preprocessor_cache_mode): (Arc<dyn Storage>, _) = match &cache {
        #[cfg(feature = "azure")]
        Some(CacheType::Azure(config::AzureCacheConfig {
            ref connection_string,
            ref container,
            ref key_prefix,
            ref preprocessor_cache_mode,
        })) => {
            debug!("Init azure cache with container {container}, key_prefix {key_prefix}");
            let storage = AzureBlobCache::build(connection_string, container, key_prefix)
                .map_err(|err| anyhow!("create azure cache failed: {err:?}"))?;

            (Arc::new(storage), *preprocessor_cache_mode)
        }
        #[cfg(feature = "gcs")]
        Some(CacheType::GCS(config::GCSCacheConfig {
            bucket,
            key_prefix,
            cred_path,
            rw_mode,
            service_account,
            credential_url,
            preprocessor_cache_mode,
        })) => {
            debug!("Init gcs cache with bucket {bucket}, key_prefix {key_prefix}");

            let storage = GCSCache::build(
                bucket,
                key_prefix,
                cred_path.as_deref(),
                service_account.as_deref(),
                (*rw_mode).into(),
                credential_url.as_deref(),
            )
            .map_err(|err| anyhow!("create gcs cache failed: {err:?}"))?;

            (Arc::new(storage), *preprocessor_cache_mode)
        }
        #[cfg(feature = "gha")]
        Some(CacheType::GHA(config::GHACacheConfig {
            ref version,
            ref preprocessor_cache_mode,
            ..
        })) => {
            debug!("Init gha cache with version {version}");

            let storage = GHACache::build(version)
                .map_err(|err| anyhow!("create gha cache failed: {err:?}"))?;
            (Arc::new(storage), *preprocessor_cache_mode)
        }
        #[cfg(feature = "memcached")]
        Some(CacheType::Memcached(config::MemcachedCacheConfig {
            ref url,
            ref username,
            ref password,
            ref expiration,
            ref key_prefix,
            ref preprocessor_cache_mode,
        })) => {
            debug!("Init memcached cache with url {url}");

            let storage = MemcachedCache::build(
                url,
                username.as_deref(),
                password.as_deref(),
                key_prefix,
                *expiration,
            )
            .map_err(|err| anyhow!("create memcached cache failed: {err:?}"))?;
            (Arc::new(storage), *preprocessor_cache_mode)
        }
        #[cfg(feature = "redis")]
        Some(CacheType::Redis(config::RedisCacheConfig {
            ref endpoint,
            ref cluster_endpoints,
            ref username,
            ref password,
            ref db,
            ref url,
            ref ttl,
            ref key_prefix,
            ref preprocessor_cache_mode,
        })) => {
            let storage = match (endpoint, cluster_endpoints, url) {
                    (Some(url), None, None) => {
                        debug!("Init redis single-node cache with url {url}");
                        RedisCache::build_single(
                            url,
                            username.as_deref(),
                            password.as_deref(),
                            *db,
                            key_prefix,
                            *ttl,
                        )
                    }
                    (None, Some(urls), None) => {
                        debug!("Init redis cluster cache with urls {urls}");
                        RedisCache::build_cluster(
                            urls,
                            username.as_deref(),
                            password.as_deref(),
                            *db,
                            key_prefix,
                            *ttl,
                        )
                    }
                    (None, None, Some(url)) => {
                        warn!("Init redis single-node cache from deprecated API with url {url}");
                        if username.is_some() || password.is_some() || *db != crate::config::DEFAULT_REDIS_DB {
                            bail!("`username`, `password` and `db` has no effect when `url` is set. Please use `endpoint` or `cluster_endpoints` for new API accessing");
                        }

                        RedisCache::build_from_url(url, key_prefix, *ttl)
                    }
                    _ => bail!("Only one of `endpoint`, `cluster_endpoints`, `url` must be set"),
                }
                .map_err(|err| anyhow!("create redis cache failed: {err:?}"))?;
            (Arc::new(storage), *preprocessor_cache_mode)
        }
        #[cfg(feature = "s3")]
        Some(CacheType::S3(config::S3CacheConfig {
            ref bucket,
            ref enable_virtual_host_style,
            ref endpoint,
            ref key_prefix,
            ref no_credentials,
            ref region,
            ref server_side_encryption,
            ref use_ssl,
            ref preprocessor_cache_mode,
        })) => {
            debug!(
                    "Init s3 cache with endpoint {endpoint:?}, bucket {bucket:?}, and key prefix: {key_prefix:?}"
                );
            let storage_builder = S3Cache::new(bucket.clone(), key_prefix.clone(), *no_credentials);
            let storage = storage_builder
                .with_region(region.clone())
                .with_endpoint(endpoint.clone())
                .with_use_ssl(*use_ssl)
                .with_server_side_encryption(*server_side_encryption)
                .with_enable_virtual_host_style(*enable_virtual_host_style)
                .build()
                .map_err(|err| anyhow!("create s3 cache failed: {err:?}"))?;

            (Arc::new(storage), *preprocessor_cache_mode)
        }
        #[cfg(feature = "webdav")]
        Some(CacheType::Webdav(config::WebdavCacheConfig {
            ref endpoint,
            ref key_prefix,
            ref password,
            ref token,
            ref username,
            ref preprocessor_cache_mode,
        })) => {
            debug!("Init webdav cache with endpoint {}", endpoint);

            let storage = WebdavCache::build(
                endpoint,
                key_prefix,
                username.as_deref(),
                password.as_deref(),
                token.as_deref(),
            )
            .map_err(|err| anyhow!("create webdav cache failed: {err:?}"))?;

            (Arc::new(storage), *preprocessor_cache_mode)
        }
        #[cfg(feature = "oss")]
        Some(CacheType::OSS(config::OSSCacheConfig {
            ref bucket,
            ref endpoint,
            ref key_prefix,
            ref no_credentials,
            ref preprocessor_cache_mode,
        })) => {
            debug!(
                "Init oss cache with bucket {}, endpoint {:?}",
                bucket, endpoint
            );

            let storage = OSSCache::build(bucket, key_prefix, endpoint.as_deref(), *no_credentials)
                .map_err(|err| anyhow!("create oss cache failed: {err:?}"))?;

            (Arc::new(storage), *preprocessor_cache_mode)
        }
        #[allow(unreachable_patterns)]
        // if we build only with `cargo build --no-default-features`
        // we only want to use sccache with a local cache (no remote storage)
        _ => fallback_storage(),
    };

    if let Some(cfg) = preprocessor_cache_mode {
        Ok((storage.clone(), Arc::new(PreprocessorCache(storage, cfg))))
    } else {
        let (disk, cfg) = fallback_storage();
        Ok((
            storage,
            Arc::new(PreprocessorCache(disk, cfg.unwrap_or_default())),
        ))
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
        let mut config = Config {
            cache: None,
            ..Default::default()
        };

        let tempdir = tempfile::Builder::new()
            .prefix("sccache_test_rust_cargo")
            .tempdir()
            .context("Failed to create tempdir")
            .unwrap();
        let cache_dir = tempdir.path().join("cache");
        fs::create_dir(&cache_dir).unwrap();

        config.fallback_cache.dir = cache_dir;

        // Test Read Write
        config.fallback_cache.rw_mode = CacheModeConfig::ReadWrite;

        {
            let (cache, preprocessor_cache) =
                storage_from_config(&config.cache, &config.fallback_cache).unwrap();

            runtime.block_on(async move {
                cache.put("test1", CacheWrite::default()).await.unwrap();
                preprocessor_cache
                    .put_preprocessor_cache_entry("test1", PreprocessorCacheEntry::default())
                    .await
                    .unwrap();
            });
        }

        // Test Read-only
        config.fallback_cache.rw_mode = CacheModeConfig::ReadOnly;

        {
            let (cache, preprocessor_cache) =
                storage_from_config(&config.cache, &config.fallback_cache).unwrap();

            runtime.block_on(async move {
                assert_eq!(
                    cache
                        .put("test1", CacheWrite::default())
                        .await
                        .unwrap_err()
                        .to_string(),
                    "Cannot write to a read-only cache"
                );
                assert_eq!(
                    preprocessor_cache
                        .put_preprocessor_cache_entry("test1", PreprocessorCacheEntry::default())
                        .await
                        .unwrap_err()
                        .to_string(),
                    "Cannot write to a read-only cache"
                );
            });
        }
    }
}
