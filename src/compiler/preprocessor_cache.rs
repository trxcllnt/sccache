// Copyright 2023 Mozilla Foundation
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

//! The preprocessor cache entry is a description of all information needed
//! to cache pre-processor output in C-family languages for a given input file.
//! The current implementation is very much inspired from the "manifest"
//! that `ccache` uses for its "direct mode", though the on-disk format is
//! different.

use std::{
    collections::HashSet,
    ffi::{OsStr, OsString},
    hash::Hash,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use anyhow::Context;
use futures::lock::Mutex;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    cache::{Cache, Storage},
    compiler::{ColorMode, c::ParsedArguments},
    errors::*,
    lru_disk_cache::{LruCache, lru_cache},
    util::{
        Digest, HashToDigest, MetadataCtimeExt, OsStrExt, Timestamp, path_to_bytes, strip_basedirs,
    },
};

/// The current format is 1 header byte for the version + bincode encoding
/// of the [`PreprocessorCacheEntry`] struct.
const FORMAT_VERSION: u8 = 2;
const MAX_PREPROCESSOR_CACHE_ENTRIES: u64 = 1_000;

#[derive(Clone)]
pub struct PreprocessorCacheEntry {
    /// The digest of a result is computed by hashing the output of the
    /// C preprocessor. Entries correspond to the included files during the
    /// preprocessing step.
    results: LruCache<String, Vec<IncludeEntry>>,
}

impl std::fmt::Debug for PreprocessorCacheEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "PreprocessorCacheEntry {{")?;
        writeln!(f, "  \"version\": {FORMAT_VERSION},")?;
        writeln!(f, "  \"results\": [")?;
        for (key, entries) in self.iter().rev() {
            writeln!(f, "    {{")?;
            writeln!(f, "      \"object_key\": {key:?},")?;
            writeln!(f, "      \"dependencies\": [")?;
            for entry in entries {
                writeln!(f, "        {{")?;
                writeln!(f, "          \"path\": {:?},", entry.path)?;
                writeln!(f, "          \"digest\": {:?},", entry.digest)?;
                writeln!(f, "          \"file_size\": {:?},", entry.file_size)?;
                writeln!(
                    f,
                    "          \"mtime\": {:?},",
                    entry
                        .mtime
                        .and_then(|t| t.to_utc())
                        .map(|t| t.to_string())
                        .unwrap_or_default()
                )?;
                writeln!(
                    f,
                    "          \"ctime\": {:?},",
                    entry
                        .ctime
                        .and_then(|t| t.to_utc())
                        .map(|t| t.to_string())
                        .unwrap_or_default()
                )?;
                writeln!(f, "        }},")?;
            }
            writeln!(f, "      ]")?;
            writeln!(f, "    }},")?;
        }
        writeln!(f, "  ]")?;
        write!(f, "}}")
    }
}

impl Default for PreprocessorCacheEntry {
    fn default() -> Self {
        PreprocessorCacheEntry::new()
    }
}

impl PreprocessorCacheEntry {
    pub fn new() -> Self {
        Self {
            results: LruCache::new(MAX_PREPROCESSOR_CACHE_ENTRIES),
        }
    }

    /// Return the preprocessor cache entry for a given preprocessor key if it exists in storage.
    /// Only applicable when using preprocessor cache mode.
    pub async fn get(storage: &dyn Storage, key: &str) -> Result<Cache<PreprocessorCacheEntry>> {
        use async_compression::futures::bufread::ZlibDecoder as ZlibDecoderAsync;
        use bytes::{Buf, Bytes};
        use futures::{AsyncReadExt, TryFutureExt};

        let res = storage
            .get(&format!("{key}.zz"))
            .and_then(|res| async {
                match res {
                    Cache::Miss => Err(anyhow!("compressed miss")),
                    Cache::Hit(buf) => {
                        let reader = futures::io::AllowStdIo::new(buf.reader());
                        let mut reader = ZlibDecoderAsync::new(reader);
                        let mut out = vec![];
                        reader
                            .read_to_end(&mut out)
                            .await
                            .map_err(anyhow::Error::new)?;
                        Ok(Cache::Hit(Bytes::from(out).into()))
                    }
                }
            })
            .or_else(|_| storage.get(key))
            .await;

        match res {
            Err(err) => Err(err),
            Ok(Cache::Miss) => Ok(Cache::Miss),
            Ok(Cache::Hit(buf)) => Ok(Cache::Hit(
                PreprocessorCacheEntry::deserialize_from(buf.reader()).await?,
            )),
        }
    }

    /// Write a preprocessor cache entry for the given preprocessor key to storage,
    /// overwriting if it exists.
    /// Only applicable when using preprocessor cache mode.
    pub async fn put(self, storage: &dyn Storage, key: &str) -> Result<()> {
        use bytes::{BufMut, BytesMut};
        use flate2::write::ZlibEncoder as ZlibEncoderSync;

        let buf = tokio::task::spawn_blocking(move || {
            let mut writer =
                ZlibEncoderSync::new(BytesMut::new().writer(), flate2::Compression::best());

            self.serialize_to(&mut writer)?;

            writer
                .finish()
                .map_err(anyhow::Error::new)
                .map(|writer| writer.into_inner().freeze().into())
        })
        .await??;

        storage.put(&format!("{key}.zz"), buf).await.map(|_| ())
    }

    fn deserialize<R: std::io::Read>(reader: R) -> Result<Self> {
        let mut entry = Self::new();

        bincode::deserialize_from::<R, Vec<(String, Vec<IncludeEntry>)>>(reader)?
            .into_iter()
            .rev()
            .for_each(|(k, v)| {
                entry.results.insert(k, v);
            });

        Ok(entry)
    }

    /// Tries to deserialize a preprocessor cache entry from `contents`
    pub fn read(contents: &[u8]) -> Result<Self> {
        if contents.is_empty() {
            Ok(Self::new())
        } else if contents[0] != FORMAT_VERSION {
            Err(Error::UnknownFormat(contents[0]).into())
        } else {
            Ok(Self::deserialize(&contents[1..])?)
        }
    }

    pub async fn deserialize_from<R: Read + Send + 'static>(reader: R) -> Result<Self> {
        let mut format = [0u8; 1];
        let mut reader = reader.take(1);
        if let Err(err) = reader.read_exact(&mut format) {
            if matches!(err.kind(), std::io::ErrorKind::UnexpectedEof) {
                trace!("PreprocessorCacheEntry::deserialize_from empty reader");
                return Ok(Self::new());
            }
            return Err(err.into());
        }
        trace!(
            "PreprocessorCacheEntry::deserialize_from format: {}",
            format[0]
        );
        if format[0] != FORMAT_VERSION {
            Err(Error::UnknownFormat(format[0]).into())
        } else {
            trace!("PreprocessorCacheEntry::deserialize_from format is good");
            tokio::task::spawn_blocking(move || Self::deserialize(reader.into_inner())).await?
        }
    }

    /// Serialize the preprocessor cache entry to `buf`
    pub fn serialize_to(&self, mut writer: impl Write) -> std::result::Result<(), Error> {
        // Add the starting byte for version check since `bincode` doesn't
        // support it.
        writer.write_all(&[FORMAT_VERSION])?;
        bincode::serialize_into(writer, &self.iter().rev().collect::<Vec<_>>())?;
        Ok(())
    }

    pub fn to_bytes(&self) -> Result<bytes::Bytes> {
        use bytes::{BufMut, BytesMut};
        let mut writer = BytesMut::new().writer();
        self.serialize_to(&mut writer)?;
        Ok(writer.into_inner().freeze())
    }

    pub fn iter(&self) -> lru_cache::Iter<'_, String, Vec<IncludeEntry>> {
        self.results.iter()
    }

    pub fn iter_mut(&mut self) -> lru_cache::IterMut<'_, String, Vec<IncludeEntry>> {
        self.results.iter_mut()
    }

    /// Insert the full compilation key and included files for a given source file.
    ///
    /// There can be more than one result at once for a source file if one
    /// or more of the include files has changed but not the source file.
    pub fn add_result(
        &mut self,
        preprocessor_key: &str,
        result_key: &str,
        included_files: impl IntoIterator<Item = (String, PathBuf, std::fs::Metadata)>,
        out_pretty: &str,
    ) -> &mut Self {
        let included_files = included_files
            .into_iter()
            .sorted_unstable_by(|a, b| a.1.cmp(&b.1))
            .map(|(digest, path, meta)| IncludeEntry {
                ctime: meta.ctime_or_creation().ok(),
                digest,
                file_size: meta.len(),
                mtime: meta.modified().map(Into::into).ok(),
                path: path.into_os_string(),
            })
            .collect::<Vec<_>>();

        let num_includes = included_files.len();

        self.results.insert(result_key.to_string(), included_files);

        let num_results = self.results.len();

        debug!(
            "[{out_pretty}, {preprocessor_key}, {result_key}]: Added result to preprocessor cache entry: num_includes={num_includes}, num_results={num_results}"
        );

        self
    }

    /// Returns the digest of the first result whose expected included files
    /// are already on disk and have not changed.
    pub async fn lookup_result_digest(
        &mut self,
        preprocessor_dependencies_cache: &PreprocessorDependenciesCache,
        compile_timestamp: chrono::DateTime<chrono::Utc>,
        out_pretty: &str,
        preprocessor_key: &str,
    ) -> (bool, Option<String>) {
        let mut maybe_result = None;
        let mut needs_update = false;

        // Find the first result key whose include files on disk match this
        // preprocessor cache entry. Check newest results first since they're
        // more likely to match.
        for (idx, (result_key, includes)) in self.iter_mut().rev().enumerate() {
            let (failure, updated) = Self::result_matches(
                preprocessor_dependencies_cache,
                compile_timestamp,
                includes,
                out_pretty,
                preprocessor_key,
            )
            .await;

            needs_update = needs_update || updated;

            if let Some(reason) = failure {
                trace!(
                    "[{out_pretty}, {preprocessor_key}, {result_key}]: Preprocessor cache entry lookup failure: {reason}"
                );
            } else {
                if idx != 0 {
                    // Need to write back to storage if the LRU order changes
                    needs_update = true;
                    trace!(
                        "[{out_pretty}, {preprocessor_key}, {result_key}]: Preprocessor cache entry results LRU order changed"
                    );
                }
                maybe_result = Some(result_key.clone());
                break;
            }
        }

        if let Some(ref result_key) = maybe_result {
            // Move the entry to the back of the LRU
            self.results.get(result_key);
        }

        (
            // Signal this preprocessor cache entry was modified and should
            // be written back to storage.
            // Note: This can happen regardless whether a result was found.
            needs_update,
            // Return the object hash
            maybe_result,
        )
    }

    /// A result matches if all of its include files exist on disk and have not changed.
    async fn result_matches(
        preprocessor_dependencies_cache: &PreprocessorDependenciesCache,
        compile_timestamp: chrono::DateTime<chrono::Utc>,
        includes: &mut [IncludeEntry],
        out_pretty: &str,
        preprocessor_key: &str,
    ) -> (Option<String>, bool) {
        let mut failure = None;
        let mut updated = false;

        for prev in includes.iter_mut() {
            let path = Path::new(prev.path.as_os_str());

            let entry = match preprocessor_dependencies_cache
                .get(path, compile_timestamp)
                .await
            {
                Ok(entry) => entry,
                Err(err) => {
                    failure = Some(format!("{err:#}"));
                    break;
                }
            };

            let CachedIncludeEntry {
                entry: curr,
                found_time,
                found_date,
                found_timestamp,
                ..
            } = entry.as_ref();

            if *found_time {
                // We don't know for sure that the program actually uses the __TIME__ macro,
                // but we have to assume it anyway and hash the time stamp. However, that's
                // not very useful since the chance that we get a cache hit later the same
                // second should be quite slim... So, just signal back to the caller that
                // __TIME__ has been found so that the preprocessor cache mode can be disabled.
                failure = Some(format!("Found __TIME__ in {path:?}"));
                break;
            }

            // If the digests are different, disable preprocessor cache mode
            if prev.digest != curr.digest {
                failure = Some(format!("Digest mismatch for {path:?}"));
                break;
            }

            // If __DATE__ or __TIMESTAMP__ found, update the includes for this result.
            if *found_date {
                trace!(
                    "[{out_pretty}, {preprocessor_key}]: updating entry, __DATE__ found in {path:?}"
                );
                *prev = curr.clone();
                updated = true;
            } else if *found_timestamp {
                trace!(
                    "[{out_pretty}, {preprocessor_key}]: updating entry, __TIMESTAMP__ found in {path:?}"
                );
                *prev = curr.clone();
                updated = true;
            }
        }

        (failure, updated)
    }
}

/// Environment variables that are factored into the preprocessor cache entry cached key.
static CACHED_ENV_VARS: LazyLock<HashSet<&'static OsStr>> = LazyLock::new(|| {
    [
        // SCCACHE_C_CUSTOM_CACHE_BUSTER has no particular meaning behind it,
        // serving as a way for the user to factor custom data into the hash.
        // One can set it to different values for different invocations
        // to prevent cache reuse between them.
        "SCCACHE_C_CUSTOM_CACHE_BUSTER",
        "CPATH",
        "C_INCLUDE_PATH",
        "CPLUS_INCLUDE_PATH",
        "OBJC_INCLUDE_PATH",
        "OBJCPLUS_INCLUDE_PATH",
    ]
    .iter()
    .map(OsStr::new)
    .collect()
});

/// Compute the hash key of compiler preprocessing `input` with `args`.
#[allow(clippy::too_many_arguments)]
pub async fn preprocessor_cache_entry_hash_key(
    compiler_digest: &str,
    parsed_args: &ParsedArguments,
    extra_hashes: &[&str],
    env_vars: &[(OsString, OsString)],
    cwd: &Path,
    plusplus: bool,
    basedirs: &[Vec<u8>],
    compile_timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<Option<String>> {
    // If you change any of the inputs to the hash, you should change `FORMAT_VERSION`.

    let mut digest = Digest::new();

    digest.update(compiler_digest.as_bytes());
    // clang and clang++ have different behavior despite being byte-for-byte identical binaries, so
    // we have to incorporate that into the hash as well.
    digest.update(&[plusplus as u8]);
    digest.update(&[FORMAT_VERSION]);
    // Encode the color mode too, because that affects the cached stdout/stderr
    digest.update(&[(parsed_args.color_mode != ColorMode::Off) as u8]);
    digest.update(parsed_args.language.as_str().as_bytes());

    // Hash preprocessor, dependency, common, and arch args
    // If the dependency args change, we need to re-run the preprocessor to generate them
    // common_args is used in preprocessing too
    for arguments in [
        &parsed_args.preprocessor_args[..],
        &parsed_args.dependency_args[..],
        &parsed_args.common_args[..],
        &parsed_args.arch_args[..],
    ] {
        for arg in arguments {
            arg.hash(&mut HashToDigest {
                digest: &mut digest,
            });
        }
    }

    for hash in extra_hashes {
        digest.update(hash.as_bytes());
    }

    for (var, val) in env_vars.iter() {
        if CACHED_ENV_VARS.contains(var.as_os_str()) {
            var.hash(&mut HashToDigest {
                digest: &mut digest,
            });
            digest.update(&b"="[..]);

            // Canonicalize the paths in CPATH and friends, otherwise we
            // can get false-positive preprocessor cache hits when these
            // envvars traverse symlinks
            if matches!(
                var.to_str().unwrap_or_default(),
                "CPATH"
                    | "C_INCLUDE_PATH"
                    | "CPLUS_INCLUDE_PATH"
                    | "OBJC_INCLUDE_PATH"
                    | "OBJCPLUS_INCLUDE_PATH"
            ) {
                #[cfg(windows)]
                let sep = ";";
                #[cfg(not(windows))]
                let sep = ":";

                let mut iter = val.split(sep).map(|path| cwd.join(path));
                let mut next = iter.next();
                let mut val = OsString::new();

                while let Some(path) = next {
                    // Fallback to original path if we can't canonicalize
                    if let Ok(path) = dunce::canonicalize(&path) {
                        val.push(path.as_os_str());
                    } else {
                        val.push(path.as_os_str());
                    }
                    next = iter.next();
                    if next.is_some() {
                        val.push(OsStr::new(sep));
                    }
                }

                val.hash(&mut HashToDigest {
                    digest: &mut digest,
                });
            } else {
                val.hash(&mut HashToDigest {
                    digest: &mut digest,
                });
            }
        }
    }

    let input_path = cwd.join(&parsed_args.input);

    {
        // Hash the input file path, otherwise:
        // - a/r.h exists.
        // - a/x.c has #include "r.h".
        // - b/x.c is identical to a/x.c.
        // - Compiling a/x.c records a/r.h in the preprocessor cache entry.
        // - Compiling b/x.c results in a false cache hit since a/x.c and b/x.c
        // share preprocessor cache entries and a/r.h exists.
        let buf = path_to_bytes(input_path.as_path())?;
        // Strip basedirs from the input file path if configured
        let buf_to_hash = strip_basedirs(&buf, basedirs);
        digest.update(&buf_to_hash);
    }

    digest = {
        let (digest, finder) = digest
            .with_file_and_time_macros(&input_path, compile_timestamp)
            .await?;
        if finder.found_time() {
            // Disable preprocessor cache mode
            debug!("Found __TIME__ in {input_path:?}");
            return Ok(None);
        }
        digest
    };

    Ok(Some(digest.finish()))
}

/// Copied from cargo.
///
/// Normalize a path, removing things like `.` and `..`.
///
/// CAUTION: This does not resolve symlinks (unlike [`std::fs::canonicalize`]).
pub fn normalize_path<P: AsRef<Path>>(path: P) -> PathBuf {
    use std::path::Component;
    let mut components = path.as_ref().components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().copied() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

/// Limited abstraction of `std::fs::Metadata`, allowing us to create fake
/// values during testing.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PreprocessorFileMetadata {
    is_dir: bool,
    is_file: bool,
    modified: Option<Timestamp>,
    ctime_or_creation: Option<Timestamp>,
}

impl From<std::fs::Metadata> for PreprocessorFileMetadata {
    fn from(meta: std::fs::Metadata) -> Self {
        From::from(&meta)
    }
}

impl From<&std::fs::Metadata> for PreprocessorFileMetadata {
    fn from(meta: &std::fs::Metadata) -> Self {
        Self {
            is_dir: meta.is_dir(),
            is_file: meta.is_file(),
            modified: meta.modified().ok().map(Into::into),
            ctime_or_creation: meta.ctime_or_creation().ok(),
        }
    }
}

/// Opt out of preprocessor cache mode because of a race condition.
///
/// The race condition consists of these events:
///
/// - the preprocessor is run
/// - an include file is modified by someone
/// - the new include file is hashed by sccache
/// - the real compiler is run on the preprocessor's output, which contains
///   data from the old header file
/// - the wrong object file is stored in the cache.
pub fn include_is_too_new(
    path: &Path,
    meta: &PreprocessorFileMetadata,
    time_of_compilation: std::time::SystemTime,
) -> bool {
    // The comparison using >= is intentional, due to a possible race between
    // starting compilation and writing the include file.
    if let Some(mtime) = meta.modified
        && mtime >= time_of_compilation.into()
    {
        debug!("Include file {path:?} is too new");
        return true;
    }

    // The same >= logic as above applies to the change time of the file.
    if let Some(ctime) = meta.ctime_or_creation
        && ctime >= time_of_compilation.into()
    {
        debug!("Include file {path:?} is too new");
        return true;
    }

    false
}

/// Corresponds to a cached include file used in the pre-processor stage
#[derive(Clone, Deserialize, Serialize, Debug, Default, PartialEq, Eq)]
pub struct IncludeEntry {
    /// Its absolute path
    pub path: OsString,
    /// The hash of its contents
    pub digest: String,
    /// Its file size, in bytes.
    pub file_size: u64,
    /// Its modification time, `None` if not recorded.
    pub mtime: Option<Timestamp>,
    /// Its status change time, `None` if not recorded.
    pub ctime: Option<Timestamp>,
}

#[derive(Clone)]
pub struct CachedIncludeEntry {
    pub compiled_at: chrono::DateTime<chrono::Utc>,
    pub entry: IncludeEntry,
    pub found_time: bool,
    pub found_date: bool,
    pub found_timestamp: bool,
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Deserialization(bincode::Error),
    UnknownFormat(u8),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Self::Deserialization(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => e.fmt(f),
            Error::Deserialization(e) => e.fmt(f),
            Error::UnknownFormat(format) => f.write_fmt(format_args!(
                "Unknown preprocessor cache entry format {format:x}"
            )),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug)]
pub struct PreprocessorDependenciesCache {
    lru: Arc<Mutex<LruCache<PathBuf, Arc<CachedIncludeEntry>>>>,
}

impl PreprocessorDependenciesCache {
    pub fn new(capacity: u64) -> Self {
        PreprocessorDependenciesCache {
            lru: Arc::new(Mutex::new(LruCache::new(capacity))),
        }
    }
    pub async fn get<P: AsRef<Path>>(
        &self,
        path: P,
        compiled_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Arc<CachedIncludeEntry>> {
        use chrono::Datelike;

        let path = path.as_ref();
        let meta = match tokio::fs::symlink_metadata(path).await {
            Ok(meta) => meta,
            Err(err) => {
                bail!("Error reading metadata for {path:?}: {err:#}");
            }
        };
        let mtime = meta.modified().map(Into::into).ok();
        let ctime = meta.ctime_or_creation().ok();
        let file_size = meta.len();

        let cached = self.lru.lock().await.get(path).and_then(|cached| {
            // If the file hasn't changed, reuse the cached entry
            let file_good = cached.entry.file_size == file_size
                && cached.entry.ctime == ctime
                && cached.entry.mtime == mtime;

            // If __TIMESTAMP__ or __DATE__ were found, compare
            // compilation timestamp to now (or SOURCE_DATE_EPOCH)

            // If neither were found, use this cached include entry
            let time_good = if cached.found_timestamp {
                cached.compiled_at == compiled_at
            }
            // If __TIMESTAMP__ was found, compare timestamp seconds exactly
            else if cached.found_date {
                // If __DATE__ was found, only compare the parts of the
                // timestamp the compiler embeds (day, month, and year)
                let date_0 = cached.compiled_at.date_naive();
                let date_1 = compiled_at.date_naive();
                date_0.day() == date_1.day()
                    && date_0.month() == date_1.month()
                    && date_0.year() == date_1.year()
            } else {
                // If neither were found, use this cached include entry
                true
            };

            if file_good && time_good {
                Some(cached.clone())
            } else {
                None
            }
        });

        let cached = if let Some(cached) = cached {
            cached.clone()
        } else {
            let digest = Digest::from_file_with_time_macros(path, compiled_at)
                .await
                .with_context(|| format!("while reading {path:?}"))
                .inspect_err(|err| trace!("[result_matches]: {err:#}"))
                .map(|(digest, finder)| (digest.finish(), finder));

            let (digest, finder) = match digest {
                Ok(res) => res,
                Err(err) => {
                    bail!("Error computing digest: {err:#}");
                }
            };

            let cached = Arc::new(CachedIncludeEntry {
                compiled_at,
                entry: IncludeEntry {
                    ctime,
                    digest,
                    file_size,
                    mtime,
                    path: path.into(),
                },
                found_time: finder.found_time(),
                found_date: finder.found_date(),
                found_timestamp: finder.found_timestamp(),
            });

            self.lru
                .lock()
                .await
                .insert(path.to_path_buf(), cached.clone());

            cached
        };

        Ok(cached)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        compiler::Language,
        test::utils::*,
        util::{HASH_BUFFER_SIZE, MAX_TIME_MACRO_HAYSTACK_LEN},
    };
    use futures::io::AllowStdIo;

    use super::*;

    #[test]
    fn test_serialize_deserialize() {
        let mut entry_a = PreprocessorCacheEntry::new();
        entry_a.add_result("abc", "def", [], "src.c");

        assert_eq!(entry_a.iter().count(), 1);

        let bytes = entry_a.to_bytes().unwrap();
        let entry_b = PreprocessorCacheEntry::read(&bytes).unwrap();

        assert_eq!(entry_b.iter().count(), 1);
        assert_eq!(bytes, entry_b.to_bytes().unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_find_time_macros_empty_file() {
        let buf: Vec<u8> = vec![];
        let buf = AllowStdIo::new(&buf[..]);
        let hash = Digest::from_reader_with_time_macros(buf).await.unwrap().0;
        assert_eq!(hash.finish(), Digest::new().finish());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_find_time_macros_small_file_no_match() {
        let buf = b"This is a small file, which doesn't contain any time macros.";
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(!finder.found_time_macros());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_find_time_macros_small_file_match() {
        let buf = b"__TIME__";
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(!finder.found_date());
        let buf = b"__DATE__";
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(finder.found_date());
        let buf = b"__TIMESTAMP__";
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(finder.found_timestamp());
        assert!(!finder.found_date());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_find_time_macros_small_file_match_multiple() {
        let buf = b"__TIMESTAMP____DATE____TIME__";
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(finder.found_timestamp());
        assert!(finder.found_date());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_find_time_macros_large_file_no_match() {
        let buf = vec![0; HASH_BUFFER_SIZE * 2];
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(!finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(!finder.found_date());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_find_time_macros_large_file_match_no_overlap() {
        let mut buf = vec![0; HASH_BUFFER_SIZE * 2];
        buf.extend(b"__TIMESTAMP____DATE____TIME__");
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(finder.found_timestamp());
        assert!(finder.found_date());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn test_find_time_macros_large_file_match_overlap() {
        let mut buf = vec![0; HASH_BUFFER_SIZE * 2];
        // Make the pattern overlap two buffer chunks to make sure we account for this
        let start = HASH_BUFFER_SIZE - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__TIMESTAMP__".len()].copy_from_slice(b"__TIMESTAMP__");
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(finder.found_timestamp());
        assert!(!finder.found_date());

        let mut buf = vec![0; HASH_BUFFER_SIZE * 2];
        // Make the pattern overlap two buffer chunks to make sure we account for this
        let start = HASH_BUFFER_SIZE - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__TIME__".len()].copy_from_slice(b"__TIME__");
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(!finder.found_date());

        let mut buf = vec![0; HASH_BUFFER_SIZE * 2];
        // Make the pattern overlap two buffer chunks to make sure we account for this
        let start = HASH_BUFFER_SIZE - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__DATE__".len()].copy_from_slice(b"__DATE__");
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(finder.found_date());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_find_time_macros_large_file_match_overlap_multiple_pages() {
        let mut buf = vec![0; HASH_BUFFER_SIZE * 3];
        // Make the patterns overlap buffer chunks twice to make sure we account for this
        let start = HASH_BUFFER_SIZE - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__TIME__".len()].copy_from_slice(b"__TIME__");
        let start = HASH_BUFFER_SIZE * 2 - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__DATE__".len()].copy_from_slice(b"__DATE__");
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(finder.found_date());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_find_time_macros_large_file_match_overlap_multiple_pages_tiny() {
        let mut buf = vec![0; HASH_BUFFER_SIZE * 3];
        // Make the patterns overlap buffer chunks twice to make sure we account for this
        let start = HASH_BUFFER_SIZE - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__TIME__".len()].copy_from_slice(b"__TIME__");
        let start = HASH_BUFFER_SIZE * 2 - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__DATE__".len()].copy_from_slice(b"__DATE__");
        // Test overlap with the last chunk being less than the haystack
        buf.extend([0; MAX_TIME_MACRO_HAYSTACK_LEN / 2 + 1]);
        let start = HASH_BUFFER_SIZE * 3 - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__TIMESTAMP__".len()].copy_from_slice(b"__TIMESTAMP__");
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(finder.found_timestamp());
        assert!(finder.found_date());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_find_time_macros_ghost_pattern() {
        // Check the (unlikely) case of a pattern being spread between the
        // start of a chunk and its end.
        let mut buf = vec![0; HASH_BUFFER_SIZE * 3];
        buf[HASH_BUFFER_SIZE..HASH_BUFFER_SIZE + b"__TI".len()].copy_from_slice(b"__TI");
        buf[HASH_BUFFER_SIZE * 2 - "ME__".len()..HASH_BUFFER_SIZE * 2].copy_from_slice(b"ME__");
        let buf = AllowStdIo::new(&buf[..]);
        let finder = Digest::from_reader_with_time_macros(buf).await.unwrap().1;
        assert!(!finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(!finder.found_date());
    }

    #[test]
    fn test_preprocessor_cache_entry_hash_key_basedirs() {
        #[cfg(target_os = "windows")]
        use crate::util::normalize_win_path;
        use std::fs;
        use tempfile::TempDir;

        // Create two different base directories
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let dirs = [&dir1, &dir2]
            .iter()
            .map(|dir| {
                let bytes = dir.path().to_string_lossy().into_owned().into_bytes();
                #[cfg(target_os = "windows")]
                return normalize_win_path(&bytes);
                #[cfg(not(target_os = "windows"))]
                bytes
            })
            .collect::<Vec<_>>();

        // Create identical files with the same relative path in each directory
        let file_path = Path::new("test.c");
        let content = b"int main() { return 0; }";
        fs::write(dir1.path().join(file_path), content).unwrap();
        fs::write(dir2.path().join(file_path), content).unwrap();

        let args = ParsedArguments {
            language: Language::C,
            input: file_path.into(),
            ..Default::default()
        };

        let compile_timestamp = chrono::Utc::now();

        // Test 1: With basedirs, hashes should be the same
        let hash1_with_basedirs = preprocessor_cache_entry_hash_key(
            "test_digest",
            &args,
            &[],
            &[],
            dir1.path(),
            false,
            &dirs,
            compile_timestamp,
        )
        .wait()
        .unwrap()
        .unwrap();

        let hash2_with_basedirs = preprocessor_cache_entry_hash_key(
            "test_digest",
            &args,
            &[],
            &[],
            dir2.path(),
            false,
            &dirs,
            compile_timestamp,
        )
        .wait()
        .unwrap()
        .unwrap();

        assert_eq!(
            hash1_with_basedirs, hash2_with_basedirs,
            "Hashes should be equal when using basedirs with identical files in different directories"
        );

        // Test 2: With basedir1 for first, and basedir2 for second, hashes should be the same
        let hash1_with_basedirs = preprocessor_cache_entry_hash_key(
            "test_digest",
            &args,
            &[],
            &[],
            dir1.path(),
            false,
            &dirs[..1],
            compile_timestamp,
        )
        .wait()
        .unwrap()
        .unwrap();

        let hash2_with_basedirs = preprocessor_cache_entry_hash_key(
            "test_digest",
            &args,
            &[],
            &[],
            dir2.path(),
            false,
            &dirs[1..],
            compile_timestamp,
        )
        .wait()
        .unwrap()
        .unwrap();

        assert_eq!(
            hash1_with_basedirs, hash2_with_basedirs,
            "Hashes should be equal when using basedirs with identical files in different directories"
        );

        // Test 3: Without basedirs, hashes should be different
        let hash1_no_basedirs = preprocessor_cache_entry_hash_key(
            "test_digest",
            &args,
            &[],
            &[],
            dir1.path(),
            false,
            &[],
            compile_timestamp,
        )
        .wait()
        .unwrap()
        .unwrap();

        let hash2_no_basedirs = preprocessor_cache_entry_hash_key(
            "test_digest",
            &args,
            &[],
            &[],
            dir2.path(),
            false,
            &[],
            compile_timestamp,
        )
        .wait()
        .unwrap()
        .unwrap();

        assert_ne!(
            hash1_no_basedirs, hash2_no_basedirs,
            "Hashes should be different without basedirs for files in different directories"
        );
    }
}
