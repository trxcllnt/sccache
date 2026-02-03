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
    collections::{HashMap, HashSet},
    ffi::{OsStr, OsString},
    hash::Hash,
    io::{self, Read, Write},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use anyhow::Context;
use futures::lock::Mutex;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    errors::*,
    lru_disk_cache::{LruCache, lru_cache},
    util::{Digest, HashToDigest, MetadataCtimeExt, OsStrExt, Timestamp, decode_path, encode_path},
};

use super::Language;

/// The current format is 1 header byte for the version + bincode encoding
/// of the [`PreprocessorCacheEntry`] struct.
const FORMAT_VERSION: u8 = 1;
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
        let results = self.iter().rev().collect::<Vec<_>>();
        write!(f, "PreprocessorCacheEntry {{ results: {results:?} }}")
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
            "Added result to preprocessor cache entry (preprocessor_key={preprocessor_key:?}, result_key={result_key:?}, num_includes={num_includes}, num_results={num_results})"
        );

        self
    }

    /// Returns the digest of the first result whose expected included files
    /// are already on disk and have not changed.
    pub async fn lookup_result_digest(
        &mut self,
        preprocessor_dependencies_cache: &Mutex<LruCache<PathBuf, Arc<Result<CachedIncludeEntry>>>>,
        env_vars: &[(OsString, OsString)],
    ) -> (bool, Option<String>) {
        let mut maybe_result = None;
        let mut needs_update = false;
        let compile_timestamp = env_vars
            .iter()
            .find(|(k, _)| k == "SOURCE_DATE_EPOCH")
            .and_then(|(_, v)| v.as_os_str().to_str())
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| chrono::Utc::now().timestamp());

        // Find the first result key whose include files on disk match this
        // preprocessor cache entry. Check newest results first since they're
        // more likely to match.
        for (idx, (result_key, includes)) in self.iter_mut().rev().enumerate() {
            if let Some(updated) = Self::result_matches(
                preprocessor_dependencies_cache,
                compile_timestamp,
                includes,
                env_vars,
            )
            .await
            {
                needs_update = needs_update || updated;
                // Need to write back to storage if the LRU order changes
                needs_update = needs_update || idx != 0;
                maybe_result = Some(result_key.clone());
                break;
            }
        }

        if let Some(result_key) = maybe_result.as_ref() {
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
        preprocessor_dependencies_cache: &Mutex<LruCache<PathBuf, Arc<Result<CachedIncludeEntry>>>>,
        compile_timestamp: i64,
        includes: &mut [IncludeEntry],
        env_vars: &[(OsString, OsString)],
    ) -> Option<bool> {
        use std::ops::Deref;

        let mut updated = false;

        for prev in includes.iter_mut() {
            let path = Path::new(prev.path.as_os_str());
            let meta = tokio::fs::symlink_metadata(path).await.ok()?;
            let mtime = meta.modified().map(Into::into).ok();
            let ctime = meta.ctime_or_creation().ok();
            let file_size = meta.len();

            let seen = match preprocessor_dependencies_cache.lock().await.get(path) {
                Some(seen) => seen
                    .deref()
                    .as_ref()
                    .ok()
                    .filter(|cached| {
                        // If the file hasn't changed, reuse the cached entry
                        if cached.entry.file_size != file_size
                            || cached.entry.ctime != ctime
                            || cached.entry.mtime != mtime
                        {
                            false
                        }
                        // If __DATE__ or __TIMESTAMP__ were found, compare
                        // compilation timestamp to now (or SOURCE_DATE_EPOCH)
                        else if cached.found_date || cached.found_timestamp {
                            cached.compiled_at == compile_timestamp
                        } else {
                            true
                        }
                    })
                    .map(|_| seen.clone()),
                _ => None,
            };

            let cached = if let Some(seen) = seen {
                seen.clone()
            } else {
                let digest = Digest::from_file_with_time_macros(path, env_vars)
                    .await
                    .with_context(|| format!("while reading {}", path.display()))
                    .inspect_err(|err| trace!("[result_matches]: {err:#}"))
                    .map(|(digest, finder)| (digest.finish(), finder));

                let (digest, finder) = match digest {
                    Ok(res) => res,
                    Err(err) => {
                        preprocessor_dependencies_cache
                            .lock()
                            .await
                            .insert(path.to_path_buf(), Arc::new(Err(err)));
                        return None;
                    }
                };

                let cached = Arc::new(Ok(CachedIncludeEntry {
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
                    compiled_at: compile_timestamp,
                }));

                preprocessor_dependencies_cache
                    .lock()
                    .await
                    .insert(path.to_path_buf(), cached.clone());

                cached
            };

            let CachedIncludeEntry {
                entry: curr,
                found_time,
                found_date,
                found_timestamp,
                ..
            } = cached.deref().as_ref().unwrap();

            if *found_time {
                // We don't know for sure that the program actually uses the __TIME__ macro,
                // but we have to assume it anyway and hash the time stamp. However, that's
                // not very useful since the chance that we get a cache hit later the same
                // second should be quite slim... So, just signal back to the caller that
                // __TIME__ has been found so that the preprocessor cache mode can be disabled.
                debug!("Found __TIME__ in {path:?}");
                return None;
            }

            // If the digests are different, disable preprocessor cache mode
            if prev.digest != curr.digest {
                return None;
            }

            // If __DATE__ or __TIMESTAMP__ found, update the includes for this result.
            if *found_date || *found_timestamp {
                *prev = curr.clone();
                updated = true;
            }
        }

        Some(updated)
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
    language: Language,
    arguments: &[OsString],
    extra_hashes: &[String],
    env_vars: &[(OsString, OsString)],
    cwd: &Path,
    input: &Path,
    plusplus: bool,
) -> Result<Option<String>> {
    // If you change any of the inputs to the hash, you should change `FORMAT_VERSION`.

    let mut digest = Digest::new();

    digest.update(compiler_digest.as_bytes());
    // clang and clang++ have different behavior despite being byte-for-byte identical binaries, so
    // we have to incorporate that into the hash as well.
    digest.update(&[plusplus as u8]);
    digest.update(&[FORMAT_VERSION]);
    digest.update(language.as_str().as_bytes());

    for arg in arguments {
        arg.hash(&mut HashToDigest {
            digest: &mut digest,
        });
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

    let input_path = cwd.join(input);

    {
        // Hash the input file path, otherwise:
        // - a/r.h exists.
        // - a/x.c has #include "r.h".
        // - b/x.c is identical to a/x.c.
        // - Compiling a/x.c records a/r.h in the preprocessor cache entry.
        // - Compiling b/x.c results in a false cache hit since a/x.c and b/x.c
        // share preprocessor cache entries and a/r.h exists.
        let mut buf = vec![];
        encode_path(&mut buf, &input_path)?;
        digest.update(&buf);
    }

    digest = {
        let (digest, finder) = digest
            .with_file_and_time_macros(&input_path, env_vars)
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

const PRAGMA_GCC_PCH_PREPROCESS: &[u8] = b"pragma GCC pch_preprocess";
const HASH_31_COMMAND_LINE_NEWLINE: &[u8] = b"# 31 \"<command-line>\"\n";
const HASH_32_COMMAND_LINE_2_NEWLINE: &[u8] = b"# 32 \"<command-line>\" 2\n";
const INCBIN_DIRECTIVE: &[u8] = b".incbin";

/// Remember the include files in the preprocessor output if it can be cached.
/// Returns `Ok(None)` if preprocessor cache mode should be disabled.
pub fn process_preprocessed_file(
    input_file: &Path,
    cwd: &Path,
    bytes: &mut [u8],
    fs_impl: impl PreprocessorFSAbstraction,
    skip_system_headers: bool,
) -> Result<Option<Vec<PathBuf>>> {
    let mut start = 0;
    let mut hash_start = 0;
    let total_len = bytes.len();
    let mut included_files = HashMap::new();
    let mut normalized_include_paths = HashMap::new();
    // There must be at least 7 characters (# 1 "x") left to potentially find an
    // include file path.
    while start < total_len.saturating_sub(7) {
        let mut slice = &bytes[start..];
        // Check if we look at a line containing the file name of an included file.
        // At least the following formats exist (where N is a positive integer):
        //
        // GCC:
        //
        //   # N "file"
        //   # N "file" N
        //   #pragma GCC pch_preprocess "file"
        //
        // HP's compiler:
        //
        //   #line N "file"
        //
        // AIX's compiler:
        //
        //   #line N "file"
        //   #line N
        //
        // Note that there may be other lines starting with '#' left after
        // preprocessing as well, for instance "#    pragma".
        if slice[0] == b'#'
        // GCC:
        && ((slice[1] == b' ' && slice[2] >= b'0' && slice[2] <= b'9')
            // GCC precompiled header:
            || slice[1..].starts_with(PRAGMA_GCC_PCH_PREPROCESS)
            // HP/AIX:
            || &slice[1..6] == b"line ")
        && (start == 0 || bytes[start - 1] == b'\n')
        {
            match process_preprocessor_line(
                input_file,
                cwd,
                &mut included_files,
                bytes,
                start,
                hash_start,
                total_len,
                &mut normalized_include_paths,
                &fs_impl,
                skip_system_headers,
            )? {
                ControlFlow::Continue((s, h)) => {
                    start = s;
                    hash_start = h;
                }
                ControlFlow::Break((s, h, continue_preprocessor_cache_mode)) => {
                    if !continue_preprocessor_cache_mode {
                        return Ok(None);
                    }
                    start = s;
                    hash_start = h;
                    continue;
                }
            };
        } else if slice
            .strip_prefix(INCBIN_DIRECTIVE)
            .filter(|slice| {
                slice.starts_with(b"\"") || slice.starts_with(b" \"") || slice.starts_with(b" \\\"")
            })
            .is_some()
        {
            // An assembler .inc bin (without the space) statement, which could be
            // part of inline assembly, refers to an external file. If the file
            // changes, the hash should change as well, but finding out what file to
            // hash is too hard for sccache, so just bail out.
            debug!("Found potential unsupported .inc bin directive in source code");
            return Ok(None);
        } else if slice.starts_with(b"___________") && (start == 0 || bytes[start - 1] == b'\n') {
            // Unfortunately the distcc-pump wrapper outputs standard output lines:
            // __________Using distcc-pump from /usr/bin
            // __________Using # distcc servers in pump mode
            // __________Shutting down distcc-pump include server
            while start < total_len && slice[0] != b'\n' {
                start += 1;
                if start < total_len {
                    slice = &bytes[start..];
                }
            }
            slice = &bytes[start..];
            if slice[0] == b'\n' {
                start += 1;
            }
            hash_start = start;
            continue;
        } else {
            start += 1;
        }
    }
    Ok(Some(
        included_files.drain().map(|(k, _)| k).collect::<Vec<_>>(),
    ))
}

/// What to do after handling a preprocessor number line.
/// The `Break` variant is `(start, hash_start, continue_preprocessor_cache_mode)`.
/// The `Continue` variant is `(start, hash_start)`.
type PreprocessedLineAction = ControlFlow<(usize, usize, bool), (usize, usize)>;

#[allow(clippy::too_many_arguments)]
fn process_preprocessor_line(
    input_file: &Path,
    cwd: &Path,
    included_files: &mut HashMap<PathBuf, bool>,
    bytes: &mut [u8],
    mut start: usize,
    mut hash_start: usize,
    total_len: usize,
    normalized_include_paths: &mut HashMap<Vec<u8>, Option<Vec<u8>>>,
    fs_impl: &impl PreprocessorFSAbstraction,
    skip_system_headers: bool,
) -> Result<PreprocessedLineAction> {
    let mut slice = &bytes[start..];
    // Workarounds for preprocessor linemarker bugs in GCC version 6.
    if slice.get(2) == Some(&b'3') {
        if slice.starts_with(HASH_31_COMMAND_LINE_NEWLINE) {
            // Bogus extra line with #31, after the regular #1:
            // Ignore the whole line, and continue parsing.
            while start < hash_start && slice[0] != b'\n' {
                start += 1;
            }
            start += 1;
            hash_start = start;
            return Ok(ControlFlow::Break((start, hash_start, true)));
        } else if slice.starts_with(HASH_32_COMMAND_LINE_2_NEWLINE) {
            // Bogus wrong line with #32, instead of regular #1:
            // Replace the line number with the usual one.
            start += 1;
            bytes[start..=start + 2].copy_from_slice(b"# 1");
            hash_start = start;
            slice = &bytes[start..];
        }
    }
    while start < total_len && slice[0] != b'"' && slice[0] != b'\n' {
        start += 1;
        if start < total_len {
            slice = &bytes[start..];
        }
    }
    slice = &bytes[start..];
    if start < total_len && slice[0] == b'\n' {
        // a newline before the quotation mark -> no match
        return Ok(ControlFlow::Break((start, hash_start, true)));
    }
    start += 1;
    if start >= total_len {
        bail!("Failed to parse included file path");
    }
    // `start` points to the beginning of an include file path
    hash_start = start;
    slice = &bytes[start..];
    while start < total_len && slice[0] != b'"' {
        start += 1;
        if start < total_len {
            slice = &bytes[start..];
        }
    }
    if start == hash_start {
        // Skip empty file name.
        return Ok(ControlFlow::Break((start, hash_start, true)));
    }
    // Look for preprocessor flags, after the "filename".
    let mut system = false;
    let mut pointer = start + 1;
    while pointer < total_len && bytes[pointer] != b'\n' {
        if bytes[pointer] == b'3' {
            // System header.
            system = true;
        }
        pointer += 1;
    }

    // `hash_start` and `start` span the include file path.
    let include_path = &bytes[hash_start..start];
    // We need to normalize the path now since it's part of the
    // hash and since we need to deduplicate the include files.
    // We cache the results since they are often quite a bit repeated.
    let include_path: &[u8] = if let Some(opt) = normalized_include_paths.get(include_path) {
        match opt {
            Some(normalized) => normalized,
            None => include_path,
        }
    } else {
        let path_buf = decode_path(include_path)?;
        let normalized = normalize_path(&path_buf);
        if normalized == path_buf {
            // `None` is a marker that the normalization is the same
            normalized_include_paths.insert(include_path.to_owned(), None);
            include_path
        } else {
            let mut encoded = Vec::with_capacity(include_path.len());
            encode_path(&mut encoded, &normalized)?;
            normalized_include_paths
                .entry(include_path.to_owned())
                .or_insert(Some(encoded))
                .as_ref()
                .unwrap()
        }
    };

    if !remember_include_file(
        include_path,
        input_file,
        cwd,
        included_files,
        fs_impl,
        system,
        skip_system_headers,
    )? {
        return Ok(ControlFlow::Break((start, hash_start, false)));
    };
    // Everything of interest between hash_start and start has been hashed now.
    hash_start = start;
    Ok(ControlFlow::Continue((start, hash_start)))
}

/// Copied from cargo.
///
/// Normalize a path, removing things like `.` and `..`.
///
/// CAUTION: This does not resolve symlinks (unlike [`std::fs::canonicalize`]).
pub fn normalize_path(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
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

/// An abstraction to filesystem access for use during the preprocessor
/// caching phase, to make testing easier.
///
/// This may help non-local preprocessor caching in the future, if it ends up
/// being viable.
pub trait PreprocessorFSAbstraction {
    fn metadata(&self, path: impl AsRef<Path>) -> io::Result<PreprocessorFileMetadata> {
        std::fs::metadata(path).map(Into::into)
    }
}

/// Provides filesystem access with the expected standard library functions.
pub struct StandardFsAbstraction;

impl PreprocessorFSAbstraction for StandardFsAbstraction {}

// Returns false if the include file was "too new" (meaning modified during or
// after the start of the compilation) and therefore should disable
// the preprocessor cache mode, otherwise true.
#[allow(clippy::too_many_arguments)]
fn remember_include_file(
    mut path: &[u8],
    input_file: &Path,
    cwd: &Path,
    included_files: &mut HashMap<PathBuf, bool>,
    fs_impl: &impl PreprocessorFSAbstraction,
    #[allow(unused_variables)] system: bool,
    #[allow(unused_variables)] skip_system_headers: bool,
) -> Result<bool> {
    // TODO if precompiled header.
    if path.len() >= 2 && path[0] == b'<' && path[path.len() - 1] == b'>' {
        // Typically <built-in> or <command-line>.
        return Ok(true);
    }

    #[cfg(test)]
    // Allow skipping system includes in tests
    if system && skip_system_headers {
        // Don't remember this system header.
        return Ok(true);
    }

    // Canonicalize path for comparison; Clang uses ./header.h.
    #[cfg(windows)]
    {
        if path.starts_with(br".\") || path.starts_with(b"./") {
            path = &path[2..];
        }
    }
    #[cfg(not(windows))]
    {
        if path.starts_with(b"./") {
            path = &path[2..];
        }
    }

    let path = cwd.join(decode_path(path).context("failed to decode path")?);

    if included_files.contains_key(&path) {
        // Already known include file
        return Ok(true);
    }

    if path == input_file {
        // Don't remember the input file.
        return Ok(true);
    }

    let meta = match fs_impl.metadata(&path) {
        Ok(meta) => meta,
        Err(e) => {
            debug!("Failed to stat include file {path:?}: {e}");
            return Ok(false);
        }
    };

    if meta.is_dir {
        // Ignore directory, typically $PWD.
        return Ok(true);
    }

    if !meta.is_file {
        // Device, pipe, socket or other strange creature.
        debug!("Non-regular include file {path:?}");
        return Ok(false);
    }

    // Remember this include file
    included_files.insert(path, true);

    Ok(true)
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
    if let Some(mtime) = meta.modified {
        if mtime >= time_of_compilation.into() {
            debug!("Include file {path:?} is too new");
            return true;
        }
    }

    // The same >= logic as above applies to the change time of the file.
    if let Some(ctime) = meta.ctime_or_creation {
        if ctime >= time_of_compilation.into() {
            debug!("Include file {path:?} is too new");
            return true;
        }
    }

    false
}

/// Corresponds to a cached include file used in the pre-processor stage
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct IncludeEntry {
    /// Its absolute path
    path: OsString,
    /// The hash of its contents
    digest: String,
    /// Its file size, in bytes.
    file_size: u64,
    /// Its modification time, `None` if not recorded.
    mtime: Option<Timestamp>,
    /// Its status change time, `None` if not recorded.
    ctime: Option<Timestamp>,
}

pub struct CachedIncludeEntry {
    entry: IncludeEntry,
    found_time: bool,
    found_date: bool,
    found_timestamp: bool,
    compiled_at: i64,
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

#[cfg(test)]
mod test {
    use crate::util::{HASH_BUFFER_SIZE, MAX_TIME_MACRO_HAYSTACK_LEN};
    use futures::io::AllowStdIo;
    use std::{collections::VecDeque, sync::Mutex};

    use super::*;

    #[test]
    fn test_serialize_deserialize() {
        let mut entry_a = PreprocessorCacheEntry::new();
        entry_a.add_result("abc", "def", []);

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
    fn test_process_preprocessed_file() {
        env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init()
            .ok();
        let input_file = Path::new("tests/test.c");
        let path = Path::new(file!())
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        // This should be portable since the only headers present in this
        // output are system headers, which aren't interacted with
        // on the filesystem if configured.
        let path = path.join("tests/test.c.gcc-13.2.0-preproc");
        let mut bytes = std::fs::read(path).unwrap();
        let original_bytes = bytes.clone();

        let result = process_preprocessed_file(
            input_file,
            Path::new(""),
            &mut bytes,
            StandardFsAbstraction,
            true,
        )
        .unwrap_or(None);

        assert_eq!(&bytes, &original_bytes);

        assert!(result.is_some());

        let include_files = result.unwrap();
        assert_eq!(include_files.len(), 0);
    }

    /// A filesystem interface that only panics to test that we don't access it.
    struct PanicFs;

    impl PreprocessorFSAbstraction for PanicFs {
        fn metadata(&self, path: impl AsRef<Path>) -> io::Result<PreprocessorFileMetadata> {
            panic!("called metadata at {}", path.as_ref().display());
        }
    }

    /// A filesystem interface that gives back expected values.
    struct TestFs {
        metadata_results: Mutex<VecDeque<(PathBuf, PreprocessorFileMetadata)>>,
    }

    impl PreprocessorFSAbstraction for TestFs {
        fn metadata(&self, path: impl AsRef<Path>) -> io::Result<PreprocessorFileMetadata> {
            let (expected_path, meta) = self
                .metadata_results
                .lock()
                .unwrap()
                .pop_front()
                .expect("not enough 'metadata' results");
            assert_eq!(expected_path, path.as_ref(), "{}", path.as_ref().display());
            Ok(meta)
        }
    }

    // Short-circuit the parameters we don't need to change during tests
    fn do_single_preprocessor_line_call(
        line: &[u8],
        include_files: &mut HashMap<PathBuf, bool>,
        fs_impl: &impl PreprocessorFSAbstraction,
        skip_system_headers: bool,
    ) -> PreprocessedLineAction {
        let input_file = Path::new("tests/test.c");

        let mut bytes = line.to_vec();
        let total_len = bytes.len();
        process_preprocessor_line(
            input_file,
            Path::new(""),
            include_files,
            &mut bytes,
            0,
            0,
            total_len,
            &mut HashMap::new(),
            fs_impl,
            skip_system_headers,
        )
        .unwrap()
    }

    /// Test cases where we don't access the filesystem
    #[test]
    fn test_process_preprocessor_line_simple() {
        env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init()
            .ok();

        let mut include_files = HashMap::new();
        assert_eq!(
            do_single_preprocessor_line_call(
                br#"// # 0 "tests/test.c""#,
                &mut include_files,
                &PanicFs,
                true,
            ),
            ControlFlow::Continue((20, 20)),
        );
        assert_eq!(include_files.len(), 0);

        assert_eq!(
            do_single_preprocessor_line_call(
                br#"// # 0 "<built-in>""#,
                &mut include_files,
                &PanicFs,
                true,
            ),
            ControlFlow::Continue((18, 18)),
        );
        assert_eq!(include_files.len(), 0);

        assert_eq!(
            do_single_preprocessor_line_call(
                br#"// # 0 "<command-line>""#,
                &mut include_files,
                &PanicFs,
                true,
            ),
            ControlFlow::Continue((22, 22)),
        );
        assert_eq!(include_files.len(), 0);

        assert_eq!(
            do_single_preprocessor_line_call(
                br#"// # 0 "<command-line>" 2"#,
                &mut include_files,
                &PanicFs,
                true,
            ),
            ControlFlow::Continue((22, 22)),
        );
        assert_eq!(include_files.len(), 0);

        assert_eq!(
            do_single_preprocessor_line_call(
                br#"// # 1 "tests/test.c""#,
                &mut include_files,
                &PanicFs,
                true,
            ),
            ControlFlow::Continue((20, 20)),
        );
        assert_eq!(include_files.len(), 0);

        assert_eq!(
            do_single_preprocessor_line_call(
                br#"// # 1 "/usr/include/stdc-predef.h" 1 3 4"#,
                &mut include_files,
                &PanicFs,
                true,
            ),
            ControlFlow::Continue((34, 34)),
        );
        assert_eq!(include_files.len(), 0);
    }

    /// Test cases where we test our tests...
    #[test]
    fn test_test_helpers() {
        env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init()
            .ok();

        // Test PanicFs
        let res = std::panic::catch_unwind(|| {
            let mut include_files = HashMap::new();
            assert_eq!(
                do_single_preprocessor_line_call(
                    br#"// # 1 "/usr/include/stdc-predef.h" 1 3 4"#,
                    &mut include_files,
                    &PanicFs,
                    false,
                ),
                ControlFlow::Continue((34, 34)),
            );
        });
        assert_eq!(
            res.unwrap_err().downcast_ref::<String>().unwrap(),
            "called metadata at /usr/include/stdc-predef.h"
        );

        // Test TestFs's safeguard
        let res = std::panic::catch_unwind(|| {
            let mut include_files = HashMap::new();
            let fs_impl = TestFs {
                metadata_results: Mutex::new(VecDeque::new()),
            };
            assert_eq!(
                do_single_preprocessor_line_call(
                    br#"// # 33 "/usr/include/x86_64-linux-gnu/bits/libc-header-start.h" 3 4"#,
                    &mut include_files,
                    &fs_impl,
                    false,
                ),
                ControlFlow::Continue((34, 34)),
            );
        });
        assert_eq!(
            res.unwrap_err().downcast_ref::<String>().unwrap(),
            "not enough 'metadata' results"
        );
    }

    /// Test cases where we test filesystem access
    #[test]
    fn test_process_preprocessor_line_fs_access() {
        env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init()
            .ok();

        // Test invalid include file is actually a dir
        let mut include_files = HashMap::new();
        let fs_impl = TestFs {
            metadata_results: Mutex::new(
                [(
                    PathBuf::from("/usr/include/x86_64-linux-gnu/bits/libc-header-start.h"),
                    PreprocessorFileMetadata {
                        is_dir: true,
                        is_file: false,
                        modified: Some(Timestamp::new(12341234, 0)),
                        ctime_or_creation: None,
                    },
                )]
                .into_iter()
                .collect(),
            ),
        };
        assert_eq!(
            do_single_preprocessor_line_call(
                br#"// # 33 "/usr/include/x86_64-linux-gnu/bits/libc-header-start.h" 3 4"#,
                &mut include_files,
                &fs_impl,
                false,
            ),
            // preprocessor cache mode is *not* disabled,
            ControlFlow::Continue((63, 63)),
        );
        assert_eq!(include_files.len(), 0);

        // Test correct include file
        let mut include_files = HashMap::new();
        let fs_impl = TestFs {
            metadata_results: Mutex::new(
                [(
                    PathBuf::from("/usr/include/x86_64-linux-gnu/bits/libc-header-start.h"),
                    PreprocessorFileMetadata {
                        is_dir: false,
                        is_file: true,
                        modified: Some(Timestamp::new(12341234, 0)),
                        ctime_or_creation: None,
                    },
                )]
                .into_iter()
                .collect(),
            ),
        };
        assert_eq!(
            do_single_preprocessor_line_call(
                br#"// # 33 "/usr/include/x86_64-linux-gnu/bits/libc-header-start.h" 3 4"#,
                &mut include_files,
                &fs_impl,
                false,
            ),
            ControlFlow::Continue((63, 63)),
        );
        assert_eq!(include_files.len(), 1);
    }
}
