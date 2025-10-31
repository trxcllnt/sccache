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
    collections::{BTreeMap, HashMap, HashSet},
    ffi::{OsStr, OsString},
    hash::Hash,
    io::{self, Read, Seek, Write},
    ops::ControlFlow,
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::Context;
use chrono::Datelike;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::{
    cache::PreprocessorCacheModeConfig,
    errors::*,
    util::{
        Digest, HashToDigest, MetadataCtimeExt, TimeMacroFinder, Timestamp, decode_path,
        encode_path,
    },
};

use super::Language;

/// The current format is 1 header byte for the version + bincode encoding
/// of the [`PreprocessorCacheEntry`] struct.
const FORMAT_VERSION: u8 = 0;
const MAX_PREPROCESSOR_CACHE_ENTRIES: usize = 100;
const MAX_PREPROCESSOR_CACHE_FILE_INFO_ENTRIES: usize = 10000;

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq)]
pub struct PreprocessorCacheEntry {
    /// A counter of the overall number of [`IncludeEntry`] in this
    /// preprocessor cache entry, as an optimization when checking
    /// we're not ballooning in size.
    number_of_entries: usize,
    /// The digest of a result is computed by hashing the output of the
    /// C preprocessor. Entries correspond to the included files during the
    /// preprocessing step.
    results: BTreeMap<String, Vec<IncludeEntry>>,
}

impl PreprocessorCacheEntry {
    pub fn new() -> Self {
        Default::default()
    }

    /// Tries to deserialize a preprocessor cache entry from `contents`
    pub fn read(contents: &[u8]) -> std::result::Result<Self, Error> {
        if contents.is_empty() {
            Ok(Self {
                number_of_entries: 0,
                results: Default::default(),
            })
        } else if contents[0] != FORMAT_VERSION {
            Err(Error::UnknownFormat(contents[0]))
        } else {
            Ok(bincode::deserialize(&contents[1..])?)
        }
    }

    pub async fn deserialize_from<R: Read + Send + 'static>(
        reader: R,
    ) -> std::result::Result<Self, Error> {
        let mut format = [0u8; 1];
        let mut reader = reader.take(1);
        if let Err(err) = reader.read_exact(&mut format) {
            if matches!(err.kind(), std::io::ErrorKind::UnexpectedEof) {
                trace!("PreprocessorCacheEntry::deserialize_from empty reader");
                return Ok(Self {
                    number_of_entries: 0,
                    results: Default::default(),
                });
            }
            return Err(err.into());
        }
        trace!(
            "PreprocessorCacheEntry::deserialize_from format: {}",
            format[0]
        );
        if format[0] != FORMAT_VERSION {
            Err(Error::UnknownFormat(format[0]))
        } else {
            trace!("PreprocessorCacheEntry::deserialize_from format is good");
            tokio::task::spawn_blocking(move || {
                bincode::deserialize_from::<R, Self>(reader.into_inner())
                    .map_err(|err| Error::Io(std::io::Error::other(err)))
            })
            .await
            .map_err(|err| Error::Io(std::io::Error::other(err)))?
        }
    }

    /// Serialize the preprocessor cache entry to `buf`
    pub fn serialize_to(&self, mut buf: impl Write) -> std::result::Result<(), Error> {
        // Add the starting byte for version check since `bincode` doesn't
        // support it.
        buf.write_all(&[FORMAT_VERSION])?;
        bincode::serialize_into(buf, self)?;
        Ok(())
    }

    pub async fn serialize_into(self) -> std::result::Result<std::io::Cursor<Vec<u8>>, Error> {
        tokio::task::spawn_blocking(move || {
            let mut cursor = std::io::Cursor::new(vec![]);
            self.serialize_to(&mut cursor)?;
            cursor.seek(std::io::SeekFrom::Start(0))?;
            std::result::Result::Ok(cursor)
        })
        .await
        .map_err(|err| Error::Io(std::io::Error::other(err)))?
    }

    /// Insert the full compilation key and included files for a given source file.
    ///
    /// There can be more than one result at once for a source file if one
    /// or more of the include files has changed but not the source file.
    pub fn add_result(
        &mut self,
        compilation_time_start: SystemTime,
        preprocessor_key: &str,
        result_key: &str,
        included_files: impl IntoIterator<Item = (String, PathBuf)>,
    ) {
        if self.results.len() > MAX_PREPROCESSOR_CACHE_ENTRIES {
            // Normally, there shouldn't be many result entries in the
            // preprocessor cache entry since new entries are added only if
            // an include file has changed but not the source file, and you
            // typically change source files more often than header files.
            // However, it's certainly possible to imagine cases where the
            // preprocessor cache entry will grow large (for instance,
            // a generated header file that changes for every build), and this
            // must be taken care of since processing an ever growing
            // preprocessor cache entry eventually will take too much time.
            // A good way of solving this would be to maintain the
            // result entries in LRU order and discarding the old ones.
            // An easy way is to throw away all entries when there are too many.
            // Let's do that for now.
            debug!(
                "Too many entries in preprocessor cache entry file ({}/{}), starting over",
                self.results.len(),
                MAX_PREPROCESSOR_CACHE_ENTRIES
            );
            self.results.clear();
            self.number_of_entries = 0;
        }
        let includes: std::result::Result<Vec<_>, std::io::Error> = included_files
            .into_iter()
            .map(|(digest, path)| {
                let meta = std::fs::symlink_metadata(&path)?;
                let mtime: Option<Timestamp> = meta.modified().ok().map(|t| t.into());
                let ctime = meta.ctime_or_creation().ok();

                let should_cache_time = match (mtime, ctime) {
                    (Some(mtime), Some(ctime)) => {
                        Timestamp::from(compilation_time_start) > mtime.max(ctime)
                    }
                    _ => false,
                };
                Ok(IncludeEntry {
                    path: path.into_os_string(),
                    digest,
                    file_size: meta.len(),
                    mtime: if should_cache_time { mtime } else { None },
                    ctime: if should_cache_time { ctime } else { None },
                })
            })
            .collect();
        match includes {
            Ok(includes) => {
                let new_number_of_entries = includes.len() + self.number_of_entries;
                if new_number_of_entries > MAX_PREPROCESSOR_CACHE_FILE_INFO_ENTRIES {
                    // Rarely, entries can grow large in pathological cases
                    // where many included files change, but the main file
                    // does not. This also puts an upper bound on the number
                    // of entries.
                    debug!(
                        "Too many include entries in preprocessor cache entry file ({}/{}), starting over",
                        new_number_of_entries, MAX_PREPROCESSOR_CACHE_FILE_INFO_ENTRIES
                    );
                    self.results.clear();
                }
                match self.results.entry(result_key.to_string()) {
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        self.number_of_entries -= entry.get().len();
                        self.number_of_entries += includes.len();
                        *entry.get_mut() = includes;
                    }
                    std::collections::btree_map::Entry::Vacant(vacant) => {
                        self.number_of_entries += includes.len();
                        vacant.insert(includes);
                    }
                };
                debug!(
                    "Added result key {result_key:?} to preprocessor cache entry {preprocessor_key:?}"
                );
            }
            Err(e) => {
                debug!(
                    "Could not add result key {result_key:?} to preprocessor cache entry {preprocessor_key:?}: {e}"
                );
            }
        }
    }

    /// Returns the digest of the first result whose expected included files
    /// are already on disk and have not changed.
    pub fn lookup_result_digest(
        &mut self,
        config: &PreprocessorCacheModeConfig,
        updated: &mut bool,
    ) -> Option<String> {
        // Check newest result first since it's more likely to match.
        for (digest, includes) in self.results.iter_mut().rev() {
            let result_matches = Self::result_matches(digest, includes, config, updated);
            if result_matches {
                return Some(digest.to_string());
            }
        }
        None
    }

    /// A result matches if all of its include files exist on disk and have not changed.
    fn result_matches(
        digest: &str,
        includes: &mut [IncludeEntry],
        config: &PreprocessorCacheModeConfig,
        updated: &mut bool,
    ) -> bool {
        for include in includes {
            let path = Path::new(include.path.as_os_str());
            let meta = match std::fs::symlink_metadata(path) {
                Ok(meta) => {
                    if meta.len() != include.file_size {
                        return false;
                    }
                    meta
                }
                Err(e) => {
                    debug!(
                        "{} is in a preprocessor cache entry but can't be read ({})",
                        path.display(),
                        e
                    );
                    return false;
                }
            };

            if config.file_stat_matches {
                match (include.mtime, include.ctime) {
                    (Some(mtime), Some(ctime)) if config.use_ctime_for_stat => {
                        let mtime_matches = meta.modified().map(Into::into).ok() == Some(mtime);
                        let ctime_matches = meta.ctime_or_creation().ok() == Some(ctime);
                        if mtime_matches && ctime_matches {
                            trace!("mtime+ctime hit for {}", path.display());
                            continue;
                        } else {
                            trace!("mtime+ctime miss for {}", path.display());
                        }
                    }
                    (Some(mtime), None) => {
                        let mtime_matches = meta.modified().map(Into::into).ok() == Some(mtime);
                        if mtime_matches {
                            trace!("mtime hit for {}", path.display());
                            continue;
                        } else {
                            trace!("mtime miss for {}", path.display());
                        }
                    }
                    _ => { /* Nothing was recorded, fall back to contents comparison */ }
                }
            }

            let file = match std::fs::File::open(path) {
                Ok(file) => file,
                Err(e) => {
                    debug!(
                        "{} is in a preprocessor cache entry but can't be opened ({})",
                        path.display(),
                        e
                    );
                    return false;
                }
            };

            if config.ignore_time_macros {
                match Digest::reader_sync(file) {
                    Ok(new_digest) => return include.digest == new_digest,
                    Err(e) => {
                        debug!(
                            "{} is in a preprocessor cache entry but can't be read ({})",
                            path.display(),
                            e
                        );
                        return false;
                    }
                }
            } else {
                let (new_digest, finder): (String, _) = match Digest::reader_sync_time_macros(file)
                {
                    Ok((new_digest, finder)) => (new_digest, finder),
                    Err(e) => {
                        debug!(
                            "{} is in a preprocessor cache entry but can't be read ({})",
                            path.display(),
                            e
                        );
                        return false;
                    }
                };
                if !finder.found_time_macros() && include.digest != new_digest {
                    return false;
                }
                if finder.found_time() {
                    // We don't know for sure that the program actually uses the __TIME__ macro,
                    // but we have to assume it anyway and hash the time stamp. However, that's
                    // not very useful since the chance that we get a cache hit later the same
                    // second should be quite slim... So, just signal back to the caller that
                    // __TIME__ has been found so that the preprocessor cache mode can be disabled.
                    debug!("Found __TIME__ in {}", path.display());
                    return false;
                }

                // __DATE__ or __TIMESTAMP__ found. We now make sure that the digest changes
                // if the (potential) expansion of those macros changes by computing a new
                // digest comprising the file digest and time information that represents the
                // macro expansions.
                let mut new_digest = Digest::new();
                new_digest.update(digest.as_bytes());

                if finder.found_date() {
                    debug!("found __DATE__ in {}", path.display());
                    new_digest.delimiter(b"date");
                    let date = chrono::Local::now().date_naive();
                    new_digest.update(&date.year().to_le_bytes());
                    new_digest.update(&date.month().to_le_bytes());
                    new_digest.update(&date.day().to_le_bytes());

                    // If the compiler has support for it, the expansion of __DATE__ will change
                    // according to the value of SOURCE_DATE_EPOCH. Note: We have to hash both
                    // SOURCE_DATE_EPOCH and the current date since we can't be sure that the
                    // compiler honors SOURCE_DATE_EPOCH.
                    if let Ok(source_date_epoch) = std::env::var("SOURCE_DATE_EPOCH") {
                        new_digest.update(source_date_epoch.as_bytes())
                    }
                }

                if finder.found_timestamp() {
                    debug!("found __TIMESTAMP__ in {}", path.display());
                    let meta = match std::fs::symlink_metadata(path) {
                        Ok(meta) => meta,
                        Err(e) => {
                            debug!(
                                "{} is in a preprocessor cache entry but can't be read ({})",
                                path.display(),
                                e
                            );
                            return false;
                        }
                    };
                    let mtime = match meta.modified() {
                        Ok(mtime) => mtime,
                        Err(_) => {
                            debug!(
                                "Couldn't get mtime of {} which contains __TIMESTAMP__",
                                path.display()
                            );
                            return false;
                        }
                    };
                    let mtime: chrono::DateTime<chrono::Local> = chrono::DateTime::from(mtime);
                    new_digest.delimiter(b"timestamp");
                    new_digest.update(&mtime.naive_local().and_utc().timestamp().to_le_bytes());
                    include.digest = new_digest.finish();
                    // Signal that the preprocessor cache entry has been updated and needs to be
                    // written to disk.
                    *updated = true;
                }
            }
        }
        true
    }
}

/// Environment variables that are factored into the preprocessor cache entry cached key.
static CACHED_ENV_VARS: Lazy<HashSet<&'static OsStr>> = Lazy::new(|| {
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
pub fn preprocessor_cache_entry_hash_key(
    compiler_digest: &str,
    language: Language,
    arguments: &[OsString],
    extra_hashes: &[String],
    env_vars: &[(OsString, OsString)],
    input_file: &Path,
    plusplus: bool,
    config: &PreprocessorCacheModeConfig,
) -> anyhow::Result<Option<String>> {
    // If you change any of the inputs to the hash, you should change `FORMAT_VERSION`.
    let mut m = Digest::new();
    m.update(compiler_digest.as_bytes());
    // clang and clang++ have different behavior despite being byte-for-byte identical binaries, so
    // we have to incorporate that into the hash as well.
    m.update(&[plusplus as u8]);
    m.update(&[FORMAT_VERSION]);
    m.update(language.as_str().as_bytes());
    for arg in arguments {
        arg.hash(&mut HashToDigest { digest: &mut m });
    }
    for hash in extra_hashes {
        m.update(hash.as_bytes());
    }

    for (var, val) in env_vars.iter() {
        if CACHED_ENV_VARS.contains(var.as_os_str()) {
            var.hash(&mut HashToDigest { digest: &mut m });
            m.update(&b"="[..]);
            val.hash(&mut HashToDigest { digest: &mut m });
        }
    }

    // Hash the input file otherwise:
    // - a/r.h exists.
    // - a/x.c has #include "r.h".
    // - b/x.c is identical to a/x.c.
    // - Compiling a/x.c records a/r.h in the preprocessor cache entry.
    // - Compiling b/x.c results in a false cache hit since a/x.c and b/x.c
    // share preprocessor cache entries and a/r.h exists.
    let mut buf = vec![];
    encode_path(&mut buf, input_file)?;
    m.update(&buf);
    let reader = std::fs::File::open(input_file)
        .with_context(|| format!("while hashing the input file '{}'", input_file.display()))?;

    let digest = if config.ignore_time_macros {
        Digest::reader_sync(reader)?
    } else {
        let (digest, finder) = Digest::reader_sync_time_macros(reader)?;
        if finder.found_time() {
            // Disable preprocessor cache mode
            debug!("Found __TIME__ in {}", input_file.display());
            return Ok(None);
        }
        digest
    };
    m.update(digest.as_bytes());
    Ok(Some(m.finish()))
}

const PRAGMA_GCC_PCH_PREPROCESS: &[u8] = b"pragma GCC pch_preprocess";
const HASH_31_COMMAND_LINE_NEWLINE: &[u8] = b"# 31 \"<command-line>\"\n";
const HASH_32_COMMAND_LINE_2_NEWLINE: &[u8] = b"# 32 \"<command-line>\" 2\n";
const INCBIN_DIRECTIVE: &[u8] = b".incbin";

/// Remember the include files in the preprocessor output if it can be cached.
/// Returns `Ok(None)` if preprocessor cache mode should be disabled.
#[allow(dead_code)]
pub fn process_preprocessed_file(
    input_file: &Path,
    cwd: &Path,
    bytes: &mut [u8],
    config: PreprocessorCacheModeConfig,
    time_of_compilation: std::time::SystemTime,
    fs_impl: impl PreprocessorFSAbstraction,
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
                &config,
                time_of_compilation,
                bytes,
                start,
                hash_start,
                total_len,
                &mut normalized_include_paths,
                &fs_impl,
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
    config: &PreprocessorCacheModeConfig,
    time_of_compilation: std::time::SystemTime,
    bytes: &mut [u8],
    mut start: usize,
    mut hash_start: usize,
    total_len: usize,
    normalized_include_paths: &mut HashMap<Vec<u8>, Option<Vec<u8>>>,
    fs_impl: &impl PreprocessorFSAbstraction,
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
        system,
        config,
        time_of_compilation,
        fs_impl,
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
/// CAUTION: This does not resolve symlinks (unlike
/// [`std::fs::canonicalize`]).
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

    fn open(&self, path: impl AsRef<Path>) -> io::Result<Box<dyn std::io::Read>> {
        Ok(Box::new(std::fs::File::open(path)?))
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
    system: bool,
    config: &PreprocessorCacheModeConfig,
    time_of_compilation: std::time::SystemTime,
    fs_impl: &impl PreprocessorFSAbstraction,
) -> Result<bool> {
    // TODO if precompiled header.
    if path.len() >= 2 && path[0] == b'<' && path[path.len() - 1] == b'>' {
        // Typically <built-in> or <command-line>.
        return Ok(true);
    }

    if system && config.skip_system_headers
        // Don't remember system includes when testing.
        // Always remember them in prod.
        && cfg!(test)
    {
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
            debug!("Failed to stat include file {}: {}", path.display(), e);
            return Ok(false);
        }
    };

    if meta.is_dir {
        // Ignore directory, typically $PWD.
        return Ok(true);
    }

    if !meta.is_file {
        // Device, pipe, socket or other strange creature.
        debug!("Non-regular include file {}", path.display());
        return Ok(false);
    }

    // TODO add an option to ignore some header files?
    if include_is_too_new(&path, &meta, time_of_compilation) {
        return Ok(false);
    }

    // Let's hash the include file content.
    let mut file = match fs_impl.open(&path) {
        Ok(file) => file,
        Err(e) => {
            debug!("Failed to open header file {}: {}", path.display(), e);
            return Ok(false);
        }
    };

    let finder = if config.ignore_time_macros {
        TimeMacroFinder::new()
    } else {
        let mut finder = TimeMacroFinder::new();
        let mut buffer = [0; crate::util::HASH_BUFFER_SIZE];
        loop {
            let count = file.read(&mut buffer[..])?;
            if count == 0 || finder.found_time_macros() {
                break;
            }
            finder.find_time_macros(&buffer[..count]);
        }
        finder
    };

    if finder.found_date() {
        debug!("Found __DATE__ in header file {}", path.display());
        Ok(false)
    } else if finder.found_time() {
        debug!("Found __TIME__ in header file {}", path.display());
        Ok(false)
    } else if finder.found_timestamp() {
        debug!("Found __TIMESTAMP__ in header file {}", path.display());
        Ok(false)
    } else {
        included_files.insert(path, true);
        Ok(true)
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
fn include_is_too_new(
    path: &Path,
    meta: &PreprocessorFileMetadata,
    time_of_compilation: std::time::SystemTime,
) -> bool {
    // The comparison using >= is intentional, due to a possible race between
    // starting compilation and writing the include file.
    if let Some(mtime) = meta.modified {
        if mtime >= time_of_compilation.into() {
            debug!("Include file {} is too new", path.display());
            return true;
        }
    }

    // The same >= logic as above applies to the change time of the file.
    if let Some(ctime) = meta.ctime_or_creation {
        if ctime >= time_of_compilation.into() {
            debug!("Include file {} is too new", path.display());
            return true;
        }
    }

    false
}

/// Corresponds to a cached include file used in the pre-processor stage
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct IncludeEntry {
    /// Its absolute path
    path: OsString,
    /// The hash of its contents
    digest: String,
    /// Its file size, in bytes
    file_size: u64,
    /// Its modification time, `None` if not recorded.
    mtime: Option<Timestamp>,
    /// Its status change time, `None` if not recorded.
    ctime: Option<Timestamp>,
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
    use std::{collections::VecDeque, sync::Mutex};

    use super::*;

    #[test]
    fn test_find_time_macros_empty_file() {
        let buf: Vec<u8> = vec![];
        let hash = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().0;
        assert_eq!(hash, Digest::new().finish());
    }

    #[test]
    fn test_find_time_macros_small_file_no_match() {
        let buf = b"This is a small file, which doesn't contain any time macros.";
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(!finder.found_time_macros());
    }

    #[test]
    fn test_find_time_macros_small_file_match() {
        let buf = b"__TIME__";
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(!finder.found_date());
        let buf = b"__DATE__";
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(finder.found_date());
        let buf = b"__TIMESTAMP__";
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(finder.found_timestamp());
        assert!(!finder.found_date());
    }

    #[test]
    fn test_find_time_macros_small_file_match_multiple() {
        let buf = b"__TIMESTAMP____DATE____TIME__";
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(finder.found_timestamp());
        assert!(finder.found_date());
    }

    #[test]
    fn test_find_time_macros_large_file_no_match() {
        let buf = vec![0; HASH_BUFFER_SIZE * 2];
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(!finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(!finder.found_date());
    }

    #[test]
    fn test_find_time_macros_large_file_match_no_overlap() {
        let mut buf = vec![0; HASH_BUFFER_SIZE * 2];
        buf.extend(b"__TIMESTAMP____DATE____TIME__");
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(finder.found_timestamp());
        assert!(finder.found_date());
    }
    #[test]
    fn test_find_time_macros_large_file_match_overlap() {
        let mut buf = vec![0; HASH_BUFFER_SIZE * 2];
        // Make the pattern overlap two buffer chunks to make sure we account for this
        let start = HASH_BUFFER_SIZE - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__TIMESTAMP__".len()].copy_from_slice(b"__TIMESTAMP__");
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(finder.found_timestamp());
        assert!(!finder.found_date());

        let mut buf = vec![0; HASH_BUFFER_SIZE * 2];
        // Make the pattern overlap two buffer chunks to make sure we account for this
        let start = HASH_BUFFER_SIZE - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__TIME__".len()].copy_from_slice(b"__TIME__");
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(!finder.found_date());

        let mut buf = vec![0; HASH_BUFFER_SIZE * 2];
        // Make the pattern overlap two buffer chunks to make sure we account for this
        let start = HASH_BUFFER_SIZE - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__DATE__".len()].copy_from_slice(b"__DATE__");
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(finder.found_time_macros());
        assert!(!finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(finder.found_date());
    }

    #[test]
    fn test_find_time_macros_large_file_match_overlap_multiple_pages() {
        let mut buf = vec![0; HASH_BUFFER_SIZE * 3];
        // Make the patterns overlap buffer chunks twice to make sure we account for this
        let start = HASH_BUFFER_SIZE - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__TIME__".len()].copy_from_slice(b"__TIME__");
        let start = HASH_BUFFER_SIZE * 2 - MAX_TIME_MACRO_HAYSTACK_LEN / 2;
        buf[start..][..b"__DATE__".len()].copy_from_slice(b"__DATE__");
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(!finder.found_timestamp());
        assert!(finder.found_date());
    }

    #[test]
    fn test_find_time_macros_large_file_match_overlap_multiple_pages_tiny() {
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
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
        assert!(finder.found_time_macros());
        assert!(finder.found_time());
        assert!(finder.found_timestamp());
        assert!(finder.found_date());
    }

    #[test]
    fn test_find_time_macros_ghost_pattern() {
        // Check the (unlikely) case of a pattern being spread between the
        // start of a chunk and its end.
        let mut buf = vec![0; HASH_BUFFER_SIZE * 3];
        buf[HASH_BUFFER_SIZE..HASH_BUFFER_SIZE + b"__TI".len()].copy_from_slice(b"__TI");
        buf[HASH_BUFFER_SIZE * 2 - "ME__".len()..HASH_BUFFER_SIZE * 2].copy_from_slice(b"ME__");
        let finder = Digest::reader_sync_time_macros(buf.as_slice()).unwrap().1;
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
            PreprocessorCacheModeConfig {
                use_preprocessor_cache_mode: true,
                skip_system_headers: true,
                ..Default::default()
            },
            std::time::SystemTime::now(),
            StandardFsAbstraction,
        )
        .unwrap_or(None);

        assert_eq!(&bytes, &original_bytes);

        assert!(result.is_some());

        let include_files = result.unwrap();
        assert_eq!(include_files.len(), 0);
    }

    // struct YesFs {
    //     yes: &'static str,
    // }

    // impl PreprocessorFSAbstraction for YesFs {
    //     fn metadata(&self, _path: impl AsRef<Path>) -> io::Result<PreprocessorFileMetadata> {
    //         Ok(PreprocessorFileMetadata {
    //             is_dir: false,
    //             is_file: true,
    //             modified: Some(Timestamp::new(0, 0)),
    //             ctime_or_creation: None,
    //         })
    //     }

    //     fn open(&self, _path: impl AsRef<Path>) -> io::Result<Box<dyn std::io::Read>> {
    //         use bytes::Buf;
    //         Ok(Box::new(self.yes.as_bytes().reader()))
    //     }
    // }

    // #[test]
    // fn test_process_preprocessed_file_msvc() {
    //     env_logger::builder()
    //         .is_test(true)
    //         .filter_level(log::LevelFilter::Trace)
    //         .try_init()
    //         .ok();
    //     let input_file = Path::new("tests/test.c");
    //     let path = Path::new(file!())
    //         .parent()
    //         .unwrap()
    //         .parent()
    //         .unwrap()
    //         .parent()
    //         .unwrap();
    //     // This should be portable since the only headers present in this
    //     // output are system headers, which aren't interacted with
    //     // on the filesystem if configured.
    //     let path = path.join("tests/msvc.ii");
    //     let mut bytes = std::fs::read(path).unwrap();
    //     let original_bytes = bytes.clone();

    //     let result = process_preprocessed_file(
    //         input_file,
    //         Path::new(""),
    //         &mut bytes,
    //         PreprocessorCacheModeConfig {
    //             use_preprocessor_cache_mode: true,
    //             skip_system_headers: true,
    //             ..Default::default()
    //         },
    //         std::time::SystemTime::now(),
    //         YesFs { yes: "yes" },
    //     )
    //     .unwrap_or(None);

    //     assert_eq!(&bytes, &original_bytes);

    //     assert!(result.is_some());

    //     let include_files = result.unwrap();

    //     debug!("include_files: {include_files:?}");

    //     assert_ne!(include_files.len(), 0);
    // }

    /// A filesystem interface that only panics to test that we don't access it.
    struct PanicFs;

    impl PreprocessorFSAbstraction for PanicFs {
        fn metadata(&self, path: impl AsRef<Path>) -> io::Result<PreprocessorFileMetadata> {
            panic!("called metadata at {}", path.as_ref().display());
        }

        fn open(&self, path: impl AsRef<Path>) -> io::Result<Box<dyn std::io::Read>> {
            panic!("called open at {}", path.as_ref().display());
        }
    }

    /// A filesystem interface that gives back expected values.
    struct TestFs {
        metadata_results: Mutex<VecDeque<(PathBuf, PreprocessorFileMetadata)>>,
        open_results: Mutex<VecDeque<(PathBuf, Box<dyn std::io::Read>)>>,
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

        fn open(&self, path: impl AsRef<Path>) -> io::Result<Box<dyn std::io::Read>> {
            let (expected_path, impls_read) = self
                .open_results
                .lock()
                .unwrap()
                .pop_front()
                .expect("not enough 'open' results");
            assert_eq!(expected_path, path.as_ref(), "{}", path.as_ref().display());
            Ok(impls_read)
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

        let config = PreprocessorCacheModeConfig {
            use_preprocessor_cache_mode: true,
            skip_system_headers,
            ..Default::default()
        };

        let mut bytes = line.to_vec();
        let total_len = bytes.len();
        process_preprocessor_line(
            input_file,
            Path::new(""),
            include_files,
            &config,
            std::time::SystemTime::now(),
            &mut bytes,
            0,
            0,
            total_len,
            &mut HashMap::new(),
            fs_impl,
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
                open_results: Mutex::new(VecDeque::new()),
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

        // Test "too new" include file
        let mut include_files = HashMap::new();
        let fs_impl = TestFs {
            metadata_results: Mutex::new(
                [(
                    PathBuf::from("/usr/include/x86_64-linux-gnu/bits/libc-header-start.h"),
                    PreprocessorFileMetadata {
                        is_dir: false,
                        is_file: true,
                        modified: Some(Timestamp::new(i64::MAX - 1, 0)),
                        ctime_or_creation: None,
                    },
                )]
                .into_iter()
                .collect(),
            ),
            open_results: Mutex::new(VecDeque::new()),
        };
        assert_eq!(
            do_single_preprocessor_line_call(
                br#"// # 33 "/usr/include/x86_64-linux-gnu/bits/libc-header-start.h" 3 4"#,
                &mut include_files,
                &fs_impl,
                false,
            ),
            // preprocessor cache mode is disabled
            ControlFlow::Break((63, 9, false)),
        );

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
            open_results: Mutex::new(VecDeque::new()),
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
            open_results: Mutex::new(
                [(
                    PathBuf::from("/usr/include/x86_64-linux-gnu/bits/libc-header-start.h"),
                    Box::new(&b"contents"[..]) as Box<dyn std::io::Read>,
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
        // assert_eq!(
        //     include_files
        //         .get(Path::new(
        //             "/usr/include/x86_64-linux-gnu/bits/libc-header-start.h",
        //         ))
        //         .unwrap(),
        //     // hash of `b"contents"`
        //     "a93900c371d997927c5bc568ea538bed59ae5c960021dcfe7b0b369da5267528",
        // );
    }
}
