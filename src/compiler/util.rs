// Copyright 2016 Mozilla Foundation
// SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
    cache::{Cache, Storage},
    errors::*,
    lru_disk_cache::{LruCache, lru_cache},
    util::{Digest, MetadataCtimeExt, Timestamp},
};
use bytes::{Buf, BufMut, BytesMut};
use futures::lock::Mutex;
use serde::{Deserialize, Serialize, de, ser::SerializeMap};
use serde_with::{
    DeserializeFromStr, DisplayFromStr, IfIsHumanReadable, SerializeDisplay, serde_as,
};
use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fmt,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[derive(Clone)]
pub struct SourceFileEntry {
    pub compiled_at: chrono::DateTime<chrono::Utc>,
    pub meta: SourceFileMeta,
    pub found_time: bool,
    pub found_date: bool,
    pub found_timestamp: bool,
}

#[derive(Clone, Default, DeserializeFromStr, SerializeDisplay, PartialEq, Eq)]
pub struct OsStringDisplay {
    inner: OsString,
}

impl std::ops::Deref for OsStringDisplay {
    type Target = OsStr;

    fn deref(&self) -> &Self::Target {
        self.inner.as_os_str()
    }
}

impl AsRef<OsStr> for OsStringDisplay {
    fn as_ref(&self) -> &OsStr {
        self.inner.as_os_str()
    }
}

impl fmt::Debug for OsStringDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner.display())
    }
}

impl fmt::Display for OsStringDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner.display())
    }
}

impl std::str::FromStr for OsStringDisplay {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self {
            inner: OsString::from(s),
        })
    }
}

impl<'a> From<&'a OsStr> for OsStringDisplay {
    fn from(value: &'a OsStr) -> Self {
        OsStringDisplay {
            inner: value.to_owned(),
        }
    }
}

impl<'a> From<&'a Path> for OsStringDisplay {
    fn from(value: &'a Path) -> Self {
        OsStringDisplay {
            inner: value.as_os_str().to_owned(),
        }
    }
}

impl From<OsString> for OsStringDisplay {
    fn from(value: OsString) -> Self {
        OsStringDisplay { inner: value }
    }
}

impl From<PathBuf> for OsStringDisplay {
    fn from(value: PathBuf) -> Self {
        OsStringDisplay {
            inner: value.into_os_string(),
        }
    }
}

impl From<OsStringDisplay> for OsString {
    fn from(value: OsStringDisplay) -> Self {
        value.inner
    }
}

/// Corresponds to a cached include file used in the pre-processor stage
#[serde_as]
#[derive(Clone, Deserialize, Serialize, Debug, Default, PartialEq, Eq)]
pub struct SourceFileMeta {
    /// The file's absolute path.
    #[serde_as(as = "IfIsHumanReadable<DisplayFromStr>")]
    pub path: OsStringDisplay,
    /// The hash of the file's contents.
    pub digest: String,
    /// The file's size (in bytes).
    pub fsize: u64,
    /// The file's modification time, `None` if not recorded.
    pub mtime: Option<Timestamp>,
    /// The file's status change time, `None` if not recorded.
    pub ctime: Option<Timestamp>,
}

#[derive(Clone, Debug)]
pub struct SourceFileCache {
    lru: Arc<Mutex<LruCache<PathBuf, Arc<SourceFileEntry>>>>,
}

impl SourceFileCache {
    pub fn new(capacity: u64) -> Self {
        SourceFileCache {
            lru: Arc::new(Mutex::new(LruCache::new(capacity))),
        }
    }

    pub async fn get<P: AsRef<Path>>(
        &self,
        path: P,
        compile_timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<Arc<SourceFileEntry>> {
        use chrono::Datelike;

        // Assumes path has been canonicalized
        let path = path.as_ref();
        let meta = match tokio::fs::symlink_metadata(path).await {
            Ok(meta) => meta,
            Err(err) => {
                bail!("Error reading metadata for '{}': {err:#}", path.display());
            }
        };

        let mtime = meta.modified().map(Into::into).ok();
        let ctime = meta.ctime_or_creation().ok();
        let fsize = meta.len();

        let entry = self.lru.lock().await.get(path).and_then(|entry| {
            // If the file hasn't changed, reuse the cached entry
            let file_good =
                entry.meta.fsize == fsize && entry.meta.ctime == ctime && entry.meta.mtime == mtime;

            // If __TIMESTAMP__ or __DATE__ were found, compare compile time to now (or SOURCE_DATE_EPOCH)
            let time_good = if entry.found_timestamp {
                // If __TIMESTAMP__ was found, compare timestamp seconds exactly
                entry.compiled_at == compile_timestamp
            } else if entry.found_date {
                // If __DATE__ was found, only compare the parts of the
                // timestamp the compiler embeds (day, month, and year)
                let date_0 = entry.compiled_at.date_naive();
                let date_1 = compile_timestamp.date_naive();
                date_0.day() == date_1.day()
                    && date_0.month() == date_1.month()
                    && date_0.year() == date_1.year()
            } else {
                // If neither were found, use this cached include entry
                true
            };

            if file_good && time_good {
                Some(entry.clone())
            } else {
                None
            }
        });

        let entry = if let Some(entry) = entry {
            entry
        } else {
            let digest_and_finder = Digest::from_file_with_time_macros(path, compile_timestamp)
                .await
                .with_context(|| format!("while reading '{}'", path.display()))
                .inspect_err(|err| trace!("[result_matches]: {err:#}"))
                .map(|(digest, finder)| (digest.finish(), finder));

            let (digest, finder) = match digest_and_finder {
                Ok((digest, finder)) => (digest, finder),
                Err(err) => {
                    bail!("Error computing digest: {err:#}");
                }
            };

            let entry = Arc::new(SourceFileEntry {
                compiled_at: compile_timestamp,
                meta: SourceFileMeta {
                    ctime,
                    digest,
                    fsize,
                    mtime,
                    path: path.into(),
                },
                found_time: finder.found_time(),
                found_date: finder.found_date(),
                found_timestamp: finder.found_timestamp(),
            });

            self.lru.lock().await.insert(path.to_owned(), entry.clone());

            entry
        };

        Ok(entry)
    }
}

// The dependencies cache entry is a description of all information needed
// to cache pre-processor output in C-family languages for a given input file.
// The current implementation is very much inspired from the "manifest"
// that `ccache` uses for its "direct mode", though the on-disk format is
// different.

const MAX_REVISIONS: u64 = 1_000;

#[derive(Clone, Default, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct Dependencies {
    /// The list of included dependencies
    pub sources: Vec<SourceFileMeta>,
    /// Dependency files produced the compiler (if any)
    pub outputs: BTreeMap<String, String>,
}

#[derive(Clone)]
pub struct SourceFileDeps {
    pub key: String,
    /// Information about the original source file
    pub meta: SourceFileMeta,
    /// An LRU reverse-map of object hash key -> dependencies + depfile(s).
    ///
    /// The dependencies are the included files reported by the compiler
    /// and the depfiles (if any) that were produced by the preprocessor's
    /// dependency generation routine.
    pub revs: LruCache<String, Dependencies>,
}

impl SourceFileDeps {
    pub fn new(key: String, meta: SourceFileMeta) -> Self {
        Self {
            key,
            meta,
            revs: LruCache::new(MAX_REVISIONS),
        }
    }

    /// Return the dependencies cache entry for a given dependencies key if it exists in storage.
    pub async fn get(storage: &dyn Storage, key: &str) -> Result<Cache<SourceFileDeps>> {
        match storage.get(key).await {
            Err(err) => Err(err),
            Ok(Cache::Miss) => Ok(Cache::Miss),
            Ok(Cache::Hit(deflated)) => tokio::task::spawn_blocking(move || {
                let mut inflated = vec![];
                flate2::read::ZlibDecoder::new(deflated.reader())
                    .read_to_end(&mut inflated)
                    .map_err(Into::into)
                    .and_then(|_| Self::try_from(opendal::Buffer::from(inflated)))
                    .map_or_else(|_| Cache::Miss, Cache::Hit)
            })
            .await
            .or(Ok(Cache::Miss)),
        }
    }

    /// Write a dependencies cache entry for the given dependencies key to storage,
    /// overwriting if it exists.
    pub async fn put(self, storage: &dyn Storage) -> Result<Duration> {
        use bytes::{BufMut, BytesMut};

        let key = self.key.clone();

        let buf = tokio::task::spawn_blocking(move || {
            let mut writer = flate2::write::ZlibEncoder::new(
                BytesMut::new().writer(),
                flate2::Compression::best(),
            );

            bincode::serialize_into(&mut writer, &self)?;

            writer
                .finish()
                .map_err(anyhow::Error::new)
                .map(|writer| writer.into_inner().freeze().into())
        })
        .await??;

        storage.put(&key, buf).await
    }

    pub fn from_buffer(buffer: opendal::Buffer) -> Result<Self> {
        Ok(bincode::deserialize_from(buffer.reader())?)
    }

    pub fn to_buffer(&self) -> Result<opendal::Buffer> {
        let mut writer = BytesMut::new().writer();
        bincode::serialize_into(&mut writer, self)?;
        Ok(opendal::Buffer::from(writer.into_inner().freeze()))
    }

    pub fn iter(&self) -> lru_cache::Iter<'_, String, Dependencies> {
        self.revs.iter()
    }

    pub fn iter_mut(&mut self) -> lru_cache::IterMut<'_, String, Dependencies> {
        self.revs.iter_mut()
    }

    /// Insert the full compilation key and included files for a given source file.
    ///
    /// There can be more than one result at once for a source file if one
    /// or more of the include files has changed but not the source file.
    pub fn add_revision(
        &mut self,
        obj_key: &str,
        dependencies: Dependencies,
        out_pretty: &str,
    ) -> &mut Self {
        let num_includes = dependencies.sources.len();

        self.revs.insert(obj_key.to_string(), dependencies);

        let num_results = self.revs.len();

        debug!(
            "[{out_pretty}, {src_key}, {obj_key}]: Added revision to dependencies cache entry: num_includes={num_includes}, num_results={num_results}",
            src_key = self.key
        );

        self
    }

    /// Returns the digest of the first result whose expected included files
    /// are already on disk and have not changed.
    pub async fn find_dependencies(
        &mut self,
        source_file_cache: &SourceFileCache,
        compile_timestamp: chrono::DateTime<chrono::Utc>,
        out_pretty: &str,
    ) -> (bool, Option<(String, Dependencies)>) {
        let mut maybe_result = None;
        let mut needs_update = false;
        let mut log_messages = vec![];

        // Find the first result key whose include files on disk match this
        // dependencies cache entry. Check newest results first since they're
        // more likely to match.
        for (idx, (obj_key, deps)) in self.iter_mut().rev().enumerate() {
            let (failure, updated) =
                Self::test_dependencies(source_file_cache, compile_timestamp, &mut deps.sources)
                    .await;

            if let Some(reason) = updated {
                needs_update = true;
                if log_enabled!(log::Level::Trace) {
                    log_messages.push(format!("{obj_key}]: {reason}"));
                }
            }

            if let Some(reason) = failure {
                if log_enabled!(log::Level::Trace) {
                    log_messages.push(format!("{obj_key}]: Deps lookup failure: {reason}"));
                }
            } else {
                if idx != 0 {
                    // Need to write back to storage if the LRU order changes
                    needs_update = true;
                    if log_enabled!(log::Level::Trace) {
                        log_messages.push(format!("{obj_key}]: Deps LRU order changed"));
                    }
                }
                maybe_result = Some((obj_key.clone(), deps.clone()));
                break;
            }
        }

        for msg in log_messages {
            trace!("[{out_pretty}, {src_key}, {msg}", src_key = self.key);
        }

        if let Some((obj_key, _)) = maybe_result.as_ref() {
            // Move the entry to the back of the LRU
            self.revs.get(obj_key);
        }

        (
            // Signal this dependencies cache entry was modified and should
            // be written back to storage.
            // Note: This can happen regardless whether a result was found.
            needs_update,
            // Return the object key and dependencies
            maybe_result,
        )
    }

    /// A result matches if all of its include files exist on disk and have not changed.
    async fn test_dependencies(
        source_file_cache: &SourceFileCache,
        compile_timestamp: chrono::DateTime<chrono::Utc>,
        dependencies: &mut [SourceFileMeta],
    ) -> (Option<String>, Option<String>) {
        let mut failure = None;
        let mut updated = None;

        for prev in dependencies.iter_mut() {
            let path = Path::new(&prev.path);

            let curr = match source_file_cache.get(path, compile_timestamp).await {
                Ok(entry) => entry,
                Err(err) => {
                    failure = Some(format!("{err:#}"));
                    break;
                }
            };

            let SourceFileEntry {
                meta: curr,
                found_time,
                found_date,
                found_timestamp,
                ..
            } = curr.as_ref();

            if *found_time {
                // We don't know for sure that the program actually uses the __TIME__ macro,
                // but we have to assume it anyway and hash the time stamp. However, that's
                // not very useful since the chance that we get a cache hit later the same
                // second should be quite slim... So, just signal back to the caller that
                // __TIME__ has been found so that the dependencies cache can be disabled.
                failure = Some(format!("Found __TIME__ in {path:?}"));
                break;
            }

            // If the digests are different, disable dependencies cache
            if prev.digest != curr.digest {
                failure = Some(format!("Digest mismatch for {path:?}"));
                break;
            }

            // If __DATE__ or __TIMESTAMP__ found, update the includes for this result.
            if *found_date {
                // trace!("[{out_pretty}, {src_key}]: updating entry, __DATE__ found in {path:?}");
                updated = Some(format!("updating entry, __DATE__ found in {path:?}"));
                *prev = curr.clone();
            } else if *found_timestamp {
                // trace!("[{out_pretty}, {src_key}]: updating entry, __TIMESTAMP__ found in {path:?}");
                updated = Some(format!("updating entry, __TIMESTAMP__ found in {path:?}"));
                *prev = curr.clone();
            }
        }

        (failure, updated)
    }
}

impl TryFrom<SourceFileDeps> for opendal::Buffer {
    type Error = anyhow::Error;

    fn try_from(src: SourceFileDeps) -> std::result::Result<Self, Self::Error> {
        src.to_buffer()
    }
}

impl TryFrom<opendal::Buffer> for SourceFileDeps {
    type Error = anyhow::Error;

    fn try_from(buf: opendal::Buffer) -> std::result::Result<Self, Self::Error> {
        Self::from_buffer(buf)
    }
}

impl fmt::Debug for SourceFileDeps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}",
            serde_json::to_string_pretty(self).expect("json serialize to succeed")
        )
    }
}

impl fmt::Display for SourceFileDeps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}",
            serde_json::to_string(self).expect("json serialize to succeed")
        )
    }
}

// impl fmt::Debug for SourceFileDeps {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("SourceFileDeps")
//             .field("key", &self.key)
//             .field("meta", &self.meta)
//             .field("revs", &self.iter().rev().collect::<Vec<_>>())
//             .finish()
//     }
// }

// impl fmt::Display for SourceFileDeps {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         writeln!(f, "{self:?}")
//     }
// }

impl<'de> Deserialize<'de> for SourceFileDeps {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SourceFileDepsVisitor;
        impl<'de> de::Visitor<'de> for SourceFileDepsVisitor {
            type Value = SourceFileDeps;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("expected a depepdency cache entry")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut key = None;
                let mut meta = None;
                let mut revs = None;
                let mut size = None;

                while let Ok(Some(name)) = map.next_key::<String>() {
                    match name.as_str() {
                        "key" => {
                            key = Some(map.next_value::<String>()?);
                        }
                        "revs" => {
                            revs = Some(map.next_value::<Vec<(String, Dependencies)>>()?);
                        }
                        "meta" => {
                            meta = Some(map.next_value::<SourceFileMeta>()?);
                        }
                        "size" => {
                            size = Some(map.next_value::<u64>()?);
                        }
                        name => {
                            return Err(de::Error::unknown_field(
                                name,
                                &["key", "meta", "revs", "size"],
                            ));
                        }
                    }
                }

                let Some(key) = key else {
                    return Err(de::Error::missing_field("key"));
                };

                let Some(meta) = meta else {
                    return Err(de::Error::missing_field("meta"));
                };

                let mut deps = SourceFileDeps {
                    key,
                    meta,
                    revs: LruCache::new(size.unwrap_or(MAX_REVISIONS).max(MAX_REVISIONS)),
                };

                for (key, val) in revs.into_iter().flatten().rev() {
                    deps.revs.insert(key, val);
                }

                Ok(deps)
            }
        }

        deserializer.deserialize_map(SourceFileDepsVisitor)
    }
}

impl Serialize for SourceFileDeps {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("key", &self.key)?;
        map.serialize_entry("meta", &self.meta)?;
        map.serialize_entry("size", &self.revs.capacity())?;
        map.serialize_entry("revs", &self.iter().rev().collect::<Vec<_>>())?;
        map.end()
    }
}

#[cfg(unix)]
static GCC_PRAGMA_ONCE_FIX_LOCK: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(1));

#[cfg(unix)]
pub async fn build_gcc_pragma_once_fix<
    Exe: AsRef<Path>,
    Key: AsRef<std::ffi::OsStr>,
    Var: AsRef<std::ffi::OsStr>,
    Envs: Iterator<Item = (Key, Var)> + Clone,
>(
    exe: Exe,
    env_vars: Envs,
) -> Result<PathBuf> {
    use crate::{
        jobserver,
        mock_command::{AsyncCommand, RunCommand},
        util::{Digest, SCCACHE_TMPDIR, run_input_output},
    };
    use std::ffi::OsStr;

    let _permit = GCC_PRAGMA_ONCE_FIX_LOCK.acquire().await?;

    let temp = SCCACHE_TMPDIR.as_ref()?;
    let name = format!(
        "pragma_once_fix_{name}_{hash}.so",
        name = exe
            .as_ref()
            .file_stem()
            .unwrap_or(OsStr::new("gcc"))
            .display(),
        hash = Digest::from_file(exe.as_ref()).await?.finish()
    );
    let path = temp.join(&name);

    let file = tempfile::Builder::new()
        .rand_bytes(0)
        .prefix(&name)
        .suffix(".lock")
        .tempfile_in(temp)?;

    file.as_file().lock()?;

    if !path.exists() || !path.is_file() || path.metadata()?.len() == 0 {
        let mut cmd = AsyncCommand::new(exe.as_ref(), jobserver::Client::new_num(1));

        cmd.env_clear()
            .envs(env_vars)
            .args(&["-x", "c", "-O2", "-shared", "-fPIC"])
            .arg("-o")
            .arg(&path)
            .arg("-")
            .arg("-ldl");

        run_input_output(cmd, Some(r#"
/*
 * LD_PRELOAD shim that makes GCC's #pragma once identify a header by its
 * (st_dev, st_ino) instead of by its (st_mtime, st_size, contents).
 *
 * libcpp's has_unique_contents() treats a newly opened header as a file it has
 * already seen when an earlier #pragma once file has the same st_mtime (whole
 * seconds) and st_size and identical bytes.  Two distinct copies of a header
 * with identical bytes and mtimes are therefore merged, and the second copy is
 * silently skipped even if it would include different files.
 *
 * libcpp gets st_mtime from the fstat() in open_file().  This shim interposes
 * fstat() in GCC's compilers proper (cc1, cc1plus, ...) so that no two distinct
 * regular files report the same st_mtime and size: a file keeps its real mtime
 * unless a different file already reported that mtime and size, in which case
 * it gets the next free second.  A file gets the same value every time it is
 * opened, so libcpp only ever compares a file's contents with its own, which
 * still merges one file reached through different paths (symlinks, hard links,
 * "a/../b").
 *
 * Build: cc -O2 -shared -fPIC -o libpragma_once_fix.so pragma_once_fix.c -ldl
 * Use:   LD_PRELOAD=/abs/path/libpragma_once_fix.so gcc ...
 * Set PRAGMA_ONCE_FIX_DEBUG=1 to log each file whose st_mtime was changed: the
 * files stock GCC would have compared byte-by-byte against a different file.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* The pre-2.33 glibc ABI, still called by GCCs built against older glibc. */
int __fxstat(int ver, int fd, struct stat *st);
int __fxstat64(int ver, int fd, struct stat64 *st);

static bool active, debug;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

__attribute__((constructor)) static void init(void)
{
	static const char *const compilers[] = { "cc1", "cc1plus", "cc1obj", "cc1objplus" };

	for (size_t i = 0; i < sizeof(compilers) / sizeof(*compilers); i++)
		if (strcmp(program_invocation_short_name, compilers[i]) == 0)
			active = true;
	debug = getenv("PRAGMA_ONCE_FIX_DEBUG") != NULL;
	if (active && debug)
		fprintf(stderr, "pragma-once-fix: active in %s\n", program_invocation_short_name);
}

/* Open-addressing hash table from a (k1, k2) key to a value. */
struct slot {
	uint64_t k1, k2, val;
	bool used;
};
struct table {
	struct slot *slots;
	size_t cap, len;
};

/* Returns the slot holding (k1, k2), or the empty slot where it belongs. */
static struct slot *find(struct table *t, uint64_t k1, uint64_t k2)
{
	size_t i = ((k1 ^ k2 * 0x9e3779b97f4a7c15u) * 0xbf58476d1ce4e5b9u) >> 32;

	for (;; i++) {
		struct slot *s = &t->slots[i & (t->cap - 1)];
		if (!s->used || (s->k1 == k1 && s->k2 == k2))
			return s;
	}
}

/* Makes room for one more entry, keeping the table at most half full. */
static bool reserve(struct table *t)
{
	if (2 * (t->len + 1) <= t->cap)
		return true;

	struct table bigger = { NULL, t->cap ? 2 * t->cap : 256, t->len };
	if (!(bigger.slots = calloc(bigger.cap, sizeof(*bigger.slots))))
		return false;
	for (size_t i = 0; i < t->cap; i++)
		if (t->slots[i].used)
			*find(&bigger, t->slots[i].k1, t->slots[i].k2) = t->slots[i];
	free(t->slots);
	*t = bigger;
	return true;
}

/* (st_dev, st_ino) -> the st_mtime reported for that file. */
static struct table files;
/* (st_mtime, size) reported for some file -> a later st_mtime to try next. */
static struct table taken;

/* The size libcpp compares for UTF-8 input, which excludes a byte order mark. */
static uint64_t content_size(int fd, uint64_t size)
{
	unsigned char bom[3];

	if (size >= 3 && pread(fd, bom, 3, 0) == 3 && memcmp(bom, "\xef\xbb\xbf", 3) == 0)
		size -= 3;
	return size;
}

static void log_remap(int fd, uint64_t real, uint64_t fake)
{
	char link[32], path[PATH_MAX];

	snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
	ssize_t n = readlink(link, path, sizeof(path) - 1);
	path[n > 0 ? n : 0] = '\0';
	fprintf(stderr, "pragma-once-fix: %s: st_mtime %lld -> %lld (another file has the same mtime and size)\n",
		path, (long long)real, (long long)fake);
}

/* Replaces *mtime with the second reserved for this file, reserving it if new. */
static void remap(int fd, uint64_t dev, uint64_t ino, mode_t mode, uint64_t size,
		  time_t *mtime)
{
	if (!active || !S_ISREG(mode))
		return;

	pthread_mutex_lock(&lock);
	if (reserve(&files) && reserve(&taken)) {
		struct slot *f = find(&files, dev, ino), *s;
		if (!f->used) {
			/* Skip past seconds other files of this size already have. */
			uint64_t real = (uint64_t)*mtime, t = real;
			size = content_size(fd, size);
			while ((s = find(&taken, t, size))->used)
				t = s->val;
			*s = (struct slot){ t, size, t + 1, true };
			taken.len++;
			/* Let later files with this mtime and size skip straight past t. */
			for (uint64_t u = real; u != t;) {
				s = find(&taken, u, size);
				u = s->val;
				s->val = t + 1;
			}
			*f = (struct slot){ dev, ino, t, true };
			files.len++;
			if (debug && t != real)
				log_remap(fd, real, t);
		}
		*mtime = (time_t)f->val;
	}
	pthread_mutex_unlock(&lock);
}

/* Declares `next`, the definition this library shadows, or fails the call. */
#define NEXT(name)                                                            \
	static __typeof__(name) *next;                                        \
	if (!next && !(next = (__typeof__(name) *)dlsym(RTLD_NEXT, #name))) { \
		errno = ENOSYS;                                               \
		return -1;                                                    \
	}

int fstat(int fd, struct stat *st)
{
	NEXT(fstat);
	int ret = next(fd, st);
	if (ret == 0)
		remap(fd, st->st_dev, st->st_ino, st->st_mode, st->st_size, &st->st_mtime);
	return ret;
}

int fstat64(int fd, struct stat64 *st)
{
	NEXT(fstat64);
	int ret = next(fd, st);
	if (ret == 0)
		remap(fd, st->st_dev, st->st_ino, st->st_mode, st->st_size, &st->st_mtime);
	return ret;
}

int __fxstat(int ver, int fd, struct stat *st)
{
	NEXT(__fxstat);
	int ret = next(ver, fd, st);
	if (ret == 0)
		remap(fd, st->st_dev, st->st_ino, st->st_mode, st->st_size, &st->st_mtime);
	return ret;
}

int __fxstat64(int ver, int fd, struct stat64 *st)
{
	NEXT(__fxstat64);
	int ret = next(ver, fd, st);
	if (ret == 0)
		remap(fd, st->st_dev, st->st_ino, st->st_mode, st->st_size, &st->st_mtime);
	return ret;
}
"#.as_bytes().to_vec())).await?;
    }

    file.as_file().unlock()?;

    Ok(path)
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::test::utils::*;

    #[test]
    fn test_serialize_deserialize() -> Result<()> {
        let f = TestFixture::new();
        let p = f.touch("file.c")?;

        let mut entry_a = SourceFileDeps::new(
            "src_key".into(),
            SourceFileMeta {
                path: p.into(),
                digest: String::new(),
                fsize: 0,
                mtime: None,
                ctime: None,
            },
        );

        entry_a.add_revision("obj_key", Dependencies::default(), "file.c");

        assert_eq!(entry_a.iter().count(), 1);

        let buf = opendal::Buffer::try_from(entry_a)?;
        let entry_b = SourceFileDeps::try_from(buf.clone())?;

        assert_eq!(entry_b.iter().count(), 1);
        assert_eq!(buf.to_bytes(), entry_b.to_buffer()?.to_bytes());

        Ok(())
    }

    // #[test]
    // fn test_preprocessor_cache_entry_hash_key_basedirs() {
    //     #[cfg(target_os = "windows")]
    //     use crate::util::normalize_win_path;
    //     use std::fs;
    //     use tempfile::TempDir;

    //     // Create two different base directories
    //     let dir1 = TempDir::new().unwrap();
    //     let dir2 = TempDir::new().unwrap();
    //     let dirs = [&dir1, &dir2]
    //         .iter()
    //         .map(|dir| {
    //             let bytes = dir.path().to_string_lossy().into_owned().into_bytes();
    //             #[cfg(target_os = "windows")]
    //             return normalize_win_path(&bytes);
    //             #[cfg(not(target_os = "windows"))]
    //             bytes
    //         })
    //         .collect::<Vec<_>>();

    //     // Create identical files with the same relative path in each directory
    //     let file_path = Path::new("test.c");
    //     let content = b"int main() { return 0; }";
    //     fs::write(dir1.path().join(file_path), content).unwrap();
    //     fs::write(dir2.path().join(file_path), content).unwrap();

    //     let args = ParsedArguments {
    //         language: Language::C,
    //         input: file_path.into(),
    //         ..Default::default()
    //     };

    //     let compile_timestamp = chrono::Utc::now();

    //     // Test 1: With basedirs, hashes should be the same
    //     let hash1_with_basedirs = preprocessor_cache_entry_hash_key(
    //         "test_digest",
    //         None,
    //         &args,
    //         &[],
    //         &[],
    //         dir1.path(),
    //         false,
    //         &dirs,
    //         compile_timestamp,
    //     )
    //     .wait()
    //     .unwrap()
    //     .unwrap();

    //     let hash2_with_basedirs = preprocessor_cache_entry_hash_key(
    //         "test_digest",
    //         None,
    //         &args,
    //         &[],
    //         &[],
    //         dir2.path(),
    //         false,
    //         &dirs,
    //         compile_timestamp,
    //     )
    //     .wait()
    //     .unwrap()
    //     .unwrap();

    //     assert_ne!(
    //         hash1_with_basedirs, hash2_with_basedirs,
    //         "Hashes should be different when using basedirs with identical files in different directories"
    //     );

    //     // Test 2: With basedir1 for first, and basedir2 for second, hashes should be the same
    //     let hash1_with_basedirs = preprocessor_cache_entry_hash_key(
    //         "test_digest",
    //         None,
    //         &args,
    //         &[],
    //         &[],
    //         dir1.path(),
    //         false,
    //         &dirs[..1],
    //         compile_timestamp,
    //     )
    //     .wait()
    //     .unwrap()
    //     .unwrap();

    //     let hash2_with_basedirs = preprocessor_cache_entry_hash_key(
    //         "test_digest",
    //         None,
    //         &args,
    //         &[],
    //         &[],
    //         dir2.path(),
    //         false,
    //         &dirs[1..],
    //         compile_timestamp,
    //     )
    //     .wait()
    //     .unwrap()
    //     .unwrap();

    //     assert_ne!(
    //         hash1_with_basedirs, hash2_with_basedirs,
    //         "Hashes should be different when using basedirs with identical files in different directories"
    //     );

    //     // Test 3: Without basedirs, hashes should be different
    //     let hash1_no_basedirs = preprocessor_cache_entry_hash_key(
    //         "test_digest",
    //         None,
    //         &args,
    //         &[],
    //         &[],
    //         dir1.path(),
    //         false,
    //         &[],
    //         compile_timestamp,
    //     )
    //     .wait()
    //     .unwrap()
    //     .unwrap();

    //     let hash2_no_basedirs = preprocessor_cache_entry_hash_key(
    //         "test_digest",
    //         None,
    //         &args,
    //         &[],
    //         &[],
    //         dir2.path(),
    //         false,
    //         &[],
    //         compile_timestamp,
    //     )
    //     .wait()
    //     .unwrap()
    //     .unwrap();

    //     assert_ne!(
    //         hash1_no_basedirs, hash2_no_basedirs,
    //         "Hashes should be different without basedirs for files in different directories"
    //     );
    // }
}
