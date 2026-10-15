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
    errors::*,
    lru_disk_cache::LruCache,
    util::{Digest, MetadataCtimeExt, Timestamp},
};
use futures::lock::Mutex;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone)]
pub struct SourceFileEntry {
    pub compiled_at: chrono::DateTime<chrono::Utc>,
    /// Its status change time, `None` if not recorded.
    pub ctime: Option<Timestamp>,
    /// The hash of its contents
    pub digest: String,
    /// Its file size, in bytes.
    pub fsize: u64,
    /// Its modification time, `None` if not recorded.
    pub mtime: Option<Timestamp>,
    pub found_time: bool,
    pub found_date: bool,
    pub found_timestamp: bool,
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
            let file_good = entry.fsize == fsize && entry.ctime == ctime && entry.mtime == mtime;

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
                ctime,
                digest,
                fsize,
                mtime,
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
    use fs_err as fs;

    let temp = SCCACHE_TMPDIR.as_ref()?;
    let hash = Digest::from_file(exe.as_ref()).await?.finish();
    let path = temp.join(format!("pragma_once_fix_{hash}.so"));
    let temp = path.clone().with_added_extension("1");
    let mut file = fs::File::options().write(true).create(true).open(&path)?;
    file.lock()?;

    if file.metadata()?.len() > 0 {
        return Ok(path);
    }

    let mut cmd = AsyncCommand::new(exe.as_ref(), jobserver::Client::new_num(1));

    cmd.env_clear()
        .envs(env_vars)
        .args(&["-x", "c", "-O2", "-shared", "-fPIC"])
        .arg("-o")
        .arg(&temp)
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

    std::io::copy(&mut std::fs::File::open(&temp)?, &mut file)?;
    std::fs::remove_file(temp)?;

    Ok(path)
}
