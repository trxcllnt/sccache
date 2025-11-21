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

use async_trait::async_trait;
use notify_debouncer_full as notify;
use notify_debouncer_full::{
    DebounceEventResult, RecommendedCache, new_debouncer,
    notify::{RecommendedWatcher, RecursiveMode},
};
use std::{ffi::OsString, future::Future, path::PathBuf, pin::Pin, sync::Arc, time::Duration};

use crate::cache::{Cache, CacheMode, PreprocessorCacheModeConfig, Storage};
use crate::errors::*;

pub struct WatchStorage {
    storage: Arc<futures::lock::Mutex<Arc<dyn Storage>>>,
    preprocessor_cache_mode_config: PreprocessorCacheModeConfig,
    #[allow(dead_code)]
    watcher: notify::Debouncer<RecommendedWatcher, RecommendedCache>,
}

impl WatchStorage {
    pub async fn from<T>(create: T, paths: &[PathBuf]) -> Result<Arc<dyn Storage>>
    where
        T: Fn() -> Pin<Box<dyn Future<Output = Result<Arc<dyn Storage>>> + Send>> + Send + 'static,
    {
        let storage = create().await?;
        let paths = Self::dirs_and_paths(paths);
        if paths.is_empty() {
            trace!("No paths to watch, returning storage");
            return Ok(storage);
        }
        let preprocessor_cache_mode_config = storage.preprocessor_cache_mode_config();
        let storage = Arc::new(futures::lock::Mutex::new(storage));
        let watcher = Self::watch(create, storage.clone(), paths)?;
        Ok(Arc::new(Self {
            preprocessor_cache_mode_config,
            storage,
            watcher,
        }))
    }

    fn dirs_and_paths(paths: &[PathBuf]) -> Vec<(PathBuf, Vec<OsString>)> {
        trace!("Filtering for {paths:?}");
        use itertools::Itertools;
        paths
            .iter()
            .filter_map(|p| {
                p.parent()
                    .filter(|dir| dir.exists())
                    .map(|dir| dir.to_owned())
                    .map(|dir| (p, dir))
            })
            .filter_map(|(p, dir)| p.file_name().map(|name| (dir, name.to_owned())))
            .sorted_by_key(|(dir, _)| dir.clone())
            .group_by(|(dir, _)| dir.clone())
            .into_iter()
            .map(|(dir, names)| {
                (
                    dir,
                    names
                        .into_iter()
                        .map(|(_, filename)| filename)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    }

    fn watch<T>(
        create: T,
        storage: Arc<futures::lock::Mutex<Arc<dyn Storage>>>,
        dirs: Vec<(PathBuf, Vec<OsString>)>,
    ) -> Result<notify::Debouncer<RecommendedWatcher, RecommendedCache>>
    where
        T: Fn() -> Pin<Box<dyn Future<Output = Result<Arc<dyn Storage>>> + Send>> + Send + 'static,
    {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DebounceEventResult>();

        let mut debouncer = new_debouncer(
            Duration::from_secs(5),
            None,
            move |res: DebounceEventResult| {
                let _ = tx.send(res);
            },
        )?;

        for (dir, _) in dirs.iter() {
            debug!("Watching for changes in dir: {dir:?}");
            debouncer.watch(dir, RecursiveMode::NonRecursive)?;
        }

        let paths = dirs
            .into_iter()
            .flat_map(|(parent, filenames)| {
                filenames
                    .into_iter()
                    .map(|filename| parent.join(filename))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        debug!("Watching for changes to files: {paths:?}");

        tokio::spawn(async move {
            use notify_debouncer_full::notify::{
                EventKind,
                event::{AccessKind, AccessMode, CreateKind, ModifyKind},
            };

            while let Some(res) = rx.recv().await {
                let events = match res {
                    Ok(events) => events
                        .into_iter()
                        .filter(|event| {
                            matches!(
                                event.kind,
                                EventKind::Access(AccessKind::Close(AccessMode::Write))
                                    | EventKind::Create(CreateKind::File)
                                    | EventKind::Modify(ModifyKind::Data(_))
                                    | EventKind::Remove(_)
                            )
                        })
                        .map(|event| (event.kind, event.paths.clone()))
                        .collect::<Vec<_>>(),
                    Err(err) => {
                        error!("Notify error: {err:?}");
                        continue;
                    }
                };

                trace!("Notify events: {events:?}");

                // Only respond if the changes are to paths we care about
                let changes = paths
                    .iter()
                    .filter(|path| events.iter().any(|(_, changes)| changes.contains(path)));

                if !changes.clone().any(|_| true) {
                    trace!("Ignoring changes");
                    continue;
                }

                info!(
                    "[WatchStorage::watch]: Recreating storage after changes to: [{:?}]",
                    changes.collect::<Vec<_>>()
                );

                let mut guard = storage.lock().await;
                match create().await {
                    Ok(storage) => *guard = storage,
                    Err(err) => {
                        error!("Failed to recreate storage: {err:?}")
                    }
                }
                drop(guard);
            }
        });

        Ok(debouncer)
    }

    async fn inner(&self) -> Arc<dyn Storage> {
        // clone() so the lock is dropped immediately, otherwise concurrent
        // operations are serialized until until their Future completes.
        self.storage.lock().await.clone()
    }
}

#[async_trait]
impl Storage for WatchStorage {
    async fn get(&self, key: &str) -> Result<Cache<bytes::Bytes>> {
        self.inner().await.get(key).await
    }

    async fn del(&self, key: &str) -> Result<()> {
        self.inner().await.del(key).await
    }

    async fn has(&self, key: &str) -> bool {
        self.inner().await.has(key).await
    }

    async fn put(&self, key: &str, entry: bytes::Bytes) -> Result<Duration> {
        self.inner().await.put(key, entry).await
    }

    async fn size(&self, key: &str) -> Result<u64> {
        self.inner().await.size(key).await
    }

    /// Check the cache capability.
    async fn check(&self) -> Result<CacheMode> {
        self.inner().await.check().await
    }

    /// Get the storage location.
    async fn location(&self) -> String {
        self.inner().await.location().await
    }

    /// Get the current storage usage, if applicable.
    async fn current_size(&self) -> Result<Option<u64>> {
        self.inner().await.current_size().await
    }

    /// Get the maximum storage size, if applicable.
    async fn max_size(&self) -> Result<Option<u64>> {
        self.inner().await.max_size().await
    }

    /// Return the config for preprocessor cache mode if applicable
    fn preprocessor_cache_mode_config(&self) -> PreprocessorCacheModeConfig {
        self.preprocessor_cache_mode_config.clone()
    }
}
