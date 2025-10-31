// Copyright 2017 Mozilla Foundation
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

use crate::cache::{AsyncReadSeek, Cache, PreprocessorCacheModeConfig, ReadSeek, Storage};
use crate::errors::*;
use async_trait::async_trait;
use futures::channel::mpsc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

/// A mock `Storage` implementation.
pub struct MockStorage {
    rx: Arc<Mutex<mpsc::UnboundedReceiver<Result<Cache<Box<dyn ReadSeek>>>>>>,
    tx: mpsc::UnboundedSender<Result<Cache<Box<dyn ReadSeek>>>>,
    delay: Option<Duration>,
    preprocessor_cache_mode: bool,
}

impl MockStorage {
    /// Create a new `MockStorage`. if `delay` is `Some`, wait for that amount of time before returning from operations.
    pub(crate) fn new(delay: Option<Duration>, preprocessor_cache_mode: bool) -> MockStorage {
        let (tx, rx) = mpsc::unbounded();
        Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
            delay,
            preprocessor_cache_mode,
        }
    }

    /// Queue up `res` to be returned as the next result from `Storage::get`.
    pub(crate) fn next_get(&self, res: Result<Cache<Box<dyn ReadSeek>>>) {
        self.tx.unbounded_send(res).unwrap();
    }
}

#[async_trait]
impl Storage for MockStorage {
    async fn get(&self, _key: &str) -> Result<Cache<Box<dyn ReadSeek>>> {
        if let Some(delay) = self.delay {
            sleep(delay).await;
        }
        let next = match self.rx.lock().await.try_next() {
            Ok(next) => next,
            Err(_) => return Ok(Cache::Miss),
        };

        next.expect("MockStorage get called but no get results available")
    }
    async fn get_async_reader(&self, key: &str) -> Result<Cache<Box<dyn AsyncReadSeek + Unpin>>> {
        if let Some(delay) = self.delay {
            sleep(delay).await;
        }
        match self.get(key).await? {
            // Err(err) => Err(anyhow!("No cache entry for key `{key}`")),
            Cache::Miss => Ok(Cache::Miss),
            Cache::Hit(reader) => Ok(Cache::Hit(Box::new(futures::io::AllowStdIo::new(reader)))),
        }
    }
    async fn del(&self, _key: &str) -> Result<()> {
        if let Some(delay) = self.delay {
            sleep(delay).await;
        }
        Ok(())
    }
    async fn has(&self, _key: &str) -> bool {
        false
    }
    async fn put(&self, _key: &str, _entry: &mut dyn ReadSeek) -> Result<Duration> {
        Ok(if let Some(delay) = self.delay {
            sleep(delay).await;
            delay
        } else {
            Duration::from_secs(0)
        })
    }
    async fn put_async_reader(
        &self,
        _key: &str,
        _size: u64,
        _source: &mut (dyn AsyncReadSeek + Unpin),
    ) -> Result<()> {
        if let Some(delay) = self.delay {
            sleep(delay).await;
        }
        Ok(())
    }
    async fn size(&self, _key: &str) -> Result<u64> {
        if let Some(delay) = self.delay {
            sleep(delay).await;
        }
        let next = self.rx.lock().await.try_next().unwrap();
        if let Some(Ok(Cache::Hit(mut next))) = next {
            let mut data = vec![];
            return Ok(next.read_to_end(&mut data)? as u64);
        }
        Ok(0)
    }
    async fn location(&self) -> String {
        "Mock Storage".to_string()
    }
    async fn current_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }
    async fn max_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }
    fn preprocessor_cache_mode_config(&self) -> PreprocessorCacheModeConfig {
        PreprocessorCacheModeConfig {
            use_preprocessor_cache_mode: self.preprocessor_cache_mode,
            ..Default::default()
        }
    }
}
