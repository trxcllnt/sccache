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

#[cfg(feature = "dist-client")]
pub use self::client::Client;
#[cfg(feature = "dist-server")]
pub use self::{
    common::AsyncMemoizer,
    server::{ClientAuthCheck, ClientVisibleMsg},
};

pub use self::common::{bincode_deserialize, bincode_serialize};

mod common {
    use reqwest::header;

    use futures::lock::Mutex;
    use std::cmp::Eq;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::ops::DerefMut;
    use std::sync::Arc;

    use crate::errors::*;
    use crate::lru_disk_cache::LruCache;

    pub async fn bincode_deserialize<T>(bytes: Vec<u8>) -> Result<T>
    where
        T: for<'de> serde::Deserialize<'de> + Send + 'static,
    {
        tokio::runtime::Handle::current()
            .spawn_blocking(move || bincode::deserialize(&bytes))
            .await
            .map_err(anyhow::Error::new)?
            .map_err(anyhow::Error::new)
    }

    pub async fn bincode_serialize<T>(value: T) -> Result<Vec<u8>>
    where
        T: serde::Serialize + Send + 'static,
    {
        tokio::runtime::Handle::current()
            .spawn_blocking(move || bincode::serialize(&value))
            .await
            .map_err(anyhow::Error::new)?
            .map_err(anyhow::Error::new)
    }

    // Note that content-length is necessary due to https://github.com/tiny-http/tiny-http/issues/147
    pub trait ReqwestRequestBuilderExt: Sized {
        fn bincode<T: serde::Serialize + ?Sized>(self, bincode: &T) -> Result<Self>;
        fn bytes(self, bytes: Vec<u8>) -> Self;
    }
    impl ReqwestRequestBuilderExt for reqwest::blocking::RequestBuilder {
        fn bincode<T: serde::Serialize + ?Sized>(self, bincode: &T) -> Result<Self> {
            let bytes =
                bincode::serialize(bincode).context("Failed to serialize body to bincode")?;
            Ok(self.bytes(bytes))
        }
        fn bytes(self, bytes: Vec<u8>) -> Self {
            self.header(
                header::CONTENT_TYPE,
                mime::APPLICATION_OCTET_STREAM.to_string(),
            )
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(bytes)
        }
    }
    impl ReqwestRequestBuilderExt for reqwest::RequestBuilder {
        fn bincode<T: serde::Serialize + ?Sized>(self, bincode: &T) -> Result<Self> {
            let bytes =
                bincode::serialize(bincode).context("Failed to serialize body to bincode")?;
            Ok(self.bytes(bytes))
        }
        fn bytes(self, bytes: Vec<u8>) -> Self {
            self.header(
                header::CONTENT_TYPE,
                mime::APPLICATION_OCTET_STREAM.to_string(),
            )
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(bytes)
        }
    }

    #[cfg(any(feature = "dist-client", feature = "dist-server"))]
    pub async fn bincode_req_fut<T: serde::de::DeserializeOwned + 'static>(
        req: reqwest::RequestBuilder,
    ) -> Result<T> {
        let res = match req.send().await {
            Ok(res) => res,
            Err(err) => {
                error!("Response error: err={err}");
                return Err(err.into());
            }
        };

        let url = res.url().clone();
        let status = res.status();
        let bytes = match res.bytes().await {
            Ok(b) => b,
            Err(err) => {
                error!("Body error: url={url}, status={status}, err={err}");
                return Err(err.into());
            }
        };
        trace!("Response: url={url}, status={status}, body={}", bytes.len());
        if !status.is_success() {
            let errmsg = format!(
                "Error {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            );
            if status.is_client_error() {
                anyhow::bail!(HttpClientError(errmsg));
            } else {
                anyhow::bail!(errmsg);
            }
        } else {
            Ok(bincode::deserialize(&bytes)?)
        }
    }

    #[derive(Clone)]
    pub struct AsyncMemoizer<K: Eq + Hash, V> {
        run_f: Arc<
            dyn Fn(&K) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<V>> + Send>>
                + Send
                + Sync,
        >,
        state: Arc<
            Mutex<(
                LruCache<K, V>,
                HashMap<K, tokio::sync::broadcast::Sender<V>>,
            )>,
        >,
    }

    impl<K, V> AsyncMemoizer<K, V>
    where
        K: Clone + std::fmt::Debug + Eq + Hash + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        pub fn new<
            F: Fn(&K) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<V>> + Send>>
                + Send
                + Sync
                + 'static,
        >(
            capacity: u64,
            run_f: F,
        ) -> Self {
            Self {
                run_f: Arc::new(run_f),
                state: Arc::new(Mutex::new((LruCache::new(capacity), HashMap::new()))),
            }
        }

        pub async fn call(&self, args: &K) -> Result<V> {
            // Lock state
            let mut state = self.state.lock().await;
            let (results, pending) = state.deref_mut();

            // Return result if cached
            if let Some(val) = results.get(args) {
                return Ok(val.clone());
            }

            let mut recv = if let Some(sndr) = pending.get(args) {
                // Return shared broadcast receiver if pending
                sndr.subscribe()
            } else {
                // Create broadcast sender/receiver on first call
                let (sndr, recv) = tokio::sync::broadcast::channel(1);
                pending.insert(args.clone(), sndr);

                // Run the function on a worker thread
                tokio::runtime::Handle::current().spawn({
                    let args = args.clone();
                    let run_f = self.run_f.clone();
                    let state = self.state.clone();
                    async move {
                        // Call the function
                        let res = run_f(&args).await;
                        // Lock state again to store and notify
                        let mut state = state.lock().await;
                        let (results, pending) = state.deref_mut();
                        match res {
                            Ok(val) => {
                                // Since both we have a lock on state, the order of these doesn't matter.
                                // Choosing to do the one that doesn't require copying args again.
                                let sender = pending.remove(&args).unwrap();
                                if results.capacity() > 0 {
                                    results.insert(args, val.clone());
                                }
                                // Unlock state before we notify
                                drop(state);
                                // Notify receivers
                                let _ = sender.send(val);
                            }
                            Err(err) => {
                                // TODO: Broadcast this error instead of just closing the channel
                                error!("AsyncMemoizer: Error loading resource: {err:?}");
                                pending.remove(&args);
                            }
                        }
                    }
                });

                recv
            };

            // Unlock state while we await the receiver
            drop(state);

            recv.recv().await.map_err(anyhow::Error::new)
        }
    }
}

pub mod urls {
    pub fn scheduler_status(scheduler_url: &reqwest::Url) -> reqwest::Url {
        scheduler_url
            .join("/api/v2/status")
            .expect("failed to create alloc job url")
    }
    pub fn scheduler_new_job(scheduler_url: &reqwest::Url) -> reqwest::Url {
        scheduler_url
            .join("/api/v2/jobs/new")
            .expect("failed to create new job url")
    }
    pub fn scheduler_put_job(scheduler_url: &reqwest::Url, job_id: &str) -> reqwest::Url {
        scheduler_url
            .join(&format!("/api/v2/job/{job_id}"))
            .expect("failed to create put job url")
    }
    pub fn scheduler_run_job(scheduler_url: &reqwest::Url, job_id: &str) -> reqwest::Url {
        scheduler_url
            .join(&format!("/api/v2/job/{job_id}"))
            .expect("failed to create run job url")
    }
    pub fn scheduler_del_job(scheduler_url: &reqwest::Url, job_id: &str) -> reqwest::Url {
        scheduler_url
            .join(&format!("/api/v2/job/{job_id}"))
            .expect("failed to create put job url")
    }
    pub fn scheduler_submit_toolchain(
        scheduler_url: &reqwest::Url,
        archive_id: &str,
    ) -> reqwest::Url {
        scheduler_url
            .join(&format!("/api/v2/toolchain/{archive_id}"))
            .expect("failed to create submit toolchain url")
    }
}

#[cfg(feature = "dist-server")]
mod server {
    use async_trait::async_trait;

    // Messages that are non-sensitive and can be sent to the client
    #[derive(Debug)]
    pub struct ClientVisibleMsg(pub String);
    impl ClientVisibleMsg {
        pub fn from_nonsensitive(s: String) -> Self {
            ClientVisibleMsg(s)
        }
    }

    #[async_trait]
    pub trait ClientAuthCheck: Send + Sync {
        async fn check(&self, token: &str) -> std::result::Result<(), ClientVisibleMsg>;
    }
}

#[cfg(feature = "dist-client")]
mod client {
    use super::super::cache;
    use crate::config;
    use crate::dist::pkg::ToolchainPackager;
    use crate::dist::{
        self, CompileCommand, NewJobRequest, NewJobResponse, RunJobRequest, RunJobResponse,
        SchedulerStatus, SubmitToolchainResult, Toolchain,
    };
    use crate::util::new_reqwest_client;

    use futures::lock::Mutex;

    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use reqwest::Body;
    use std::path::{Path, PathBuf};

    use super::common::{bincode_req_fut, AsyncMemoizer, ReqwestRequestBuilderExt};
    use super::urls;
    use crate::errors::*;

    pub struct Client {
        auth_token: String,
        client: Arc<Mutex<reqwest::Client>>,
        max_retries: f64,
        pool: tokio::runtime::Handle,
        rewrite_includes_only: bool,
        scheduler_url: reqwest::Url,
        submit_toolchain_reqs: AsyncMemoizer<(Toolchain, PathBuf), SubmitToolchainResult>,
        tc_cache: Arc<cache::ClientToolchains>,
    }

    impl Client {
        #[allow(clippy::too_many_arguments)]
        pub async fn new(
            pool: &tokio::runtime::Handle,
            scheduler_url: reqwest::Url,
            cache_dir: &Path,
            cache_size: u64,
            toolchain_configs: &[config::DistToolchainConfig],
            auth_token: String,
            max_retries: f64,
            rewrite_includes_only: bool,
            net: &config::DistNetworking,
        ) -> Result<Self> {
            let client = new_reqwest_client(Some(net.clone()));
            let client = Arc::new(Mutex::new(client));
            let client_toolchains =
                cache::ClientToolchains::new(cache_dir, cache_size, toolchain_configs)
                    .context("failed to initialise client toolchains")?;

            let submit_toolchain_reqs = AsyncMemoizer::new(0, {
                let client = client.clone();
                let auth_token = auth_token.clone();
                let scheduler_url = scheduler_url.clone();

                move |(tc, path): &(Toolchain, PathBuf)| {
                    let path = path.clone();
                    let client = client.clone();
                    let auth_token = auth_token.clone();
                    let url = urls::scheduler_submit_toolchain(&scheduler_url, &tc.archive_id);
                    Box::pin(async move {
                        let req = client.lock().await.put(url).bearer_auth(auth_token).body(
                            Body::wrap_stream(tokio_util::io::ReaderStream::new(
                                tokio::fs::File::open(path).await?,
                            )),
                        );
                        bincode_req_fut::<SubmitToolchainResult>(req).await
                    })
                }
            });

            Ok(Self {
                auth_token: auth_token.clone(),
                client,
                max_retries,
                pool: pool.clone(),
                rewrite_includes_only,
                scheduler_url: scheduler_url.clone(),
                submit_toolchain_reqs,
                tc_cache: Arc::new(client_toolchains),
            })
        }
    }

    #[async_trait]
    impl dist::Client for Client {
        async fn new_job(&self, toolchain: Toolchain, inputs: &[u8]) -> Result<NewJobResponse> {
            let req = self
                .client
                .lock()
                .await
                .post(urls::scheduler_new_job(&self.scheduler_url))
                .bearer_auth(self.auth_token.clone())
                .bincode(&NewJobRequest {
                    inputs: inputs.to_vec(),
                    toolchain,
                })?;

            bincode_req_fut(req).await
        }

        async fn put_job(&self, job_id: &str, inputs: &[u8]) -> Result<()> {
            let req = self
                .client
                .lock()
                .await
                .put(urls::scheduler_put_job(&self.scheduler_url, job_id))
                .bearer_auth(self.auth_token.clone())
                .body(inputs.to_vec());

            bincode_req_fut(req).await
        }

        async fn run_job(
            &self,
            job_id: &str,
            timeout: Duration,
            toolchain: Toolchain,
            command: CompileCommand,
            outputs: Vec<String>,
        ) -> Result<RunJobResponse> {
            let req = self
                .client
                .lock()
                .await
                .post(urls::scheduler_run_job(&self.scheduler_url, job_id))
                .bearer_auth(self.auth_token.clone())
                .timeout(timeout)
                .bincode(&RunJobRequest {
                    command,
                    outputs,
                    toolchain,
                })?;

            bincode_req_fut(req).await
        }

        async fn del_job(&self, job_id: &str) -> Result<()> {
            let req = self
                .client
                .lock()
                .await
                .delete(urls::scheduler_del_job(&self.scheduler_url, job_id))
                .bearer_auth(self.auth_token.clone());

            bincode_req_fut(req).await
        }

        async fn get_status(&self) -> Result<SchedulerStatus> {
            let req = self
                .client
                .lock()
                .await
                .get(urls::scheduler_status(&self.scheduler_url))
                .bearer_auth(self.auth_token.clone());
            bincode_req_fut(req).await
        }

        async fn put_toolchain(&self, tc: Toolchain) -> Result<SubmitToolchainResult> {
            match self.tc_cache.get_toolchain(&tc) {
                Ok(Some(toolchain_file)) => {
                    self.submit_toolchain_reqs
                        .call(&(tc, toolchain_file.path().to_path_buf()))
                        .await
                }
                Ok(None) => return Err(anyhow!("couldn't find toolchain locally")),
                Err(e) => return Err(e),
            }
        }

        async fn put_toolchain_local(
            &self,
            compiler_path: PathBuf,
            weak_key: String,
            toolchain_packager: Box<dyn ToolchainPackager>,
        ) -> Result<(Toolchain, Option<(String, PathBuf)>)> {
            let compiler_path = compiler_path.to_owned();
            let weak_key = weak_key.to_owned();
            let tc_cache = self.tc_cache.clone();

            self.pool
                .spawn_blocking(move || {
                    tc_cache.put_toolchain(&compiler_path, &weak_key, toolchain_packager)
                })
                .await?
        }

        fn max_retries(&self) -> f64 {
            self.max_retries
        }

        fn rewrite_includes_only(&self) -> bool {
            self.rewrite_includes_only
        }

        fn get_custom_toolchain(&self, exe: &Path) -> Option<PathBuf> {
            match self.tc_cache.get_custom_toolchain(exe) {
                Some(Ok((_, _, path))) => Some(path),
                _ => None,
            }
        }
    }
}
