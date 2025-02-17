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
    common::{AsyncMulticast, AsyncMulticastFn},
    server::{retry_with_jitter, ClientAuthCheck, ClientVisibleMsg},
};

pub use self::common::{bincode_deserialize, bincode_serialize};

mod common {
    use async_trait::async_trait;

    use reqwest::header;

    use futures::lock::Mutex;
    use std::cmp::Eq;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::sync::Arc;

    use crate::errors::*;

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

    #[async_trait]
    pub trait AsyncMulticastFn<'a, K: Eq + Hash, V> {
        async fn call(&self, args: &K) -> Result<V>;
    }

    #[derive(Clone)]
    pub struct AsyncMulticast<K: Eq + Hash, V> {
        run_f: Arc<dyn for<'a> AsyncMulticastFn<'a, K, V> + Send + Sync>,
        state: Arc<Mutex<HashMap<K, tokio::sync::broadcast::Sender<V>>>>,
    }

    impl<K, V> AsyncMulticast<K, V>
    where
        K: Clone + std::fmt::Debug + Eq + Hash + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        pub fn new<F>(run_f: F) -> Self
        where
            F: for<'a> AsyncMulticastFn<'a, K, V> + Send + Sync + 'static,
        {
            Self {
                run_f: Arc::new(run_f),
                state: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub async fn call(&self, args: K) -> Result<V> {
            // Lock state
            let mut state = self.state.lock().await;

            let mut recv = if let Some(sndr) = state.get(&args) {
                // Return shared broadcast receiver if pending
                sndr.subscribe()
            } else {
                // Create broadcast sender/receiver on first call
                let (sndr, recv) = tokio::sync::broadcast::channel(1);

                state.insert(args.clone(), sndr);

                // Run the function on a worker thread
                tokio::runtime::Handle::current().spawn({
                    let run_f = self.run_f.clone();
                    let state = self.state.clone();
                    async move {
                        // Call the function
                        let res = run_f.call(&args).await;

                        // Unwrap the result
                        let val = match res {
                            Ok(val) => val,
                            Err(err) => {
                                // TODO: Broadcast this error instead of just closing the channel
                                error!("AsyncMulticast error: {err:?}");
                                state.lock().await.remove(&args);
                                return;
                            }
                        };

                        // Notify receivers
                        if let Some(sndr) = state.lock().await.remove(&args) {
                            let _ = sndr.send(val);
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
    use tokio_retry2::strategy::{ExponentialBackoff, MaxInterval};
    use tokio_retry2::Retry;

    pub async fn retry_with_jitter<F>(
        limit: usize,
        func: F,
    ) -> std::result::Result<F::Item, F::Error>
    where
        F: tokio_retry2::Action,
    {
        Retry::spawn(
            ExponentialBackoff::from_millis(1000) // wait 1s before retrying
                .max_interval(10000) // set max interval to 10 seconds
                .map(tokio_retry2::strategy::jitter) // add jitter to the retry interval
                .take(limit), // limit retries
            func,
        )
        .await
    }

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

    use tokio_util::compat::FuturesAsyncReadCompatExt;

    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use reqwest::Body;
    use std::path::{Path, PathBuf};

    use super::common::{
        bincode_req_fut, AsyncMulticast, AsyncMulticastFn, ReqwestRequestBuilderExt,
    };
    use super::urls;
    use crate::errors::*;

    struct SubmitToolchainFn {
        client: Arc<reqwest::Client>,
        auth_token: String,
        scheduler_url: reqwest::Url,
        client_toolchains: Arc<cache::ClientToolchains>,
    }

    #[async_trait]
    impl AsyncMulticastFn<'_, Toolchain, SubmitToolchainResult> for SubmitToolchainFn {
        async fn call(&self, tc: &Toolchain) -> Result<SubmitToolchainResult> {
            debug!("Uploading toolchain {:?}", tc.archive_id);

            let Self {
                client,
                auth_token,
                scheduler_url,
                client_toolchains,
            } = self;

            match client_toolchains.get_toolchain(tc) {
                Err(e) => Err(e),
                Ok(None) => Err(anyhow!("Couldn't find toolchain locally")),
                Ok(Some(file)) => {
                    let body = futures::io::AllowStdIo::new(file);
                    let body = tokio_util::io::ReaderStream::new(body.compat());
                    let url = urls::scheduler_submit_toolchain(scheduler_url, &tc.archive_id);
                    let req = client
                        .put(url)
                        .bearer_auth(auth_token)
                        .body(Body::wrap_stream(body));
                    bincode_req_fut::<SubmitToolchainResult>(req).await
                }
            }
        }
    }

    pub struct Client {
        auth_token: String,
        client: Arc<reqwest::Client>,
        max_retries: f64,
        pool: tokio::runtime::Handle,
        rewrite_includes_only: bool,
        scheduler_url: reqwest::Url,
        submit_toolchain_reqs: AsyncMulticast<Toolchain, SubmitToolchainResult>,
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
            let client = Arc::new(new_reqwest_client(Some(net.clone())));
            let client_toolchains = Arc::new(
                cache::ClientToolchains::new(cache_dir, cache_size, toolchain_configs)
                    .context("failed to initialise client toolchains")?,
            );

            let submit_toolchain_reqs = AsyncMulticast::new(SubmitToolchainFn {
                client: client.clone(),
                auth_token: auth_token.clone(),
                scheduler_url: scheduler_url.clone(),
                client_toolchains: client_toolchains.clone(),
            });

            Ok(Self {
                auth_token: auth_token.clone(),
                client,
                max_retries,
                pool: pool.clone(),
                rewrite_includes_only,
                scheduler_url: scheduler_url.clone(),
                submit_toolchain_reqs,
                tc_cache: client_toolchains,
            })
        }
    }

    #[async_trait]
    impl dist::Client for Client {
        async fn new_job(&self, toolchain: Toolchain, inputs: &[u8]) -> Result<NewJobResponse> {
            bincode_req_fut(
                self.client
                    .post(urls::scheduler_new_job(&self.scheduler_url))
                    .bearer_auth(self.auth_token.clone())
                    .bincode(&NewJobRequest {
                        inputs: inputs.to_vec(),
                        toolchain,
                    })?,
            )
            .await
        }

        async fn put_job(&self, job_id: &str, inputs: &[u8]) -> Result<()> {
            bincode_req_fut(
                self.client
                    .put(urls::scheduler_put_job(&self.scheduler_url, job_id))
                    .bearer_auth(self.auth_token.clone())
                    .body(inputs.to_vec()),
            )
            .await
        }

        async fn run_job(
            &self,
            job_id: &str,
            timeout: Duration,
            toolchain: Toolchain,
            command: CompileCommand,
            outputs: Vec<String>,
        ) -> Result<RunJobResponse> {
            bincode_req_fut(
                self.client
                    .post(urls::scheduler_run_job(&self.scheduler_url, job_id))
                    .bearer_auth(self.auth_token.clone())
                    .timeout(timeout)
                    .bincode(&RunJobRequest {
                        command,
                        outputs,
                        toolchain,
                    })?,
            )
            .await
        }

        async fn del_job(&self, job_id: &str) -> Result<()> {
            bincode_req_fut(
                self.client
                    .delete(urls::scheduler_del_job(&self.scheduler_url, job_id))
                    .bearer_auth(self.auth_token.clone()),
            )
            .await
        }

        async fn get_status(&self) -> Result<SchedulerStatus> {
            bincode_req_fut(
                self.client
                    .get(urls::scheduler_status(&self.scheduler_url))
                    .bearer_auth(self.auth_token.clone()),
            )
            .await
        }

        async fn put_toolchain(&self, tc: Toolchain) -> Result<SubmitToolchainResult> {
            let id = tc.archive_id.clone();
            self.submit_toolchain_reqs
                .call(tc)
                .await
                .map_err(|_| anyhow!("Failed to submit toolchain {id:?}"))
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
