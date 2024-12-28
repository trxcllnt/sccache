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
    common::{
        bincode_deserialize, bincode_req_fut, bincode_serialize, for_all_concurrent,
        ResourceLoaderQueue,
    },
    server::{ClientAuthCheck, ClientVisibleMsg},
};

use std::env;
use std::time::Duration;

/// Default timeout for connections to an sccache-dist server
const DEFAULT_DIST_CONNECT_TIMEOUT: u64 = 5;

/// Timeout for connections to an sccache-dist server
pub fn get_dist_connect_timeout() -> Duration {
    Duration::new(
        env::var("SCCACHE_DIST_CONNECT_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_DIST_CONNECT_TIMEOUT),
        0,
    )
}

/// Default timeout for compile requests to an sccache-dist server
const DEFAULT_DIST_REQUEST_TIMEOUT: u64 = 600;

/// Timeout for compile requests to an sccache-dist server
pub fn get_dist_request_timeout() -> Duration {
    Duration::new(
        env::var("SCCACHE_DIST_REQUEST_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_DIST_REQUEST_TIMEOUT),
        0,
    )
}

mod common {
    use reqwest::header;

    use futures::{lock::Mutex, FutureExt, StreamExt, TryFutureExt};
    use std::cmp::Eq;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::ops::DerefMut;
    use std::sync::Arc;

    use crate::errors::*;
    use crate::lru_disk_cache::LruCache;

    pub fn for_all_concurrent<S, F, Fut>(
        pool: &tokio::runtime::Handle,
        recv: S,
        token: tokio_util::sync::CancellationToken,
        mut f: F,
    ) -> tokio::task::JoinHandle<()>
    where
        S: futures::stream::StreamExt + Send + 'static,
        F: FnMut(S::Item) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = std::ops::ControlFlow<String, String>> + Send + 'static,
    {
        let pool1 = pool.clone();
        let token1 = token.clone();
        let token2 = token.clone();
        pool.spawn(
            recv.flat_map_unordered(None, move |item| {
                if !token1.is_cancelled() {
                    futures::stream::once(
                        pool1
                            .spawn(f(item))
                            .unwrap_or_else(|_| std::ops::ControlFlow::Continue(String::new())),
                    )
                    .boxed()
                } else {
                    futures::stream::once(futures::future::ready(std::ops::ControlFlow::Break(
                        String::new(),
                    )))
                    .boxed()
                }
            })
            .take_while(move |control_flow| {
                futures::future::ready(match control_flow {
                    std::ops::ControlFlow::Break(msg) => {
                        if !msg.is_empty() {
                            debug!("{msg}");
                        }
                        token2.cancel();
                        !token2.is_cancelled()
                    }
                    std::ops::ControlFlow::Continue(msg) => {
                        if !msg.is_empty() {
                            debug!("{msg}");
                        }
                        !token2.is_cancelled()
                    }
                })
            })
            .for_each_concurrent(None, |_| async move {})
            .boxed(),
        )
    }

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
        // // Work around tiny_http issue #151 by disabling HTTP pipeline with
        // // `Connection: close`.
        // let req = req.header(header::CONNECTION, "close");
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

    pub struct ResourceLoaderQueue<K: Eq + Hash, V> {
        fetch: Arc<
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

    impl<K, V> ResourceLoaderQueue<K, V>
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
            fetch: F,
        ) -> Self {
            Self {
                fetch: Arc::new(fetch),
                state: Arc::new(Mutex::new((LruCache::new(capacity), HashMap::new()))),
            }
        }

        pub async fn enqueue(&self, input: &K) -> Result<V> {
            let mut state = self.state.lock().await;
            let (fetched, pending) = state.deref_mut();
            if fetched.capacity() > 0 {
                if let Some(val) = fetched.get(input) {
                    return Ok(val.clone());
                }
            }
            let mut recv = if let Some(sndr) = pending.get(input) {
                sndr.subscribe()
            } else {
                let (sndr, recv) = tokio::sync::broadcast::channel(1);
                pending.insert(input.clone(), sndr);
                tokio::runtime::Handle::current().spawn({
                    let input = input.clone();
                    let fetch = self.fetch.clone();
                    let state = self.state.clone();
                    async move {
                        let val = match fetch(&input).await {
                            Ok(val) => val,
                            Err(_) => {
                                state.lock().await.1.remove(&input);
                                return;
                            }
                        };
                        let mut state = state.lock().await;
                        let (fetched, pending) = state.deref_mut();
                        let sender = pending.remove(&input).unwrap();
                        if fetched.capacity() > 0 {
                            fetched.insert(input, val.clone());
                        }
                        let _ = sender.send(val);
                    }
                });

                recv
            };

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
    pub fn scheduler_run_job(scheduler_url: &reqwest::Url, job_id: &str) -> reqwest::Url {
        scheduler_url
            .join(&format!("/api/v2/job/{job_id}/run"))
            .expect("failed to create run job url")
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

    // Messages that are non-sensitive and can be sent to the client
    #[derive(Debug)]
    pub struct ClientVisibleMsg(pub String);
    impl ClientVisibleMsg {
        pub fn from_nonsensitive(s: String) -> Self {
            ClientVisibleMsg(s)
        }
    }

    pub trait ClientAuthCheck: Send + Sync {
        fn check(&self, token: &str) -> std::result::Result<(), ClientVisibleMsg>;
    }
}

#[cfg(feature = "dist-client")]
mod client {
    use super::super::cache;
    use crate::config;
    use crate::dist::http::common::{bincode_deserialize, bincode_serialize};
    use crate::dist::pkg::{InputsPackager, ToolchainPackager};
    use crate::dist::{
        self, ClientIncoming, ClientOutgoing, CompileCommand, NewJobRequest, NewJobResponse,
        PathTransformer, RunJobRequest, RunJobResponse, SchedulerStatusResult,
        SubmitToolchainResult, Toolchain,
    };
    use crate::util::new_reqwest_client;

    use futures::{lock::Mutex, StreamExt};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc::UnboundedSender;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::Message;

    use async_trait::async_trait;
    use flate2::write::ZlibEncoder as ZlibWriteEncoder;
    use flate2::Compression;
    use reqwest::Body;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    use super::common::{
        bincode_req_fut, for_all_concurrent, ReqwestRequestBuilderExt, ResourceLoaderQueue,
    };
    use super::urls;
    use crate::errors::*;

    type WebSocketCallback<Incoming> = tokio::sync::oneshot::Sender<Incoming>;

    pub type WebSocketRequest<Outgoing, Incoming> = (
        String,                              // request id
        Option<Outgoing>,                    // request payload
        Option<WebSocketCallback<Incoming>>, // response callback
    );

    pub struct WebSocketClient<Outgoing, Incoming> {
        connection: tokio_util::sync::CancellationToken,
        outgoing: Arc<Mutex<UnboundedSender<WebSocketRequest<Outgoing, Incoming>>>>,
        requests: Arc<Mutex<HashMap<String, WebSocketCallback<Incoming>>>>,
    }

    impl<Outgoing, Incoming> WebSocketClient<Outgoing, Incoming>
    where
        Outgoing: serde::Serialize + std::fmt::Display + Send + Sync + 'static,
        Incoming:
            for<'a> serde::Deserialize<'a> + Clone + std::fmt::Display + Send + Sync + 'static,
    {
        pub async fn new<Error, Sender, Receiver, Shutdown, Connect, ConnectFut>(
            runtime: &tokio::runtime::Handle,
            connect: Connect,
            shutdown: Shutdown,
        ) -> Result<Self>
        where
            Error: std::fmt::Debug + std::fmt::Display + Send + 'static,
            Sender: futures::SinkExt<Message> + Send + Unpin + 'static,
            Receiver: futures::StreamExt<Item = std::result::Result<Message, Error>>
                + Send
                + Unpin
                + 'static,
            Shutdown: FnOnce() -> Incoming + Send + 'static,
            Connect: FnOnce() -> ConnectFut + Send + 'static,
            ConnectFut: std::future::Future<Output = Result<(Sender, Receiver)>> + Send,
        {
            let (sndr, recv) = match connect().await.context("WebSocket connect failed") {
                Ok(res) => res,
                Err(err) => {
                    info!("{err}");
                    return Err(anyhow!(err));
                }
            };

            let connection = tokio_util::sync::CancellationToken::new();
            let requests_map = Arc::new(Mutex::new(HashMap::new()));
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let requests = UnboundedReceiverStream::new(rx);

            let pool = runtime.clone();
            let token = connection.clone();
            let reqs_map = requests_map.clone();

            runtime.spawn(async move {
                // Wrap shared sender in a Mutex so the concurrent
                // `flat_map_unordered` task writes are serialized
                let sndr = Arc::new(Mutex::new(sndr));
                let ping_sndr = sndr.clone();
                let send_sndr = sndr.clone();

                // Clones to move into the futures that need mutable refs
                let sndr_reqs = reqs_map.clone();
                let recv_reqs = reqs_map.clone();

                // Create child tokens for the subtasks so we can imperatively
                // know when the parent token has been canceled
                let ping_token = token.child_token();
                let sndr_token = token.child_token();
                let recv_token = token.child_token();

                let ping_timer = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(Duration::from_secs(10)));

                let mut ping_task = for_all_concurrent(&pool, ping_timer, ping_token, move |_| {

                    // Local clones for this individual task
                    let sndr = ping_sndr.clone();

                    async move {
                        // Send the ping. Abort on error.
                        if sndr.lock().await.send(Message::Ping(uuid::Uuid::new_v4().as_bytes().into())).await.is_err() {
                            return std::ops::ControlFlow::Break("WebSocket send failure".into());
                        }
                        std::ops::ControlFlow::Continue(String::new())
                    }
                });

                let mut send_task = for_all_concurrent(&pool, requests, sndr_token, move |(req_id, req, cb): WebSocketRequest<Outgoing, Incoming>| {

                    // Local clones for this individual task
                    let sndr = send_sndr.clone();
                    let reqs = sndr_reqs.clone();

                    async move {
                        // Support sending a message without listening for a response
                        if let Some(cb) = cb {
                            // Insert request callback into the requests map
                            reqs.lock().await.insert(req_id.clone(), cb);
                        }

                        // Support listening for a message without sending a request
                        if let Some(req) = req {
                            debug!("WebSocket sending request: id={req_id} req={req}");
                            // Serialize the request
                            let buf = match bincode_serialize((req_id, req)).await {
                                Ok(buf) => buf,
                                Err(err) => {
                                    return std::ops::ControlFlow::Continue(format!("WebSocket failed to serialize request: {err}"));
                                },
                            };
                            // Send the message. Abort on error.
                            if sndr.lock().await.send(Message::Binary(buf)).await.is_err() {
                                return std::ops::ControlFlow::Break("WebSocket send failure".into());
                            }
                        }

                        std::ops::ControlFlow::Continue(String::new())
                    }
                });

                let mut recv_task = for_all_concurrent(&pool, recv, recv_token, move |msg| {

                    // Local clones for this individual task
                    let reqs = recv_reqs.clone();

                    async move {
                        let (req_id, res) = match msg {
                            Err(err) => {
                                return std::ops::ControlFlow::Break(format!("WebSocket recv failure: {err:?}"))
                            }
                            Ok(msg) => {
                                match msg {
                                    Message::Close(None) => {
                                        return std::ops::ControlFlow::Break("WebSocket close without CloseFrame".into());
                                    }
                                    Message::Close(Some(CloseFrame { code, reason })) => {
                                        return std::ops::ControlFlow::Break(format!("WebSocket disconnected code={}, reason=`{}`", code, reason));
                                    }
                                    Message::Text(str) => {
                                        return std::ops::ControlFlow::Continue(format!("WebSocket received unexpected text response: {str}"));
                                    }
                                    Message::Binary(buf) => {
                                        match bincode_deserialize::<(String, Incoming)>(buf).await {
                                            Ok(res) => res,
                                            Err(err) => {
                                                return std::ops::ControlFlow::Continue(format!("WebSocket failed to deserialize response: {err}"));
                                            }
                                        }
                                    }
                                    _ => return std::ops::ControlFlow::Continue(String::new()),
                                }
                            }
                        };

                        if let Some(cb) = reqs.lock().await.remove(&req_id) {
                            debug!("WebSocket received response: id={req_id}, res={res}");
                            if !cb.is_closed() && cb.send(res).is_err() {
                                return std::ops::ControlFlow::Break(format!("WebSocket failed to notify client of response with id={req_id}"));
                            }
                        }

                        std::ops::ControlFlow::Continue(String::new())
                    }
                });

                // Wait for either cancel/ping/send/recv to finish
                tokio::select! {
                    _ = token.cancelled() => {
                        ping_task.abort();
                        send_task.abort();
                        recv_task.abort();
                    }
                    _ = (&mut ping_task) => {
                        token.cancel();
                        send_task.abort();
                        recv_task.abort();
                    },
                    _ = (&mut send_task) => {
                        token.cancel();
                        ping_task.abort();
                        recv_task.abort();
                    },
                    _ = (&mut recv_task) => {
                        token.cancel();
                        ping_task.abort();
                        send_task.abort();
                    }
                }

                // Notify all outstanding request handlers that the WebSocket client has shutdown.
                let res = shutdown();
                for (req_id, cb) in reqs_map.lock().await.drain() {
                    if !cb.is_closed() && cb.send(res.clone()).is_err() {
                        warn!("WebSocket failed to notify client of shutdown (req_id={req_id})")
                    }
                }
            });

            Ok(Self {
                connection,
                outgoing: Arc::new(Mutex::new(tx)),
                requests: requests_map.clone(),
            })
        }

        pub fn is_closed(&self) -> bool {
            self.connection.is_cancelled()
        }

        // pub async fn close(&mut self) {
        //     self.connection.cancel();
        // }

        // pub async fn send(&mut self, req_id: String, req: Outgoing) -> Result<()> {
        //     self.outgoing
        //         .lock()
        //         .await
        //         .send((req_id, Some(req), None))
        //         .map_err(|err| {
        //             let (req_id, _, _) = err.0;
        //             anyhow!("WebSocketClient error sending request for id={req_id}")
        //         })?;
        //     Ok(())
        // }

        pub async fn send_recv(
            &self,
            req_id: String,
            req: Option<Outgoing>,
            timeout: Option<Duration>,
        ) -> Result<Incoming> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.outgoing
                .lock()
                .await
                .send((req_id.clone(), req, Some(tx)))
                .map_err(|err| {
                    let (req_id, _, _) = err.0;
                    anyhow!("WebSocketClient error sending request for id={req_id}")
                })?;

            if let Some(duration) = timeout {
                match tokio::time::timeout(duration, rx).await {
                    Ok(res) => res.map_err(anyhow::Error::new),
                    Err(_) => {
                        // Remove the callback from the requests map
                        self.requests.lock().await.remove(&req_id);
                        Err(anyhow!("WebSocket request timeout (req_id={req_id})"))
                    }
                }
            } else {
                rx.await.map_err(anyhow::Error::new)
            }
        }

        // pub async fn listen(&self, req_id: String) -> Result<Incoming> {
        //     self.send_recv(req_id, None, None).await
        // }

        // pub async fn listen_with_timeout(
        //     &self,
        //     req_id: String,
        //     timeout: Duration,
        // ) -> Result<Incoming> {
        //     self.send_recv(req_id, None, Some(timeout)).await
        // }

        // pub async fn request(&self, req: Outgoing) -> Result<Incoming> {
        //     self.send_recv(uuid::Uuid::new_v4().to_string(), Some(req), None)
        //         .await
        // }

        // pub async fn request_with_timeout(
        //     &self,
        //     req: Outgoing,
        //     timeout: Duration,
        // ) -> Result<Incoming> {
        //     self.send_recv(uuid::Uuid::new_v4().to_string(), Some(req), Some(timeout))
        //         .await
        // }
    }

    fn make_scheduler_ws_uri(scheduler_url: &reqwest::Url) -> Result<http::Uri> {
        let mut uri = scheduler_url.clone();
        uri.set_path("api/v2/client/ws");

        match uri.scheme() {
            "http" => uri.set_scheme("ws").map(|_| uri),
            "https" => uri.set_scheme("wss").map(|_| uri),
            scheme => {
                error!("Unknown scheduler URL scheme `{scheme}`");
                return Err(anyhow!("Unknown scheduler URL scheme `{scheme}`"));
            }
        }
        .map_err(|_| anyhow!("Failed to set scheduler WebSocket URI scheme"))
        .and_then(|uri| {
            http::Uri::try_from(uri.as_str().as_bytes()).map_err(|err| {
                error!("Failed to create scheduler WebSocket URI: {err}");
                anyhow::Error::new(err)
            })
        })
    }

    pub struct Client {
        auth_token: String,
        scheduler_url: reqwest::Url,
        client: Arc<Mutex<reqwest::Client>>,
        ws_client: tokio::sync::OnceCell<WebSocketClient<ClientOutgoing, ClientIncoming>>,
        pool: tokio::runtime::Handle,
        tc_cache: Arc<cache::ClientToolchains>,
        rewrite_includes_only: bool,
        submit_toolchain_reqs: ResourceLoaderQueue<(Toolchain, PathBuf), SubmitToolchainResult>,
    }

    impl Client {
        pub async fn new(
            pool: &tokio::runtime::Handle,
            scheduler_url: reqwest::Url,
            cache_dir: &Path,
            cache_size: u64,
            toolchain_configs: &[config::DistToolchainConfig],
            auth_token: String,
            rewrite_includes_only: bool,
        ) -> Result<Self> {
            let client = new_reqwest_client();
            let client = Arc::new(Mutex::new(client));
            let client_toolchains =
                cache::ClientToolchains::new(cache_dir, cache_size, toolchain_configs)
                    .context("failed to initialise client toolchains")?;

            let submit_toolchain_reqs = ResourceLoaderQueue::new(0, {
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
                scheduler_url: scheduler_url.clone(),
                client,
                pool: pool.clone(),
                tc_cache: Arc::new(client_toolchains),
                submit_toolchain_reqs,
                rewrite_includes_only,
                ws_client: tokio::sync::OnceCell::new_with(
                    {
                        let mut connect_req =
                            make_scheduler_ws_uri(&scheduler_url)?.into_client_request()?;

                        connect_req.headers_mut().insert(
                            http::header::AUTHORIZATION,
                            http::header::HeaderValue::from_str(&format!("Bearer {}", auth_token))?,
                        );

                        WebSocketClient::new(
                            pool,
                            || async move {
                                info!("Attempting to connect to dist server: {}", &scheduler_url);
                                let config =
                                    tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
                                        max_message_size: None,
                                        max_frame_size: None,
                                        ..Default::default()
                                    };
                                let (sock, response) =
                                    match tokio_tungstenite::connect_async_with_config(
                                        connect_req,
                                        Some(config),
                                        false,
                                    )
                                    .await
                                    {
                                        Ok(res) => res,
                                        Err(err) => {
                                            info!("Failed to connect to dist server: {err}");
                                            return Err(anyhow!(err));
                                        }
                                    };
                                info!("WebSocket handshake complete, response was {response:?}");
                                Ok(sock.split())
                            },
                            || {
                                info!("WebSocketClient shutdown");
                                ClientIncoming::Error {
                                    message: "WebSocketClient closed".into(),
                                }
                            },
                        )
                        .await
                    }
                    .ok(),
                ),
            })
        }

        fn ws_client(&self) -> Option<&WebSocketClient<ClientOutgoing, ClientIncoming>> {
            if let Some(ws_client) = self.ws_client.get() {
                if !ws_client.is_closed() {
                    return Some(ws_client);
                }
            }
            None
        }
    }

    #[async_trait]
    impl dist::Client for Client {
        async fn new_job(&self, toolchain: Toolchain) -> Result<NewJobResponse> {
            if let Some(ws_client) = self.ws_client() {
                match ws_client
                    .send_recv(
                        uuid::Uuid::new_v4().to_string(),
                        Some(ClientOutgoing::NewJob(NewJobRequest { toolchain })),
                        None,
                    )
                    .await
                {
                    Err(err) => Err(err),
                    Ok(ClientIncoming::NewJob(res)) => Ok(res),
                    Ok(ClientIncoming::Error { message }) => Err(anyhow!(message)),
                    Ok(res) => Err(anyhow!("Unexpected new_job response: {res:?}")),
                }
            } else {
                let req = self
                    .client
                    .lock()
                    .await
                    .post(urls::scheduler_new_job(&self.scheduler_url))
                    .bearer_auth(self.auth_token.clone())
                    .bincode(&toolchain)?;
                bincode_req_fut(req).await
            }
        }

        async fn run_job(
            &self,
            job_id: &str,
            timeout: Duration,
            toolchain: Toolchain,
            command: CompileCommand,
            outputs: Vec<String>,
            inputs_packager: Box<dyn InputsPackager>,
        ) -> Result<(RunJobResponse, PathTransformer)> {
            let job_id = job_id.to_owned();
            let (req, path_transformer) = self
                .pool
                .spawn_blocking(move || -> Result<_> {
                    let mut inputs = vec![];
                    let path_transformer;
                    {
                        let mut compressor =
                            ZlibWriteEncoder::new(&mut inputs, Compression::fast());
                        path_transformer = inputs_packager
                            .write_inputs(&mut compressor)
                            .context("Could not write inputs for compilation")?;
                        compressor.flush().context("failed to flush compressor")?;
                        trace!(
                            "Compressed inputs from {} -> {}",
                            compressor.total_in(),
                            compressor.total_out()
                        );
                        compressor.finish().context("failed to finish compressor")?;
                    }
                    Ok((
                        RunJobRequest {
                            job_id,
                            command,
                            inputs,
                            outputs,
                            toolchain,
                        },
                        path_transformer,
                    ))
                })
                .await??;

            if let Some(ws_client) = self.ws_client() {
                match ws_client
                    .send_recv(
                        uuid::Uuid::new_v4().to_string(),
                        Some(ClientOutgoing::RunJob(req)),
                        Some(timeout),
                    )
                    .await
                {
                    Err(err) => Err(err),
                    Ok(ClientIncoming::RunJob(res)) => Ok(res),
                    Ok(ClientIncoming::Error { message }) => Err(anyhow!(message)),
                    Ok(res) => Err(anyhow!("Unexpected run_job response: {res:?}")),
                }
            } else {
                debug!(
                    "[run_job({})]: sending scheduler_run_job request",
                    req.job_id
                );
                let req = self
                    .client
                    .lock()
                    .await
                    .post(urls::scheduler_run_job(&self.scheduler_url, &req.job_id))
                    .bearer_auth(self.auth_token.clone())
                    .timeout(timeout)
                    .bincode(&req)?;
                bincode_req_fut(req).await
            }
            .map(|res| (res, path_transformer))
        }

        async fn do_get_status(&self) -> Result<SchedulerStatusResult> {
            let req = self
                .client
                .lock()
                .await
                .get(urls::scheduler_status(&self.scheduler_url))
                .bearer_auth(self.auth_token.clone());
            bincode_req_fut(req).await
        }

        async fn do_submit_toolchain(&self, tc: Toolchain) -> Result<SubmitToolchainResult> {
            match self.tc_cache.get_toolchain(&tc) {
                Ok(Some(toolchain_file)) => {
                    self.submit_toolchain_reqs
                        .enqueue(&(tc, toolchain_file.path().to_path_buf()))
                        .await
                }
                Ok(None) => return Err(anyhow!("couldn't find toolchain locally")),
                Err(e) => return Err(e),
            }
        }

        async fn put_toolchain(
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
