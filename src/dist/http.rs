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
    scheduler::Scheduler,
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
                if log_enabled!(log::Level::Debug) {
                    debug!(
                        "Request error: {}",
                        [
                            err.url().map(|u| format!("url={u:?}")).unwrap_or_default(),
                            err.status()
                                .map(|u| format!("status={u:?}"))
                                .unwrap_or_default(),
                            format!("err={err:?}"),
                        ]
                        .join(", ")
                    );
                }
                return Err(err.into());
            }
        };

        let url = res.url().clone();
        let status = res.status();
        let bytes = match res.bytes().await {
            Ok(b) => b,
            Err(err) => {
                debug!("Request body error: url={url}, status={status}, err={err}");
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
    use tokio_retry2::strategy::FibonacciBackoff;
    use tokio_retry2::Retry;

    pub async fn retry_with_jitter<F>(
        limit: usize,
        func: F,
    ) -> std::result::Result<F::Item, F::Error>
    where
        F: tokio_retry2::Action,
    {
        Retry::spawn(
            FibonacciBackoff::from_millis(1000) // wait 1s before retrying
                .max_delay_millis(10000) // set max interval to 10 seconds
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

#[cfg(feature = "dist-server")]
mod scheduler {

    use async_trait::async_trait;

    use axum::{
        body::Bytes,
        extract::{
            ConnectInfo, DefaultBodyLimit, Extension, FromRequest, FromRequestParts, MatchedPath,
            Path, Request,
        },
        http::{request::Parts, HeaderMap, Method, StatusCode, Uri},
        response::{IntoResponse, Response},
        routing, RequestPartsExt, Router,
    };

    use axum_extra::{
        headers::{authorization::Bearer, Authorization},
        TypedHeader,
    };

    use futures::TryStreamExt;

    use hyper_util::rt::{TokioExecutor, TokioIo};

    use serde_json::json;

    use std::{io, net::SocketAddr, str::FromStr, sync::Arc, time::Instant};

    use tokio::net::TcpListener;
    use tokio_util::{compat::TokioAsyncReadCompatExt, io::StreamReader};
    use tower::{Service, ServiceBuilder, ServiceExt};
    use tower_http::{
        request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
        sensitive_headers::{SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer},
        trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
    };

    use crate::dist::{
        http::{bincode_deserialize, ClientAuthCheck},
        metrics::Metrics,
        NewJobRequest, RunJobRequest, SchedulerService, Toolchain,
    };

    use crate::errors::*;

    fn get_header_value<'a>(headers: &'a HeaderMap, name: &'a str) -> Option<&'a str> {
        if let Some(header) = headers.get(name) {
            if let Ok(header) = header.to_str() {
                return Some(header);
            }
        }
        None
    }

    fn get_content_length(headers: &HeaderMap) -> u64 {
        Option::<&str>::None
            .or(get_header_value(headers, "Content-Length"))
            .or(get_header_value(headers, "content-length"))
            .unwrap_or("0")
            .parse()
            .unwrap_or(0)
    }

    /// Return `content` as either a bincode or json encoded `Response` depending on the Accept header.
    fn result_to_response<T>(
        headers: HeaderMap,
    ) -> impl FnOnce(T) -> std::result::Result<Response, Response>
    where
        T: serde::Serialize,
    {
        move |content: T| {
            if let Some(header) = headers.get("Accept") {
                // This is the only function we use from rouille.
                // Maybe we can find a replacement?
                match rouille::input::priority_header_preferred(
                    header.to_str().unwrap_or("*/*"),
                    ["application/octet-stream", "application/json"]
                        .iter()
                        .cloned(),
                ) {
                    // application/octet-stream
                    Some(0) => match bincode::serialize(&content) {
                        Ok(body) => Ok((StatusCode::OK, body).into_response()),
                        Err(err) => {
                            tracing::error!("Failed to serialize response body: {err:?}");
                            Err((
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("Failed to serialize response body: {err}").into_bytes(),
                            )
                                .into_response())
                        }
                    },
                    // application/json
                    Some(1) => Ok((
                        StatusCode::OK,
                        json!(content).as_str().unwrap().to_string().into_bytes(),
                    )
                        .into_response()),
                    _ => {
                        tracing::error!(
                            "Request must accept application/json or application/octet-stream"
                        );
                        Err((
                            StatusCode::BAD_REQUEST,
                            "Request must accept application/json or application/octet-stream"
                                .to_string()
                                .into_bytes(),
                        )
                            .into_response())
                    }
                }
            } else {
                tracing::error!("Request must accept application/json or application/octet-stream");
                Err((
                    StatusCode::BAD_REQUEST,
                    "Request must accept application/json or application/octet-stream"
                        .to_string()
                        .into_bytes(),
                )
                    .into_response())
            }
        }
    }

    fn anyhow_to_response(
        method: Method,
        uri: Uri,
    ) -> impl FnOnce(anyhow::Error) -> std::result::Result<Response, Response> {
        move |err: anyhow::Error| {
            tracing::error!("sccache: `{method} {uri}` failed with: {err:?}");
            let msg = format!("sccache: `{method} {uri}` failed with: `{err}`");
            Err((StatusCode::INTERNAL_SERVER_ERROR, msg.into_bytes()).into_response())
        }
    }

    fn unwrap_infallible<T>(result: std::result::Result<T, std::convert::Infallible>) -> T {
        match result {
            Ok(value) => value,
            Err(err) => match err {},
        }
    }

    fn with_request_tracing(app: Router) -> Router {
        // Mark these headers as sensitive so they don't show in logs
        let headers_to_redact: Arc<[_]> = Arc::new([
            http::header::AUTHORIZATION,
            http::header::PROXY_AUTHORIZATION,
            http::header::COOKIE,
            http::header::SET_COOKIE,
        ]);

        let request_id_header_name = http::HeaderName::from_str(
            &std::env::var("SCCACHE_DIST_REQUEST_ID_HEADER_NAME")
                // tower_http::request_id::X_REQUEST_ID inlined here because it is not public
                .unwrap_or("x-request-id".to_owned()),
        )
        .unwrap();

        app.layer(
            ServiceBuilder::new()
                .layer(SetSensitiveRequestHeadersLayer::from_shared(Arc::clone(
                    &headers_to_redact,
                )))
                .layer(SetRequestIdLayer::new(
                    request_id_header_name,
                    MakeRequestUuid,
                ))
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(DefaultMakeSpan::new().include_headers(true))
                        .on_response(DefaultOnResponse::new().include_headers(true)),
                )
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(SetSensitiveResponseHeadersLayer::from_shared(
                    headers_to_redact,
                )),
        )
    }

    fn with_metrics(app: Router, metrics: Metrics) -> Router {
        let app = if let Some(path) = metrics.listen_path() {
            app.route(
                &path,
                routing::get(move || std::future::ready(metrics.render())),
            )
        } else {
            app
        };

        async fn record_metrics(req: Request, next: axum::middleware::Next) -> impl IntoResponse {
            let start = Instant::now();
            let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
                matched_path.as_str().to_owned()
            } else {
                req.uri().path().to_owned()
            };
            let method = req.method().clone();

            let response = next.run(req).await;

            let latency = start.elapsed().as_secs_f64();
            let status = response.status().as_u16().to_string();

            let labels = [
                ("method", method.to_string()),
                ("path", path),
                ("status", status),
            ];

            metrics::counter!("sccache::scheduler::http::request_count", &labels).increment(1);
            metrics::histogram!("sccache::scheduler::http::request_time", &labels).record(latency);

            response
        }

        // Define this at the end so we also track metrics for the `/metrics` route
        app.route_layer(axum::middleware::from_fn(record_metrics))
    }

    // Verify authenticated sccache clients
    #[allow(dead_code)]
    struct AuthenticatedClient(SocketAddr);

    #[async_trait]
    impl<S> FromRequestParts<S> for AuthenticatedClient
    where
        S: Send + Sync,
    {
        type Rejection = StatusCode;

        async fn from_request_parts(
            parts: &mut Parts,
            _state: &S,
        ) -> std::result::Result<Self, Self::Rejection> {
            let TypedHeader(Authorization(bearer)) = parts
                .extract::<TypedHeader<Authorization<Bearer>>>()
                .await
                .map_err(|_| StatusCode::UNAUTHORIZED)?;

            let ConnectInfo(remote_addr) = parts
                .extract::<ConnectInfo<SocketAddr>>()
                .await
                .map_err(|_| StatusCode::BAD_REQUEST)?;

            let Extension(this) = parts
                .extract::<Extension<Arc<SchedulerState>>>()
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            this.client_auth
                .check(bearer.token())
                .await
                .map(|_| AuthenticatedClient(remote_addr))
                .map_err(|err| {
                    tracing::warn!(
                        "[AuthenticatedClient({remote_addr})]: invalid client auth: {}",
                        err.0
                    );
                    StatusCode::UNAUTHORIZED
                })
        }
    }

    struct Bincode<T>(T);

    #[async_trait]
    impl<S, T> FromRequest<S> for Bincode<T>
    where
        Bytes: FromRequest<S>,
        S: Send + Sync,
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        type Rejection = Response;

        async fn from_request(
            req: Request,
            state: &S,
        ) -> std::result::Result<Self, Self::Rejection> {
            let data = match get_header_value(req.headers(), "Content-Type") {
                Some("application/octet-stream") => Bytes::from_request(req, state)
                    .await
                    .map_err(IntoResponse::into_response)?
                    .to_vec(),
                _ => return Err((StatusCode::BAD_REQUEST, "Wrong content type").into_response()),
            };

            let data = bincode_deserialize::<T>(data)
                .await
                .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()).into_response())?;

            Ok(Self(data))
        }
    }

    struct RequestBodyAsyncRead(Box<dyn futures::AsyncRead + Send + Unpin>);

    #[async_trait]
    impl<S> FromRequest<S> for RequestBodyAsyncRead
    where
        S: Send + Sync,
    {
        type Rejection = Response;

        async fn from_request(
            req: Request,
            _state: &S,
        ) -> std::result::Result<Self, Self::Rejection> {
            // Convert the request body stream into an `AsyncRead`
            let reader = StreamReader::new(
                req.into_body()
                    .into_data_stream()
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err)),
            )
            .compat();

            Ok(Self(
                Box::new(reader) as Box<dyn futures::AsyncRead + Send + Unpin>
            ))
        }
    }

    struct SchedulerState {
        service: Arc<dyn SchedulerService>,
        // Test whether clients are permitted to use the scheduler
        client_auth: Box<dyn ClientAuthCheck>,
    }

    pub struct Scheduler {
        state: Arc<SchedulerState>,
    }

    impl Scheduler {
        pub fn new(
            service: Arc<dyn SchedulerService>,
            client_auth: Box<dyn ClientAuthCheck>,
        ) -> Self {
            Self {
                state: Arc::new(SchedulerState {
                    service,
                    client_auth,
                }),
            }
        }

        fn make_router() -> axum::Router {
            Router::new()
                .route(
                    "/api/v2/status",
                    routing::get(
                        |// Authenticate the client bearer token first
                         _: AuthenticatedClient,
                         headers: HeaderMap,
                         method: Method,
                         uri: Uri,
                         Extension(state): Extension<Arc<SchedulerState>>| async move {
                            state.service.get_status().await.map_or_else(
                                anyhow_to_response(method, uri),
                                result_to_response(headers),
                            )
                        },
                    ),
                )

                //
                // TOOLCHAIN OPERATIONS
                //

                // HEAD
                .route(
                    "/api/v2/toolchain/:archive_id",
                    routing::head(
                        |// Authenticate the client bearer token first
                         _: AuthenticatedClient,
                         Extension(state): Extension<Arc<SchedulerState>>,
                         Path(archive_id): Path<String>| async move {
                            if state.service.has_toolchain(&Toolchain { archive_id }).await {
                                (StatusCode::OK).into_response()
                            } else {
                                (StatusCode::NOT_FOUND).into_response()
                            }
                        },
                    ),
                )
                // PUT
                .route(
                    "/api/v2/toolchain/:archive_id",
                    routing::put(
                        |// Authenticate the client bearer token first
                         _: AuthenticatedClient,
                         headers: HeaderMap,
                         method: Method,
                         uri: Uri,
                         Extension(state): Extension<Arc<SchedulerState>>,
                         Path(archive_id): Path<String>,
                         RequestBodyAsyncRead(toolchain): RequestBodyAsyncRead| async move {
                            let tc = Toolchain { archive_id };
                            let src = std::pin::pin!(toolchain);
                            let len = get_content_length(&headers);
                            state
                                .service
                                .put_toolchain(&tc, len, src)
                                .await
                                .map_or_else(
                                    anyhow_to_response(method, uri),
                                    result_to_response(headers),
                                )
                        },
                    ),
                )
                // DELETE
                .route(
                    "/api/v2/toolchain/:archive_id",
                    routing::delete(
                        |// Authenticate the client bearer token first
                         _: AuthenticatedClient,
                         headers: HeaderMap,
                         method: Method,
                         uri: Uri,
                         Extension(state): Extension<Arc<SchedulerState>>,
                         Path(archive_id): Path<String>| async move {
                            state
                                .service
                                .del_toolchain(&Toolchain { archive_id })
                                .await
                                .map_or_else(
                                    anyhow_to_response(method, uri),
                                    result_to_response(headers),
                                )
                        },
                    ),
                )

                //
                // JOB OPERATIONS
                //

                // CREATE
                .route(
                    "/api/v2/jobs/new",
                    routing::post(
                        |// Authenticate the client bearer token first
                         _: AuthenticatedClient,
                         headers: HeaderMap,
                         method: Method,
                         uri: Uri,
                         Extension(state): Extension<Arc<SchedulerState>>,
                         Bincode(req): Bincode<NewJobRequest>| async move {
                            state.service.new_job(req).await.map_or_else(
                                anyhow_to_response(method, uri),
                                result_to_response(headers),
                            )
                        },
                    ),
                )
                // PUT
                .route(
                    "/api/v2/job/:job_id",
                    routing::put(
                        |// Authenticate the client bearer token first
                         _: AuthenticatedClient,
                         headers: HeaderMap,
                         method: Method,
                         uri: Uri,
                         Extension(state): Extension<Arc<SchedulerState>>,
                         Path(job_id): Path<String>,
                         RequestBodyAsyncRead(inputs): RequestBodyAsyncRead| async move {
                            let src = std::pin::pin!(inputs);
                            let len = get_content_length(&headers);
                            state.service.put_job(&job_id, len, src).await.map_or_else(
                                anyhow_to_response(method, uri),
                                result_to_response(headers),
                            )
                        },
                    ),
                )
                // POST
                .route(
                    "/api/v2/job/:job_id",
                    routing::post(
                        |// Authenticate the client bearer token first
                         _: AuthenticatedClient,
                         headers: HeaderMap,
                         method: Method,
                         uri: Uri,
                         Extension(state): Extension<Arc<SchedulerState>>,
                         Path(job_id): Path<String>,
                         Bincode(req): Bincode<RunJobRequest>| async move {
                            state.service.run_job(&job_id, req).await.map_or_else(
                                anyhow_to_response(method, uri),
                                result_to_response(headers),
                            )
                        },
                    ),
                )
                // DELETE
                .route(
                    "/api/v2/job/:job_id",
                    routing::delete(
                        |// Authenticate the client bearer token first
                         _: AuthenticatedClient,
                         headers: HeaderMap,
                         method: Method,
                         uri: Uri,
                         Extension(state): Extension<Arc<SchedulerState>>,
                         Path(job_id): Path<String>| async move {
                            state.service.del_job(&job_id).await.map_or_else(
                                anyhow_to_response(method, uri),
                                result_to_response(headers),
                            )
                        },
                    ),
                )
        }

        pub async fn serve(
            self,
            addr: SocketAddr,
            max_body_size: usize,
            metrics: Metrics,
        ) -> Result<()> {
            let app = Self::make_router()
                .fallback(|| async move { (StatusCode::NOT_FOUND, "404") })
                .layer(DefaultBodyLimit::max(max_body_size))
                .layer(Extension(self.state.clone()));

            let app = with_metrics(app, metrics);

            let app = with_request_tracing(app);

            let mut make_service = app.into_make_service_with_connect_info::<SocketAddr>();

            let listener = TcpListener::bind(addr).await.unwrap();

            tracing::info!("sccache: Scheduler listening for clients on {}", addr);

            loop {
                let (tcp_stream, remote_addr) = listener.accept().await.unwrap();
                let tower_service = unwrap_infallible(make_service.call(remote_addr).await);

                tokio::spawn(async move {
                    // Hyper has its own `AsyncRead` and `AsyncWrite` traits and doesn't use tokio.
                    // `TokioIo` converts between them.
                    let tok_stream = TokioIo::new(tcp_stream);

                    let hyper_service = hyper::service::service_fn(
                        move |request: Request<hyper::body::Incoming>| {
                            // Clone `tower_service` because hyper's `Service` uses `&self` whereas
                            // tower's `Service` requires `&mut self`.
                            tower_service.clone().oneshot(request)
                        },
                    );

                    if let Err(err) =
                        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .http2()
                            .serve_connection(tok_stream, hyper_service)
                            .await
                    {
                        tracing::debug!("sccache: failed to serve connection: {err:#}");
                    }
                });
            }
        }
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

            match client_toolchains.get_toolchain(tc).await {
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
        fallback_to_local_compile: bool,
        max_retries: f64,
        rewrite_includes_only: bool,
        scheduler_url: reqwest::Url,
        submit_toolchain_reqs: AsyncMulticast<Toolchain, SubmitToolchainResult>,
        tc_cache: Arc<cache::ClientToolchains>,
    }

    impl Client {
        #[allow(clippy::too_many_arguments)]
        pub async fn new(
            scheduler_url: reqwest::Url,
            cache_dir: &Path,
            cache_size: u64,
            toolchain_configs: &[config::DistToolchainConfig],
            auth_token: String,
            fallback_to_local_compile: bool,
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
                fallback_to_local_compile,
                max_retries,
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
            self.tc_cache
                .put_toolchain(&compiler_path, &weak_key, toolchain_packager)
                .await
        }

        fn fallback_to_local_compile(&self) -> bool {
            self.fallback_to_local_compile
        }

        fn max_retries(&self) -> f64 {
            self.max_retries
        }

        fn rewrite_includes_only(&self) -> bool {
            self.rewrite_includes_only
        }

        async fn get_custom_toolchain(&self, exe: &Path) -> Option<PathBuf> {
            match self.tc_cache.get_custom_toolchain(exe).await {
                Some(Ok((_, _, path))) => Some(path),
                _ => None,
            }
        }
    }
}
