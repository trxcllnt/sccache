#[cfg(feature = "dist-server")]
pub use self::internal::Scheduler;

#[cfg(feature = "dist-server")]
mod internal {

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

    use futures::{pin_mut, TryStreamExt};

    use hyper_util::rt::{TokioExecutor, TokioIo};

    use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
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

    use crate::{
        config::MetricsConfig,
        dist::{
            http::{bincode_deserialize, ClientAuthCheck},
            NewJobRequest, RunJobRequest, SchedulerService, Toolchain,
        },
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

    fn with_metrics(app: Router, metrics: MetricsConfig) -> Router {
        const EXPONENTIAL_SECONDS: &[f64] = &[
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];

        let builder = PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full("http_requests_duration_seconds".to_string()),
                EXPONENTIAL_SECONDS,
            )
            .unwrap();

        let app = match metrics {
            MetricsConfig::Listen { path, .. } => {
                let handle = builder.install_recorder().unwrap();

                tokio::spawn({
                    let handle = handle.clone();
                    async move {
                        loop {
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            handle.run_upkeep();
                        }
                    }
                });

                app.route(
                    &path.unwrap_or("/metrics".to_owned()),
                    routing::get(move || std::future::ready(handle.render())),
                )
            }
            MetricsConfig::Gateway {
                endpoint,
                interval,
                username,
                password,
            } => {
                builder
                    .with_push_gateway(
                        endpoint,
                        std::time::Duration::from_millis(interval),
                        username,
                        password,
                    )
                    .unwrap()
                    .install()
                    .unwrap();
                app
            }
        };

        // Define this here so we also track metrics on the `/metrics` route
        app.route_layer(axum::middleware::from_fn(track_metrics))
    }

    async fn track_metrics(req: Request, next: axum::middleware::Next) -> impl IntoResponse {
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

        metrics::counter!("sccache_scheduler_http_requests", &labels).increment(1);
        metrics::histogram!("sccache_scheduler_http_requests_duration", &labels).record(latency);

        response
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

    struct SchedulerState {
        service: Arc<dyn SchedulerService>,
        // Test whether clients are permitted to use the scheduler
        client_auth: Box<dyn ClientAuthCheck>,
    }

    pub struct Scheduler {
        state: Arc<SchedulerState>,
    }

    impl Scheduler {
        pub fn new<S: SchedulerService + 'static>(
            service: Arc<S>,
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
                .route(
                    "/api/v2/toolchain/:archive_id",
                    routing::head(
                        |// Authenticate the client bearer token first
                         _: AuthenticatedClient,
                         Extension(state): Extension<Arc<SchedulerState>>,
                         Path(archive_id): Path<String>| async move {
                            if state.service.has_toolchain(Toolchain { archive_id }).await {
                                (StatusCode::OK).into_response()
                            } else {
                                (StatusCode::NOT_FOUND).into_response()
                            }
                        },
                    ),
                )
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
                         request: Request| async move {
                            // Convert the request body stream into an `AsyncRead`
                            let toolchain_reader = StreamReader::new(
                                request
                                    .into_body()
                                    .into_data_stream()
                                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err)),
                            )
                            .compat();

                            pin_mut!(toolchain_reader);

                            state
                                .service
                                .put_toolchain(Toolchain { archive_id }, toolchain_reader)
                                .await
                                .map_or_else(
                                    anyhow_to_response(method, uri),
                                    result_to_response(headers),
                                )
                        },
                    ),
                )
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
                .route(
                    "/api/v2/job/:job_id/run",
                    routing::post(
                        |// Authenticate the client bearer token first
                         _: AuthenticatedClient,
                         headers: HeaderMap,
                         method: Method,
                         uri: Uri,
                         Extension(state): Extension<Arc<SchedulerState>>,
                         Path(job_id): Path<String>,
                         Bincode(req): Bincode<RunJobRequest>| async move {
                            if job_id != req.job_id {
                                Ok((StatusCode::BAD_REQUEST).into_response())
                            } else {
                                state.service.run_job(req).await.map_or_else(
                                    anyhow_to_response(method, uri),
                                    result_to_response(headers),
                                )
                            }
                        },
                    ),
                )
        }

        pub async fn serve(
            self,
            addr: SocketAddr,
            max_body_size: usize,
            metrics: Option<MetricsConfig>,
        ) -> Result<()> {
            let app = Self::make_router()
                .fallback(|| async move { (StatusCode::NOT_FOUND, "404") })
                .layer(DefaultBodyLimit::max(max_body_size))
                .layer(Extension(self.state.clone()));

            let app = if let Some(metrics) = metrics {
                with_metrics(app, metrics)
            } else {
                app
            };

            let app = with_request_tracing(app);

            let mut make_service = app.into_make_service_with_connect_info::<SocketAddr>();

            let listener = TcpListener::bind(addr).await.unwrap();

            tracing::info!("Scheduler listening for clients on {}", addr);

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
