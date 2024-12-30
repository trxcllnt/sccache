#[cfg(feature = "dist-server")]
pub use self::internal::Scheduler;

#[cfg(feature = "dist-server")]
mod internal {

    use async_trait::async_trait;

    use axum::{
        body::Bytes,
        extract::{
            ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
            ConnectInfo, DefaultBodyLimit, Extension, FromRequest, FromRequestParts, Path, Request,
        },
        http::{request::Parts, HeaderMap, Method, StatusCode, Uri},
        response::{IntoResponse, Response},
        routing, RequestPartsExt, Router,
    };

    use axum_extra::{
        headers::{authorization::Bearer, Authorization},
        TypedHeader,
    };

    use futures::{lock::Mutex, pin_mut, SinkExt, StreamExt, TryStreamExt};

    use hyper_util::rt::{TokioExecutor, TokioIo};

    use serde_json::json;
    use tokio_tungstenite::tungstenite::error::ProtocolError;

    use std::{io, net::SocketAddr, sync::Arc, time::Duration};

    use tokio::net::TcpListener;
    use tokio_util::{compat::TokioAsyncReadCompatExt, io::StreamReader};
    use tower::{Service, ServiceBuilder, ServiceExt};
    use tower_http::{
        request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
        sensitive_headers::{SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer},
        trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
    };

    use crate::dist::{
        http::{bincode_deserialize, bincode_serialize, for_all_concurrent, ClientAuthCheck},
        ClientIncoming, ClientOutgoing, NewJobRequest, RunJobRequest, SchedulerService,
        ServerIncoming, Toolchain,
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
                        Err(err) => Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Failed to serialize response body: {err}").into_bytes(),
                        )
                            .into_response()),
                    },
                    // application/json
                    Some(1) => Ok((
                        StatusCode::OK,
                        json!(content).as_str().unwrap().to_string().into_bytes(),
                    )
                        .into_response()),
                    _ => Err((
                        StatusCode::BAD_REQUEST,
                        "Request must accept application/json or application/octet-stream"
                            .to_string()
                            .into_bytes(),
                    )
                        .into_response()),
                }
            } else {
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
            let msg = format!("sccache: `{method} {uri}` failed with {err}");
            tracing::error!("{}", msg);
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
        app.layer(
            ServiceBuilder::new()
                .layer(SetSensitiveRequestHeadersLayer::from_shared(Arc::clone(
                    &headers_to_redact,
                )))
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
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

    // Verify authenticated sccache clients
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

        fn with_websocket_routes(app: axum::Router) -> axum::Router {
            async fn handle_socket(
                state: Arc<SchedulerState>,
                client_addr: SocketAddr,
                socket: WebSocket,
            ) {
                tracing::debug!(
                    "[handle_socket({client_addr})]: client websocket upgrade successful"
                );

                let (sndr, recv) = socket.split();

                // Wrap shared sender in a Mutex so the concurrent
                // `flat_map_unordered` task writes are serialized
                let sndr = Arc::new(Mutex::new(sndr));
                let ping_sndr = sndr.clone();
                let recv_sndr = sndr.clone();

                let service = state.service.clone();
                let pool = tokio::runtime::Handle::current();
                let token = tokio_util::sync::CancellationToken::new();

                let ping_timer =
                    tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(
                        // Arbitrary, but this is the socket.io default
                        Duration::from_secs(25),
                    ));

                // TODO:
                // Create a span context and ensure its used for each message
                // (https://docs.rs/tracing/0.1.41/tracing/span/index.html)

                let mut ping_task =
                    for_all_concurrent(&pool, ping_timer, token.child_token(), move |_| {
                        // Local clones for this individual task
                        let sndr = ping_sndr.clone();

                        async move {
                            trace!("WebSocket sending ping");
                            // Send the ping. Abort on error.
                            if sndr
                                .lock()
                                .await
                                .send(Message::Ping(uuid::Uuid::new_v4().as_bytes().into()))
                                .await
                                .is_err()
                            {
                                return std::ops::ControlFlow::Break(
                                    "WebSocket send failure".into(),
                                );
                            }
                            std::ops::ControlFlow::Continue(String::new())
                        }
                    });

                let mut recv_task =
                    for_all_concurrent(&pool, recv, token.child_token(), move |msg| {
                        // Local clones for this task
                        let sndr = recv_sndr.clone();
                        let service = service.clone();

                        async move {
                            let (req_id, req) = match msg {
                                Err(err) => {
                                    let err = err.into_inner();
                                    return match err.downcast_ref::<ProtocolError>() {
                                        // TODO: Downcasting the client disconnect error should run
                                        // this case, but it's always running the `None` case below
                                        Some(ProtocolError::ResetWithoutClosingHandshake) => {
                                            std::ops::ControlFlow::Break(String::new())
                                        }
                                        Some(err) => std::ops::ControlFlow::Break(format!(
                                            "WebSocket recv error: {err:?}"
                                        )),
                                        None => std::ops::ControlFlow::Break(format!(
                                            "WebSocket recv error: {err:?}"
                                        )),
                                    };
                                }
                                Ok(msg) => match msg {
                                    Message::Close(None) => {
                                        return std::ops::ControlFlow::Break(
                                            "WebSocket close without CloseFrame".into(),
                                        );
                                    }
                                    Message::Close(Some(CloseFrame { code, reason })) => {
                                        return std::ops::ControlFlow::Break(format!(
                                            "WebSocket disconnected code={code}, reason=`{reason}`"
                                        ));
                                    }
                                    Message::Pong(_) => {
                                        tracing::trace!("WebSocket received pong");
                                        return std::ops::ControlFlow::Continue(String::new());
                                    }
                                    Message::Text(str) => {
                                        return std::ops::ControlFlow::Continue(format!(
                                            "WebSocket received unexpected text response: {str}"
                                        ));
                                    }
                                    Message::Binary(buf) => {
                                        match bincode_deserialize::<(String, ServerIncoming)>(buf)
                                            .await
                                        {
                                            Ok(res) => res,
                                            Err(err) => {
                                                return std::ops::ControlFlow::Continue(format!(
                                                "WebSocket failed to deserialize response: {err}"
                                            ));
                                            }
                                        }
                                    }
                                    _ => return std::ops::ControlFlow::Continue(String::new()),
                                },
                            };

                            tracing::debug!("WebSocket received request: id={req_id} req={req}");

                            let res = match req {
                                ClientOutgoing::NewJob(req) => match service.new_job(req).await {
                                    Ok(res) => ClientIncoming::NewJob(res),
                                    Err(err) => ClientIncoming::Error {
                                        message: err.to_string(),
                                    },
                                },
                                ClientOutgoing::RunJob(req) => match service.run_job(req).await {
                                    Ok(res) => ClientIncoming::RunJob(res),
                                    Err(err) => ClientIncoming::Error {
                                        message: err.to_string(),
                                    },
                                },
                            };

                            tracing::debug!("WebSocket sending response: id={req_id} res={res}");

                            // Serialize the request
                            let buf = match bincode_serialize((req_id.clone(), res)).await {
                                Ok(buf) => buf,
                                Err(err) => {
                                    return std::ops::ControlFlow::Continue(format!(
                                        "WebSocket failed to serialize request: {err}"
                                    ));
                                }
                            };

                            if sndr.lock().await.send(Message::Binary(buf)).await.is_err() {
                                return std::ops::ControlFlow::Break(format!(
                                    "WebSocket failed to notify client of response with id={req_id}"
                                ));
                            }

                            std::ops::ControlFlow::Continue(String::new())
                        }
                    });

                // Wait for either cancel/ping/recv to finish
                tokio::select! {
                    _ = token.cancelled() => {
                        ping_task.abort();
                        recv_task.abort();
                    }
                    _ = (&mut ping_task) => {
                        token.cancel();
                        recv_task.abort();
                    }
                    _ = (&mut recv_task) => {
                        token.cancel();
                        ping_task.abort();
                    }
                }

                // TODO: Figure out how to cancel in-progress tasks for this client

                tracing::info!("sccache: {client_addr} shutdown");
            }

            app.route(
                "/api/v2/client/ws",
                routing::get(
                    |// Authenticate the client bearer token first
                     AuthenticatedClient(client): AuthenticatedClient,
                     ws: WebSocketUpgrade,
                     Extension(state): Extension<Arc<SchedulerState>>| async move {
                        tracing::debug!(
                            "/api/v2/client/ws incoming websocket connection from {client}"
                        );
                        ws.on_upgrade(move |socket| handle_socket(state, client, socket))
                    },
                ),
            )
        }

        pub async fn serve(
            self,
            addr: SocketAddr,
            enable_web_socket_server: bool,
            max_body_size: usize,
        ) -> Result<()> {
            let state = self.state.clone();

            let mut app = Self::make_router();

            if enable_web_socket_server {
                app = Self::with_websocket_routes(app);
            }

            app = with_request_tracing(
                app.fallback(|| async move { (StatusCode::NOT_FOUND, "404") })
                    .layer(DefaultBodyLimit::max(max_body_size))
                    .layer(Extension(Arc::clone(&state))),
            );

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
                            .serve_connection_with_upgrades(tok_stream, hyper_service)
                            .await
                    {
                        tracing::debug!("sccache: failed to serve connection: {err:#}");
                    }
                });
            }
        }
    }
}
