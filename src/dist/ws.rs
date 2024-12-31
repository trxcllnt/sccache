#[cfg(feature = "dist-client")]
pub use self::client::make_websocket_client;
#[cfg(feature = "dist-server")]
pub use self::server::handle_websocket_connection;

mod common {
    use futures::{FutureExt, StreamExt, TryFutureExt};

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
}

#[cfg(feature = "dist-client")]
mod client {
    use futures::{lock::Mutex, StreamExt};
    use futures::{FutureExt, SinkExt};
    use tokio::net::TcpStream;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::Message;

    use super::common::for_all_concurrent;
    use crate::dist::http::{bincode_deserialize, bincode_serialize};
    use crate::dist::{ClientIncoming, ClientOutgoing};
    use crate::errors::*;

    type WebSocketCallback<Incoming> = tokio::sync::oneshot::Sender<Incoming>;

    pub type WebSocketRequest<Outgoing, Incoming> = (
        String,                              // request id
        Option<Outgoing>,                    // request payload
        Option<WebSocketCallback<Incoming>>, // response callback
    );

    #[derive(Clone)]
    pub struct WebSocketClient<Outgoing, Incoming> {
        client_outgoing: mpsc::UnboundedSender<WebSocketRequest<Outgoing, Incoming>>,
        connect_fn: Arc<
            dyn Fn() -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<WebSocketStream<MaybeTlsStream<TcpStream>>>,
                            > + Send,
                    >,
                > + Send
                + Sync,
        >,
        connection: tokio_util::sync::CancellationToken,
        recv_task: Arc<tokio::task::JoinHandle<()>>,
        requests: Arc<Mutex<HashMap<String, WebSocketCallback<Incoming>>>>,
        runtime: tokio::runtime::Handle,
        send_task: Arc<tokio::task::JoinHandle<()>>,
        server_incoming: Arc<Mutex<mpsc::UnboundedSender<Result<Message>>>>,
        server_outgoing: Arc<Mutex<mpsc::UnboundedReceiver<Message>>>,
        shutdown_fn: Arc<dyn Fn() -> Incoming + Send + Sync>,
    }

    impl<Outgoing, Incoming> Drop for WebSocketClient<Outgoing, Incoming> {
        fn drop(&mut self) {
            debug!("Dropping WebSocketClient");
            self.connection.cancel();
            self.send_task.abort();
            self.recv_task.abort();
        }
    }

    impl<Outgoing, Incoming> WebSocketClient<Outgoing, Incoming>
    where
        Outgoing: serde::Serialize + Clone + std::fmt::Display + Send + Sync + 'static,
        Incoming:
            for<'a> serde::Deserialize<'a> + Clone + std::fmt::Display + Send + Sync + 'static,
    {
        pub fn new<Connect, Shutdown>(
            runtime: &tokio::runtime::Handle,
            connect_fn: Connect,
            shutdown_fn: Shutdown,
        ) -> Self
        where
            Connect: Fn() -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<WebSocketStream<MaybeTlsStream<TcpStream>>>,
                            > + Send,
                    >,
                > + Send
                + Sync
                + 'static,
            Shutdown: Fn() -> Incoming + Send + Sync + 'static,
        {
            let requests = Arc::new(Mutex::new(HashMap::new()));
            let connection = tokio_util::sync::CancellationToken::new();

            let (client_outgoing, client_outgoing_recv) = tokio::sync::mpsc::unbounded_channel();
            let (server_incoming, server_incoming_recv) = tokio::sync::mpsc::unbounded_channel();
            let (server_outgoing_sink, server_outgoing) = tokio::sync::mpsc::unbounded_channel();

            // Clones to move into the futures that need mutable refs
            let send_sndr = Arc::new(Mutex::new(server_outgoing_sink));
            let sndr_reqs = requests.clone();
            let recv_reqs = requests.clone();

            let send_task = for_all_concurrent(
                &runtime.clone(),
                UnboundedReceiverStream::new(client_outgoing_recv),
                connection.child_token(),
                move |(req_id, req, cb): WebSocketRequest<Outgoing, Incoming>| {
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
                                    return std::ops::ControlFlow::Continue(format!(
                                        "WebSocket failed to serialize request: {err}"
                                    ));
                                }
                            };
                            // Send the message. Abort on error.
                            if sndr.lock().await.send(Message::Binary(buf)).is_err() {
                                return std::ops::ControlFlow::Continue(
                                    "WebSocket send failure".into(),
                                );
                            }
                        }

                        std::ops::ControlFlow::Continue(String::new())
                    }
                },
            );

            let recv_task = for_all_concurrent(
                &runtime.clone(),
                UnboundedReceiverStream::new(server_incoming_recv),
                connection.child_token(),
                move |msg| {
                    // Local clones for this individual task
                    let reqs = recv_reqs.clone();
                    async move {
                        let (req_id, res) = match msg {
                            Err(err) => {
                                return std::ops::ControlFlow::Continue(format!(
                                    "WebSocket recv failure: {err:?}"
                                ))
                            }
                            Ok(msg) => {
                                match msg {
                                    Message::Close(None) => {
                                        return std::ops::ControlFlow::Continue(
                                            "WebSocket close without CloseFrame".into(),
                                        );
                                    }
                                    Message::Close(Some(CloseFrame { code, reason })) => {
                                        return std::ops::ControlFlow::Continue(format!(
                                            "WebSocket disconnected code={}, reason=`{}`",
                                            code, reason
                                        ));
                                    }
                                    Message::Text(str) => {
                                        return std::ops::ControlFlow::Continue(format!(
                                            "WebSocket received unexpected text response: {str}"
                                        ));
                                    }
                                    Message::Ping(_) => {
                                        trace!("WebSocket received ping");
                                        return std::ops::ControlFlow::Continue(String::new());
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
                            if cb.is_closed() || cb.send(res).is_err() {
                                return std::ops::ControlFlow::Continue(format!("WebSocket failed to notify client of response with id={req_id}"));
                            }
                        }

                        std::ops::ControlFlow::Continue(String::new())
                    }
                },
            );

            Self {
                client_outgoing,
                connect_fn: Arc::new(connect_fn),
                connection,
                recv_task: Arc::new(recv_task),
                requests: Arc::new(Mutex::new(HashMap::new())),
                runtime: runtime.clone(),
                send_task: Arc::new(send_task),
                server_incoming: Arc::new(Mutex::new(server_incoming)),
                server_outgoing: Arc::new(Mutex::new(server_outgoing)),
                shutdown_fn: Arc::new(shutdown_fn),
            }
        }

        pub async fn connect(&self, initial_reconnect_delay: Option<Duration>) -> Result<()> {
            match initial_reconnect_delay {
                None => self.connect_once().await,
                Some(initial_reconnect_delay) => {
                    self.connect_with_retry(initial_reconnect_delay).await
                }
            }
        }

        async fn connect_with_retry(&self, initial_delay: Duration) -> Result<()> {
            let mut aggregate_delay = initial_delay;
            while !self.is_closed() {
                match self.connect_once().await {
                    Ok(_) => {
                        // If successfully connected, reset the exponential backoff counter
                        aggregate_delay = initial_delay;
                    }
                    Err(_) => {
                        debug!(
                            "WebSocket::connect_with_retry error, is_closed={}",
                            self.is_closed()
                        );
                        if !self.is_closed() {
                            debug!(
                                "WebSocket connection failed, retrying in {}s",
                                aggregate_delay.as_secs()
                            );
                            tokio::time::sleep(aggregate_delay).await;
                            // Exponential back-off up to 60s
                            aggregate_delay = (aggregate_delay * 2)
                                .min(Duration::from_secs(60))
                                .max(Duration::from_secs(1));
                        }
                    }
                }
            }
            Ok(())
        }

        async fn connect_once(&self) -> Result<()> {
            let (mut sndr, mut recv) = match self.connect_fn.clone()()
                .await
                .context("WebSocket connect failed")
            {
                Ok(sock) => sock.split(),
                Err(err) => {
                    info!("{err}");
                    return Err(anyhow!(err));
                }
            };

            let mut send_task = self.runtime.spawn({
                let server_outgoing = self.server_outgoing.clone();
                async move {
                    let mut server_outgoing = server_outgoing.lock().await;
                    while let Some(msg) = server_outgoing.recv().await {
                        if sndr.send(msg).await.is_err() {
                            break;
                        }
                    }
                }
            });

            let mut recv_task = self.runtime.spawn({
                let server_incoming = self.server_incoming.clone();
                async move {
                    let server_incoming = server_incoming.lock().await;
                    while let Some(msg) = recv.next().await {
                        if server_incoming
                            .send(msg.map_err(anyhow::Error::new))
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            });

            // Wait for either cancel/send/recv to finish
            tokio::select! {
                _ = self.connection.cancelled() => {
                    debug!("[WebSocket::connect_once()]: connection canceled");
                    send_task.abort();
                    recv_task.abort();
                }
                _ = (&mut send_task) => {
                    debug!("[WebSocket::connect_once()]: send_task completed");
                    recv_task.abort();
                },
                _ = (&mut recv_task) => {
                    debug!("[WebSocket::connect_once()]: recv_task completed");
                    send_task.abort();
                }
            }

            // Notify all outstanding request handlers that the WebSocket client has shutdown.
            let res = self.shutdown_fn.clone()();
            for (req_id, cb) in self.requests.lock().await.drain() {
                if cb.is_closed() || cb.send(res.clone()).is_err() {
                    warn!("WebSocket failed to notify client of shutdown (req_id={req_id})")
                }
            }

            Ok(())
        }

        pub fn is_closed(&self) -> bool {
            self.connection.is_cancelled()
        }

        // pub fn close(&mut self) {
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
            self.client_outgoing
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

    pub fn make_websocket_client(
        runtime: &tokio::runtime::Handle,
        scheduler_url: reqwest::Url,
        auth_token: String,
    ) -> Result<WebSocketClient<ClientOutgoing, ClientIncoming>> {
        let mut connect_req = make_scheduler_ws_uri(&scheduler_url)?.into_client_request()?;

        connect_req.headers_mut().insert(
            http::header::AUTHORIZATION,
            http::header::HeaderValue::from_str(&format!("Bearer {}", auth_token))?,
        );

        let client = WebSocketClient::new(
            runtime,
            move || {
                let connect_req = connect_req.clone();
                let scheduler_url = scheduler_url.clone();
                async move {
                    info!("Attempting to connect to dist server: {scheduler_url}");
                    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
                        max_message_size: None,
                        max_frame_size: None,
                        ..Default::default()
                    };
                    let (sock, response) = match tokio_tungstenite::connect_async_with_config(
                        connect_req,
                        Some(config),
                        true,
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
                    Ok(sock)
                }
                .boxed()
            },
            || {
                info!("WebSocketClient shutdown");
                ClientIncoming::Error {
                    message: "WebSocketClient closed".into(),
                }
            },
        );

        runtime.spawn({
            let client = client.clone();
            async move { client.connect(Some(Duration::from_secs(0))).await }
        });

        Ok(client)
    }
}

#[cfg(feature = "dist-server")]
mod server {

    use axum::extract::ws::{CloseFrame, Message, WebSocket};

    use futures::{lock::Mutex, SinkExt, StreamExt};

    use tokio_tungstenite::tungstenite::error::ProtocolError;

    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use crate::dist::{
        http::{bincode_deserialize, bincode_serialize},
        ClientIncoming, ClientOutgoing, SchedulerService, ServerIncoming,
    };

    use super::common::for_all_concurrent;

    pub async fn handle_websocket_connection(
        socket: WebSocket,
        client_addr: SocketAddr,
        service: Arc<dyn SchedulerService>,
    ) {
        tracing::debug!("[handle_socket({client_addr})]: client websocket upgrade successful");

        let (sndr, recv) = socket.split();

        // Wrap shared sender in a Mutex so the concurrent
        // `flat_map_unordered` task writes are serialized
        let sndr = Arc::new(Mutex::new(sndr));
        let ping_sndr = sndr.clone();
        let recv_sndr = sndr.clone();

        let pool = tokio::runtime::Handle::current();
        let token = tokio_util::sync::CancellationToken::new();

        let ping_timer = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(
            // Arbitrary, but this is the socket.io default
            Duration::from_secs(25),
        ));

        // TODO:
        // Create a span context and ensure its used for each message
        // (https://docs.rs/tracing/0.1.41/tracing/span/index.html)

        let mut ping_task = for_all_concurrent(&pool, ping_timer, token.child_token(), move |_| {
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
                    return std::ops::ControlFlow::Break("WebSocket send failure".into());
                }
                std::ops::ControlFlow::Continue(String::new())
            }
        });

        let mut recv_task = for_all_concurrent(&pool, recv, token.child_token(), move |msg| {
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
                            match bincode_deserialize::<(String, ServerIncoming)>(buf).await {
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
}
