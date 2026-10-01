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

use crate::{
    cache::{Storage, disk::DiskCache},
    dist::{
        self, BuildError, BuildResult, BuilderIncoming, CompileCommand, RunJobError,
        RunJobResponse, RunJobStatus, ServerService, ServerToolchains, StatusUpdate, Toolchain,
        ToolchainService,
        io::{Jobs, Toolchains},
        metrics::{JobsMetrics, Metrics, ServerMetrics, ToolchainsMetrics},
    },
    errors::*,
    util::{self, AsyncMulticast, AsyncMulticastArgs, AsyncMulticastFunc},
};
use async_trait::async_trait;
use futures::{future::FutureExt, lock::Mutex};
use itertools::Itertools;
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Default)]
pub struct ServerBuilder {
    builder: Option<Arc<dyn BuilderIncoming>>,
    job_queue: Option<Arc<tokio::sync::Semaphore>>,
    jobs_storage: Option<Arc<dyn Storage>>,
    metrics: Option<Metrics>,
    num_cpus: Option<usize>,
    occupancy: Option<usize>,
    pre_fetch: Option<usize>,
    server_id: Option<String>,
    should_inflate_toolchains: Option<bool>,
    tasks: Option<Arc<dyn dist::tasks::ServerTasks>>,
    toolchains_cache: Option<Arc<DiskCache>>,
    toolchains_storage: Option<Arc<dyn Storage>>,
}

impl ServerBuilder {
    pub fn with_builder(self, builder: Arc<dyn BuilderIncoming>) -> Self {
        Self {
            builder: Some(builder),
            ..self
        }
    }
    pub fn with_job_queue(self, job_queue: Arc<tokio::sync::Semaphore>) -> Self {
        Self {
            job_queue: Some(job_queue),
            ..self
        }
    }
    pub fn with_jobs_storage(self, jobs_storage: Arc<dyn Storage>) -> Self {
        Self {
            jobs_storage: Some(jobs_storage),
            ..self
        }
    }
    pub fn with_metrics(self, metrics: Metrics) -> Self {
        Self {
            metrics: Some(metrics),
            ..self
        }
    }
    pub fn with_num_cpus(self, num_cpus: usize) -> Self {
        Self {
            num_cpus: Some(num_cpus),
            ..self
        }
    }
    pub fn with_occupancy(self, occupancy: usize) -> Self {
        Self {
            occupancy: Some(occupancy),
            ..self
        }
    }
    pub fn with_pre_fetch(self, pre_fetch: usize) -> Self {
        Self {
            pre_fetch: Some(pre_fetch),
            ..self
        }
    }
    pub fn with_server_id(self, server_id: String) -> Self {
        Self {
            server_id: Some(server_id),
            ..self
        }
    }
    pub fn with_should_inflate_toolchains(self, should_inflate_toolchains: bool) -> Self {
        Self {
            should_inflate_toolchains: Some(should_inflate_toolchains),
            ..self
        }
    }
    pub fn with_tasks(self, tasks: Arc<dyn dist::tasks::ServerTasks>) -> Self {
        Self {
            tasks: Some(tasks),
            ..self
        }
    }
    pub fn with_toolchains_cache(self, toolchains_cache: Arc<DiskCache>) -> Self {
        Self {
            toolchains_cache: Some(toolchains_cache),
            ..self
        }
    }
    pub fn with_toolchains_storage(self, toolchains_storage: Arc<dyn Storage>) -> Self {
        Self {
            toolchains_storage: Some(toolchains_storage),
            ..self
        }
    }

    pub fn build(self) -> Result<Arc<Server>> {
        let ServerBuilder {
            builder,
            job_queue,
            jobs_storage,
            metrics,
            num_cpus,
            occupancy,
            pre_fetch,
            server_id,
            should_inflate_toolchains,
            tasks,
            toolchains_cache,
            toolchains_storage,
        } = self;

        let server_id = server_id
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `server_id`"))?;
        let job_queue = job_queue
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `job_queue`"))?;
        let builder = builder
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `builder`"))?;
        let jobs_storage = jobs_storage
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `jobs_storage`"))?;
        let metrics = metrics
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `metrics`"))?;
        let tasks = tasks
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `tasks`"))?;
        let toolchains_storage = toolchains_storage
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `toolchains_storage`"))?;
        let toolchains_cache = toolchains_cache
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `toolchains_cache`"))?;
        let should_inflate_toolchains = should_inflate_toolchains
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `should_inflate_toolchains`"))?;
        let num_cpus = num_cpus
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `num_cpus`"))?;
        let occupancy = occupancy
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `occupancy`"))?;
        let pre_fetch = pre_fetch
            .map(Ok)
            .unwrap_or_else(|| bail!("Missing required field `pre_fetch`"))?;

        let (lifetime, _) = tokio::sync::watch::channel(true);

        let lifetime = Arc::new(lifetime);

        let jobs_io = Jobs::new(
            jobs_storage,
            JobsMetrics::new("sccache::server", metrics.clone()),
        );

        let toolchains_metrics = ToolchainsMetrics::new("sccache::server", metrics.clone());
        let toolchains = Toolchains::new(toolchains_storage, toolchains_metrics.clone());

        let load_toolchain = AsyncMulticast::new(LoadToolchainFn {
            toolchains: Arc::new(ServerToolchains::new(
                toolchains_cache,
                toolchains,
                toolchains_metrics,
                should_inflate_toolchains,
            )),
        });

        let metrics = ServerMetrics::new("sccache::server", metrics, num_cpus);

        let server = Arc::new(Server {
            builder,
            job_queue,
            jobs_io,
            jobs: Default::default(),
            lifetime,
            metrics,
            occupancy,
            pre_fetch,
            schedulers: Default::default(),
            server_id,
            tasks,
            load_toolchain,
        });

        server.tasks.set_service(server.clone())?;

        Ok(server)
    }
}

#[derive(Clone)]
pub struct Server {
    builder: Arc<dyn BuilderIncoming>,
    job_queue: Arc<tokio::sync::Semaphore>,
    jobs_io: Jobs,
    jobs: Arc<Mutex<HashMap<String, Instant>>>,
    lifetime: Arc<tokio::sync::watch::Sender<bool>>,
    metrics: ServerMetrics,
    occupancy: usize,
    pre_fetch: usize,
    schedulers: Arc<Mutex<HashMap<String, Instant>>>,
    server_id: String,
    tasks: Arc<dyn dist::tasks::ServerTasks>,
    load_toolchain: AsyncMulticast<Toolchain, PathBuf>,
}

impl Server {
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    pub async fn start(
        &self,
        heartbeat_interval: Duration,
        shutdown_timeout: Duration,
        health_check_bind_addr: Option<SocketAddr>,
    ) -> Result<()> {
        self.tasks.app().display_pretty().await;

        tracing::info!(
            "Server `{}` initialized to run {} parallel build job(s) and prefetch up to {} job(s) in the background",
            self.server_id,
            self.occupancy,
            self.pre_fetch,
        );

        // Start celery
        let celery = async {
            let res = self
                .tasks
                .app()
                .consume_from(&self.tasks.queues_to_consume())
                .await;
            tracing::info!("Celery shutdown");
            res.context("Celery error")
        };

        // Install our own handlers so pending jobs can bail early and record
        // server_terminated responses. This gives the client a chance to retry
        // the job or build locally.
        let sigint = async {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigint = signal(SignalKind::interrupt())?;
            let mut sigterm = signal(SignalKind::terminate())?;
            tokio::select! {
                biased;
                _ = sigint.recv() => {
                    tracing::info!("Received SIGINT");
                },
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM");
                },
            }
            Ok(())
        };

        // Install a health check listener for load balancers
        let health = if let Some(health_check_bind_addr) = health_check_bind_addr {
            self.health_check_listener(health_check_bind_addr).boxed()
        } else {
            futures::future::pending::<()>().boxed()
        };

        let status = util::spawn({
            let this = self.clone();
            async move {
                loop {
                    let _ = futures::future::join(
                        this.poll_for_cancelled_jobs(),     //
                        this.broadcast_server_status(None), //
                    )
                    .await;

                    tokio::time::sleep(heartbeat_interval).await;
                }
            }
        });

        // tokio::select! deterministically polls its futures in order, so
        // register celery first and sigint second so this sigint handler
        // doesn't override celery's internal sigint handler.
        //
        // This ensures celery progresses to "warm shutdown" mode, cancelling
        // the queue consumers, and doesn't attempt to pull more messages from
        // the queue while it is shutting down.
        let shutdown_celery = tokio::select! {
            biased;
            res = celery => res,
            res = sigint => res,
            // These should never resolve before either celery or sigint unless
            // a catastrophic error occurs (tokio dies somehow?). Just polling
            // it here so the tasks are cancelled once either of the above two
            // futures complete first.
            _ = health => Ok(()),
            _ = status => Ok(()),
        };

        // Broadcast the server is shutting down.
        self.lifetime.send_replace(false);

        tracing::info!(
            "Waiting {}s for graceful shutdown",
            shutdown_timeout.as_secs()
        );

        // Kill build processes, remove containers, overlayfs dirs, etc.
        self.builder.shutdown().await;

        let shutdown_start = Instant::now();

        // Wait until all jobs are cancelled. When the build processes are
        // cancelled, they should error with `BuildError::Cancelled`, which is
        // translated into a `RunJobResponse::Retryable("Server terminated")`
        // response written in `job_finished`
        let shutdown_jobs = loop {
            if Instant::now().duration_since(shutdown_start) <= shutdown_timeout {
                let jobs = {
                    let jobs = self.jobs.lock().await;
                    if jobs.is_empty() {
                        break Ok(());
                    }
                    jobs.keys().cloned().collect::<Vec<_>>()
                };

                let jobs_info = if jobs.len() == 1 {
                    format!("{} pending job", jobs.len())
                } else {
                    format!("{} pending jobs", jobs.len())
                };

                tracing::info!("Cancelling {jobs_info}");

                let res = RunJobResponse::server_terminated(&self.server_id);
                // Kill the jobs
                let _ = futures::future::join_all(
                    jobs.iter()
                        .map(|job_id| self.notify_run_job_res(job_id, &res)),
                )
                .await;

                // Kill build processes, remove containers, overlayfs dirs, etc.
                self.builder.shutdown().await;

                tracing::info!("Cancelled {jobs_info}");

                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                tracing::warn!(
                    "Waited {}s for graceful shutdown, proceeding...",
                    shutdown_timeout.as_secs()
                );

                break Err(anyhow!("Shutdown deadline elapsed"));
            }
        };

        // Close broker connection.
        let shutdown_broker = self.tasks.app().close().await.context("Broker error");
        tracing::info!("Closed broker connection");

        shutdown_celery
            .and(shutdown_jobs)
            .and(shutdown_broker)
            .inspect(|_| tracing::info!("Server shutdown gracefully"))
            .inspect_err(|err| tracing::warn!("Server did not shutdown gracefully: {err:?}"))
    }

    async fn status_update(&self) -> StatusUpdate {
        let (num_cpus, cpu_usage, mem_avail, mem_total) = self.metrics.system_metrics();

        let running = self
            .occupancy
            .saturating_sub(self.job_queue.available_permits()) as u64;

        let loading = self.metrics.jobs_loading.value();
        let pending = self.metrics.jobs_pending.value().saturating_sub(running);
        let accepted = self
            .metrics
            .jobs_accepted
            .load(std::sync::atomic::Ordering::SeqCst);
        let finished = self
            .metrics
            .jobs_finished
            .load(std::sync::atomic::Ordering::SeqCst);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_micros();

        let max_job_age = {
            let jobs = self.jobs.lock().await;
            let now = Instant::now();
            jobs.values()
                .map(|created_at| now.duration_since(*created_at).as_micros())
                .max()
                .unwrap_or(0)
        };

        StatusUpdate {
            id: self.server_id.clone(),
            queue: self.tasks.app().default_queue.clone(),
            info: dist::SysStats {
                cpu_usage,
                mem_avail,
                mem_total,
                num_cpus,
                occupancy: self.occupancy,
                pre_fetch: self.pre_fetch,
            },
            jobs: dist::JobStats {
                loading,
                pending,
                running,
                accepted,
                finished,
            },
            // Work around serde missing u128 type...
            // u128 works fine if I use rusty-celery's macros,
            // but not if I implement the Task trait directly.
            // The only difference I see is they use `celery::export::Deserialize`
            // instead of `serde::Deserialize`, but that does not solve the issue.
            timestamp: (
                // microseconds as seconds
                timestamp.saturating_div(1_000_000) as u64,
                // remainder microseconds as nanoseconds
                (timestamp % 1_000_000).saturating_mul(1_000) as u32,
            ),
            max_job_age: (
                // microseconds as seconds
                max_job_age.saturating_div(1_000_000) as u64,
                // remainder microseconds as nanoseconds
                (max_job_age % 1_000_000).saturating_mul(1_000) as u32,
            ),
        }
    }

    async fn poll_for_cancelled_jobs(&self) -> Result<()> {
        let cancelled_jobs =
            futures::future::join_all(self.jobs.lock().await.keys().map(|job_id| async move {
                self.jobs_io
                    .get_job_status(job_id)
                    .await
                    .unwrap_or(None)
                    .filter(|status| matches!(status, RunJobStatus::Cancelled))
                    .map(|_| job_id.clone())
            }))
            .await;

        let res = RunJobResponse::job_cancelled(&self.server_id);
        let _ = futures::future::join_all(
            cancelled_jobs
                .iter()
                .filter_map(|maybe_job_id| maybe_job_id.as_deref())
                // Kill the job
                .map(|job_id| self.notify_run_job_res(job_id, &res)),
        )
        .await;

        Ok(())
    }

    async fn broadcast_server_status(&self, schedulers: Option<Vec<String>>) {
        let schedulers = if let Some(schedulers) = schedulers {
            schedulers
        } else {
            // Prune schedulers we haven't seen in 90s
            let now = Instant::now();
            let timeout = Duration::from_secs(90);
            let mut schedulers = self.schedulers.lock().await;
            schedulers.retain(|_, last_seen| now.duration_since(*last_seen) <= timeout);
            schedulers.keys().cloned().collect::<Vec<_>>()
        };

        let status = self.status_update().await;

        // Notify interested schedulers
        let _ = futures::future::join_all(
            std::iter::once(self.tasks.update_status(status.clone(), None)).chain(
                schedulers
                    .iter()
                    .map(|send_to| self.tasks.update_status(status.clone(), Some(send_to))),
            ),
        )
        .await;
    }

    async fn health_check_listener(&self, health_check_bind_addr: SocketAddr) -> () {
        use futures::stream::StreamExt;
        use hyper::{
            Response, StatusCode, server::conn::http1::Builder as HyperHttpBuilder,
            service::service_fn,
        };
        use hyper_util::rt::TokioIo;
        use tokio::sync::mpsc::{error::SendError, unbounded_channel};

        // Track all outstanding TCP response futures
        let (futures_tx, futures_rx) = unbounded_channel();

        let outer = util::spawn({
            let ok_service = service_fn(move |_| async move {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(http_body_util::Full::<bytes::Bytes>::default())
            });

            async move {
                let listener = match tokio::net::TcpListener::bind(health_check_bind_addr).await {
                    Ok(listener) => listener,
                    Err(err) => {
                        tracing::warn!("Failed to create TCP listener: {err}");
                        return;
                    }
                };

                loop {
                    let stream = match listener.accept().await {
                        Ok((stream, _)) => TokioIo::new(stream),
                        Err(err) => {
                            tracing::warn!("Error accepting connection, ignoring request: {err}");
                            continue;
                        }
                    };

                    let inner = util::spawn(async move {
                        if let Err(err) = HyperHttpBuilder::new()
                            .serve_connection(stream, ok_service)
                            .await
                        {
                            tracing::warn!("Error serving healthcheck connection: {err}");
                        }
                    });

                    if let Err(SendError(handle)) = futures_tx.send(inner) {
                        // If we can't push the handle to the receiver,
                        // just detach and allow task to run untracked.
                        handle.detach();
                    }
                }
            }
        });

        let inners = tokio_stream::wrappers::UnboundedReceiverStream::new(futures_rx)
            .for_each_concurrent(None, |fut| async move {
                if let Err(err) = fut.await {
                    tracing::warn!("Healthcheck response future error: {err}");
                }
            });

        let _ = tokio::join!(outer, inners);
    }

    async fn load_job_and_run_build(
        &self,
        job_id: &str,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> std::result::Result<RunJobResponse, RunJobError> {
        // Record total run_job time
        let _timer = self.metrics.run_job_timer();

        // Increment the job_started counter
        self.metrics.inc_job_accepted_count();

        // Load the job toolchain and inputs
        let (toolchain_dir, inputs) = self.load_job(job_id, toolchain).await?;

        // Increment the job_loaded counter
        self.metrics.inc_job_loaded_count();

        // Increment the jobs_pending gauge
        let _pending = self.metrics.jobs_pending.increment();

        // Run the build
        self.run_build(job_id, toolchain_dir, inputs, command, outputs)
            .await
            .map(|result| {
                if !result.output.success() && !result.output.exit() {
                    return RunJobResponse::build_process_killed(&self.server_id);
                }
                RunJobResponse::Complete {
                    result,
                    server_id: self.server_id.clone(),
                }
            })
            .map_err(|err| {
                if let BuildError::UnpackInputs(_) = err {
                    RunJobError::MissingJobInputs
                } else {
                    RunJobError::Retryable(err.into())
                }
            })
    }

    async fn load_job(
        &self,
        job_id: &str,
        toolchain: Toolchain,
    ) -> std::result::Result<(PathBuf, opendal::Buffer), RunJobError> {
        // Record load_job time
        let _timer = self.metrics.load_job_timer();
        let _loading = self.metrics.jobs_loading.increment();

        // Load and unpack the toolchain
        let toolchain_dir = self
            .load_toolchain
            .call(toolchain)
            .await
            .map(|(_, res)| res)
            .map_err(|_| RunJobError::MissingToolchain)?;

        // Load job inputs into memory
        let inputs = self
            .jobs_io
            .get_job_inputs(job_id)
            .await
            .map_err(|_| RunJobError::MissingJobInputs)?;

        Ok((toolchain_dir, inputs))
    }

    async fn run_build(
        &self,
        job_id: &str,
        toolchain_dir: PathBuf,
        inputs: opendal::Buffer,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> std::result::Result<BuildResult, BuildError> {
        // Record build time
        let _timer = self.metrics.run_build_timer();
        self.builder
            .run_build(job_id, &toolchain_dir, inputs, command, outputs)
            .await
            .map_err(|err| {
                // Record run_build errors
                self.metrics.inc_job_build_error_count();
                tracing::warn!("[run_job({job_id})]: Build error: {err:?}");
                err
            })
    }

    async fn job_finished(&self, job_id: &str, res: &RunJobResponse) -> Result<()> {
        let server_terminated = if *self.lifetime.borrow() {
            // Store the job result for retrieval by a scheduler
            let _ = self.jobs_io.put_job_result(job_id, res).await;
            matches!(res, RunJobResponse::RetryableError { message, .. } if message == "Server terminated")
        } else {
            // If the build failed because the server was terminated,
            // report it as a server termination, not a failed build.
            let _ = self
                .jobs_io
                .put_job_result(job_id, &RunJobResponse::server_terminated(&self.server_id))
                .await;
            true
        };

        // Delete the status for the now finished job
        let schedulers = self.del_finished_job_status(job_id).await?;

        if server_terminated {
            tracing::info!("Sending server terminated response for job {job_id} to {schedulers:?}");
        }

        let status = self.status_update().await;

        // Notify interested schedulers
        futures::future::try_join_all(schedulers.iter().map(|reply_to| async {
            self.tasks
                .job_finished(job_id, reply_to, status.clone())
                .await
                .map_err(anyhow::Error::new)
                .map(|_| ())
        }))
        .await?;

        Ok(())
    }

    async fn check_if_job_cancelled(&self, job_id: &str, reply_to: &str) -> Result<RunJobStatus> {
        loop {
            let mut status = self
                .jobs_io
                .get_job_status(job_id)
                .await?
                .unwrap_or_else(|| RunJobStatus::Active {
                    schedulers: Vec::with_capacity(1),
                });

            match &mut status {
                RunJobStatus::Active { schedulers } => {
                    if schedulers.iter().map(|s| s.as_str()).contains(reply_to) {
                        break Ok(status);
                    }
                    schedulers.push(reply_to.into());
                    self.jobs_io.put_job_status(job_id, &status).await?;
                }
                RunJobStatus::Cancelled => break Ok(status),
            }
        }
    }

    async fn del_finished_job_status(&self, job_id: &str) -> Result<Vec<String>> {
        let mut result = None;
        loop {
            let status = self.jobs_io.get_job_status(job_id).await?;

            if let Some(status) = status {
                match status {
                    RunJobStatus::Active { schedulers } => result = Some(schedulers),
                    RunJobStatus::Cancelled => result = None,
                }
                self.jobs_io.del_job_status(job_id).await?;
            } else {
                break Ok(result.unwrap_or_default());
            }
        }
    }
}

#[async_trait]
impl ServerService for Server {
    async fn run_job(
        &self,
        job_id: &str,
        reply_to: &str,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> Result<RunJobResponse> {
        // Add job
        self.jobs
            .lock()
            .await
            .insert(job_id.to_owned(), Instant::now());

        // Add or update scheduler
        self.schedulers
            .lock()
            .await
            .entry(reply_to.into())
            .and_modify(|last_seen| {
                *last_seen = Instant::now();
            })
            .or_insert_with(Instant::now);

        let mut lifetime = self.lifetime.subscribe();

        if *lifetime.borrow() {
            let job = async {
                match self.check_if_job_cancelled(job_id, reply_to).await? {
                    RunJobStatus::Cancelled => {
                        // Broadcast status after accepting the job
                        self.broadcast_server_status(None).await;
                        Ok(RunJobResponse::job_cancelled(&self.server_id))
                    }
                    RunJobStatus::Active { schedulers } => {
                        // Broadcast status after accepting the job
                        self.broadcast_server_status(Some(schedulers)).await;
                        // Load and run the job
                        self.load_job_and_run_build(job_id, toolchain, command, outputs)
                            .await
                            .map_err(anyhow::Error::new)
                    }
                }
            };

            if matches!(lifetime.has_changed(), Ok(false)) {
                tokio::select! {
                    biased;
                    _ = lifetime.changed() => {
                        Ok(RunJobResponse::server_terminated(&self.server_id))
                    }
                    res = job => res
                }
            } else {
                Ok(RunJobResponse::server_terminated(&self.server_id))
            }
        } else {
            Ok(RunJobResponse::server_terminated(&self.server_id))
        }
    }

    async fn notify_run_job_err(&self, job_id: &str, err: RunJobError) -> Result<()> {
        self.notify_run_job_res(
            job_id,
            &RunJobResponse::from_run_job_error(&self.server_id, &err),
        )
        .await
    }

    async fn notify_run_job_res(&self, job_id: &str, res: &RunJobResponse) -> Result<()> {
        // Remove the job
        if self.jobs.lock().await.remove(job_id).is_some() {
            if matches!(res, RunJobResponse::Complete { .. }) {
                tracing::debug!("[run_job_success({job_id})]: {res:?}");
            } else {
                tracing::debug!("[run_job_failure({job_id})]: {res:?}");
            }

            // Increment the job_finished counter
            self.metrics.inc_job_finished_count();

            // Clean up the build resources
            self.builder.finish_build(job_id).await;

            // Store the job result and notify the interested schedulers
            self.job_finished(job_id, res).await
        } else {
            Ok(())
        }
    }

    async fn update_scheduler_status(&self, status: StatusUpdate) -> Result<()> {
        self.schedulers
            .lock()
            .await
            .entry(status.queue.clone())
            .and_modify(|last_seen| {
                *last_seen = Instant::now();
            })
            .or_insert_with(Instant::now);
        Ok(())
    }
}

impl AsyncMulticastArgs for Toolchain {
    type Key = String;
    fn hash(&self) -> Self::Key {
        self.archive_id.clone()
    }
}

struct LoadToolchainFn {
    toolchains: Arc<dyn ToolchainService>,
}

#[async_trait]
impl AsyncMulticastFunc<Toolchain, PathBuf> for LoadToolchainFn {
    async fn call(&self, tc: &Toolchain) -> Result<PathBuf> {
        self.toolchains.get_toolchain(tc).await
    }
}
