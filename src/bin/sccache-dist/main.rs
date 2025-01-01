// Ignore lint introduced by celery::task macros
#![allow(non_local_definitions)]

use async_trait::async_trait;

use celery::prelude::*;
use celery::protocol::MessageContentType;
use futures::lock::Mutex;
use futures::FutureExt;

use std::collections::HashMap;
use std::env;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::OnceCell;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg_attr(target_os = "freebsd", path = "build_freebsd.rs")]
mod build;

mod cmdline;
use cmdline::Command;
mod token_check;

use sccache::{
    config::{
        scheduler as scheduler_config, server as server_config, MessageBroker,
        INSECURE_DIST_CLIENT_TOKEN,
    },
    dist::{
        self, BuildResult, BuildServerInfo, BuildServerStatus, BuilderIncoming, CompileCommand,
        NewJobRequest, NewJobResponse, RunJobRequest, RunJobResponse, SchedulerService,
        SchedulerStatusResult, ServerService, ServerStatusResult, ServerToolchains,
        SubmitToolchainResult, Toolchain,
    },
    errors::*,
    util::daemonize,
};

static SERVER: OnceCell<Arc<dyn ServerService>> = OnceCell::const_new();
static SCHEDULER: OnceCell<Arc<dyn SchedulerService>> = OnceCell::const_new();

// Only supported on x86_64/aarch64 Linux machines and on FreeBSD
#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64"),
    target_os = "freebsd"
))]
fn main() {
    init_logging();

    let incr_env_strs = ["CARGO_BUILD_INCREMENTAL", "CARGO_INCREMENTAL"];
    incr_env_strs
        .iter()
        .for_each(|incr_str| match env::var(incr_str) {
            Ok(incr_val) if incr_val == "1" => {
                println!("sccache: cargo incremental compilation is not supported");
                std::process::exit(1);
            }
            _ => (),
        });

    let command = match cmdline::try_parse_from(env::args()) {
        Ok(cmd) => cmd,
        Err(e) => match e.downcast::<clap::error::Error>() {
            Ok(clap_err) => clap_err.exit(),
            Err(some_other_err) => {
                println!("sccache-dist: {some_other_err}");
                for source_err in some_other_err.chain().skip(1) {
                    println!("sccache-dist: caused by: {source_err}");
                }
                std::process::exit(1);
            }
        },
    };

    std::process::exit(match run(command) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("sccache-dist: error: {}", e);

            for e in e.chain().skip(1) {
                eprintln!("sccache-dist: caused by: {}", e);
            }
            2
        }
    });
}

fn message_broker_uri(message_broker: Option<MessageBroker>) -> Result<String> {
    match message_broker {
        Some(MessageBroker::AMQP(uri)) => Ok(uri),
        Some(MessageBroker::Redis(uri)) => Ok(uri),
        None => bail!(
            "Missing required message broker configuration!\n\n{}",
            message_broker_info_text()
        ),
    }
}

fn message_broker_info_text() -> String {
    "\
The sccache-dist scheduler and servers communicate via an external message
broker, either an AMQP v0.9.1 implementation (like RabbitMQ) or Redis.

All major CSPs provide managed AMQP or Redis services, or you can deploy
RabbitMQ or Redis as part of your infrastructure.

For local development, you can install RabbitMQ/Redis services locally or
run their containers.

More details can be found in in the sccache-dist documentation at:
https://github.com/mozilla/sccache/blob/main/docs/Distributed.md#message-brokers"
        .into()
}

fn queue_name_with_env_info(prefix: &str) -> String {
    let arch = std::env::var("SCCACHE_DIST_ARCH").unwrap_or(std::env::consts::ARCH.to_owned());
    let os = std::env::var("SCCACHE_DIST_OS").unwrap_or(std::env::consts::OS.to_owned());
    format!("{prefix}-{os}-{arch}")
}

fn scheduler_to_servers_queue() -> String {
    queue_name_with_env_info("scheduler-to-servers")
}

fn server_to_schedulers_queue() -> String {
    queue_name_with_env_info("server-to-schedulers")
}

fn to_scheduler_queue(id: &str) -> String {
    queue_name_with_env_info(&format!("scheduler-{id}"))
}

fn to_server_queue(id: &str) -> String {
    queue_name_with_env_info(&format!("server-{id}"))
}

async fn celery_app(
    broker_uri: &str,
    to_this_instance: &str,
    prefetch_count: u16,
) -> Result<celery::Celery> {
    let to_servers = scheduler_to_servers_queue();
    let to_schedulers = server_to_schedulers_queue();

    celery::CeleryBuilder::new(to_this_instance, broker_uri)
        .default_queue(to_this_instance)
        .task_content_type(MessageContentType::MsgPack)
        // Register at least one task route for each queue, because that's
        // how celery knows which queues to create in the message broker.
        .task_route(scheduler_to_servers::run_job::NAME, &to_servers)
        .task_route(server_to_schedulers::status::NAME, &to_schedulers)
        .prefetch_count(prefetch_count)
        .heartbeat(Some(10))
        .task_time_limit(10)
        .build()
        .await
        .map_err(|err| {
            let err_message = match err {
                CeleryError::BrokerError(err) => err.to_string(),
                err => err.to_string(),
            };
            anyhow!("{}\n\n{}", err_message, message_broker_info_text())
        })
}

fn run(command: Command) -> Result<()> {
    let num_cpus = std::thread::available_parallelism()?.get();

    match command {
        Command::Scheduler(scheduler_config::Config {
            client_auth,
            job_time_limit,
            max_body_size,
            message_broker,
            public_addr,
            scheduler_id,
            toolchains_fallback,
            toolchains,
        }) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            let toolchain_storage = sccache::cache::cache::storage_from_config(
                &toolchains,
                &toolchains_fallback,
                runtime.handle(),
            )
            .context("Failed to initialize toolchain storage")?;

            runtime.block_on(async move {
                // Verify read/write access to toolchain storage
                match toolchain_storage.check().await {
                    Ok(sccache::cache::CacheMode::ReadWrite) => {}
                    _ => {
                        bail!("Scheduler toolchain storage must be read/write")
                    }
                }

                let broker_uri = message_broker_uri(message_broker)?;
                tracing::trace!("Message broker URI: {broker_uri}");

                let to_schedulers = server_to_schedulers_queue();
                let to_this_scheduler = to_scheduler_queue(&scheduler_id);

                let task_queue = Arc::new(
                    celery_app(&broker_uri, &to_this_scheduler, 100 * num_cpus as u16).await?,
                );

                // Tasks this scheduler receives
                task_queue
                    .register_task::<server_to_schedulers::job_failure>()
                    .await
                    .unwrap();

                task_queue
                    .register_task::<server_to_schedulers::job_success>()
                    .await
                    .unwrap();

                task_queue
                    .register_task::<server_to_schedulers::status>()
                    .await
                    .unwrap();

                let scheduler = Arc::new(Scheduler::new(
                    job_time_limit,
                    to_this_scheduler.clone(),
                    task_queue.clone(),
                    toolchain_storage,
                ));

                SCHEDULER
                    .set(scheduler.clone())
                    .map_err(|e| anyhow!(e.to_string()))?;

                let server = dist::server::Scheduler::new(
                    scheduler.clone(),
                    match client_auth {
                        scheduler_config::ClientAuth::Insecure => Box::new(
                            token_check::EqCheck::new(INSECURE_DIST_CLIENT_TOKEN.to_owned()),
                        ),
                        scheduler_config::ClientAuth::Token { token } => {
                            Box::new(token_check::EqCheck::new(token))
                        }
                        scheduler_config::ClientAuth::JwtValidate {
                            audience,
                            issuer,
                            jwks_url,
                        } => Box::new(
                            token_check::ValidJWTCheck::new(audience, issuer, &jwks_url)
                                .context("Failed to create a checker for valid JWTs")?,
                        ),
                        scheduler_config::ClientAuth::Mozilla { required_groups } => {
                            Box::new(token_check::MozillaCheck::new(required_groups))
                        }
                        scheduler_config::ClientAuth::ProxyToken { url, cache_secs } => {
                            Box::new(token_check::ProxyTokenCheck::new(url, cache_secs))
                        }
                    },
                );

                task_queue.display_pretty().await;

                daemonize()?;

                let queues = [to_this_scheduler.as_str(), to_schedulers.as_str()];

                let celery = task_queue.consume_from(&queues);
                let server = server.serve(public_addr, max_body_size);
                let status = scheduler.request_status();

                futures::select_biased! {
                    res = celery.fuse() => res?,
                    res = server.fuse() => res?,
                    res = status.fuse() => res?,
                };

                task_queue.close().await?;

                Ok::<(), anyhow::Error>(())
            })
        }

        Command::Server(server_config::Config {
            message_broker,
            builder,
            cache_dir,
            max_per_core_load,
            num_cpus_to_ignore,
            server_id,
            toolchain_cache_size,
            toolchains,
            toolchains_fallback,
        }) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            let toolchain_storage = sccache::cache::cache::storage_from_config(
                &toolchains,
                &toolchains_fallback,
                runtime.handle(),
            )
            .context("Failed to initialize toolchain storage")?;

            runtime.block_on(async move {
                // Verify toolchain storage
                toolchain_storage
                    .check()
                    .await
                    .context("Failed to initialize toolchain storage")?;

                let server_toolchains = ServerToolchains::new(
                    &cache_dir.join("tc"),
                    toolchain_cache_size,
                    toolchain_storage,
                );

                let num_cpus = (num_cpus - num_cpus_to_ignore).max(1) as f64;
                let prefetch_count = (num_cpus * max_per_core_load).floor().max(1f64) as u16;

                let broker_uri = message_broker_uri(message_broker)?;
                tracing::trace!("Message broker URI: {broker_uri}");

                let to_servers = scheduler_to_servers_queue();
                let to_this_server = to_server_queue(&server_id);

                let task_queue =
                    Arc::new(celery_app(&broker_uri, &to_this_server, prefetch_count).await?);

                // Tasks this server receives
                task_queue
                    .register_task::<scheduler_to_servers::run_job>()
                    .await?;

                task_queue
                    .register_task::<scheduler_to_servers::report>()
                    .await?;

                let builder: Arc<dyn BuilderIncoming> = match builder {
                    #[cfg(not(target_os = "freebsd"))]
                    sccache::config::server::BuilderType::Docker => Arc::new(
                        build::DockerBuilder::new()
                            .await
                            .context("Docker builder failed to start")?,
                    ),
                    #[cfg(not(target_os = "freebsd"))]
                    sccache::config::server::BuilderType::Overlay {
                        bwrap_path,
                        build_dir,
                    } => Arc::new(
                        build::OverlayBuilder::new(bwrap_path, build_dir)
                            .await
                            .context("Overlay builder failed to start")?,
                    ),
                    #[cfg(target_os = "freebsd")]
                    sccache::config::server::BuilderType::Pot {
                        pot_fs_root,
                        clone_from,
                        pot_cmd,
                        pot_clone_args,
                    } => Arc::new(
                        build::PotBuilder::new(pot_fs_root, clone_from, pot_cmd, pot_clone_args)
                            .await
                            .context("Pot builder failed to start")?,
                    ),
                    _ => bail!(
                        "Builder type `{}` not supported on this platform",
                        format!("{:?}", builder)
                            .split_whitespace()
                            .next()
                            .unwrap_or("")
                    ),
                };

                let server = Arc::new(Server::new(
                    max_per_core_load,
                    num_cpus.floor() as usize,
                    server_id,
                    builder,
                    task_queue.clone(),
                    server_toolchains,
                ));

                SERVER.set(server.clone()).map_err(|err| anyhow!("{err}"))?;

                tracing::info!("sccache: Server initialized to run {num_cpus} parallel build jobs");

                task_queue.display_pretty().await;

                daemonize()?;

                let queues = [to_this_server.as_str(), to_servers.as_str()];

                let celery = task_queue.consume_from(&queues);
                let status = server.broadcast_status();

                futures::select_biased! {
                    res = celery.fuse() => res?,
                    res = status.fuse() => res?,
                };

                task_queue.close().await?;

                Ok(())
            })
        }
    }
}

fn init_logging() {
    if env::var(sccache::LOGGING_ENV).is_ok() {
        match tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_env(sccache::LOGGING_ENV).unwrap_or_else(
                    |_| {
                        // axum logs rejections from built-in extractors with the `axum::rejection`
                        // target, at `TRACE` level. `axum::rejection=trace` enables showing those events
                        format!(
                            "{}=debug,tower_http=debug,axum::rejection=trace",
                            env!("CARGO_CRATE_NAME")
                        )
                        .into()
                    },
                ),
            )
            .with(tracing_subscriber::fmt::layer())
            .try_init()
        {
            Ok(_) => (),
            Err(e) => panic!("Failed to initialize logging: {:?}", e),
        }
    }
}

pub struct Scheduler {
    job_time_limit: u32,
    to_this_scheduler: String,
    jobs: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<RunJobResponse>>>>,
    servers: Arc<Mutex<HashMap<String, BuildServerStatus>>>,
    task_queue: Arc<celery::Celery>,
    toolchains: Arc<dyn sccache::cache::Storage>,
    remember_server_error_timeout: Duration,
}

impl Scheduler {
    pub fn new(
        job_time_limit: u32,
        to_this_scheduler_queue: String,
        task_queue: Arc<celery::Celery>,
        toolchains: Arc<dyn sccache::cache::Storage>,
    ) -> Self {
        Self {
            to_this_scheduler: to_this_scheduler_queue,
            job_time_limit,
            jobs: Arc::new(Mutex::new(HashMap::new())),
            servers: Arc::new(Mutex::new(HashMap::new())),
            task_queue,
            toolchains,
            remember_server_error_timeout: Duration::from_secs(30),
        }
    }

    fn prune_servers(
        servers: &mut HashMap<String, BuildServerStatus>,
        remember_server_error_timeout: &Duration,
    ) {
        let now = Instant::now();

        fn prune_servers(
            now: Instant,
            remember_server_error_timeout: Duration,
            servers: &mut HashMap<String, BuildServerStatus>,
        ) -> Vec<String> {
            let mut to_remove = vec![];
            // Remove servers we haven't seen in 5 minutes
            for (server_id, server) in servers.iter_mut() {
                let last_seen = now - server.last_success;
                let last_seen = if let Some(last_failure) = server.last_failure {
                    if now.duration_since(last_failure) >= remember_server_error_timeout {
                        server.last_failure = None;
                    }
                    last_seen.min(now - last_failure)
                } else {
                    last_seen
                };
                if last_seen > Duration::from_secs(180) {
                    to_remove.push(server_id.clone());
                }
            }
            to_remove
        }

        for server_id in prune_servers(now, *remember_server_error_timeout, servers) {
            servers.remove(&server_id);
        }
    }
}

#[async_trait]
impl SchedulerService for Scheduler {
    async fn get_status(&self) -> Result<SchedulerStatusResult> {
        let mut servers = HashMap::<String, ServerStatusResult>::new();

        for (server_id, server) in self.servers.lock().await.iter() {
            servers.insert(
                server_id.clone(),
                ServerStatusResult {
                    max_per_core_load: server.max_per_core_load,
                    num_cpus: server.num_cpus,
                    num_jobs: server.num_jobs,
                    last_success: server.last_success.elapsed().as_secs(),
                    last_failure: server
                        .last_failure
                        .map(|last_failure| last_failure.elapsed().as_secs())
                        .unwrap_or(0),
                },
            );
        }

        Ok(SchedulerStatusResult {
            num_cpus: servers.values().map(|v| v.num_cpus).sum(),
            num_jobs: servers.values().map(|v| v.num_jobs).sum(),
            num_servers: servers.len(),
            servers,
        })
    }

    async fn has_toolchain(&self, toolchain: Toolchain) -> bool {
        self.toolchains.has(&toolchain.archive_id).await
    }

    async fn put_toolchain(
        &self,
        toolchain: Toolchain,
        toolchain_reader: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<SubmitToolchainResult> {
        // Upload toolchain to toolchains storage (S3, GCS, etc.)
        self.toolchains
            .put_stream(&toolchain.archive_id, toolchain_reader)
            .await
            .context("Failed to put toolchain")
            .map(|_| SubmitToolchainResult::Success)
            .map_err(|err| {
                tracing::error!("[put_toolchain({})]: {err:?}", toolchain.archive_id);
                err
            })
    }

    async fn new_job(&self, request: NewJobRequest) -> Result<NewJobResponse> {
        Ok(NewJobResponse {
            has_toolchain: self.has_toolchain(request.toolchain).await,
            job_id: uuid::Uuid::new_v4().simple().to_string(),
            timeout: self.job_time_limit,
        })
    }

    async fn run_job(
        &self,
        RunJobRequest {
            job_id,
            toolchain,
            command,
            outputs,
            inputs,
        }: RunJobRequest,
    ) -> Result<RunJobResponse> {
        let (tx, rx) = tokio::sync::oneshot::channel::<RunJobResponse>();
        self.jobs.lock().await.insert(job_id.clone(), tx);

        let res = self
            .task_queue
            .send_task(
                scheduler_to_servers::run_job::new(
                    job_id.clone(),
                    self.to_this_scheduler.clone(),
                    toolchain,
                    command,
                    outputs,
                    inputs,
                )
                .with_time_limit(self.job_time_limit),
            )
            .await
            .map_err(anyhow::Error::new);

        if let Err(err) = res {
            self.jobs.lock().await.remove(&job_id);
            Err(err)
        } else {
            rx.await.map_err(anyhow::Error::new)
        }
    }

    async fn job_failure(&self, job_id: &str, reason: &str, info: BuildServerInfo) -> Result<()> {
        let send_res = if let Some(sndr) = self.jobs.lock().await.remove(job_id) {
            sndr.send(RunJobResponse::JobFailed {
                reason: reason.to_owned(),
            })
            .map_err(|_| anyhow!("Failed to send job result"))
        } else {
            Err(anyhow!(
                "[job_failed({job_id})]: Failed to send response for unknown job"
            ))
        };
        let info_res = self.receive_status(info, Some(false)).await;
        send_res.and(info_res)
    }

    async fn job_success(
        &self,
        job_id: &str,
        result: BuildResult,
        info: BuildServerInfo,
    ) -> Result<()> {
        let send_res = if let Some(sndr) = self.jobs.lock().await.remove(job_id) {
            sndr.send(RunJobResponse::JobComplete {
                result,
                server_id: info.server_id.clone(),
            })
            .map_err(|_| anyhow!("Failed to send job result"))
        } else {
            Err(anyhow!(
                "[job_complete({job_id})]: Failed to send response for unknown job"
            ))
        };
        let info_res = self.receive_status(info, Some(true)).await;
        send_res.and(info_res)
    }

    async fn request_status(&self) -> Result<()> {
        tokio::spawn({
            let servers = self.servers.clone();
            let task_queue = self.task_queue.clone();
            let respond_to = self.to_this_scheduler.clone();
            let error_timeout = self.remember_server_error_timeout;
            // Report status at least every 60s (1min)
            let request_interval = Duration::from_secs(60);

            async move {
                loop {
                    let due_time = {
                        let mut servers = servers.lock().await;

                        // Prune servers before requesting status
                        Self::prune_servers(&mut servers, &error_timeout);

                        if servers.len() == 0 {
                            request_interval
                        } else {
                            tracing::trace!("Requesting servers status");

                            let report_reqs = futures::future::try_join_all(servers.iter().map(
                                |(server_id, _)| {
                                    task_queue.send_task(
                                        scheduler_to_servers::report::new(respond_to.clone())
                                            .with_queue(&to_server_queue(server_id)),
                                    )
                                },
                            ));

                            drop(servers);

                            match report_reqs.await {
                                Ok(_) => {
                                    tracing::trace!("Request servers status success");
                                    request_interval
                                }
                                Err(e) => {
                                    tracing::error!("Request servers status failure: {e}");
                                    request_interval / 2
                                }
                            }
                        }
                    };

                    tokio::time::sleep(due_time).await;
                }
            }
        })
        .await
        .map_err(anyhow::Error::new)
    }

    async fn receive_status(&self, info: BuildServerInfo, job_status: Option<bool>) -> Result<()> {
        tracing::trace!("Received server status: {info:?}");

        let mut servers = self.servers.lock().await;

        // Insert or update the server info
        servers
            .entry(info.server_id.clone())
            .and_modify(|server| {
                if let Some(success) = job_status {
                    if success {
                        server.last_success = Instant::now();
                    } else {
                        server.last_failure = Some(Instant::now());
                    }
                }
                server.max_per_core_load = info.max_per_core_load;
                server.num_cpus = info.num_cpus;
                server.num_jobs = info.num_jobs;
            })
            .or_insert_with(|| BuildServerStatus {
                last_success: Instant::now(),
                last_failure: None,
                max_per_core_load: info.max_per_core_load,
                num_cpus: info.num_cpus,
                num_jobs: info.num_jobs,
            });

        Self::prune_servers(&mut servers, &self.remember_server_error_timeout);

        Ok(())
    }
}

#[derive(Clone)]
pub struct Server {
    max_per_core_load: f64,
    num_cpus: usize,
    server_id: String,
    builder: Arc<dyn BuilderIncoming>,
    last_report_time: Arc<std::sync::atomic::AtomicU64>,
    jobs: Arc<Mutex<HashMap<String, (String, String)>>>,
    task_queue: Arc<celery::Celery>,
    toolchains: ServerToolchains,
}

impl Server {
    pub fn new(
        max_per_core_load: f64,
        num_cpus: usize,
        server_id: String,
        builder: Arc<dyn BuilderIncoming>,
        task_queue: Arc<celery::Celery>,
        toolchains: ServerToolchains,
    ) -> Self {
        Self {
            builder,
            jobs: Default::default(),
            last_report_time: Default::default(),
            max_per_core_load,
            num_cpus,
            server_id,
            task_queue,
            toolchains,
        }
    }

    fn now() -> Option<Duration> {
        // Now as the duration since unix epoch
        std::time::UNIX_EPOCH.elapsed().ok()
    }

    fn update_last_report_time(last_report_time: &std::sync::atomic::AtomicU64) {
        let _ = last_report_time.fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |_| Self::now().map(|d| d.as_secs()),
        );
    }

    async fn send_status(&self, respond_to: &str, num_jobs: usize) -> Result<()> {
        let info = BuildServerInfo {
            max_per_core_load: self.max_per_core_load,
            num_cpus: self.num_cpus,
            num_jobs,
            server_id: self.server_id.clone(),
        };

        tracing::trace!("Reporting server status: {info:?}");

        self.task_queue
            .send_task(server_to_schedulers::status::new(info).with_queue(respond_to))
            .await
            .map_err(anyhow::Error::new)
            .map(|_| ())?;

        // Update last_report_time
        Self::update_last_report_time(&self.last_report_time);

        Ok(())
    }
}

#[async_trait]
impl ServerService for Server {
    async fn broadcast_status(&self) -> Result<()> {
        tokio::spawn({
            let task_queue = self.task_queue.clone();
            let last_report_time = self.last_report_time.clone();
            let max_per_core_load = self.max_per_core_load;
            let num_cpus = self.num_cpus;
            let jobs = self.jobs.clone();
            let server_id = self.server_id.clone();
            // Report status at least every 180s (3min)
            let report_interval = Duration::from_secs(180);

            async move {
                loop {
                    let due_time = {
                        let time_since_last_report =
                            // Now as the duration since the unix epoch
                            Self::now().unwrap()
                            // Last update as duration since the unix epoch
                            - Duration::from_secs(
                                last_report_time.load(std::sync::atomic::Ordering::SeqCst),
                            );
                        if time_since_last_report < report_interval {
                            tracing::trace!(
                                "Not sending heartbeat due to {} < {}",
                                time_since_last_report.as_secs(),
                                report_interval.as_secs()
                            );
                            // due time to next report
                            report_interval - time_since_last_report
                        } else {
                            let info = BuildServerInfo {
                                max_per_core_load,
                                num_cpus,
                                num_jobs: jobs.lock().await.len(),
                                server_id: server_id.clone(),
                            };

                            tracing::trace!("Sending heartbeat: {info:?}");

                            match task_queue
                                .send_task(server_to_schedulers::status::new(info))
                                .await
                            {
                                Ok(_) => {
                                    tracing::trace!("Heartbeat success");

                                    // Update last_report_time
                                    Self::update_last_report_time(&last_report_time);

                                    report_interval
                                }
                                Err(e) => {
                                    tracing::error!("Failed to send heartbeat to scheduler: {e}");
                                    Duration::from_secs(30)
                                }
                            }
                        }
                    };

                    tokio::time::sleep(due_time).await;
                }
            }
        })
        .await
        .map_err(anyhow::Error::new)
    }

    async fn report_status(&self, respond_to: &str) -> Result<()> {
        let num_jobs = self.jobs.lock().await.len();
        self.send_status(respond_to, num_jobs).await
    }

    async fn run_job(
        &self,
        task_id: &str,
        job_id: &str,
        respond_to: &str,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs: Vec<u8>,
    ) -> Result<BuildResult> {
        let num_jobs = {
            let mut jobs = self.jobs.lock().await;
            // Associate the task with the scheduler and job so we can report success or failure
            jobs.insert(
                task_id.to_owned(),
                (respond_to.to_owned(), job_id.to_owned()),
            );
            jobs.len()
        };

        // Load the toolchain and report status in parallel
        let (toolchain_dir, _) = futures::future::try_join(
            self.toolchains.acquire(&toolchain),
            self.send_status(respond_to, num_jobs),
        )
        .await?;

        self.builder
            .run_build(job_id, &toolchain_dir, command, outputs, inputs)
            .await
    }

    async fn job_failure(&self, task_id: &str, reason: &str) -> Result<()> {
        let mut jobs = self.jobs.lock().await;
        if let Some((respond_to, job_id)) = jobs.remove(task_id) {
            let info = BuildServerInfo {
                max_per_core_load: self.max_per_core_load,
                num_cpus: self.num_cpus,
                num_jobs: jobs.len(),
                server_id: self.server_id.clone(),
            };

            drop(jobs);

            self.task_queue
                .send_task(
                    server_to_schedulers::job_failure::new(job_id, reason.to_owned(), info)
                        .with_queue(&respond_to),
                )
                .await
                .map_err(anyhow::Error::new)
                .map(|_| ())?;

            // Update last_report_time on success
            Self::update_last_report_time(&self.last_report_time);

            Ok(())
        } else {
            tracing::error!("[job_failure({task_id})]: Failed to report task failure");
            Err(anyhow!("Cannot report task failure ({task_id})"))
        }
    }

    async fn job_success(&self, task_id: &str, result: &BuildResult) -> Result<()> {
        let mut jobs = self.jobs.lock().await;
        if let Some((respond_to, job_id)) = jobs.remove(task_id) {
            let info = BuildServerInfo {
                max_per_core_load: self.max_per_core_load,
                num_cpus: self.num_cpus,
                num_jobs: jobs.len(),
                server_id: self.server_id.clone(),
            };

            drop(jobs);

            self.task_queue
                .send_task(
                    server_to_schedulers::job_success::new(job_id, result.to_owned(), info)
                        .with_queue(&respond_to),
                )
                .await
                .map_err(anyhow::Error::new)
                .map(|_| ())?;

            // Update last_report_time on success
            Self::update_last_report_time(&self.last_report_time);

            Ok(())
        } else {
            tracing::error!("[job_success({task_id})]: Failed to report task success");
            Err(anyhow!("Cannot report task success ({task_id})"))
        }
    }
}

// Sent by schedulers, runs on servers
mod scheduler_to_servers {
    use anyhow::anyhow;
    use celery::prelude::*;

    use futures::FutureExt;
    use sccache::dist::{BuildResult, CompileCommand, Toolchain};

    #[celery::task(
        bind = true,
        acks_late = true,
        acks_on_failure_or_timeout = false,
        nacks_enabled = true,
        on_failure = on_run_job_failure,
        on_success = on_run_job_success,
    )]
    pub async fn run_job(
        task: &Self,
        job_id: String,
        respond_to: String,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs: Vec<u8>,
    ) -> TaskResult<BuildResult> {
        let task_id = task.request.id.clone();

        tracing::debug!(
            "[run_build({task_id}, {job_id}, {respond_to}, {}, {:?}, {:?}, {outputs:?})]",
            toolchain.archive_id,
            command.executable,
            command.arguments,
        );

        if let Some(server) = super::SERVER.get() {
            server
                .run_job(
                    &task_id,
                    &job_id,
                    &respond_to,
                    toolchain,
                    command,
                    outputs,
                    inputs,
                )
                .await
                .map_err(|e| {
                    tracing::error!("[run_build({job_id})]: run_job failed with: {e:?}");
                    TaskError::UnexpectedError(e.to_string())
                })
        } else {
            Err(TaskError::UnexpectedError(
                "sccache-dist server is not initialized".into(),
            ))
        }
    }

    async fn on_run_job_failure(task: &run_job, err: &TaskError) {
        let task_id = task.request().id.clone();
        let reason = match err {
            TaskError::TimeoutError => {
                format!("[run_build({task_id})]: Timed out")
            }
            _ => {
                format!("[run_build({task_id})]: Failed with `{err}`")
            }
        };
        if let Err(err) = super::SERVER
            .get()
            .map(|server| server.job_failure(&task_id, &reason))
            .unwrap_or_else(|| {
                futures::future::err(anyhow!("sccache-dist server is not initialized")).boxed()
            })
            .await
        {
            tracing::error!("[on_run_job_failure({task_id})]: {err}");
        }
    }

    async fn on_run_job_success(task: &run_job, result: &BuildResult) {
        let task_id = task.request().id.clone();
        if let Err(err) = super::SERVER
            .get()
            .map(|server| server.job_success(&task_id, result))
            .unwrap_or_else(|| {
                futures::future::err(anyhow!("sccache-dist server is not initialized")).boxed()
            })
            .await
        {
            tracing::error!("[on_run_job_success({task_id})]: {err}");
        }
    }

    // Sent by schedulers to check the status of servers they've seen
    #[celery::task]
    pub async fn report(respond_to: String) {
        if let Err(err) = super::SERVER
            .get()
            .map(|server| server.report_status(&respond_to))
            .unwrap_or_else(|| {
                futures::future::err(anyhow!("sccache-dist server is not initialized")).boxed()
            })
            .await
        {
            tracing::error!("[report_status({respond_to})]: {err}");
        }
    }
}

// Sent by servers, runs on schedulers
mod server_to_schedulers {
    use celery::prelude::*;

    use sccache::dist::{BuildResult, BuildServerInfo};

    // Runs on scheduler to handle heartbeats from servers
    #[celery::task]
    pub async fn status(info: BuildServerInfo) -> TaskResult<()> {
        super::SCHEDULER
            .get()
            .unwrap()
            .receive_status(info, None)
            .await
            .map_err(|e| TaskError::UnexpectedError(e.to_string()))
    }

    // Runs on the scheduler
    #[celery::task]
    pub async fn job_failure(
        job_id: String,
        reason: String,
        info: BuildServerInfo,
    ) -> TaskResult<()> {
        super::SCHEDULER
            .get()
            .unwrap()
            .job_failure(&job_id, &reason, info)
            .await
            .map_err(|e| TaskError::UnexpectedError(e.to_string()))
    }

    // Runs on the scheduler
    #[celery::task]
    pub async fn job_success(
        job_id: String,
        result: BuildResult,
        info: BuildServerInfo,
    ) -> TaskResult<()> {
        super::SCHEDULER
            .get()
            .unwrap()
            .job_success(&job_id, result, info)
            .await
            .map_err(|e| TaskError::UnexpectedError(e.to_string()))
    }
}
