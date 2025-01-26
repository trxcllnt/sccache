// Ignore lint introduced by celery::task macros
#![allow(non_local_definitions)]

use async_trait::async_trait;

use bytes::Buf;
use celery::prelude::*;
use celery::protocol::MessageContentType;
use futures::lock::Mutex;
use futures::{pin_mut, AsyncReadExt, FutureExt, TryFutureExt};
use metrics_exporter_prometheus::PrometheusBuilder;
use sccache::config::MetricsConfig;

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::pin::Pin;
use std::str::FromStr;
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
        self,
        http::{bincode_deserialize, bincode_serialize},
        BuilderIncoming, CompileCommand, JobStats, NewJobRequest, NewJobResponse, RunJobRequest,
        RunJobResponse, SchedulerService, SchedulerStatus, ServerDetails, ServerService,
        ServerStats, ServerStatus, ServerToolchains, SubmitToolchainResult, Toolchain,
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
        _ => bail!(
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

fn env_info() -> String {
    let arch = std::env::var("SCCACHE_DIST_ARCH").unwrap_or(std::env::consts::ARCH.to_owned());
    let os = std::env::var("SCCACHE_DIST_OS").unwrap_or(std::env::consts::OS.to_owned());
    format!("{os}-{arch}")
}

fn queue_name_with_env_info(prefix: &str) -> String {
    format!("{prefix}-{}", env_info())
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

async fn celery_app(
    app_name: &str,
    broker_uri: &str,
    default_queue: &str,
    prefetch_count: u16,
) -> Result<celery::Celery> {
    let to_servers = scheduler_to_servers_queue();
    let to_schedulers = server_to_schedulers_queue();

    celery::CeleryBuilder::new(app_name, broker_uri)
        .default_queue(default_queue)
        .task_content_type(MessageContentType::MsgPack)
        // Register at least one task route for each queue, because that's
        // how celery knows which queues to create in the message broker.
        .task_route(scheduler_to_servers::run_job::NAME, &to_servers)
        .task_route(server_to_schedulers::status::NAME, &to_schedulers)
        .prefetch_count(prefetch_count)
        // Wait at most 10s before retrying failed tasks
        .task_max_retry_delay(10)
        // // Don't retry tasks that fail with unexpected errors
        // .task_retry_for_unexpected(false)
        // Indefinitely retry connecting to the broker
        .broker_connection_max_retries(u32::MAX)
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
            jobs,
            max_body_size,
            message_broker,
            metrics,
            public_addr,
            scheduler_id,
            toolchains,
        }) => {
            let metric_labels = vec![
                ("env".into(), env_info()),
                ("type".into(), "scheduler".into()),
                ("scheduler_id".into(), scheduler_id.clone()),
            ];

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            let jobs_storage = sccache::cache::cache::storage_from_config(
                &jobs.storage,
                &jobs.fallback,
                runtime.handle(),
            )
            .context("Failed to initialize jobs storage")?;

            let toolchains_storage = sccache::cache::cache::storage_from_config(
                &toolchains.storage,
                &toolchains.fallback,
                runtime.handle(),
            )
            .context("Failed to initialize toolchain storage")?;

            runtime.block_on(async move {
                // Verify read/write access to jobs storage
                match jobs_storage.check().await {
                    Ok(sccache::cache::CacheMode::ReadWrite) => {}
                    _ => {
                        bail!("Scheduler jobs storage must be read/write")
                    }
                }

                // Verify read/write access to toolchain storage
                match toolchains_storage.check().await {
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
                    celery_app(
                        &scheduler_id,
                        &broker_uri,
                        &to_this_scheduler,
                        100 * num_cpus as u16,
                    )
                    .await?,
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
                    metric_labels.clone(),
                    to_this_scheduler.clone(),
                    task_queue.clone(),
                    jobs_storage,
                    toolchains_storage,
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
                let server = server.serve(public_addr, max_body_size, metrics);

                futures::select_biased! {
                    res = celery.fuse() => res?,
                    res = server.fuse() => res?,
                };

                task_queue.close().await?;

                Ok::<(), anyhow::Error>(())
            })
        }

        Command::Server(server_config::Config {
            message_broker,
            builder,
            cache_dir,
            jobs,
            max_per_core_load,
            metrics,
            server_id,
            toolchain_cache_size,
            toolchains,
        }) => {
            let metric_labels = vec![
                ("env".into(), env_info()),
                ("type".into(), "server".into()),
                ("server_id".into(), server_id.clone()),
            ];

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            let jobs_storage = sccache::cache::cache::storage_from_config(
                &jobs.storage,
                &jobs.fallback,
                runtime.handle(),
            )
            .context("Failed to initialize jobs storage")?;

            let toolchains_storage = sccache::cache::cache::storage_from_config(
                &toolchains.storage,
                &toolchains.fallback,
                runtime.handle(),
            )
            .context("Failed to initialize toolchain storage")?;

            runtime.block_on(async move {
                // Verify read/write access to jobs storage
                match jobs_storage.check().await {
                    Ok(sccache::cache::CacheMode::ReadWrite) => {}
                    _ => {
                        bail!("Server jobs storage must be read/write")
                    }
                }

                // Verify toolchain storage
                toolchains_storage
                    .check()
                    .await
                    .context("Failed to initialize toolchain storage")?;

                let server_toolchains = ServerToolchains::new(
                    &cache_dir.join("tc"),
                    toolchain_cache_size,
                    toolchains_storage,
                    metric_labels.clone(),
                );

                let occupancy = (num_cpus as f64 * max_per_core_load.max(0.0)).floor().max(1.0);
                let pre_fetch = (occupancy - num_cpus as f64).abs().floor() as usize;
                let occupancy = occupancy.min(num_cpus as f64) as usize;

                let broker_uri = message_broker_uri(message_broker)?;
                tracing::trace!("Message broker URI: {broker_uri}");

                let to_servers = scheduler_to_servers_queue();

                let task_queue =
                    Arc::new(celery_app(&server_id, &broker_uri, &to_servers, (occupancy + pre_fetch) as u16).await?);

                // Tasks this server receives
                task_queue
                    .register_task::<scheduler_to_servers::run_job>()
                    .await?;

                let job_queue = Arc::new(tokio::sync::Semaphore::new(occupancy));

                let builder: Arc<dyn BuilderIncoming> = match builder {
                    #[cfg(not(target_os = "freebsd"))]
                    sccache::config::server::BuilderType::Docker => Arc::new(
                        build::DockerBuilder::new(job_queue.clone())
                            .await
                            .context("Docker builder failed to start")?,
                    ),
                    #[cfg(not(target_os = "freebsd"))]
                    sccache::config::server::BuilderType::Overlay {
                        bwrap_path,
                        build_dir,
                    } => Arc::new(
                        build::OverlayBuilder::new(bwrap_path, build_dir, job_queue.clone())
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
                        build::PotBuilder::new(pot_fs_root, clone_from, pot_cmd, pot_clone_args, job_queue.clone())
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
                    server_id,
                    dist::ServerStats {
                        num_cpus,
                        occupancy,
                        pre_fetch,
                        ..Default::default()
                    },
                    job_queue.clone(),
                    metric_labels.clone(),
                    builder,
                    task_queue.clone(),
                    jobs_storage,
                    server_toolchains,
                ));

                SERVER.set(server.clone()).map_err(|err| anyhow!("{err}"))?;

                if let Some(metrics) = metrics {
                    let builder = PrometheusBuilder::new();
                    match metrics {
                        MetricsConfig::Listen { addr, .. } => {
                            let addr = addr.unwrap_or(SocketAddr::from_str("0.0.0.0:9000")?);
                            tracing::info!("Listening for metrics at {addr}");
                            builder.with_http_listener(addr).install()?;
                        },
                        MetricsConfig::Gateway { endpoint, interval, username, password } => {
                            tracing::info!("Pushing metrics to {endpoint} every {interval}ms");
                            builder.with_push_gateway(endpoint, Duration::from_millis(interval), username, password)?.install()?;
                        }
                    };
                }

                tracing::info!("sccache: Server initialized to run {occupancy} parallel build jobs and prefetch up to {pre_fetch} job(s) in the background");

                task_queue.display_pretty().await;

                daemonize()?;

                let queues = [to_servers.as_str()];

                let celery = task_queue.consume_from(&queues);
                let status = server.report_status();

                futures::select_biased! {
                    res = celery.fuse() => res?,
                    res = status.fuse() => res?,
                };

                server.close().await;
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

fn job_inputs_key(job_id: &str) -> String {
    format!("{job_id}-inputs")
}

fn job_result_key(job_id: &str) -> String {
    format!("{job_id}-result")
}

#[derive(Clone, Debug)]
struct ServerInfo {
    pub u_time: Instant,
    pub info: ServerStats,
    pub jobs: JobStats,
}

pub struct Scheduler {
    job_time_limit: u32,
    jobs_storage: Arc<dyn sccache::cache::Storage>,
    jobs: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<RunJobResponse>>>>,
    jobs_storage_queue: Arc<tokio::sync::Semaphore>,
    metric_labels: Vec<(String, String)>,
    toolchains_storage_queue: Arc<tokio::sync::Semaphore>,
    queue_name: String,
    servers: Arc<Mutex<HashMap<String, ServerInfo>>>,
    task_queue: Arc<celery::Celery>,
    toolchains: Arc<dyn sccache::cache::Storage>,
}

impl Scheduler {
    pub fn new(
        job_time_limit: u32,
        metric_labels: Vec<(String, String)>,
        to_this_scheduler_queue: String,
        task_queue: Arc<celery::Celery>,
        jobs_storage: Arc<dyn sccache::cache::Storage>,
        toolchains: Arc<dyn sccache::cache::Storage>,
    ) -> Self {
        Self {
            job_time_limit,
            jobs_storage,
            jobs: Arc::new(Mutex::new(HashMap::new())),
            // Only load up to 32 jobs concurrently
            jobs_storage_queue: Arc::new(tokio::sync::Semaphore::new(32)),
            metric_labels,
            // Only load up to 32 toolchains concurrently
            toolchains_storage_queue: Arc::new(tokio::sync::Semaphore::new(32)),
            servers: Arc::new(Mutex::new(HashMap::new())),
            task_queue,
            queue_name: to_this_scheduler_queue,
            toolchains,
        }
    }

    fn prune_servers(servers: &mut HashMap<String, ServerInfo>) {
        let now = Instant::now();
        // Prune servers we haven't seen in 90s
        let timeout = Duration::from_secs(90);
        servers.retain(|_, server| now.duration_since(server.u_time) <= timeout);
    }

    async fn has_job_inputs(&self, job_id: &str) -> bool {
        // Guard loading until we get a slot in the network queue
        let start = Instant::now();
        let _ = self.jobs_storage_queue.acquire().await;
        // Record has_job_inputs wait time
        metrics::histogram!("sccache_scheduler_has_job_inputs_wait", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        let start = Instant::now();
        let res = self.jobs_storage.has(&job_inputs_key(job_id)).await;
        // Record has_job_inputs load time
        metrics::histogram!("sccache_scheduler_has_job_inputs_load", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        res
    }

    async fn has_job_result(&self, job_id: &str) -> bool {
        // Guard loading until we get a slot in the network queue
        let start = Instant::now();
        let _ = self.jobs_storage_queue.acquire().await;
        // Record has_job_result wait time
        metrics::histogram!("sccache_scheduler_has_job_result_wait", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        let start = Instant::now();
        let res = self.jobs_storage.has(&job_result_key(job_id)).await;
        // Record has_job_result load time
        metrics::histogram!("sccache_scheduler_has_job_result_load", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        res
    }

    async fn get_job_result(&self, job_id: &str) -> Result<RunJobResponse> {
        // Guard loading until we get a slot in the network queue
        let start = Instant::now();
        let job_slot = self.jobs_storage_queue.acquire().await;
        // Record has_job_result wait time
        metrics::histogram!("sccache_scheduler_get_job_result_wait", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        // Retrieve the result
        let start = Instant::now();
        let mut reader = self
            .jobs_storage
            .get_stream(&job_result_key(job_id))
            .await
            .map_err(|err| {
                tracing::warn!("[get_job_result({job_id})]: Error loading stream: {err:?}");
                err
            })?;

        let mut result = vec![];
        reader.read_to_end(&mut result).await.map_err(|err| {
            tracing::warn!("[get_job_result({job_id})]: Error reading stream: {err:?}");
            err
        })?;
        // Record get_job_result load time
        metrics::histogram!("sccache_scheduler_get_job_result_load", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        // Delete the inputs
        let start = Instant::now();
        let _ = self
            .jobs_storage
            .del(&job_inputs_key(job_id))
            .map_err(|e| {
                tracing::warn!("[get_job_result({job_id})]: Error deleting job inputs: {e:?}");
                e
            })
            .await;

        // Record del_job_inputs load time
        metrics::histogram!("sccache_scheduler_del_job_inputs_load", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        // Delete the result
        let start = Instant::now();
        let _ = self
            .jobs_storage
            .del(&job_result_key(job_id))
            .map_err(|e| {
                tracing::warn!("[get_job_result({job_id})]: Error deleting job result: {e:?}");
                e
            })
            .await;

        // Record del_job_result load time
        metrics::histogram!("sccache_scheduler_del_job_result_load", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        // Drop the lock
        drop(job_slot);

        // Deserialize and return the result
        bincode_deserialize(result).await.map_err(|err| {
            tracing::warn!("[get_job_result({job_id})]: Error reading result: {err:?}");
            err
        })
    }

    async fn put_job_inputs(&self, job_id: &str, inputs: &[u8]) -> Result<()> {
        // Guard storing until we get a slot in the network queue
        let start = Instant::now();
        let _ = self.jobs_storage_queue.acquire().await;
        // Record put_job_inputs wait time
        metrics::histogram!("sccache_scheduler_put_job_inputs_wait", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        let start = Instant::now();
        let reader = futures::io::AllowStdIo::new(inputs.reader());
        pin_mut!(reader);
        self.jobs_storage
            .put_stream(&job_inputs_key(job_id), reader)
            .await
            .map_err(|err| {
                tracing::warn!("[put_job_inputs({job_id})]: Error writing stream: {err:?}");
                err
            })?;
        // Record put_job_inputs load time
        metrics::histogram!("sccache_scheduler_put_job_inputs_load", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        Ok(())
    }
}

#[async_trait]
impl SchedulerService for Scheduler {
    async fn get_status(&self) -> Result<SchedulerStatus> {
        let mut servers = vec![];

        for (server_id, server) in self.servers.lock().await.iter() {
            servers.push(ServerStatus {
                id: server_id.clone(),
                info: server.info.clone(),
                jobs: server.jobs.clone(),
                u_time: server.u_time.elapsed().as_secs(),
            });
        }

        Ok(SchedulerStatus {
            info: Some(
                servers
                    .iter()
                    .fold(dist::ServerStats::default(), |mut info, server| {
                        info.cpu_usage += server.info.cpu_usage;
                        info.mem_avail += server.info.mem_avail;
                        info.mem_total += server.info.mem_total;
                        info.num_cpus += server.info.num_cpus;
                        info.occupancy += server.info.occupancy;
                        info.pre_fetch += server.info.pre_fetch;
                        info
                    }),
            )
            .map(|mut info| {
                info.cpu_usage /= servers.len() as f32;
                info
            })
            .unwrap(),
            jobs: dist::JobStats {
                fetched: servers.iter().map(|server| server.jobs.fetched).sum(),
                running: servers.iter().map(|server| server.jobs.running).sum(),
            },
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
        let start = Instant::now();
        // Guard storing until we get a slot in the network queue
        let _ = self.toolchains_storage_queue.acquire().await;

        metrics::histogram!("sccache_scheduler_put_toolchain_wait", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        let start = Instant::now();
        // Upload toolchain to toolchains storage (S3, GCS, etc.)
        let res = self
            .toolchains
            .put_stream(&toolchain.archive_id, toolchain_reader)
            .await
            .context("Failed to put toolchain")
            .map(|_| SubmitToolchainResult::Success)
            .map_err(|err| {
                tracing::error!("[put_toolchain({})]: {err:?}", toolchain.archive_id);
                err
            })?;

        metrics::histogram!("sccache_scheduler_put_toolchain_load", &self.metric_labels)
            .record(start.elapsed().as_secs_f64());

        Ok(res)
    }

    async fn new_job(&self, request: NewJobRequest) -> Result<NewJobResponse> {
        let job_id = uuid::Uuid::new_v4().simple().to_string();

        let (has_toolchain, _) = futures::future::try_join(
            async { Ok(self.has_toolchain(request.toolchain).await) },
            self.put_job_inputs(&job_id, &request.inputs),
        )
        .await?;

        Ok(NewJobResponse {
            has_toolchain,
            job_id,
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
        }: RunJobRequest,
    ) -> Result<RunJobResponse> {
        if self.has_job_result(&job_id).await {
            return self.get_job_result(&job_id).await;
        }

        if !self.has_job_inputs(&job_id).await {
            return Ok(RunJobResponse::JobFailed {
                reason: "Missing inputs".into(),
            });
        }

        let (tx, rx) = tokio::sync::oneshot::channel::<RunJobResponse>();
        self.jobs.lock().await.insert(job_id.clone(), tx);

        let res = self
            .task_queue
            .send_task(
                scheduler_to_servers::run_job::new(
                    job_id.clone(),
                    self.queue_name.clone(),
                    toolchain,
                    command,
                    outputs,
                )
                .with_time_limit(self.job_time_limit)
                .with_expires_in(self.job_time_limit),
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

    async fn job_failure(&self, job_id: &str, reason: &str, info: ServerDetails) -> Result<()> {
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
        send_res.and(self.receive_status(info, Some(false)).await)
    }

    async fn job_success(&self, job_id: &str, info: ServerDetails) -> Result<()> {
        let send_res = if let Some(sndr) = self.jobs.lock().await.remove(job_id) {
            self.get_job_result(job_id)
                .await
                .context(format!("Failed to retrieve result for job {job_id}"))
                .and_then(|result| {
                    sndr.send(result)
                        .map_err(|_| anyhow!("Failed to send job result"))
                })
        } else {
            Err(anyhow!(
                "[job_complete({job_id})]: Failed to send response for unknown job"
            ))
        };
        let success = send_res.is_ok();
        send_res.and(self.receive_status(info, Some(success)).await)
    }

    async fn receive_status(&self, details: ServerDetails, job_status: Option<bool>) -> Result<()> {
        if let Some(success) = job_status {
            if success {
                tracing::trace!("Received server success: {details:?}");
            } else {
                tracing::trace!("Received server failure: {details:?}");
            }
        } else {
            tracing::trace!("Received server status: {details:?}");
        }

        let mut servers = self.servers.lock().await;

        // Insert or update the server info
        servers
            .entry(details.id.clone())
            .and_modify(|server| {
                server.info = details.info.clone();
                server.jobs = details.jobs.clone();
                server.u_time = Instant::now();
            })
            .or_insert_with(|| ServerInfo {
                info: details.info.clone(),
                jobs: details.jobs.clone(),
                u_time: Instant::now(),
            });

        Self::prune_servers(&mut servers);

        Ok(())
    }
}

struct ServerState {
    jobs: HashMap<String, (String, String)>,
    sys: sysinfo::System,
}

#[derive(Clone)]
pub struct Server {
    builder: Arc<dyn BuilderIncoming>,
    jobs_storage: Arc<dyn sccache::cache::Storage>,
    job_queue: Arc<tokio::sync::Semaphore>,
    jobs_storage_queue: Arc<tokio::sync::Semaphore>,
    metric_labels: Vec<(String, String)>,
    report_interval: Duration,
    schedulers: Arc<Mutex<HashMap<String, Instant>>>,
    server_id: String,
    state: Arc<Mutex<ServerState>>,
    stats: dist::ServerStats,
    task_queue: Arc<celery::Celery>,
    toolchains: ServerToolchains,
}

impl Server {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        server_id: String,
        stats: dist::ServerStats,
        job_queue: Arc<tokio::sync::Semaphore>,
        metric_labels: Vec<(String, String)>,
        builder: Arc<dyn BuilderIncoming>,
        task_queue: Arc<celery::Celery>,
        jobs_storage: Arc<dyn sccache::cache::Storage>,
        toolchains: ServerToolchains,
    ) -> Self {
        use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
        Self {
            builder,
            jobs_storage,
            job_queue,
            // Only load up to 16 job inputs concurrently
            jobs_storage_queue: Arc::new(tokio::sync::Semaphore::new(16)),
            metric_labels,
            // Report status at least every 30s
            report_interval: Duration::from_secs(30),
            schedulers: Default::default(),
            server_id,
            stats,
            task_queue,
            toolchains,
            state: Arc::new(Mutex::new(ServerState {
                jobs: Default::default(),
                sys: System::new_with_specifics(
                    RefreshKind::nothing()
                        .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
                        .with_memory(MemoryRefreshKind::nothing().with_ram()),
                ),
            })),
        }
    }

    pub async fn close(&self) {
        let stats = self.state.lock().await;
        let task_ids = stats.jobs.keys().cloned().collect::<Vec<_>>();
        futures::future::join_all(
            task_ids
                .iter()
                .map(|task_id| self.job_failure(task_id, "server shutdown").boxed()),
        )
        .await;
    }

    async fn update_last_report_time(&self, scheduler: &str) {
        self.schedulers
            .lock()
            .await
            .entry(scheduler.to_owned())
            .and_modify(|t| *t = Instant::now())
            .or_insert_with(Instant::now);
    }

    fn state_to_details(&self, state: &mut ServerState) -> ServerDetails {
        let start = Instant::now();
        let running = self.stats.occupancy - self.job_queue.available_permits();
        let fetched = state.jobs.len() - running;

        state
            .sys
            .refresh_cpu_specifics(sysinfo::CpuRefreshKind::nothing().with_cpu_usage());
        state
            .sys
            .refresh_memory_specifics(sysinfo::MemoryRefreshKind::nothing().with_ram());

        let cpu_usage = state.sys.global_cpu_usage();
        let mem_avail = state.sys.available_memory();
        let mem_total = state.sys.total_memory();

        // Record cpu_usage
        metrics::histogram!("sccache_server_cpu_usage_ratio", &self.metric_labels)
            .record(cpu_usage / 100.0);
        // Record mem_avail
        metrics::histogram!("sccache_server_mem_avail_bytes", &self.metric_labels)
            .record(mem_avail as f64);
        // Record mem_total
        metrics::histogram!("sccache_server_mem_total_bytes", &self.metric_labels)
            .record(mem_total as f64);

        let details = ServerDetails {
            id: self.server_id.clone(),
            info: dist::ServerStats {
                cpu_usage,
                mem_avail,
                mem_total,
                num_cpus: self.stats.num_cpus,
                occupancy: self.stats.occupancy,
                pre_fetch: self.stats.pre_fetch,
            },
            jobs: dist::JobStats { fetched, running },
        };

        // Record server_stats time (e.g. in case sys.refresh_* functions are expensive?)
        metrics::histogram!(
            "sccache_server_stats_to_details_time_seconds",
            &self.metric_labels
        )
        .record(start.elapsed().as_secs_f64());

        details
    }

    async fn send_status(
        &self,
        respond_to: impl Into<Option<String>>,
        details: ServerDetails,
    ) -> Result<()> {
        let respond_to = respond_to.into();

        tracing::trace!("Reporting server status: {details:?}");

        let task = server_to_schedulers::status::new(details);
        let task = task.with_expires_in(60);
        let task = if let Some(ref respond_to) = respond_to {
            task.with_queue(respond_to)
        } else {
            task
        };

        self.task_queue
            .send_task(task)
            .await
            .map_err(anyhow::Error::new)
            .map(|_| ())?;

        // Update last_report_time
        if let Some(ref respond_to) = respond_to {
            self.update_last_report_time(respond_to).await;
        }

        Ok(())
    }

    async fn broadcast_status(&self) -> Vec<Result<()>> {
        let start = Instant::now();
        // Separate these statements so `broadcast_status_details().await` doesn't hold the lock on `self.state`
        let details = self.state_to_details(self.state.lock().await.deref_mut());
        let res = self.broadcast_status_details(details).await;
        // Record broadcast_status time
        metrics::histogram!(
            "sccache_server_broadcast_status_time_seconds",
            &self.metric_labels
        )
        .record(start.elapsed().as_secs_f64());
        res
    }

    // A version of `broadcast_status` that accepts the details
    async fn broadcast_status_details(&self, details: ServerDetails) -> Vec<Result<()>> {
        let start = Instant::now();
        let ids = {
            let report_interval = self.report_interval;
            let mut schedulers = self.schedulers.lock().await;
            Self::prune_schedulers(&mut schedulers);
            let now = Instant::now();
            schedulers
                .iter()
                .filter(|&(_, &last_ack_time)| now.duration_since(last_ack_time) > report_interval)
                .map(|(scheduler_id, _)| scheduler_id)
                .cloned()
                .collect::<Vec<_>>()
        };

        // Send status updates to all schedulers we know about and haven't notified recently.
        // If there are no schedulers to update, send the status to any scheduler.
        //
        // This would be simpler if rusty-celery supported Broadcast queues[1], so
        // a copy of the same message is delivered to to all interested schedulers.
        //
        // As it stands, we have to manually track and notify schedulers that we know about,
        // and periodically send general status updates that might or might not be handled
        // by schedulers we don't yet know about.
        //
        // 1. https://docs.celeryq.dev/en/stable/userguide/routing.html#broadcast

        let requests = ids.into_iter().fold(vec![], |mut futs, id| {
            futs.push(self.send_status(Some(id), details.clone()));
            futs
        });

        let res = futures::future::join_all({
            if !requests.is_empty() {
                requests
            } else {
                // If no schedulers to update, send status to any scheduler
                vec![self.send_status(None, details.clone())]
            }
        })
        .await;

        // Record broadcast_status_details time
        metrics::histogram!(
            "sccache_server_broadcast_status_details_time_seconds",
            &self.metric_labels
        )
        .record(start.elapsed().as_secs_f64());

        res
    }

    // Prune schedulers that haven't ack'd our status updates in 90s
    fn prune_schedulers(schedulers: &mut HashMap<String, Instant>) {
        let now = Instant::now();
        // Prune schedulers we haven't seen in 90s
        let timeout = Duration::from_secs(90);
        schedulers.retain(|_, &mut last_ack_time| now.duration_since(last_ack_time) <= timeout);
    }

    async fn get_job_inputs(&self, job_id: &str) -> Result<Vec<u8>> {
        let start = Instant::now();
        // Guard loading until we get a slot in the network queue
        let _ = self.jobs_storage_queue.acquire().await;
        // Record get_job_inputs wait time
        metrics::histogram!(
            "sccache_server_get_job_inputs_wait_time_seconds",
            &self.metric_labels
        )
        .record(start.elapsed().as_secs_f64());

        let start = Instant::now();
        let mut reader = self
            .jobs_storage
            .get_stream(&job_inputs_key(job_id))
            .await
            .map_err(|err| {
                tracing::warn!("[get_job_inputs({job_id})]: Error loading stream: {err:?}");
                err
            })?;

        let mut inputs = vec![];
        reader.read_to_end(&mut inputs).await.map_err(|err| {
            tracing::warn!("[get_job_inputs({job_id})]: Error reading stream: {err:?}");
            err
        })?;
        // Record get_job_inputs load time
        metrics::histogram!(
            "sccache_server_get_job_inputs_load_time_seconds",
            &self.metric_labels
        )
        .record(start.elapsed().as_secs_f64());

        Ok(inputs)
    }

    async fn put_job_result(&self, job_id: &str, result: RunJobResponse) -> Result<()> {
        let start = Instant::now();
        // Guard storing until we get a slot in the network queue
        let _ = self.jobs_storage_queue.acquire().await;
        // Record put_job_result wait time
        metrics::histogram!(
            "sccache_server_put_job_result_wait_time_seconds",
            &self.metric_labels
        )
        .record(start.elapsed().as_secs_f64());

        let start = Instant::now();
        let result = bincode_serialize(result).await?;
        let reader = futures::io::AllowStdIo::new(result.reader());
        pin_mut!(reader);

        self.jobs_storage
            .put_stream(&job_result_key(job_id), reader)
            .await
            .map_err(|err| {
                tracing::warn!("[put_job_result({job_id})]: Error writing stream: {err:?}");
                err
            })?;

        // Record put_job_result load time
        metrics::histogram!(
            "sccache_server_put_job_result_load_time_seconds",
            &self.metric_labels
        )
        .record(start.elapsed().as_secs_f64());

        Ok(())
    }
}

#[async_trait]
impl ServerService for Server {
    async fn report_status(&self) -> Result<()> {
        tokio::spawn({
            let this = Arc::new(self.clone());
            async move {
                loop {
                    tokio::time::sleep(
                        this.broadcast_status()
                            .await
                            .iter()
                            .any(|res| res.is_err())
                            .then(|| this.report_interval / 2)
                            .unwrap_or(this.report_interval),
                    )
                    .await;
                }
            }
        })
        .await
        .map_err(anyhow::Error::new)
    }

    async fn run_job(
        &self,
        task_id: &str,
        job_id: &str,
        respond_to: &str,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> Result<()> {
        metrics::counter!("sccache_server_job_fetched", &self.metric_labels).increment(1);

        let start = Instant::now();
        // Associate the task with the scheduler and job,
        // then compute and report the latest server details
        let details = {
            let mut state = self.state.lock().await;
            state.jobs.insert(
                task_id.to_owned(),
                (respond_to.to_owned(), job_id.to_owned()),
            );
            // Record the time it took to accept the job
            metrics::histogram!("sccache_server_job_wait_time_seconds", &self.metric_labels)
                .record(start.elapsed().as_secs_f64());
            // Get details before releasing state lock
            self.state_to_details(&mut state)
        };

        // Report status after accepting the job
        self.broadcast_status_details(details).await;

        // Load and unpack the toolchain
        let toolchain_dir = {
            let start = std::time::Instant::now();
            let toolchain_dir = self
                .toolchains
                .acquire(&toolchain)
                .map_err(|err| {
                    tracing::warn!("[run_job({job_id})]: Error loading toolchain: {err:?}");
                    err
                })
                .await?;
            // Record toolchain load time
            metrics::histogram!(
                "sccache_server_toolchain_acquired_time_seconds",
                &self.metric_labels
            )
            .record(start.elapsed().as_secs_f64());
            toolchain_dir
        };

        // Report status after loading the toolchain
        self.broadcast_status().await;

        // Load job inputs into memory
        let inputs = self.get_job_inputs(job_id).await?;

        // Report status after loading the inputs
        self.broadcast_status().await;

        // Record toolchain load time
        metrics::histogram!(
            "sccache_server_job_fetched_time_seconds",
            &self.metric_labels
        )
        .record(start.elapsed().as_secs_f64());

        metrics::counter!("sccache_server_job_started", &self.metric_labels).increment(1);

        match self
            .builder
            .run_build(job_id, &toolchain_dir, command, outputs, inputs)
            .await
        {
            Ok(result) => {
                self.put_job_result(
                    job_id,
                    RunJobResponse::JobComplete {
                        result,
                        server_id: self.server_id.clone(),
                    },
                )
                .await
                .map(|x| {
                    // Record total run_job time
                    metrics::histogram!("sccache_server_job_time_seconds", &self.metric_labels)
                        .record(start.elapsed().as_secs_f64());
                    x
                })
                .map_err(|e| {
                    // Record total run_job time
                    metrics::histogram!("sccache_server_job_time_seconds", &self.metric_labels)
                        .record(start.elapsed().as_secs_f64());
                    e
                })
            }
            Err(e) => {
                // Record total run_job time
                metrics::histogram!("sccache_server_job_time_seconds", &self.metric_labels)
                    .record(start.elapsed().as_secs_f64());
                Err(e)
            }
        }
    }

    async fn job_failure(&self, task_id: &str, reason: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some((respond_to, job_id)) = state.jobs.remove(task_id) {
            // Increment job_failure count
            metrics::counter!("sccache_server_job_failure", &self.metric_labels).increment(1);

            // Get ServerDetails
            let details = self.state_to_details(&mut state);
            // Drop lock
            drop(state);

            self.task_queue
                .send_task(
                    server_to_schedulers::job_failure::new(job_id, reason.to_owned(), details)
                        .with_queue(&respond_to)
                        .with_expires_in(60),
                )
                .await
                .map_err(anyhow::Error::new)
                .map(|_| ())?;

            // Update last_report_time on failure
            self.update_last_report_time(&respond_to).await;

            Ok(())
        } else {
            tracing::error!("[job_failure({task_id})]: Failed to report task failure");
            Err(anyhow!("Cannot report task failure ({task_id})"))
        }
    }

    async fn job_success(&self, task_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some((respond_to, job_id)) = state.jobs.remove(task_id) {
            // Increment job_success count
            metrics::counter!("sccache_server_job_success", &self.metric_labels).increment(1);

            // Get ServerDetails
            let details = self.state_to_details(&mut state);
            // Drop lock
            drop(state);

            self.task_queue
                .send_task(
                    server_to_schedulers::job_success::new(job_id, details)
                        .with_queue(&respond_to)
                        .with_expires_in(60),
                )
                .await
                .map_err(anyhow::Error::new)
                .map(|_| ())?;

            // Update last_report_time on success
            self.update_last_report_time(&respond_to).await;

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
    use sccache::dist::{CompileCommand, Toolchain};

    #[celery::task(
        bind = true,
        acks_late = true,
        acks_on_failure_or_timeout = false,
        max_retries = 0,
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
    ) -> TaskResult<()> {
        let task_id = task.request.id.clone();

        tracing::debug!(
            "[run_job({task_id}, {job_id}, {respond_to}, {}, {:?}, {:?}, {outputs:?})]",
            toolchain.archive_id,
            command.executable,
            command.arguments,
        );

        super::SERVER
            .get()
            .map(|server| {
                server.run_job(&task_id, &job_id, &respond_to, toolchain, command, outputs)
            })
            .unwrap_or_else(|| {
                futures::future::err(anyhow!("sccache-dist server is not initialized")).boxed()
            })
            .await
            .map_err(|e| {
                let msg = format!("run_job failed with error: {e:?}");
                tracing::error!("[run_job({job_id})]: {msg}");
                TaskError::UnexpectedError(msg)
            })
    }

    async fn on_run_job_failure(task: &run_job, err: &TaskError) {
        let task_id = task.request().id.clone();
        let reason = match err {
            TaskError::TimeoutError => {
                format!("[run_job({task_id})]: run_job timed out")
            }
            _ => {
                format!("[run_job({task_id})]: {err}")
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

    async fn on_run_job_success(task: &run_job, _: &()) {
        let task_id = task.request().id.clone();
        if let Err(err) = super::SERVER
            .get()
            .map(|server| server.job_success(&task_id))
            .unwrap_or_else(|| {
                futures::future::err(anyhow!("sccache-dist server is not initialized")).boxed()
            })
            .await
        {
            tracing::error!("[on_run_job_success({task_id})]: {err}");
        }
    }
}

// Sent by servers, runs on schedulers
mod server_to_schedulers {
    use celery::prelude::*;

    use sccache::dist::ServerDetails;

    // Limit retries. These are all tasks sent to a specific scheduler.
    // If that scheduler goes offline, we want the broker to expire its
    // messages instead of keeping them in the queue indefinitely.

    // Runs on scheduler to handle heartbeats from servers
    #[celery::task(max_retries = 10)]
    pub async fn status(info: ServerDetails) -> TaskResult<()> {
        super::SCHEDULER
            .get()
            .unwrap()
            .receive_status(info, None)
            .await
            .map_err(|e| TaskError::UnexpectedError(e.to_string()))
    }

    #[celery::task(max_retries = 10)]
    pub async fn job_failure(
        job_id: String,
        reason: String,
        info: ServerDetails,
    ) -> TaskResult<()> {
        super::SCHEDULER
            .get()
            .unwrap()
            .job_failure(&job_id, &reason, info)
            .await
            .map_err(|e| TaskError::UnexpectedError(e.to_string()))
    }

    #[celery::task(max_retries = 10)]
    pub async fn job_success(job_id: String, info: ServerDetails) -> TaskResult<()> {
        super::SCHEDULER
            .get()
            .unwrap()
            .job_success(&job_id, info)
            .await
            .map_err(|e| TaskError::UnexpectedError(e.to_string()))
    }
}
