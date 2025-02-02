// Ignore lint introduced by celery::task macros
#![allow(non_local_definitions)]

use async_trait::async_trait;

use bytes::Buf;
use celery::prelude::*;
use celery::protocol::MessageContentType;
use futures::lock::Mutex;
use futures::{pin_mut, AsyncReadExt, FutureExt, TryFutureExt};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
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

fn init_metrics(
    config: MetricsConfig,
    labels: &[(&str, &str)],
) -> Result<(MetricsConfig, PrometheusHandle)> {
    let builder = labels
        .iter()
        .fold(PrometheusBuilder::new(), |builder, &(key, val)| {
            builder.add_global_label(key, val)
        });

    let (recorder, exporter) = match config {
        MetricsConfig::ListenAddr { ref addr } => {
            let addr = addr.unwrap_or(SocketAddr::from_str("0.0.0.0:9000")?);
            tracing::info!("Listening for metrics at {addr}");
            builder.with_http_listener(addr).build()?
        }
        MetricsConfig::ListenPath { ref path } => {
            let path = path.clone().unwrap_or("/metrics".to_owned());
            tracing::info!("Listening for metrics at {path}");
            builder.build()?
        }
        MetricsConfig::Gateway {
            ref endpoint,
            interval,
            ref username,
            ref password,
        } => {
            tracing::info!("Pushing metrics to {endpoint} every {interval}ms");
            builder
                .set_bucket_count(std::num::NonZeroU32::new(3).unwrap())
                .with_push_gateway(
                    endpoint,
                    Duration::from_millis(interval),
                    username.clone(),
                    password.clone(),
                )?
                .build()?
        }
    };

    let handle = recorder.handle();

    metrics::set_global_recorder(recorder)?;

    tokio::spawn(exporter);

    Ok((config, handle))
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
        // Don't retry tasks that fail with unexpected errors
        .task_retry_for_unexpected(false)
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
                let metrics = metrics
                    .ok_or(anyhow!(""))
                    .and_then(|config| {
                        init_metrics(
                            config,
                            &[
                                ("env", &env_info()),
                                ("type", "scheduler"),
                                ("scheduler_id", &scheduler_id),
                            ],
                        )
                    })
                    .ok();

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
                // This URI can contain the username/password, so log at trace level
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

                tracing::info!("sccache: Scheduler `{scheduler_id}` initialized");

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
            max_per_core_prefetch,
            metrics,
            server_id,
            toolchain_cache_size,
            toolchains,
        }) => {
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

                if let Some(config) = metrics {
                    let _ = init_metrics(
                        config,
                        &[
                            ("env", &env_info()),
                            ("type", "server"),
                            ("server_id", &server_id),
                        ],
                    )?;
                }

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
                );

                let broker_uri = message_broker_uri(message_broker)?;
                // This URI can contain the username/password, so log at trace level
                tracing::trace!("Message broker URI: {broker_uri}");

                let to_servers = scheduler_to_servers_queue();
                let occupancy = (num_cpus as f64 * max_per_core_load.max(0.0)).floor().max(1.0) as usize;
                let pre_fetch = (num_cpus as f64 * max_per_core_prefetch.max(0.0)).floor().max(0.0) as usize;

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
                    server_id.clone(),
                    dist::ServerStats {
                        num_cpus,
                        occupancy,
                        pre_fetch,
                        ..Default::default()
                    },
                    job_queue.clone(),
                    builder,
                    task_queue.clone(),
                    jobs_storage,
                    server_toolchains,
                ));

                SERVER.set(server.clone()).map_err(|err| anyhow!("{err}"))?;

                task_queue.display_pretty().await;

                tracing::info!("sccache: Server `{server_id}` initialized to run {occupancy} parallel build jobs and prefetch up to {pre_fetch} job(s) in the background");

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
    queue_name: String,
    servers: Arc<Mutex<HashMap<String, ServerInfo>>>,
    task_queue: Arc<celery::Celery>,
    toolchains: Arc<dyn sccache::cache::Storage>,
}

impl Scheduler {
    pub fn new(
        job_time_limit: u32,
        to_this_scheduler_queue: String,
        task_queue: Arc<celery::Celery>,
        jobs_storage: Arc<dyn sccache::cache::Storage>,
        toolchains: Arc<dyn sccache::cache::Storage>,
    ) -> Self {
        Self {
            job_time_limit,
            jobs_storage,
            jobs: Arc::new(Mutex::new(HashMap::new())),
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
        let start = Instant::now();
        let res = self.jobs_storage.has(&job_inputs_key(job_id)).await;
        // Record has_job_inputs time
        metrics::histogram!("sccache::scheduler::has_job_inputs_time")
            .record(start.elapsed().as_secs_f64());

        res
    }

    async fn has_job_result(&self, job_id: &str) -> bool {
        let start = Instant::now();
        let res = self.jobs_storage.has(&job_result_key(job_id)).await;
        // Record has_job_result time
        metrics::histogram!("sccache::scheduler::has_job_result_time")
            .record(start.elapsed().as_secs_f64());

        res
    }

    async fn get_job_result(&self, job_id: &str) -> Result<RunJobResponse> {
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

        // Deserialize the result
        let res = bincode_deserialize(result).await.map_err(|err| {
            tracing::warn!("[get_job_result({job_id})]: Error reading result: {err:?}");
            err
        });

        // Record get_job_result time
        metrics::histogram!("sccache::scheduler::get_job_result_time")
            .record(start.elapsed().as_secs_f64());

        res
    }

    async fn del_job_inputs(&self, job_id: &str) -> Result<()> {
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

        // Record del_job_inputs time
        metrics::histogram!("sccache::scheduler::del_job_inputs_time")
            .record(start.elapsed().as_secs_f64());

        Ok(())
    }

    async fn del_job_result(&self, job_id: &str) -> Result<()> {
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

        // Record del_job_result time
        metrics::histogram!("sccache::scheduler::del_job_result_time")
            .record(start.elapsed().as_secs_f64());

        Ok(())
    }

    async fn put_job_inputs(
        &self,
        job_id: &str,
        inputs: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<()> {
        let start = Instant::now();
        self.jobs_storage
            .put_stream(&job_inputs_key(job_id), inputs)
            .await
            .map_err(|err| {
                tracing::warn!("[put_job_inputs({job_id})]: Error writing stream: {err:?}");
                err
            })?;

        // Record put_job_inputs time
        metrics::histogram!("sccache::scheduler::put_job_inputs_time")
            .record(start.elapsed().as_secs_f64());

        Ok(())
    }
}

#[async_trait]
impl SchedulerService for Scheduler {
    async fn get_status(&self) -> Result<SchedulerStatus> {
        let servers = {
            let mut servers = self.servers.lock().await;
            Self::prune_servers(&mut servers);

            let mut server_statuses = vec![];
            for (server_id, server) in servers.iter() {
                server_statuses.push(ServerStatus {
                    id: server_id.clone(),
                    info: server.info.clone(),
                    jobs: server.jobs.clone(),
                    u_time: server.u_time.elapsed().as_secs(),
                });
            }
            server_statuses
        };

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
            });

        metrics::histogram!("sccache::scheduler::put_toolchain_time")
            .record(start.elapsed().as_secs_f64());

        res
    }

    async fn del_toolchain(&self, toolchain: Toolchain) -> Result<()> {
        let start = Instant::now();
        // Delete the toolchain from toolchains storage (S3, GCS, etc.)
        let res = self
            .toolchains
            .del(&toolchain.archive_id)
            .await
            .context("Failed to delete toolchain")
            .map_err(|err| {
                tracing::error!("[del_toolchain({})]: {err:?}", toolchain.archive_id);
                err
            });

        metrics::histogram!("sccache::scheduler::del_toolchain_time")
            .record(start.elapsed().as_secs_f64());

        res
    }

    async fn new_job(&self, request: NewJobRequest) -> Result<NewJobResponse> {
        let job_id = uuid::Uuid::new_v4().simple().to_string();

        let inputs = futures::io::AllowStdIo::new(request.inputs.reader());
        pin_mut!(inputs);

        let (has_toolchain, _) = futures::future::try_join(
            async { Ok(self.has_toolchain(request.toolchain).await) },
            self.put_job(&job_id, inputs),
        )
        .await?;

        Ok(NewJobResponse {
            has_toolchain,
            job_id,
            timeout: self.job_time_limit,
        })
    }

    async fn put_job(
        &self,
        job_id: &str,
        inputs: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<()> {
        self.put_job_inputs(job_id, inputs).await
    }

    async fn run_job(
        &self,
        job_id: &str,
        RunJobRequest {
            toolchain,
            command,
            outputs,
        }: RunJobRequest,
    ) -> Result<RunJobResponse> {
        if self.has_job_result(job_id).await {
            return self.get_job_result(job_id).await;
        }

        if !self.has_job_inputs(job_id).await {
            return Ok(RunJobResponse::JobFailed {
                reason: "Missing inputs".into(),
            });
        }

        let (tx, rx) = tokio::sync::oneshot::channel::<RunJobResponse>();
        self.jobs.lock().await.insert(job_id.to_owned(), tx);

        let res = self
            .task_queue
            .send_task(
                scheduler_to_servers::run_job::new(
                    job_id.to_owned(),
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
            self.jobs.lock().await.remove(job_id);
            Err(err)
        } else {
            rx.await.map_err(anyhow::Error::new)
        }
    }

    async fn del_job(&self, job_id: &str) -> Result<()> {
        if self.has_job_inputs(job_id).await {
            self.del_job_inputs(job_id).await?;
        }
        if self.has_job_result(job_id).await {
            self.del_job_result(job_id).await?;
        }
        Ok(())
    }

    async fn job_failure(&self, job_id: &str, reason: &str, server: ServerDetails) -> Result<()> {
        let send_res = if let Some(sndr) = self.jobs.lock().await.remove(job_id) {
            sndr.send(if server.alive {
                RunJobResponse::JobFailed {
                    reason: reason.to_owned(),
                    server_id: server.id.clone(),
                }
            } else {
                RunJobResponse::ServerShutdown {
                    reason: reason.to_owned(),
                    server_id: server.id.clone(),
                }
            })
            .map_err(|_| anyhow!("Failed to send job result"))
        } else {
            Err(anyhow!(
                "[job_failed({job_id})]: Failed to send response for unknown job"
            ))
        };
        send_res.and(self.receive_status(server, Some(false)).await)
    }

    async fn job_success(&self, job_id: &str, server: ServerDetails) -> Result<()> {
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
        send_res.and(self.receive_status(server, Some(success)).await)
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
    alive: bool,
    jobs: HashMap<String, (String, String)>,
    sys: sysinfo::System,
}

#[derive(Clone)]
pub struct Server {
    builder: Arc<dyn BuilderIncoming>,
    jobs_storage: Arc<dyn sccache::cache::Storage>,
    job_queue: Arc<tokio::sync::Semaphore>,
    report_interval: Duration,
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
            // Report status every 1s
            report_interval: Duration::from_secs(1),
            server_id,
            stats,
            task_queue,
            toolchains,
            state: Arc::new(Mutex::new(ServerState {
                alive: true,
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
        let task_ids = {
            let mut state = self.state.lock().await;
            state.alive = false;
            state.jobs.keys().cloned().collect::<Vec<_>>()
        };
        futures::future::join_all(task_ids.iter().map(|task_id| {
            self.job_failure(task_id, "sccache-dist build server shutdown")
                .boxed()
        }))
        .await;
    }

    fn state_to_details(&self, state: &mut ServerState) -> ServerDetails {
        let running = (self.stats.occupancy as i64 - self.job_queue.available_permits() as i64)
            .max(0) as usize;
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
        metrics::histogram!("sccache::server::cpu_usage_ratio").record(cpu_usage);
        // Record mem_avail
        metrics::histogram!("sccache::server::mem_avail_bytes").record(mem_avail as f64);
        // Record mem_total
        metrics::histogram!("sccache::server::mem_total_bytes").record(mem_total as f64);

        ServerDetails {
            alive: state.alive,
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
        }
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

        Ok(())
    }

    async fn broadcast_status(&self, details: Option<ServerDetails>) -> Result<()> {
        // Separate these statements so `send_status().await` doesn't hold the lock on `self.state`
        let details = if let Some(details) = details {
            details
        } else {
            self.state_to_details(self.state.lock().await.deref_mut())
        };
        self.send_status(None, details.clone()).await
    }

    async fn get_job_inputs(&self, job_id: &str) -> Result<Vec<u8>> {
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
        metrics::histogram!("sccache::server::get_job_inputs_time")
            .record(start.elapsed().as_secs_f64());

        Ok(inputs)
    }

    async fn put_job_result(&self, job_id: &str, result: RunJobResponse) -> Result<()> {
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
        metrics::histogram!("sccache::server::put_job_result_time")
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
                        this.broadcast_status(None)
                            .await
                            .map(|_| this.report_interval)
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
        metrics::counter!("sccache::server::job_fetched").increment(1);

        let job_start = Instant::now();

        // Associate the task with the scheduler and job,
        // then compute and report the latest server details
        let details = {
            let mut state = self.state.lock().await;
            state.jobs.insert(
                task_id.to_owned(),
                (respond_to.to_owned(), job_id.to_owned()),
            );
            // Get details before releasing state lock
            self.state_to_details(&mut state)
        };

        // Report status after accepting the job
        self.broadcast_status(Some(details)).await?;

        // Load and unpack the toolchain
        let toolchain_dir = self.toolchains.acquire(&toolchain).await.map_err(|err| {
            tracing::warn!("[run_job({job_id})]: Error loading toolchain: {err:?}");
            TaskError::UnexpectedError(format!("{err:#}"))
        })?;

        // Report status after loading the toolchain
        self.broadcast_status(None).await?;

        // Load job inputs into memory
        let inputs = self.get_job_inputs(job_id).await.map_err(|err| {
            tracing::warn!("[run_job({job_id})]: Error retrieving job inputs: {err:?}");
            TaskError::UnexpectedError(format!("{err:#}"))
        })?;

        // Report status after loading the inputs
        self.broadcast_status(None).await?;

        // Record toolchain load time
        metrics::histogram!("sccache::server::job_fetched_time")
            .record(job_start.elapsed().as_secs_f64());

        metrics::counter!("sccache::server::job_started").increment(1);

        let build_start = Instant::now();

        let result = self
            .builder
            .run_build(job_id, &toolchain_dir, command, outputs, inputs)
            .await
            .map_err(|err| {
                tracing::warn!("[run_job({job_id})]: Build error: {err:?}");
                TaskError::ExpectedError(format!("{err:#}"))
            });

        // Record build time
        metrics::histogram!("sccache::server::job_build_time")
            .record(build_start.elapsed().as_secs_f64());

        // Store the job result for retrieval by the scheduler
        let result = self
            .put_job_result(
                job_id,
                RunJobResponse::JobComplete {
                    result: result?,
                    server_id: self.server_id.clone(),
                },
            )
            .await
            .map_err(|err| {
                tracing::warn!("[run_job({job_id})]: Error storing job result: {err:?}");
                TaskError::UnexpectedError(format!("{err:#}"))
            });

        // Record total run_job time
        metrics::histogram!("sccache::server::job_time").record(job_start.elapsed().as_secs_f64());

        result.map_err(anyhow::Error::new)
    }

    async fn job_failure(&self, task_id: &str, reason: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some((respond_to, job_id)) = state.jobs.remove(task_id) {
            // Increment job_failure count
            metrics::counter!("sccache::server::job_failed_count").increment(1);

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
            metrics::counter!("sccache::server::job_success_count").increment(1);

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
        max_retries = 2,
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
            .map_err(|e| match e.downcast_ref::<TaskError>() {
                Some(TaskError::UnexpectedError(msg)) => {
                    let msg = format!("run_job failed with unexpected error: {msg}");
                    tracing::error!("[run_job({job_id})]: {msg}");
                    TaskError::UnexpectedError(msg.clone())
                }
                _ => {
                    let msg = format!("run_job failed with error: {e:?}");
                    tracing::error!("[run_job({job_id})]: {msg}");
                    TaskError::ExpectedError(msg)
                }
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
    #[celery::task(max_retries = 0)]
    pub async fn status(server: ServerDetails) -> TaskResult<()> {
        super::SCHEDULER
            .get()
            .unwrap()
            .receive_status(server, None)
            .await
            .map_err(|e| match e.downcast_ref::<TaskError>() {
                Some(TaskError::UnexpectedError(msg)) => {
                    let msg = format!("receive_status failed with unexpected error: {msg}");
                    tracing::error!("[receive_status]: {msg}");
                    TaskError::UnexpectedError(msg.clone())
                }
                _ => {
                    let msg = format!("receive_status failed with error: {e:?}");
                    tracing::error!("[receive_status]: {msg}");
                    TaskError::ExpectedError(msg)
                }
            })
    }

    #[celery::task(max_retries = 0)]
    pub async fn job_failure(
        job_id: String,
        reason: String,
        server: ServerDetails,
    ) -> TaskResult<()> {
        super::SCHEDULER
            .get()
            .unwrap()
            .job_failure(&job_id, &reason, server)
            .await
            .map_err(|e| match e.downcast_ref::<TaskError>() {
                Some(TaskError::UnexpectedError(msg)) => {
                    let msg = format!("job_failure failed with unexpected error: {msg}");
                    tracing::error!("[job_failure({job_id})]: {msg}");
                    TaskError::UnexpectedError(msg.clone())
                }
                _ => {
                    let msg = format!("job_failure failed with error: {e:?}");
                    tracing::error!("[job_failure({job_id})]: {msg}");
                    TaskError::ExpectedError(msg)
                }
            })
    }

    #[celery::task(max_retries = 0)]
    pub async fn job_success(job_id: String, server: ServerDetails) -> TaskResult<()> {
        super::SCHEDULER
            .get()
            .unwrap()
            .job_success(&job_id, server)
            .await
            .map_err(|e| match e.downcast_ref::<TaskError>() {
                Some(TaskError::UnexpectedError(msg)) => {
                    let msg = format!("job_success failed with unexpected error: {msg}");
                    tracing::error!("[job_success({job_id})]: {msg}");
                    TaskError::UnexpectedError(msg.clone())
                }
                _ => {
                    let msg = format!("job_success failed with error: {e:?}");
                    tracing::error!("[job_success({job_id})]: {msg}");
                    TaskError::ExpectedError(msg)
                }
            })
    }
}
