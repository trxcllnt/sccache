// Ignore lint introduced by celery::task macros
#![allow(non_local_definitions)]

use async_trait::async_trait;

use bytes::Buf;
use celery::prelude::*;
use celery::protocol::MessageContentType;
use futures::lock::Mutex;
use futures::{AsyncReadExt, FutureExt, TryFutureExt};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use sccache::config::MetricsConfig;
use sccache::dist::http::{retry_with_jitter, AsyncMulticast, AsyncMulticastFn};
use sccache::dist::{BuildResult, RunJobError};
use tokio_retry2::RetryError;

use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
        .task_route(scheduler_to_servers::run_job::NAME, &to_servers)
        .task_route(server_to_schedulers::job_finished::NAME, &to_schedulers)
        .task_route(server_to_schedulers::status_update::NAME, &to_schedulers)
        .prefetch_count(prefetch_count)
        // Wait at most 1s before retrying failed tasks
        .task_max_retry_delay(1)
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
            let jobs_storage =
                sccache::cache::cache::storage_from_config(&jobs.storage, &jobs.fallback)
                    .context("Failed to initialize jobs storage")?;

            let toolchains_storage = sccache::cache::cache::storage_from_config(
                &toolchains.storage,
                &toolchains.fallback,
            )
            .context("Failed to initialize toolchain storage")?;

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async move {
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

                    let tasks = Arc::new(
                        celery_app(
                            &scheduler_id,
                            &broker_uri,
                            &to_schedulers,
                            100 * num_cpus as u16,
                        )
                        .await?,
                    );

                    // Tasks this scheduler receives
                    tasks
                        .register_task::<server_to_schedulers::job_finished>()
                        .await
                        .unwrap();

                    tasks
                        .register_task::<server_to_schedulers::status_update>()
                        .await
                        .unwrap();

                    let scheduler = Arc::new(Scheduler::new(
                        job_time_limit,
                        jobs_storage,
                        scheduler_id.clone(),
                        tasks.clone(),
                        toolchains_storage,
                    ));

                    SCHEDULER
                        .set(scheduler.clone())
                        .map_err(|e| anyhow!(e.to_string()))?;

                    let server = dist::server::Scheduler::new(
                        scheduler,
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

                    tasks.display_pretty().await;

                    tracing::info!("sccache: Scheduler `{scheduler_id}` initialized");

                    daemonize()?;

                    let queues = [to_schedulers.as_str()];
                    let celery = tasks.consume_from(&queues);
                    let server = server.serve(public_addr, max_body_size, metrics);

                    futures::select_biased! {
                        res = celery.fuse() => res?,
                        res = server.fuse() => res?,
                    };

                    tasks.close().await?;

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
            let jobs_storage =
                sccache::cache::cache::storage_from_config(&jobs.storage, &jobs.fallback)
                    .context("Failed to initialize jobs storage")?;

            let toolchains_storage = sccache::cache::cache::storage_from_config(
                &toolchains.storage,
                &toolchains.fallback,
            )
            .context("Failed to initialize toolchain storage")?;

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async move {

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

                let to_servers = scheduler_to_servers_queue();
                let occupancy = (num_cpus as f64 * max_per_core_load.max(0.0)).floor().max(1.0) as usize;
                let pre_fetch = (num_cpus as f64 * max_per_core_prefetch.max(0.0)).floor().max(0.0) as usize;

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

                let broker_uri = message_broker_uri(message_broker)?;
                // This URI can contain the username/password, so log at trace level
                tracing::trace!("Message broker URI: {broker_uri}");

                let tasks =
                    Arc::new(celery_app(&server_id, &broker_uri, &to_servers, (occupancy + pre_fetch) as u16).await?);

                // Tasks this server receives
                tasks
                    .register_task::<scheduler_to_servers::run_job>()
                    .await?;

                struct LoadToolchainFn {
                    toolchains: ServerToolchains,
                }

                #[async_trait]
                impl AsyncMulticastFn<'_, Toolchain, PathBuf> for LoadToolchainFn {
                    async fn call(&self, tc: &Toolchain) -> Result<PathBuf> {
                        self.toolchains.load(tc).await
                    }
                }

                let server = Arc::new(Server::new(
                    builder,
                    job_queue,
                    jobs_storage,
                    server_id.clone(),
                    dist::ServerStats {
                        num_cpus,
                        occupancy,
                        pre_fetch,
                        ..Default::default()
                    },
                    tasks.clone(),
                    AsyncMulticast::new(LoadToolchainFn {
                        toolchains: ServerToolchains::new(
                            cache_dir.join("tc"),
                            toolchain_cache_size,
                            toolchains_storage,
                        )
                    }),
                ));

                SERVER.set(server.clone()).map_err(|err| anyhow!("{err}"))?;

                tasks.display_pretty().await;

                tracing::info!("sccache: Server `{server_id}` initialized to run {occupancy} parallel build jobs and prefetch up to {pre_fetch} job(s) in the background");

                daemonize()?;

                let queues = [to_servers.as_str()];
                let celery = tasks.consume_from(&queues);
                let status = server.start();

                futures::select_biased! {
                    res = celery.fuse() => res?,
                    res = status.fuse() => res?,
                };

                server.close().await;
                tasks.close().await?;

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
    pub u_time: SystemTime,
    pub info: ServerStats,
    pub jobs: JobStats,
}

pub struct Scheduler {
    job_time_limit: u32,
    jobs_storage: Arc<dyn sccache::cache::Storage>,
    jobs: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<RunJobResponse>>>>,
    scheduler_id: String,
    servers: Arc<Mutex<HashMap<String, ServerInfo>>>,
    tasks: Arc<celery::Celery>,
    toolchains: Arc<dyn sccache::cache::Storage>,
}

impl Scheduler {
    pub fn new(
        job_time_limit: u32,
        jobs_storage: Arc<dyn sccache::cache::Storage>,
        scheduler_id: String,
        tasks: Arc<celery::Celery>,
        toolchains: Arc<dyn sccache::cache::Storage>,
    ) -> Self {
        Self {
            job_time_limit,
            jobs_storage,
            jobs: Arc::new(Mutex::new(HashMap::new())),
            scheduler_id,
            servers: Arc::new(Mutex::new(HashMap::new())),
            tasks,
            toolchains,
        }
    }

    fn prune_servers(servers: &mut HashMap<String, ServerInfo>) {
        let now = SystemTime::now();
        // Prune servers we haven't seen in 90s
        let timeout = Duration::from_secs(90);
        servers.retain(|_, server| now.duration_since(server.u_time).unwrap() <= timeout);
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
        let job_result_name = job_result_key(job_id);
        let res = retry_with_jitter(10, || async {
            let mut reader = self
                .jobs_storage
                .get_stream(&job_result_name)
                .await
                .map_err(|err| {
                    tracing::warn!("[get_job_result({job_id})]: Error loading stream: {err:?}");
                    RetryError::transient(err)
                })?;

            let mut result = vec![];
            reader.read_to_end(&mut result).await.map_err(|err| {
                tracing::warn!("[get_job_result({job_id})]: Error reading stream: {err:?}");
                RetryError::permanent(anyhow!(err))
            })?;

            Ok(result)
        })
        .and_then(|result| async move {
            // Deserialize the result
            bincode_deserialize(result).await.map_err(|err| {
                tracing::warn!("[get_job_result({job_id})]: Error deserializing result: {err:?}");
                err
            })
        })
        .await;

        // Record get_job_result time
        metrics::histogram!("sccache::scheduler::get_job_result_time")
            .record(start.elapsed().as_secs_f64());

        res
    }

    async fn del_job_inputs(&self, job_id: &str) -> Result<()> {
        // Delete the inputs
        let start = Instant::now();
        let res = self
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

        res
    }

    async fn del_job_result(&self, job_id: &str) -> Result<()> {
        // Delete the result
        let start = Instant::now();
        let res = self
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

        res
    }

    async fn put_job_result(
        &self,
        job_id: &str,
        inputs_size: u64,
        inputs: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<()> {
        let start = Instant::now();
        let res = self
            .jobs_storage
            .put_stream(&job_inputs_key(job_id), inputs_size, inputs)
            .await
            .map_err(|err| {
                tracing::warn!("[put_job_result({job_id})]: Error writing stream: {err:?}");
                err
            });

        // Record put_job_result time
        metrics::histogram!("sccache::scheduler::put_job_result_time")
            .record(start.elapsed().as_secs_f64());

        res
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
                    u_time: server.u_time.elapsed().unwrap().as_secs(),
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
                accepted: servers.iter().map(|server| server.jobs.accepted).sum(),
                finished: servers.iter().map(|server| server.jobs.finished).sum(),
                loading: servers.iter().map(|server| server.jobs.loading).sum(),
                running: servers.iter().map(|server| server.jobs.running).sum(),
            },
            servers,
        })
    }

    async fn has_toolchain(&self, toolchain: &Toolchain) -> bool {
        self.toolchains.has(&toolchain.archive_id).await
    }

    async fn put_toolchain(
        &self,
        toolchain: &Toolchain,
        toolchain_size: u64,
        toolchain_reader: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<SubmitToolchainResult> {
        let start = Instant::now();
        // Upload toolchain to toolchains storage (S3, GCS, etc.)
        let res = self
            .toolchains
            .put_stream(&toolchain.archive_id, toolchain_size, toolchain_reader)
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

    async fn del_toolchain(&self, toolchain: &Toolchain) -> Result<()> {
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
        let inputs = std::pin::pin!(futures::io::AllowStdIo::new(request.inputs.reader()));
        let (has_toolchain, _) = futures::future::try_join(
            async { Ok(self.has_toolchain(&request.toolchain).await) },
            self.put_job(&job_id, request.inputs.len() as u64, inputs),
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
        inputs_size: u64,
        inputs: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<()> {
        self.put_job_result(job_id, inputs_size, inputs).await
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
            match self.get_job_result(job_id).await {
                Ok(RunJobResponse::JobFailed { reason, server_id }) => {
                    return Ok(RunJobResponse::JobFailed { reason, server_id })
                }
                Ok(RunJobResponse::JobComplete { result, server_id }) => {
                    return Ok(RunJobResponse::JobComplete { result, server_id })
                }
                _ => {
                    let _ = self.del_job_result(job_id).await;
                }
            }
        }

        if !self.has_toolchain(&toolchain).await {
            return Ok(RunJobResponse::MissingToolchain {
                server_id: self.scheduler_id.clone(),
            });
        }

        if !self.has_job_inputs(job_id).await {
            return Ok(RunJobResponse::MissingJobInputs {
                server_id: self.scheduler_id.clone(),
            });
        }

        let (tx, rx) = tokio::sync::oneshot::channel::<RunJobResponse>();
        self.jobs.lock().await.insert(job_id.to_owned(), tx);

        let res = self
            .tasks
            .send_task(
                scheduler_to_servers::run_job::new(job_id.to_owned(), toolchain, command, outputs)
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

    async fn has_job(&self, job_id: &str) -> bool {
        self.jobs.lock().await.contains_key(job_id)
    }

    async fn del_job(&self, job_id: &str) -> Result<()> {
        if self.has_job_inputs(job_id).await {
            let _ = self.del_job_inputs(job_id).await;
        }
        if self.has_job_result(job_id).await {
            let _ = self.del_job_result(job_id).await;
        }
        Ok(())
    }

    async fn job_finished(&self, job_id: &str, server: ServerDetails) -> Result<()> {
        if let Some(sndr) = self.jobs.lock().await.remove(job_id) {
            let job_result = self.get_job_result(job_id).await.unwrap_or_else(|_| {
                RunJobResponse::MissingJobResult {
                    server_id: server.id.clone(),
                }
            });
            let job_status = sndr.send(job_result).map_or_else(|_| false, |_| true);
            self.update_status(server, Some(job_status)).await
        } else {
            Err(anyhow!("Not my job"))
        }
    }

    async fn update_status(&self, details: ServerDetails, job_status: Option<bool>) -> Result<()> {
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

        fn duration_from_micros(us: u128) -> Duration {
            Duration::new(
                (us / 1_000_000) as u64, // MICROS_PER_SEC
                (us % 1_000_000) as u32, // MICROS_PER_SEC
            )
        }

        // Insert or update the server info
        servers
            .entry(details.id.clone())
            .and_modify(|server| {
                let t1 = server.u_time.duration_since(UNIX_EPOCH).unwrap();
                let t2 = duration_from_micros(details.created_at);
                if t2 >= t1 {
                    server.info = details.info.clone();
                    server.jobs = details.jobs.clone();
                    server.u_time = server.u_time.checked_add(t2 - t1).unwrap();
                }
            })
            .or_insert_with(|| ServerInfo {
                info: details.info.clone(),
                jobs: details.jobs.clone(),
                u_time: UNIX_EPOCH
                    .checked_add(duration_from_micros(details.created_at))
                    .unwrap(),
            });

        Self::prune_servers(&mut servers);

        Ok(())
    }
}

#[derive(Clone)]
struct ServerState {
    id: String,
    jobs: Arc<std::sync::Mutex<HashSet<String>>>,
    job_queue: Arc<tokio::sync::Semaphore>,
    job_stats: Arc<std::sync::Mutex<dist::JobStats>>,
    server_stats: ServerStats,
    sys: Arc<std::sync::Mutex<sysinfo::System>>,
}

impl From<&ServerState> for ServerDetails {
    fn from(state: &ServerState) -> Self {
        let (cpu_usage, mem_avail, mem_total) = {
            let mut sys = state.sys.lock().unwrap();
            sys.refresh_cpu_specifics(sysinfo::CpuRefreshKind::nothing().with_cpu_usage());
            sys.refresh_memory_specifics(sysinfo::MemoryRefreshKind::nothing().with_ram());
            (
                sys.global_cpu_usage(),
                sys.available_memory(),
                sys.total_memory(),
            )
        };

        // Record cpu_usage
        metrics::histogram!("sccache::server::cpu_usage_ratio").record(cpu_usage);
        // Record mem_avail
        metrics::histogram!("sccache::server::mem_avail_bytes").record(mem_avail as f64);
        // Record mem_total
        metrics::histogram!("sccache::server::mem_total_bytes").record(mem_total as f64);

        let ServerStats {
            num_cpus,
            occupancy,
            pre_fetch,
            ..
        } = state.server_stats;

        let dist::JobStats {
            accepted,
            finished,
            loading,
            ..
        } = *state.job_stats.lock().unwrap();

        let running = occupancy.saturating_sub(state.job_queue.available_permits());

        ServerDetails {
            id: state.id.to_owned(),
            info: dist::ServerStats {
                cpu_usage,
                mem_avail,
                mem_total,
                num_cpus,
                occupancy,
                pre_fetch,
            },
            jobs: dist::JobStats {
                loading,
                running,
                accepted,
                finished,
            },
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_micros(),
        }
    }
}

#[derive(Clone)]
pub struct Server {
    builder: Arc<dyn BuilderIncoming>,
    jobs_storage: Arc<dyn sccache::cache::Storage>,
    report_interval: Duration,
    server_id: String,
    state: ServerState,
    tasks: Arc<celery::Celery>,
    toolchains: AsyncMulticast<Toolchain, PathBuf>,
}

impl Server {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        builder: Arc<dyn BuilderIncoming>,
        job_queue: Arc<tokio::sync::Semaphore>,
        jobs_storage: Arc<dyn sccache::cache::Storage>,
        server_id: String,
        server_stats: dist::ServerStats,
        tasks: Arc<celery::Celery>,
        toolchains: AsyncMulticast<Toolchain, PathBuf>,
    ) -> Self {
        use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
        Self {
            builder,
            jobs_storage,
            // Report status every 1s
            report_interval: Duration::from_secs(1),
            server_id: server_id.clone(),
            tasks,
            toolchains,
            state: ServerState {
                id: server_id,
                jobs: Default::default(),
                job_queue,
                job_stats: Default::default(),
                server_stats,
                sys: Arc::new(std::sync::Mutex::new(System::new_with_specifics(
                    RefreshKind::nothing()
                        .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
                        .with_memory(MemoryRefreshKind::nothing().with_ram()),
                ))),
            },
        }
    }

    pub async fn start(&self) -> Result<()> {
        tokio::spawn({
            let this = self.clone();
            async move {
                loop {
                    tokio::time::sleep(
                        this.send_status()
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

    pub async fn close(&self) {
        let job_ids = self.state.jobs.lock().unwrap().drain().collect::<Vec<_>>();
        futures::future::join_all(job_ids.iter().map(|job_id| {
            self.job_failed(job_id, RunJobError::MissingJobResult)
                .boxed()
        }))
        .await;
    }

    async fn send_status(&self) -> Result<()> {
        self.tasks
            .send_task(server_to_schedulers::status_update::new(From::from(
                &self.state,
            )))
            .await
            .map_err(anyhow::Error::new)
            .map(|_| ())?;
        Ok(())
    }

    async fn load_job(
        &self,
        start: &Instant,
        job_id: &str,
        toolchain: Toolchain,
    ) -> std::result::Result<(PathBuf, Vec<u8>), RunJobError> {
        self.state.job_stats.lock().unwrap().loading += 1;

        // Broadcast status after accepting the job
        self.send_status().await?;

        // Load and unpack the toolchain
        let toolchain_dir = self
            .get_toolchain_dir(job_id, toolchain)
            .await
            .map_err(|_| RunJobError::MissingToolchain)?;

        // Load job inputs into memory
        let inputs = self
            .get_job_inputs(job_id)
            .await
            .map_err(|_| RunJobError::MissingJobInputs)?;

        // Record toolchain load time
        metrics::histogram!("sccache::server::job_fetched_time")
            .record(start.elapsed().as_secs_f64());

        metrics::counter!("sccache::server::job_fetched_count").increment(1);

        self.state.job_stats.lock().unwrap().loading -= 1;

        Ok((toolchain_dir, inputs))
    }

    async fn get_toolchain_dir(&self, job_id: &str, toolchain: Toolchain) -> Result<PathBuf> {
        // ServerToolchains retries internally, so no need to retry here
        self.toolchains.call(toolchain).await.map_err(|err| {
            // Record toolchain errors
            metrics::counter!("sccache::server::toolchain_error_count").increment(1);
            tracing::warn!("[run_job({job_id})]: Error loading toolchain: {err:?}");
            err
        })
    }

    async fn get_job_inputs(&self, job_id: &str) -> Result<Vec<u8>> {
        let start = Instant::now();
        let job_inputs_name = job_inputs_key(job_id);
        let res = retry_with_jitter(10, || async {
            let mut reader = self
                .jobs_storage
                .get_stream(&job_inputs_name)
                .await
                .map_err(|err| {
                    tracing::warn!("[get_job_inputs({job_id})]: Error loading stream: {err:?}");
                    RetryError::transient(err)
                })?;

            let mut inputs = vec![];
            reader.read_to_end(&mut inputs).await.map_err(|err| {
                tracing::warn!("[get_job_inputs({job_id})]: Error reading stream: {err:?}");
                RetryError::permanent(anyhow!(err))
            })?;

            Ok(inputs)
        })
        .await;

        // Record get_job_inputs load time after retrying
        metrics::histogram!("sccache::server::get_job_inputs_time")
            .record(start.elapsed().as_secs_f64());

        res.map_err(|err| {
            // Record get_job_inputs errors after retrying
            metrics::counter!("sccache::server::get_job_inputs_error_count").increment(1);
            tracing::warn!("[run_job({job_id})]: Error retrieving job inputs: {err:?}");
            err
        })
    }

    async fn run_job_build(
        &self,
        job_id: &str,
        toolchain_dir: PathBuf,
        inputs: Vec<u8>,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> Result<BuildResult> {
        let start = Instant::now();

        let res = self
            .builder
            .run_build(job_id, &toolchain_dir, inputs, command, outputs)
            .await;

        // Record build time
        metrics::histogram!("sccache::server::job_build_time")
            .record(start.elapsed().as_secs_f64());

        res.map_err(|err| {
            // Record run_build errors
            metrics::counter!("sccache::server::job_build_error_count").increment(1);
            tracing::warn!("[run_job({job_id})]: Build error: {err:?}");
            err
        })
    }

    async fn put_job_result(&self, job_id: &str, result: RunJobResponse) -> Result<()> {
        let start = Instant::now();
        let result = bincode_serialize(result).await.map_err(|err| {
            tracing::warn!("[put_job_result({job_id})]: Error serializing result: {err:?}");
            err
        })?;
        let job_result_size = result.len() as u64;
        let job_result_name = job_result_key(job_id);
        let res = retry_with_jitter(10, || async {
            let source = std::pin::pin!(futures::io::AllowStdIo::new(result.reader()));
            self.jobs_storage
                .put_stream(&job_result_name, job_result_size, source)
                .await
                .map_err(|err| {
                    tracing::warn!("[put_job_result({job_id})]: Error writing stream: {err:?}");
                    RetryError::transient(err)
                })
        })
        .await;

        // Record put_job_result load time after retrying
        metrics::histogram!("sccache::server::put_job_result_time")
            .record(start.elapsed().as_secs_f64());

        res.map_err(|err| {
            // Record put_job_result errors after retrying
            metrics::counter!("sccache::server::put_job_result_error_count").increment(1);
            tracing::warn!("[run_job({job_id})]: Error storing job result: {err:?}");
            err
        })
    }
}

#[async_trait]
impl ServerService for Server {
    async fn run_job(
        &self,
        job_id: &str,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> std::result::Result<(), RunJobError> {
        let start = Instant::now();
        // Add job and increment job_started count
        self.state.job_stats.lock().unwrap().accepted += 1;
        self.state.jobs.lock().unwrap().insert(job_id.to_owned());
        metrics::counter!("sccache::server::job_started_count").increment(1);

        let res = self
            // Load the job toolchain and inputs
            .load_job(&start, job_id, toolchain)
            // Run the build
            .and_then(|(toolchain_dir, inputs)| {
                self.run_job_build(job_id, toolchain_dir, inputs, command, outputs)
                    .map_err(|error| error.into())
                    .map_ok(|result| RunJobResponse::JobComplete {
                        result,
                        server_id: self.server_id.clone(),
                    })
            })
            // Store the job result for retrieval by a scheduler
            .and_then(|result| self.put_job_result(job_id, result).map_err(|e| e.into()))
            .await;

        // Record total run_job time
        metrics::histogram!("sccache::server::job_time").record(start.elapsed().as_secs_f64());

        res
    }

    async fn job_failed(&self, job_id: &str, job_err: RunJobError) -> Result<()> {
        let server_id = self.server_id.clone();
        let _ = self
            .put_job_result(
                job_id,
                match job_err {
                    RunJobError::MissingJobInputs => RunJobResponse::MissingJobInputs { server_id },
                    RunJobError::MissingJobResult => RunJobResponse::MissingJobResult { server_id },
                    RunJobError::MissingToolchain => RunJobResponse::MissingToolchain { server_id },
                    RunJobError::Err(e) => RunJobResponse::JobFailed {
                        reason: format!("{e:#}"),
                        server_id,
                    },
                },
            )
            .await;
        self.job_finished(job_id).await
    }

    async fn job_finished(&self, job_id: &str) -> Result<()> {
        // Remove job and increment job_finished count
        self.state.jobs.lock().unwrap().remove(job_id);
        self.state.job_stats.lock().unwrap().finished += 1;
        metrics::counter!("sccache::server::job_finished_count").increment(1);
        self.tasks
            .send_task(server_to_schedulers::job_finished::new(
                job_id.to_owned(),
                From::from(&self.state),
            ))
            .await
            .map_err(anyhow::Error::new)
            .map(|_| ())
    }
}

// Sent by schedulers, runs on servers
mod scheduler_to_servers {

    use celery::prelude::*;
    use sccache::dist::{CompileCommand, RunJobError, Toolchain};
    use sccache::errors::*;

    #[celery::task(
        acks_late = true,
        max_retries = 10,
        on_failure = on_run_job_failure,
        on_success = on_run_job_success,
    )]
    pub async fn run_job(
        job_id: String,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> TaskResult<()> {
        tracing::debug!(
            "[run_job({job_id}, {}, {:?}, {:?}, {outputs:?})]",
            toolchain.archive_id,
            command.executable,
            command.arguments,
        );

        let server = match super::SERVER.get() {
            Some(server) => server,
            None => {
                tracing::debug!("sccache-dist server is not initialized");
                return Err(TaskError::ExpectedError(
                    "sccache-dist server is not initialized".into(),
                ));
            }
        };

        server
            .run_job(&job_id, toolchain, command, outputs)
            .await
            .map_err(|e| match e {
                RunJobError::MissingJobInputs => {
                    TaskError::ExpectedError("MissingJobInputs".into())
                }
                RunJobError::MissingJobResult => {
                    TaskError::ExpectedError("MissingJobResult".into())
                }
                RunJobError::MissingToolchain => {
                    TaskError::ExpectedError("MissingToolchain".into())
                }
                RunJobError::Err(e) => {
                    tracing::debug!("[run_job({job_id})]: Failed with expected error: {e:#}");
                    TaskError::ExpectedError(format!(
                        "Job {job_id} failed with expected error: {e:#}"
                    ))
                }
            })
    }

    async fn on_run_job_failure(task: &run_job, err: &TaskError) {
        let server = match super::SERVER.get() {
            Some(server) => server,
            None => {
                tracing::error!("sccache-dist server is not initialized: {err:#}");
                return;
            }
        };

        let job_id = &task.request().params.job_id;

        let job_err = {
            if let TaskError::UnexpectedError(msg) = err {
                // Never retry unexpected errors
                Err(RunJobError::Err(anyhow!(msg.to_owned())))
            } else if let TaskError::ExpectedError(msg) = err {
                // Don't retry errors due to missing inputs, result, or toolchain
                // Notify the client so they can be retried or compiled locally.
                // Matching strings because that's the only data in TaskError.
                match msg.as_ref() {
                    "MissingJobInputs" => Err(RunJobError::MissingJobInputs),
                    "MissingJobResult" => Err(RunJobError::MissingJobResult),
                    "MissingToolchain" => Err(RunJobError::MissingToolchain),
                    // Maybe retry other errors
                    _ => Ok(RunJobError::Err(anyhow!(msg.to_owned()))),
                }
            } else if let TaskError::Retry(_) = err {
                // Maybe retry TaskError::Retry
                Ok(RunJobError::Err(anyhow!(
                    "Job {job_id} exceeded the retry limit"
                )))
            } else {
                // Maybe retry TaskError::TimeoutError
                Ok(RunJobError::Err(anyhow!("Job {job_id} timed out")))
            }
        };

        let job_err = match job_err {
            Ok(job_err) => {
                if task.request().retries < task.options().max_retries.unwrap_or(0) {
                    return;
                }
                job_err
            }
            Err(job_err) => job_err,
        };

        tracing::debug!("[run_job({job_id})]: Failed with error: {job_err:#}");

        if let Err(err) = server.job_failed(job_id, job_err).await {
            tracing::error!("[on_run_job_failure({job_id})]: Error reporting job failure: {err:#}");
        }
    }

    async fn on_run_job_success(task: &run_job, _: &()) {
        let server = match super::SERVER.get() {
            Some(server) => server,
            None => {
                tracing::error!("sccache-dist server is not initialized");
                return;
            }
        };
        let job_id = &task.request().params.job_id;

        if let Err(err) = server.job_finished(job_id).await {
            tracing::error!("[on_run_job_success({job_id})]: Error reporting job success: {err:#}");
        }
    }
}

// Sent by servers, runs on schedulers
mod server_to_schedulers {
    use celery::prelude::*;

    use sccache::dist::ServerDetails;

    // Runs on scheduler to handle heartbeats from servers
    #[celery::task(max_retries = 0)]
    pub async fn status_update(server: ServerDetails) -> TaskResult<()> {
        let scheduler = match super::SCHEDULER.get() {
            Some(scheduler) => scheduler,
            None => {
                tracing::error!("sccache-dist scheduler is not initialized");
                return Err(TaskError::UnexpectedError(
                    "sccache-dist scheduler is not initialized".into(),
                ));
            }
        };

        scheduler.update_status(server, None).await.map_err(|e| {
            TaskError::UnexpectedError(format!(
                "receive_status failed with unexpected error: {e:#}"
            ))
        })
    }

    // Retry the task until it lands on the scheduler that owns the job.
    //
    // While a less efficient, this avoids needing to create scheduler-specific
    // queues that live on in the message broker after a scheduler instance is
    // torn down. Ideally rust-celery would provide a mechanism for destroying
    // queues on shutdown.
    #[celery::task(acks_late = true, max_retries = 500)]
    pub async fn job_finished(job_id: String, server: ServerDetails) -> TaskResult<()> {
        let scheduler = match super::SCHEDULER.get() {
            Some(scheduler) => scheduler,
            None => {
                tracing::debug!("sccache-dist scheduler is not initialized");
                return Err(TaskError::ExpectedError(
                    "sccache-dist scheduler is not initialized".into(),
                ));
            }
        };

        scheduler
            .job_finished(&job_id, server)
            .await
            .map_err(|_| TaskError::ExpectedError("Unknown job".into()))
    }
}
