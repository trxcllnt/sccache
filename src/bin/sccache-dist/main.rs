use async_trait::async_trait;

use celery::prelude::*;
use celery::protocol::MessageContentType;
use futures::lock::Mutex;
use futures::FutureExt;
use sccache::config::{
    scheduler as scheduler_config, server as server_config, MessageBroker,
    INSECURE_DIST_CLIENT_TOKEN,
};

use sccache::dist::{
    self, BuildResult, CompileCommand, NewJobRequest, NewJobResponse, RunJobRequest,
    RunJobResponse, SchedulerService, SchedulerStatusResult, ServerService, SubmitToolchainResult,
    Toolchain,
};
use sccache::util::daemonize;
use std::collections::HashMap;
use std::env;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::OnceCell;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg_attr(target_os = "freebsd", path = "build_freebsd.rs")]
// #[cfg_attr(not(target_os = "freebsd"), path = "build_freebsd.rs")]
mod build;

mod cmdline;
use cmdline::Command;
mod token_check;

use crate::dist::ServerToolchains;
use sccache::errors::*;

static SERVER: OnceCell<Box<dyn crate::dist::ServerService>> = OnceCell::const_new();
static SCHEDULER: OnceCell<Arc<dyn crate::dist::SchedulerService>> = OnceCell::const_new();

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
                println!("sccache: increment compilation is  prohibited.");
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

fn run(command: Command) -> Result<()> {
    let num_cpus = std::thread::available_parallelism()?.get();

    match command {
        Command::Scheduler(scheduler_config::Config {
            enable_web_socket_server,
            client_auth,
            job_time_limit,
            max_body_size,
            message_broker,
            public_addr,
            toolchains_fallback,
            toolchains,
        }) => {
            let broker_uri =
                match message_broker.expect("Missing required message broker configuration") {
                    MessageBroker::AMQP(uri) => uri,
                    MessageBroker::Redis(uri) => uri,
                };

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
                let scheduler_id = format!(
                    "sccache-dist-scheduler-{}",
                    uuid::Uuid::new_v4().to_u128_le()
                );

                let task_queue = Arc::new(
                    celery::CeleryBuilder::new("sccache-dist", &broker_uri)
                        .default_queue(&scheduler_id)
                        .task_content_type(MessageContentType::MsgPack)
                        .task_route("scheduler_build_failed", &scheduler_id)
                        .task_route("scheduler_build_success", &scheduler_id)
                        .task_route("server_run_build", "sccache-dist-server")
                        .prefetch_count(100 * num_cpus as u16)
                        .heartbeat(Some(10))
                        .acks_late(true)
                        .acks_on_failure_or_timeout(false)
                        .nacks_enabled(true)
                        .build()
                        .await
                        .unwrap(),
                );

                task_queue
                    .register_task::<scheduler_build_failed>()
                    .await
                    .unwrap();

                task_queue
                    .register_task::<scheduler_build_success>()
                    .await
                    .unwrap();

                let scheduler = Arc::new(Scheduler::new(
                    job_time_limit,
                    scheduler_id.clone(),
                    task_queue.clone(),
                    toolchain_storage,
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

                task_queue.display_pretty().await;

                daemonize()?;

                let cancel = tokio::signal::ctrl_c();
                let celery = task_queue.consume();
                let server = server.serve(public_addr, enable_web_socket_server, max_body_size);

                futures::select! {
                    res = cancel.fuse() => res?,
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
            max_per_core_load,
            num_cpus_to_ignore,
            toolchain_cache_size,
            toolchains,
            toolchains_fallback,
        }) => {
            let num_cpus = (num_cpus - num_cpus_to_ignore).max(1) as f64;
            let num_cpus = (num_cpus * max_per_core_load).floor().max(1f64) as u16;

            let broker_uri =
                match message_broker.expect("Missing required message broker configuration") {
                    MessageBroker::AMQP(uri) => uri,
                    MessageBroker::Redis(uri) => uri,
                };

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            let toolchain_base_dir = cache_dir.join("tc");

            let toolchain_storage = sccache::cache::cache::storage_from_config(
                &toolchains,
                &toolchains_fallback,
                runtime.handle(),
            )
            .context("Failed to initialize toolchain storage")?;

            let toolchains_disk_cache = Arc::new(Mutex::new(ServerToolchains::new(
                &toolchain_base_dir,
                toolchain_cache_size,
                toolchain_storage,
            )));

            runtime.block_on(async move {
                let builder: Box<dyn dist::BuilderIncoming> = match builder {
                    #[cfg(not(target_os = "freebsd"))]
                    // #[cfg(target_os = "freebsd")]
                    sccache::config::server::BuilderType::Docker => Box::new(
                        build::DockerBuilder::new(&toolchain_base_dir)
                            .await
                            .context("Docker builder failed to start")?,
                    ),
                    #[cfg(not(target_os = "freebsd"))]
                    // #[cfg(target_os = "freebsd")]
                    sccache::config::server::BuilderType::Overlay {
                        bwrap_path,
                        build_dir,
                    } => Box::new(
                        build::OverlayBuilder::new(bwrap_path, build_dir)
                            .await
                            .context("Overlay builder failed to start")?,
                    ),
                    #[cfg(target_os = "freebsd")]
                    // #[cfg(not(target_os = "freebsd"))]
                    sccache::config::server::BuilderType::Pot {
                        pot_fs_root,
                        clone_from,
                        pot_cmd,
                        pot_clone_args,
                    } => Box::new(
                        build::PotBuilder::new(
                            &toolchain_base_dir,
                            pot_fs_root,
                            clone_from,
                            pot_cmd,
                            pot_clone_args,
                        )
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

                let task_queue = Arc::new(
                    celery::CeleryBuilder::new("sccache-dist", &broker_uri)
                        .default_queue("sccache-dist-server")
                        .task_content_type(MessageContentType::MsgPack)
                        .prefetch_count(num_cpus)
                        .heartbeat(Some(10))
                        .acks_late(true)
                        .acks_on_failure_or_timeout(false)
                        .nacks_enabled(true)
                        .build()
                        .await?,
                );

                task_queue.register_task::<server_run_build>().await?;

                SERVER
                    .set(Box::new(Server::new(
                        builder,
                        task_queue.clone(),
                        toolchains_disk_cache,
                    )))
                    .map_err(|err| anyhow!("{err}"))?;

                tracing::debug!(
                    "sccache: Server initialized to run {num_cpus} parallel build jobs"
                );

                task_queue.display_pretty().await;

                daemonize()?;

                task_queue.consume().await?;

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
    id: String,
    job_time_limit: u32,
    job_result: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<RunJobResponse>>>>,
    task_queue: Arc<celery::Celery>,
    toolchains: Arc<dyn sccache::cache::Storage>,
}

impl Scheduler {
    pub fn new(
        job_time_limit: u32,
        scheduler_id: String,
        task_queue: Arc<celery::Celery>,
        toolchains: Arc<dyn sccache::cache::Storage>,
    ) -> Self {
        Self {
            id: scheduler_id,
            job_time_limit,
            job_result: Arc::new(Mutex::new(HashMap::new())),
            task_queue,
            toolchains,
        }
    }
}

#[async_trait]
impl SchedulerService for Scheduler {
    async fn get_status(&self) -> Result<SchedulerStatusResult> {
        // TODO
        Ok(SchedulerStatusResult {
            num_cpus: 0,             // servers.values().map(|v| v.num_cpus).sum(),
            num_servers: 0,          // servers.len(),
            assigned: 0,             // assigned_jobs,
            active: 0,               // active_jobs,
            servers: HashMap::new(), // servers_map,
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
            job_id: uuid::Uuid::new_v4().to_string(),
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
        self.job_result.lock().await.insert(job_id.clone(), tx);

        let res = self
            .task_queue
            .send_task(
                server_run_build::new(
                    job_id.clone(),
                    self.id.clone(),
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
            self.job_result.lock().await.remove(&job_id);
            Err(err)
        } else {
            rx.await.map_err(anyhow::Error::new)
        }
    }

    async fn job_failure(&self, job_id: &str, reason: &str) -> Result<()> {
        if let Some(sndr) = self.job_result.lock().await.remove(job_id) {
            sndr.send(RunJobResponse::JobFailed {
                reason: reason.to_owned(),
            })
            .map_err(|_| anyhow!("Failed to send job result"))
        } else {
            Err(anyhow!(
                "[job_failed({job_id})]: Failed to send response for unknown job"
            ))
        }
    }

    async fn job_success(&self, job_id: &str, result: BuildResult) -> Result<()> {
        if let Some(sndr) = self.job_result.lock().await.remove(job_id) {
            sndr.send(RunJobResponse::JobComplete { result })
                .map_err(|_| anyhow!("Failed to send job result"))
        } else {
            Err(anyhow!(
                "[job_complete({job_id})]: Failed to send response for unknown job"
            ))
        }
    }
}

pub struct Server {
    builder: Box<dyn crate::dist::BuilderIncoming>,
    jobs: Arc<Mutex<HashMap<String, (String, String)>>>,
    task_queue: Arc<celery::Celery>,
    toolchains: Arc<Mutex<ServerToolchains>>,
}

impl Server {
    pub fn new(
        builder: Box<dyn crate::dist::BuilderIncoming>,
        task_queue: Arc<celery::Celery>,
        toolchains: Arc<Mutex<ServerToolchains>>,
    ) -> Self {
        Self {
            builder,
            jobs: Default::default(),
            task_queue,
            toolchains,
        }
    }
}

#[async_trait]
impl ServerService for Server {
    async fn run_job(
        &self,
        task_id: &str,
        job_id: &str,
        scheduler_id: &str,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs: Vec<u8>,
    ) -> Result<BuildResult> {
        // Associate the task with the scheduler and job so we can report success or failure
        self.jobs.lock().await.insert(
            task_id.to_owned(),
            (scheduler_id.to_owned(), job_id.to_owned()),
        );

        let tc_dir = self.toolchains.lock().await.acquire(&toolchain).await?;

        let result = self
            .builder
            .run_build(job_id, &tc_dir, command, outputs, inputs)
            .await;

        self.toolchains.lock().await.release(&toolchain).await?;

        result
    }

    async fn job_failure(&self, task_id: &str, reason: &str) -> Result<()> {
        if let Some((scheduler_id, job_id)) = self.jobs.lock().await.remove(task_id) {
            return self
                .task_queue
                .send_task(
                    scheduler_build_failed::new(job_id, reason.to_owned())
                        .with_queue(&scheduler_id),
                )
                .await
                .map_err(anyhow::Error::new)
                .map(|_| ());
        }
        Err(anyhow!(
            "[job_failed({task_id})]: Failed to respond to scheduler, unknown job_id"
        ))
    }

    async fn job_success(&self, task_id: &str, result: &BuildResult) -> Result<()> {
        if let Some((scheduler_id, job_id)) = self.jobs.lock().await.remove(task_id) {
            return self
                .task_queue
                .send_task(
                    scheduler_build_success::new(job_id, result.to_owned())
                        .with_queue(&scheduler_id),
                )
                .await
                .map_err(anyhow::Error::new)
                .map(|_| ());
        }
        Err(anyhow!(
            "[job_complete({task_id})]: Failed to respond to scheduler, unknown job_id"
        ))
    }
}

// Runs on the server
#[celery::task(
    bind = true,
    on_failure = on_server_run_build_failure,
    on_success = on_server_run_build_success,
)]
pub async fn server_run_build(
    task: &Self,
    job_id: String,
    scheduler_id: String,
    toolchain: Toolchain,
    command: CompileCommand,
    outputs: Vec<String>,
    inputs: Vec<u8>,
) -> TaskResult<BuildResult> {
    let task_id = task.request.id.clone();

    tracing::debug!(
        "server_run_build: job_id={}, task_id={}, scheduler_id={}, toolchain={}, command={:?}, outputs={:?}",
        job_id,
        task_id,
        scheduler_id,
        toolchain.archive_id,
        command,
        outputs
    );

    if let Some(server) = SERVER.get() {
        let job_id1 = job_id.clone();
        // let scheduler_id1 = scheduler_id.clone();
        tokio::spawn(async move {
            server
                .run_job(
                    &task_id,
                    &job_id1,
                    &scheduler_id,
                    toolchain,
                    command,
                    outputs,
                    inputs,
                )
                .await
                .map_err(|e| {
                    tracing::error!("[server_run_build({job_id1})]: run_job failed with: {e:?}");
                    TaskError::UnexpectedError(e.to_string())
                })
        })
        .await
        .map_err(|e| {
            tracing::error!("[server_run_build({job_id})]: run_job failed with: {e:?}");
            TaskError::UnexpectedError(e.to_string())
        })?
    } else {
        Err(TaskError::UnexpectedError(
            "sccache-dist server is not initialized".into(),
        ))
    }
}

async fn on_server_run_build_failure(task: &server_run_build, err: &TaskError) {
    let task_id = task.request().id.clone();
    if let Err(err) = SERVER
        .get()
        .unwrap()
        .job_failure(
            &task_id,
            &match err {
                TaskError::TimeoutError => {
                    format!("[server_run_build({task_id})]: Timed out")
                }
                _ => {
                    format!("[server_run_build({task_id})]: Failed with `{err}`")
                }
            },
        )
        .await
    {
        tracing::error!("[on_server_run_build_failure({task_id})]: {err}");
    }
}

async fn on_server_run_build_success(task: &server_run_build, result: &BuildResult) {
    let task_id = task.request().id.clone();
    if let Err(err) = SERVER.get().unwrap().job_success(&task_id, result).await {
        tracing::error!("[on_server_run_build_success({task_id})]: {err}");
    }
}

// Runs on the scheduler
#[celery::task]
async fn scheduler_build_failed(job_id: String, reason: String) -> TaskResult<()> {
    SCHEDULER
        .get()
        .unwrap()
        .job_failure(&job_id, &reason)
        .await
        .map_err(|e| TaskError::UnexpectedError(e.to_string()))
}

// Runs on the scheduler
#[celery::task]
async fn scheduler_build_success(job_id: String, result: BuildResult) -> TaskResult<()> {
    SCHEDULER
        .get()
        .unwrap()
        .job_success(&job_id, result)
        .await
        .map_err(|e| TaskError::UnexpectedError(e.to_string()))
}
