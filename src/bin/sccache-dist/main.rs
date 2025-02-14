use futures::FutureExt;
use sccache::dist::metrics::Metrics;
use sccache::{cache::cache::storage_from_config, config::server::BuilderType};
use scheduler::{SchedulerMetrics, SchedulerTasks};

use std::env;
use std::sync::Arc;
use std::time::Duration;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg_attr(target_os = "freebsd", path = "build_freebsd.rs")]
mod build;

mod cmdline;
use cmdline::Command;
mod scheduler;
mod server;
mod tasks;

use sccache::{
    config::{scheduler as scheduler_config, server as server_config},
    dist::{self, BuilderIncoming, ServerToolchains},
    errors::*,
};

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

fn run(command: Command) -> Result<()> {
    let num_cpus = std::thread::available_parallelism()?.get();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
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
                    let metrics = Metrics::new(
                        metrics,
                        vec![
                            ("env".into(), env_info()),
                            ("type".into(), "scheduler".into()),
                            ("scheduler_id".into(), scheduler_id.clone()),
                        ],
                    )?;

                    let jobs_storage = storage_from_config(&jobs.storage, &jobs.fallback)
                        .context("Failed to initialize jobs storage")?;

                    // Verify read/write access to jobs storage
                    match jobs_storage.check().await {
                        Ok(sccache::cache::CacheMode::ReadWrite) => {}
                        _ => {
                            bail!("Scheduler jobs storage must be read/write")
                        }
                    }

                    let toolchains_storage =
                        storage_from_config(&toolchains.storage, &toolchains.fallback)
                            .context("Failed to initialize toolchain storage")?;

                    // Verify read/write access to toolchain storage
                    match toolchains_storage.check().await {
                        Ok(sccache::cache::CacheMode::ReadWrite) => {}
                        _ => {
                            bail!("Scheduler toolchain storage must be read/write")
                        }
                    }

                    let scheduler = scheduler::Scheduler::new(
                        jobs_storage,
                        SchedulerMetrics::new(metrics.clone()),
                        &scheduler_id,
                        tasks::Tasks::scheduler(
                            &scheduler_id,
                            &to_scheduler_queue(&scheduler_id),
                            100 * num_cpus as u16,
                            message_broker,
                        )
                        .await?
                        .set_job_time_limit(job_time_limit),
                        toolchains_storage,
                    )?;

                    let server = dist::server::Scheduler::new(
                        scheduler.clone(),
                        client_auth.into(),
                    )
                    .serve(public_addr, max_body_size, metrics);

                    let celery = scheduler.start();

                    futures::select_biased! {
                        res = celery.fuse() => res?,
                        res = server.fuse() => res?,
                    };

                    scheduler.close().await?;

                    Ok(())
                }

                Command::Server(server_config::Config {
                    message_broker,
                    builder,
                    cache_dir,
                    heartbeat_interval_ms,
                    jobs,
                    max_per_core_load,
                    max_per_core_prefetch,
                    metrics,
                    server_id,
                    toolchain_cache_size,
                    toolchains,
                }) => {
                    let metrics = Metrics::new(
                        metrics,
                        vec![
                            ("env".into(), env_info()),
                            ("type".into(), "server".into()),
                            ("server_id".into(), server_id.clone()),
                        ],
                    )?;

                    let jobs_storage = storage_from_config(&jobs.storage, &jobs.fallback)
                        .context("Failed to initialize jobs storage")?;

                    // Verify read/write access to jobs storage
                    match jobs_storage.check().await {
                        Ok(sccache::cache::CacheMode::ReadWrite) => {}
                        _ => {
                            bail!("Server jobs storage must be read/write")
                        }
                    }

                    let toolchains_storage =
                        storage_from_config(&toolchains.storage, &toolchains.fallback)
                            .context("Failed to initialize toolchain storage")?;

                    // Verify toolchain storage
                    toolchains_storage
                        .check()
                        .await
                        .context("Failed to initialize toolchain storage")?;

                    let occupancy = (num_cpus as f64 * max_per_core_load.max(0.0))
                        .floor()
                        .max(1.0) as usize;

                    let pre_fetch = (num_cpus as f64 * max_per_core_prefetch.max(0.0))
                        .floor()
                        .max(0.0) as usize;

                    let job_queue = Arc::new(tokio::sync::Semaphore::new(occupancy));

                    let server = server::Server::new(
                        init_builder(builder, job_queue.clone()).await?,
                        jobs_storage,
                        server::ServerState {
                            id: server_id.clone(),
                            job_queue,
                            metrics: server::ServerMetrics::new(metrics.clone()),
                            num_cpus,
                            occupancy,
                            pre_fetch,
                            ..Default::default()
                        },
                        tasks::Tasks::server(
                            &server_id,
                            &scheduler_to_servers_queue(),
                            (occupancy as u16).saturating_add(pre_fetch as u16),
                            message_broker,
                        )
                        .await?,
                        ServerToolchains::new(
                            cache_dir.join("tc"),
                            toolchain_cache_size,
                            toolchains_storage,
                            metrics.clone(),
                        ),
                    )?;

                    // Report status every `heartbeat_interval_ms` milliseconds
                    server
                        .start(Duration::from_millis(heartbeat_interval_ms))
                        .await
                }
            }
        })
}

pub(crate) fn job_inputs_key(job_id: &str) -> String {
    format!("{job_id}-inputs")
}

pub(crate) fn job_result_key(job_id: &str) -> String {
    format!("{job_id}-result")
}

pub(crate) fn scheduler_to_servers_queue() -> String {
    queue_name_with_env_info("scheduler-to-servers")
}

pub(crate) fn server_to_schedulers_queue() -> String {
    queue_name_with_env_info("server-to-schedulers")
}

pub(crate) fn to_scheduler_queue(id: &str) -> String {
    queue_name_with_env_info(&format!("scheduler-{id}"))
}

fn queue_name_with_env_info(prefix: &str) -> String {
    format!("{prefix}-{}", env_info())
}

fn env_info() -> String {
    let arch = std::env::var("SCCACHE_DIST_ARCH").unwrap_or(std::env::consts::ARCH.to_owned());
    let os = std::env::var("SCCACHE_DIST_OS").unwrap_or(std::env::consts::OS.to_owned());
    format!("{os}-{arch}")
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

async fn init_builder(
    config: BuilderType,
    job_queue: Arc<tokio::sync::Semaphore>,
) -> Result<Arc<dyn BuilderIncoming>> {
    match config {
        #[cfg(not(target_os = "freebsd"))]
        BuilderType::Docker => Ok(Arc::new(
            build::DockerBuilder::new(job_queue.clone())
                .await
                .context("Docker builder failed to start")?,
        )),
        #[cfg(not(target_os = "freebsd"))]
        BuilderType::Overlay {
            bwrap_path,
            build_dir,
        } => Ok(Arc::new(
            build::OverlayBuilder::new(bwrap_path, build_dir, job_queue.clone())
                .await
                .context("Overlay builder failed to start")?,
        )),
        #[cfg(target_os = "freebsd")]
        BuilderType::Pot {
            pot_fs_root,
            clone_from,
            pot_cmd,
            pot_clone_args,
        } => Ok(Arc::new(
            build::PotBuilder::new(
                pot_fs_root,
                clone_from,
                pot_cmd,
                pot_clone_args,
                job_queue.clone(),
            )
            .await
            .context("Pot builder failed to start")?,
        )),
        _ => bail!(
            "Builder type `{}` not supported on this platform",
            format!("{:?}", config)
                .split_whitespace()
                .next()
                .unwrap_or("")
        ),
    }
}
