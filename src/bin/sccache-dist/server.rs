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

use async_trait::async_trait;

use bytes::Buf;
use celery::{error::CeleryError, task::AsyncResult};
use futures::{AsyncReadExt, FutureExt};

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sccache::{
    cache::Storage,
    dist::{
        self,
        http::{bincode_serialize, retry_with_jitter, AsyncMulticast, AsyncMulticastFn},
        metrics::{Action, Metrics},
        BuildResult, BuilderIncoming, CompileCommand, RunJobError, RunJobResponse, ServerDetails,
        ServerService, ServerToolchains, Toolchain,
    },
    errors::*,
    util::daemonize,
};

use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

use tokio_retry2::RetryError;

use crate::{job_inputs_key, job_result_key};

const CPU_USAGE_RATIO: &str = "sccache::server::cpu_usage_ratio";
const MEM_AVAIL_BYTES: &str = "sccache::server::mem_avail_bytes";
const MEM_TOTAL_BYTES: &str = "sccache::server::mem_total_bytes";
const TOOLCHAIN_ERROR_COUNT: &str = "sccache::server::toolchain_error_count";
const GET_JOB_INPUTS_ERROR_COUNT: &str = "sccache::server::get_job_inputs_error_count";
const JOB_BUILD_ERROR_COUNT: &str = "sccache::server::job_build_error_count";
const PUT_JOB_RESULT_ERROR_COUNT: &str = "sccache::server::put_job_result_error_count";
const JOB_STARTED_COUNT: &str = "sccache::server::job_started_count";
const JOB_FINISHED_COUNT: &str = "sccache::server::job_finished_count";
const GET_JOB_INPUTS_TIME: &str = "sccache::server::get_job_inputs_time";
const PUT_JOB_RESULT_TIME: &str = "sccache::server::put_job_result_time";
const LOAD_JOB_TIME: &str = "sccache::server::load_job_time";
const RUN_BUILD_TIME: &str = "sccache::server::run_build_time";
const RUN_JOB_TIME: &str = "sccache::server::run_job_time";

#[derive(Clone, Default)]
pub struct ServerMetrics {
    metrics: Metrics,
    sysinfo: Arc<std::sync::Mutex<sysinfo::System>>,
}

impl ServerMetrics {
    pub fn new(metrics: Metrics) -> Self {
        Self {
            metrics,
            sysinfo: Arc::new(std::sync::Mutex::new(System::new_with_specifics(
                RefreshKind::nothing()
                    .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
                    .with_memory(MemoryRefreshKind::nothing().with_ram()),
            ))),
        }
    }

    pub fn system_metrics(&self) -> (f32, u64, u64) {
        let mut sys = self.sysinfo.lock().unwrap();
        sys.refresh_cpu_specifics(sysinfo::CpuRefreshKind::nothing().with_cpu_usage());
        sys.refresh_memory_specifics(sysinfo::MemoryRefreshKind::nothing().with_ram());
        let cpu_usage = sys.global_cpu_usage();
        let mem_avail = sys.available_memory();
        let mem_total = sys.total_memory();
        metrics::histogram!(CPU_USAGE_RATIO).record(cpu_usage);
        metrics::histogram!(MEM_AVAIL_BYTES).record(mem_avail as f64);
        metrics::histogram!(MEM_TOTAL_BYTES).record(mem_total as f64);
        (cpu_usage, mem_avail, mem_total)
    }

    pub fn inc_toolchain_error_count(&self) {
        metrics::counter!(TOOLCHAIN_ERROR_COUNT).increment(1);
    }

    pub fn inc_get_job_inputs_error_count(&self) {
        metrics::counter!(GET_JOB_INPUTS_ERROR_COUNT).increment(1);
    }

    pub fn inc_job_build_error_count(&self) {
        metrics::counter!(JOB_BUILD_ERROR_COUNT).increment(1);
    }

    pub fn inc_put_job_result_error_count(&self) {
        metrics::counter!(PUT_JOB_RESULT_ERROR_COUNT).increment(1);
    }

    pub fn inc_job_started_count(&self) {
        metrics::counter!(JOB_STARTED_COUNT).increment(1);
    }

    pub fn inc_job_finished_count(&self) {
        metrics::counter!(JOB_FINISHED_COUNT).increment(1);
    }

    pub async fn get_job_inputs<F>(&self, func: F) -> F::Result
    where
        F: Action,
    {
        self.metrics.histogram(GET_JOB_INPUTS_TIME, &[], func).await
    }

    pub async fn put_job_result<F>(&self, func: F) -> F::Result
    where
        F: Action,
    {
        self.metrics.histogram(PUT_JOB_RESULT_TIME, &[], func).await
    }

    pub async fn load_job<F>(&self, func: F) -> F::Result
    where
        F: Action,
    {
        self.metrics.histogram(LOAD_JOB_TIME, &[], func).await
    }

    pub async fn run_build<F>(&self, func: F) -> F::Result
    where
        F: Action,
    {
        self.metrics.histogram(RUN_BUILD_TIME, &[], func).await
    }

    pub async fn run_job<F>(&self, func: F) -> F::Result
    where
        F: Action,
    {
        self.metrics.histogram(RUN_JOB_TIME, &[], func).await
    }
}

#[async_trait]
pub trait ServerTasks: Send + Sync {
    fn set_server(server: Arc<dyn ServerService>) -> Result<()>
    where
        Self: Sized;

    fn app(&self) -> &Arc<celery::Celery>;

    async fn status_update(
        &self,
        server: ServerDetails,
    ) -> std::result::Result<AsyncResult, CeleryError>;

    async fn job_finished(
        &self,
        job_id: String,
        server: ServerDetails,
    ) -> std::result::Result<AsyncResult, CeleryError>;
}

#[derive(Clone)]
pub struct ServerState {
    pub id: String,
    pub jobs: Arc<std::sync::Mutex<HashSet<String>>>,
    pub job_queue: Arc<tokio::sync::Semaphore>,
    pub job_stats: Arc<std::sync::Mutex<dist::JobStats>>,
    pub metrics: ServerMetrics,
    pub num_cpus: usize,
    pub occupancy: usize,
    pub pre_fetch: usize,
}

impl Default for ServerState {
    fn default() -> Self {
        Self {
            id: Default::default(),
            job_queue: Arc::new(tokio::sync::Semaphore::new(1)),
            jobs: Default::default(),
            job_stats: Default::default(),
            metrics: Default::default(),
            num_cpus: 1,
            occupancy: 1,
            pre_fetch: 0,
        }
    }
}

impl From<&ServerState> for ServerDetails {
    fn from(state: &ServerState) -> Self {
        let (cpu_usage, mem_avail, mem_total) = state.metrics.system_metrics();

        let dist::JobStats {
            accepted,
            finished,
            loading,
            ..
        } = *state.job_stats.lock().unwrap();

        let running = state
            .occupancy
            .saturating_sub(state.job_queue.available_permits());

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_micros();

        ServerDetails {
            id: state.id.to_owned(),
            info: dist::ServerStats {
                cpu_usage,
                mem_avail,
                mem_total,
                num_cpus: state.num_cpus,
                occupancy: state.occupancy,
                pre_fetch: state.pre_fetch,
            },
            jobs: dist::JobStats {
                loading,
                running,
                accepted,
                finished,
            },
            // Work around serde missing u128 type...
            // u128 works fine if I use rusty-celery's macros,
            // but not if I implement the Task trait directly.
            // The only difference I see is they use `celery::export::Deserialize`
            // instead of `serde::Deserialize`, but that does not solve the issue.
            created_at: (
                // microseconds as seconds
                created_at.saturating_div(1_000_000) as u64,
                // remainder microseconds as nanoseconds
                (created_at % 1_000_000).saturating_mul(1_000) as u32,
            ),
        }
    }
}

struct LoadToolchainFn {
    toolchains: ServerToolchains,
}

#[async_trait]
impl AsyncMulticastFn<'_, Toolchain, PathBuf> for LoadToolchainFn {
    async fn call(&self, tc: &Toolchain) -> Result<PathBuf> {
        self.toolchains.load(tc).await
    }
}

#[derive(Clone)]
pub struct Server {
    builder: Arc<dyn BuilderIncoming>,
    jobs_storage: Arc<dyn Storage>,
    state: ServerState,
    tasks: Arc<dyn ServerTasks>,
    toolchains: AsyncMulticast<Toolchain, PathBuf>,
}

impl Server {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        builder: Arc<dyn BuilderIncoming>,
        jobs_storage: Arc<dyn Storage>,
        state: ServerState,
        tasks: impl ServerTasks + 'static,
        toolchains: ServerToolchains,
    ) -> Result<Arc<Self>> {
        let this = Arc::new(Self {
            builder,
            jobs_storage,
            state,
            tasks: Arc::new(tasks),
            toolchains: AsyncMulticast::new(LoadToolchainFn { toolchains }),
        });

        crate::tasks::Tasks::set_server(this.clone())?;

        Ok(this)
    }

    pub async fn start(&self, report_interval: Duration) -> Result<()> {
        self.tasks.app().display_pretty().await;
        tracing::info!(
            "sccache: Server `{}` initialized to run {} parallel build jobs and prefetch up to {} job(s) in the background",
            self.state.id,
            self.state.occupancy,
            self.state.pre_fetch,
        );

        daemonize()?;

        let celery = self.tasks.app().consume();
        let status = self.start_updates(report_interval);

        futures::select_biased! {
            res = celery.fuse() => res.map_err(|e| e.into()),
            res = status.fuse() => res,
        }
    }

    pub async fn close(&self) -> Result<()> {
        self.terminate_pending_jobs().await;
        self.tasks.app().close().await.map_err(|e| e.into())
    }

    async fn start_updates(&self, report_interval: Duration) -> Result<()> {
        tokio::spawn({
            let this = self.clone();
            async move {
                loop {
                    tokio::time::sleep(
                        this.send_status()
                            .await
                            .map(|_| report_interval)
                            .unwrap_or(report_interval),
                    )
                    .await;
                }
            }
        })
        .await
        .map_err(anyhow::Error::new)
    }

    async fn terminate_pending_jobs(&self) {
        let job_ids = self.state.jobs.lock().unwrap().drain().collect::<Vec<_>>();
        futures::future::join_all(job_ids.iter().map(|job_id| {
            self.job_failed(job_id, RunJobError::MissingJobResult)
                .boxed()
        }))
        .await;
    }

    async fn send_status(&self) -> Result<()> {
        self.tasks
            .status_update(From::from(&self.state))
            .await
            .map_err(anyhow::Error::new)
            .map(|_| ())?;
        Ok(())
    }

    async fn load_job(
        &self,
        job_id: &str,
        toolchain: Toolchain,
    ) -> std::result::Result<(PathBuf, Vec<u8>), RunJobError> {
        self.state.job_stats.lock().unwrap().loading += 1;

        // Record load_job time
        let (toolchain_dir, inputs) = self
            .state
            .metrics
            .load_job(|| async {
                // Broadcast status after accepting the job
                self.send_status()
                    .await
                    .map_err(|_| RunJobError::MissingJobResult)?;

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

                Ok::<_, RunJobError>((toolchain_dir, inputs))
            })
            .await?;

        self.state.job_stats.lock().unwrap().loading -= 1;

        Ok((toolchain_dir, inputs))
    }

    async fn get_toolchain_dir(&self, job_id: &str, toolchain: Toolchain) -> Result<PathBuf> {
        // ServerToolchains retries internally, so no need to retry here
        self.toolchains.call(toolchain).await.map_err(|err| {
            // Record toolchain errors
            self.state.metrics.inc_toolchain_error_count();
            tracing::warn!("[run_job({job_id})]: Error loading toolchain: {err:?}");
            err
        })
    }

    async fn get_job_inputs(&self, job_id: &str) -> Result<Vec<u8>> {
        self.state
            .metrics
            .get_job_inputs(|| async {
                retry_with_jitter(10, || async {
                    let mut reader = self
                        .jobs_storage
                        .get_stream(&job_inputs_key(job_id))
                        .await
                        .map_err(|err| {
                            tracing::warn!(
                                "[get_job_inputs({job_id})]: Error loading stream: {err:?}"
                            );
                            RetryError::transient(err)
                        })?;

                    let mut inputs = vec![];
                    reader.read_to_end(&mut inputs).await.map_err(|err| {
                        tracing::warn!("[get_job_inputs({job_id})]: Error reading stream: {err:?}");
                        RetryError::permanent(anyhow!(err))
                    })?;

                    Ok(inputs)
                })
                .await
            })
            .await
            .map_err(|err| {
                // Record get_job_inputs errors after retrying
                self.state.metrics.inc_get_job_inputs_error_count();
                tracing::warn!("[run_job({job_id})]: Error retrieving job inputs: {err:?}");
                err
            })
    }

    async fn run_build(
        &self,
        job_id: &str,
        toolchain_dir: PathBuf,
        inputs: Vec<u8>,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> Result<BuildResult> {
        // Record build time
        self.state
            .metrics
            .run_build(|| async {
                self.builder
                    .run_build(job_id, &toolchain_dir, inputs, command, outputs)
                    .await
            })
            .await
            .map_err(|err| {
                // Record run_build errors
                self.state.metrics.inc_job_build_error_count();
                tracing::warn!("[run_job({job_id})]: Build error: {err:?}");
                err
            })
    }

    async fn put_job_result(&self, job_id: &str, result: RunJobResponse) -> Result<()> {
        // Record put_job_result load time after retrying
        self.state
            .metrics
            .put_job_result(|| async {
                let result = bincode_serialize(result).await.map_err(|err| {
                    tracing::warn!("[put_job_result({job_id})]: Error serializing result: {err:?}");
                    err
                })?;
                retry_with_jitter(10, || async {
                    self.jobs_storage
                        .put_stream(
                            &job_result_key(job_id),
                            result.len() as u64,
                            std::pin::pin!(futures::io::AllowStdIo::new(result.reader())),
                        )
                        .await
                        .map_err(|err| {
                            tracing::warn!(
                                "[put_job_result({job_id})]: Error writing stream: {err:?}"
                            );
                            RetryError::transient(err)
                        })
                })
                .await
            })
            .await
            .map_err(|err| {
                // Record put_job_result errors after retrying
                self.state.metrics.inc_put_job_result_error_count();
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
        // Add job and increment job_started count
        self.state.job_stats.lock().unwrap().accepted += 1;
        self.state.jobs.lock().unwrap().insert(job_id.to_owned());
        self.state.metrics.inc_job_started_count();
        // Record total run_job time
        self.state
            .metrics
            .run_job(|| async {
                // Load the job toolchain and inputs
                let (toolchain_dir, inputs) = self.load_job(job_id, toolchain).await?;

                // Run the build
                let result = self
                    .run_build(job_id, toolchain_dir, inputs, command, outputs)
                    .await
                    .map(|result| RunJobResponse::JobComplete {
                        result,
                        server_id: self.state.id.clone(),
                    })?;

                // Store the job result for retrieval by a scheduler
                self.put_job_result(job_id, result)
                    .await
                    .map_err(|e| e.into())
            })
            .await
    }

    async fn job_failed(&self, job_id: &str, job_err: RunJobError) -> Result<()> {
        let server_id = self.state.id.clone();
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
        self.state.metrics.inc_job_finished_count();
        self.tasks
            .job_finished(job_id.to_owned(), From::from(&self.state))
            .await
            .map_err(anyhow::Error::new)
            .map(|_| ())
    }
}
