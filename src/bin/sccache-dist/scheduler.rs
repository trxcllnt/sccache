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

use futures::{lock::Mutex, AsyncReadExt};

use sccache::{
    cache::Storage,
    dist::{
        self,
        http::{bincode_deserialize, retry_with_jitter},
        metrics::{Metrics, TimeRecorder},
        CompileCommand, JobStats, NewJobRequest, NewJobResponse, RunJobRequest, RunJobResponse,
        SchedulerService, SchedulerStatus, ServerDetails, ServerStats, ServerStatus,
        SubmitToolchainResult, Toolchain,
    },
    errors::*,
    util::daemonize,
};

use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio_retry2::RetryError;

use crate::{job_inputs_key, job_result_key};

const HAS_JOB_INPUTS_TIME: &str = "sccache::scheduler::has_job_inputs_time";
const HAS_JOB_RESULT_TIME: &str = "sccache::scheduler::has_job_result_time";
const GET_JOB_RESULT_TIME: &str = "sccache::scheduler::get_job_result_time";
const DEL_JOB_INPUTS_TIME: &str = "sccache::scheduler::del_job_inputs_time";
const DEL_JOB_RESULT_TIME: &str = "sccache::scheduler::del_job_result_time";
const PUT_JOB_INPUTS_TIME: &str = "sccache::scheduler::put_job_inputs_time";
const PUT_TOOLCHAIN_TIME: &str = "sccache::scheduler::put_toolchain_time";
const DEL_TOOLCHAIN_TIME: &str = "sccache::scheduler::del_toolchain_time";

pub struct SchedulerMetrics {
    metrics: Metrics,
}

impl SchedulerMetrics {
    pub fn new(metrics: Metrics) -> Self {
        Self { metrics }
    }

    pub fn has_job_inputs_timer(&self) -> TimeRecorder {
        self.metrics.timer(HAS_JOB_INPUTS_TIME, &[])
    }

    pub fn has_job_result_timer(&self) -> TimeRecorder {
        self.metrics.timer(HAS_JOB_RESULT_TIME, &[])
    }

    pub fn get_job_result_timer(&self) -> TimeRecorder {
        self.metrics.timer(GET_JOB_RESULT_TIME, &[])
    }

    pub fn del_job_inputs_timer(&self) -> TimeRecorder {
        self.metrics.timer(DEL_JOB_INPUTS_TIME, &[])
    }

    pub fn del_job_result_timer(&self) -> TimeRecorder {
        self.metrics.timer(DEL_JOB_RESULT_TIME, &[])
    }

    pub fn put_job_inputs_timer(&self) -> TimeRecorder {
        self.metrics.timer(PUT_JOB_INPUTS_TIME, &[])
    }

    pub fn put_toolchain_timer(&self) -> TimeRecorder {
        self.metrics.timer(PUT_TOOLCHAIN_TIME, &[])
    }

    pub fn del_toolchain_timer(&self) -> TimeRecorder {
        self.metrics.timer(DEL_TOOLCHAIN_TIME, &[])
    }
}

#[async_trait]
pub trait SchedulerTasks: Send + Sync {
    fn app(&self) -> &Arc<celery::Celery>;

    fn get_job_time_limit(&self) -> u32;
    fn set_job_time_limit(self, job_time_limit: u32) -> Self
    where
        Self: Sized;

    fn set_scheduler(&self, scheduler: Arc<dyn SchedulerService>) -> Result<()>;

    async fn run_job(
        &self,
        job_id: String,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> std::result::Result<AsyncResult, CeleryError>;
}

#[derive(Clone, Debug)]
struct ServerInfo {
    pub u_time: SystemTime,
    pub info: ServerStats,
    pub jobs: JobStats,
}

pub struct Scheduler {
    jobs_storage: Arc<dyn Storage>,
    jobs: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<RunJobResponse>>>>,
    metrics: SchedulerMetrics,
    scheduler_id: String,
    servers: Arc<Mutex<HashMap<String, ServerInfo>>>,
    tasks: Arc<dyn SchedulerTasks>,
    toolchains: Arc<dyn Storage>,
}

impl Scheduler {
    pub fn new(
        jobs_storage: Arc<dyn Storage>,
        metrics: SchedulerMetrics,
        scheduler_id: String,
        tasks: impl SchedulerTasks + 'static,
        toolchains: Arc<dyn Storage>,
    ) -> Result<Arc<Self>> {
        let this = Arc::new(Self {
            jobs_storage,
            jobs: Arc::new(Mutex::new(HashMap::new())),
            metrics,
            scheduler_id,
            servers: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(tasks),
            toolchains,
        });

        this.tasks.set_scheduler(this.clone())?;

        Ok(this)
    }

    pub async fn start(&self) -> Result<()> {
        self.tasks.app().display_pretty().await;
        tracing::info!("sccache: Scheduler `{}` initialized", self.scheduler_id);
        daemonize()?;
        self.tasks.app().consume().await.map_err(|e| e.into())
    }

    pub async fn close(&self) -> Result<()> {
        self.tasks.app().close().await.map_err(|e| e.into())
    }

    fn prune_servers(servers: &mut HashMap<String, ServerInfo>) {
        let now = SystemTime::now();
        // Prune servers we haven't seen in 90s
        let timeout = Duration::from_secs(90);
        servers.retain(|_, server| now.duration_since(server.u_time).unwrap() <= timeout);
    }

    async fn has_job_inputs(&self, job_id: &str) -> bool {
        // Record has_job_inputs time
        let _timer = self.metrics.has_job_inputs_timer();
        self.jobs_storage.has(&job_inputs_key(job_id)).await
    }

    async fn has_job_result(&self, job_id: &str) -> bool {
        // Record has_job_result time
        let _timer = self.metrics.has_job_result_timer();
        self.jobs_storage.has(&job_result_key(job_id)).await
    }

    async fn get_job_result(&self, job_id: &str) -> Result<RunJobResponse> {
        // Record get_job_result time
        let _timer = self.metrics.get_job_result_timer();
        // Retrieve the result (with retry)
        let result = retry_with_jitter(10, || async {
            let mut reader = self
                .jobs_storage
                .get_stream(&job_result_key(job_id))
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

            Ok::<_, RetryError<Error>>(result)
        })
        .await?;

        // Deserialize the result
        bincode_deserialize(result).await.map_err(|err| {
            tracing::warn!("[get_job_result({job_id})]: Error deserializing result: {err:?}");
            err
        })
    }

    async fn del_job_inputs(&self, job_id: &str) -> Result<()> {
        // Record del_job_inputs time
        let _timer = self.metrics.del_job_inputs_timer();
        // Delete the inputs
        self.jobs_storage
            .del(&job_inputs_key(job_id))
            .await
            .map_err(|e| {
                tracing::warn!("[del_job_inputs({job_id})]: Error deleting job inputs: {e:?}");
                e
            })
    }

    async fn del_job_result(&self, job_id: &str) -> Result<()> {
        // Record del_job_result time
        let _timer = self.metrics.del_job_result_timer();
        // Delete the result
        self.jobs_storage
            .del(&job_result_key(job_id))
            .await
            .map_err(|e| {
                tracing::warn!("[del_job_result({job_id})]: Error deleting job result: {e:?}");
                e
            })
    }

    async fn put_job_inputs(
        &self,
        job_id: &str,
        inputs_size: u64,
        inputs: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<()> {
        // Record put_job_inputs time
        let _timer = self.metrics.put_job_inputs_timer();
        // Store the job inputs
        self.jobs_storage
            .put_stream(&job_inputs_key(job_id), inputs_size, inputs)
            .await
            .map_err(|e| {
                tracing::warn!("[put_job_inputs({job_id})]: Error writing stream: {e:?}");
                e
            })
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
                accepted: servers
                    .iter()
                    .fold(0u64, |acc, server| acc.saturating_add(server.jobs.accepted)),
                finished: servers
                    .iter()
                    .fold(0u64, |acc, server| acc.saturating_add(server.jobs.finished)),
                loading: servers
                    .iter()
                    .fold(0u64, |acc, server| acc.saturating_add(server.jobs.loading)),
                pending: servers
                    .iter()
                    .fold(0u64, |acc, server| acc.saturating_add(server.jobs.pending)),
                running: servers
                    .iter()
                    .fold(0u64, |acc, server| acc.saturating_add(server.jobs.running)),
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
        // Record put_toolchain time
        let _timer = self.metrics.put_toolchain_timer();
        // Upload toolchain to toolchains storage (S3, GCS, etc.)
        self.toolchains
            .put_stream(&toolchain.archive_id, toolchain_size, toolchain_reader)
            .await
            .context("Failed to put toolchain")
            .map(|_| SubmitToolchainResult::Success)
            .map_err(|err| {
                tracing::error!("[put_toolchain({})]: {err:?}", toolchain.archive_id);
                err
            })
    }

    async fn del_toolchain(&self, toolchain: &Toolchain) -> Result<()> {
        // Record del_toolchain time
        let _timer = self.metrics.del_toolchain_timer();
        // Delete the toolchain from toolchains storage (S3, GCS, etc.)
        self.toolchains
            .del(&toolchain.archive_id)
            .await
            .context("Failed to delete toolchain")
            .map_err(|err| {
                tracing::error!("[del_toolchain({})]: {err:?}", toolchain.archive_id);
                err
            })
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
            timeout: self.tasks.get_job_time_limit(),
        })
    }

    async fn put_job(
        &self,
        job_id: &str,
        inputs_size: u64,
        inputs: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<()> {
        self.put_job_inputs(job_id, inputs_size, inputs).await
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
            .run_job(job_id.to_owned(), toolchain, command, outputs)
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
            let job_success = matches!(job_result, RunJobResponse::JobComplete { .. });
            let job_success = sndr
                .send(job_result)
                .map_or_else(|_| false, |_| job_success);
            self.update_status(server, Some(job_success)).await
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

        fn duration_from_micros((secs, nanos): (u64, u32)) -> Duration {
            Duration::new(secs, nanos)
        }

        // Insert or update the server info
        servers
            .entry(details.id.clone())
            .and_modify(|server| {
                // Convert to absolute durations since the Unix epoch
                let t1 = server.u_time.duration_since(UNIX_EPOCH).unwrap();
                let t2 = duration_from_micros(details.created_at);
                // If this event is newer than the latest state, it is now the latest state.
                if t2 >= t1 {
                    server.info = details.info.clone();
                    server.jobs = details.jobs.clone();
                    // Increment prev time with the difference between prev and next
                    server.u_time = server.u_time.checked_add(t2 - t1).unwrap();
                }
            })
            .or_insert_with(|| ServerInfo {
                info: details.info.clone(),
                jobs: details.jobs.clone(),
                // Convert to absolute duration since the Unix epoch
                u_time: UNIX_EPOCH
                    .checked_add(duration_from_micros(details.created_at))
                    .unwrap(),
            });

        Self::prune_servers(&mut servers);

        Ok(())
    }
}
