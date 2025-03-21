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

use celery::{
    error::CeleryError,
    prelude::*,
    protocol::MessageContentType,
    task::{AsyncResult, Request, Signature, Task, TaskOptions},
    Celery,
};

use futures::FutureExt;

use std::{boxed::Box, sync::Arc};

use crate::{
    config::MessageBroker,
    dist::{
        scheduler::SchedulerTasks, scheduler_to_servers_queue, server::ServerTasks,
        server_to_schedulers_queue, CompileCommand, RunJobError, RunJobResponse, SchedulerService,
        ServerDetails, ServerService, Toolchain,
    },
    errors::*,
};

use serde::{Deserialize, Serialize};

use tokio::sync::OnceCell;

static SERVER: OnceCell<Arc<dyn ServerService>> = OnceCell::const_new();
static SCHEDULER: OnceCell<Arc<dyn SchedulerService>> = OnceCell::const_new();

const MESSAGE_BROKER_ERROR_TEXT: &str = "\
    The sccache-dist scheduler and servers communicate via an external message
    broker, either an AMQP v0.9.1 implementation (like RabbitMQ) or Redis.

    All major CSPs provide managed AMQP or Redis services, or you can deploy
    RabbitMQ or Redis as part of your infrastructure.

    For local development, you can install RabbitMQ/Redis services locally or
    run their containers.

    More details can be found in in the sccache-dist documentation at:
    https://github.com/mozilla/sccache/blob/main/docs/Distributed.md#message-brokers";

pub struct Tasks {
    app: Arc<Celery>,
    job_time_limit: u32,
}

impl Tasks {
    pub async fn new(celery: celery::CeleryBuilder) -> Result<Self> {
        Ok(Self {
            job_time_limit: u32::MAX,
            app: Arc::new(celery.build().await.map_err(|err| {
                let err_message = match err {
                    CeleryError::BrokerError(err) => err.to_string(),
                    err => err.to_string(),
                };
                anyhow!("{}\n\n{}", err_message, MESSAGE_BROKER_ERROR_TEXT)
            })?),
        })
    }

    pub async fn scheduler(
        app_name: &str,
        default_queue: &str,
        prefetch_count: u16,
        message_broker: Option<MessageBroker>,
    ) -> Result<Self> {
        let tasks = Self::new(
            Self::celery(app_name, prefetch_count, message_broker)?
                // Instruct the broker to delete this queue when the scheduler disconnects
                .broker_declare_exclusive_queue(default_queue)
                .default_queue(default_queue),
        )
        .await?;

        // Tasks the scheduler receives
        tasks.app.register_task::<JobFinished>().await?;
        tasks.app.register_task::<StatusUpdate>().await?;

        Ok(tasks)
    }

    pub async fn server(
        app_name: &str,
        default_queue: &str,
        prefetch_count: u16,
        message_broker: Option<MessageBroker>,
    ) -> Result<Self> {
        let tasks = Self::new(
            Self::celery(app_name, prefetch_count, message_broker)?.default_queue(default_queue),
        )
        .await?;

        // Tasks the server receives
        tasks.app.register_task::<RunJob>().await?;

        Ok(tasks)
    }

    fn celery(
        app_name: &str,
        prefetch_count: u16,
        message_broker: Option<MessageBroker>,
    ) -> Result<celery::CeleryBuilder> {
        if let Some(message_broker) = message_broker {
            let scheduler_to_servers = scheduler_to_servers_queue();
            let server_to_schedulers = server_to_schedulers_queue();
            Ok(celery::CeleryBuilder::new(
                app_name,
                match message_broker {
                    MessageBroker::AMQP(ref uri) => uri,
                    MessageBroker::Redis(ref uri) => uri,
                },
            )
            // Indefinitely retry connecting to the broker
            .broker_connection_max_retries(u32::MAX)
            // Queues with no consumers should be deleted after 60s
            .broker_set_queue_expire_time(&scheduler_to_servers, 60 * 1000)
            .broker_set_queue_expire_time(&server_to_schedulers, 60 * 1000)
            // Undelivered messages should be discarded after 60s
            .broker_set_queue_message_ttl(&scheduler_to_servers, 60 * 1000)
            .broker_set_queue_message_ttl(&server_to_schedulers, 60 * 1000)
            // These tasks are sent to these queues
            .task_route(RunJob::NAME, &scheduler_to_servers)
            .task_route(StatusUpdate::NAME, &server_to_schedulers)
            // MessagePack is faster than JSON/Yaml/pickle etc.
            .task_content_type(MessageContentType::MsgPack)
            // Prefetch messages
            .prefetch_count(prefetch_count)
            // Immediately retry failed tasks
            .task_min_retry_delay(0)
            .task_max_retry_delay(0)
            // Don't retry tasks that fail with unexpected errors
            .task_retry_for_unexpected(false))
        } else {
            bail!(
                "Missing required message broker configuration!\n\n{}",
                MESSAGE_BROKER_ERROR_TEXT
            )
        }
    }
}

#[async_trait]
impl SchedulerTasks for Tasks {
    fn set_scheduler(&self, scheduler: Arc<dyn SchedulerService>) -> Result<()> {
        SCHEDULER
            .set(scheduler.clone())
            .map_err(|err| anyhow!("{err:#}"))
    }

    fn app(&self) -> &Arc<Celery> {
        &self.app
    }

    fn get_job_time_limit(&self) -> u32 {
        self.job_time_limit
    }

    fn set_job_time_limit(mut self, job_time_limit: u32) -> Self {
        self.job_time_limit = job_time_limit;
        self
    }

    async fn run_job(
        &self,
        job_id: String,
        reply_to: String,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> std::result::Result<AsyncResult, CeleryError> {
        self.app
            .send_task(
                RunJob::new(job_id, reply_to, toolchain, command, outputs)
                    .with_time_limit(self.job_time_limit.saturating_sub(30))
                    .with_expires_in(self.job_time_limit.saturating_sub(30)),
            )
            .await
    }
}

#[async_trait]
impl ServerTasks for Tasks {
    fn set_server(&self, server: Arc<dyn ServerService>) -> Result<()> {
        SERVER.set(server.clone()).map_err(|err| anyhow!("{err:#}"))
    }

    fn app(&self) -> &Arc<Celery> {
        &self.app
    }

    async fn status_update(
        &self,
        server: ServerDetails,
    ) -> std::result::Result<AsyncResult, CeleryError> {
        self.app.send_task(StatusUpdate::new(server)).await
    }

    async fn job_finished(
        &self,
        job_id: String,
        reply_to: &str,
        server: ServerDetails,
    ) -> std::result::Result<AsyncResult, CeleryError> {
        self.app
            .send_task(JobFinished::new(job_id, server).with_queue(reply_to))
            .await
    }
}

struct RunJob {
    request: Request<Self>,
    options: TaskOptions,
}

#[derive(Clone, Serialize, Deserialize)]
struct RunJobParams {
    job_id: String,
    reply_to: String,
    toolchain: Toolchain,
    command: CompileCommand,
    outputs: Vec<String>,
}

impl RunJob {
    fn new(
        job_id: String,
        reply_to: String,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> Signature<Self> {
        Signature::<Self>::new(RunJobParams {
            job_id,
            reply_to,
            toolchain,
            command,
            outputs,
        })
    }
}

impl RunJob {
    fn server(&self) -> Result<&Arc<dyn ServerService>> {
        match SERVER.get() {
            Some(server) => Ok(server),
            None => {
                tracing::error!("sccache-dist server is not initialized");
                Err(anyhow!("sccache-dist server is not initialized"))
            }
        }
    }
}

#[async_trait]
impl Task for RunJob {
    const NAME: &'static str = "run_job";
    const ARGS: &'static [&'static str] =
        &["job_id", "reply_to", "toolchain", "command", "outputs"];

    type Params = RunJobParams;
    type Returns = RunJobResponse;

    fn from_request(request: Request<Self>, mut options: TaskOptions) -> Self {
        options.acks_late = Some(true);
        // Set `max_retries` to 0 because we don't want rust-celery
        // to ever retry these tasks -- that is the client's responsibility.
        //
        // Since the client has an open connection to the scheduler waiting for
        // the job result, the client must be notified of the result as soon as
        // possible. If not, the client, load balancer, or scheduler may close
        // the connection due to inactivity, which would be bad if rust-celery
        // retried the job on a server that could finish it in time.
        options.max_retries = Some(0);
        Self { request, options }
    }

    fn request(&self) -> &Request<Self> {
        &self.request
    }

    fn options(&self) -> &TaskOptions {
        &self.options
    }

    async fn run(&self, params: Self::Params) -> std::result::Result<Self::Returns, TaskError> {
        let Self::Params {
            job_id,
            reply_to,
            toolchain,
            command,
            outputs,
        } = params;

        tracing::trace!(
            "[run_job({job_id}, {}, {:?}, {:?}, {outputs:?})]",
            toolchain.archive_id,
            command.executable,
            command.arguments,
        );

        self.server()
            .map(|server| server.run_job(&job_id, &reply_to, toolchain, command, outputs))
            .unwrap_or_else(|err| futures::future::err(RunJobError::Retryable(err)).boxed())
            .await
            .map_err(|err| match err {
                RunJobError::MissingJobInputs => {
                    TaskError::ExpectedError("MissingJobInputs".into())
                }
                RunJobError::MissingToolchain => {
                    TaskError::ExpectedError("MissingToolchain".into())
                }
                RunJobError::MissingJobResult => {
                    TaskError::ExpectedError("MissingJobResult".into())
                }
                RunJobError::Retryable(e) => {
                    TaskError::ExpectedError(format!("Error in job {job_id}: {e:#}"))
                }
                RunJobError::Fatal(e) => {
                    TaskError::UnexpectedError(format!("Fatal error in job {job_id}: {e:#}"))
                }
            })
    }

    async fn on_failure(&self, err: &TaskError) {
        let job_id = &self.request.params.job_id;
        let reply_to = &self.request.params.reply_to;

        let err = match err {
            // The client can choose to retry these or compile locally.
            // Matching strings because that's the only type in TaskError.
            TaskError::ExpectedError(ref msg) => match msg.as_ref() {
                "MissingJobInputs" => RunJobError::MissingJobInputs,
                "MissingToolchain" => RunJobError::MissingToolchain,
                "MissingJobResult" => RunJobError::MissingJobResult,
                msg => anyhow!(msg.to_owned()).into(),
            },
            // We don't expect many unexpected/fatal errors to happen.
            //
            // Report task timeouts as a fatal error, since this means the
            // compilation took longer than the scheduler's `job_time_limit`.
            //
            // In this case, retrying the compilation on a different server
            // won't finish any sooner, so we report a fatal error so clients
            // see their build fail and can either increase the job time limit,
            // or apply source optimizations so their files compile in a
            // reasonable amount of time.
            TaskError::UnexpectedError(ref msg) => RunJobError::Fatal(anyhow!(msg.to_owned())),
            TaskError::TimeoutError => RunJobError::Fatal(anyhow!("Job {job_id} timed out")),
            TaskError::Retry(_) => {
                RunJobError::Fatal(anyhow!("Job {job_id} exceeded the retry limit"))
            }
        };

        tracing::warn!("[run_job({job_id})]: {err:#}");

        if let Err(err) = self
            .server()
            .map(|server| server.job_failed(job_id, reply_to, err).boxed())
            .unwrap_or_else(|err| futures::future::err(err).boxed())
            .await
        {
            tracing::error!("[on_run_job_failure({job_id})]: Error reporting job failure: {err:#}");
        }
    }

    async fn on_success(&self, res: &Self::Returns) {
        let job_id = &self.request.params.job_id;
        let reply_to = &self.request.params.reply_to;

        if let Err(err) = self
            .server()
            .map(|server| server.job_finished(job_id, reply_to, res).boxed())
            .unwrap_or_else(|e| futures::future::err(e).boxed())
            .await
        {
            tracing::error!("[on_run_job_success({job_id})]: Error reporting job success: {err:#}");
        }
    }
}

struct JobFinished {
    request: Request<Self>,
    options: TaskOptions,
}

#[derive(Clone, Serialize, Deserialize)]
struct JobFinishedParams {
    job_id: String,
    server: ServerDetails,
}

impl JobFinished {
    fn new(job_id: String, server: ServerDetails) -> Signature<Self> {
        Signature::<Self>::new(JobFinishedParams { job_id, server })
    }
}

impl JobFinished {
    fn scheduler(&self) -> Result<&Arc<dyn SchedulerService>> {
        match SCHEDULER.get() {
            Some(scheduler) => Ok(scheduler),
            None => {
                tracing::error!("sccache-dist scheduler is not initialized");
                Err(anyhow!("sccache-dist scheduler is not initialized"))
            }
        }
    }
}

#[async_trait]
impl Task for JobFinished {
    const NAME: &'static str = "job_finished";
    const ARGS: &'static [&'static str] = &["job_id", "server"];

    type Params = JobFinishedParams;
    type Returns = ();

    fn from_request(request: Request<Self>, mut options: TaskOptions) -> Self {
        options.max_retries = Some(0);
        Self { request, options }
    }

    fn request(&self) -> &Request<Self> {
        &self.request
    }

    fn options(&self) -> &TaskOptions {
        &self.options
    }

    async fn run(&self, params: Self::Params) -> std::result::Result<Self::Returns, TaskError> {
        let Self::Params { job_id, server } = params;

        tracing::trace!("[job_finished({job_id}, {server:?})]");

        self.scheduler()
            .map(|scheduler| scheduler.job_finished(&job_id, server))
            .unwrap_or_else(|err| futures::future::err(err).boxed())
            .await
            .map_err(|e| {
                tracing::error!("[job_finished({job_id})]: Failed with unexpected error: {e:#}");
                TaskError::UnexpectedError(format!(
                    "Job {job_id} failed with unexpected error: {e:#}"
                ))
            })
    }
}

struct StatusUpdate {
    request: Request<Self>,
    options: TaskOptions,
}

#[derive(Clone, Serialize, Deserialize)]
struct StatusUpdateParams {
    server: ServerDetails,
}

impl StatusUpdate {
    fn new(server: ServerDetails) -> Signature<Self> {
        Signature::<Self>::new(StatusUpdateParams { server })
    }
}

impl StatusUpdate {
    fn scheduler(&self) -> Result<&Arc<dyn SchedulerService>> {
        match SCHEDULER.get() {
            Some(scheduler) => Ok(scheduler),
            None => {
                tracing::error!("sccache-dist scheduler is not initialized");
                Err(anyhow!("sccache-dist scheduler is not initialized"))
            }
        }
    }
}

#[async_trait]
impl Task for StatusUpdate {
    const NAME: &'static str = "status_update";
    const ARGS: &'static [&'static str] = &["server"];

    type Params = StatusUpdateParams;
    type Returns = ();

    fn from_request(request: Request<Self>, mut options: TaskOptions) -> Self {
        options.max_retries = Some(0);
        Self { request, options }
    }

    fn request(&self) -> &Request<Self> {
        &self.request
    }

    fn options(&self) -> &TaskOptions {
        &self.options
    }

    async fn run(&self, params: Self::Params) -> std::result::Result<Self::Returns, TaskError> {
        let Self::Params { server } = params;
        let server_id = server.id.clone();

        tracing::trace!("[status_update({server_id})]: {server:?}");

        self.scheduler()
            .map(|scheduler| scheduler.update_status(server, None))
            .unwrap_or_else(|err| futures::future::err(err).boxed())
            .await
            .map_err(|e| {
                tracing::error!(
                    "[status_update({server_id})]: Failed with unexpected error: {e:#}"
                );
                TaskError::UnexpectedError(format!(
                    "Task status_update for {server_id} failed with unexpected error: {e:#}"
                ))
            })
    }
}
