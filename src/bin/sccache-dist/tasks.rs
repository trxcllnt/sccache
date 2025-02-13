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

use sccache::{
    config::MessageBroker,
    dist::{
        CompileCommand, RunJobError, SchedulerService, ServerDetails, ServerService, Toolchain,
    },
    errors::*,
};

use serde::{Deserialize, Serialize};

use tokio::sync::OnceCell;

use crate::{
    scheduler::SchedulerTasks, scheduler_to_servers_queue, server::ServerTasks,
    server_to_schedulers_queue,
};

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
    pub async fn new(
        app_name: &str,
        default_queue: &str,
        prefetch_count: u16,
        message_broker: Option<MessageBroker>,
    ) -> Result<Self> {
        if let Some(message_broker) = message_broker {
            let broker_uri = match message_broker {
                MessageBroker::AMQP(ref uri) => uri,
                MessageBroker::Redis(ref uri) => uri,
            };
            // This URI can contain the username/password, so log at trace level
            tracing::trace!("Message broker URI: {broker_uri}");

            let to_servers = scheduler_to_servers_queue();
            let to_schedulers = server_to_schedulers_queue();

            Ok(Self {
                job_time_limit: u32::MAX,
                app: Arc::new(
                    celery::CeleryBuilder::new(app_name, broker_uri)
                        .default_queue(default_queue)
                        .task_route(RunJob::NAME, &to_servers)
                        .task_route(StatusUpdate::NAME, &to_schedulers)
                        .task_content_type(MessageContentType::MsgPack)
                        .prefetch_count(prefetch_count)
                        // Immediately retry failed tasks
                        .task_min_retry_delay(0)
                        .task_max_retry_delay(0)
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
                            anyhow!("{}\n\n{}", err_message, MESSAGE_BROKER_ERROR_TEXT)
                        })?,
                ),
            })
        } else {
            bail!(
                "Missing required message broker configuration!\n\n{}",
                MESSAGE_BROKER_ERROR_TEXT
            )
        }
    }

    pub async fn scheduler(
        app_name: &str,
        default_queue: &str,
        prefetch_count: u16,
        message_broker: Option<MessageBroker>,
    ) -> Result<Self> {
        let tasks = Self::new(app_name, default_queue, prefetch_count, message_broker).await?;

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
        let tasks = Self::new(app_name, default_queue, prefetch_count, message_broker).await?;

        // Tasks the server receives
        tasks.app.register_task::<RunJob>().await?;

        Ok(tasks)
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
                    .with_time_limit(self.job_time_limit)
                    .with_expires_in(self.job_time_limit),
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
    type Returns = ();

    fn from_request(request: Request<Self>, mut options: TaskOptions) -> Self {
        options.acks_late = Some(true);
        options.max_retries = Some(10);
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
            .unwrap_or_else(|err| futures::future::err(RunJobError::Err(err)).boxed())
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
                RunJobError::ServerTerminated => {
                    TaskError::ExpectedError("ServerTerminated".into())
                }
                RunJobError::Err(e) => {
                    tracing::debug!("[run_job({job_id})]: Failed with expected error: {e:#}");
                    TaskError::ExpectedError(format!(
                        "Job {job_id} failed with expected error: {e:#}"
                    ))
                }
            })
    }

    async fn on_failure(&self, err: &TaskError) {
        let job_id = &self.request.params.job_id;
        let reply_to = &self.request.params.reply_to;

        let err_res = {
            if let TaskError::UnexpectedError(msg) = err {
                // Never retry unexpected errors
                Err(RunJobError::Err(anyhow!(msg.to_owned())))
            } else if let TaskError::ExpectedError(msg) = err {
                // Don't retry errors due to missing toolchain or inputs.
                // Notify the client so they can be retried or compiled locally.
                // Matching strings because that's the only data in TaskError.
                match msg.as_ref() {
                    "MissingJobInputs" => Err(RunJobError::MissingJobInputs),
                    "MissingToolchain" => Err(RunJobError::MissingToolchain),
                    // Maybe retry other errors
                    "MissingJobResult" => Ok(RunJobError::MissingJobResult),
                    "ServerTerminated" => Ok(RunJobError::ServerTerminated),
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

        let err = match err_res {
            // Maybe retry "OK" errors
            Ok(job_err) => {
                if self.request.retries < self.max_retries().unwrap_or(0) {
                    return;
                }
                job_err
            }
            Err(job_err) => job_err,
        };

        tracing::debug!("[run_job({job_id})]: Failed with error: {err:#}");

        if let Err(err) = self
            .server()
            .map(|server| server.job_failed(job_id, reply_to, err).boxed())
            .unwrap_or_else(|err| futures::future::err(err).boxed())
            .await
        {
            tracing::error!("[on_run_job_failure({job_id})]: Error reporting job failure: {err:#}");
        }
    }

    async fn on_success(&self, _: &Self::Returns) {
        let job_id = &self.request.params.job_id;
        let reply_to = &self.request.params.reply_to;

        if let Err(err) = self
            .server()
            .map(|server| server.job_finished(job_id, reply_to).boxed())
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
        options.acks_late = Some(true);
        options.max_retries = Some(50);
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
            .map_err(|_| TaskError::ExpectedError("Unknown job".into()))
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

        tracing::trace!("[status_update({server:?})]");

        self.scheduler()
            .map(|scheduler| scheduler.update_status(server, None))
            .unwrap_or_else(|err| futures::future::err(err).boxed())
            .await
            .map_err(|e| {
                TaskError::UnexpectedError(format!(
                    "status_update failed with unexpected error: {e:#}"
                ))
            })
    }
}
