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
    Celery,
    error::CeleryError,
    protocol::MessageContentType,
    task::{AsyncResult, Task},
};

use std::{boxed::Box, collections::HashMap, sync::Arc};

use crate::{
    config::MessageBroker,
    dist::{
        CompileCommand, SchedulerService, ServerService, StatusUpdate, Toolchain,
        scheduler_to_servers_queue, server_to_schedulers_queue,
    },
    errors::*,
};

use tokio::sync::OnceCell;

static SCHEDULER: OnceCell<Arc<dyn SchedulerService>> = OnceCell::const_new();
static SERVER: OnceCell<Arc<dyn ServerService>> = OnceCell::const_new();

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
    pub async fn scheduler(
        id: &str,
        prefetch_count: u16,
        job_time_limit: u32,
        message_broker: Option<MessageBroker>,
    ) -> Result<Arc<dyn SchedulerTasks>> {
        Ok(Arc::new(
            Self::new(
                job_time_limit,
                Tasks::celery(id, prefetch_count, message_broker)?
                    .default_queue(&server_to_schedulers_queue()),
            )
            .await?
            // Tasks that servers send and schedulers receive
            .register_task::<task_impls::job_finished>()
            .await?
            .register_task::<task_impls::server_status>()
            .await?,
        ))
    }

    pub async fn server(
        id: &str,
        prefetch_count: u16,
        message_broker: Option<MessageBroker>,
    ) -> Result<Arc<dyn ServerTasks>> {
        Ok(Arc::new(
            Self::new(
                u32::MAX,
                Tasks::celery(id, prefetch_count, message_broker)?
                    .default_queue(&scheduler_to_servers_queue()),
            )
            .await?
            // Tasks that schedulers send and servers receive
            .register_task::<task_impls::run_job>()
            .await?,
        ))
    }

    async fn new(job_time_limit: u32, builder: celery::CeleryBuilder) -> Result<Self> {
        Ok(Self {
            app: Arc::new(builder.build().await.map_err(|err| {
                let err_message = match err {
                    CeleryError::BrokerError(err) => err.to_string(),
                    err => err.to_string(),
                };
                anyhow!("{}\n\n{}", err_message, MESSAGE_BROKER_ERROR_TEXT)
            })?),
            job_time_limit,
        })
    }

    fn celery(
        id: &str,
        prefetch_count: u16,
        message_broker: Option<MessageBroker>,
    ) -> Result<celery::CeleryBuilder> {
        if let Some(message_broker) = message_broker {
            let scheduler_to_servers = &scheduler_to_servers_queue();
            let server_to_schedulers = &server_to_schedulers_queue();
            Ok(celery::CeleryBuilder::new(
                id,
                match message_broker {
                    MessageBroker::AMQP(ref uri) => uri,
                    MessageBroker::Redis(ref uri) => uri,
                },
            )
            // Prefetch this many messages
            .prefetch_count(prefetch_count)
            // Wait 2s before trying to reconnect to the broker
            .broker_connection_retry_delay(2)
            // Don't retry tasks
            .task_max_retries(0)
            // Don't retry tasks that fail with unexpected errors
            .task_retry_for_unexpected(false)
            // MessagePack is faster than JSON/Yaml/pickle etc
            .task_content_type(MessageContentType::MsgPack)
            // Declare a worker quorum queue for jobs
            .broker_declare_queue(scheduler_to_servers)
            .broker_set_queue_type(scheduler_to_servers, "quorum")
            // Declare broadcast queue for status updates
            .broker_declare_broadcast_queue(server_to_schedulers)
            // Declare the routes for tasks that schedulers send and servers receive
            .task_route(task_impls::run_job::NAME, scheduler_to_servers)
            // Declare the routes for tasks that servers send and schedulers receive
            .task_route(task_impls::job_finished::NAME, server_to_schedulers)
            .task_route(task_impls::server_status::NAME, server_to_schedulers))
        } else {
            bail!(
                "Missing required message broker configuration!\n\n{}",
                MESSAGE_BROKER_ERROR_TEXT
            )
        }
    }

    async fn register_task<T: Task + 'static>(self) -> std::result::Result<Self, CeleryError> {
        self.app.register_task::<T>().await.map(|_| self)
    }
}

#[async_trait]
pub trait AppTasks: Send + Sync {
    fn app(&self) -> &Arc<celery::Celery>;
}

#[async_trait]
impl AppTasks for Tasks {
    fn app(&self) -> &Arc<Celery> {
        &self.app
    }
}

#[async_trait]
pub trait SchedulerTasks: AppTasks + Send + Sync {
    fn set_service(&self, scheduler: Arc<dyn SchedulerService>) -> Result<()> {
        SCHEDULER
            .set(scheduler.clone())
            .map_err(|err| anyhow!("{err:#}"))
    }

    fn job_time_limit(&self) -> u32;

    async fn run_job(
        &self,
        job_id: &str,
        toolchain: &Toolchain,
        command: &CompileCommand,
        outputs: &[String],
        labels: &Option<HashMap<String, String>>,
    ) -> std::result::Result<AsyncResult, CeleryError>;
}

#[async_trait]
impl SchedulerTasks for Tasks {
    fn job_time_limit(&self) -> u32 {
        self.job_time_limit
    }

    async fn run_job(
        &self,
        job_id: &str,
        toolchain: &Toolchain,
        command: &CompileCommand,
        outputs: &[String],
        labels: &Option<HashMap<String, String>>,
    ) -> std::result::Result<AsyncResult, CeleryError> {
        self.app()
            .send_task(
                task_impls::run_job::new(
                    job_id.to_owned(),
                    toolchain.to_owned(),
                    command.to_owned(),
                    outputs.to_owned(),
                    labels.clone().unwrap_or_default(),
                )
                .with_time_limit(self.job_time_limit.saturating_sub(5))
                .with_expires_in(self.job_time_limit.saturating_sub(5)),
            )
            .await
    }
}

#[async_trait]
pub trait ServerTasks: AppTasks + Send + Sync {
    fn set_service(&self, server: Arc<dyn ServerService>) -> Result<()> {
        SERVER.set(server.clone()).map_err(|err| anyhow!("{err:#}"))
    }

    async fn update_status(
        &self,
        status: StatusUpdate,
    ) -> std::result::Result<AsyncResult, CeleryError> {
        self.app()
            .send_task(task_impls::server_status::new(status))
            .await
    }

    async fn job_finished(
        &self,
        job_id: &str,
        server: StatusUpdate,
    ) -> std::result::Result<AsyncResult, CeleryError> {
        self.app()
            .send_task(task_impls::job_finished::new(job_id.to_owned(), server))
            .await
    }
}

impl ServerTasks for Tasks {}

#[allow(non_local_definitions)]
mod task_impls {
    use celery::prelude::*;

    use futures::FutureExt;
    use std::{boxed::Box, collections::HashMap, sync::Arc};

    use crate::{
        dist::{
            CompileCommand, RunJobError, RunJobResponse, SchedulerService, ServerService,
            StatusUpdate, Toolchain,
        },
        errors::*,
    };

    use super::{SCHEDULER, SERVER};

    fn scheduler_service<'a>() -> Result<&'a Arc<dyn SchedulerService>> {
        SCHEDULER.get().ok_or_else(|| {
            let err = anyhow!("sccache-dist scheduler is not initialized");
            tracing::error!("{err:?}");
            err
        })
    }

    fn server_service<'a>() -> Result<&'a Arc<dyn ServerService>> {
        SERVER.get().ok_or_else(|| {
            let err = anyhow!("sccache-dist server is not initialized");
            tracing::error!("{err:?}");
            err
        })
    }

    #[celery::task(
        acks_late = true,
        on_failure = on_run_job_failure,
        on_success = on_run_job_success,
    )]
    pub async fn run_job(
        job_id: String,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
        labels: HashMap<String, String>,
    ) -> TaskResult<RunJobResponse> {
        tracing::trace!(
            "[run_job({job_id}, {}, {:?}, {:?}, {outputs:?})]",
            toolchain.archive_id,
            command.executable,
            command.arguments,
        );

        server_service()
            .map(|svc| svc.run_job(&job_id, toolchain, command, outputs, labels))
            .unwrap_or_else(|err| futures::future::err(err).boxed())
            .await
            .map_err(|err| match err.downcast_ref::<RunJobError>() {
                Some(RunJobError::MissingJobInputs) => {
                    TaskError::ExpectedError("MissingJobInputs".into())
                }
                Some(RunJobError::MissingToolchain) => {
                    TaskError::ExpectedError("MissingToolchain".into())
                }
                Some(RunJobError::MissingJobResult) => {
                    TaskError::ExpectedError("MissingJobResult".into())
                }
                // RunJobError::Fatal and RunJobError::Retryable errors
                Some(err) => TaskError::ExpectedError(err.to_string()),
                _ => TaskError::ExpectedError(err.to_string()),
            })
    }

    async fn on_run_job_failure(task: &run_job, err: &TaskError) {
        let job_id = &task.request().params.job_id;

        let err = match err {
            // The client can choose to retry these or compile locally.
            // Matching strings because that's the only type in TaskError.
            TaskError::ExpectedError(msg) => match msg.as_ref() {
                "MissingJobInputs" => RunJobError::MissingJobInputs,
                "MissingToolchain" => RunJobError::MissingToolchain,
                "MissingJobResult" => RunJobError::MissingJobResult,
                msg => RunJobError::Retryable(anyhow!(msg.to_owned())),
            },
            TaskError::UnexpectedError(msg) => RunJobError::Retryable(anyhow!(msg.to_owned())),
            // Report task timeouts as a fatal errors, since this means the
            // compilation took longer than the scheduler's `job_time_limit`.
            //
            // In this case, retrying the compilation on a different server
            // won't finish any sooner, so we report a fatal error so clients
            // see their build fail and can either increase the job time limit,
            // or apply source optimizations so their files compile in a
            // reasonable amount of time.
            TaskError::TimeoutError => RunJobError::Fatal(anyhow!("Job {job_id} timed out")),
            TaskError::Retry(_) => RunJobError::Fatal(anyhow!("Job {job_id} retries exceeded")),
        };

        if let Err(err) = server_service()
            .map(|svc| svc.on_failure(job_id, err).boxed())
            .unwrap_or_else(|err| futures::future::err(err).boxed())
            .await
        {
            tracing::error!("[run_job_on_failure({job_id})]: Error reporting job failure: {err:#}");
        }
    }

    async fn on_run_job_success(task: &run_job, res: &RunJobResponse) {
        let job_id = &task.request().params.job_id;

        if let Err(err) = server_service()
            .map(|svc| svc.on_success(job_id, res).boxed())
            .unwrap_or_else(|err| futures::future::err(err).boxed())
            .await
        {
            tracing::error!("[run_job_on_success({job_id})]: Error reporting job success: {err:#}");
        }
    }

    #[celery::task]
    pub async fn job_finished(job_id: String, status: StatusUpdate) -> TaskResult<()> {
        tracing::trace!("[job_finished({job_id}, {status:?})]");

        scheduler_service()
            .map(|svc| svc.job_finished(&job_id, status))
            .unwrap_or_else(|err| futures::future::err(err).boxed())
            .await
            .map_err(|e| {
                tracing::error!("[job_finished({job_id})]: Failed with unexpected error: {e:#}");
                TaskError::UnexpectedError(format!(
                    "Job {job_id} failed with unexpected error: {e:#}"
                ))
            })
    }

    #[celery::task]
    pub async fn server_status(status: StatusUpdate) -> TaskResult<()> {
        let id = status.id.clone();

        tracing::trace!("[server_status({id})]: {status:?}");

        scheduler_service()
            .map(|svc| svc.recv_server_status(status, None))
            .unwrap_or_else(|err| futures::future::err(err).boxed())
            .await
            .map_err(|e| {
                tracing::error!("[server_status({id})]: Failed with unexpected error: {e:#}");
                TaskError::UnexpectedError(format!(
                    "Task server_status for {id} failed with unexpected error: {e:#}"
                ))
            })
    }
}
