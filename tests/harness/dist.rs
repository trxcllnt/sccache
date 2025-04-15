use bytes::Buf;
use fs_err as fs;
use itertools::Itertools;
use std::{
    collections::HashMap,
    io::{BufRead, Write},
    net::{self, SocketAddr},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    str,
    sync::{
        atomic::{AtomicU16, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    thread,
    time::{Duration, Instant},
};

use nix::{
    sys::{
        signal::Signal,
        wait::{WaitPidFlag, WaitStatus},
    },
    unistd::Pid,
};

use sccache::{
    config::{
        scheduler::ClientAuth, scheduler::Config as SchedulerConfig, server::BuilderType,
        server::Config as ServerConfig, CacheType, DiskCacheConfig, FileConfig, HTTPUrl,
        MessageBroker, RedisCacheConfig, StorageConfig, INSECURE_DIST_CLIENT_TOKEN,
    },
    dist::{http::urls::scheduler_status as scheduler_status_url, SchedulerStatus},
};

use uuid::Uuid;

use super::{client::SccacheClient, prune_command, write_json_cfg, TC_CACHE_SIZE};

const CONTAINER_NAME_PREFIX: &str = "sccache_dist";
const CONTAINER_EXTERNAL_PATH: &str = "/sccache/external";
const CONTAINER_INTERNAL_PATH: &str = "/sccache/internal";
const DIST_IMAGE: &str = "sccache_dist_test_image";
const DIST_DOCKERFILE: &str = include_str!("Dockerfile.sccache-dist");
const DIST_IMAGE_BWRAP_PATH: &str = "/usr/bin/bwrap";
const MAX_STARTUP_WAIT: Duration = Duration::from_secs(30);

static SCHEDULER_PORT: AtomicU16 = AtomicU16::new(10500);
static RABBITMQ_PORT: AtomicU16 = AtomicU16::new(5672);
static REDIS_PORT: AtomicU16 = AtomicU16::new(6379);

pub fn cargo_command() -> Command {
    prune_command(Command::new("cargo"))
}

pub fn sccache_dist_path() -> PathBuf {
    assert_cmd::cargo::cargo_bin("sccache-dist")
}

fn sccache_scheduler_cfg(message_broker: &MessageBroker) -> SchedulerConfig {
    let mut config = SchedulerConfig::load(None).unwrap();
    let scheduler_port = SCHEDULER_PORT.fetch_add(1, Ordering::SeqCst);
    config.client_auth = ClientAuth::Insecure;
    config.message_broker = Some(message_broker.clone());
    config.public_addr = SocketAddr::from(([0, 0, 0, 0], scheduler_port));
    config.jobs.fallback.dir = Path::new(CONTAINER_EXTERNAL_PATH).join("jobs");
    config.toolchains.fallback.dir = Path::new(CONTAINER_EXTERNAL_PATH).join("toolchains");
    config
}

fn sccache_server_cfg(message_broker: &MessageBroker) -> ServerConfig {
    let mut config = ServerConfig::load(None).unwrap();
    config.max_per_core_load = 0.0;
    config.max_per_core_prefetch = 0.0;
    config.message_broker = Some(message_broker.clone());
    config.builder = BuilderType::Overlay {
        build_dir: CONTAINER_EXTERNAL_PATH.into(),
        bwrap_path: DIST_IMAGE_BWRAP_PATH.into(),
    };
    config.cache_dir = Path::new(CONTAINER_INTERNAL_PATH).to_path_buf();
    config.jobs.fallback.dir = Path::new(CONTAINER_EXTERNAL_PATH).join("jobs");
    config.toolchains.fallback.dir = Path::new(CONTAINER_EXTERNAL_PATH).join("toolchains");
    config.toolchain_cache_size = TC_CACHE_SIZE;
    config
}

static DIST_SYSTEM_GLOBALS: OnceLock<Mutex<Option<Arc<DistSystemGlobals>>>> = OnceLock::new();

struct DistSystemGlobals {
    dropped_handles_count: AtomicU64,
    message_brokers: Mutex<Vec<DistHandle>>,
    teardown_failures: Mutex<HashMap<(u64, String), std::process::Output>>,
    teardown_successes: Mutex<HashMap<(u64, String), std::process::Output>>,
}

impl DistSystemGlobals {
    fn inst<'a>() -> std::sync::MutexGuard<'a, Option<Arc<DistSystemGlobals>>> {
        DIST_SYSTEM_GLOBALS
            .get_or_init(|| {
                // Make sure the docker image is available, building it if necessary.
                // This is here (and not below) so that it only happens once.
                let mut cmd = Command::new("docker")
                    .args(["build", "-q", "-t", DIST_IMAGE, "-"])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();

                cmd.stdin
                    .as_mut()
                    .unwrap()
                    .write_all(DIST_DOCKERFILE.as_bytes())
                    .unwrap();

                check_output(&cmd.wait_with_output().unwrap());

                Mutex::new(Some(DistSystemGlobals::new()))
            })
            .lock()
            .unwrap()
    }

    fn get() -> Arc<Self> {
        let mut maybe = DistSystemGlobals::inst();
        if let Some(inst) = maybe.as_ref() {
            Arc::clone(inst)
        } else {
            let inst = DistSystemGlobals::new();
            *maybe = Some(Arc::clone(&inst));
            inst
        }
    }

    fn new() -> Arc<Self> {
        Arc::new(Self {
            dropped_handles_count: AtomicU64::new(0),
            message_brokers: Mutex::new(vec![
                Self::add_message_broker("rabbitmq"),
                Self::add_message_broker("redis"),
            ]),
            teardown_failures: Mutex::new(HashMap::new()),
            teardown_successes: Mutex::new(HashMap::new()),
        })
    }

    fn add_message_broker(message_broker: &str) -> DistHandle {
        let container_name = name_with_uuid(message_broker);
        let message_broker = DistMessageBroker::new(message_broker);

        let DistMessageBroker {
            image,
            host_port,
            container_port,
            ..
        } = &message_broker;

        let output = Command::new("docker")
            .args([
                "run",
                "--name",
                &container_name,
                "-p",
                &format!("{host_port}:{container_port}"),
                "-d",
                image,
            ])
            .output()
            .unwrap();

        check_output(&output);

        DistHandle::MessageBroker(MessageBrokerHandle {
            broker: message_broker,
            handle: ResourceHandle::Container {
                id: container_name.clone(),
                cid: container_name,
            },
        })
    }

    #[allow(unused)]
    pub fn rabbitmq(&self) -> Option<DistMessageBroker> {
        self.message_broker("rabbitmq")
    }

    pub fn redis(&self) -> Option<DistMessageBroker> {
        self.message_broker("redis")
    }

    pub fn message_broker(&self, message_broker: &str) -> Option<DistMessageBroker> {
        self.message_brokers
            .lock()
            .unwrap()
            .iter()
            .filter_map(|component| {
                if let DistHandle::MessageBroker(handle) = component {
                    Some(handle)
                } else {
                    None
                }
            })
            .find(|b| match message_broker {
                "rabbitmq" => b.is_amqp(),
                "redis" => b.is_redis(),
                _ => false,
            })
            .map(|b| b.broker.clone())
    }

    pub fn resource_key(&self) -> u64 {
        self.dropped_handles_count.fetch_add(1, Ordering::SeqCst)
    }

    pub fn teardown_failure(&self, idx: u64, id: &str, output: std::process::Output) {
        self.teardown_failures
            .lock()
            .unwrap()
            .insert((idx, id.to_owned()), output.clone());
    }

    pub fn teardown_success(&self, idx: u64, id: &str, output: std::process::Output) {
        self.teardown_successes
            .lock()
            .unwrap()
            .insert((idx, id.to_owned()), output.clone());
    }

    fn destroy(self: &Arc<Self>) {
        let refcount = Arc::strong_count(self).saturating_sub(1);
        // If the only reference to DistSystemGlobals is the OnceLock,
        // destroy DistSystemGlobals and replace the `Some` with `None`
        if refcount == 1 {
            self.message_brokers
                .lock()
                .unwrap()
                .drain(..)
                .for_each(|component| {
                    if let DistHandle::MessageBroker(broker) = component {
                        broker.handle.destroy(self, false);
                    }
                });

            self.print_ouputs_and_errors();

            // Replace the current Some(DistSystemGlobals) with None
            DistSystemGlobals::inst().take();
        }
    }

    fn print_ouputs_and_errors(&self) {
        fn print_output(prefix: &str, output: &std::process::Output) {
            println!("\n{prefix}");
            if !output.stdout.is_empty() {
                println!("\n=== begin stdout {}\n", "=".repeat(43));
                output
                    .stdout
                    .reader()
                    .lines()
                    .for_each(|line| println!("    {}", line.unwrap_or_default()));
                println!("\n===== end stdout  {}\n", "=".repeat(43));
            }
            if !output.stderr.is_empty() {
                println!("\n=== begin stderr  {}\n", "=".repeat(43));
                output
                    .stderr
                    .reader()
                    .lines()
                    .for_each(|line| println!("    {}", line.unwrap_or_default()));
                println!("\n===== end stderr  {}\n", "=".repeat(43));
            }
        }

        let mut has_errors = false;

        for ((_, id), output) in self
            .teardown_successes
            .lock()
            .unwrap()
            .drain()
            .sorted_by(|((a, _), _), ((b, _), _)| a.cmp(b))
        {
            print_output(
                &format!(
                    "=== server output {}\n\nid: {id}\n{}",
                    "=".repeat(61),
                    output.status
                ),
                &output,
            );
        }

        for ((_, id), errors) in self
            .teardown_failures
            .lock()
            .unwrap()
            .drain()
            .sorted_by(|((a, _), _), ((b, _), _)| a.cmp(b))
        {
            has_errors = true;
            print_output(
                &format!(
                    "=== server errors {}\n\nid: {id}\n{}",
                    "=".repeat(61),
                    errors.status
                ),
                &errors,
            );
        }

        if has_errors && !thread::panicking() {
            panic!("Encountered failures during dist system teardown");
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct DistMessageBroker {
    url: String,
    host_port: u16,
    container_port: u16,
    config: MessageBroker,
    image: String,
}

impl DistMessageBroker {
    pub fn new(message_broker: &str) -> Self {
        match message_broker {
            "rabbitmq" => {
                let host_port = RABBITMQ_PORT.fetch_add(1, Ordering::SeqCst);
                let path = format!("amqp://127.0.0.1:{host_port}//");
                Self {
                    config: MessageBroker::AMQP(path.clone()),
                    image: "rabbitmq:4".into(),
                    container_port: 5672,
                    host_port,
                    url: path,
                }
            }
            "redis" => {
                let host_port = REDIS_PORT.fetch_add(1, Ordering::SeqCst);
                let path = format!("redis://127.0.0.1:{host_port}/");
                Self {
                    config: MessageBroker::Redis(path.clone()),
                    image: "redis:7".into(),
                    container_port: 6379,
                    host_port,
                    url: path,
                }
            }
            _ => unreachable!(""),
        }
    }

    pub fn is_amqp(&self) -> bool {
        matches!(self.config, MessageBroker::AMQP(_))
    }

    pub fn is_redis(&self) -> bool {
        matches!(self.config, MessageBroker::Redis(_))
    }

    pub fn url(&self) -> HTTPUrl {
        HTTPUrl::from_url(reqwest::Url::parse(&self.url).unwrap())
    }
}

#[derive(Clone)]
pub struct MessageBrokerHandle {
    broker: DistMessageBroker,
    handle: ResourceHandle,
}

#[allow(unused)]
impl MessageBrokerHandle {
    pub fn is_amqp(&self) -> bool {
        self.broker.is_amqp()
    }
    pub fn is_redis(&self) -> bool {
        self.broker.is_redis()
    }
}

#[derive(Clone)]
pub struct SchedulerHandle {
    port: u16,
    handle: ResourceHandle,
    globals: Arc<DistSystemGlobals>,
}

impl Drop for SchedulerHandle {
    fn drop(&mut self) {
        self.handle.destroy(&self.globals, true);
        self.globals.destroy();
    }
}

#[derive(Clone)]
pub struct ServerHandle {
    handle: ResourceHandle,
    globals: Arc<DistSystemGlobals>,
}

impl SchedulerHandle {
    pub fn url(&self) -> HTTPUrl {
        let url = format!("http://127.0.0.1:{}", self.port);
        HTTPUrl::from_url(reqwest::Url::parse(&url).unwrap())
    }
    pub fn status(&self) -> SchedulerStatus {
        let mut req = reqwest::blocking::Client::builder()
            .build()
            .unwrap()
            .get(scheduler_status_url(&self.url().to_url()));
        req = req.bearer_auth(INSECURE_DIST_CLIENT_TOKEN);
        let res = req.send().unwrap();
        assert!(res.status().is_success());
        bincode::deserialize_from(res).unwrap()
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.handle.destroy(&self.globals, true);
        self.globals.destroy();
    }
}

#[derive(Clone)]
pub enum ResourceHandle {
    Container {
        id: String,
        cid: String,
    },
    #[allow(unused)]
    Process {
        id: String,
        #[allow(dead_code)]
        pid: Pid,
    },
}

impl ResourceHandle {
    fn destroy(&self, globals: &Arc<DistSystemGlobals>, log_outputs: bool) {
        let resource_key = globals.resource_key();

        // Panicking halfway through drop would either abort (if it's a double panic) or leave us with
        // resources that aren't yet cleaned up. Instead, do as much as possible then decide what to do
        // at the end - panic (if not already doing so) or let the panic continue
        macro_rules! log_err {
            ($e:expr) => {
                match $e {
                    Ok(()) => (),
                    Err((id, err)) => {
                        globals.teardown_failure(
                            resource_key,
                            id,
                            std::process::Output {
                                status: sccache::mock_command::exit_status(1),
                                stdout: vec![],
                                stderr: err.into_bytes(),
                            },
                        );
                    }
                }
            };
        }

        match self {
            ResourceHandle::Container { id, cid } => {
                log_err!(Command::new("docker")
                    .args(["logs", cid])
                    .output()
                    .map(|o| {
                        if log_outputs {
                            globals.teardown_success(resource_key, cid, o);
                        }
                    })
                    .map_err(|e| (id, format!("[{id}, {cid})]: {e:?}"))));
                log_err!(Command::new("docker")
                    .args(["kill", cid])
                    .output()
                    .map(|_| ())
                    .map_err(|e| (id, format!("[{id}, {cid})]: {e:?}"))));
                // If you want containers to hang around (e.g. for debugging), comment out these lines
                log_err!(Command::new("docker")
                    .args(["rm", "-f", cid])
                    .output()
                    .map(|_| ())
                    .map_err(|e| (id, format!("[{id}, {cid})]: {e:?}"))));
            }
            ResourceHandle::Process { id, pid } => {
                log_err!(nix::sys::signal::kill(*pid, Signal::SIGINT)
                    .map_err(|e| (id, format!("[{id}, {pid})]: {e:?}"))));
                thread::sleep(Duration::from_secs(5));
                // Default to trying to kill again, e.g. if there was an error waiting on the pid
                let mut killagain = true;
                log_err!(nix::sys::wait::waitpid(*pid, Some(WaitPidFlag::WNOHANG))
                    .map(|ws| {
                        if ws != WaitStatus::StillAlive {
                            killagain = false;
                        }
                    })
                    .map_err(|e| (id, format!("[{id}, {pid})]: {e:?}"))));
                if killagain {
                    eprintln!("[{id}, {pid})]: process ignored SIGINT, trying SIGKILL");
                    log_err!(nix::sys::signal::kill(*pid, Signal::SIGKILL)
                        .map_err(|e| (id, format!("[{id}, {pid})]: {e:?}"))));
                    log_err!(nix::sys::wait::waitpid(*pid, Some(WaitPidFlag::WNOHANG))
                        .map_err(|e| e.to_string())
                        .and_then(|ws| if ws == WaitStatus::StillAlive {
                            eprintln!("[{id}, {pid})]: process still alive after SIGKILL");
                            Err("process still alive after SIGKILL".into())
                        } else {
                            Ok(())
                        })
                        .map_err(|e| (id, format!("[{id}, {pid})]: {e}"))));
                }
            }
        }
    }
}

#[derive(Clone)]
pub enum DistHandle {
    MessageBroker(MessageBrokerHandle),
    Scheduler(SchedulerHandle),
    Server(ServerHandle),
}

pub struct DistSystemBuilder {
    dist_system_name: String,
    globals: Arc<DistSystemGlobals>,
    job_time_limit: Option<u32>,
    message_broker: Option<DistMessageBroker>,
    scheduler_count: u64,
    scheduler_jobs_redis_storage: Option<DistMessageBroker>,
    scheduler_toolchains_redis_storage: Option<DistMessageBroker>,
    server_count: u64,
    server_jobs_redis_storage: Option<DistMessageBroker>,
    server_toolchains_redis_storage: Option<DistMessageBroker>,
}

impl DistSystemBuilder {
    pub fn new() -> Self {
        Self {
            dist_system_name: name_with_uuid("sccache_dist_test"),
            globals: DistSystemGlobals::get(),
            job_time_limit: None,
            message_broker: None,
            scheduler_count: 0,
            scheduler_jobs_redis_storage: None,
            scheduler_toolchains_redis_storage: None,
            server_count: 0,
            server_jobs_redis_storage: None,
            server_toolchains_redis_storage: None,
        }
    }

    pub fn with_name(self, name: &str) -> Self {
        Self {
            dist_system_name: name.to_owned(),
            ..self
        }
    }

    pub fn with_scheduler(self) -> Self {
        Self {
            scheduler_count: self.scheduler_count + 1,
            ..self
        }
    }

    pub fn with_server(self) -> Self {
        Self {
            server_count: self.server_count + 1,
            ..self
        }
    }

    pub fn with_job_time_limit(self, job_time_limit: u32) -> Self {
        Self {
            job_time_limit: Some(job_time_limit),
            ..self
        }
    }

    pub fn with_message_broker(self, message_broker: &str) -> Self {
        Self {
            message_broker: self.globals.message_broker(message_broker),
            ..self
        }
    }

    pub fn with_redis_storage(self) -> Self {
        self.with_scheduler_jobs_redis_storage()
            .with_scheduler_toolchains_redis_storage()
            .with_server_jobs_redis_storage()
            .with_server_toolchains_redis_storage()
    }

    pub fn with_scheduler_jobs_redis_storage(self) -> Self {
        Self {
            scheduler_jobs_redis_storage: self.globals.redis(),
            ..self
        }
    }

    pub fn with_scheduler_toolchains_redis_storage(self) -> Self {
        Self {
            scheduler_toolchains_redis_storage: self.globals.redis(),
            ..self
        }
    }

    pub fn with_server_jobs_redis_storage(self) -> Self {
        Self {
            server_jobs_redis_storage: self.globals.redis(),
            ..self
        }
    }

    pub fn with_server_toolchains_redis_storage(self) -> Self {
        Self {
            server_toolchains_redis_storage: self.globals.redis(),
            ..self
        }
    }

    pub fn build(self) -> DistSystem {
        let name = &self.dist_system_name;
        let mut system = DistSystem::new(name);
        let message_broker = self.message_broker.unwrap();

        fn storage_cfg(suffix: &str, redis: &DistMessageBroker) -> StorageConfig {
            StorageConfig {
                storage: Some(CacheType::Redis(RedisCacheConfig {
                    endpoint: Some(redis.url().to_url().to_string()),
                    ..Default::default()
                })),
                fallback: DiskCacheConfig {
                    dir: Path::new(CONTAINER_INTERNAL_PATH).join(suffix),
                    ..Default::default()
                },
            }
        }

        for i in 0..self.scheduler_count {
            let mut cfg = SchedulerConfig {
                scheduler_id: format!("{name}_scheduler_{i}"),
                ..system.scheduler_cfg(&message_broker.config)
            };
            if let Some(job_time_limit) = self.job_time_limit {
                cfg.job_time_limit = job_time_limit;
            }
            if let Some(redis) = self.scheduler_jobs_redis_storage.as_ref() {
                cfg.jobs = storage_cfg("jobs", redis);
            }
            if let Some(redis) = self.scheduler_toolchains_redis_storage.as_ref() {
                cfg.toolchains = storage_cfg("toolchains", redis);
            }
            system.add_scheduler(cfg);
        }

        for i in 0..self.server_count {
            let mut cfg = ServerConfig {
                server_id: format!("{name}_server_{i}"),
                ..system.server_cfg(&message_broker.config)
            };
            if let Some(redis) = self.server_jobs_redis_storage.as_ref() {
                cfg.jobs = storage_cfg("jobs", redis);
            }
            if let Some(redis) = self.server_toolchains_redis_storage.as_ref() {
                cfg.toolchains = storage_cfg("toolchains", redis);
            }
            system.add_server(cfg);
        }

        system
    }
}

pub struct DistSystem {
    name: String,
    handles: Vec<DistHandle>,
    data_dir: tempfile::TempDir,
    dist_dir: PathBuf,
    sccache_dist: PathBuf,
}

impl DistSystem {
    pub fn builder() -> DistSystemBuilder {
        DistSystemBuilder::new()
    }

    fn new(name: &str) -> Self {
        let data_dir = tempfile::Builder::new()
            .prefix(&format!("sccache_dist_{name}"))
            .tempdir()
            .unwrap();

        let dist_dir = data_dir.path().join("distsystem");
        fs::create_dir_all(&dist_dir).unwrap();

        Self {
            name: name.to_owned(),
            handles: vec![],
            data_dir,
            dist_dir,
            // globals,
            sccache_dist: sccache_dist_path(),
        }
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir.path()
    }

    pub fn dist_dir(&self) -> &Path {
        self.dist_dir.as_path()
    }

    pub fn new_client(&self, client_config: &FileConfig) -> Arc<SccacheClient> {
        let data_dir = self.data_dir();
        write_json_cfg(data_dir, "sccache-client.json", client_config);
        Arc::new(
            SccacheClient::new(
                &data_dir.join("sccache-client.json"),
                &data_dir.join("sccache-cached-cfg"),
            )
            .start(),
        )
    }

    pub fn scheduler_cfg(&self, message_broker: &MessageBroker) -> SchedulerConfig {
        sccache_scheduler_cfg(message_broker)
    }

    pub fn add_scheduler(&mut self, scheduler_cfg: SchedulerConfig) -> &mut Self {
        let scheduler_id = scheduler_cfg.scheduler_id.clone();
        let scheduler_port = scheduler_cfg.public_addr.port();
        let container_name = name_with_uuid(&scheduler_id);
        let cfg_file_name = format!("{scheduler_id}-cfg.json");

        for path in [
            &scheduler_cfg.jobs.fallback.dir,
            &scheduler_cfg.toolchains.fallback.dir,
        ] {
            if let Ok(path) = path.strip_prefix(CONTAINER_EXTERNAL_PATH) {
                fs::create_dir_all(self.dist_dir().join(path)).unwrap();
            }
        }

        fs::File::create(self.dist_dir().join(&cfg_file_name))
            .unwrap()
            .write_all(&serde_json::to_vec(&scheduler_cfg.into_file()).unwrap())
            .unwrap();

        // Create the scheduler
        let output = Command::new("docker")
            .args([
                "run",
                "--name",
                &container_name,
                "-e",
                "SCCACHE_NO_DAEMON=1",
                "-e",
                "SCCACHE_LOG=sccache=info,tower_http=debug,axum::rejection=trace",
                "-e",
                &format!("SCCACHE_DIST_DEPLOYMENT_NAME={}", &self.name),
                "-e",
                "RUST_BACKTRACE=1",
                "-e",
                "TOKIO_WORKER_THREADS=2",
                "--network",
                "host",
                "--restart",
                "always",
                "-v",
                &format!("{}:/sccache-dist:z", self.sccache_dist.to_str().unwrap()),
                "-v",
                &format!(
                    "{}:{}:z",
                    self.dist_dir().to_str().unwrap(),
                    CONTAINER_EXTERNAL_PATH
                ),
                "-d",
                DIST_IMAGE,
                "bash",
                "-c",
                &format!(
                    r#"
                        set -o errexit &&
                        exec /sccache-dist scheduler --config {cfg}
                    "#,
                    cfg = Path::new(CONTAINER_EXTERNAL_PATH)
                        .join(&cfg_file_name)
                        .to_str()
                        .unwrap()
                ),
            ])
            .output()
            .unwrap();

        check_output(&output);

        self.handles.push(DistHandle::Scheduler(SchedulerHandle {
            port: scheduler_port,
            globals: DistSystemGlobals::get(),
            handle: ResourceHandle::Container {
                id: scheduler_id,
                cid: container_name,
            },
        }));

        let scheduler = self.schedulers().into_iter().last().unwrap();

        wait_for_http(
            scheduler.url(),
            Duration::from_millis(1000),
            MAX_STARTUP_WAIT,
        );

        wait_for(
            || {
                let status = scheduler.status();
                if matches!(status, SchedulerStatus { .. }) {
                    Ok(())
                } else {
                    Err(format!("{:?}", status))
                }
            },
            Duration::from_millis(1000),
            MAX_STARTUP_WAIT,
        );

        self
    }

    pub fn server_cfg(&self, message_broker: &MessageBroker) -> ServerConfig {
        sccache_server_cfg(message_broker)
    }

    pub fn add_server(&mut self, server_cfg: ServerConfig) -> &mut Self {
        let server_id = server_cfg.server_id.clone();
        let container_name = name_with_uuid(&server_id);
        let cfg_file_name = format!("{server_id}-cfg.json");

        for path in [
            &server_cfg.jobs.fallback.dir,
            &server_cfg.toolchains.fallback.dir,
        ] {
            if let Ok(path) = path.strip_prefix(CONTAINER_EXTERNAL_PATH) {
                fs::create_dir_all(self.dist_dir().join(path)).unwrap();
            }
        }

        fs::File::create(self.dist_dir().join(&cfg_file_name))
            .unwrap()
            .write_all(&serde_json::to_vec(&server_cfg.into_file()).unwrap())
            .unwrap();

        let output = Command::new("docker")
            .args([
                "run",
                // Important for the bubblewrap builder
                "--privileged",
                "--name",
                &container_name,
                "-e",
                "SCCACHE_NO_DAEMON=1",
                "-e",
                "SCCACHE_LOG=sccache=debug",
                "-e",
                &format!("SCCACHE_DIST_DEPLOYMENT_NAME={}", &self.name),
                "-e",
                "RUST_BACKTRACE=1",
                "-e",
                "TOKIO_WORKER_THREADS=2",
                "--network",
                "host",
                "--restart",
                "always",
                "-v",
                &format!("{}:/sccache-dist:z", self.sccache_dist.to_str().unwrap()),
                "-v",
                &format!(
                    "{}:{}:z",
                    self.dist_dir().to_str().unwrap(),
                    CONTAINER_EXTERNAL_PATH
                ),
                "-d",
                DIST_IMAGE,
                "bash",
                "-c",
                &format!(
                    r#"
                    set -o errexit &&
                    exec /sccache-dist server --config {cfg}
                "#,
                    cfg = Path::new(CONTAINER_EXTERNAL_PATH)
                        .join(&cfg_file_name)
                        .to_str()
                        .unwrap()
                ),
            ])
            .output()
            .unwrap();

        check_output(&output);

        self.handles.push(DistHandle::Server(ServerHandle {
            globals: DistSystemGlobals::get(),
            handle: ResourceHandle::Container {
                id: server_id,
                cid: container_name,
            },
        }));

        self.wait_server_ready(self.servers().into_iter().last().unwrap());

        self
    }

    fn schedulers(&self) -> impl IntoIterator<Item = &SchedulerHandle> {
        self.handles.iter().filter_map(|component| {
            if let DistHandle::Scheduler(handle) = component {
                Some(handle)
            } else {
                None
            }
        })
    }

    fn servers(&self) -> impl IntoIterator<Item = &ServerHandle> {
        self.handles.iter().filter_map(|component| {
            if let DistHandle::Server(handle) = component {
                Some(handle)
            } else {
                None
            }
        })
    }

    pub fn scheduler(&self, scheduler_index: usize) -> Option<&SchedulerHandle> {
        self.schedulers().into_iter().nth(scheduler_index)
    }

    pub fn server(&self, server_index: usize) -> Option<&ServerHandle> {
        self.servers().into_iter().nth(server_index)
    }

    pub fn restart_server(&self, server: &ServerHandle) {
        match server.handle {
            ResourceHandle::Container { ref cid, .. } => {
                let output = Command::new("docker")
                    .args(["restart", cid])
                    .output()
                    .unwrap();
                check_output(&output);
            }
            ResourceHandle::Process { .. } => {
                // TODO: pretty easy, just no need yet
                panic!("restart not yet implemented for pids")
            }
        }
        self.wait_server_ready(server)
    }

    pub fn wait_server_ready(&self, server: &ServerHandle) {
        let server_id = match server.handle {
            ResourceHandle::Container { ref id, .. } => id,
            ResourceHandle::Process { ref id, .. } => id,
        };
        wait_for(
            || {
                let statuses = self
                    .schedulers()
                    .into_iter()
                    .map(|scheduler| scheduler.status())
                    .collect::<Vec<_>>();
                if statuses
                    .iter()
                    .any(|status| status.servers.iter().any(|server| &server.id == server_id))
                {
                    Ok(())
                } else {
                    Err(format!("{:?}", statuses))
                }
            },
            Duration::from_millis(1000),
            MAX_STARTUP_WAIT,
        );
    }
}

fn name_with_uuid(name: &str) -> String {
    format!(
        "{}_{}_{}",
        CONTAINER_NAME_PREFIX,
        name,
        Uuid::new_v4().simple()
    )
}

fn check_output(output: &Output) {
    if !output.status.success() {
        println!("{}\n\n[BEGIN STDOUT]\n===========\n{}\n===========\n[FIN STDOUT]\n\n[BEGIN STDERR]\n===========\n{}\n===========\n[FIN STDERR]\n\n",
            output.status, String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        panic!()
    }
}

fn wait_for_http(url: HTTPUrl, interval: Duration, max_wait: Duration) {
    // TODO: after upgrading to reqwest >= 0.9, use 'danger_accept_invalid_certs' and stick with that rather than tcp
    wait_for(
        || {
            let url = url.to_url();
            let url = url.socket_addrs(|| None).unwrap();
            match net::TcpStream::connect(url.as_slice()) {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        },
        interval,
        max_wait,
    )
}

fn wait_for<F: Fn() -> Result<(), String>>(f: F, interval: Duration, max_wait: Duration) {
    let start = Instant::now();
    let mut lasterr;
    loop {
        match tokio::task::block_in_place(&f) {
            Ok(()) => return,
            Err(e) => lasterr = e,
        }
        if start.elapsed() > max_wait {
            break;
        }
        thread::sleep(interval)
    }
    panic!("wait timed out, last error result: {}", lasterr)
}
