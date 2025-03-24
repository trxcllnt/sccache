use bytes::Buf;
use fs_err as fs;
use itertools::Itertools;
use std::{
    collections::HashMap,
    env,
    ffi::OsString,
    io::{BufRead, Write},
    net::{self, SocketAddr},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    str,
    sync::{
        atomic::{AtomicU16, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(feature = "dist-server")]
use nix::{
    sys::{
        signal::Signal,
        wait::{WaitPidFlag, WaitStatus},
    },
    unistd::Pid,
};

use once_cell::sync::Lazy;

#[cfg(feature = "dist-server")]
use sccache::{
    config::{
        scheduler::ClientAuth, scheduler::Config as SchedulerConfig, server::BuilderType,
        server::Config as ServerConfig, CacheType, HTTPUrl, MessageBroker, RedisCacheConfig,
        StorageConfig, INSECURE_DIST_CLIENT_TOKEN,
    },
    dist,
};
use sccache::{
    config::{CacheConfigs, DiskCacheConfig, DistConfig, FileConfig, PreprocessorCacheModeConfig},
    server::{ServerInfo, ServerStats},
};

use serde::Serialize;
use uuid::Uuid;

const CONTAINER_NAME_PREFIX: &str = "sccache_dist";
const CONTAINER_EXTERNAL_PATH: &str = "/sccache/external";
const CONTAINER_INTERNAL_PATH: &str = "/sccache/internal";
const DIST_IMAGE: &str = "sccache_dist_test_image";
const DIST_DOCKERFILE: &str = include_str!("Dockerfile.sccache-dist");
const DIST_IMAGE_BWRAP_PATH: &str = "/usr/bin/bwrap";
const MAX_STARTUP_WAIT: Duration = Duration::from_secs(30);

const TC_CACHE_SIZE: u64 = 1024 * 1024 * 1024; // 1 gig

static CLIENT_PORT: AtomicU16 = AtomicU16::new(4227);
static SCHEDULER_PORT: AtomicU16 = AtomicU16::new(10500);
static RABBITMQ_PORT: AtomicU16 = AtomicU16::new(5672);
static REDIS_PORT: AtomicU16 = AtomicU16::new(6379);

pub struct SccacheClient {
    envvars: Vec<(OsString, OsString)>,
    pub path: PathBuf,
}

#[allow(unused)]
impl SccacheClient {
    pub fn new_no_cfg() -> Self {
        let path = assert_cmd::cargo::cargo_bin("sccache");
        let port = CLIENT_PORT.fetch_add(1, Ordering::SeqCst);

        let mut envvars = vec![
            ("SCCACHE_SERVER_PORT".into(), port.to_string().into()),
            ("TOKIO_WORKER_THREADS".into(), "2".into()),
        ];

        // Send daemon logs to a file if SCCACHE_DEBUG is defined
        if env::var("SCCACHE_DEBUG").is_ok() {
            envvars.extend_from_slice(&[
                // Allow overriding log level
                (
                    "SCCACHE_SERVER_LOG".into(),
                    env::var_os("SCCACHE_SERVER_LOG")
                        .or(env::var_os("SCCACHE_LOG"))
                        .unwrap_or("sccache=trace".into()),
                ),
                // Allow overriding log output path
                (
                    "SCCACHE_ERROR_LOG".into(),
                    env::var_os("SCCACHE_ERROR_LOG").unwrap_or(
                        env::temp_dir()
                            .join(format!("sccache_local_daemon.{port}.txt"))
                            .into_os_string(),
                    ),
                ),
            ]);
        }

        Self { envvars, path }
    }

    pub fn new(cfg_path: &Path, cached_cfg_path: &Path) -> Self {
        let mut this = Self::new_no_cfg();
        this.envvars.push(("SCCACHE_CONF".into(), cfg_path.into()));
        this.envvars
            .push(("SCCACHE_CACHED_CONF".into(), cached_cfg_path.into()));
        this
    }

    pub fn start(self) -> Self {
        trace!("sccache --start-server");
        // Don't run this with run() because on Windows `wait_with_output`
        // will hang because the internal server process is not detached.
        if !self
            .cmd()
            .arg("--start-server")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
        {
            panic!("Failed to start local daemon");
        }
        self
    }

    pub fn stop(&self) -> bool {
        trace!("sccache --stop-server");
        self.cmd()
            .arg("--stop-server")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    pub fn cmd(&self) -> Command {
        let mut cmd = prune_command(Command::new(assert_cmd::cargo::cargo_bin("sccache")));
        cmd.envs(
            self.envvars
                .iter()
                .map(|(k, v)| (k.as_os_str(), v.as_os_str())),
        );
        cmd
    }

    pub fn info(&self) -> sccache::errors::Result<ServerInfo> {
        self.cmd()
            .args(["--show-stats", "--stats-format=json"])
            .output()
            .map_err(anyhow::Error::new)
            .map_err(|e| e.context("`sccache --show-stats --stats-format=json` failed"))
            .map(|output| {
                let s = str::from_utf8(&output.stdout).expect("Output not UTF-8");
                serde_json::from_str(s).expect("Failed to parse JSON stats")
            })
    }

    pub fn stats(&self) -> sccache::errors::Result<ServerStats> {
        self.info().map(|info| info.stats)
    }

    pub fn zero_stats(&self) {
        trace!("sccache --zero-stats");
        drop(
            self.cmd()
                .arg("--zero-stats")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status(),
        );
    }
}

impl Drop for SccacheClient {
    fn drop(&mut self) {
        self.stop();
    }
}

pub fn write_json_cfg<T: Serialize>(path: &Path, filename: &str, contents: &T) {
    let p = path.join(filename);
    let mut f = fs::File::create(p).unwrap();
    f.write_all(&serde_json::to_vec(contents).unwrap()).unwrap();
}

pub fn write_source(path: &Path, filename: &str, contents: &str) {
    let p = path.join(filename);
    let mut f = fs::File::create(p).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
}

pub fn init_cargo(path: &Path, cargo_name: &str) -> PathBuf {
    let cargo_path = path.join(cargo_name);
    let source_path = "src";
    fs::create_dir_all(cargo_path.join(source_path)).unwrap();
    cargo_path
}

// Prune any environment variables that could adversely affect test execution.
pub fn prune_command(mut cmd: Command) -> Command {
    use sccache::util::OsStrExt;

    for (var, _) in env::vars_os() {
        if var.starts_with("SCCACHE_") {
            cmd.env_remove(var);
        }
    }
    cmd
}

pub fn cargo_command() -> Command {
    prune_command(Command::new("cargo"))
}

#[cfg(feature = "dist-server")]
pub fn sccache_dist_path() -> PathBuf {
    assert_cmd::cargo::cargo_bin("sccache-dist")
}

pub fn sccache_client_cfg(tmpdir: &Path, preprocessor_cache_mode: bool) -> FileConfig {
    let cache_relpath = "client-cache";
    let dist_cache_relpath = "client-dist-cache";
    fs::create_dir_all(tmpdir.join(cache_relpath)).unwrap();
    fs::create_dir_all(tmpdir.join(dist_cache_relpath)).unwrap();

    let disk_cache = DiskCacheConfig {
        dir: tmpdir.join(cache_relpath),
        preprocessor_cache_mode: PreprocessorCacheModeConfig {
            use_preprocessor_cache_mode: preprocessor_cache_mode,
            ..Default::default()
        },
        ..Default::default()
    };
    FileConfig {
        cache: CacheConfigs {
            azure: None,
            disk: Some(disk_cache),
            gcs: None,
            gha: None,
            memcached: None,
            redis: None,
            s3: None,
            webdav: None,
            oss: None,
        },
        dist: DistConfig {
            auth: Default::default(), // dangerously_insecure
            scheduler_url: None,
            cache_dir: tmpdir.join(dist_cache_relpath),
            toolchains: vec![],
            toolchain_cache_size: TC_CACHE_SIZE,
            rewrite_includes_only: false, // TODO
            ..Default::default()
        },
        server_startup_timeout_ms: None,
    }
}

#[cfg(feature = "dist-server")]
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

#[cfg(feature = "dist-server")]
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

#[cfg(feature = "dist-server")]
#[derive(Clone, Eq, PartialEq)]
pub struct DistMessageBroker {
    url: String,
    host_port: u16,
    container_port: u16,
    config: MessageBroker,
    image: String,
}

#[cfg(feature = "dist-server")]
#[allow(unused)]
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

    pub fn config(&self) -> MessageBroker {
        self.config.clone()
    }

    pub fn url(&self) -> HTTPUrl {
        HTTPUrl::from_url(reqwest::Url::parse(&self.url).unwrap())
    }
}

#[cfg(feature = "dist-server")]
#[derive(Clone)]
pub struct MessageBrokerHandle {
    broker: DistMessageBroker,
    #[allow(unused)]
    handle: ServerHandle,
}

#[cfg(feature = "dist-server")]
#[allow(unused)]
impl MessageBrokerHandle {
    pub fn is_amqp(&self) -> bool {
        self.broker.is_amqp()
    }
    pub fn is_redis(&self) -> bool {
        self.broker.is_redis()
    }
}

#[cfg(feature = "dist-server")]
#[derive(Clone)]
pub struct SchedulerHandle {
    port: u16,
    #[allow(unused)]
    handle: ServerHandle,
}

#[cfg(feature = "dist-server")]
impl SchedulerHandle {
    pub fn url(&self) -> HTTPUrl {
        let url = format!("http://127.0.0.1:{}", self.port);
        HTTPUrl::from_url(reqwest::Url::parse(&url).unwrap())
    }
    pub fn status(&self) -> dist::SchedulerStatus {
        let mut req = reqwest::blocking::Client::builder()
            .build()
            .unwrap()
            .get(dist::http::urls::scheduler_status(&self.url().to_url()));
        req = req.bearer_auth(INSECURE_DIST_CLIENT_TOKEN);
        let res = req.send().unwrap();
        assert!(res.status().is_success());
        bincode::deserialize_from(res).unwrap()
    }
}

#[cfg(feature = "dist-server")]
static DIST_SYSTEM_GLOBALS: Lazy<Arc<DistSystemGlobals>> = Lazy::new(DistSystemGlobals::new);
static DIST_SYSTEM_GLOBALS_TEARDOWN: once_cell::sync::OnceCell<tokio::task::JoinHandle<()>> =
    once_cell::sync::OnceCell::new();

#[cfg(feature = "dist-server")]
struct DistSystemGlobals {
    pub dropped_handles_count: AtomicU64,
    message_brokers: Mutex<Vec<DistHandle>>,
    #[allow(unused)]
    handle: &'static tokio::task::JoinHandle<()>,
    errors: Mutex<HashMap<(u64, String), std::process::Output>>,
    output: Mutex<HashMap<(u64, String), std::process::Output>>,
}

#[cfg(feature = "dist-server")]
impl DistSystemGlobals {
    pub fn new() -> Arc<Self> {
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

        Arc::new(Self {
            dropped_handles_count: AtomicU64::new(0),
            message_brokers: Mutex::new(vec![
                Self::add_message_broker("rabbitmq"),
                Self::add_message_broker("redis"),
            ]),
            errors: Mutex::new(HashMap::new()),
            output: Mutex::new(HashMap::new()),
            handle: DIST_SYSTEM_GLOBALS_TEARDOWN
                .get_or_init(|| Self::exit_after_delay(Duration::from_secs(1))),
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
            handle: ServerHandle::Container {
                id: container_name.clone(),
                cid: container_name,
                log_outputs: false,
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

    pub fn errors(&self, idx: u64, id: &str, o: std::process::Output) {
        self.errors
            .lock()
            .unwrap()
            .insert((idx, id.to_owned()), o.clone());
    }

    pub fn output(&self, idx: u64, id: &str, o: std::process::Output) {
        self.output
            .lock()
            .unwrap()
            .insert((idx, id.to_owned()), o.clone());
    }

    fn exit_after_delay(interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn_blocking(move || {
            let mut attempts = 0;
            loop {
                std::thread::sleep(interval);
                if Arc::strong_count(&DIST_SYSTEM_GLOBALS) <= 1 {
                    if attempts >= 5 {
                        DIST_SYSTEM_GLOBALS.exit();
                        break;
                    }
                    attempts += 1;
                } else {
                    attempts = 0;
                }
            }
        })
    }

    fn exit(&self) {
        self.message_brokers.lock().unwrap().drain(..);
        self.print();
    }

    fn print(&self) {
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

        for ((_, id), output) in self
            .output
            .lock()
            .unwrap()
            .drain()
            .sorted_by(|a, b| a.0 .0.cmp(&b.0 .0))
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
            .errors
            .lock()
            .unwrap()
            .drain()
            .sorted_by(|a, b| a.0 .0.cmp(&b.0 .0))
        {
            print_output(
                &format!(
                    "=== server errors {}\n\nid: {id}\n{}",
                    "=".repeat(61),
                    errors.status
                ),
                &errors,
            );
        }
    }
}

#[cfg(feature = "dist-server")]
#[derive(Clone)]
pub enum ServerHandle {
    Container {
        id: String,
        cid: String,
        log_outputs: bool,
    },
    #[allow(unused)]
    Process {
        id: String,
        #[allow(dead_code)]
        pid: Pid,
    },
}

// If you want containers to hang around (e.g. for debugging), comment out the "docker rm -f" lines
#[cfg(feature = "dist-server")]
impl Drop for ServerHandle {
    fn drop(&mut self) {
        let drop_index = DIST_SYSTEM_GLOBALS
            .dropped_handles_count
            .fetch_add(1, Ordering::SeqCst);

        // Panicking halfway through drop would either abort (if it's a double panic) or leave us with
        // resources that aren't yet cleaned up. Instead, do as much as possible then decide what to do
        // at the end - panic (if not already doing so) or let the panic continue
        macro_rules! log_err {
            ($e:expr) => {
                match $e {
                    Ok(()) => (),
                    Err((id, err)) => {
                        DIST_SYSTEM_GLOBALS.errors(
                            drop_index,
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
            ServerHandle::Container {
                ref id,
                cid,
                log_outputs,
            } => {
                log_err!(Command::new("docker")
                    .args(["logs", cid])
                    .output()
                    .map(|o| {
                        if *log_outputs {
                            DIST_SYSTEM_GLOBALS.output(drop_index, cid, o);
                        }
                    })
                    .map_err(|e| (id, format!("[{id}, {cid})]: {e:?}"))));
                log_err!(Command::new("docker")
                    .args(["kill", cid])
                    .output()
                    .map(|_| ())
                    .map_err(|e| (id, format!("[{id}, {cid})]: {e:?}"))));
                log_err!(Command::new("docker")
                    .args(["rm", "-f", cid])
                    .output()
                    .map(|_| ())
                    .map_err(|e| (id, format!("[{id}, {cid})]: {e:?}"))));
            }
            ServerHandle::Process { ref id, pid } => {
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

#[cfg(feature = "dist-server")]
#[derive(Clone)]
pub enum DistHandle {
    MessageBroker(MessageBrokerHandle),
    Scheduler(SchedulerHandle),
    Server(ServerHandle),
}

#[cfg(feature = "dist-server")]
pub struct DistSystemBuilder {
    scheduler_count: u64,
    server_count: u64,
    dist_system_name: String,
    job_time_limit: Option<u32>,
    message_broker: Option<DistMessageBroker>,
    scheduler_jobs_redis_storage: Option<DistMessageBroker>,
    scheduler_toolchains_redis_storage: Option<DistMessageBroker>,
    server_jobs_redis_storage: Option<DistMessageBroker>,
    server_toolchains_redis_storage: Option<DistMessageBroker>,
}

#[cfg(feature = "dist-server")]
impl DistSystemBuilder {
    pub fn new() -> Self {
        Self {
            dist_system_name: name_with_uuid("sccache_dist_test"),
            job_time_limit: None,
            message_broker: None,
            scheduler_count: 0,
            server_count: 0,
            scheduler_jobs_redis_storage: None,
            scheduler_toolchains_redis_storage: None,
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
            message_broker: DIST_SYSTEM_GLOBALS.message_broker(message_broker),
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
            scheduler_jobs_redis_storage: DIST_SYSTEM_GLOBALS.redis(),
            ..self
        }
    }

    pub fn with_scheduler_toolchains_redis_storage(self) -> Self {
        Self {
            scheduler_toolchains_redis_storage: DIST_SYSTEM_GLOBALS.redis(),
            ..self
        }
    }

    pub fn with_server_jobs_redis_storage(self) -> Self {
        Self {
            server_jobs_redis_storage: DIST_SYSTEM_GLOBALS.redis(),
            ..self
        }
    }

    pub fn with_server_toolchains_redis_storage(self) -> Self {
        Self {
            server_toolchains_redis_storage: DIST_SYSTEM_GLOBALS.redis(),
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

#[cfg(feature = "dist-server")]
pub struct DistSystem {
    name: String,
    handles: Vec<DistHandle>,
    data_dir: tempfile::TempDir,
    dist_dir: PathBuf,
    sccache_dist: PathBuf,
    #[allow(unused)]
    globals: Arc<DistSystemGlobals>,
}

#[cfg(feature = "dist-server")]
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
            sccache_dist: sccache_dist_path(),
            globals: Arc::clone(&DIST_SYSTEM_GLOBALS),
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
            handle: ServerHandle::Container {
                id: scheduler_id,
                cid: container_name,
                log_outputs: true,
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
                if matches!(status, dist::SchedulerStatus { .. }) {
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

        self.handles
            .push(DistHandle::Server(ServerHandle::Container {
                id: server_id,
                cid: container_name,
                log_outputs: true,
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

    pub fn restart_server(&self, handle: &ServerHandle) {
        match handle {
            ServerHandle::Container { cid, .. } => {
                let output = Command::new("docker")
                    .args(["restart", cid])
                    .output()
                    .unwrap();
                check_output(&output);
            }
            ServerHandle::Process { .. } => {
                // TODO: pretty easy, just no need yet
                panic!("restart not yet implemented for pids")
            }
        }
        self.wait_server_ready(handle)
    }

    pub fn wait_server_ready(&self, handle: &ServerHandle) {
        let server_id = match handle {
            ServerHandle::Container { id, .. } => id,
            ServerHandle::Process { id, .. } => id,
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

#[cfg(feature = "dist-server")]
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
