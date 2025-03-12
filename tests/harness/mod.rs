use fs_err as fs;
use std::{
    env,
    ffi::OsString,
    io::Write,
    net::{self, SocketAddr},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    str,
    sync::atomic::{AtomicU16, Ordering},
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
const DIST_IMAGE: &str = "sccache_dist_test_image";
const DIST_DOCKERFILE: &str = include_str!("Dockerfile.sccache-dist");
const DIST_IMAGE_BWRAP_PATH: &str = "/usr/bin/bwrap";
const MAX_STARTUP_WAIT: Duration = Duration::from_secs(30);

const CONFIGS_CONTAINER_PATH: &str = "/sccache-bits";
const BUILD_DIR_CONTAINER_PATH: &str = "/sccache-bits/build-dir";

const TC_CACHE_SIZE: u64 = 1024 * 1024 * 1024; // 1 gig

pub static CLIENT_PORT: AtomicU16 = AtomicU16::new(4227);
static SCHEDULER_PORT: AtomicU16 = AtomicU16::new(10500);
static RABBITMQ_PORT: AtomicU16 = AtomicU16::new(5673);
static REDIS_PORT: AtomicU16 = AtomicU16::new(6380);

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
                            .join("sccache_local_daemon.txt")
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
fn sccache_scheduler_cfg(tmpdir: &Path, message_broker: &MessageBroker) -> SchedulerConfig {
    let jobs_path = "server-jobs";
    let toolchains_path = "server-toolchains";
    fs::create_dir_all(tmpdir.join(toolchains_path)).unwrap();

    let mut config = SchedulerConfig::load(None).unwrap();
    let scheduler_port = SCHEDULER_PORT.fetch_add(1, Ordering::SeqCst);
    config.client_auth = ClientAuth::Insecure;
    config.message_broker = Some(message_broker.clone());
    config.public_addr = SocketAddr::from(([0, 0, 0, 0], scheduler_port));
    config.jobs.fallback.dir = Path::new(CONFIGS_CONTAINER_PATH).join(jobs_path);
    config.toolchains.fallback.dir = Path::new(CONFIGS_CONTAINER_PATH).join(toolchains_path);
    config
}

#[cfg(feature = "dist-server")]
fn sccache_server_cfg(
    tmpdir: &Path,
    message_broker: &MessageBroker,
    // server_ip: IpAddr,
) -> ServerConfig {
    let relpath = "server-cache";
    let jobs_path = "server-jobs";
    let toolchains_path = "server-toolchains";
    fs::create_dir_all(tmpdir.join(relpath)).unwrap();
    fs::create_dir_all(tmpdir.join(toolchains_path)).unwrap_or_default();

    let mut config = ServerConfig::load(None).unwrap();
    config.max_per_core_load = 0.0;
    config.max_per_core_prefetch = 0.0;
    config.message_broker = Some(message_broker.clone());
    config.builder = BuilderType::Overlay {
        build_dir: BUILD_DIR_CONTAINER_PATH.into(),
        bwrap_path: DIST_IMAGE_BWRAP_PATH.into(),
    };
    config.cache_dir = Path::new(CONFIGS_CONTAINER_PATH).join(relpath);
    config.jobs.fallback.dir = Path::new(CONFIGS_CONTAINER_PATH).join(jobs_path);
    config.toolchains.fallback.dir = Path::new(CONFIGS_CONTAINER_PATH).join(toolchains_path);
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
#[derive(Clone)]
pub enum ServerHandle {
    Container {
        id: String,
        cid: String,
    },
    Process {
        id: String,
        #[allow(dead_code)]
        pid: Pid,
    },
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
    default_scheduler: bool,
    default_server: bool,
    dist_system_name: String,
    job_time_limit: Option<u32>,
    custom_server: Option<Pid>,
    message_broker: Option<DistMessageBroker>,
    scheduler_jobs_storage: Option<DistMessageBroker>,
    scheduler_toolchains_storage: Option<DistMessageBroker>,
    server_jobs_storage: Option<DistMessageBroker>,
    server_toolchains_storage: Option<DistMessageBroker>,
}

#[cfg(feature = "dist-server")]
impl DistSystemBuilder {
    pub fn new() -> Self {
        Self {
            default_scheduler: false,
            default_server: false,
            custom_server: None,
            dist_system_name: make_container_name("sccache_dist_test"),
            job_time_limit: None,
            message_broker: None,
            scheduler_jobs_storage: None,
            scheduler_toolchains_storage: None,
            server_jobs_storage: None,
            server_toolchains_storage: None,
        }
    }

    pub fn with_name(self, name: &str) -> Self {
        Self {
            dist_system_name: name.to_owned(),
            ..self
        }
    }

    pub fn with_default_message_broker(self, message_broker: &str) -> Self {
        Self {
            message_broker: Some(DistMessageBroker::new(message_broker)),
            ..self
        }
    }

    pub fn with_default_scheduler(self) -> Self {
        Self {
            default_scheduler: true,
            ..self
        }
    }

    pub fn with_default_server(self) -> Self {
        Self {
            custom_server: None,
            default_server: true,
            ..self
        }
    }

    #[allow(dead_code)]
    pub fn with_custom_server(self, child: Pid) -> Self {
        Self {
            custom_server: Some(child),
            default_server: false,
            ..self
        }
    }

    pub fn with_job_time_limit(self, job_time_limit: u32) -> Self {
        Self {
            job_time_limit: Some(job_time_limit),
            ..self
        }
    }

    pub fn with_message_broker(self, message_broker: &DistMessageBroker) -> Self {
        Self {
            message_broker: Some(message_broker.clone()),
            ..self
        }
    }

    pub fn with_scheduler_jobs_storage(self, redis_storage: &DistMessageBroker) -> Self {
        Self {
            scheduler_jobs_storage: Some(redis_storage.clone()),
            ..self
        }
    }

    pub fn with_scheduler_toolchains_storage(self, redis_storage: &DistMessageBroker) -> Self {
        Self {
            scheduler_toolchains_storage: Some(redis_storage.clone()),
            ..self
        }
    }

    pub fn with_server_jobs_storage(self, redis_storage: &DistMessageBroker) -> Self {
        Self {
            server_jobs_storage: Some(redis_storage.clone()),
            ..self
        }
    }

    pub fn with_server_toolchains_storage(self, redis_storage: &DistMessageBroker) -> Self {
        Self {
            server_toolchains_storage: Some(redis_storage.clone()),
            ..self
        }
    }

    pub fn build(self) -> DistSystem {
        let name = &self.dist_system_name;
        let mut system = DistSystem::new(name);
        let message_broker = self.message_broker.clone().unwrap();

        system.add_message_broker(&message_broker);

        if self.default_scheduler {
            let mut cfg = SchedulerConfig {
                scheduler_id: name.to_owned(),
                ..system.scheduler_cfg(&message_broker.config)
            };
            if let Some(job_time_limit) = self.job_time_limit {
                cfg.job_time_limit = job_time_limit;
            }
            if let Some(jobs_storage) = self.scheduler_jobs_storage {
                cfg.jobs = system.add_redis_storage(jobs_storage);
            }
            if let Some(toolchains_storage) = self.scheduler_toolchains_storage {
                cfg.toolchains = system.add_redis_storage(toolchains_storage);
            }
            system.add_scheduler(cfg);
        }

        if self.default_server {
            let mut cfg = ServerConfig {
                server_id: name.to_owned(),
                ..system.server_cfg(&message_broker.config)
            };
            if let Some(jobs_storage) = self.server_jobs_storage {
                cfg.jobs = system.add_redis_storage(jobs_storage);
            }
            if let Some(toolchains_storage) = self.server_toolchains_storage {
                cfg.toolchains = system.add_redis_storage(toolchains_storage);
            }
            system.add_server(cfg);
        }

        if let Some(custom_server_pid) = self.custom_server {
            system.add_custom_server(name, custom_server_pid);
        }

        system
    }
}

#[cfg(feature = "dist-server")]
pub struct DistSystem {
    handles: Vec<DistHandle>,
    data_dir: tempfile::TempDir,
    dist_dir: PathBuf,
    sccache_dist: PathBuf,
}

#[cfg(feature = "dist-server")]
impl DistSystem {
    pub fn builder() -> DistSystemBuilder {
        DistSystemBuilder::new()
    }

    fn new(name: &str) -> Self {
        // Make sure the docker image is available, building it if necessary
        let mut child = Command::new("docker")
            .args(["build", "-q", "-t", DIST_IMAGE, "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(DIST_DOCKERFILE.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        check_output(&output);

        let data_dir = tempfile::Builder::new()
            .prefix(&format!("sccache_dist_{name}"))
            .tempdir()
            .unwrap();

        let dist_dir = data_dir.path().join("distsystem");
        fs::create_dir_all(&dist_dir).unwrap();

        Self {
            handles: vec![],
            data_dir,
            dist_dir,
            sccache_dist: sccache_dist_path(),
        }
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir.path()
    }

    pub fn dist_dir(&self) -> &Path {
        self.dist_dir.as_path()
    }

    pub fn new_client(&self, client_config: &FileConfig) -> SccacheClient {
        let data_dir = self.data_dir();
        write_json_cfg(data_dir, "sccache-client.json", client_config);
        SccacheClient::new(
            &data_dir.join("sccache-client.json"),
            &data_dir.join("sccache-cached-cfg"),
        )
    }

    pub fn add_message_broker(&mut self, message_broker: &DistMessageBroker) -> &mut Self {
        let DistMessageBroker {
            image,
            host_port,
            container_port,
            ..
        } = message_broker;

        let container_name = make_container_name("message_broker");
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

        self.handles
            .push(DistHandle::MessageBroker(MessageBrokerHandle {
                broker: message_broker.clone(),
                handle: ServerHandle::Container {
                    id: container_name.clone(),
                    cid: container_name,
                },
            }));

        self
    }

    pub fn scheduler_cfg(&self, message_broker: &MessageBroker) -> SchedulerConfig {
        sccache_scheduler_cfg(self.dist_dir(), message_broker)
    }

    pub fn add_scheduler(&mut self, scheduler_cfg: SchedulerConfig) -> &mut Self {
        let scheduler_id = scheduler_cfg.scheduler_id.clone();
        let scheduler_port = scheduler_cfg.public_addr.port();
        let container_name = make_container_name(&format!("{scheduler_id}_scheduler"));
        let scheduler_cfg_relpath = "scheduler-cfg.json";
        let scheduler_cfg_path = self.dist_dir().join(scheduler_cfg_relpath);
        let scheduler_cfg_container_path =
            Path::new(CONFIGS_CONTAINER_PATH).join(scheduler_cfg_relpath);
        fs::File::create(scheduler_cfg_path)
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
                    CONFIGS_CONTAINER_PATH
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
                    cfg = scheduler_cfg_container_path.to_str().unwrap()
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
        sccache_server_cfg(self.dist_dir(), message_broker)
    }

    pub fn add_server(&mut self, server_cfg: ServerConfig) -> &mut Self {
        let server_id = server_cfg.server_id.clone();
        let container_name = make_container_name(&format!("{server_id}_server"));
        let server_cfg_relpath = format!("server-cfg-{}.json", self.handles.len());
        let server_cfg_path = self.dist_dir().join(&server_cfg_relpath);
        let server_cfg_container_path = Path::new(CONFIGS_CONTAINER_PATH).join(server_cfg_relpath);
        fs::File::create(server_cfg_path)
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
                    CONFIGS_CONTAINER_PATH
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
                    cfg = server_cfg_container_path.to_str().unwrap()
                ),
            ])
            .output()
            .unwrap();

        check_output(&output);

        self.handles
            .push(DistHandle::Server(ServerHandle::Container {
                id: server_id,
                cid: container_name,
            }));

        self.wait_server_ready(self.servers().into_iter().last().unwrap());

        self
    }

    pub fn add_redis_storage(self: &mut DistSystem, redis: DistMessageBroker) -> StorageConfig {
        if !self
            .message_brokers()
            .into_iter()
            .any(|b| b.broker == redis)
        {
            self.add_message_broker(&redis);
        }
        StorageConfig {
            storage: Some(CacheType::Redis(RedisCacheConfig {
                endpoint: Some(redis.url().to_url().to_string()),
                ..Default::default()
            })),
            fallback: DiskCacheConfig {
                dir: self.dist_dir().join("not_used"),
                ..Default::default()
            },
        }
    }

    pub fn add_custom_server(&mut self, server_id: &str, pid: Pid) -> &Self {
        self.handles.push(DistHandle::Server(ServerHandle::Process {
            pid,
            id: server_id.to_owned(),
        }));

        self.wait_server_ready(self.servers().into_iter().last().unwrap());

        self
    }

    fn message_brokers(&self) -> impl IntoIterator<Item = &MessageBrokerHandle> {
        self.handles.iter().filter_map(|component| {
            if let DistHandle::MessageBroker(handle) = component {
                Some(handle)
            } else {
                None
            }
        })
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

    #[allow(unused)]
    pub fn message_broker(&self, message_broker_index: usize) -> Option<&MessageBrokerHandle> {
        self.message_brokers().into_iter().nth(message_broker_index)
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

// If you want containers to hang around (e.g. for debugging), comment out the "rm -f" lines
#[cfg(feature = "dist-server")]
impl Drop for DistSystem {
    fn drop(&mut self) {
        let mut did_err = false;

        // Panicking halfway through drop would either abort (if it's a double panic) or leave us with
        // resources that aren't yet cleaned up. Instead, do as much as possible then decide what to do
        // at the end - panic (if not already doing so) or let the panic continue
        macro_rules! droperr {
            ($e:expr) => {
                match $e {
                    Ok(()) => (),
                    Err(e) => {
                        did_err = true;
                        eprintln!("Error with {}: {}", stringify!($e), e)
                    }
                }
            };
        }

        let mut logs = vec![];
        let mut outputs = vec![];
        let mut exits = vec![];

        let mut handles: Vec<&ServerHandle> = vec![];
        // Kill servers first
        handles.extend(self.servers());
        // Then kill schedulers
        handles.extend(self.schedulers().into_iter().map(|s| &s.handle));
        // Message brokers last
        handles.extend(self.message_brokers().into_iter().map(|s| &s.handle));

        for handle in handles.iter() {
            match handle {
                ServerHandle::Container { cid, id } => {
                    droperr!(Command::new("docker")
                        .args(["logs", cid])
                        .output()
                        .map(|o| logs.push((cid, o))));
                    droperr!(Command::new("docker")
                        .args(["rm", "-f", cid])
                        .output()
                        .map(|o| outputs.push((id, o))));
                }
                ServerHandle::Process { id, pid } => {
                    droperr!(nix::sys::signal::kill(*pid, Signal::SIGINT));
                    thread::sleep(Duration::from_secs(5));
                    let mut killagain = true; // Default to trying to kill again, e.g. if there was an error waiting on the pid
                    droperr!(
                        nix::sys::wait::waitpid(*pid, Some(WaitPidFlag::WNOHANG)).map(|ws| {
                            if ws != WaitStatus::StillAlive {
                                killagain = false;
                                exits.push(ws)
                            }
                        })
                    );
                    if killagain {
                        eprintln!("[{pid}, {id})]: SIGINT didn't kill process, trying SIGKILL");
                        droperr!(nix::sys::signal::kill(*pid, Signal::SIGKILL));
                        droperr!(nix::sys::wait::waitpid(*pid, Some(WaitPidFlag::WNOHANG))
                            .map_err(|e| e.to_string())
                            .and_then(|ws| if ws == WaitStatus::StillAlive {
                                Err(format!("[{pid}, {id})]: process alive after sigkill"))
                            } else {
                                exits.push(ws);
                                Ok(())
                            }));
                    }
                }
            }
        }

        for (
            id,
            Output {
                status,
                stdout,
                stderr,
            },
        ) in logs
        {
            println!(
                "LOGS == ({}) ==\n> {} <:\n## STDOUT\n{}\n\n## STDERR\n{}\n====",
                status,
                id,
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }

        for (
            id,
            Output {
                status,
                stdout,
                stderr,
            },
        ) in outputs
        {
            println!(
                "OUTPUTS == ({}) ==\n> {} <:\n## STDOUT\n{}\n\n## STDERR\n{}\n====",
                status,
                id,
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }

        for exit in exits {
            println!("EXIT: {:?}", exit)
        }

        if did_err && !thread::panicking() {
            panic!("Encountered failures during dist system teardown")
        }
    }
}

fn make_container_name(tag: &str) -> String {
    format!(
        "{}_{}_{}",
        CONTAINER_NAME_PREFIX,
        tag,
        Uuid::new_v4().hyphenated()
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
        match f() {
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
