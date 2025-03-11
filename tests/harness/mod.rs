use fs_err as fs;
use sccache::server::ServerInfo;
#[cfg(any(feature = "dist-client", feature = "dist-server"))]
use sccache::{
    cache::storage_from_config,
    config::{CacheType, DiskCacheConfig, HTTPUrl},
};
use std::env;
use std::io::Write;
use std::net::{self, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::str;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(feature = "dist-server")]
use nix::{
    sys::{
        signal::Signal,
        wait::{WaitPidFlag, WaitStatus},
    },
    unistd::Pid,
};
#[cfg(feature = "dist-server")]
use sccache::{config::MessageBroker, dist};

use serde::Serialize;
use uuid::Uuid;

const CONTAINER_NAME_PREFIX: &str = "sccache_dist_test";
const DIST_IMAGE: &str = "sccache_dist_test_image";
const DIST_DOCKERFILE: &str = include_str!("Dockerfile.sccache-dist");
const DIST_IMAGE_BWRAP_PATH: &str = "/usr/bin/bwrap";
const MAX_STARTUP_WAIT: Duration = Duration::from_secs(5);

const CONFIGS_CONTAINER_PATH: &str = "/sccache-bits";
const BUILD_DIR_CONTAINER_PATH: &str = "/sccache-bits/build-dir";
const SCHEDULER_PORT: u16 = 10500;

const TC_CACHE_SIZE: u64 = 1024 * 1024 * 1024; // 1 gig

pub fn start_local_daemon(cfg_path: &Path, cached_cfg_path: &Path) {
    let mut cmd = sccache_command();

    cmd.arg("--start-server");

    // Send daemon logs to a file if SCCACHE_DEBUG is defined
    if env::var("SCCACHE_DEBUG").is_ok() {
        cmd.env(
            "SCCACHE_LOG",
            // Allow overriding log level
            env::var("SCCACHE_LOG").unwrap_or("sccache=trace".into()),
        )
        .env("RUST_LOG_STYLE", "never")
        .env(
            "SCCACHE_ERROR_LOG",
            // Allow overriding log output path
            env::var_os("SCCACHE_ERROR_LOG").unwrap_or(
                env::temp_dir()
                    .join("sccache_local_daemon.txt")
                    .into_os_string(),
            ),
        );
    }

    cmd.env("SCCACHE_CONF", cfg_path)
        .env("SCCACHE_CACHED_CONF", cached_cfg_path);

    // Don't run this with run() because on Windows `wait_with_output`
    // will hang because the internal server process is not detached.
    if !cmd.status().unwrap().success() {
        panic!("Failed to start local daemon");
    }
}

pub fn stop_local_daemon() -> bool {
    trace!("sccache --stop-server");
    sccache_command()
        .arg("--stop-server")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

pub fn server_info() -> ServerInfo {
    let output = sccache_command()
        .args(["--show-stats", "--stats-format=json"])
        .output()
        .expect("`sccache --show-stats --stats-format=json` failed")
        .stdout;

    let s = str::from_utf8(&output).expect("Output not UTF-8");
    serde_json::from_str(s).expect("Failed to parse JSON stats")
}

pub fn get_stats<F: 'static + Fn(ServerInfo)>(f: F) {
    let info = server_info();
    eprintln!("get server stats: {info:?}");
    f(info);
}

#[allow(unused)]
pub fn zero_stats() {
    trace!("sccache --zero-stats");
    drop(
        sccache_command()
            .arg("--zero-stats")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status(),
    );
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

pub fn sccache_command() -> Command {
    prune_command(Command::new(assert_cmd::cargo::cargo_bin("sccache")))
}

pub fn cargo_command() -> Command {
    prune_command(Command::new("cargo"))
}

#[cfg(feature = "dist-server")]
pub fn sccache_dist_path() -> PathBuf {
    assert_cmd::cargo::cargo_bin("sccache-dist")
}

pub fn sccache_client_cfg(
    tmpdir: &Path,
    preprocessor_cache_mode: bool,
) -> sccache::config::FileConfig {
    let cache_relpath = "client-cache";
    let dist_cache_relpath = "client-dist-cache";
    fs::create_dir(tmpdir.join(cache_relpath)).unwrap();
    fs::create_dir(tmpdir.join(dist_cache_relpath)).unwrap();

    let disk_cache = sccache::config::DiskCacheConfig {
        dir: tmpdir.join(cache_relpath),
        preprocessor_cache_mode: sccache::config::PreprocessorCacheModeConfig {
            use_preprocessor_cache_mode: preprocessor_cache_mode,
            ..Default::default()
        },
        ..Default::default()
    };
    sccache::config::FileConfig {
        cache: sccache::config::CacheConfigs {
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
        dist: sccache::config::DistConfig {
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
fn sccache_scheduler_cfg(
    tmpdir: &Path,
    message_broker: &MessageBroker,
) -> sccache::config::scheduler::Config {
    let jobs_path = "server-jobs";
    let toolchains_path = "server-toolchains";
    fs::create_dir(tmpdir.join(toolchains_path)).unwrap();

    let mut config = sccache::config::scheduler::Config::load(None).unwrap();
    config.message_broker = Some(message_broker.clone());
    config.public_addr = SocketAddr::from(([0, 0, 0, 0], SCHEDULER_PORT));
    config.client_auth = sccache::config::scheduler::ClientAuth::Insecure;
    config.jobs.fallback.dir = Path::new(CONFIGS_CONTAINER_PATH).join(jobs_path);
    config.toolchains.fallback.dir = Path::new(CONFIGS_CONTAINER_PATH).join(toolchains_path);
    config
}

#[cfg(feature = "dist-server")]
fn sccache_server_cfg(
    tmpdir: &Path,
    message_broker: &MessageBroker,
    // server_ip: IpAddr,
) -> sccache::config::server::Config {
    let relpath = "server-cache";
    let jobs_path = "server-jobs";
    let toolchains_path = "server-toolchains";
    fs::create_dir(tmpdir.join(relpath)).unwrap();
    fs::create_dir(tmpdir.join(toolchains_path)).unwrap_or_default();

    let mut config = sccache::config::server::Config::load(None).unwrap();
    config.message_broker = Some(message_broker.clone());
    config.builder = sccache::config::server::BuilderType::Overlay {
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
pub enum ServerHandle {
    Container {
        id: String,
    },
    Process {
        id: String,
        #[allow(dead_code)]
        pid: Pid,
    },
}

#[cfg(feature = "dist-server")]
pub struct DistSystem {
    sccache_dist: PathBuf,
    tmpdir: PathBuf,

    message_broker_names: Option<Vec<String>>,
    scheduler_name: Option<String>,
    server_names: Vec<String>,
    server_pids: Vec<Pid>,
}

#[cfg(feature = "dist-server")]
impl DistSystem {
    pub fn new(sccache_dist: &Path, tmpdir: &Path) -> Self {
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

        let tmpdir = tmpdir.join("distsystem");
        fs::create_dir(&tmpdir).unwrap();

        Self {
            sccache_dist: sccache_dist.to_owned(),
            tmpdir,

            message_broker_names: None,
            scheduler_name: None,
            server_names: vec![],
            server_pids: vec![],
        }
    }

    pub fn message_broker_cfg(message_broker: &str) -> MessageBroker {
        match message_broker {
            "rabbitmq" => MessageBroker::AMQP(Self::rabbit_mq_url()),
            "redis" => MessageBroker::Redis(Self::redis_url()),
            _ => unreachable!(""),
        }
    }

    pub fn rabbit_mq_url() -> String {
        "amqp://127.0.0.1:5672//".into()
    }

    pub fn redis_url() -> String {
        "redis://127.0.0.1:6379/".into()
    }

    pub fn add_message_broker(&mut self, message_broker: &str) -> MessageBroker {
        match message_broker {
            "rabbitmq" => self.add_rabbit_mq(),
            "redis" => self.add_redis(),
            _ => unreachable!(""),
        }
    }

    pub fn add_rabbit_mq(&mut self) -> MessageBroker {
        self.run_message_broker("rabbitmq:4", "5672:5672");
        MessageBroker::AMQP(Self::rabbit_mq_url())
    }

    pub fn add_redis(&mut self) -> MessageBroker {
        self.run_message_broker("redis:7", "6379:6379");
        MessageBroker::Redis(Self::redis_url())
    }

    fn run_message_broker(&mut self, image_tag: &str, ports: &str) {
        let message_broker_name = make_container_name("message_broker");
        let output = Command::new("docker")
            .args([
                "run",
                "--name",
                &message_broker_name,
                "-p",
                ports,
                "-d",
                image_tag,
            ])
            .output()
            .unwrap();

        check_output(&output);

        thread::sleep(Duration::from_secs(5));

        if let Some(message_broker_names) = self.message_broker_names.as_mut() {
            message_broker_names.push(message_broker_name);
        } else {
            self.message_broker_names = Some(vec![message_broker_name]);
        }
    }

    pub fn scheduler_cfg(
        &self,
        message_broker: &MessageBroker,
    ) -> sccache::config::scheduler::Config {
        sccache_scheduler_cfg(&self.tmpdir, message_broker)
    }

    pub fn add_scheduler(&mut self, scheduler_cfg: sccache::config::scheduler::Config) {
        let scheduler_cfg_relpath = "scheduler-cfg.json";
        let scheduler_cfg_path = self.tmpdir.join(scheduler_cfg_relpath);
        let scheduler_cfg_container_path =
            Path::new(CONFIGS_CONTAINER_PATH).join(scheduler_cfg_relpath);
        fs::File::create(scheduler_cfg_path)
            .unwrap()
            .write_all(&serde_json::to_vec(&scheduler_cfg.into_file()).unwrap())
            .unwrap();

        // Create the scheduler
        let scheduler_name = make_container_name("scheduler");
        let output = Command::new("docker")
            .args([
                "run",
                "--name",
                &scheduler_name,
                "-e",
                "SCCACHE_NO_DAEMON=1",
                "-e",
                "SCCACHE_LOG=sccache=info,tower_http=debug,axum::rejection=trace",
                "-e",
                "RUST_BACKTRACE=1",
                "--network",
                "host",
                "-v",
                &format!("{}:/sccache-dist:z", self.sccache_dist.to_str().unwrap()),
                "-v",
                &format!(
                    "{}:{}:z",
                    self.tmpdir.to_str().unwrap(),
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

        self.scheduler_name = Some(scheduler_name);

        let scheduler_url = self.scheduler_url();
        wait_for_http(scheduler_url, Duration::from_millis(100), MAX_STARTUP_WAIT);
        wait_for(
            || {
                let status = self.scheduler_status();
                if matches!(status, dist::SchedulerStatus { .. }) {
                    Ok(())
                } else {
                    Err(format!("{:?}", status))
                }
            },
            Duration::from_millis(100),
            MAX_STARTUP_WAIT,
        );
    }

    pub fn server_cfg(&self, message_broker: &MessageBroker) -> sccache::config::server::Config {
        sccache_server_cfg(&self.tmpdir, message_broker)
    }

    pub fn add_server(&mut self, server_cfg: sccache::config::server::Config) -> ServerHandle {
        let server_cfg_relpath = format!("server-cfg-{}.json", self.server_names.len());
        let server_cfg_path = self.tmpdir.join(&server_cfg_relpath);
        let server_cfg_container_path = Path::new(CONFIGS_CONTAINER_PATH).join(server_cfg_relpath);
        fs::File::create(server_cfg_path)
            .unwrap()
            .write_all(&serde_json::to_vec(&server_cfg.into_file()).unwrap())
            .unwrap();

        let server_name = make_container_name("server");

        let output = Command::new("docker")
            .args([
                "run",
                // Important for the bubblewrap builder
                "--privileged",
                "--name",
                &server_name,
                "-e",
                &format!("SCCACHE_DIST_SERVER_ID={server_name}"),
                "-e",
                "SCCACHE_NO_DAEMON=1",
                "-e",
                "SCCACHE_LOG=sccache=info,tower_http=debug,axum::rejection=trace",
                "-e",
                "RUST_BACKTRACE=1",
                "--network",
                "host",
                "-v",
                &format!("{}:/sccache-dist:z", self.sccache_dist.to_str().unwrap()),
                "-v",
                &format!(
                    "{}:{}:z",
                    self.tmpdir.to_str().unwrap(),
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

        self.server_names.push(server_name.clone());

        let handle = ServerHandle::Container { id: server_name };
        self.wait_server_ready(&handle);
        handle
    }

    pub fn add_custom_server(&mut self, server_id: &str, pid: Pid) -> ServerHandle {
        self.server_pids.push(pid);

        let handle = ServerHandle::Process {
            pid,
            id: server_id.to_owned(),
        };

        self.wait_server_ready(&handle);

        handle
    }

    pub fn restart_server(&mut self, handle: &ServerHandle) {
        match handle {
            ServerHandle::Container { id: cid } => {
                let output = Command::new("docker")
                    .args(["restart", cid])
                    .output()
                    .unwrap();
                check_output(&output);
            }
            ServerHandle::Process { pid: _, id: _ } => {
                // TODO: pretty easy, just no need yet
                panic!("restart not yet implemented for pids")
            }
        }
        self.wait_server_ready(handle)
    }

    pub fn wait_server_ready(&mut self, handle: &ServerHandle) {
        let server_id = match handle {
            ServerHandle::Container { id } => id,
            ServerHandle::Process { id, .. } => id,
        };
        wait_for(
            || {
                let status = self.scheduler_status();
                if status.servers.iter().any(|server| &server.id == server_id) {
                    Ok(())
                } else {
                    Err(format!("{:?}", status))
                }
            },
            Duration::from_millis(100),
            MAX_STARTUP_WAIT,
        );
    }

    pub fn scheduler_url(&self) -> HTTPUrl {
        let url = format!("http://127.0.0.1:{}", SCHEDULER_PORT);
        HTTPUrl::from_url(reqwest::Url::parse(&url).unwrap())
    }

    fn scheduler_status(&self) -> dist::SchedulerStatus {
        let mut req = reqwest::blocking::Client::builder().build().unwrap().get(
            dist::http::urls::scheduler_status(&self.scheduler_url().to_url()),
        );
        req = req.bearer_auth(sccache::config::INSECURE_DIST_CLIENT_TOKEN);
        let res = req.send().unwrap();
        assert!(res.status().is_success());
        bincode::deserialize_from(res).unwrap()
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

        if let Some(scheduler_name) = self.scheduler_name.as_ref() {
            droperr!(Command::new("docker")
                .args(["logs", scheduler_name])
                .output()
                .map(|o| logs.push((scheduler_name, o))));
            droperr!(Command::new("docker")
                .args(["kill", scheduler_name])
                .output()
                .map(|o| outputs.push((scheduler_name, o))));
            droperr!(Command::new("docker")
                .args(["rm", "-f", scheduler_name])
                .output()
                .map(|o| outputs.push((scheduler_name, o))));
        }

        for server_name in self.server_names.iter() {
            droperr!(Command::new("docker")
                .args(["logs", server_name])
                .output()
                .map(|o| logs.push((server_name, o))));
            droperr!(Command::new("docker")
                .args(["kill", server_name])
                .output()
                .map(|o| outputs.push((server_name, o))));
            droperr!(Command::new("docker")
                .args(["rm", "-f", server_name])
                .output()
                .map(|o| outputs.push((server_name, o))));
        }

        for &pid in self.server_pids.iter() {
            droperr!(nix::sys::signal::kill(pid, Signal::SIGINT));
            thread::sleep(MAX_STARTUP_WAIT);
            let mut killagain = true; // Default to trying to kill again, e.g. if there was an error waiting on the pid
            droperr!(
                nix::sys::wait::waitpid(pid, Some(WaitPidFlag::WNOHANG)).map(|ws| {
                    if ws != WaitStatus::StillAlive {
                        killagain = false;
                        exits.push(ws)
                    }
                })
            );
            if killagain {
                eprintln!("SIGINT didn't kill process, trying SIGKILL");
                droperr!(nix::sys::signal::kill(pid, Signal::SIGKILL));
                droperr!(nix::sys::wait::waitpid(pid, Some(WaitPidFlag::WNOHANG))
                    .map_err(|e| e.to_string())
                    .and_then(|ws| if ws == WaitStatus::StillAlive {
                        Err("process alive after sigkill".to_owned())
                    } else {
                        exits.push(ws);
                        Ok(())
                    }));
            }
        }

        // Kill the message broker last
        if let Some(message_broker_names) = self.message_broker_names.as_ref() {
            for message_broker_name in message_broker_names.iter() {
                droperr!(Command::new("docker")
                    .args(["logs", message_broker_name])
                    .output()
                    .map(|o| logs.push((message_broker_name, o))));
                droperr!(Command::new("docker")
                    .args(["kill", message_broker_name])
                    .output()
                    .map(|o| outputs.push((message_broker_name, o))));
                droperr!(Command::new("docker")
                    .args(["rm", "-f", message_broker_name])
                    .output()
                    .map(|o| outputs.push((message_broker_name, o))));
            }
        }

        for (
            container,
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
                container,
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }
        for (
            container,
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
                container,
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

#[cfg(feature = "dist-server")]
pub fn add_custom_server<B: dist::BuilderIncoming + 'static>(
    server_id: &str,
    message_broker: &MessageBroker,
    builder: B,
    storage: Option<CacheType>,
    tmpdir: &Path,
) -> std::result::Result<(), anyhow::Error> {
    let server_id = server_id.to_owned();
    let builder = std::sync::Arc::new(builder);
    let message_broker = message_broker.clone();

    env::set_var("SCCACHE_NO_DAEMON", "1");

    if env::var("SCCACHE_LOG").is_err() {
        env::set_var("SCCACHE_LOG", "sccache=debug");
    }

    dist::init_logging();

    let ncpu = 4;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(ncpu)
        .build()?;

    runtime.block_on(async {
        let job_queue = std::sync::Arc::new(tokio::sync::Semaphore::new(ncpu));
        let state = dist::server::ServerState {
            id: server_id.to_owned(),
            job_queue,
            num_cpus: ncpu,
            occupancy: ncpu,
            pre_fetch: ncpu,
            ..Default::default()
        };

        let tasks = loop {
            if let Ok(tasks) = dist::tasks::Tasks::server(
                &server_id,
                &dist::scheduler_to_servers_queue(),
                ncpu as u16,
                Some(message_broker.clone()),
            )
            .await
            {
                break tasks;
            }
        };

        let toolchains = dist::ServerToolchains::new(
            tmpdir.join("tc"),
            u32::MAX as u64,
            storage_from_config(
                &storage,
                &DiskCacheConfig {
                    dir: tmpdir.join("toolchains"),
                    ..Default::default()
                },
            )?,
            Default::default(),
        );

        let server = dist::server::Server::new(
            builder,
            storage_from_config(
                &storage,
                &DiskCacheConfig {
                    dir: tmpdir.join("jobs"),
                    ..Default::default()
                },
            )?,
            state,
            tasks,
            toolchains,
        )?;

        match server.start(Duration::from_secs(1)).await {
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        }
    })
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
