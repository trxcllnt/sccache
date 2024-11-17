#[macro_use]
extern crate log;

use anyhow::{bail, Context, Error, Result};
use base64::Engine;
use itertools::Itertools;
use rand::{rngs::OsRng, RngCore};
use sccache::config::{
    scheduler as scheduler_config, server as server_config, INSECURE_DIST_CLIENT_TOKEN,
};
use sccache::dist::http::{HEARTBEAT_ERROR_INTERVAL, HEARTBEAT_INTERVAL};
use sccache::dist::{
    self, AllocJobResult, AssignJobResult, BuilderIncoming, CompileCommand, HeartbeatServerResult,
    InputsReader, JobAlloc, JobAuthorizer, JobComplete, JobId, ReserveJobResult, RunJobResult,
    SchedulerIncoming, SchedulerOutgoing, SchedulerStatusResult, ServerId, ServerIncoming,
    ServerNonce, ServerOutgoing, ServerStatusResult, SubmitToolchainResult, TcCache, Toolchain,
    ToolchainReader,
};
use sccache::util::daemonize;
use sccache::util::BASE64_URL_SAFE_ENGINE;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

#[cfg_attr(target_os = "freebsd", path = "build_freebsd.rs")]
mod build;

mod cmdline;
mod token_check;

use cmdline::{AuthSubcommand, Command};

pub const INSECURE_DIST_SERVER_TOKEN: &str = "dangerously_insecure_server";

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
                println!("sccache: increment compilation is  prohibited.");
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
        Ok(s) => s,
        Err(e) => {
            eprintln!("sccache-dist: error: {}", e);

            for e in e.chain().skip(1) {
                eprintln!("sccache-dist: caused by: {}", e);
            }
            2
        }
    });
}

fn create_server_token(server_id: ServerId, auth_token: &str) -> String {
    format!("{} {}", server_id.addr(), auth_token)
}
fn check_server_token(server_token: &str, auth_token: &str) -> Option<ServerId> {
    let mut split = server_token.splitn(2, |c| c == ' ');
    let server_addr = split.next().and_then(|addr| addr.parse().ok())?;
    match split.next() {
        Some(t) if t == auth_token => Some(ServerId::new(server_addr)),
        Some(_) | None => None,
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerJwt {
    exp: u64,
    server_id: ServerId,
}
fn create_jwt_server_token(
    server_id: ServerId,
    header: &jwt::Header,
    key: &[u8],
) -> Result<String> {
    let key = jwt::EncodingKey::from_secret(key);
    jwt::encode(header, &ServerJwt { exp: 0, server_id }, &key).map_err(Into::into)
}
fn dangerous_insecure_extract_jwt_server_token(server_token: &str) -> Result<ServerId> {
    let validation = {
        let mut validation = jwt::Validation::default();
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.insecure_disable_signature_validation();
        validation
    };
    let dummy_key = jwt::DecodingKey::from_secret(b"secret");
    jwt::decode::<ServerJwt>(server_token, &dummy_key, &validation)
        .map(|res| res.claims.server_id)
        .map_err(Into::into)
}
fn check_jwt_server_token(
    server_token: &str,
    key: &[u8],
    validation: &jwt::Validation,
) -> Option<ServerId> {
    let key = jwt::DecodingKey::from_secret(key);
    jwt::decode::<ServerJwt>(server_token, &key, validation)
        .map(|res| res.claims.server_id)
        .ok()
}

fn run(command: Command) -> Result<i32> {
    match command {
        Command::Auth(AuthSubcommand::Base64 { num_bytes }) => {
            let mut bytes = vec![0; num_bytes];
            OsRng.fill_bytes(&mut bytes);
            // As long as it can be copied, it doesn't matter if this is base64 or hex etc
            println!("{}", BASE64_URL_SAFE_ENGINE.encode(bytes));
            Ok(0)
        }
        Command::Auth(AuthSubcommand::JwtHS256ServerToken {
            secret_key,
            server_id,
        }) => {
            let header = jwt::Header::new(jwt::Algorithm::HS256);
            let secret_key = BASE64_URL_SAFE_ENGINE.decode(secret_key)?;
            let token = create_jwt_server_token(server_id, &header, &secret_key)
                .context("Failed to create server token")?;
            println!("{}", token);
            Ok(0)
        }

        Command::Scheduler(scheduler_config::Config {
            public_addr,
            client_auth,
            server_auth,
            remember_server_error_timeout,
        }) => {
            let check_client_auth: Box<dyn dist::http::ClientAuthCheck> = match client_auth {
                scheduler_config::ClientAuth::Insecure => Box::new(token_check::EqCheck::new(
                    INSECURE_DIST_CLIENT_TOKEN.to_owned(),
                )),
                scheduler_config::ClientAuth::Token { token } => {
                    Box::new(token_check::EqCheck::new(token))
                }
                scheduler_config::ClientAuth::JwtValidate {
                    audience,
                    issuer,
                    jwks_url,
                } => Box::new(
                    token_check::ValidJWTCheck::new(audience, issuer, &jwks_url)
                        .context("Failed to create a checker for valid JWTs")?,
                ),
                scheduler_config::ClientAuth::Mozilla { required_groups } => {
                    Box::new(token_check::MozillaCheck::new(required_groups))
                }
                scheduler_config::ClientAuth::ProxyToken { url, cache_secs } => {
                    Box::new(token_check::ProxyTokenCheck::new(url, cache_secs))
                }
            };

            let check_server_auth: dist::http::ServerAuthCheck = match server_auth {
                scheduler_config::ServerAuth::Insecure => {
                    warn!("Scheduler starting with DANGEROUSLY_INSECURE server authentication");
                    let token = INSECURE_DIST_SERVER_TOKEN;
                    Box::new(move |server_token| check_server_token(server_token, token))
                }
                scheduler_config::ServerAuth::Token { token } => {
                    Box::new(move |server_token| check_server_token(server_token, &token))
                }
                scheduler_config::ServerAuth::JwtHS256 { secret_key } => {
                    let secret_key = BASE64_URL_SAFE_ENGINE
                        .decode(secret_key)
                        .context("Secret key base64 invalid")?;
                    if secret_key.len() != 256 / 8 {
                        bail!("Size of secret key incorrect")
                    }
                    let validation = {
                        let mut validation = jwt::Validation::new(jwt::Algorithm::HS256);
                        validation.leeway = 0;
                        validation.validate_exp = false;
                        validation.validate_nbf = false;
                        validation
                    };
                    Box::new(move |server_token| {
                        check_jwt_server_token(server_token, &secret_key, &validation)
                    })
                }
            };

            daemonize()?;
            let scheduler = Scheduler::new(remember_server_error_timeout);
            let http_scheduler = dist::http::Scheduler::new(
                public_addr,
                scheduler,
                check_client_auth,
                check_server_auth,
            );
            http_scheduler.start()?;
            unreachable!();
        }

        Command::Server(server_config::Config {
            builder,
            cache_dir,
            public_addr,
            bind_addr,
            scheduler_url,
            scheduler_auth,
            toolchain_cache_size,
            max_per_core_load,
            num_cpus_to_ignore,
        }) => {
            let bind_addr = bind_addr.unwrap_or(public_addr);
            let builder: Box<dyn dist::BuilderIncoming> = match builder {
                #[cfg(not(target_os = "freebsd"))]
                server_config::BuilderType::Docker => {
                    Box::new(build::DockerBuilder::new().context("Docker builder failed to start")?)
                }
                #[cfg(not(target_os = "freebsd"))]
                server_config::BuilderType::Overlay {
                    bwrap_path,
                    build_dir,
                } => Box::new(
                    build::OverlayBuilder::new(bwrap_path, build_dir)
                        .context("Overlay builder failed to start")?,
                ),
                #[cfg(target_os = "freebsd")]
                server_config::BuilderType::Pot {
                    pot_fs_root,
                    clone_from,
                    pot_cmd,
                    pot_clone_args,
                } => Box::new(
                    build::PotBuilder::new(pot_fs_root, clone_from, pot_cmd, pot_clone_args)
                        .context("Pot builder failed to start")?,
                ),
                _ => bail!(
                    "Builder type `{}` not supported on this platform",
                    format!("{:?}", builder)
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                ),
            };

            let server_id = ServerId::new(public_addr);
            let scheduler_auth = match scheduler_auth {
                server_config::SchedulerAuth::Insecure => {
                    warn!("Server starting with DANGEROUSLY_INSECURE scheduler authentication");
                    create_server_token(server_id, INSECURE_DIST_SERVER_TOKEN)
                }
                server_config::SchedulerAuth::Token { token } => {
                    create_server_token(server_id, &token)
                }
                server_config::SchedulerAuth::JwtToken { token } => {
                    let token_server_id: ServerId =
                        dangerous_insecure_extract_jwt_server_token(&token)
                            .context("Could not decode scheduler auth jwt")?;
                    if token_server_id != server_id {
                        bail!(
                            "JWT server id ({:?}) did not match configured server id ({:?})",
                            token_server_id,
                            server_id
                        )
                    }
                    token
                }
            };

            let server = Server::new(builder, &cache_dir, toolchain_cache_size)
                .context("Failed to create sccache server instance")?;
            let http_server = dist::http::Server::new(
                public_addr,
                bind_addr,
                scheduler_url.to_url(),
                scheduler_auth,
                max_per_core_load,
                num_cpus_to_ignore,
                server,
            )
            .context("Failed to create sccache HTTP server instance")?;
            http_server.start()?;
            unreachable!();
        }
    }
}

fn init_logging() {
    if env::var(sccache::LOGGING_ENV).is_ok() {
        match env_logger::Builder::from_env(sccache::LOGGING_ENV).try_init() {
            Ok(_) => (),
            Err(e) => panic!("Failed to initialize logging: {:?}", e),
        }
    }
}

// To avoid deadlocking, make sure to do all locking at once (i.e. no further locking in a downward scope),
// in alphabetical order
pub struct Scheduler {
    servers: Mutex<HashMap<ServerId, ServerDetails>>,
    remember_server_error_timeout: Duration,
}

struct ServerDetails {
    last_seen: Instant,
    last_error: Option<Instant>,
    num_cpus: usize,
    max_per_core_load: f64,
    server_nonce: ServerNonce,
    job_authorizer: Box<dyn JobAuthorizer>,
    num_assigned_jobs: usize,
    num_active_jobs: usize,
    num_pending_jobs: usize,
}

impl Scheduler {
    pub fn new(remember_server_error_timeout: u64) -> Self {
        Scheduler {
            servers: Mutex::new(HashMap::new()),
            remember_server_error_timeout: Duration::from_secs(remember_server_error_timeout),
        }
    }

    fn prune_servers(&self, servers: &mut MutexGuard<HashMap<ServerId, ServerDetails>>) {
        let now = Instant::now();

        let mut dead_servers = Vec::new();

        for (&server_id, server) in servers.iter_mut() {
            if now.duration_since(server.last_seen) > dist::http::HEARTBEAT_TIMEOUT {
                dead_servers.push(server_id);
            }
        }

        for server_id in dead_servers {
            warn!(
                "[prune_servers({})]: Server appears to be dead, pruning it in the scheduler",
                server_id.addr()
            );
            servers.remove(&server_id);
        }
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new(scheduler_config::default_remember_server_error_timeout())
    }
}

fn error_chain_to_string(err: &Error) -> String {
    let mut err_msg = err.to_string();
    let mut maybe_cause = err.source();
    while let Some(cause) = maybe_cause {
        err_msg.push_str(", caused by: ");
        err_msg.push_str(&cause.to_string());
        maybe_cause = cause.source();
    }
    err_msg
}

impl SchedulerIncoming for Scheduler {
    fn handle_alloc_job(
        &self,
        requester: &dyn SchedulerOutgoing,
        tc: Toolchain,
    ) -> Result<AllocJobResult> {
        let Scheduler {
            remember_server_error_timeout,
            ..
        } = *self;

        // Attempt to allocate a job to the best server. The best server is the server
        // with the fewest assigned jobs and least-recently-reported error. Servers
        // whose load exceeds `num_cpus` are not considered candidates for assignment.
        //
        // If we fail to assign a job to a server, attempt to assign the job to the next
        // best candidate until either the job has been assigned successfully, or the
        // candidate list has been exhausted.
        //
        // Special care is taken to not lock `self.servers` while network requests are
        // in-flight, as that will block other request-handling threads and deadlock
        // the scheduler.
        //
        // Do not assert!() anywhere, as that permanently corrupts the scheduler.
        // All error conditions must fail gracefully.

        let try_reserve_job = |server_id: ServerId| {
            // LOCKS
            let mut servers = self.servers.lock().unwrap();
            let server = match servers.get_mut(&server_id) {
                Some(server) => server,
                _ => bail!("Failed to reserve job on unknown server"),
            };

            let reserve_auth = server
                .job_authorizer
                .generate_token(JobId(server.server_nonce.as_u64()))
                .map_err(Error::from)
                .context("Could not create auth token")?;

            server.num_pending_jobs += 1;

            // Drop lock
            drop(servers);

            let ReserveJobResult {
                id,
                num_active_jobs,
                num_assigned_jobs,
            } = match requester.do_reserve_job(server_id, reserve_auth) {
                Ok(res) => res,
                Err(err) => {
                    // LOCKS
                    let mut servers = self.servers.lock().unwrap();
                    // Couldn't reserve the job, track the error time and bail
                    if let Some(server) = servers.get_mut(&server_id) {
                        server.last_error = Some(Instant::now());
                        server.num_pending_jobs =
                            (server.num_pending_jobs as i64 - 1).max(0) as usize;
                    }
                    return Err(err);
                }
            };

            // LOCKS
            let mut servers = self.servers.lock().unwrap();
            let server = match servers.get_mut(&server_id) {
                Some(server) => server,
                _ => bail!("Failed to reserve job on unknown server"),
            };

            // Assigned the job, so update server stats
            server.last_seen = Instant::now();
            server.num_assigned_jobs = num_assigned_jobs;
            server.num_active_jobs = num_active_jobs;

            server
                .job_authorizer
                .generate_token(id)
                .map_err(Error::from)
                .context("Could not create auth token")
                .map(|auth| (id, auth))
        };

        let try_assign_job = |job_id: JobId, server_id: ServerId, auth: String, tc: Toolchain| {
            let need_toolchain = match requester.do_assign_job(server_id, job_id, tc, auth.clone())
            {
                Ok(AssignJobResult {
                    need_toolchain,
                    num_assigned_jobs,
                    num_active_jobs,
                }) => {
                    // LOCKS
                    let mut servers = self.servers.lock().unwrap();
                    // Assigned the job, so update server stats
                    if let Some(server) = servers.get_mut(&server_id) {
                        server.last_seen = Instant::now();
                        server.num_assigned_jobs = num_assigned_jobs;
                        server.num_active_jobs = num_active_jobs;
                        server.num_pending_jobs =
                            (server.num_pending_jobs as i64 - 1).max(0) as usize;
                    }
                    need_toolchain
                }
                Err(err) => {
                    // LOCKS
                    let mut servers = self.servers.lock().unwrap();
                    // Couldn't assign the job, so undo the eager assignment above
                    if let Some(server) = servers.get_mut(&server_id) {
                        server.last_error = Some(Instant::now());
                        server.num_pending_jobs =
                            (server.num_pending_jobs as i64 - 1).max(0) as usize;
                    }
                    return Err(err);
                }
            };

            if log_enabled!(log::Level::Trace) {
                // LOCKS
                let mut servers = self.servers.lock().unwrap();
                if let Some(server) = servers.get_mut(&server_id) {
                    if let Some(last_error) = server.last_error {
                        trace!(
                            "[alloc_job({}, {})]: Assigned job to server whose most recent error was {:?} ago",
                            server_id.addr(),
                            job_id,
                            Instant::now() - last_error
                        );
                    }
                }
            }

            debug!(
                "[alloc_job({}, {})]: Job created and assigned to server",
                server_id.addr(),
                job_id,
            );

            Ok(AllocJobResult::Success {
                job_alloc: JobAlloc {
                    auth,
                    job_id,
                    server_id,
                },
                need_toolchain,
            })
        };

        let get_best_server_by_least_load_and_oldest_error = |tried_servers: &HashSet<ServerId>| {
            let now = Instant::now();
            // LOCKS
            let mut servers = self.servers.lock().unwrap();

            // Compute instantaneous load and update shared server state
            servers
                .iter_mut()
                .filter_map(|(server_id, server)| {
                    // Forget errors that are too old to care about anymore
                    if let Some(last_error) = server.last_error {
                        if now.duration_since(last_error) >= remember_server_error_timeout {
                            server.last_error = None;
                        }
                    }

                    // Each server defines its own `max_per_core_load` multiple
                    let num_vcpus = (server.num_cpus as f64 * server.max_per_core_load)
                        .floor()
                        .max(1.0);

                    // Assume all pending and assigned jobs will eventually be handled
                    let num_assigned_jobs =
                        // number of jobs this scheduler has reserved on the server but have not yet been assigned
                        server.num_pending_jobs
                        // number of jobs assigned to this server and are waiting on the client to start
                        + server.num_assigned_jobs
                        // number of jobs assigned to this server and are running
                        + server.num_active_jobs;

                    let load = num_assigned_jobs as f64 / num_vcpus;

                    // Exclude servers at max load and servers we've already tried
                    if load >= 1.0 || tried_servers.contains(server_id) {
                        None
                    } else {
                        Some((server_id, server, load))
                    }
                })
                // Sort servers by least load and oldest error
                .sorted_by(|(_, server_a, load_a), (_, server_b, load_b)| {
                    match (server_a.last_error, server_b.last_error) {
                        // If neither server has a recent error, prefer the one with lowest load
                        (None, None) => load_a.total_cmp(load_b),
                        // Prefer servers with no recent errors over servers with recent errors
                        (None, Some(_)) => std::cmp::Ordering::Less,
                        (Some(_), None) => std::cmp::Ordering::Greater,
                        // If both servers have an error, prefer the one with the oldest error
                        (Some(err_a), Some(err_b)) => err_b.cmp(&err_a),
                    }
                })
                .find_or_first(|_| true)
                .map(|(server_id, _, _)| *server_id)
        };

        let mut tried_servers = HashSet::<ServerId>::new();
        let mut result = None;

        // Loop through candidate servers.
        // Exit the loop once we've allocated the job.
        // Try the next candidate if we encounter an error.
        loop {
            // Get the latest best server candidate after sorting all servers by least load
            // and oldest error, sans the servers we've already tried.
            //
            // This computes each server's load again local to this loop.
            //
            // Since alloc_job in other threads can recover from errors and assign jobs to the
            // next-best candidate, the load could drift if we only compute it once outside this
            // loop. Computing load again ensures we allocate accurately based on the current
            // statistics.
            let server_id = get_best_server_by_least_load_and_oldest_error(&tried_servers);

            // Take the top candidate. If we can't allocate the job to it,
            // remove it from the candidates list and try the next server.
            if let Some(server_id) = server_id {
                // Generate job auth token for this server
                let (job_id, auth) = match try_reserve_job(server_id) {
                    Ok(res) => res,
                    Err(err) => {
                        // If try_reserve_job failed, try the next best server
                        warn!(
                            "[alloc_job({})]: {}",
                            server_id.addr(),
                            error_chain_to_string(&err)
                        );
                        tried_servers.insert(server_id);
                        result = Some(Err(err));
                        continue;
                    }
                };
                // Attempt to assign the job to this server. If assign_job fails,
                // store the error and attempt to assign to the next server.
                // If all servers error, return the last error to the client.
                match try_assign_job(job_id, server_id, auth, tc.clone()) {
                    Ok(res) => {
                        // If assign_job succeeded, return the result
                        result = Some(Ok(res));
                        break;
                    }
                    Err(err) => {
                        // If alloc_job failed, try the next best server
                        warn!(
                            "[alloc_job({}, {})]: {}",
                            server_id.addr(),
                            job_id,
                            error_chain_to_string(&err)
                        );
                        tried_servers.insert(server_id);
                        result = Some(Err(err));
                        continue;
                    }
                }
            }
            break;
        }

        result.unwrap_or_else(|| {
            // Fallback to the default failure case
            Ok(AllocJobResult::Fail {
                msg: format!(
                    "[alloc_job]: Insufficient capacity across {} available servers",
                    tried_servers.len()
                ),
            })
        })
    }

    fn handle_heartbeat_server(
        &self,
        server_id: ServerId,
        server_nonce: ServerNonce,
        num_cpus: usize,
        max_per_core_load: f64,
        job_authorizer: Box<dyn JobAuthorizer>,
        num_assigned_jobs: usize,
        num_active_jobs: usize,
    ) -> Result<HeartbeatServerResult> {
        if num_cpus == 0 {
            bail!("Invalid number of CPUs (0) specified in heartbeat")
        }

        // LOCKS
        let mut servers = self.servers.lock().unwrap();

        match servers.get_mut(&server_id) {
            Some(ref mut server) if server.server_nonce == server_nonce => {
                server.last_seen = Instant::now();
                server.num_cpus = num_cpus;
                server.job_authorizer = job_authorizer;
                server.max_per_core_load = max_per_core_load;
                server.num_assigned_jobs = num_assigned_jobs;
                server.num_active_jobs = num_active_jobs;

                // Prune servers only after updating this server's last_seen time.
                // This ensures the server which sent this heartbeat isn't pruned.
                self.prune_servers(&mut servers);

                return Ok(HeartbeatServerResult { is_new: false });
            }
            _ => (),
        }

        self.prune_servers(&mut servers);

        info!("Registered new server {:?}", server_id);

        servers.insert(
            server_id,
            ServerDetails {
                last_seen: Instant::now(),
                last_error: None,
                num_cpus,
                max_per_core_load,
                server_nonce,
                job_authorizer,
                num_assigned_jobs,
                num_active_jobs,
                num_pending_jobs: 0,
            },
        );

        Ok(HeartbeatServerResult { is_new: true })
    }

    fn handle_status(&self) -> Result<SchedulerStatusResult> {
        let Scheduler {
            remember_server_error_timeout,
            ..
        } = *self;

        // LOCKS
        let mut servers = self.servers.lock().unwrap();

        // Prune servers before reporting the scheduler status
        self.prune_servers(&mut servers);

        let mut active_jobs = 0;
        let mut accepted_jobs = 0;
        let mut pending_jobs = 0;

        let mut servers_map = HashMap::<std::net::SocketAddr, ServerStatusResult>::new();
        for (server_id, server) in servers.iter() {
            active_jobs += server.num_active_jobs;
            accepted_jobs += server.num_assigned_jobs;
            pending_jobs += server.num_pending_jobs;
            servers_map.insert(
                server_id.addr(),
                ServerStatusResult {
                    pending: server.num_pending_jobs,
                    assigned: server.num_assigned_jobs,
                    active: server.num_active_jobs,
                    num_cpus: server.num_cpus,
                    max_per_core_load: server.max_per_core_load,
                    last_seen: server.last_seen.elapsed().as_secs(),
                    last_error: server
                        .last_error
                        .map(|e| (remember_server_error_timeout - e.elapsed()).as_secs()),
                },
            );
        }

        Ok(SchedulerStatusResult {
            num_cpus: servers.values().map(|v| v.num_cpus).sum(),
            num_servers: servers.len(),
            pending: pending_jobs,
            assigned: accepted_jobs,
            active: active_jobs,
            servers: servers_map,
        })
    }
}

struct ServerState {
    toolchains: HashMap<JobId, Toolchain>,
    jobs_queued: HashMap<JobId, Instant>,
    jobs_active: usize,
    job_count: usize,
}

pub struct Server {
    builder: Box<dyn BuilderIncoming>,
    cache: Mutex<TcCache>,
    state: Arc<Mutex<ServerState>>,
}

impl Server {
    pub fn new(
        builder: Box<dyn BuilderIncoming>,
        cache_dir: &Path,
        toolchain_cache_size: u64,
    ) -> Result<Server> {
        let cache = TcCache::new(&cache_dir.join("tc"), toolchain_cache_size)
            .context("Failed to create toolchain cache")?;
        Ok(Server {
            builder,
            cache: Mutex::new(cache),
            state: Arc::new(Mutex::new(ServerState {
                toolchains: HashMap::new(),
                jobs_queued: HashMap::new(),
                jobs_active: 0,
                job_count: 0,
            })),
        })
    }
}

impl ServerIncoming for Server {
    fn start_heartbeat(&self, requester: std::sync::Arc<dyn ServerOutgoing>) {
        let state = self.state.clone();
        // TODO: detect if this panics
        thread::spawn(move || {
            let unstarted_job_timeout = std::time::Duration::from_secs(90);
            loop {
                let mut state = state.lock().unwrap();
                let now = std::time::Instant::now();

                for (&job_id, &ctime) in state.jobs_queued.clone().iter() {
                    if now.duration_since(ctime) >= unstarted_job_timeout {
                        state.toolchains.remove(&job_id);
                        state.jobs_queued.remove(&job_id);
                    }
                }

                let num_assigned_jobs = state.jobs_queued.len();
                let num_active_jobs = state.jobs_active;

                drop(state);

                trace!("Performing heartbeat");

                match requester.do_heartbeat(num_assigned_jobs, num_active_jobs) {
                    Ok(HeartbeatServerResult { is_new }) => {
                        trace!("Heartbeat success is_new={}", is_new);
                        thread::sleep(HEARTBEAT_INTERVAL)
                    }
                    Err(e) => {
                        error!("Failed to send heartbeat to server: {}", e);
                        thread::sleep(HEARTBEAT_ERROR_INTERVAL)
                    }
                }
            }
        });
    }

    fn handle_reserve_job(&self) -> Result<ReserveJobResult> {
        let mut state = self.state.lock().unwrap();
        let num_assigned_jobs = state.jobs_queued.len();
        let num_active_jobs = state.jobs_active;
        let id = JobId(state.job_count as u64);

        state.job_count += 1;
        // Drop lock
        drop(state);

        Ok(ReserveJobResult {
            id,
            num_assigned_jobs,
            num_active_jobs,
        })
    }

    fn handle_assign_job(&self, job_id: JobId, tc: Toolchain) -> Result<AssignJobResult> {
        let need_toolchain = !self.cache.lock().unwrap().contains_toolchain(&tc);

        let mut state = self.state.lock().unwrap();
        if let Some(other_tc) = state.toolchains.insert(job_id, tc.clone()) {
            // Remove the toolchain on error
            state.toolchains.remove(&job_id);
            bail!(
                "[{}]: Duplicate toolchain assigned, not replacing toolchain {:?} with {:?}",
                job_id,
                other_tc,
                tc
            );
        };

        state.jobs_queued.insert(job_id, Instant::now());
        let num_assigned_jobs = state.jobs_queued.len();
        let num_active_jobs = state.jobs_active;

        Ok(AssignJobResult {
            need_toolchain,
            num_assigned_jobs,
            num_active_jobs,
        })
    }

    fn handle_submit_toolchain(
        &self,
        job_id: JobId,
        mut tc_rdr: ToolchainReader,
    ) -> Result<SubmitToolchainResult> {
        // TODO: need to lock the toolchain until the container has started
        // TODO: can start prepping container
        let mut state = self.state.lock().unwrap();
        let tc = match state.toolchains.get(&job_id).cloned() {
            Some(tc) => tc,
            None => {
                // Remove the job on error
                state.jobs_queued.remove(&job_id);
                return Ok(SubmitToolchainResult::JobNotFound);
            }
        };

        // Drop lock
        drop(state);

        let mut cache = self.cache.lock().unwrap();

        let res = if cache.contains_toolchain(&tc) {
            // Drop the lock
            drop(cache);
            // Ignore the toolchain request body
            // TODO: Investigate if this causes early hangup warnings in
            // the load balancer. If so, use the implementation below.
            Ok(())
            // // Consume the entire toolchain request body
            // std::io::copy(&mut tc_rdr, &mut std::io::empty())
            //     .map(|_| ())
            //     .or_else(|err| {
            //         warn!("[handle_submit_toolchain({})]: {:?}", job_id, err);
            //         // Ignore errors reading the request body
            //         Ok(())
            //     })
        } else {
            cache.insert_with(&tc, |mut file| io::copy(&mut tc_rdr, &mut file).map(|_| ()))
        };

        Ok(res
            .map(|_| SubmitToolchainResult::Success)
            .unwrap_or_else(|err| {
                warn!("[handle_submit_toolchain({})]: {:?}", job_id, err);
                // Remove the job on error
                let mut state = self.state.lock().unwrap();
                state.jobs_queued.remove(&job_id);
                state.toolchains.remove(&job_id);
                SubmitToolchainResult::CannotCache
            }))
    }

    fn handle_run_job(
        &self,
        job_id: JobId,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs_rdr: InputsReader,
    ) -> Result<RunJobResult> {
        let mut state = self.state.lock().unwrap();
        let tc = match state.toolchains.remove(&job_id) {
            Some(tc) => tc,
            None => {
                // Remove the job on error
                state.jobs_queued.remove(&job_id);
                return Ok(RunJobResult::JobNotFound);
            }
        };

        // Move job from queued to active
        state.jobs_queued.remove(&job_id);
        state.jobs_active += 1;

        // Drop lock
        drop(state);

        // Do the build
        let res = self
            .builder
            .run_build(job_id, tc, command, outputs, inputs_rdr, &self.cache);

        // Move job from active to done
        let mut state = self.state.lock().unwrap();
        state.jobs_active -= 1;
        // Drop lock
        drop(state);

        match res {
            Err(e) => Err(e.context("run_job build failed")),
            Ok(res) => Ok(RunJobResult::Complete(JobComplete {
                output: res.output,
                outputs: res.outputs,
            })),
        }
    }
}
