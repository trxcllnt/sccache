#[macro_use]
extern crate log;

use anyhow::{bail, Context, Error, Result};
use base64::Engine;
use itertools::Itertools;
use rand::{rngs::OsRng, RngCore};
use sccache::config::{
    scheduler as scheduler_config, server as server_config, INSECURE_DIST_CLIENT_TOKEN,
};
use sccache::dist::http::get_dist_request_timeout;
use sccache::dist::{
    self, AllocJobResult, AssignJobResult, BuilderIncoming, CompileCommand, HeartbeatServerResult,
    InputsReader, JobAlloc, JobAuthorizer, JobComplete, JobId, JobState, RunJobResult,
    SchedulerIncoming, SchedulerOutgoing, SchedulerStatusResult, ServerId, ServerIncoming,
    ServerNonce, ServerOutgoing, ServerStatusResult, SubmitToolchainResult, TcCache, Toolchain,
    ToolchainReader, UpdateJobStateResult,
};
use sccache::util::daemonize;
use sccache::util::BASE64_URL_SAFE_ENGINE;
use serde::{Deserialize, Serialize};
use std::collections::{btree_map, BTreeMap, HashMap, HashSet};
use std::env;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
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
            max_per_core_load,
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
            let scheduler = Scheduler::new(max_per_core_load, remember_server_error_timeout);
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
                server.job_toolchains.clone(),
                server.jobs_queued.clone(),
                server.jobs_active.clone(),
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

#[derive(Copy, Clone)]
struct JobDetail {
    mtime: Instant,
    server_id: ServerId,
    state: JobState,
}

// To avoid deadlicking, make sure to do all locking at once (i.e. no further locking in a downward scope),
// in alphabetical order
pub struct Scheduler {
    job_count: AtomicUsize,

    // Currently running jobs, can never be Complete
    jobs: Mutex<BTreeMap<JobId, JobDetail>>,

    servers: Mutex<HashMap<ServerId, ServerDetails>>,

    max_per_core_load: f64,
    remember_server_error_timeout: Duration,
}

struct ServerDetails {
    jobs_assigned: HashSet<JobId>,
    last_seen: Instant,
    last_error: Option<Instant>,
    num_cpus: usize,
    server_nonce: ServerNonce,
    job_authorizer: Box<dyn JobAuthorizer>,
    num_queued_jobs: usize,
    num_active_jobs: usize,
    num_pending_jobs: usize,
}

impl Scheduler {
    pub fn new(max_per_core_load: f64, remember_server_error_timeout: u64) -> Self {
        Scheduler {
            job_count: AtomicUsize::new(0),
            jobs: Mutex::new(BTreeMap::new()),
            servers: Mutex::new(HashMap::new()),
            max_per_core_load,
            remember_server_error_timeout: Duration::from_secs(remember_server_error_timeout),
        }
    }

    fn prune_servers(
        &self,
        servers: &mut MutexGuard<HashMap<ServerId, ServerDetails>>,
        jobs: &mut MutexGuard<BTreeMap<JobId, JobDetail>>,
    ) {
        let now = Instant::now();

        let mut dead_servers = Vec::new();

        for (&server_id, details) in servers.iter_mut() {
            if now.duration_since(details.last_seen) > dist::http::HEARTBEAT_TIMEOUT {
                dead_servers.push(server_id);
            }
        }

        for server_id in dead_servers {
            warn!(
                "Server {} appears to be dead, pruning it in the scheduler",
                server_id.addr()
            );
            let server_details = servers
                .remove(&server_id)
                .expect("server went missing from map");
            for job_id in server_details.jobs_assigned {
                warn!(
                    "Non-terminated job {} was cleaned up in server pruning",
                    job_id
                );
                // A job may be missing here if it failed to allocate
                // initially, so just warn if it's not present.
                if jobs.remove(&job_id).is_none() {
                    warn!(
                        "Non-terminated job {} assignment originally failed.",
                        job_id
                    );
                }
            }
        }
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new(
            scheduler_config::default_max_per_core_load(),
            scheduler_config::default_remember_server_error_timeout(),
        )
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
            max_per_core_load,
            remember_server_error_timeout,
            ..
        } = *self;

        // Attempt to allocate a job to the best server. The best server is the server
        // with the fewest assigned jobs and least-recently-reported error. Servers
        // whose load exceeds `max_per_core_load` are not considered candidates for
        // job assignment.
        //
        // If we fail to assign a job to a server, attempt to assign the job to the next
        // best candidate until either the job has been assigned successfully, or the
        // candidate list has been exhausted.
        //
        // Special care is taken to not lock `self.servers` or `self.jobs` while network
        // requests are in-flight, as that will block other request-handling threads and
        // deadlock the scheduler.
        //
        // Do not assert!() anywhere, as that permanently corrupts the scheduler.
        // All error conditions must fail gracefully.

        let make_auth_token = |job_id: JobId, server_id: ServerId| {
            // LOCKS
            let mut servers = self.servers.lock().unwrap();
            if let Some(details) = servers.get_mut(&server_id) {
                let auth = details
                    .job_authorizer
                    .generate_token(job_id)
                    .map_err(Error::from)
                    .context("Could not create an auth token")?;

                //
                // Eagerly associate this job with the server so other threads consider this job
                // when computing load for this server, and potentially select another server for
                // assignment.
                //

                // Throw an error if a job with the same ID has already been assigned to this server.
                if details.jobs_assigned.contains(&job_id) {
                    bail!("Failed to assign job to server {}", server_id.addr());
                }

                details.num_pending_jobs += 1;
                details.jobs_assigned.insert(job_id);

                Ok(auth)
            } else {
                bail!("Failed to assign job to unknown server")
            }
        };

        let try_alloc_job = |job_id: JobId, server_id: ServerId, auth: String, tc: Toolchain| {
            let AssignJobResult {
                state,
                need_toolchain,
                ..
            } = match requester.do_assign_job(server_id, job_id, tc, auth.clone()) {
                Ok(res) => {
                    // LOCKS
                    let mut servers = self.servers.lock().unwrap();
                    // Assigned the job, so update server stats
                    if let Some(details) = servers.get_mut(&server_id) {
                        details.num_pending_jobs -= 1;
                        details.last_seen = Instant::now();
                        details.num_queued_jobs = res.num_queued_jobs;
                        details.num_active_jobs = res.num_active_jobs;
                    }
                    res
                }
                Err(err) => {
                    // LOCKS
                    let mut servers = self.servers.lock().unwrap();
                    // Couldn't assign the job, so undo the eager assignment above
                    if let Some(details) = servers.get_mut(&server_id) {
                        details.num_pending_jobs -= 1;
                        details.jobs_assigned.remove(&job_id);
                        details.last_error = Some(Instant::now());
                    }
                    return Err(err);
                }
            };

            // LOCKS
            let mut jobs = self.jobs.lock().unwrap();
            if jobs.contains_key(&job_id) {
                bail!(
                    "Failed to assign job to server {} with state {}",
                    server_id.addr(),
                    state
                );
            }

            jobs.insert(
                job_id,
                JobDetail {
                    server_id,
                    state,
                    mtime: Instant::now(),
                },
            );

            drop(jobs);

            if log_enabled!(log::Level::Trace) {
                // LOCKS
                let mut servers = self.servers.lock().unwrap();
                if let Some(details) = servers.get_mut(&server_id) {
                    if let Some(last_error) = details.last_error {
                        trace!(
                            "[alloc_job({})]: Assigned job to server {:?} whose most recent error was {:?} ago",
                            job_id,
                            server_id.addr(),
                            Instant::now() - last_error
                        );
                    }
                }
            }

            debug!(
                "[alloc_job({})]: Job created and assigned to server {:?} with state {:?}",
                job_id,
                server_id.addr(),
                state
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
                .filter_map(|(server_id, details)| {
                    // Assume all jobs assigned to this server will eventually be handled.
                    let num_assigned_jobs =
                        // number of jobs this scheduler has assigned to the server but have not yet been accepted
                        details.num_pending_jobs
                        // number of jobs this server has accepted and is waiting on the client to start
                        + details.num_queued_jobs
                        // number of jobs this server has accepted and are running
                        + details.num_active_jobs;
                    let load = num_assigned_jobs as f64 / details.num_cpus as f64;
                    // Forget errors that are too old to care about anymore
                    if let Some(last_error) = details.last_error {
                        if now.duration_since(last_error) >= remember_server_error_timeout {
                            details.last_error = None;
                        }
                    }
                    // Exclude servers at max load and servers we've already tried
                    if load >= max_per_core_load || tried_servers.contains(server_id) {
                        None
                    } else {
                        Some((server_id, details, load))
                    }
                })
                // Sort servers by least load and oldest error
                .sorted_by(|(_, details_a, load_a), (_, details_b, load_b)| {
                    match (details_a.last_error, details_b.last_error) {
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

        let job_id = self.job_count.fetch_add(1, Ordering::SeqCst) as u64;
        let job_id = JobId(job_id);

        let num_servers = { self.servers.lock().unwrap().len() };
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
                let auth = match make_auth_token(job_id, server_id) {
                    Ok(auth) => auth,
                    Err(err) => {
                        // If make_auth_token failed, try the next best server
                        warn!("[alloc_job({})]: {}", job_id, error_chain_to_string(&err));
                        tried_servers.insert(server_id);
                        result = Some(Err(err));
                        continue;
                    }
                };
                // Attempt to allocate the job to this server. If alloc_job fails,
                // store the error and attempt to allocate to the next server.
                // If all servers error, return the last error to the client.
                match try_alloc_job(job_id, server_id, auth, tc.clone()) {
                    Ok(res) => {
                        // If alloc_job succeeded, return the result
                        result = Some(Ok(res));
                        break;
                    }
                    Err(err) => {
                        // If alloc_job failed, try the next best server
                        warn!("[alloc_job({})]: {}", job_id, error_chain_to_string(&err));
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
                    "[alloc_job({})]: Insufficient capacity across {} available servers",
                    job_id, num_servers
                ),
            })
        })
    }

    fn handle_heartbeat_server(
        &self,
        server_id: ServerId,
        server_nonce: ServerNonce,
        num_cpus: usize,
        job_authorizer: Box<dyn JobAuthorizer>,
        num_queued_jobs: usize,
        num_active_jobs: usize,
    ) -> Result<HeartbeatServerResult> {
        if num_cpus == 0 {
            bail!("Invalid number of CPUs (0) specified in heartbeat")
        }

        // LOCKS
        let mut jobs = self.jobs.lock().unwrap();
        let mut servers = self.servers.lock().unwrap();

        self.prune_servers(&mut servers, &mut jobs);

        match servers.get_mut(&server_id) {
            Some(ref mut details) if details.server_nonce == server_nonce => {
                let now = Instant::now();
                details.last_seen = now;
                details.num_queued_jobs = num_queued_jobs;
                details.num_active_jobs = num_active_jobs;

                let mut stale_jobs = Vec::new();

                // If the server has jobs assigned that have taken longer than `SCCACHE_DIST_REQUEST_TIMEOUT` seconds,
                // either the client retried the compilation (due to a server error), or the client abandoned the job
                // after calling `run_job/<job_id>` (for example, when the user kills the build).
                //
                // In both cases the scheduler moves the job from pending/ready to started, and is expecting to hear
                // from the build server that the job has been completed. That message will never arrive, so we track
                // the time when jobs are started, and prune them if they take longer than the configured request
                // timeout.
                //
                // Note: this assumes the scheduler's `SCCACHE_DIST_REQUEST_TIMEOUT` is >= the client's value, but
                // that should be easy for the user to configure.
                for &job_id in details.jobs_assigned.iter() {
                    if let Some(detail) = jobs.get(&job_id) {
                        if now.duration_since(detail.mtime) >= get_dist_request_timeout() {
                            stale_jobs.push(job_id);
                        }
                    }
                }

                if !stale_jobs.is_empty() {
                    warn!(
                        "The following stale jobs will be de-allocated: {:?}",
                        stale_jobs
                    );

                    for job_id in stale_jobs {
                        if !details.jobs_assigned.remove(&job_id) {
                            warn!(
                                "Stale job for server {} not assigned: {}",
                                server_id.addr(),
                                job_id
                            );
                        }
                        if jobs.remove(&job_id).is_none() {
                            warn!(
                                "Unknown stale job for server {}: {}",
                                server_id.addr(),
                                job_id
                            );
                        }
                    }
                }

                return Ok(HeartbeatServerResult { is_new: false });
            }
            Some(ref mut details) if details.server_nonce != server_nonce => {
                for job_id in details.jobs_assigned.iter() {
                    if jobs.remove(job_id).is_none() {
                        warn!(
                            "Unknown job found when replacing server {}: {}",
                            server_id.addr(),
                            job_id
                        );
                    }
                }
            }
            _ => (),
        }
        info!("Registered new server {:?}", server_id);
        servers.insert(
            server_id,
            ServerDetails {
                last_seen: Instant::now(),
                last_error: None,
                jobs_assigned: HashSet::new(),
                num_cpus,
                server_nonce,
                job_authorizer,
                num_queued_jobs: 0,
                num_active_jobs: 0,
                num_pending_jobs: 0,
            },
        );
        Ok(HeartbeatServerResult { is_new: true })
    }

    fn handle_update_job_state(
        &self,
        job_id: JobId,
        server_id: ServerId,
        job_state: JobState,
        num_queued_jobs: usize,
        num_active_jobs: usize,
    ) -> Result<UpdateJobStateResult> {
        // LOCKS
        let mut jobs = self.jobs.lock().unwrap();
        let mut servers = self.servers.lock().unwrap();

        if let btree_map::Entry::Occupied(mut job) = jobs.entry(job_id) {
            let cur_state = job.get().state;

            if job.get().server_id != server_id {
                bail!(
                    "[update_job_state({}, {})]: Job state updated from {:?} to {:?}, but job is not registered to server",
                    job_id,
                    server_id.addr(),
                    cur_state, job_state
                )
            }

            let now = Instant::now();

            let server = match servers.get_mut(&server_id) {
                Some(server) => {
                    server.last_seen = now;
                    server.num_queued_jobs = num_queued_jobs;
                    server.num_active_jobs = num_active_jobs;
                    server
                }
                None => {
                    let (job_id, _) = job.remove_entry();
                    bail!(
                        "[update_job_state({}, {})]: Job state updated from {:?} to {:?}, but server is not known to scheduler",
                        job_id, server_id.addr(), cur_state, job_state
                    )
                }
            };

            match (cur_state, job_state) {
                (JobState::Pending, JobState::Ready) => {
                    job.get_mut().mtime = now;
                    job.get_mut().state = job_state;
                }
                (JobState::Ready, JobState::Started) => {
                    job.get_mut().mtime = now;
                    job.get_mut().state = job_state;
                }
                (JobState::Started, JobState::Complete) => {
                    let (job_id, _) = job.remove_entry();
                    if !server.jobs_assigned.remove(&job_id) {
                        bail!(
                            "[update_job_state({}, {})]: Job was marked as finished, but job is not known to scheduler",
                            job_id, server_id.addr()
                        )
                    }
                }
                (from, to) => {
                    let (job_id, _) = job.remove_entry();
                    server.jobs_assigned.remove(&job_id);
                    bail!(
                        "[update_job_state({}, {})]: Invalid job state transition from {:?} to {:?}",
                        job_id,
                        server_id.addr(),
                        from,
                        to,
                    )
                }
            }
            debug!(
                "[update_job_state({}, {})]: Job state updated from {:?} to {:?}",
                job_id,
                server_id.addr(),
                cur_state,
                job_state
            );
        } else {
            bail!(
                "[update_job_state({}, {})]: Cannot update unknown job state to {:?}",
                job_id,
                server_id.addr(),
                job_state
            )
        }
        Ok(UpdateJobStateResult::Success)
    }

    fn handle_status(&self) -> Result<SchedulerStatusResult> {
        // LOCKS
        let mut jobs = self.jobs.lock().unwrap();
        let mut servers = self.servers.lock().unwrap();

        self.prune_servers(&mut servers, &mut jobs);

        drop(jobs);

        let mut active_jobs = 0;
        let mut queued_jobs = 0;
        let mut maybe_servers = None;

        if servers.len() > 0 {
            let mut servers_result = HashMap::<std::net::SocketAddr, ServerStatusResult>::new();

            for (server_id, details) in servers.iter() {
                active_jobs += details.num_active_jobs;
                queued_jobs += details.num_queued_jobs + details.num_pending_jobs;
                servers_result.insert(
                    server_id.addr(),
                    ServerStatusResult {
                        active: details.num_active_jobs,
                        queued: details.num_queued_jobs + details.num_pending_jobs,
                        last_seen: details.last_seen.elapsed().as_secs(),
                        last_error: details.last_error.map(|e| e.elapsed().as_secs()),
                    },
                );
            }

            maybe_servers = Some(servers_result);
        }

        Ok(SchedulerStatusResult {
            num_cpus: servers.values().map(|v| v.num_cpus).sum(),
            num_servers: servers.len(),
            active: active_jobs,
            queued: queued_jobs,
            servers: maybe_servers,
        })
    }
}

pub struct Server {
    builder: Box<dyn BuilderIncoming>,
    cache: Mutex<TcCache>,
    pub job_toolchains: Arc<Mutex<HashMap<JobId, Toolchain>>>,
    pub jobs_queued: Arc<Mutex<HashMap<JobId, Instant>>>,
    pub jobs_active: Arc<AtomicUsize>,
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
            job_toolchains: Arc::new(Mutex::new(HashMap::new())),
            jobs_queued: Arc::new(Mutex::new(HashMap::new())),
            jobs_active: Arc::new(AtomicUsize::new(0)),
        })
    }
}

impl ServerIncoming for Server {
    fn handle_assign_job(&self, job_id: JobId, tc: Toolchain) -> Result<AssignJobResult> {
        let need_toolchain = !self.cache.lock().unwrap().contains_toolchain(&tc);
        let mut job_toolchains = self.job_toolchains.lock().unwrap();

        if let Some(other_tc) = job_toolchains.insert(job_id, tc.clone()) {
            // Remove the toolchain on error
            job_toolchains.remove(&job_id);
            bail!(
                "[{}]: Duplicate toolchain assigned, not replacing toolchain {:?} with {:?}",
                job_id,
                other_tc,
                tc
            );
        };

        // Drop lock
        drop(job_toolchains);

        let num_active_jobs = self.jobs_active.load(Ordering::SeqCst);
        let mut jobs_queued = self.jobs_queued.lock().unwrap();
        jobs_queued.insert(job_id, Instant::now());
        let num_queued_jobs = jobs_queued.len();

        // Drop lock
        drop(jobs_queued);

        let state = if need_toolchain {
            JobState::Pending
        } else {
            // TODO: can start prepping the build environment now
            JobState::Ready
        };

        Ok(AssignJobResult {
            state,
            need_toolchain,
            num_queued_jobs,
            num_active_jobs,
        })
    }

    fn handle_submit_toolchain(
        &self,
        requester: &dyn ServerOutgoing,
        job_id: JobId,
        mut tc_rdr: ToolchainReader,
    ) -> Result<SubmitToolchainResult> {
        // TODO: need to lock the toolchain until the container has started
        // TODO: can start prepping container
        let tc = match self.job_toolchains.lock().unwrap().get(&job_id).cloned() {
            Some(tc) => tc,
            None => {
                // Remove the job on error
                self.jobs_queued.lock().unwrap().remove(&job_id);
                return Ok(SubmitToolchainResult::JobNotFound);
            }
        };

        if let Err(err) = requester
            .do_update_job_state(
                job_id,
                JobState::Ready,
                self.jobs_queued.lock().unwrap().len(),
                self.jobs_active.load(Ordering::SeqCst),
            )
            .context("Failed to update job state")
        {
            // Remove the job on error
            self.jobs_queued.lock().unwrap().remove(&job_id);
            self.job_toolchains.lock().unwrap().remove(&job_id);

            warn!(
                "[handle_submit_toolchain({})]: {:?} ({} -> {})",
                job_id,
                err,
                JobState::Pending,
                JobState::Ready
            );
            return Err(err);
        }

        let mut cache = self.cache.lock().unwrap();
        if cache.contains_toolchain(&tc) {
            // Drop the lock
            drop(cache);
            // Consume the entire toolchain request body
            std::io::copy(&mut tc_rdr, &mut std::io::empty()).unwrap_or_else(|err| {
                warn!("[handle_submit_toolchain({})]: {:?}", job_id, err);
                0
            });
            Ok(SubmitToolchainResult::Success)
        } else {
            Ok(cache
                .insert_with(&tc, |mut file| io::copy(&mut tc_rdr, &mut file).map(|_| ()))
                .map(|_| SubmitToolchainResult::Success)
                .unwrap_or(SubmitToolchainResult::CannotCache))
        }
    }

    fn handle_run_job(
        &self,
        requester: &dyn ServerOutgoing,
        job_id: JobId,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs_rdr: InputsReader,
    ) -> Result<RunJobResult> {
        // Remove the job toolchain as early as possible.
        let tc = match self.job_toolchains.lock().unwrap().remove(&job_id) {
            Some(tc) => tc,
            None => {
                // Remove the job on error
                self.jobs_queued.lock().unwrap().remove(&job_id);
                return Ok(RunJobResult::JobNotFound);
            }
        };

        // Move job from queued to active
        let num_active_jobs = self.jobs_active.fetch_add(1, Ordering::SeqCst) + 1;
        let mut jobs_queued = self.jobs_queued.lock().unwrap();
        jobs_queued.remove(&job_id);
        let num_queued_jobs = jobs_queued.len();

        // Drop lock
        drop(jobs_queued);

        // Notify the scheduler the job has started.
        // Don't return an error, because this request is between the client and this server.
        // The client is expecting the server to perform this work, regardless of whether the
        // scheduler has pruned this job due to missing the pending timeout.
        if let Err(err) = requester
            .do_update_job_state(job_id, JobState::Started, num_queued_jobs, num_active_jobs)
            .context("Failed to update job state")
        {
            warn!(
                "[handle_run_job({})]: {:?} ({} -> {})",
                job_id,
                err,
                JobState::Ready,
                JobState::Started
            );
        }

        // Do the build
        let res = self
            .builder
            .run_build(job_id, tc, command, outputs, inputs_rdr, &self.cache);

        // Move job from active to done
        let num_queued_jobs = self.jobs_queued.lock().unwrap().len();
        let num_active_jobs = self.jobs_active.fetch_sub(1, Ordering::SeqCst) - 1;

        // Notify the scheduler the job is complete.
        // Don't return an error, because this request is between the client and this server.
        // The client is expecting the server to perform this work, regardless of whether the
        // scheduler has pruned this job due to missing the pending timeout.
        if let Err(err) = requester
            .do_update_job_state(job_id, JobState::Complete, num_queued_jobs, num_active_jobs)
            .context("Failed to update job state")
        {
            warn!(
                "[handle_run_job({})]: {:?} ({} -> {})",
                job_id,
                err,
                JobState::Started,
                JobState::Complete
            );
        }

        match res {
            Err(e) => Err(e.context("run_job build failed")),
            Ok(res) => Ok(RunJobResult::Complete(JobComplete {
                output: res.output,
                outputs: res.outputs,
            })),
        }
    }
}
