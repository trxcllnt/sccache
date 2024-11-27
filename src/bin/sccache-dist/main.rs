use anyhow::{bail, Context, Error, Result};
use async_trait::async_trait;
use base64::Engine;
use futures::lock::Mutex;
use itertools::Itertools;
use rand::{rngs::OsRng, RngCore};
use sccache::config::{
    scheduler as scheduler_config, server as server_config, INSECURE_DIST_CLIENT_TOKEN,
};
use sccache::dist::http::{HEARTBEAT_ERROR_INTERVAL, HEARTBEAT_INTERVAL};
use sccache::dist::{
    self, AllocJobResult, AssignJobResult, BuilderIncoming, CompileCommand, HeartbeatServerResult,
    JobAlloc, JobAuthorizer, JobComplete, JobId, RunJobResult, SchedulerIncoming,
    SchedulerOutgoing, SchedulerStatusResult, ServerId, ServerIncoming, ServerNonce,
    ServerOutgoing, ServerStatusResult, SubmitToolchainResult, TcCache, Toolchain,
    UpdateJobStateResult,
};
use sccache::util::daemonize;
use sccache::util::BASE64_URL_SAFE_ENGINE;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
                    tracing::warn!(
                        "Scheduler starting with DANGEROUSLY_INSECURE server authentication"
                    );
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

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            let scheduler = Scheduler::new(remember_server_error_timeout);

            let http_scheduler = dist::http::Scheduler::new(
                public_addr,
                scheduler,
                check_client_auth,
                check_server_auth,
            );

            runtime.block_on(async move {
                match http_scheduler.start().await {
                    Ok(_) => {}
                    Err(err) => panic!("Err: {err}"),
                }
            });

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
            let num_cpus =
                (std::thread::available_parallelism().unwrap().get() - num_cpus_to_ignore).max(1);

            tracing::debug!("Server num_cpus={num_cpus}");

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            runtime.block_on(async move {
                let builder: Box<dyn dist::BuilderIncoming> = match builder {
                    #[cfg(not(target_os = "freebsd"))]
                    server_config::BuilderType::Docker => Box::new(
                        build::DockerBuilder::new(sccache::jobserver::Client::new_num(num_cpus))
                            .await
                            .context("Docker builder failed to start")?,
                    ),
                    #[cfg(not(target_os = "freebsd"))]
                    server_config::BuilderType::Overlay {
                        bwrap_path,
                        build_dir,
                    } => Box::new(
                        build::OverlayBuilder::new(
                            bwrap_path,
                            build_dir,
                            sccache::jobserver::Client::new_num(num_cpus),
                        )
                        .await
                        .context("Overlay builder failed to start")?,
                    ),
                    #[cfg(target_os = "freebsd")]
                    server_config::BuilderType::Pot {
                        pot_fs_root,
                        clone_from,
                        pot_cmd,
                        pot_clone_args,
                    } => Box::new(
                        build::PotBuilder::new(
                            pot_fs_root,
                            clone_from,
                            pot_cmd,
                            pot_clone_args,
                            sccache::jobserver::Client::new_num(num_cpus),
                        )
                        .await
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
                        tracing::warn!(
                            "Server starting with DANGEROUSLY_INSECURE scheduler authentication"
                        );
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
                    num_cpus,
                    server,
                )
                .context("Failed to create sccache HTTP server instance")?;

                match http_server.start().await {
                    Ok(_) => Ok(()),
                    Err(err) => panic!("Err: {err}"),
                }
            })?;

            unreachable!();
        }
    }
}

fn init_logging() {
    if env::var(sccache::LOGGING_ENV).is_ok() {
        match tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_env(sccache::LOGGING_ENV).unwrap_or_else(
                    |_| {
                        // axum logs rejections from built-in extractors with the `axum::rejection`
                        // target, at `TRACE` level. `axum::rejection=trace` enables showing those events
                        format!(
                            "{}=debug,tower_http=debug,axum::rejection=trace",
                            env!("CARGO_CRATE_NAME")
                        )
                        .into()
                    },
                ),
            )
            .with(tracing_subscriber::fmt::layer())
            .try_init()
        {
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
}

impl Scheduler {
    pub fn new(remember_server_error_timeout: u64) -> Self {
        Scheduler {
            servers: Mutex::new(HashMap::new()),
            remember_server_error_timeout: Duration::from_secs(remember_server_error_timeout),
        }
    }

    fn prune_servers(&self, servers: &mut HashMap<ServerId, ServerDetails>) {
        let now = Instant::now();

        let mut dead_servers = Vec::new();

        for (&server_id, server) in servers.iter_mut() {
            if now.duration_since(server.last_seen) > dist::http::HEARTBEAT_TIMEOUT {
                dead_servers.push(server_id);
            }
        }

        for server_id in dead_servers {
            tracing::warn!(
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

#[async_trait]
impl SchedulerIncoming for Scheduler {
    async fn handle_alloc_job(
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

        let try_assign_job = |server_id: ServerId, tc: Toolchain| async move {
            // LOCKS
            let mut servers = self.servers.lock().await;
            let server = match servers.get_mut(&server_id) {
                Some(server) => server,
                _ => bail!("Failed to assign job to unknown server"),
            };

            let assign_auth = server
                .job_authorizer
                .generate_token(JobId(server.server_nonce.as_u64()))
                .map_err(Error::from)
                .context("Could not create assign_auth token")?;

            drop(servers);

            let AssignJobResult {
                job_id,
                need_toolchain,
                num_assigned_jobs,
                num_active_jobs,
            } = match requester.do_assign_job(server_id, tc, assign_auth).await {
                Ok(res) => res,
                Err(err) => {
                    // LOCKS
                    let mut servers = self.servers.lock().await;
                    // Couldn't assign the job, so store the last_error
                    if let Some(server) = servers.get_mut(&server_id) {
                        server.last_error = Some(Instant::now());
                    }
                    // Prune servers
                    self.prune_servers(&mut servers);
                    return Err(err);
                }
            };

            // LOCKS
            let mut servers = self.servers.lock().await;
            let server = match servers.get_mut(&server_id) {
                Some(server) => server,
                _ => bail!("Failed to assign job to unknown server"),
            };

            // Assigned the job, so update server stats
            server.last_seen = Instant::now();
            server.num_assigned_jobs = num_assigned_jobs;
            server.num_active_jobs = num_active_jobs;

            let job_auth = server
                .job_authorizer
                .generate_token(job_id)
                .map_err(Error::from)
                .context("Could not create job auth token")?;

            if let Some(last_error) = server.last_error {
                tracing::debug!(
                    "[alloc_job({}, {})]: Assigned job to server whose most recent error was {:?} ago",
                    server_id.addr(),
                    job_id,
                    Instant::now() - last_error
                );
            } else {
                tracing::debug!(
                    "[alloc_job({}, {})]: Job created and assigned to server",
                    server_id.addr(),
                    job_id,
                );
            }

            // Prune servers only after updating this server's last_seen time.
            self.prune_servers(&mut servers);

            Ok(AllocJobResult::Success {
                job_alloc: JobAlloc {
                    auth: job_auth,
                    job_id,
                    server_id,
                },
                need_toolchain,
            })
        };

        let get_best_server_by_least_load_and_oldest_error =
            |servers: &mut HashMap<ServerId, ServerDetails>, tried_servers: &HashSet<ServerId>| {
                let now = Instant::now();

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
                        // Number of jobs assigned to this server and are waiting on the client to start
                        (server.num_assigned_jobs * 2) // Penalize un-started jobs 2x
                        // Number of jobs assigned to this server and are running
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
            // LOCKS
            let server_id = {
                // LOCKS
                let mut servers = self.servers.lock().await;
                get_best_server_by_least_load_and_oldest_error(&mut servers, &tried_servers)
            };

            // Take the top candidate. If we can't allocate the job to it,
            // remove it from the candidates list and try the next server.
            if let Some(server_id) = server_id {
                // Attempt to assign the job to this server. If assign_job fails,
                // store the error and attempt to assign to the next server.
                // If all servers error, return the last error to the client.
                match try_assign_job(server_id, tc.clone()).await {
                    Ok(res) => {
                        // If assign_job succeeded, return the result
                        result = Some(Ok(res));
                        break;
                    }
                    Err(err) => {
                        // If alloc_job failed, try the next best server
                        tracing::warn!(
                            "[alloc_job({})]: Error assigning job to server: {}",
                            server_id.addr(),
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

    async fn handle_heartbeat_server(
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
        let mut servers = self.servers.lock().await;

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

        tracing::info!("Registered new server {:?}", server_id);

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
            },
        );

        Ok(HeartbeatServerResult { is_new: true })
    }

    async fn handle_update_job_state(
        &self,
        server_id: ServerId,
        num_assigned_jobs: usize,
        num_active_jobs: usize,
    ) -> Result<UpdateJobStateResult> {
        let mut servers = self.servers.lock().await;
        let server = match servers.get_mut(&server_id) {
            Some(server) => server,
            _ => bail!("Failed to reserve job on unknown server"),
        };

        server.last_seen = Instant::now();
        server.num_assigned_jobs = num_assigned_jobs;
        server.num_active_jobs = num_active_jobs;

        // Prune servers only after updating this server's last_seen time.
        self.prune_servers(&mut servers);

        Ok(UpdateJobStateResult::Success)
    }

    async fn handle_status(&self) -> Result<SchedulerStatusResult> {
        let Scheduler {
            remember_server_error_timeout,
            ..
        } = *self;

        // LOCKS
        let mut servers = self.servers.lock().await;

        // Prune servers before reporting the scheduler status
        self.prune_servers(&mut servers);

        let mut assigned_jobs = 0;
        let mut active_jobs = 0;

        let mut servers_map = HashMap::<std::net::SocketAddr, ServerStatusResult>::new();
        for (server_id, server) in servers.iter() {
            assigned_jobs += server.num_assigned_jobs;
            active_jobs += server.num_active_jobs;
            servers_map.insert(
                server_id.addr(),
                ServerStatusResult {
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
            assigned: assigned_jobs,
            active: active_jobs,
            servers: servers_map,
        })
    }
}

struct JobInfo {
    ctime: Instant,
    toolchain: Toolchain,
}

pub struct Server {
    builder: Box<dyn BuilderIncoming>,
    cache: Mutex<TcCache>,
    job_count: AtomicUsize,
    jobs_active: Arc<AtomicUsize>,
    jobs_assigned: Arc<Mutex<HashMap<JobId, JobInfo>>>,
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
            job_count: AtomicUsize::new(0),
            jobs_active: Arc::new(AtomicUsize::new(0)),
            jobs_assigned: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl ServerIncoming for Server {
    fn start_heartbeat(&self, requester: Arc<dyn ServerOutgoing>) {
        let jobs_active = self.jobs_active.clone();
        let jobs_assigned = self.jobs_assigned.clone();
        // Wait up to 90s for a client to start a job.
        // Remove jobs the client hasn't started within this interval.
        let unstarted_job_timeout = std::time::Duration::from_secs(90);

        // TODO: detect if this panics
        tokio::spawn(async move {
            loop {
                let stale_jobs = {
                    let jobs_assigned = jobs_assigned.lock().await;
                    let now = std::time::Instant::now();
                    let mut stale_jobs = vec![];
                    for (&job_id, job_info) in jobs_assigned.iter() {
                        if now.duration_since(job_info.ctime) >= unstarted_job_timeout {
                            stale_jobs.push(job_id);
                        }
                    }
                    stale_jobs
                };

                let num_assigned_jobs = {
                    let mut jobs_assigned = jobs_assigned.lock().await;
                    for job_id in stale_jobs {
                        jobs_assigned.remove(&job_id);
                    }
                    jobs_assigned.len()
                };

                let num_active_jobs = jobs_active.load(std::sync::atomic::Ordering::SeqCst);

                let due_time = match requester
                    .do_heartbeat(num_assigned_jobs, num_active_jobs)
                    .await
                {
                    Ok(HeartbeatServerResult { is_new }) => {
                        tracing::trace!("Heartbeat success is_new={}", is_new);
                        HEARTBEAT_INTERVAL
                    }
                    Err(e) => {
                        tracing::error!("Failed to send heartbeat to server: {}", e);
                        HEARTBEAT_ERROR_INTERVAL
                    }
                };

                tokio::time::sleep(due_time).await;
            }
        });
    }

    async fn handle_assign_job(&self, tc: Toolchain) -> Result<AssignJobResult> {
        let need_toolchain = !self.cache.lock().await.contains_toolchain(&tc);
        let job_id = JobId(
            self.job_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst) as u64,
        );
        let num_assigned_jobs = {
            let mut jobs_assigned = self.jobs_assigned.lock().await;
            jobs_assigned.insert(
                job_id,
                JobInfo {
                    ctime: Instant::now(),
                    toolchain: tc,
                },
            );
            jobs_assigned.len()
        };
        let num_active_jobs = self.jobs_active.load(std::sync::atomic::Ordering::Relaxed);

        Ok(AssignJobResult {
            job_id,
            need_toolchain,
            num_assigned_jobs,
            num_active_jobs,
        })
    }

    async fn handle_submit_toolchain(
        &self,
        job_id: JobId,
        mut tc_rdr: std::pin::Pin<&mut (dyn tokio::io::AsyncRead + Send)>,
    ) -> Result<SubmitToolchainResult> {
        // TODO: need to lock the toolchain until the container has started
        // TODO: can start prepping container

        let tc = match self
            .jobs_assigned
            .lock()
            .await
            .get(&job_id)
            .map(|j| j.toolchain.clone())
        {
            Some(tc) => tc,
            None => {
                // Remove the job on error
                self.jobs_assigned.lock().await.remove(&job_id);
                return Ok(SubmitToolchainResult::JobNotFound);
            }
        };

        let mut cache = self.cache.lock().await;

        let res = if cache.contains_toolchain(&tc) {
            // Drop the lock
            drop(cache);

            // // Ignore the toolchain request body
            // // TODO: Investigate if this causes early hangup warnings in
            // // the load balancer. If so, use the implementation below.
            // Ok(())

            // Consume the entire toolchain request body
            tokio::io::copy(&mut tc_rdr, &mut tokio::io::empty())
                .await
                .map(|_| ())
                .or_else(|err| {
                    tracing::warn!("[handle_submit_toolchain({})]: {:?}", job_id, err);
                    // Ignore errors reading the request body
                    Ok(())
                })
        } else {
            cache
                .insert_with(&tc, |mut file| async move {
                    tokio::io::copy(&mut tc_rdr, &mut file).await
                })
                .await
        };

        match res {
            Ok(_) => Ok(SubmitToolchainResult::Success),
            Err(err) => {
                tracing::warn!("[handle_submit_toolchain({})]: {:?}", job_id, err);
                // Remove the job on error
                self.jobs_assigned.lock().await.remove(&job_id);
                Ok(SubmitToolchainResult::CannotCache)
            }
        }
    }

    async fn handle_run_job(
        &self,
        requester: &dyn ServerOutgoing,
        job_id: JobId,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs_rdr: std::pin::Pin<&mut (dyn tokio::io::AsyncRead + Send)>,
    ) -> Result<RunJobResult> {
        // Remove the job from assigned map
        let (tc, num_assigned_jobs) = {
            let mut jobs_assigned = self.jobs_assigned.lock().await;
            match jobs_assigned.remove(&job_id).map(|j| j.toolchain.clone()) {
                Some(tc) => (tc, jobs_assigned.len()),
                None => return Ok(RunJobResult::JobNotFound),
            }
        };

        // Count the job as active
        let num_active_jobs = self
            .jobs_active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        // Notify the scheduler the job has started.
        // Don't return an error, because this request is between the client and this server.
        // The client is expecting the server to perform this work, regardless of whether the
        // scheduler has pruned this job due to missing the pending timeout.
        if let Err(err) = requester
            .do_update_job_state(num_assigned_jobs, num_active_jobs)
            .await
            .context("Failed to update job state")
        {
            tracing::warn!(
                "[handle_run_job({})]: {:?} ({} -> {})",
                job_id,
                err,
                "Ready",
                "Started"
            );
        }

        // Do the build
        let res = self
            .builder
            .run_build(job_id, tc, command, outputs, inputs_rdr, &self.cache)
            .await;

        // Move job from active to done
        let num_assigned_jobs = self.jobs_assigned.lock().await.len();
        let num_active_jobs = self
            .jobs_active
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            - 1;

        // Notify the scheduler the job is complete.
        // Don't return an error, because this request is between the client and this server.
        // The client is expecting the server to perform this work, regardless of whether the
        // scheduler has pruned this job due to missing the pending timeout.
        if let Err(err) = requester
            .do_update_job_state(num_assigned_jobs, num_active_jobs)
            .await
            .context("Failed to update job state")
        {
            tracing::warn!(
                "[handle_run_job({})]: {:?} ({} -> {})",
                job_id,
                err,
                "Started",
                "Complete"
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
