#![cfg(all(feature = "dist-client", feature = "dist-server"))]

extern crate assert_cmd;
#[macro_use]
extern crate log;
extern crate sccache;
extern crate serde_json;

use async_trait::async_trait;

use crate::harness::{
    get_stats, sccache_command, start_local_daemon, stop_local_daemon, write_json_cfg, write_source,
};
use assert_cmd::prelude::*;
use sccache::config::HTTPUrl;
use sccache::dist::{
    AssignJobResult, CompileCommand, JobId, RunJobResult, ServerIncoming, ServerOutgoing,
    SubmitToolchainResult, Toolchain,
};
use std::ffi::OsStr;
use std::path::Path;

use sccache::errors::*;

mod harness;

fn basic_compile(tmpdir: &Path, sccache_cfg_path: &Path, sccache_cached_cfg_path: &Path) {
    let envs: Vec<(_, &OsStr)> = vec![
        ("RUST_BACKTRACE", "1".as_ref()),
        ("SCCACHE_LOG", "trace".as_ref()),
        ("SCCACHE_CONF", sccache_cfg_path.as_ref()),
        ("SCCACHE_CACHED_CONF", sccache_cached_cfg_path.as_ref()),
    ];
    let source_file = "x.c";
    let obj_file = "x.o";
    write_source(tmpdir, source_file, "#if !defined(SCCACHE_TEST_DEFINE)\n#error SCCACHE_TEST_DEFINE is not defined\n#endif\nint x() { return 5; }");
    sccache_command()
        .args([
            std::env::var("CC")
                .unwrap_or_else(|_| "gcc".to_string())
                .as_str(),
            "-c",
            "-DSCCACHE_TEST_DEFINE",
        ])
        .arg(tmpdir.join(source_file))
        .arg("-o")
        .arg(tmpdir.join(obj_file))
        .envs(envs)
        .assert()
        .success();
}

pub fn dist_test_sccache_client_cfg(
    tmpdir: &Path,
    scheduler_url: HTTPUrl,
) -> sccache::config::FileConfig {
    let mut sccache_cfg = harness::sccache_client_cfg(tmpdir, false);
    sccache_cfg.cache.disk.as_mut().unwrap().size = 0;
    sccache_cfg.dist.scheduler_url = Some(scheduler_url);
    sccache_cfg
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_basic() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();
    system.add_server();

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    get_stats(|info| {
        assert_eq!(1, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, info.stats.dist_errors);
        assert_eq!(1, info.stats.compile_requests);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_restartedserver() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();
    let server_handle = system.add_server();

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    system.restart_server(&server_handle);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    get_stats(|info| {
        assert_eq!(2, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, info.stats.dist_errors);
        assert_eq!(2, info.stats.compile_requests);
        assert_eq!(2, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(2, info.stats.cache_misses.all());
    });
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_nobuilder() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    get_stats(|info| {
        assert_eq!(0, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(1, info.stats.dist_errors);
        assert_eq!(1, info.stats.compile_requests);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });
}

struct FailingServer;

#[async_trait]
impl ServerIncoming for FailingServer {
    fn start_heartbeat(&self, requester: std::sync::Arc<dyn ServerOutgoing>) {
        tokio::spawn(async move {
            trace!("Performing heartbeat");
            match requester.do_heartbeat(0, 0).await {
                Ok(sccache::dist::HeartbeatServerResult { is_new }) => {
                    trace!("Heartbeat success is_new={}", is_new);
                }
                Err(e) => {
                    error!("Failed to send heartbeat to server: {}", e);
                }
            }
        });
    }

    async fn handle_assign_job(&self, _tc: Toolchain) -> Result<AssignJobResult> {
        let need_toolchain = false;
        Ok(AssignJobResult {
            job_id: JobId(0),
            need_toolchain,
            num_assigned_jobs: 1,
            num_active_jobs: 0,
        })
    }
    async fn handle_submit_toolchain(
        &self,
        _job_id: JobId,
        _tc_rdr: std::pin::Pin<&mut (dyn tokio::io::AsyncRead + Send)>,
    ) -> Result<SubmitToolchainResult> {
        panic!("should not have submitted toolchain")
    }
    async fn handle_run_job(
        &self,
        _requester: &dyn ServerOutgoing,
        _job_id: JobId,
        _command: CompileCommand,
        _outputs: Vec<String>,
        _inputs_rdr: std::pin::Pin<&mut (dyn tokio::io::AsyncRead + Send)>,
    ) -> Result<RunJobResult> {
        bail!("internal build failure")
    }
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_failingserver() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();
    system.add_custom_server(FailingServer);

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    get_stats(|info| {
        assert_eq!(0, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(1, info.stats.dist_errors);
        assert_eq!(1, info.stats.compile_requests);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });
}
