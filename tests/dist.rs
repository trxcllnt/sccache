#![cfg(all(feature = "dist-client", feature = "dist-server"))]

extern crate assert_cmd;
#[macro_use]
extern crate log;
extern crate sccache;
extern crate serde_json;

use async_trait::async_trait;
use harness::DistSystem;
use nix::unistd::ForkResult;

use crate::harness::{
    cargo_command, get_stats, init_cargo, sccache_command, start_local_daemon, stop_local_daemon,
    write_json_cfg, write_source,
};
use assert_cmd::prelude::*;
use sccache::{
    config::{CacheType, HTTPUrl, MessageBroker, RedisCacheConfig},
    dist::{BuildResult, BuilderIncoming, CompileCommand},
};
use std::ffi::OsStr;
use std::path::Path;
use std::process::Output;

use test_case::test_case;

mod harness;

// TODO:
// * Test each builder (docker and overlay for Linux, pot for freebsd)

fn basic_compile(tmpdir: &Path, sccache_cfg_path: &Path, sccache_cached_cfg_path: &Path) {
    let envs: Vec<(_, &OsStr)> = vec![
        ("RUST_BACKTRACE", "1".as_ref()),
        ("SCCACHE_LOG", "sccache=debug".as_ref()),
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

fn rust_compile(tmpdir: &Path, sccache_cfg_path: &Path, sccache_cached_cfg_path: &Path) -> Output {
    let sccache_path = assert_cmd::cargo::cargo_bin("sccache").into_os_string();
    let envs: Vec<(_, &OsStr)> = vec![
        ("RUSTC_WRAPPER", sccache_path.as_ref()),
        ("CARGO_TARGET_DIR", "target".as_ref()),
        ("RUST_BACKTRACE", "1".as_ref()),
        ("SCCACHE_LOG", "debug".as_ref()),
        ("SCCACHE_CONF", sccache_cfg_path.as_ref()),
        ("SCCACHE_CACHED_CONF", sccache_cached_cfg_path.as_ref()),
    ];
    let cargo_name = "sccache-dist-test";
    let cargo_path = init_cargo(tmpdir, cargo_name);

    let manifest_file = "Cargo.toml";
    let source_file = "src/main.rs";

    write_source(
        &cargo_path,
        manifest_file,
        r#"[package]
        name = "sccache-dist-test"
        version = "0.1.0"
        edition = "2021"
        [dependencies]
        libc = "0.2.169""#,
    );
    write_source(
        &cargo_path,
        source_file,
        r#"fn main() {
        println!("Hello, world!");
}"#,
    );

    cargo_command()
        .current_dir(cargo_path)
        .args(["build", "--release"])
        .envs(envs)
        .output()
        .unwrap()
}

pub fn dist_test_sccache_client_cfg(
    tmpdir: &Path,
    scheduler_url: HTTPUrl,
) -> sccache::config::FileConfig {
    let mut sccache_cfg = harness::sccache_client_cfg(tmpdir, false);
    sccache_cfg.cache.disk.as_mut().unwrap().size = 0;
    sccache_cfg.dist.scheduler_url = Some(scheduler_url);
    sccache_cfg.dist.net.connect_timeout = 10;
    sccache_cfg.dist.net.request_timeout = 30;
    sccache_cfg
}

#[test_case("rabbitmq" ; "With rabbitmq")]
#[test_case("redis" ; "With redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_basic(message_broker: &str) {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    let message_broker = system.add_message_broker(message_broker);
    system.add_scheduler(system.scheduler_cfg(&message_broker));
    system.add_server(system.server_cfg(&message_broker));

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

    stop_local_daemon();
}

#[test_case("rabbitmq" ; "With rabbitmq")]
#[test_case("redis" ; "With redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_restarted_server(message_broker: &str) {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    let message_broker = system.add_message_broker(message_broker);
    system.add_scheduler(system.scheduler_cfg(&message_broker));
    let server_handle = system.add_server(system.server_cfg(&message_broker));

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

    stop_local_daemon();
}

#[test_case("rabbitmq" ; "with RabbitMQ")]
#[test_case("redis" ; "with Redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_no_builder_times_out(message_broker: &str) {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    let message_broker = system.add_message_broker(message_broker);
    let mut scheduler_cfg = system.scheduler_cfg(&message_broker);
    scheduler_cfg.job_time_limit = 10;
    system.add_scheduler(scheduler_cfg);

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

    stop_local_daemon();
}

#[test_case("rabbitmq" ; "with RabbitMQ")]
#[test_case("redis" ; "with Redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_failing_server(message_broker: &str) {
    struct FailingBuilder;

    #[async_trait]
    impl BuilderIncoming for FailingBuilder {
        async fn run_build(
            &self,
            _job_id: &str,
            _toolchain_dir: &Path,
            _inputs: Vec<u8>,
            _command: CompileCommand,
            _outputs: Vec<String>,
        ) -> anyhow::Result<BuildResult> {
            Err(anyhow::anyhow!("server failure"))
        }
    }

    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();
    let server_id = format!("failing-server-{message_broker}");

    // Use redis as job and toolchain storage instead of disk storage.
    // This avoids permissions issues with the scheduler container running
    // as root vs. the forked custom failing server process being non-root
    let storage = Some(CacheType::Redis(RedisCacheConfig {
        endpoint: Some(DistSystem::redis_url()),
        ..Default::default()
    }));

    // Fork before creating the DistSystem instance so its destructors aren't
    // also run by the child process when it exits
    match unsafe { nix::unistd::fork() } {
        Ok(ForkResult::Child) => {
            std::process::exit(
                harness::add_custom_server(
                    &server_id,
                    &DistSystem::message_broker_cfg(message_broker),
                    FailingBuilder,
                    storage.clone(),
                    tmpdir,
                )
                .is_err() as i32,
            );
        }
        Ok(ForkResult::Parent { child }) => {
            let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
            let message_broker = system.add_message_broker(message_broker);
            if let MessageBroker::AMQP(_) = message_broker {
                system.add_redis();
            }

            let mut scheduler_cfg = system.scheduler_cfg(&message_broker);
            scheduler_cfg.jobs.storage = storage.clone();
            scheduler_cfg.toolchains.storage = storage.clone();
            system.add_scheduler(scheduler_cfg);
            system.add_custom_server(&server_id, child);

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

            stop_local_daemon();
        }
        Err(e) => {
            panic!("{e}");
        }
    }
}

#[test_case("rabbitmq" ; "With rabbitmq")]
#[test_case("redis" ; "With redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_cargo_build(message_broker: &str) {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    let message_broker = system.add_message_broker(message_broker);
    system.add_scheduler(system.scheduler_cfg(&message_broker));
    system.add_server(system.server_cfg(&message_broker));

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    rust_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path)
        .assert()
        .success();
    get_stats(|info| {
        assert_eq!(1, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, info.stats.dist_errors);
        // check >= 5 because cargo >=1.82 does additional requests with -vV
        assert!(info.stats.compile_requests >= 5);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });

    stop_local_daemon();
}

#[test_case("rabbitmq" ; "With rabbitmq")]
#[test_case("redis" ; "With redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_cargo_makeflags(message_broker: &str) {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    let message_broker = system.add_message_broker(message_broker);
    system.add_scheduler(system.scheduler_cfg(&message_broker));
    system.add_server(system.server_cfg(&message_broker));

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    let compile_output = rust_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    assert!(!String::from_utf8_lossy(&compile_output.stderr)
        .contains("warning: failed to connect to jobserver from environment variable"));

    get_stats(|info| {
        assert_eq!(1, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, info.stats.dist_errors);
        // check >= 5 because cargo >=1.82 does additional requests with -vV
        assert!(info.stats.compile_requests >= 5);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });

    stop_local_daemon();
}
