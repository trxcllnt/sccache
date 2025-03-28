#![cfg(all(feature = "dist-client", feature = "dist-server"))]

extern crate assert_cmd;
#[macro_use]
extern crate log;
extern crate sccache;
extern crate serde_json;

mod harness;

use assert_cmd::prelude::*;
use sccache::config::HTTPUrl;
use std::path::Path;
use std::process::Output;

use harness::{cargo_command, client::SccacheClient, dist::DistSystem, init_cargo, write_source};

// In case of panics, this command will destroy any dangling containers:
//   docker rm -f $(docker ps -aq --filter "name=sccache_dist_*")

// TODO:
// * Test each builder (docker and overlay for Linux, pot for freebsd)

fn cpp_compile(client: &SccacheClient, tmpdir: &Path) {
    let source_file = "x.c";
    let obj_file = "x.o";
    write_source(tmpdir, source_file, "#if !defined(SCCACHE_TEST_DEFINE)\n#error SCCACHE_TEST_DEFINE is not defined\n#endif\nint x() { return 5; }");
    client
        .cmd()
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
        .env("RUST_BACKTRACE", "1")
        .env("SCCACHE_RECACHE", "1")
        .env("TOKIO_WORKER_THREADS", "2")
        .assert()
        .success();
}

fn rust_compile(client: &SccacheClient, tmpdir: &Path) -> Output {
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
        .args(["build", "--release", "--jobs", "1"])
        .envs(
            client
                .cmd()
                .get_envs()
                .map(|(k, v)| (k, v.unwrap_or_default())),
        )
        .env("RUSTC_WRAPPER", &client.path)
        .env("CARGO_TARGET_DIR", "target")
        .env("RUST_BACKTRACE", "1")
        .env("SCCACHE_RECACHE", "1")
        .env("TOKIO_WORKER_THREADS", "2")
        .output()
        .unwrap()
}

pub fn dist_test_sccache_client_cfg(
    tmpdir: &Path,
    scheduler_url: HTTPUrl,
) -> sccache::config::FileConfig {
    let mut sccache_cfg = harness::client::sccache_client_cfg(tmpdir, false);
    sccache_cfg.cache.disk.as_mut().unwrap().size = 0;
    sccache_cfg.dist.scheduler_url = Some(scheduler_url);
    sccache_cfg.dist.net.connect_timeout = 10;
    sccache_cfg.dist.net.request_timeout = 30;
    sccache_cfg
}

#[cfg_attr(not(feature = "dist-tests"), ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dist_cargo_build() {
    async fn run_test_with(message_broker: &str) {
        let test_name = format!("test_dist_cargo_build_{message_broker}");
        let system = DistSystem::builder()
            .with_name(&test_name)
            .with_scheduler()
            .with_server()
            .with_message_broker(message_broker)
            .build();

        let client = system.new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ));

        let output = rust_compile(&client, system.data_dir());

        // Ensure sccache ignores inherited jobservers in CARGO_MAKEFLAGS
        assert!(!String::from_utf8_lossy(&output.stderr)
            .contains("warning: failed to connect to jobserver from environment variable"));

        // Assert compilation succeeded
        output.assert().success();

        let stats = client.stats().unwrap();
        assert_eq!(1, stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, stats.dist_errors);
        // check >= 5 because cargo >=1.82 does additional requests with -vV
        assert!(stats.compile_requests >= 5);
        assert_eq!(1, stats.requests_executed);
        assert_eq!(0, stats.cache_hits.all());
        assert_eq!(1, stats.forced_recaches);
    }

    tokio::try_join!(
        tokio::task::spawn(run_test_with("rabbitmq")),
        tokio::task::spawn(run_test_with("redis")),
    )
    .unwrap();
}

#[cfg_attr(not(feature = "dist-tests"), ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dist_cpp_disk_storage() {
    async fn run_test_with(message_broker: &str) {
        let test_name = format!("test_dist_cpp_disk_storage_{message_broker}");
        let system = DistSystem::builder()
            .with_name(&test_name)
            .with_scheduler()
            .with_server()
            .with_message_broker(message_broker)
            .build();

        let client = system.new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ));

        cpp_compile(&client, system.data_dir());

        let stats = client.stats().unwrap();
        assert_eq!(1, stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, stats.dist_errors);
        assert_eq!(1, stats.compile_requests);
        assert_eq!(1, stats.requests_executed);
        assert_eq!(0, stats.cache_hits.all());
        assert_eq!(1, stats.forced_recaches);
    }

    tokio::try_join!(
        tokio::task::spawn(run_test_with("rabbitmq")),
        tokio::task::spawn(run_test_with("redis")),
    )
    .unwrap();
}

#[cfg_attr(not(feature = "dist-tests"), ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dist_cpp_cloud_storage() {
    async fn run_test_with(message_broker: &str) {
        let test_name = format!("test_dist_cpp_cloud_storage_{message_broker}");
        let system = DistSystem::builder()
            .with_name(&test_name)
            .with_scheduler()
            .with_server()
            .with_message_broker(message_broker)
            .with_redis_storage()
            .build();

        let client = system.new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ));

        cpp_compile(&client, system.data_dir());

        let stats = client.stats().unwrap();
        assert_eq!(1, stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, stats.dist_errors);
        assert_eq!(1, stats.compile_requests);
        assert_eq!(1, stats.requests_executed);
        assert_eq!(0, stats.cache_hits.all());
        assert_eq!(1, stats.forced_recaches);
    }

    tokio::try_join!(
        tokio::task::spawn(run_test_with("rabbitmq")),
        tokio::task::spawn(run_test_with("redis")),
    )
    .unwrap();
}

#[cfg_attr(not(feature = "dist-tests"), ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dist_cpp_server_restart() {
    async fn run_test_with(message_broker: &str) {
        let test_name = format!("test_dist_cpp_server_restart_{message_broker}");
        let system = DistSystem::builder()
            .with_name(&test_name)
            .with_scheduler()
            .with_server()
            .with_message_broker(message_broker)
            .with_redis_storage()
            .build();

        let client = system.new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ));

        cpp_compile(&client, system.data_dir());

        system.restart_server(system.server(0).unwrap());

        cpp_compile(&client, system.data_dir());

        let stats = client.stats().unwrap();
        assert_eq!(2, stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, stats.dist_errors);
        assert_eq!(2, stats.compile_requests);
        assert_eq!(2, stats.requests_executed);
        assert_eq!(0, stats.cache_hits.all());
        assert_eq!(2, stats.forced_recaches);
    }

    tokio::try_join!(
        tokio::task::spawn(run_test_with("rabbitmq")),
        tokio::task::spawn(run_test_with("redis")),
    )
    .unwrap();
}

#[cfg_attr(not(feature = "dist-tests"), ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dist_cpp_no_server_times_out() {
    async fn run_test_with(message_broker: &str) {
        let test_name = format!("test_dist_cpp_no_server_times_out_{message_broker}");
        let system = DistSystem::builder()
            .with_name(&test_name)
            .with_scheduler()
            .with_message_broker(message_broker)
            .with_job_time_limit(10)
            .with_redis_storage()
            .build();

        let client = system.new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ));

        cpp_compile(&client, system.data_dir());

        let stats = client.stats().unwrap();
        assert_eq!(0, stats.dist_compiles.values().sum::<usize>());
        assert_eq!(1, stats.dist_errors);
        assert_eq!(1, stats.compile_requests);
        assert_eq!(1, stats.requests_executed);
        assert_eq!(0, stats.cache_hits.all());
        assert_eq!(1, stats.forced_recaches);
    }

    tokio::try_join!(
        tokio::task::spawn(run_test_with("rabbitmq")),
        tokio::task::spawn(run_test_with("redis")),
    )
    .unwrap();
}

#[cfg_attr(not(feature = "dist-tests"), ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_dist_cpp_two_servers() {
    async fn run_test_with(message_broker: &str) {
        let test_name = format!("test_dist_cpp_two_servers_{message_broker}");
        let system = DistSystem::builder()
            .with_name(&test_name)
            .with_scheduler()
            .with_server()
            .with_server()
            .with_message_broker(message_broker)
            .with_redis_storage()
            .build();

        let client = system.new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ));

        let compile_cpp = || {
            let client = client.clone();
            let tmpdir = system.data_dir().to_owned();
            move || cpp_compile(&client, &tmpdir)
        };

        let _ = tokio::try_join!(
            tokio::task::spawn_blocking(compile_cpp()),
            tokio::task::spawn_blocking(compile_cpp()),
            tokio::task::spawn_blocking(compile_cpp()),
            tokio::task::spawn_blocking(compile_cpp()),
        );

        let stats = client.stats().unwrap();
        assert_eq!(4, stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, stats.dist_errors);
        assert_eq!(4, stats.compile_requests);
        assert_eq!(4, stats.requests_executed);
        assert_eq!(0, stats.cache_hits.all());
        assert_eq!(4, stats.forced_recaches);
    }

    tokio::try_join!(
        tokio::task::spawn(run_test_with("rabbitmq")),
        tokio::task::spawn(run_test_with("redis")),
    )
    .unwrap();
}

#[cfg_attr(not(feature = "dist-tests"), ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dist_cpp_errors_on_job_load_failures() {
    async fn run_test_with(message_broker: &str) {
        let test_name = format!("test_dist_cpp_errors_on_job_load_failures_{message_broker}");
        let system = DistSystem::builder()
            .with_name(&test_name)
            .with_scheduler()
            .with_server()
            .with_message_broker(message_broker)
            // Scheduler stores jobs in redis, but Server is loading from disk
            .with_scheduler_jobs_redis_storage()
            .build();

        let client = system.new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ));

        cpp_compile(&client, system.data_dir());

        let stats = client.stats().unwrap();
        assert_eq!(0, stats.dist_compiles.values().sum::<usize>());
        assert_eq!(1, stats.dist_errors);
        assert_eq!(1, stats.compile_requests);
        assert_eq!(1, stats.requests_executed);
        assert_eq!(0, stats.cache_hits.all());
        assert_eq!(1, stats.forced_recaches);
    }

    tokio::try_join!(
        tokio::task::spawn(run_test_with("rabbitmq")),
        tokio::task::spawn(run_test_with("redis")),
    )
    .unwrap();
}

#[cfg_attr(not(feature = "dist-tests"), ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dist_cpp_errors_on_toolchain_load_failures() {
    async fn run_test_with(message_broker: &str) {
        let test_name = format!("test_dist_cpp_errors_on_toolchain_load_failures_{message_broker}");
        let system = DistSystem::builder()
            .with_name(&test_name)
            .with_scheduler()
            .with_server()
            .with_message_broker(message_broker)
            // Scheduler stores toolchains in redis, but Server is loading from disk
            .with_scheduler_toolchains_redis_storage()
            .build();

        let client = system.new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ));

        cpp_compile(&client, system.data_dir());

        let stats = client.stats().unwrap();
        assert_eq!(0, stats.dist_compiles.values().sum::<usize>());
        assert_eq!(1, stats.dist_errors);
        assert_eq!(1, stats.compile_requests);
        assert_eq!(1, stats.requests_executed);
        assert_eq!(0, stats.cache_hits.all());
        assert_eq!(1, stats.forced_recaches);
    }

    tokio::try_join!(
        tokio::task::spawn(run_test_with("rabbitmq")),
        tokio::task::spawn(run_test_with("redis")),
    )
    .unwrap();
}
