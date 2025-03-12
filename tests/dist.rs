#![cfg(all(feature = "dist-client", feature = "dist-server"))]

extern crate assert_cmd;
#[macro_use]
extern crate log;
extern crate sccache;
extern crate serde_json;

use harness::{DistMessageBroker, DistSystem};

use crate::harness::{cargo_command, init_cargo, write_source, SccacheClient};
use assert_cmd::prelude::*;
use sccache::config::HTTPUrl;
use std::path::Path;
use std::process::Output;

use test_case::test_case;

mod harness;

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
        .output()
        .unwrap()
}

fn broker_and_storage(message_broker: &str) -> (DistMessageBroker, DistMessageBroker) {
    let message_broker = DistMessageBroker::new(message_broker);
    let storage = if message_broker.is_amqp() {
        DistMessageBroker::new("redis")
    } else {
        message_broker.clone()
    };
    (message_broker, storage)
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
fn test_dist_cargo_build(message_broker: &str) {
    let system = DistSystem::builder()
        .with_name(&format!("test_dist_cargo_build_{message_broker}"))
        .with_default_message_broker(message_broker)
        .with_default_scheduler()
        .with_default_server()
        .build();

    let client = system
        .new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ))
        .start();

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
    assert_eq!(1, stats.cache_misses.all());
}

#[test_case("rabbitmq" ; "With rabbitmq")]
#[test_case("redis" ; "With redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_cpp_with_disk(message_broker: &str) {
    let system = DistSystem::builder()
        .with_name(&format!("test_dist_cpp_{message_broker}"))
        .with_default_message_broker(message_broker)
        .with_default_scheduler()
        .with_default_server()
        .build();

    let client = system
        .new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ))
        .start();

    cpp_compile(&client, system.data_dir());

    let stats = client.stats().unwrap();
    assert_eq!(1, stats.dist_compiles.values().sum::<usize>());
    assert_eq!(0, stats.dist_errors);
    assert_eq!(1, stats.compile_requests);
    assert_eq!(1, stats.requests_executed);
    assert_eq!(0, stats.cache_hits.all());
    assert_eq!(1, stats.cache_misses.all());
}

#[test_case("rabbitmq" ; "With rabbitmq")]
#[test_case("redis" ; "With redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_cpp_with_cloud_storage(message_broker: &str) {
    let (broker, redis) = broker_and_storage(message_broker);

    let system = DistSystem::builder()
        .with_name(&format!(
            "test_dist_cpp_with_cloud_storage_{message_broker}"
        ))
        .with_default_scheduler()
        .with_default_server()
        .with_message_broker(&broker)
        .with_server_jobs_storage(&redis)
        .with_scheduler_jobs_storage(&redis)
        .with_server_toolchains_storage(&redis)
        .with_scheduler_toolchains_storage(&redis)
        .build();

    let client = system
        .new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ))
        .start();

    cpp_compile(&client, system.data_dir());

    let stats = client.stats().unwrap();
    assert_eq!(1, stats.dist_compiles.values().sum::<usize>());
    assert_eq!(0, stats.dist_errors);
    assert_eq!(1, stats.compile_requests);
    assert_eq!(1, stats.requests_executed);
    assert_eq!(0, stats.cache_hits.all());
    assert_eq!(1, stats.cache_misses.all());
}

#[test_case("rabbitmq" ; "With rabbitmq")]
#[test_case("redis" ; "With redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_restarted_server(message_broker: &str) {
    let system = DistSystem::builder()
        .with_name(&format!("test_dist_restarted_{message_broker}"))
        .with_default_message_broker(message_broker)
        .with_default_scheduler()
        .with_default_server()
        .build();

    let client = system
        .new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ))
        .start();

    cpp_compile(&client, system.data_dir());

    system.restart_server(system.server(0).unwrap());

    cpp_compile(&client, system.data_dir());

    let stats = client.stats().unwrap();
    assert_eq!(2, stats.dist_compiles.values().sum::<usize>());
    assert_eq!(0, stats.dist_errors);
    assert_eq!(2, stats.compile_requests);
    assert_eq!(2, stats.requests_executed);
    assert_eq!(0, stats.cache_hits.all());
    assert_eq!(2, stats.cache_misses.all());
}

#[test_case("rabbitmq" ; "with RabbitMQ")]
#[test_case("redis" ; "with Redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_no_server_times_out(message_broker: &str) {
    let system = DistSystem::builder()
        .with_name(&format!("test_dist_no_server_times_out_{message_broker}"))
        .with_default_message_broker(message_broker)
        .with_default_scheduler()
        .with_job_time_limit(10)
        .build();

    let client = system
        .new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ))
        .start();

    cpp_compile(&client, system.data_dir());

    let stats = client.stats().unwrap();
    assert_eq!(0, stats.dist_compiles.values().sum::<usize>());
    assert_eq!(1, stats.dist_errors);
    assert_eq!(1, stats.compile_requests);
    assert_eq!(1, stats.requests_executed);
    assert_eq!(0, stats.cache_hits.all());
    assert_eq!(1, stats.cache_misses.all());
}

#[test_case("rabbitmq" ; "with RabbitMQ")]
#[test_case("redis" ; "with Redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_errors_on_job_load_failures(message_broker: &str) {
    let (broker, redis) = broker_and_storage(message_broker);
    let system = DistSystem::builder()
        .with_name(&format!(
            "test_dist_errors_on_job_load_failures_{message_broker}"
        ))
        .with_default_scheduler()
        .with_default_server()
        .with_message_broker(&broker)
        // Scheduler stores jobs in redis, but Server is loading from disk
        .with_scheduler_jobs_storage(&redis)
        .build();

    let client = system
        .new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ))
        .start();

    cpp_compile(&client, system.data_dir());

    let stats = client.stats().unwrap();
    assert_eq!(0, stats.dist_compiles.values().sum::<usize>());
    assert_eq!(1, stats.dist_errors);
    assert_eq!(1, stats.compile_requests);
    assert_eq!(1, stats.requests_executed);
    assert_eq!(0, stats.cache_hits.all());
    assert_eq!(1, stats.cache_misses.all());
}

#[test_case("rabbitmq" ; "with RabbitMQ")]
#[test_case("redis" ; "with Redis")]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_errors_on_toolchain_load_failures(message_broker: &str) {
    let (broker, redis) = broker_and_storage(message_broker);
    let system = DistSystem::builder()
        .with_name(&format!(
            "test_dist_errors_on_toolchain_load_failures_{message_broker}"
        ))
        .with_default_scheduler()
        .with_default_server()
        .with_message_broker(&broker)
        // Scheduler stores toolchains in redis, but Server is loading from disk
        .with_scheduler_toolchains_storage(&redis)
        .build();

    let client = system
        .new_client(&dist_test_sccache_client_cfg(
            system.data_dir(),
            system.scheduler(0).unwrap().url(),
        ))
        .start();

    cpp_compile(&client, system.data_dir());

    let stats = client.stats().unwrap();
    assert_eq!(0, stats.dist_compiles.values().sum::<usize>());
    assert_eq!(1, stats.dist_errors);
    assert_eq!(1, stats.compile_requests);
    assert_eq!(1, stats.requests_executed);
    assert_eq!(0, stats.cache_hits.all());
    assert_eq!(1, stats.cache_misses.all());
}
