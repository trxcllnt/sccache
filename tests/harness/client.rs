use fs_err as fs;
use std::{
    env,
    ffi::OsString,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str,
    sync::atomic::{AtomicU16, Ordering},
};

use sccache::{
    config::{
        CacheConfigs, DiskCacheConfig, DistConfig, FileConfig, PreprocessorCacheModeConfig,
        try_read_config_file,
    },
    server::{ServerInfo, ServerStats},
};

use super::{TC_CACHE_SIZE, prune_command};

pub fn sccache_client_cfg(data_dir: &Path, preprocessor_cache_mode: bool) -> FileConfig {
    let disk_cache_dir = data_dir.join("cache");
    let toolchains_dir = data_dir.join("toolchains");
    fs::create_dir_all(&disk_cache_dir).unwrap();
    fs::create_dir_all(&toolchains_dir).unwrap();

    let disk_cache = DiskCacheConfig {
        dir: disk_cache_dir,
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
            cache_dir: toolchains_dir,
            toolchains: vec![],
            toolchain_cache_size: TC_CACHE_SIZE,
            rewrite_includes_only: false, // TODO
            ..Default::default()
        },
        server_startup_timeout_ms: None,
    }
}

static CLIENT_PORT: AtomicU16 = AtomicU16::new(4227);

pub struct SccacheClient {
    envvars: Vec<(OsString, OsString)>,
    #[allow(dead_code)]
    pub path: PathBuf,
}

#[allow(unused)]
impl SccacheClient {
    pub fn new_no_cfg() -> Self {
        let path = env!("CARGO_BIN_EXE_sccache").into();
        let port = CLIENT_PORT.fetch_add(1, Ordering::SeqCst);

        let mut envvars = vec![
            ("SCCACHE_SERVER_PORT".into(), port.to_string().into()),
            ("SCCACHE_MAX_THREADS".into(), "2".into()),
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
        let mut cmd = prune_command(Command::new(env!("CARGO_BIN_EXE_sccache")));
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

    pub fn clear_disk_cache(&self) -> sccache::errors::Result<PathBuf> {
        self.zero_stats();
        let disk_cache = (|| {
            if let Some(cfg_path_idx) = self.envvars.iter().position(|(k, _)| k == "SCCACHE_CONF") {
                if let Some((_, cfg_path)) = self.envvars.get(cfg_path_idx) {
                    if let Ok(Some(cfg)) =
                        try_read_config_file::<FileConfig>(&PathBuf::from(cfg_path))
                    {
                        if let Some(disk_cache) = cfg.cache.disk {
                            return disk_cache;
                        }
                    }
                }
            }
            DiskCacheConfig::default()
        })();
        println!("clear_disk_cache: {:?}", disk_cache.dir);
        fs::remove_dir_all(&disk_cache.dir)?;
        Ok(disk_cache.dir.clone())
    }

    pub fn clear_toolchains_cache(&self) -> sccache::errors::Result<PathBuf> {
        let dist_config = (|| {
            if let Some(cfg_path_idx) = self.envvars.iter().position(|(k, _)| k == "SCCACHE_CONF") {
                if let Some((_, cfg_path)) = self.envvars.get(cfg_path_idx) {
                    if let Ok(Some(cfg)) =
                        try_read_config_file::<FileConfig>(&PathBuf::from(cfg_path))
                    {
                        return cfg.dist;
                    }
                }
            }
            DistConfig::default()
        })();
        let tc_dir = dist_config.cache_dir.join("client").join("tc");
        println!("clear_toolchains_cache: {:?}", tc_dir);
        for entry in std::fs::read_dir(&tc_dir)? {
            let path = entry?.path();
            if fs::symlink_metadata(&path)?.is_dir() {
                fs::remove_dir_all(&path)?;
            } else {
                fs::remove_file(&path)?;
            }
        }
        Ok(tc_dir.clone())
    }
}

impl Drop for SccacheClient {
    fn drop(&mut self) {
        self.stop();
    }
}
