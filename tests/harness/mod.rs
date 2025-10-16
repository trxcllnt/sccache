use fs_err as fs;
use once_cell::sync::Lazy;
use regex::Regex;
use std::{
    env,
    ffi::OsString,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

use serde::Serialize;
use which::{which, which_in};

use sccache::{errors::*, mock_command::ProcessOutput};

pub mod client;

#[cfg(feature = "dist-server")]
pub mod dist;

pub const TC_CACHE_SIZE: u64 = 2 * 1024 * 1024 * 1024; // 2GiB

pub fn check_output<O: Into<ProcessOutput>>(output: O) -> Result<()> {
    let output = output.into();
    if !output.success() {
        eprintln!(
            "{}\n\n[BEGIN STDOUT]\n===========\n{}\n===========\n[FIN STDOUT]\n\n[BEGIN STDERR]\n===========\n{}\n===========\n[FIN STDERR]\n\n",
            output.desc(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Err(anyhow!("{}", output.desc()))
    } else {
        Ok(())
    }
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

#[derive(Clone)]
pub struct Compiler {
    pub name: &'static str,
    pub exe: OsString,
    #[allow(dead_code)]
    pub env_vars: Vec<(OsString, OsString)>,
    #[allow(dead_code)]
    pub version: OsString,
}

#[allow(dead_code)]
static COMPILER_VERSION_RE: Lazy<Regex> =
    regex_static::lazy_regex!(r"^.*?(?P<major>\d+).*?(?P<minor>\d+).*?(?P<patch>\d+).*?$");

impl Compiler {
    #[allow(dead_code)]
    pub fn version(&self) -> sccache::errors::Result<String> {
        use sccache::compiler_version;
        use sccache::errors::*;

        let exe = Path::new(&self.exe);
        let ver = compiler_version(exe)?;

        let (major, minor, patch) = match COMPILER_VERSION_RE.captures(&ver) {
            Some(ver) => (
                ver.name("major")
                    .and_then(|s| s.as_str().parse::<usize>().ok())
                    .unwrap_or(0),
                ver.name("minor")
                    .and_then(|s| s.as_str().parse::<usize>().ok())
                    .unwrap_or(0),
                ver.name("patch")
                    .and_then(|s| s.as_str().parse::<usize>().ok())
                    .unwrap_or(0),
            ),
            None => bail!("Could not determine compiler version (exe={exe:?})"),
        };

        Ok(format!("{major}.{minor}.{patch}"))
    }
}

// Test GCC + clang on non-OS X platforms.
#[cfg(all(unix, not(target_os = "macos")))]
pub const COMPILERS: &[&str] = &["gcc", "clang", "clang++", "nvc", "nvc++"];

// OS X ships a `gcc` that's just a clang wrapper, so only test clang there.
#[cfg(target_os = "macos")]
pub const COMPILERS: &[&str] = &["clang", "clang++"];

#[cfg(all(unix, not(target_os = "windows")))]
pub const CUDA_COMPILERS: &[&str] = &["nvcc", "clang++"];

#[cfg(target_os = "windows")]
pub const CUDA_COMPILERS: &[&str] = &["nvcc"];

#[cfg(unix)]
pub fn find_compilers() -> Vec<Compiler> {
    let cwd = env::current_dir().unwrap();
    COMPILERS
        .iter()
        .filter_map(|c| {
            which_in(c, env::var_os("PATH"), &cwd)
                .ok()
                .map(|full_path| Compiler {
                    name: c,
                    exe: full_path.as_path().into(),
                    env_vars: vec![],
                    version: full_path.as_path().into(),
                })
        })
        .collect::<Vec<_>>()
}

#[cfg(target_env = "msvc")]
pub fn find_compilers() -> Vec<Compiler> {
    let tool = cc::Build::new()
        .opt_level(1)
        .host("x86_64-pc-windows-msvc")
        .target("x86_64-pc-windows-msvc")
        .debug(false)
        .get_compiler();
    vec![Compiler {
        name: "cl",
        exe: tool.path().into(),
        env_vars: tool.env().to_vec(),
        version: tool.path().into(),
    }]
}

pub fn find_cuda_compilers() -> Vec<Compiler> {
    let cwd = env::current_dir().unwrap();

    let candidates = match env::var_os("NOTEST_CUDA_COMPILERS") {
        Some(nc) => {
            let ncs = nc.into_string().unwrap();
            let not_candidates = ncs.split(':').collect::<Vec<_>>();
            CUDA_COMPILERS
                .iter()
                .filter(|&c| !not_candidates.contains(c))
                .collect::<Vec<_>>()
        }
        None => CUDA_COMPILERS.iter().collect::<Vec<_>>(),
    };

    // CUDA compilers like clang don't come with all of the components for compilation.
    // To consider a machine to have any cuda compilers we rely on the existence of `nvcc`

    match which("nvcc") {
        Ok(_) => candidates
            .iter()
            .filter_map(|c| {
                which_in(c, env::var_os("PATH"), &cwd)
                    .ok()
                    .map(|full_path| Compiler {
                        name: c,
                        exe: full_path.as_path().into(),
                        env_vars: vec![],
                        version: full_path.as_path().into(),
                    })
            })
            .collect::<Vec<_>>(),
        Err(_) => {
            eprintln!(
                "unable to find `nvcc` in PATH={:?}",
                env::var_os("PATH").unwrap_or_default()
            );
            vec![]
        }
    }
}
