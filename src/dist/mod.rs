// Copyright 2016 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;

#[cfg(feature = "dist-server")]
use itertools::Itertools;

use serde::{Deserialize, Serialize};

use std::{
    collections::HashMap,
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::errors::*;
use crate::mock_command::ProcessOutput;

#[cfg(any(feature = "dist-client", feature = "dist-server"))]
mod cache;
#[cfg(feature = "dist-client")]
pub mod client_auth;
#[cfg(any(feature = "dist-client", feature = "dist-server"))]
pub mod http;
#[cfg(feature = "dist-server")]
pub mod metrics;
#[cfg(feature = "dist-server")]
pub mod scheduler;
#[cfg(feature = "dist-server")]
pub mod server;
#[cfg(feature = "dist-server")]
pub mod tasks;
#[cfg(feature = "dist-server")]
pub mod token_check;

#[cfg(feature = "dist-server")]
pub use crate::{cache::BufReadSeek, dist::cache::ServerToolchains};

#[cfg(feature = "dist-server")]
pub(crate) fn job_inputs_key(job_id: &str) -> String {
    format!("{job_id}-inputs")
}

#[cfg(feature = "dist-server")]
pub fn job_result_key(job_id: &str) -> String {
    format!("{job_id}-result")
}

#[cfg(feature = "dist-server")]
pub fn scheduler_to_servers_queue() -> String {
    queue_name_with_env_info("scheduler-to-servers")
}

#[cfg(feature = "dist-server")]
pub fn server_to_schedulers_queue() -> String {
    queue_name_with_env_info("server-to-schedulers")
}

#[cfg(feature = "dist-server")]
pub fn to_scheduler_queue(id: &str) -> String {
    queue_name_with_env_info(&format!("scheduler-{id}"))
}

#[cfg(feature = "dist-server")]
pub fn to_server_queue(id: &str) -> String {
    queue_name_with_env_info(&format!("server-{id}"))
}

#[cfg(feature = "dist-server")]
pub fn queue_name_with_env_info(prefix: &str) -> String {
    format!("{prefix}-{}", env_info())
}

#[cfg(feature = "dist-server")]
pub fn env_info() -> String {
    // "{os}-{arch}-{deployment}"
    // "linux-amd64-redis_zone_a" etc.
    [
        std::env::var("SCCACHE_DIST_OS").unwrap_or(std::env::consts::OS.to_owned()),
        std::env::var("SCCACHE_DIST_ARCH").unwrap_or(std::env::consts::ARCH.to_owned()),
        std::env::var("SCCACHE_DIST_DEPLOYMENT_NAME").unwrap_or_default(),
    ]
    .iter()
    .filter(|s| !s.is_empty())
    .join("-")
}

#[cfg(feature = "dist-server")]
pub fn init_logging() {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    if std::env::var(crate::LOGGING_ENV).is_ok() {
        match tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_env(crate::LOGGING_ENV).unwrap_or_else(
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
            Err(e) => panic!("Failed to initialize logging: {e:?}"),
        }
    }
}

// TODO: paths (particularly outputs, which are accessed by an unsandboxed program)
// should be some pre-sanitised AbsPath type

pub use self::path_transform::PathTransformer;

#[cfg(feature = "dist-client")]
pub mod pkg;
#[cfg(not(feature = "dist-client"))]
mod pkg {
    pub trait ToolchainPackager {}
    pub trait PackagedToolchain {}
    #[allow(dead_code)]
    pub trait InputsPackager {}
}

#[cfg(target_os = "windows")]
mod path_transform {
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::path::{Component, Components, Path, PathBuf, Prefix, PrefixComponent};
    use std::str;

    fn take_prefix<'a>(components: &'a mut Components<'_>) -> Option<PrefixComponent<'a>> {
        let prefix = components.next()?;
        let pc = match prefix {
            Component::Prefix(pc) => pc,
            _ => return None,
        };
        let root = components.next()?;
        if root != Component::RootDir {
            return None;
        }
        Some(pc)
    }

    fn transform_prefix_component(pc: PrefixComponent<'_>) -> Option<String> {
        match pc.kind() {
            // Transforming these to the same place means these may flip-flop
            // in the tracking map, but they're equivalent so not really an
            // issue
            Prefix::Disk(diskchar) | Prefix::VerbatimDisk(diskchar) => {
                assert!(diskchar.is_ascii_alphabetic());
                let diskchar = diskchar.to_ascii_uppercase();
                Some(format!(
                    "/prefix/disk-{}",
                    str::from_utf8(&[diskchar]).expect("invalid disk char")
                ))
            }
            Prefix::Verbatim(_)
            | Prefix::VerbatimUNC(_, _)
            | Prefix::DeviceNS(_)
            | Prefix::UNC(_, _) => None,
        }
    }

    #[derive(Debug, Clone)]
    pub struct PathTransformer {
        dist_to_local_path: HashMap<String, PathBuf>,
    }

    impl PathTransformer {
        pub fn new() -> Self {
            PathTransformer {
                dist_to_local_path: HashMap::new(),
            }
        }
        pub fn as_dist_input_path(&mut self, input_path: &Path) -> Option<String> {
            self.as_dist(
                &(if let Some(ext) = input_path.extension() {
                    input_path.with_extension([OsStr::new("dist"), ext].join(OsStr::new(".")))
                } else {
                    input_path.with_extension("dist")
                }),
            )
        }
        pub fn as_dist_abs(&mut self, p: &Path) -> Option<String> {
            if !p.is_absolute() {
                return None;
            }
            self.as_dist(p)
        }
        pub fn as_dist(&mut self, p: &Path) -> Option<String> {
            let mut components = p.components();

            // Extract the prefix (e.g. "C:/") if present
            let maybe_dist_prefix = if p.is_absolute() {
                let pc =
                    take_prefix(&mut components).expect("could not take prefix from absolute path");
                Some(transform_prefix_component(pc)?)
            } else {
                None
            };

            // Reconstruct the path (minus the prefix) as a Linux path
            let mut dist_suffix = String::new();
            for component in components {
                let part = match component {
                    Component::Prefix(_) | Component::RootDir => {
                        // On Windows there is such a thing as a path like C:file.txt
                        // It's not clear to me what the semantics of such a path are,
                        // so give up.
                        error!("unexpected part in path {:?}", p);
                        return None;
                    }
                    Component::Normal(osstr) => osstr.to_str()?,
                    // TODO: should be forbidden
                    Component::CurDir => ".",
                    Component::ParentDir => "..",
                };
                if !dist_suffix.is_empty() {
                    dist_suffix.push('/');
                }
                dist_suffix.push_str(part);
            }

            let dist_path = if let Some(mut dist_prefix) = maybe_dist_prefix {
                dist_prefix.push('/');
                dist_prefix.push_str(&dist_suffix);
                dist_prefix
            } else {
                dist_suffix
            };
            self.dist_to_local_path
                .insert(dist_path.clone(), p.to_owned());
            Some(dist_path)
        }
        pub fn disk_mappings(&self) -> impl Iterator<Item = (PathBuf, String)> {
            let mut normal_mappings = HashMap::new();
            let mut verbatim_mappings = HashMap::new();
            for (_dist_path, local_path) in self.dist_to_local_path.iter() {
                if !local_path.is_absolute() {
                    continue;
                }
                let mut components = local_path.components();
                let local_prefix =
                    take_prefix(&mut components).expect("could not take prefix from absolute path");
                let local_prefix_component = Component::Prefix(local_prefix);
                let local_prefix_path: &Path = local_prefix_component.as_ref();
                let mappings = if let Prefix::VerbatimDisk(_) = local_prefix.kind() {
                    &mut verbatim_mappings
                } else {
                    &mut normal_mappings
                };
                if mappings.contains_key(local_prefix_path) {
                    continue;
                }
                let dist_prefix = transform_prefix_component(local_prefix)
                    .expect("prefix already in tracking map could not be transformed");
                mappings.insert(local_prefix_path.to_owned(), dist_prefix);
            }
            // Prioritise normal mappings for the same disk, as verbatim mappings can
            // look odd to users
            normal_mappings.into_iter().chain(verbatim_mappings)
        }
        pub fn to_local(&self, p: &str) -> Option<PathBuf> {
            self.dist_to_local_path.get(p).cloned()
        }
    }

    #[test]
    fn test_basic() {
        let mut pt = PathTransformer::new();
        assert_eq!(pt.as_dist(Path::new("C:/a")).unwrap(), "/prefix/disk-C/a");
        assert_eq!(
            pt.as_dist(Path::new(r#"C:\a\b.c"#)).unwrap(),
            "/prefix/disk-C/a/b.c"
        );
        assert_eq!(
            pt.as_dist(Path::new("X:/other.c")).unwrap(),
            "/prefix/disk-X/other.c"
        );
        let mut disk_mappings: Vec<_> = pt.disk_mappings().collect();
        disk_mappings.sort();
        assert_eq!(
            disk_mappings,
            &[
                (Path::new("C:").into(), "/prefix/disk-C".into()),
                (Path::new("X:").into(), "/prefix/disk-X".into()),
            ]
        );
        assert_eq!(pt.to_local("/prefix/disk-C/a").unwrap(), Path::new("C:/a"));
        assert_eq!(
            pt.to_local("/prefix/disk-C/a/b.c").unwrap(),
            Path::new("C:/a/b.c")
        );
        assert_eq!(
            pt.to_local("/prefix/disk-X/other.c").unwrap(),
            Path::new("X:/other.c")
        );
    }

    #[test]
    fn test_relative_paths() {
        let mut pt = PathTransformer::new();
        assert_eq!(pt.as_dist(Path::new("a/b")).unwrap(), "a/b");
        assert_eq!(pt.as_dist(Path::new(r#"a\b"#)).unwrap(), "a/b");
        assert_eq!(pt.to_local("a/b").unwrap(), Path::new("a/b"));
    }

    #[test]
    fn test_verbatim_disks() {
        let mut pt = PathTransformer::new();
        assert_eq!(
            pt.as_dist(Path::new("X:/other.c")).unwrap(),
            "/prefix/disk-X/other.c"
        );
        pt.as_dist(Path::new(r#"\\?\X:\out\other.o"#));
        assert_eq!(
            pt.to_local("/prefix/disk-X/other.c").unwrap(),
            Path::new("X:/other.c")
        );
        assert_eq!(
            pt.to_local("/prefix/disk-X/out/other.o").unwrap(),
            Path::new(r#"\\?\X:\out\other.o"#)
        );
        let disk_mappings: Vec<_> = pt.disk_mappings().collect();
        // Verbatim disks should come last
        assert_eq!(
            disk_mappings,
            &[
                (Path::new("X:").into(), "/prefix/disk-X".into()),
                (Path::new(r#"\\?\X:"#).into(), "/prefix/disk-X".into()),
            ]
        );
    }

    #[test]
    fn test_slash_directions() {
        let mut pt = PathTransformer::new();
        assert_eq!(pt.as_dist(Path::new("C:/a")).unwrap(), "/prefix/disk-C/a");
        assert_eq!(pt.as_dist(Path::new("C:\\a")).unwrap(), "/prefix/disk-C/a");
        assert_eq!(pt.to_local("/prefix/disk-C/a").unwrap(), Path::new("C:/a"));
        assert_eq!(pt.disk_mappings().count(), 1);
    }
}

#[cfg(unix)]
mod path_transform {
    use std::ffi::OsStr;
    use std::iter;
    use std::path::{Path, PathBuf};

    #[derive(Debug, Clone)]
    pub struct PathTransformer;

    impl PathTransformer {
        pub fn new() -> Self {
            PathTransformer
        }
        ///
        /// Remove the extension so preprocessed file doesn't have the same name as
        /// the source file. This works around a GCC bug when compiling preprocessed
        /// input -- namely, if the preprocessed file has the same name as the source
        /// file, error source lines aren't mapped correctly:
        ///
        /// ```shell
        /// # Preprocess some file with an error:
        /// $ gcc -x c++ -E tests/test.c > tests/test.ii
        ///
        /// # Compile the preprocessed file (error source is correct):
        /// $ gcc -x c++-cpp-output -c tests/test.ii -o tests/test.c.o
        /// > tests/test.c: In function ‘void foo()’:
        /// > tests/test.c:5:19: error: invalid conversion from ‘const char*’ to ‘int’ [-fpermissive]
        /// >     5 |   const int foo = "5";
        /// >       |                   ^~~
        /// >       |                   |
        /// >       |                   const char*
        ///
        ///
        /// # Rename `test.ii` -> `test.c`
        /// $ mv tests/test.{ii,c}
        ///
        /// # Compile the renamed preprocessed file (error source is wrong):
        /// $ gcc -x c++-cpp-output -c tests/test.c -o tests/test.c.o
        /// > tests/test.c: In function ‘void foo()’:
        /// > tests/test.c:5:19: error: invalid conversion from ‘const char*’ to ‘int’ [-fpermissive]
        /// >     5 | # 0 "<command-line>" 2
        /// >       |                   ^~~
        /// >       |                   |
        /// >       |                   const char*
        /// ```
        ///
        pub fn as_dist_input_path(&mut self, input_path: &Path) -> Option<String> {
            self.as_dist(
                &(if let Some(ext) = input_path.extension() {
                    input_path.with_extension([OsStr::new("dist"), ext].join(OsStr::new(".")))
                } else {
                    input_path.with_extension("dist")
                }),
            )
        }
        pub fn as_dist_abs(&mut self, p: &Path) -> Option<String> {
            if !p.is_absolute() {
                return None;
            }
            self.as_dist(p)
        }
        pub fn as_dist(&mut self, p: &Path) -> Option<String> {
            p.as_os_str().to_str().map(Into::into)
        }
        pub fn disk_mappings(&self) -> impl Iterator<Item = (PathBuf, String)> {
            iter::empty()
        }
        pub fn to_local(&self, p: &str) -> Option<PathBuf> {
            Some(PathBuf::from(p))
        }
    }
}

pub fn osstrings_to_strings(osstrings: &[OsString]) -> Option<Vec<String>> {
    osstrings
        .iter()
        .map(|arg| arg.clone().into_string().ok())
        .collect::<Option<_>>()
}

pub fn osstring_tuples_to_strings(
    osstring_tuples: &[(OsString, OsString)],
) -> Option<Vec<(String, String)>> {
    osstring_tuples
        .iter()
        .map(|(k, v)| Some((k.clone().into_string().ok()?, v.clone().into_string().ok()?)))
        .collect::<Option<_>>()
}

pub fn strings_to_osstrings(strings: &[String]) -> Vec<OsString> {
    strings
        .iter()
        .map(|arg| std::ffi::OsStr::new(arg).to_os_string())
        .collect::<Vec<_>>()
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputData(Vec<u8>, u64);
impl OutputData {
    #[cfg(any(feature = "dist-server", all(feature = "dist-client", test)))]
    pub fn try_from_reader<R: std::io::Read>(reader: R) -> std::io::Result<Self> {
        let mut compressor = flate2::read::ZlibEncoder::new(
            reader,
            // Optimize for size since bandwidth costs more than CPU cycles
            flate2::Compression::best(),
        );
        let mut res = vec![];
        std::io::copy(&mut compressor, &mut res)?;
        Ok(OutputData(res, compressor.total_in()))
    }
    pub fn lens(&self) -> OutputDataLens {
        OutputDataLens {
            actual: self.1,
            compressed: self.0.len() as u64,
        }
    }
    #[cfg(feature = "dist-client")]
    pub fn into_reader(self) -> impl std::io::Read {
        flate2::read::ZlibDecoder::new(std::io::Cursor::new(self.0))
    }
}

impl fmt::Debug for OutputData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Size: {}->{}", self.1, self.0.len())
    }
}

pub struct OutputDataLens {
    pub actual: u64,
    pub compressed: u64,
}

impl fmt::Display for OutputDataLens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Size: {}->{}", self.actual, self.compressed)
    }
}

// BuildResult

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildResult {
    pub output: ProcessOutput,
    pub outputs: Vec<(String, OutputData)>,
}

#[cfg(feature = "dist-server")]
pub enum BuildError {
    PrepareOverlay(Error),
    EnterNewNS(nix::errno::Errno),
    MountNsRoot(nix::errno::Errno),
    MakeOverlayDir(PathBuf, std::io::Error),
    MountOverlayFS(Error),
    UnpackInputs(Error),
    MakeOutputDir(PathBuf, std::io::Error),
    SpawnChildProcess(std::io::Error),
    ReadChildOutput(std::io::Error),
    WaitChildOutput(std::io::Error),
    KillChildProcess(std::io::Error),
    ReadBuildResult(PathBuf, std::io::Error),
    Cancelled,
    Unknown(Error),
}

#[cfg(feature = "dist-server")]
impl fmt::Debug for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrepareOverlay(e) => write!(f, "Failed to prepare overlay dirs: {e:?}"),
            Self::EnterNewNS(e) => write!(f, "Failed to enter a new Linux namespace: {e:?}"),
            Self::MountNsRoot(e) => write!(f, "Failed to mount Linux namespace root: {e:?}"),
            Self::MakeOverlayDir(p, e) => write!(f, "Failed to create overlay dir {p:?}: {e:?}"),
            Self::MountOverlayFS(e) => write!(f, "Failed to mount overlay FS: {e:?}"),
            Self::UnpackInputs(e) => write!(f, "Failed to unpack inputs to overlay: {e:?}"),
            Self::MakeOutputDir(p, e) => write!(f, "Failed to create output dir {p:?}: {e:?}"),
            Self::SpawnChildProcess(e) => write!(f, "Failed to spawn build process: {e:?}"),
            Self::ReadChildOutput(e) => write!(f, "Failed to read process output: {e:?}"),
            Self::WaitChildOutput(e) => write!(f, "Failed to wait for build process: {e:?}"),
            Self::KillChildProcess(e) => write!(f, "Failed to kill build process: {e:?}"),
            Self::ReadBuildResult(p, e) => write!(f, "Failed to read build result {p:?}: {e:?}"),
            Self::Cancelled => write!(f, "Build cancelled"),
            Self::Unknown(e) => write!(f, "Build error: {e:?}"),
        }
    }
}

#[cfg(feature = "dist-server")]
impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrepareOverlay(e) => write!(f, "Failed to prepare overlay dirs: {e}"),
            Self::EnterNewNS(e) => write!(f, "Failed to enter a new Linux namespace: {e}"),
            Self::MountNsRoot(e) => write!(f, "Failed to mount Linux namespace root: {e}"),
            Self::MakeOverlayDir(p, e) => write!(f, "Failed to create overlay dir {p:?}: {e}"),
            Self::MountOverlayFS(e) => write!(f, "Failed to mount overlay FS: {e}"),
            Self::UnpackInputs(e) => write!(f, "Failed to unpack inputs to overlay: {e}"),
            Self::MakeOutputDir(p, e) => write!(f, "Failed to create output dir {p:?}: {e}"),
            Self::SpawnChildProcess(e) => write!(f, "Failed to spawn build process: {e}"),
            Self::ReadChildOutput(e) => write!(f, "Failed to read process output: {e}"),
            Self::WaitChildOutput(e) => write!(f, "Failed to wait for build process: {e}"),
            Self::KillChildProcess(e) => write!(f, "Failed to kill build process: {e}"),
            Self::ReadBuildResult(p, e) => write!(f, "Failed to read build result {p:?}: {e}"),
            Self::Cancelled => write!(f, "Build cancelled"),
            Self::Unknown(e) => write!(f, "Build error: {e:?}"),
        }
    }
}

#[cfg(feature = "dist-server")]
impl From<BuildError> for anyhow::Error {
    fn from(err: BuildError) -> Self {
        anyhow!(err)
    }
}

// CompileCommand

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompileCommand {
    pub executable: String,
    pub arguments: Vec<String>,
    pub env_vars: Vec<(String, String)>,
    pub cwd: String,
}

impl fmt::Display for CompileCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, r#"cd "{}"; "#, self.cwd)?;
        if cfg!(target_os = "windows") {
            write!(f, "& ")?;
        }
        write!(f, r#""{}""#, self.executable)?;
        for arg in self.arguments.iter() {
            write!(f, r#" "{arg}""#)?;
        }
        fmt::Result::Ok(())
    }
}

// NewJob

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewJobResponse {
    pub has_inputs: bool,
    pub has_toolchain: bool,
    pub job_id: String,
    pub timeout: u32,
}

// RunJob

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunJobRequest {
    pub command: CompileCommand,
    pub outputs: Vec<String>,
    pub toolchain: Toolchain,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunJobRequestV2 {
    pub command: CompileCommand,
    pub outputs: Vec<String>,
    pub toolchain: Toolchain,
    pub labels: Option<HashMap<String, String>>,
}

#[derive(Debug)]
pub enum RunJobError {
    Fatal(Error),
    MissingJobInputs,
    MissingJobResult,
    MissingToolchain,
    Retryable(Error),
}

impl fmt::Display for RunJobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fatal(e) => write!(f, "Fatal error: {e:?}"),
            Self::MissingJobInputs => write!(f, "Missing job inputs"),
            Self::MissingJobResult => write!(f, "Missing job result"),
            Self::MissingToolchain => write!(f, "Missing toolchain"),
            Self::Retryable(e) => write!(f, "Retryable error: {e:?}"),
        }
    }
}

impl From<anyhow::Error> for RunJobError {
    fn from(err: anyhow::Error) -> Self {
        RunJobError::Retryable(err)
    }
}

impl RunJobError {
    pub fn server_terminated() -> Self {
        anyhow!("Server terminated").into()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RunJobResponse {
    Complete {
        result: BuildResult,
        server_id: String,
    },
    FatalError {
        message: String,
        server_id: String,
    },
    MissingJobInputs {
        server_id: String,
    },
    MissingJobResult {
        server_id: String,
    },
    MissingToolchain {
        server_id: String,
    },
    RetryableError {
        message: String,
        server_id: String,
    },
}

impl RunJobResponse {
    pub fn build_process_killed(server_id: &str) -> Self {
        RunJobResponse::RetryableError {
            message: "Build process killed".into(),
            server_id: server_id.to_owned(),
        }
    }
    pub fn server_terminated(server_id: &str) -> Self {
        RunJobResponse::RetryableError {
            message: "Server terminated".into(),
            server_id: server_id.to_owned(),
        }
    }
}

// Toolchain

// TODO: Clone by assuming immutable/no GC for now
// TODO: make fields non-public?
// TODO: make archive_id validate that it's just a bunch of hex chars
#[derive(Debug, Hash, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Toolchain {
    pub archive_id: String,
}

// Status

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStatus {
    pub info: SysStats,
    pub jobs: JobStats,
    pub servers: Vec<ServerStatus>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerStatus {
    // Server id
    pub id: String,
    // Server performance stats
    pub info: SysStats,
    // Server job stats
    pub jobs: JobStats,
    // Duration since last update
    pub u_time: u64,
    // The age of the oldest active job (in seconds)
    pub max_job_age: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SysStats {
    pub cpu_usage: f32,
    pub mem_avail: u64,
    pub mem_total: u64,
    pub num_cpus: usize,
    pub occupancy: usize,
    pub pre_fetch: usize,
}

impl Default for SysStats {
    fn default() -> Self {
        Self {
            cpu_usage: 0.0,
            mem_avail: 0,
            mem_total: 0,
            num_cpus: 0,
            occupancy: 0,
            pre_fetch: 0,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobStats {
    pub accepted: u64,
    pub finished: u64,
    pub loading: u64,
    pub pending: u64,
    pub running: u64,
}

// A struct describing the status of a scheduler or server
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusUpdate {
    // The id of the scheduler or server
    pub id: String,
    // The name of the queue for this scheduler or server
    pub queue: String,
    // The sysinfo stats of the scheduler or server
    pub info: SysStats,
    pub jobs: JobStats,
    // When this event was created as microseconds since the Unix epoch as (secs, nanos)
    pub timestamp: (u64, u32),
    // The age of the oldest active job in microseconds as (secs, nanos)
    pub max_job_age: (u64, u32),
}

// SubmitToolchain

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SubmitToolchainResult {
    Success,
    Error { message: String },
}

///////////////////

///////////////////

#[cfg(feature = "dist-server")]
#[async_trait]
pub trait SchedulerService: Send + Sync {
    async fn get_status(&self) -> Result<SchedulerStatus>;

    async fn has_toolchain(&self, toolchain: &Toolchain) -> bool;

    async fn put_toolchain(
        &self,
        toolchain: &Toolchain,
        toolchain_archive: opendal::Buffer,
    ) -> Result<SubmitToolchainResult>;

    async fn del_toolchain(&self, toolchain: &Toolchain) -> Result<()>;

    async fn has_job(&self, job_id: &str) -> bool;
    async fn new_job(
        &self,
        toolchain: Toolchain,
        inputs: opendal::Buffer,
    ) -> Result<NewJobResponse>;
    async fn run_job(&self, job_id: &str, request: RunJobRequestV2) -> Result<RunJobResponse>;
    async fn put_job(&self, job_id: &str, inputs: opendal::Buffer) -> Result<()>;
    async fn del_job(&self, job_id: &str) -> Result<()>;

    async fn job_finished(&self, job_id: &str, server: StatusUpdate) -> Result<()>;

    async fn update_server_status(
        &self,
        status: StatusUpdate,
        job_status: Option<bool>,
    ) -> Result<()>;
}

#[cfg(feature = "dist-server")]
#[async_trait]
pub trait ServerService: Send + Sync {
    async fn run_job(
        &self,
        job_id: &str,
        reply_to: &str,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
        labels: HashMap<String, String>,
    ) -> Result<RunJobResponse>;

    async fn on_failure(&self, job_id: &str, reply_to: &str, err: RunJobError) -> Result<()>;
    async fn on_success(&self, job_id: &str, reply_to: &str, res: &RunJobResponse) -> Result<()>;

    async fn update_scheduler_status(&self, status: StatusUpdate) -> Result<()>;
}

#[cfg(feature = "dist-server")]
#[async_trait]
pub trait ToolchainService: Send + Sync {
    async fn load_toolchain(&self, tc: &Toolchain) -> Result<PathBuf>;
}

#[cfg(feature = "dist-server")]
#[async_trait]
pub trait BuilderIncoming: Send + Sync {
    // From Server
    async fn run_build(
        &self,
        job_id: &str,
        toolchain_dir: &Path,
        inputs: opendal::Buffer,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> std::result::Result<BuildResult, BuildError>;
    // From Server
    async fn finish_build(&self, job_id: &str);
    // From Server
    async fn shutdown(&self);
}

/////////
#[async_trait]
pub trait Client: Send + Sync {
    // To Scheduler
    async fn new_job(&self, toolchain: Toolchain, inputs: std::fs::File) -> Result<NewJobResponse>;
    // To Scheduler
    async fn put_job(&self, job_id: &str, inputs: std::fs::File) -> Result<()>;
    // To Scheduler
    async fn del_job(&self, job_id: &str) -> Result<()>;
    // To Scheduler
    async fn run_job(
        &self,
        job_id: &str,
        timeout: Duration,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> Result<RunJobResponse>;
    // To Scheduler
    async fn get_status(&self) -> Result<SchedulerStatus>;
    // Write to local toolchain cache if not already written, then upload to Scheduler
    async fn put_toolchain(
        &self,
        toolchain: Toolchain,
        packaged: Option<Arc<dyn pkg::PackagedToolchain>>,
    ) -> Result<SubmitToolchainResult>;
    async fn hash_toolchain(
        &self,
        compiler_path: &Path,
        weak_toolchain_key: &str,
        toolchain_packager: &dyn pkg::ToolchainPackager,
    ) -> Result<(
        Toolchain,
        Option<(String, PathBuf)>,
        Option<Arc<dyn pkg::PackagedToolchain>>,
    )>;
    fn fallback_to_local_compile(&self) -> bool;
    fn max_retries(&self) -> f64;
    fn request_timeout(&self) -> u32;
    fn rewrite_includes_only(&self) -> bool;
    async fn get_custom_toolchain(&self, exe: &Path) -> Option<PathBuf>;
}
