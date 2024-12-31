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
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fmt;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
#[cfg(feature = "dist-server")]
use std::pin::Pin;
use std::process;
use std::time::{Duration, Instant};

use crate::errors::*;

#[cfg(any(feature = "dist-client", feature = "dist-server"))]
mod cache;
#[cfg(feature = "dist-client")]
pub mod client_auth;
#[cfg(any(feature = "dist-client", feature = "dist-server"))]
pub mod http;
#[cfg(feature = "dist-server")]
pub mod server;
#[cfg(test)]
mod test;

#[cfg(any(feature = "dist-client", feature = "dist-server"))]
pub use crate::dist::cache::TcCache;

#[cfg(feature = "dist-server")]
pub use crate::dist::cache::ServerToolchains;

// TODO: paths (particularly outputs, which are accessed by an unsandboxed program)
// should be some pre-sanitised AbsPath type

pub use self::path_transform::PathTransformer;

#[cfg(feature = "dist-client")]
pub mod pkg;
#[cfg(not(feature = "dist-client"))]
mod pkg {
    pub trait ToolchainPackager {}
    pub trait InputsPackager {}
}

#[cfg(target_os = "windows")]
mod path_transform {
    use std::collections::HashMap;
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
                    dist_suffix.push('/')
                }
                dist_suffix.push_str(part)
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
    use std::iter;
    use std::path::{Path, PathBuf};

    #[derive(Debug, Clone)]
    pub struct PathTransformer;

    impl PathTransformer {
        pub fn new() -> Self {
            PathTransformer
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

// process::Output is not serialize so we have a custom Output type. However,
// we cannot encode all information in here, such as Unix signals, as the other
// end may not understand them (e.g. if it's Windows)
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessOutput {
    code: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}
impl ProcessOutput {
    #[cfg(unix)]
    pub fn try_from(o: process::Output) -> Result<Self> {
        let process::Output {
            status,
            stdout,
            stderr,
        } = o;
        let code = match (status.code(), status.signal()) {
            (Some(c), _) => c,
            (None, Some(s)) => bail!("Process status {} terminated with signal {}", status, s),
            (None, None) => bail!("Process status {} has no exit code or signal", status),
        };
        Ok(ProcessOutput {
            code,
            stdout,
            stderr,
        })
    }
    #[cfg(test)]
    pub fn fake_output(code: i32, stdout: Vec<u8>, stderr: Vec<u8>) -> Self {
        Self {
            code,
            stdout,
            stderr,
        }
    }
}

impl fmt::Debug for ProcessOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ code: {}, stdout: {:?}, stderr: {:?} }}",
            self.code,
            String::from_utf8_lossy(&self.stdout),
            String::from_utf8_lossy(&self.stderr)
        )
    }
}

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
#[cfg(windows)]
use std::os::windows::process::ExitStatusExt;
#[cfg(unix)]
fn exit_status(code: i32) -> process::ExitStatus {
    process::ExitStatus::from_raw(code)
}
#[cfg(windows)]
fn exit_status(code: i32) -> process::ExitStatus {
    // TODO: this is probably a subideal conversion - it's not clear how Unix exit codes map to
    // Windows exit codes (other than 0 being a success)
    process::ExitStatus::from_raw(code as u32)
}
impl From<ProcessOutput> for process::Output {
    fn from(o: ProcessOutput) -> Self {
        // TODO: handle signals, i.e. None code
        process::Output {
            status: exit_status(o.code),
            stdout: o.stdout,
            stderr: o.stderr,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputData(Vec<u8>, u64);
impl OutputData {
    #[cfg(any(feature = "dist-server", all(feature = "dist-client", test)))]
    pub fn try_from_reader<R: Read>(r: R) -> io::Result<Self> {
        use flate2::read::ZlibEncoder as ZlibReadEncoder;
        use flate2::Compression;
        let mut compressor = ZlibReadEncoder::new(r, Compression::fast());
        let mut res = vec![];
        io::copy(&mut compressor, &mut res)?;
        Ok(OutputData(res, compressor.total_in()))
    }
    pub fn lens(&self) -> OutputDataLens {
        OutputDataLens {
            actual: self.1,
            compressed: self.0.len() as u64,
        }
    }
    #[cfg(feature = "dist-client")]
    pub fn into_reader(self) -> impl Read {
        use flate2::read::ZlibDecoder as ZlibReadDecoder;
        ZlibReadDecoder::new(io::Cursor::new(self.0))
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

// CompileCommand

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompileCommand {
    pub executable: String,
    pub arguments: Vec<String>,
    pub env_vars: Vec<(String, String)>,
    pub cwd: String,
}

// NewJob

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewJobRequest {
    pub toolchain: Toolchain,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewJobResponse {
    pub has_toolchain: bool,
    pub job_id: String,
    pub timeout: u32,
}

// RunJob

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunJobRequest {
    pub job_id: String,
    pub command: CompileCommand,
    pub inputs: Vec<u8>,
    pub outputs: Vec<String>,
    pub toolchain: Toolchain,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RunJobResponse {
    JobFailed {
        reason: String,
    },
    JobComplete {
        result: BuildResult,
        server_id: String,
    },
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStatusResult {
    pub num_cpus: usize,
    pub num_jobs: usize,
    pub num_servers: usize,
    pub servers: std::collections::HashMap<String, ServerStatusResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerStatusResult {
    pub last_success: u64,
    pub last_failure: u64,
    pub max_per_core_load: f64,
    pub num_cpus: usize,
    pub num_jobs: usize,
}

#[derive(Clone, Debug)]
pub struct BuildServerStatus {
    pub last_success: Instant,
    pub last_failure: Option<Instant>,
    pub max_per_core_load: f64,
    pub num_cpus: usize,
    pub num_jobs: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildServerInfo {
    pub max_per_core_load: f64,
    pub num_cpus: usize,
    pub num_jobs: usize,
    pub server_id: String,
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
    async fn get_status(&self) -> Result<SchedulerStatusResult>;

    async fn has_toolchain(&self, toolchain: Toolchain) -> bool;

    async fn put_toolchain(
        &self,
        toolchain: Toolchain,
        toolchain_reader: Pin<&mut (dyn futures::AsyncRead + Send)>,
    ) -> Result<SubmitToolchainResult>;

    async fn new_job(&self, request: NewJobRequest) -> Result<NewJobResponse>;
    async fn run_job(&self, request: RunJobRequest) -> Result<RunJobResponse>;
    async fn job_failure(&self, job_id: &str, reason: &str, info: BuildServerInfo) -> Result<()>;
    async fn job_success(
        &self,
        job_id: &str,
        result: BuildResult,
        info: BuildServerInfo,
    ) -> Result<()>;

    async fn request_status(&self) -> Result<()>;
    async fn receive_status(&self, info: BuildServerInfo, status: Option<bool>) -> Result<()>;
}

#[cfg(feature = "dist-server")]
#[async_trait]
pub trait ServerService: Send + Sync {
    async fn broadcast_status(&self) -> Result<()>;

    async fn report_status(&self, respond_to: &str) -> Result<()>;

    #[allow(clippy::too_many_arguments)]
    async fn run_job(
        &self,
        task_id: &str,
        job_id: &str,
        respond_to: &str,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs: Vec<u8>,
    ) -> Result<BuildResult>;

    async fn job_failure(&self, task_id: &str, reason: &str) -> Result<()>;

    async fn job_success(&self, task_id: &str, result: &BuildResult) -> Result<()>;
}

#[cfg(feature = "dist-server")]
#[async_trait]
pub trait BuilderIncoming: Send + Sync {
    // From Server
    async fn run_build(
        &self,
        job_id: &str,
        toolchain_dir: &Path,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs: Vec<u8>,
    ) -> std::result::Result<BuildResult, Error>;
}

/////////
#[async_trait]
pub trait Client: Send + Sync {
    // To Scheduler
    async fn new_job(&self, toolchain: Toolchain) -> Result<NewJobResponse>;
    // To Scheduler
    async fn run_job(
        &self,
        job_id: &str,
        timeout: Duration,
        toolchain: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs_packager: Box<dyn pkg::InputsPackager>,
    ) -> Result<(RunJobResponse, PathTransformer)>;
    // To Scheduler
    async fn do_get_status(&self) -> Result<SchedulerStatusResult>;
    // To Scheduler
    async fn do_submit_toolchain(&self, tc: Toolchain) -> Result<SubmitToolchainResult>;
    async fn put_toolchain(
        &self,
        compiler_path: PathBuf,
        weak_key: String,
        toolchain_packager: Box<dyn pkg::ToolchainPackager>,
    ) -> Result<(Toolchain, Option<(String, PathBuf)>)>;
    fn rewrite_includes_only(&self) -> bool;
    fn get_custom_toolchain(&self, exe: &Path) -> Option<PathBuf>;
}
