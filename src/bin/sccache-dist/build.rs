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

use anyhow::{anyhow, bail, Context, Error, Result};
use async_compression::tokio::bufread::ZlibDecoder as ZlibDecoderAsync;
use async_trait::async_trait;
use bytes::Buf;
use flate2::read::ZlibDecoder as ZlibDecoderSync;
use fs_err as fs;
use itertools::Itertools;
use libmount::Overlay;
use sccache::dist::{BuildResult, BuilderIncoming, CompileCommand, OutputData, ProcessOutput};
use std::io;
use std::path::{self, Path, PathBuf};
use std::process::Output;
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use version_compare::Version;

#[async_trait]
trait AsyncCommandExt {
    async fn check_stdout_trim(&mut self) -> Result<String>;
}

#[async_trait]
impl AsyncCommandExt for tokio::process::Command {
    async fn check_stdout_trim(&mut self) -> Result<String> {
        let output = self.output().await.context("Failed to start command")?;
        check_output(&output)?;
        let stdout =
            String::from_utf8(output.stdout).context("Output from listing containers not UTF8")?;
        Ok(stdout.trim().to_owned())
    }
}

fn check_output(output: &Output) -> Result<()> {
    if !output.status.success() {
        tracing::warn!(
            "===========\n{}\n==========\n\n\n\n=========\n{}\n===============\n\n\n",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        bail!("Command failed with status {}", output.status)
    }
    Ok(())
}

fn list_files(root: &Path) -> Vec<PathBuf> {
    walkdir::WalkDir::new(root)
        .follow_links(false)
        .same_file_system(true)
        .into_iter()
        .flatten()
        // Only mount files and symlinks, not dirs
        .filter_map(|entr| {
            entr.metadata()
                .ok()
                .and_then(|meta| (!meta.is_dir()).then_some(entr))
        })
        .map(|file| file.path().to_path_buf())
        .collect::<Vec<_>>()
}

fn join_suffix<P: AsRef<Path>>(path: &Path, suffix: P) -> PathBuf {
    let suffixpath = suffix.as_ref();
    let mut components = suffixpath.components();
    if suffixpath.has_root() {
        assert_eq!(components.next(), Some(path::Component::RootDir));
    }
    path.join(components)
}

#[derive(Clone, Debug)]
struct OverlaySpec {
    build_dir: PathBuf,
    toolchain_dir: PathBuf,
}

pub struct OverlayBuilder {
    bubblewrap: PathBuf,
    dir: PathBuf,
}

impl OverlayBuilder {
    pub async fn new(bubblewrap: PathBuf, dir: PathBuf) -> Result<Self> {
        tracing::info!("Creating overlay builder");

        if !nix::unistd::getuid().is_root() || !nix::unistd::geteuid().is_root() {
            // Not root, or a setuid binary - haven't put enough thought into supporting this, bail
            bail!("not running as root")
        }

        let mut cmd = tokio::process::Command::new(&bubblewrap);

        let out = cmd
            .arg("--version")
            .check_stdout_trim()
            .await
            .context("Failed to execute bwrap for version check")?;

        if let Some(s) = out.split_whitespace().nth(1) {
            match (Version::from("0.3.0"), Version::from(s)) {
                (Some(min), Some(seen)) => {
                    if seen < min {
                        bail!(
                            "bubblewrap 0.3.0 or later is required, got {:?} for {:?}",
                            out,
                            bubblewrap
                        );
                    }
                }
                (_, _) => {
                    bail!("Unexpected version format running {:?}: got {:?}, expected \"bubblewrap x.x.x\"",
                          bubblewrap, out);
                }
            }
        } else {
            bail!(
                "Unexpected version format running {:?}: got {:?}, expected \"bubblewrap x.x.x\"",
                bubblewrap,
                out
            );
        }

        // TODO: pidfile
        let ret = Self { bubblewrap, dir };
        ret.cleanup().await?;
        fs::create_dir_all(&ret.dir).context("Failed to create base directory for builder")?;
        fs::create_dir_all(ret.dir.join("builds"))
            .context("Failed to create builder builds directory")?;
        fs::create_dir_all(ret.dir.join("toolchains"))
            .context("Failed to create builder toolchains directory")?;
        Ok(ret)
    }

    async fn cleanup(&self) -> Result<()> {
        if self.dir.exists() {
            fs::remove_dir_all(&self.dir).context("Failed to clean up builder directory")?
        }
        Ok(())
    }

    async fn prepare_overlay_dirs(
        &self,
        job_id: &str,
        toolchain_dir: &Path,
    ) -> Result<OverlaySpec> {
        let build_dir = self.dir.join("builds").join(job_id);

        tracing::trace!(
            "[prepare_overlay_dirs({job_id})]: Creating build directory: {build_dir:?}"
        );

        fs::create_dir_all(&build_dir)
            .context("Failed to create build dir")
            .unwrap_or_else(|err| tracing::warn!("[prepare_overlay_dirs({job_id})]: {err:?}"));

        Ok(OverlaySpec {
            build_dir,
            toolchain_dir: toolchain_dir.to_owned(),
        })
    }

    async fn perform_build(
        job_id: &str,
        bubblewrap: PathBuf,
        CompileCommand {
            executable,
            arguments,
            env_vars,
            cwd,
        }: CompileCommand,
        inputs: Vec<u8>,
        output_paths: Vec<String>,
        overlay: OverlaySpec,
    ) -> Result<BuildResult> {
        tracing::trace!("[perform_build({job_id})]: Compile environment: {env_vars:?}");
        tracing::trace!("[perform_build({job_id})]: Compile command: {executable:?} {arguments:?}");
        tracing::trace!("[perform_build({job_id})]: Output paths: {output_paths:?}");

        let job_id = job_id.to_owned();

        tokio::runtime::Handle::current()
            .spawn_blocking(move || {
                // Now mounted filesystems will be automatically unmounted when this thread dies
                // (and tmpfs filesystems will be completely destroyed)
                nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNS)
                    .context("Failed to enter a new Linux namespace")?;
                // Make sure that all future mount changes are private to this namespace
                // TODO: shouldn't need to add these annotations
                let source: Option<&str> = None;
                let fstype: Option<&str> = None;
                let data: Option<&str> = None;
                // Turn / into a 'slave', so it receives mounts from real root, but doesn't propagate back
                nix::mount::mount(
                    source,
                    "/",
                    fstype,
                    nix::mount::MsFlags::MS_REC | nix::mount::MsFlags::MS_PRIVATE,
                    data,
                )
                .context("Failed to turn / into a slave")?;

                let work_dir = overlay.build_dir.join("work");
                let upper_dir = overlay.build_dir.join("upper");
                let target_dir = overlay.build_dir.join("target");
                fs::create_dir_all(&work_dir).context("Failed to create overlay work directory")?;
                fs::create_dir_all(&upper_dir)
                    .context("Failed to create overlay upper directory")?;
                fs::create_dir_all(&target_dir)
                    .context("Failed to create overlay target directory")?;

                let () = Overlay::writable(
                    std::iter::once(overlay.toolchain_dir.as_path()),
                    upper_dir,
                    work_dir,
                    &target_dir,
                    // This error is unfortunately not Send+Sync
                )
                .mount()
                .map_err(|e| anyhow!("Failed to mount overlay FS: {}", e.to_string()))?;

                tracing::trace!("[perform_build({job_id})]: copying in inputs");
                // Note that we don't unpack directly into the upperdir since there overlayfs has some
                // special marker files that we don't want to create by accident (or malicious intent)
                tar::Archive::new(ZlibDecoderSync::new(inputs.reader()))
                    .unpack(&target_dir)
                    .context("Failed to unpack inputs to overlay")?;

                let cwd = Path::new(&cwd);

                tracing::trace!("[perform_build({job_id})]: creating output directories");

                // Canonicalize output path as either absolute or relative to cwd
                let output_paths_absolute = output_paths
                    .iter()
                    .map(|path| {
                        let path = Path::new(path);
                        if path.is_absolute() {
                            path.to_path_buf()
                        } else {
                            cwd.join(path)
                        }
                    })
                    .collect::<Vec<_>>();

                {
                    let h_cwd = join_suffix(&target_dir, cwd);
                    tracing::trace!("[perform_build({job_id})]: creating dir: {h_cwd:?}");
                    fs::create_dir_all(h_cwd).context("Failed to create cwd")?;
                }

                for path in output_paths_absolute.iter() {
                    // If it doesn't have a parent, nothing needs creating
                    if let Some(path) = path.parent() {
                        let h_path = join_suffix(&target_dir, path);
                        tracing::trace!("[perform_build({job_id})]: creating dir: {h_path:?}");
                        fs::create_dir_all(h_path)
                            .context(format!("Failed to create output directory {path:?}"))?;
                    }
                }

                tracing::trace!("[perform_build({job_id})]: performing compile");
                // Bubblewrap notes:
                // - We're running as uid 0 (to do the mounts above), and so bubblewrap is run as uid 0
                // - There's special handling in bubblewrap to compare uid and euid - of interest to us,
                //   if uid == euid == 0, bubblewrap preserves capabilities (not good!) so we explicitly
                //   drop all capabilities
                // - By entering a new user namespace means any set of capabilities do not apply to any
                //   other user namespace, i.e. you lose privileges. This is not strictly necessary because
                //   we're dropping caps anyway so it's irrelevant which namespace we're in, but it doesn't
                //   hurt.
                // - --unshare-all is not ideal as it happily continues if it fails to unshare either
                //   the user or cgroups namespace, so we list everything explicitly
                // - The order of bind vs proc + dev is important - the new root must be put in place
                //   first, otherwise proc and dev get hidden
                let mut cmd = std::process::Command::new(bubblewrap);
                cmd.arg("--die-with-parent")
                    .args(["--cap-drop", "ALL"])
                    .args([
                        "--unshare-user",
                        "--unshare-cgroup",
                        "--unshare-ipc",
                        "--unshare-pid",
                        "--unshare-net",
                        "--unshare-uts",
                    ])
                    .arg("--bind")
                    .arg(&target_dir)
                    .arg("/")
                    .args(["--proc", "/proc"])
                    .args(["--dev", "/dev"])
                    .arg("--chdir")
                    .arg(cwd);

                for (k, v) in env_vars {
                    if k.contains('=') {
                        tracing::warn!("[perform_build({job_id})]: Skipping environment variable: {k:?}");
                        continue;
                    }
                    cmd.arg("--setenv").arg(k).arg(v);
                }
                cmd.arg("--");
                cmd.arg(executable);
                cmd.args(arguments);

                tracing::trace!("[perform_build({job_id})]: bubblewrap command: {:?}", cmd);

                let compile_output = cmd
                    .output()
                    .context("Failed to retrieve output from compile")?;

                if !compile_output.status.success() {
                    tracing::warn!(
                        "[perform_build({job_id})]: compile output:\n===========\nstdout:\n{}\n==========\n=========\nstderr:\n{}\n===============\n",
                        String::from_utf8_lossy(&compile_output.stdout),
                        String::from_utf8_lossy(&compile_output.stderr)
                    );
                } else {
                    tracing::trace!("[perform_build({job_id})]: compile output: {compile_output:?}");
                }

                tracing::trace!("[perform_build({job_id})]: retrieving {output_paths:?}");

                let mut outputs = vec![];

                for (path, abspath) in output_paths.iter().zip(output_paths_absolute.iter()) {
                    let abspath = join_suffix(&target_dir, abspath);
                    match fs::File::open(&abspath) {
                        Ok(file) => {
                            match OutputData::try_from_reader(file) {
                                Ok(output) => outputs.push((path.clone(), output)),
                                Err(err) => {
                                    tracing::error!(
                                        "[perform_build({job_id})]: Failed to read and compress output file host={abspath:?}, overlay={path:?}: {err}"
                                    )
                                }
                            }
                        }
                        Err(e) => {
                            if e.kind() == io::ErrorKind::NotFound {
                                tracing::debug!("[perform_build({job_id})]: Missing output path host={abspath:?}, overlay={path:?}")
                            } else {
                                return Err(Error::from(e).context("Failed to open output file"));
                            }
                        }
                    }
                }

                let compile_output = ProcessOutput::try_from(compile_output)
                    .context("Failed to convert compilation exit status")?;

                Ok(BuildResult {
                    output: compile_output,
                    outputs,
                })
            })
            .await
            .context("Build thread exited unsuccessfully")?
    }

    // Failing during cleanup is pretty unexpected, but we can still return the successful compile
    // TODO: if too many of these fail, we should mark this builder as faulty
    async fn finish_overlay(&self, job_id: &str, overlay: &OverlaySpec) {
        let OverlaySpec {
            build_dir,
            toolchain_dir: _,
        } = overlay;

        if let Err(e) = fs::remove_dir_all(build_dir) {
            tracing::warn!(
                "[finish_overlay({job_id})]: Failed to remove build directory {build_dir:?}: {e}"
            );
        }
    }
}

#[async_trait]
impl BuilderIncoming for OverlayBuilder {
    async fn run_build(
        &self,
        job_id: &str,
        toolchain_dir: &Path,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs: Vec<u8>,
    ) -> Result<BuildResult> {
        tracing::debug!("[run_build({job_id})]: Preparing overlay");

        let overlay = self
            .prepare_overlay_dirs(job_id, toolchain_dir)
            .await
            .context("failed to prepare overlay dirs")?;

        tracing::debug!("[run_build({job_id})]: Performing build in {overlay:?}");

        let res = Self::perform_build(
            job_id,
            self.bubblewrap.clone(),
            command,
            inputs,
            outputs,
            overlay.clone(),
        )
        .await;

        tracing::debug!("[run_build({job_id})]: Finishing with overlay");

        self.finish_overlay(job_id, &overlay).await;

        tracing::debug!("[run_build({job_id})]: Returning result");

        res.context("Failed to perform build")
    }
}

const BUSYBOX_DOCKER_IMAGE: &str = "busybox:stable-musl";

pub struct DockerBuilder {}

impl DockerBuilder {
    // TODO: this should accept a unique string, e.g. inode of the tccache directory
    // having locked a pidfile, or at minimum should loudly detect other running
    // instances - pidfile in /tmp
    pub async fn new() -> Result<Self> {
        tracing::info!("Creating docker builder");
        Ok(Self {})
    }

    async fn perform_build(
        job_id: &str,
        toolchain_dir: &Path,
        CompileCommand {
            executable,
            arguments,
            env_vars,
            cwd,
        }: CompileCommand,
        output_paths: Vec<String>,
        inputs: Vec<u8>,
    ) -> Result<BuildResult> {
        tracing::trace!("[perform_build({job_id})]: Compile environment: {env_vars:?}");
        tracing::trace!("[perform_build({job_id})]: Compile command: {executable:?} {arguments:?}");
        tracing::trace!("[perform_build({job_id})]: Output paths: {output_paths:?}");

        if output_paths.is_empty() {
            bail!("Output paths is empty");
        }

        // Should automatically get deleted when host_temp goes out of scope
        let host_temp = tempfile::Builder::new().prefix("sccache_dist").tempdir()?;
        let host_root = host_temp.path();

        let cwd = Path::new(&cwd);
        let cwd_host = join_suffix(host_root, cwd);

        tracing::trace!("[perform_build({job_id})]: copying in inputs");

        // Copy inputs to host_root
        {
            let reader = inputs.reader();
            let reader = futures::io::AllowStdIo::new(reader);
            let reader = ZlibDecoderAsync::new(reader.compat());
            async_tar::Archive::new(reader.compat())
                .unpack(&host_root)
                .await
                .context("Failed to unpack inputs to tempdir")?;
        }

        // Canonicalize output path as either absolute or relative to cwd
        let output_paths_absolute = output_paths
            .iter()
            .map(|path| {
                let path = Path::new(path);
                if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    cwd.join(path)
                }
            })
            .collect::<Vec<_>>();

        let host_toolchain_paths = list_files(toolchain_dir);

        // Collect host CWD, input, and output dir paths
        let host_bindmount_paths = {
            // Always create the CWD even if it's not in the inputs archive
            std::iter::once(cwd_host.as_path())
                .chain(
                    // Input paths
                    list_files(host_root)
                        .iter()
                        .filter_map(|path| path.strip_prefix(host_root).ok()),
                )
                .chain(
                    // Output paths
                    output_paths_absolute.iter().map(Path::new),
                )
                // If it doesn't have a parent, nothing needs creating
                .filter_map(|path| path.parent().map(|p| join_suffix(host_root, p)))
                .unique()
                .collect::<Vec<_>>()
        };

        tracing::trace!("[perform_build({job_id})]: creating output directories");

        for path in host_bindmount_paths.iter() {
            tracing::trace!("[perform_build({job_id})]: creating dir: {path:?}");
            tokio::fs::create_dir_all(path)
                .await
                .context(format!("Failed to create output directory {path:?}"))?;
        }

        fn volume_mount<P: AsRef<Path>>(
            prefix: &Path,
            access: &str,
        ) -> impl FnMut(P) -> Vec<String> {
            let prefix = prefix.to_owned();
            let access = access.to_owned();
            move |h_path| {
                let h_path = h_path.as_ref();
                if let Ok(c_path) = h_path.strip_prefix(&prefix) {
                    let c_path = Path::new("/").join(c_path);
                    let h_path = h_path.display();
                    let c_path = c_path.display();
                    vec!["--volume".into(), format!("{h_path}:{c_path}:{access}")]
                } else {
                    vec![]
                }
            }
        }

        tracing::trace!("[perform_build({job_id})]: performing compile");

        // TODO: likely shouldn't perform the compile as root in the container
        let mut cmd = tokio::process::Command::new("docker");

        // Start a new container and remove it on exit
        cmd.args(["run", "--rm"])
            .args(["--name", &format!("sccache-builder-{job_id}")])
            // Run in `cwd`
            .args(["--workdir", &format!("{}", cwd.display())])
            // Mount input and output dirs as read-write
            .args(
                host_bindmount_paths
                    .iter()
                    .flat_map(volume_mount(host_root, "rw")),
            )
            // Mount toolchain files as read-only
            .args(
                host_toolchain_paths
                    .iter()
                    .flat_map(volume_mount(toolchain_dir, "ro")),
            )
            // Define envvars
            .args(env_vars.iter().flat_map(|(k, v)| {
                if k.contains('=') {
                    tracing::warn!(
                        "[perform_build({job_id})]: Skipping environment variable: {k:?}"
                    );
                    vec![]
                } else {
                    vec!["--env".into(), format!("{k}=\"{v}\"")]
                }
            }))
            // Name of the image to run (currently busybox:stable-musl)
            // TODO: Make this configurable?
            .arg(BUSYBOX_DOCKER_IMAGE)
            // Finally, the executable and arguments
            .arg(executable)
            .args(arguments);

        tracing::trace!("[perform_build({job_id})]: {:?}", cmd.as_std());

        let compile_output = cmd.output().await.context("Failed to compile")?;

        if !compile_output.status.success() {
            tracing::warn!(
                "[perform_build({job_id})]: compile output:\n===========\nstdout:\n{}\n==========\n=========\nstderr:\n{}\n===============\n",
                String::from_utf8_lossy(&compile_output.stdout),
                String::from_utf8_lossy(&compile_output.stderr)
            );
        } else {
            tracing::trace!("[perform_build({job_id})]: compile output: {compile_output:?}");
        }

        let mut outputs = vec![];

        tracing::trace!("[perform_build({job_id})]: retrieving {output_paths:?}");

        for (path, abspath) in output_paths.iter().zip(output_paths_absolute.iter()) {
            let abspath = join_suffix(host_root, abspath); // Resolve in case it's relative since we copy it from the root level
            match fs::File::open(&abspath) {
                Ok(file) => match OutputData::try_from_reader(file) {
                    Ok(output) => outputs.push((path.clone(), output)),
                    Err(err) => {
                        tracing::error!(
                                "[perform_build({job_id})]: Failed to read and compress output file host={abspath:?}, container={path:?}: {err}"
                            )
                    }
                },
                Err(e) => {
                    if e.kind() == io::ErrorKind::NotFound {
                        tracing::debug!(
                            "[perform_build({job_id})]: Missing output path host={abspath:?}, container={path:?}"
                        )
                    } else {
                        return Err(Error::from(e).context("Failed to open output file"));
                    }
                }
            }
        }

        let compile_output = ProcessOutput::try_from(compile_output)
            .context("Failed to convert compilation exit status")?;

        Ok(BuildResult {
            output: compile_output,
            outputs,
        })
    }
}

#[async_trait]
impl BuilderIncoming for DockerBuilder {
    // From Server
    async fn run_build(
        &self,
        job_id: &str,
        toolchain_dir: &Path,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs: Vec<u8>,
    ) -> Result<BuildResult> {
        tracing::debug!("[run_build({})]: Performing build in container", job_id);

        let res = Self::perform_build(job_id, toolchain_dir, command, outputs, inputs).await;

        tracing::debug!("[run_build({})]: Returning result", job_id);

        res.context("Failed to perform build")
    }
}
