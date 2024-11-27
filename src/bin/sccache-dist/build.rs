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
use async_trait::async_trait;
use bytes::Buf;
use flate2::read::GzDecoder;
use fs_err as fs;
use futures::lock::Mutex;
use libmount::Overlay;
use sccache::dist::{
    BuildResult, BuilderIncoming, CompileCommand, JobId, OutputData, ProcessOutput, TcCache,
    Toolchain,
};
use sccache::lru_disk_cache::Error as LruError;
use std::borrow::Borrow;
use std::collections::{hash_map, HashMap};
use std::io;
use std::iter;
use std::path::{self, Path, PathBuf};
use std::process::{Output, Stdio};
use tokio::io::AsyncReadExt;
use tokio::process::ChildStdin;
use version_compare::Version;

#[async_trait]
trait AsyncCommandExt {
    async fn check_stdout_trim(&mut self) -> Result<String>;
    async fn check_piped<F, Fut>(&mut self, pipe: F) -> Result<()>
    where
        F: FnOnce(ChildStdin) -> Fut + std::marker::Send,
        Fut: std::future::Future<Output = Result<()>> + std::marker::Send;
    async fn check_run(&mut self) -> Result<()>;
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
    // Should really take a FnOnce/FnBox
    async fn check_piped<F, Fut>(&mut self, pipe: F) -> Result<()>
    where
        F: FnOnce(ChildStdin) -> Fut + std::marker::Send,
        Fut: std::future::Future<Output = Result<()>> + std::marker::Send,
    {
        let mut process = self
            .stdin(Stdio::piped())
            .spawn()
            .context("Failed to start command")?;
        pipe(
            process
                .stdin
                .take()
                .expect("Requested piped stdin but not present"),
        )
        .await
        .context("Failed to pipe input to process")?;
        let output = process
            .wait_with_output()
            .await
            .context("Failed to wait for process to return")?;
        check_output(&output)
    }
    async fn check_run(&mut self) -> Result<()> {
        let output = self.output().await.context("Failed to start command")?;
        check_output(&output)
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

#[derive(Debug, Clone)]
struct DeflatedToolchain {
    path: PathBuf,
    build_count: u64,
}

pub struct OverlayBuilder {
    bubblewrap: PathBuf,
    dir: PathBuf,
    toolchain_dir_map: Mutex<HashMap<Toolchain, DeflatedToolchain>>,
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
        let ret = Self {
            bubblewrap,
            dir,
            toolchain_dir_map: Mutex::new(HashMap::new()),
        };
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

    async fn cleanup_old_toolchains(
        &self,
        job_id: JobId,
        tccache: &TcCache,
        tc_dirs: &mut HashMap<Toolchain, DeflatedToolchain>,
    ) {
        if tc_dirs.len() >= tccache.len() {
            let dir_map = tc_dirs.clone();
            for (tc, entry) in dir_map.iter() {
                // Only clean up old uncompressed toolchains that aren't currently in use
                if !tccache.contains_toolchain(tc) && entry.build_count == 0 {
                    tracing::warn!(
                        "[cleanup_old_toolchains({})]: Removing old un-compressed toolchain: {:?}",
                        job_id,
                        tc.archive_id
                    );
                    if tc_dirs.remove(tc).is_none() {
                        tracing::warn!(
                            "[cleanup_old_toolchains({})]: Toochain {} not in toolchain_dir_map",
                            job_id,
                            tc.archive_id
                        );
                    }
                    fs::remove_dir_all(self.dir.join("toolchains").join(&tc.archive_id))
                        .context("Failed to remove old toolchain")
                        .unwrap_or_else(|err| {
                            tracing::warn!("[cleanup_old_toolchains({})]: {:?}", job_id, err)
                        });
                }
            }
        }
    }

    async fn prepare_overlay_dirs(
        &self,
        job_id: JobId,
        tc: &Toolchain,
        tccache: &Mutex<TcCache>,
    ) -> Result<OverlaySpec> {
        let DeflatedToolchain {
            path: toolchain_dir,
            build_count: _,
        } = {
            let mut toolchain_dir_map = self.toolchain_dir_map.lock().await;
            // Create the toolchain dir (if necessary) while we have an exclusive lock
            let toolchain_dir = self.dir.join("toolchains").join(&tc.archive_id);
            if toolchain_dir_map.contains_key(tc) && toolchain_dir.exists() {
                // TODO: use if let when sccache can use NLL
                let entry = toolchain_dir_map
                    .get_mut(tc)
                    .expect("Key missing after checking");
                entry.build_count += 1;
                entry.clone()
            } else {
                tracing::trace!(
                    "[prepare_overlay_dirs({})]: Creating toolchain directory for archive {}: {:?}",
                    job_id,
                    tc.archive_id,
                    toolchain_dir
                );

                let mut tccache = tccache.lock().await;

                self.cleanup_old_toolchains(job_id, &tccache, &mut toolchain_dir_map)
                    .await;

                let toolchain_rdr = match tccache.get(tc) {
                    Ok(rdr) => rdr,
                    Err(LruError::FileNotInCache) => {
                        bail!(
                            "[prepare_overlay_dirs({})]: Expected toolchain {}, but not available",
                            job_id,
                            tc.archive_id
                        )
                    }
                    Err(e) => {
                        return Err(Error::from(e).context("Failed to get toolchain from cache"))
                    }
                };

                fs::create_dir_all(&toolchain_dir)
                    .context("Failed to create toolchain dir")
                    .unwrap_or_else(|err| {
                        tracing::warn!("[prepare_overlay_dirs({})]: {:?}", job_id, err)
                    });

                tar::Archive::new(GzDecoder::new(toolchain_rdr))
                    .unpack(&toolchain_dir)
                    .map_err(|err| {
                        tracing::warn!(
                            "[prepare_overlay_dirs({})]: Failed to unpack toolchain {}: {:?}",
                            job_id,
                            tc.archive_id,
                            err
                        );
                        fs::remove_dir_all(&toolchain_dir)
                            .context("Failed to remove unpacked toolchain")
                            .unwrap_or_else(|err| {
                                tracing::warn!("[prepare_overlay_dirs({})]: {:?}", job_id, err)
                            });
                        tccache
                            .remove(tc)
                            .context("Failed to remove corrupt toolchain")
                            .unwrap_or_else(|err| {
                                tracing::warn!("[prepare_overlay_dirs({})]: {:?}", job_id, err)
                            });
                        Error::from(err)
                    })?;

                let entry = DeflatedToolchain {
                    path: toolchain_dir,
                    build_count: 1,
                };

                toolchain_dir_map.insert(tc.clone(), entry.clone());

                entry
            }
        };

        let build_dir = self
            .dir
            .join("builds")
            .join(format!("{}-{}", tc.archive_id, job_id));

        tracing::trace!(
            "[prepare_overlay_dirs({})]: Creating build directory for {}-{}: {:?}",
            job_id,
            tc.archive_id,
            job_id,
            build_dir
        );

        fs::create_dir_all(&build_dir)
            .context("Failed to create build dir")
            .unwrap_or_else(|err| tracing::warn!("[prepare_overlay_dirs({})]: {:?}", job_id, err));

        Ok(OverlaySpec {
            build_dir,
            toolchain_dir,
        })
    }

    async fn perform_build(
        job_id: JobId,
        bubblewrap: PathBuf,
        compile_command: CompileCommand,
        mut inputs_rdr: std::pin::Pin<&mut (dyn tokio::io::AsyncRead + Send)>,
        output_paths: Vec<String>,
        overlay: OverlaySpec,
    ) -> Result<BuildResult> {
        tracing::trace!(
            "[perform_build({})]: Compile environment: {:?}",
            job_id,
            compile_command.env_vars
        );
        tracing::trace!(
            "[perform_build({})]: Compile command: {:?} {:?}",
            job_id,
            compile_command.executable,
            compile_command.arguments
        );

        // Read into memory because we can't use asyncio in the thread below.
        let mut inputs_buf = vec![];
        inputs_rdr.read_to_end(&mut inputs_buf).await?;

        std::thread::scope(|scope| {
            scope
                .spawn(|| {
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
                    fs::create_dir_all(&work_dir)
                        .context("Failed to create overlay work directory")?;
                    fs::create_dir_all(&upper_dir)
                        .context("Failed to create overlay upper directory")?;
                    fs::create_dir_all(&target_dir)
                        .context("Failed to create overlay target directory")?;

                    let () = Overlay::writable(
                        iter::once(overlay.toolchain_dir.as_path()),
                        upper_dir,
                        work_dir,
                        &target_dir,
                        // This error is unfortunately not Send+Sync
                    )
                    .mount()
                    .map_err(|e| anyhow!("Failed to mount overlay FS: {}", e.to_string()))?;

                    tracing::trace!("[perform_build({})]: copying in inputs", job_id);
                    // Note that we don't unpack directly into the upperdir since there overlayfs has some
                    // special marker files that we don't want to create by accident (or malicious intent)
                    tar::Archive::new(inputs_buf.reader())
                        .unpack(&target_dir)
                        .context("Failed to unpack inputs to overlay")?;

                    let CompileCommand {
                        executable,
                        arguments,
                        env_vars,
                        cwd,
                    } = compile_command;
                    let cwd = Path::new(&cwd);

                    tracing::trace!("[perform_build({})]: creating output directories", job_id);
                    fs::create_dir_all(join_suffix(&target_dir, cwd))
                        .context("Failed to create cwd")?;
                    for path in output_paths.iter() {
                        // If it doesn't have a parent, nothing needs creating
                        let output_parent = if let Some(p) = Path::new(path).parent() {
                            p
                        } else {
                            continue;
                        };
                        fs::create_dir_all(join_suffix(&target_dir, cwd.join(output_parent)))
                            .context("Failed to create an output directory")?;
                    }

                    tracing::trace!("[perform_build({})]: performing compile", job_id);
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
                            tracing::warn!(
                                "[perform_build({})]: Skipping environment variable: {:?}",
                                job_id,
                                k
                            );
                            continue;
                        }
                        cmd.arg("--setenv").arg(k).arg(v);
                    }
                    cmd.arg("--");
                    cmd.arg(executable);
                    cmd.args(arguments);

                    tracing::trace!("[perform_build({})]: bubblewrap command: {:?}", job_id, cmd);

                    let compile_output = cmd
                        .output()
                        .context("Failed to retrieve output from compile")?;
                    tracing::trace!(
                        "[perform_build({})]: compile_output: {:?}",
                        job_id,
                        compile_output
                    );

                    let mut outputs = vec![];
                    tracing::trace!("[perform_build({})]: retrieving {:?}", job_id, output_paths);
                    for path in output_paths {
                        let abspath = join_suffix(&target_dir, cwd.join(&path)); // Resolve in case it's relative since we copy it from the root level
                        match fs::File::open(abspath) {
                            Ok(file) => {
                                let output = OutputData::try_from_reader(file)
                                    .context("Failed to read output file")?;
                                outputs.push((path, output))
                            }
                            Err(e) => {
                                if e.kind() == io::ErrorKind::NotFound {
                                    tracing::debug!(
                                        "[perform_build({})]: Missing output path {:?}",
                                        job_id,
                                        path
                                    )
                                } else {
                                    return Err(
                                        Error::from(e).context("Failed to open output file")
                                    );
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
                    // Bizarrely there's no way to actually get any information from a thread::Result::Err
                })
                .join()
                .unwrap_or_else(|_e| Err(anyhow!("Build thread exited unsuccessfully")))
        })
    }

    // Failing during cleanup is pretty unexpected, but we can still return the successful compile
    // TODO: if too many of these fail, we should mark this builder as faulty
    async fn finish_overlay(
        &self,
        job_id: JobId,
        tc: &Toolchain,
        tccache: &Mutex<TcCache>,
        overlay: &OverlaySpec,
    ) {
        let OverlaySpec {
            build_dir,
            toolchain_dir: _,
        } = overlay;

        if let Err(e) = fs::remove_dir_all(build_dir) {
            tracing::warn!(
                "[finish_overlay({})]: Failed to remove build directory {}: {}",
                job_id,
                build_dir.display(),
                e
            );
        }

        // TODO: collect toolchain directories

        // Decrement the build count so its toolchain can be cleaned up later
        let mut toolchain_dir_map = self.toolchain_dir_map.lock().await;
        if let Some(entry) = toolchain_dir_map.get_mut(tc) {
            entry.build_count = std::cmp::max(0, entry.build_count - 1);
        }

        self.cleanup_old_toolchains(
            job_id,
            tccache.lock().await.borrow(),
            &mut toolchain_dir_map,
        )
        .await;
    }
}

#[async_trait]
impl BuilderIncoming for OverlayBuilder {
    async fn run_build(
        &self,
        job_id: JobId,
        tc: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs_rdr: std::pin::Pin<&mut (dyn tokio::io::AsyncRead + Send)>,
        tccache: &Mutex<TcCache>,
    ) -> Result<BuildResult> {
        tracing::debug!("[run_build({})]: Preparing overlay", job_id);
        let overlay = self
            .prepare_overlay_dirs(job_id, &tc, tccache)
            .await
            .context("failed to prepare overlay dirs")?;
        tracing::debug!("[run_build({})]: Performing build in {:?}", job_id, overlay);
        let res = Self::perform_build(
            job_id,
            self.bubblewrap.clone(),
            command,
            inputs_rdr,
            outputs,
            overlay.clone(),
        )
        .await;
        tracing::debug!("[run_build({})]: Finishing with overlay", job_id);
        self.finish_overlay(job_id, &tc, tccache, &overlay).await;
        tracing::debug!("[run_build({})]: Returning result", job_id);
        res.context("Failed to perform build")
    }
}

const BASE_DOCKER_IMAGE: &str = "busybox:stable-musl";
// Make sure sh doesn't exec the final command, since we need it to do
// init duties (reaping zombies). Also, because we kill -9 -1, that kills
// the sleep (it's not a builtin) so it needs to be a loop.
const DOCKER_SHELL_INIT: &str = "while true; do busybox sleep 365d && busybox true; done";

// Check the diff and clean up the FS
async fn docker_diff(cid: &str) -> Result<String> {
    let mut cmd = tokio::process::Command::new("docker");
    cmd.args(["diff", cid])
        .check_stdout_trim()
        .await
        .context("Failed to Docker diff container")
}

// Force remove the container
async fn docker_rm(cid: &str) -> Result<()> {
    let mut cmd = tokio::process::Command::new("docker");
    cmd.args(["rm", "-f", cid])
        .check_run()
        .await
        .context("Failed to force delete container")
}

pub struct DockerBuilder {
    image_map: Mutex<HashMap<Toolchain, String>>,
    container_lists: Mutex<HashMap<Toolchain, Vec<String>>>,
}

impl DockerBuilder {
    // TODO: this should accept a unique string, e.g. inode of the tccache directory
    // having locked a pidfile, or at minimum should loudly detect other running
    // instances - pidfile in /tmp
    pub async fn new() -> Result<Self> {
        tracing::info!("Creating docker builder");

        let ret = Self {
            image_map: Mutex::new(HashMap::new()),
            container_lists: Mutex::new(HashMap::new()),
        };
        ret.cleanup().await?;
        Ok(ret)
    }

    // TODO: this should really reclaim, and should check in the image map and container lists, so
    // that when things are removed from there it becomes a form of GC
    async fn cleanup(&self) -> Result<()> {
        tracing::info!("Performing initial Docker cleanup");

        let mut cmd = tokio::process::Command::new("docker");
        let containers = cmd
            .args(["ps", "-a", "--format", "{{.ID}} {{.Image}}"])
            .check_stdout_trim()
            .await
            .context("Unable to list all Docker containers")?;
        if !containers.is_empty() {
            let mut containers_to_rm = vec![];
            for line in containers.split(|c| c == '\n') {
                let mut iter = line.splitn(2, ' ');
                let container_id = iter
                    .next()
                    .context("Malformed container listing - no container ID")?;
                let image_name = iter
                    .next()
                    .context("Malformed container listing - no image name")?;
                if iter.next().is_some() {
                    bail!("Malformed container listing - third field on row")
                }
                if image_name.starts_with("sccache-builder-") {
                    containers_to_rm.push(container_id)
                }
            }
            if !containers_to_rm.is_empty() {
                let mut cmd = tokio::process::Command::new("docker");
                cmd.args(["rm", "-f"])
                    .args(containers_to_rm)
                    .check_run()
                    .await
                    .context("Failed to start command to remove old containers")?;
            }
        }

        let mut cmd = tokio::process::Command::new("docker");
        let images = cmd
            .args(["images", "--format", "{{.ID}} {{.Repository}}"])
            .check_stdout_trim()
            .await
            .context("Failed to list all docker images")?;
        if !images.is_empty() {
            let mut images_to_rm = vec![];
            for line in images.split(|c| c == '\n') {
                let mut iter = line.splitn(2, ' ');
                let image_id = iter
                    .next()
                    .context("Malformed image listing - no image ID")?;
                let image_name = iter
                    .next()
                    .context("Malformed image listing - no image name")?;
                if iter.next().is_some() {
                    bail!("Malformed image listing - third field on row")
                }
                if image_name.starts_with("sccache-builder-") {
                    images_to_rm.push(image_id)
                }
            }
            if !images_to_rm.is_empty() {
                let mut cmd = tokio::process::Command::new("docker");
                cmd.args(["rmi"])
                    .args(images_to_rm)
                    .check_run()
                    .await
                    .context("Failed to remove image")?
            }
        }

        tracing::info!("Completed initial Docker cleanup");
        Ok(())
    }

    // If we have a spare running container, claim it and remove it from the available list,
    // otherwise try and create a new container (possibly creating the Docker image along
    // the way)
    async fn get_container(
        &self,
        job_id: JobId,
        tc: &Toolchain,
        tccache: &Mutex<TcCache>,
    ) -> Result<String> {
        let container = {
            let mut map = self.container_lists.lock().await;
            map.entry(tc.clone()).or_default().pop()
        };
        match container {
            Some(cid) => Ok(cid),
            None => {
                // TODO: can improve parallelism (of creating multiple images at a time) by using another
                // (more fine-grained) mutex around the entry value and checking if its empty a second time
                let image = {
                    let mut map = self.image_map.lock().await;
                    match map.entry(tc.clone()) {
                        hash_map::Entry::Occupied(e) => e.get().clone(),
                        hash_map::Entry::Vacant(e) => {
                            tracing::info!("[get_container({})]: Creating Docker image for {:?} (may block requests)", job_id, tc);
                            let image = Self::make_image(job_id, tc, tccache).await?;
                            e.insert(image.clone());
                            image
                        }
                    }
                };
                Self::start_container(&image).await
            }
        }
    }

    async fn clean_container(&self, job_id: JobId, cid: &str) -> Result<()> {
        // Clean up any running processes
        let mut cmd = tokio::process::Command::new("docker");
        cmd.args(["exec", cid, "busybox", "kill", "-9", "-1"])
            .check_run()
            .await
            .context("Failed to run kill on all processes in container")?;

        let diff = docker_diff(cid).await?;
        if !diff.is_empty() {
            let mut lastpath = None;
            for line in diff.split(|c| c == '\n') {
                let mut iter = line.splitn(2, ' ');
                let changetype = iter
                    .next()
                    .context("Malformed container diff - no change type")?;
                let changepath = iter
                    .next()
                    .context("Malformed container diff - no change path")?;
                if iter.next().is_some() {
                    bail!("Malformed container diff - third field on row")
                }
                // TODO: If files are created in this dir, it gets marked as modified.
                // A similar thing applies to /root or /build etc
                if changepath == "/tmp" {
                    continue;
                }
                if changetype != "A" {
                    bail!(
                        "Path {} had a non-A changetype of {}",
                        changepath,
                        changetype
                    );
                }
                // Docker diff paths are in alphabetical order and we do `rm -rf`, so we might be able to skip
                // calling Docker more than necessary (since it's slow)
                if let Some(lastpath) = lastpath {
                    if Path::new(changepath).starts_with(lastpath) {
                        continue;
                    }
                }
                lastpath = Some(changepath);
                let mut cmd = tokio::process::Command::new("docker");
                if let Err(e) = cmd
                    .args(["exec", cid, "busybox", "rm", "-rf", changepath])
                    .check_run()
                    .await
                {
                    // We do a final check anyway, so just continue
                    tracing::warn!(
                        "[clean_container({})]: Failed to remove added path in a container: {}",
                        job_id,
                        e
                    )
                }
            }

            let newdiff = docker_diff(cid).await?;
            // See note about changepath == "/tmp" above
            if !newdiff.is_empty() && newdiff != "C /tmp" {
                bail!(
                    "Attempted to delete files, but container still has a diff: {:?}",
                    newdiff
                );
            }
        }

        Ok(())
    }

    // Failing during cleanup is pretty unexpected, but we can still return the successful compile
    // TODO: if too many of these fail, we should mark this builder as faulty
    async fn finish_container(&self, job_id: JobId, tc: &Toolchain, cid: String) {
        // TODO: collect images

        if let Err(e) = self.clean_container(job_id, &cid).await {
            tracing::info!(
                "[finish_container({})]: Failed to clean container {}: {}",
                job_id,
                cid,
                e
            );
            if let Err(e) = docker_rm(&cid).await {
                tracing::warn!(
                    "[finish_container({})]: Failed to remove container {} after failed clean: {}",
                    job_id,
                    cid,
                    e
                );
            }
            return;
        }

        // Good as new, add it back to the container list
        if let Some(entry) = self.container_lists.lock().await.get_mut(tc) {
            tracing::debug!(
                "[finish_container({})]: Reclaimed container {}",
                job_id,
                cid
            );
            entry.push(cid)
        } else {
            tracing::warn!(
                "[finish_container({})]: Was ready to reclaim container {} but toolchain went missing",
                job_id, cid
            );
            if let Err(e) = docker_rm(&cid).await {
                tracing::warn!(
                    "[finish_container({})]: Failed to remove container {}: {}",
                    job_id,
                    cid,
                    e
                );
            }
        }
    }

    async fn make_image(job_id: JobId, tc: &Toolchain, tccache: &Mutex<TcCache>) -> Result<String> {
        let mut cmd = tokio::process::Command::new("docker");
        let cid = cmd
            .args(["create", BASE_DOCKER_IMAGE, "busybox", "true"])
            .check_stdout_trim()
            .await
            .context("Failed to create docker container")?;

        let mut tccache = tccache.lock().await;
        let mut toolchain_rdr = match tccache.get_async(tc).await {
            Ok(rdr) => rdr,
            Err(LruError::FileNotInCache) => bail!(
                "Expected to find toolchain {}, but not available",
                tc.archive_id
            ),
            Err(e) => {
                return Err(e).with_context(|| format!("Failed to use toolchain {}", tc.archive_id))
            }
        };

        tracing::trace!("[make_image({})]: Copying in toolchain", job_id);
        let mut cmd = tokio::process::Command::new("docker");
        cmd.args(["cp", "-", &format!("{}:/", cid)])
            .check_piped(|mut stdin| async move {
                tokio::io::copy(&mut toolchain_rdr, &mut stdin).await?;
                Ok(())
            })
            .await
            .context("Failed to copy toolchain tar into container")?;

        let imagename = format!("sccache-builder-{}", &tc.archive_id);
        let mut cmd = tokio::process::Command::new("docker");
        cmd.args(["commit", &cid, &imagename])
            .check_run()
            .await
            .context("Failed to commit container after build")?;

        let mut cmd = tokio::process::Command::new("docker");
        cmd.args(["rm", "-f", &cid])
            .check_run()
            .await
            .context("Failed to remove temporary build container")?;

        Ok(imagename)
    }

    async fn start_container(image: &str) -> Result<String> {
        let mut cmd = tokio::process::Command::new("docker");
        cmd.args(["run", "-d", image, "busybox", "sh", "-c", DOCKER_SHELL_INIT])
            .check_stdout_trim()
            .await
            .context("Failed to run container")
    }

    async fn perform_build(
        job_id: JobId,
        compile_command: CompileCommand,
        mut inputs_rdr: std::pin::Pin<&mut (dyn tokio::io::AsyncRead + Send)>,
        output_paths: Vec<String>,
        cid: &str,
    ) -> Result<BuildResult> {
        tracing::trace!(
            "[perform_build({})]: Compile environment: {:?}",
            job_id,
            compile_command.env_vars
        );
        tracing::trace!(
            "[perform_build({})]: Compile command: {:?} {:?}",
            job_id,
            compile_command.executable,
            compile_command.arguments
        );

        tracing::trace!(
            "[perform_build({})]: Output paths: {:?}",
            job_id,
            output_paths
        );

        if output_paths.is_empty() {
            bail!("output_paths is empty");
        }

        tracing::trace!("[perform_build({})]: copying in inputs", job_id);

        let mut cmd = tokio::process::Command::new("docker");
        cmd.args(["cp", "-", &format!("{}:/", cid)])
            .check_piped(|mut stdin| async move {
                tokio::io::copy(&mut inputs_rdr, &mut stdin).await?;
                Ok(())
            })
            .await
            .context("Failed to copy inputs tar into container")?;

        let CompileCommand {
            executable,
            arguments,
            env_vars,
            cwd,
        } = compile_command;
        let cwd = Path::new(&cwd);

        tracing::trace!("[perform_build({})]: creating output directories", job_id);
        let mut cmd = tokio::process::Command::new("docker");
        cmd.args(["exec", cid, "busybox", "mkdir", "-p"]).arg(cwd);
        for path in output_paths.iter() {
            // If it doesn't have a parent, nothing needs creating
            let output_parent = if let Some(p) = Path::new(path).parent() {
                p
            } else {
                continue;
            };
            cmd.arg(cwd.join(output_parent));
        }
        cmd.check_run()
            .await
            .context("Failed to create directories required for compile in container")?;

        tracing::trace!("[perform_build({})]: performing compile", job_id);
        // TODO: likely shouldn't perform the compile as root in the container
        let mut cmd = tokio::process::Command::new("docker");
        cmd.arg("exec");
        for (k, v) in env_vars {
            if k.contains('=') {
                tracing::warn!(
                    "[perform_build({})]: Skipping environment variable: {:?}",
                    job_id,
                    k
                );
                continue;
            }
            let mut env = k;
            env.push('=');
            env.push_str(&v);
            cmd.arg("-e").arg(env);
        }
        let shell_cmd = "cd \"$1\" && shift && exec \"$@\"";
        cmd.args([cid, "busybox", "sh", "-c", shell_cmd]);
        cmd.arg(&executable);
        cmd.arg(cwd);
        cmd.arg(executable);
        cmd.args(arguments);
        let compile_output = cmd
            .output()
            .await
            .context("Failed to start executing compile")?;
        tracing::trace!(
            "[perform_build({})]: compile_output: {:?}",
            job_id,
            compile_output
        );

        let mut outputs = vec![];
        tracing::trace!("[perform_build({})]: retrieving {:?}", job_id, output_paths);
        for path in output_paths {
            let abspath = cwd.join(&path); // Resolve in case it's relative since we copy it from the root level
                                           // TODO: this isn't great, but cp gives it out as a tar
            let mut cmd = tokio::process::Command::new("docker");
            let output = cmd
                .args(["exec", cid, "busybox", "cat"])
                .arg(abspath)
                .output()
                .await
                .context("Failed to start command to retrieve output file")?;
            if output.status.success() {
                let output = OutputData::try_from_reader(&*output.stdout)
                    .expect("Failed to read compress output stdout");
                outputs.push((path, output))
            } else {
                tracing::debug!(
                    "[perform_build({})]: Missing output path {:?}",
                    job_id,
                    path
                )
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
        job_id: JobId,
        tc: Toolchain,
        command: CompileCommand,
        outputs: Vec<String>,
        inputs_rdr: std::pin::Pin<&mut (dyn tokio::io::AsyncRead + Send)>,
        tccache: &Mutex<TcCache>,
    ) -> Result<BuildResult> {
        tracing::debug!("[run_build({})]: Finding container", job_id);
        let cid = self
            .get_container(job_id, &tc, tccache)
            .await
            .context("Failed to get a container for build")?;
        tracing::debug!(
            "[run_build({})]: Performing build with container {}",
            job_id,
            cid
        );
        let res = Self::perform_build(job_id, command, inputs_rdr, outputs, &cid).await;
        tracing::debug!("[run_build({})]: Finishing with container {}", job_id, cid);
        self.finish_container(job_id, &tc, cid).await;
        tracing::debug!("[run_build({})]: Returning result", job_id);
        res.context("Failed to perform build")
    }
}
