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
use async_compression::futures::bufread::ZlibDecoder as ZlibDecoderAsync;
use async_trait::async_trait;
use bytes::Buf;
use flate2::read::ZlibDecoder as ZlibDecoderSync;
use fs_err as fs;
use futures::lock::Mutex;
use futures::FutureExt;
use itertools::Itertools;
use libmount::Overlay;
use sccache::dist::{BuildResult, BuilderIncoming, CompileCommand, OutputData};
use sccache::mock_command::ProcessOutput;
use std::collections::HashMap;
use std::io;
use std::path::{self, Path, PathBuf};
use std::process::{Output, Stdio};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::ChildStdin;
use tokio_util::compat::FuturesAsyncReadCompatExt;
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

type OverlayChildren = Mutex<
    HashMap<
        String,
        (
            OverlaySpec,
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        ),
    >,
>;

#[derive(Clone)]
pub struct OverlayBuilder {
    bubblewrap: PathBuf,
    children: Arc<OverlayChildren>,
    dir: PathBuf,
    job_queue: Arc<tokio::sync::Semaphore>,
}

impl OverlayBuilder {
    pub async fn new(
        bubblewrap: PathBuf,
        dir: PathBuf,
        job_queue: Arc<tokio::sync::Semaphore>,
    ) -> Result<Self> {
        tracing::info!("Creating overlay builder with dir {dir:?}");

        if !nix::unistd::getuid().is_root() && !nix::unistd::geteuid().is_root() {
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

        let dir = dir.join("builds");

        // TODO: pidfile
        let ret = Self {
            bubblewrap,
            children: Default::default(),
            dir,
            job_queue,
        };
        ret.cleanup().await?;
        tokio::fs::create_dir_all(&ret.dir)
            .await
            .context("Failed to create builder builds directory")?;
        Ok(ret)
    }

    async fn cleanup(&self) -> Result<()> {
        if self.dir.exists() {
            tokio::fs::remove_dir_all(&self.dir)
                .await
                .context("Failed to clean up builder directory")?
        }
        Ok(())
    }

    async fn prepare_overlay_dirs(
        &self,
        job_id: &str,
        toolchain_dir: &Path,
    ) -> Result<OverlaySpec> {
        let build_dir = self.dir.join(uuid::Uuid::new_v4().simple().to_string());

        tracing::trace!(
            "[prepare_overlay_dirs({job_id})]: Creating build directory: {build_dir:?}"
        );

        tokio::fs::create_dir_all(&build_dir)
            .await
            .context("Failed to create build dir")
            .unwrap_or_else(|err| tracing::warn!("[prepare_overlay_dirs({job_id})]: {err:?}"));

        Ok(OverlaySpec {
            build_dir,
            toolchain_dir: toolchain_dir.to_owned(),
        })
    }

    #[allow(clippy::too_many_arguments)]
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
        job_queue: &tokio::sync::Semaphore,
        children: &OverlayChildren,
    ) -> Result<BuildResult> {
        tracing::trace!("[perform_build({job_id})]: Compile environment: {env_vars:?}");
        tracing::trace!("[perform_build({job_id})]: Compile command: {executable:?} {arguments:?}");
        tracing::trace!("[perform_build({job_id})]: Output paths: {output_paths:?}");

        let job_id_1 = job_id.to_owned();
        let overlay_1 = overlay.clone();

        let build_in_overlay =
            move |runtime: &tokio::runtime::Handle,
                  cancelled_tx: tokio::sync::oneshot::Sender<()>,
                  cancelled_rx: tokio::sync::oneshot::Receiver<()>| {
                let job_id = job_id_1.clone();
                let job_id_1 = job_id.clone();
                let build_dir = overlay_1.build_dir;
                let toolchain_dir = overlay_1.toolchain_dir;

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

                let work_dir = build_dir.join("work");
                let upper_dir = build_dir.join("upper");
                let target_dir = build_dir.join("target");
                fs::create_dir_all(&work_dir).context("Failed to create overlay work directory")?;
                fs::create_dir_all(&upper_dir)
                    .context("Failed to create overlay upper directory")?;
                fs::create_dir_all(&target_dir)
                    .context("Failed to create overlay target directory")?;

                let () = Overlay::writable(
                    std::iter::once(toolchain_dir.as_path()),
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
                    .map(|path| cwd.join(Path::new(path)))
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

                tracing::trace!("[perform_build({job_id})]: creating compile command");

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
                let mut cmd = tokio::process::Command::new(bubblewrap);
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
                            "[perform_build({job_id})]: Skipping environment variable: {k:?}"
                        );
                        continue;
                    }
                    cmd.arg("--setenv").arg(k).arg(v);
                }
                cmd.arg("--");
                cmd.arg(&executable);
                cmd.args(arguments);
                cmd.kill_on_drop(true);

                tracing::trace!("[perform_build({job_id})]: performing compile");
                tracing::trace!("[perform_build({job_id})]: bubblewrap command: {:?}", cmd);

                let output = runtime.block_on(async move {
                    let mut child = match cmd.spawn() {
                        Ok(child) => child,
                        Err(e) => return Some(Err(e.into())),
                    };

                    async fn buffer_output<S>(stream: Option<S>) -> Result<Vec<u8>>
                    where
                        S: AsyncReadExt + Unpin,
                    {
                        let mut buf = Vec::new();
                        if let Some(mut stream) = stream {
                            stream
                                .read_to_end(&mut buf)
                                .await
                                .context("Failed to read output")?;
                        }
                        Ok(buf)
                    }

                    let stdout = buffer_output(child.stdout.take());
                    let stderr = buffer_output(child.stderr.take());
                    let status = {
                        let status = child.wait();
                        async move { status.await.context("Failed to wait for child") }
                    };

                    let completed = async move {
                        futures::future::try_join3(status, stdout, stderr)
                            .await
                            .map(|(status, stdout, stderr)| {
                                ProcessOutput::from(std::process::Output {
                                    status,
                                    stdout,
                                    stderr,
                                })
                            })
                    };

                    futures::select_biased! {
                        _ = cancelled_rx.fuse() => {
                            if let Err(err) = child.kill().await.context("Failed to kill child") {
                                tracing::warn!("[perform_build({job_id_1})]: {err:?}");
                            }
                            let _ = cancelled_tx.send(());
                            None
                        },
                        output = completed.fuse() => {
                            Some(output.context("Failed to retrieve output from compile"))
                        },
                    }
                });

                if output.is_none() {
                    return Err(anyhow!("Build cancelled"));
                }

                let output = output.unwrap()?;

                let mut outputs = vec![];

                if !output.success() {
                    if output.exit() {
                        tracing::trace!("[perform_build({job_id})]: compile failure: {output:?}");
                    } else {
                        // Warn on abnormal terminations (i.e. SIGTERM, SIGKILL)
                        tracing::warn!(
                            "[perform_build({job_id})]: {executable:?} terminated with {}",
                            output.desc()
                        );
                    }
                } else {
                    tracing::trace!("[perform_build({job_id})]: compile success: {output:?}");
                    tracing::trace!("[perform_build({job_id})]: retrieving {output_paths:?}");

                    for (path, abspath) in output_paths.iter().zip(output_paths_absolute.iter()) {
                        let abspath = join_suffix(&target_dir, abspath);
                        match fs::File::open(&abspath) {
                            Ok(file) => match OutputData::try_from_reader(file) {
                                Ok(output) => outputs.push((path.clone(), output)),
                                Err(err) => {
                                    tracing::error!(
                                        "[perform_build({job_id})]: Failed to read and compress output file host={abspath:?}, overlay={path:?}: {err}"
                                    )
                                }
                            },
                            Err(e) => {
                                if e.kind() == io::ErrorKind::NotFound {
                                    tracing::debug!("[perform_build({job_id})]: Missing output path host={abspath:?}, overlay={path:?}")
                                } else {
                                    return Err(
                                        Error::from(e).context("Failed to open output file")
                                    );
                                }
                            }
                        }
                    }
                }

                Ok(BuildResult { output, outputs })
            };

        let runtime = tokio::runtime::Handle::current();
        let (cancel_job_tx, cancel_job_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();

        children
            .lock()
            .await
            .insert(job_id.to_owned(), (overlay, cancel_job_tx, cancelled_rx));

        {
            // Guard compiling until we get a token from the job queue
            let _job_slot = job_queue.acquire().await?;

            // Explicitly launch a new thread outside tokio's thread pool,
            // so that our overlayfs and tmpfs are unmounted when it dies.
            //
            // Keeping the handle alive for the lifetime of the oneshot channel
            // to ensure it's only dropped if the Future is cancelled.
            let handle = std::thread::spawn(move || {
                completed_tx.send(build_in_overlay(&runtime, cancelled_tx, cancel_job_rx))
            });

            // Asynchronously wait till the build thread is done so we don't block the tokio worker thread.
            let res = completed_rx
                .await
                .unwrap_or_else(|e| Err(anyhow!("Build thread exited with error: {e:?}")));

            // Now that the build thread is done, join() will return immediately.
            handle
                .join()
                // Handle panics
                .map_err(|e| anyhow!("Build thread exited with error: {e:?}"))
                // Handle unwrapping if completed_tx.send() returns Err
                .and_then(|out| out.map(|_| res).unwrap_or_else(|out| out))
        }
    }

    async fn finish_overlay(job_id: &str, overlay: &OverlaySpec) {
        let OverlaySpec {
            build_dir,
            toolchain_dir: _,
        } = overlay;

        if build_dir.exists() {
            if let Err(e) = tokio::fs::remove_dir_all(build_dir).await {
                tracing::warn!(
                    "[finish_overlay({job_id})]: Failed to remove build directory {:?}: {e:?}",
                    build_dir.display()
                );
            }
        }
    }
}

#[async_trait]
impl BuilderIncoming for OverlayBuilder {
    async fn run_build(
        &self,
        job_id: &str,
        toolchain_dir: &Path,
        inputs: Vec<u8>,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> Result<BuildResult> {
        // Bail early if job_queue is closed while this job is running
        drop(self.job_queue.acquire().await?);

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
            self.job_queue.as_ref(),
            self.children.as_ref(),
        )
        .await;

        tracing::debug!("[run_build({job_id})]: Returning result");

        res.context("Failed to perform build")
    }

    async fn finish_build(&self, job_id: &str) {
        if let Some((overlay, cancel, cancelled)) = self.children.lock().await.remove(job_id) {
            let _ = cancel.send(());
            let _ = cancelled.await;
            tracing::debug!(
                "[finish_build({job_id})]: Finishing with overlay {:?}",
                overlay.build_dir.display()
            );
            Self::finish_overlay(job_id, &overlay).await;
        }
    }

    async fn shutdown(&self) {
        self.job_queue.close();
        futures::future::join_all(self.children.lock().await.drain().map(
            |(job_id, (overlay, cancel, cancelled))| async move {
                let _ = cancel.send(());
                let _ = cancelled.await;
                tracing::debug!(
                    "[shutdown({job_id})]: Finishing with overlay {:?}",
                    overlay.build_dir.display()
                );
                Self::finish_overlay(&job_id, &overlay).await;
            },
        ))
        .await;
    }
}

// Name of the image to run
// TODO: Make this configurable?
const BUSYBOX_DOCKER_IMAGE: &str = "busybox:stable-musl";
// Make sure sh doesn't exec the final command, since we need it to do
// init duties (reaping zombies). Also, because we kill -9 -1, that kills
// the sleep (it's not a builtin) so it needs to be a loop.
const DOCKER_SHELL_INIT: &str = "while true; do busybox sleep 365d && busybox true; done";

#[derive(Clone)]
pub struct DockerBuilder {
    containers: Arc<Mutex<HashMap<String, String>>>,
    job_queue: Arc<tokio::sync::Semaphore>,
}

impl DockerBuilder {
    // TODO: this should accept a unique string, e.g. inode of the tccache directory
    // having locked a pidfile, or at minimum should loudly detect other running
    // instances - pidfile in /tmp
    pub async fn new(job_queue: Arc<tokio::sync::Semaphore>) -> Result<Self> {
        tracing::info!("Creating docker builder");
        Ok(Self {
            containers: Default::default(),
            job_queue,
        })
    }

    async fn perform_build(
        job_id: &str,
        c_name: &str,
        toolchain_dir: &Path,
        CompileCommand {
            executable,
            arguments,
            env_vars,
            cwd,
        }: CompileCommand,
        output_paths: Vec<String>,
        inputs: Vec<u8>,
        job_queue: &tokio::sync::Semaphore,
    ) -> Result<BuildResult> {
        tracing::trace!("[perform_build({job_id})]: Compile environment: {env_vars:?}");
        tracing::trace!("[perform_build({job_id})]: Compile command: {executable:?} {arguments:?}");
        tracing::trace!("[perform_build({job_id})]: Output paths: {output_paths:?}");

        if output_paths.is_empty() {
            bail!("Output paths is empty");
        }

        // Do as much asyncio work as possible before acquiring a job slot

        fn bind_mount<P: AsRef<Path>>(prefix: &Path) -> impl FnMut(P) -> Vec<String> {
            let prefix = prefix.to_owned();
            move |h_path| {
                let h_path = h_path.as_ref();
                if let Ok(c_path) = h_path.strip_prefix(&prefix) {
                    let c_path = Path::new("/").join(c_path);
                    let h_path = h_path.display();
                    let c_path = c_path.display();
                    vec![
                        "--mount".into(),
                        format!("type=bind,src={h_path},dst={c_path}"),
                    ]
                } else {
                    vec![]
                }
            }
        }

        // Should automatically get deleted when host_temp goes out of scope
        let host_temp = tempfile::Builder::new().prefix("sccache_dist").tempdir()?;
        let host_root = host_temp.path();

        let cwd = Path::new(&cwd);
        let cwd_host = join_suffix(host_root, cwd);
        let tc_dir = format!("{}", toolchain_dir.display());

        // Canonicalize output path as either absolute or relative to cwd
        let output_paths_absolute = output_paths
            .iter()
            .map(|path| cwd.join(Path::new(path)))
            .collect::<Vec<_>>();

        // Collect host CWD, input, and output dir paths
        let host_bindmount_paths = {
            // Always create the CWD even if it's not in the inputs archive
            std::iter::once(cwd_host.as_path())
                // Output paths
                .chain(output_paths_absolute.iter().map(Path::new))
                // If it doesn't have a parent, nothing needs creating
                .filter_map(|path| path.parent().map(|p| join_suffix(host_root, p)))
                .unique()
                .collect::<Vec<_>>()
        };

        {
            // Bail early if job_queue is closed while this job is running
            drop(job_queue.acquire().await?);
            tracing::trace!("[perform_build({job_id})]: creating output directories");
            for path in host_bindmount_paths.iter() {
                tracing::trace!("[perform_build({job_id})]: creating dir: {path:?}");
                tokio::fs::create_dir_all(path)
                    .await
                    .context(format!("Failed to create output directory {path:?}"))?;
            }
        }

        {
            // Bail early if job_queue is closed while this job is running
            drop(job_queue.acquire().await?);
            tracing::trace!("[perform_build({job_id})]: creating docker container");
            let mut cmd = tokio::process::Command::new("docker");
            cmd.args(["run", "--init", "-d", "--name", c_name])
                // Mount output dirs
                .args(host_bindmount_paths.iter().flat_map(bind_mount(host_root)))
                .args([
                    BUSYBOX_DOCKER_IMAGE,
                    "busybox",
                    "sh",
                    "-c",
                    DOCKER_SHELL_INIT,
                ])
                .check_stdout_trim()
                .await
                .context("Failed to create docker container")?;
        }

        {
            // Bail early if job_queue is closed while this job is running
            drop(job_queue.acquire().await?);
            tracing::trace!("[perform_build({job_id})]: copying in toolchain");
            let mut cmd = tokio::process::Command::new("docker");
            cmd.arg("cp")
                .arg(format!("{tc_dir}/."))
                .arg(format!("{c_name}:/"))
                .check_run()
                .await
                .context("Failed to copy toolchain into container")?;
        }

        {
            // Bail early if job_queue is closed while this job is running
            drop(job_queue.acquire().await?);
            tracing::trace!("[perform_build({job_id})]: copying in inputs");
            let inputs_rdr = inputs.reader();
            let inputs_rdr = futures::io::AllowStdIo::new(inputs_rdr);
            let inputs_rdr = ZlibDecoderAsync::new(inputs_rdr);

            let mut cmd = tokio::process::Command::new("docker");
            cmd.arg("cp")
                .arg("-")
                .arg(format!("{c_name}:/"))
                .check_piped(|mut stdin| async move {
                    tokio::io::copy(&mut inputs_rdr.compat(), &mut stdin).await?;
                    Ok(())
                })
                .await
                .context("Failed to copy inputs tar into container")?;
        }

        let output: ProcessOutput = {
            // Guard compiling until we get a token from the job queue
            let _job_slot = job_queue.acquire().await?;

            tracing::trace!("[perform_build({job_id})]: creating compile command");

            // TODO: likely shouldn't perform the compile as root in the container
            let mut cmd = tokio::process::Command::new("docker");

            cmd.args(["exec"])
                // Run in `cwd`
                .arg("--workdir")
                .arg(cwd)
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
                // container name
                .arg(c_name)
                // Finally, the executable and arguments
                .arg(&executable)
                .args(arguments);

            tracing::trace!("[perform_build({job_id})]: performing compile");
            tracing::trace!("[perform_build({job_id})]: {:?}", cmd.as_std());

            cmd.output().await.context("Failed to compile")?.into()
        };

        let outputs = {
            // Bail early if job_queue is closed while this job is running
            drop(job_queue.acquire().await?);

            let mut outputs = vec![];

            if !output.success() {
                if output.exit() {
                    tracing::trace!("[perform_build({job_id})]: compile failure: {output:?}");
                } else {
                    // Warn on abnormal terminations (i.e. SIGTERM, SIGKILL)
                    tracing::warn!(
                        "[perform_build({job_id})]: {executable:?} terminated with {}",
                        output.desc()
                    );
                }
            } else {
                tracing::trace!("[perform_build({job_id})]: compile success: {output:?}");
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
            }

            outputs
        };

        Ok(BuildResult { output, outputs })
    }

    async fn finish_container(job_id: &str, c_name: &str) {
        if let Err(err) = tokio::process::Command::new("docker")
            .args(["rm", "-f", c_name])
            .check_run()
            .await
        {
            tracing::warn!(
                "[finish_container({job_id})]: Failed to remove docker container {c_name:?}: {err:#}",
            );
        }
    }
}

#[async_trait]
impl BuilderIncoming for DockerBuilder {
    // From Server
    async fn run_build(
        &self,
        job_id: &str,
        toolchain_dir: &Path,
        inputs: Vec<u8>,
        command: CompileCommand,
        outputs: Vec<String>,
    ) -> Result<BuildResult> {
        // Bail early if job_queue is closed while this job is running
        drop(self.job_queue.acquire().await?);

        tracing::debug!("[run_build({job_id})]: Performing build in container");
        let c_name = format!("sccache-builder-{job_id}");

        self.containers
            .lock()
            .await
            .insert(job_id.to_owned(), c_name.clone());

        let res = Self::perform_build(
            job_id,
            &c_name,
            toolchain_dir,
            command,
            outputs,
            inputs,
            self.job_queue.as_ref(),
        )
        .await;

        tracing::debug!("[run_build({job_id})]: Returning result");

        res.context("Failed to perform build")
    }

    async fn finish_build(&self, job_id: &str) {
        if let Some(c_name) = self.containers.lock().await.remove(job_id) {
            tracing::debug!("[finish_build({job_id})]: Removing container {c_name:?}");
            Self::finish_container(job_id, &c_name).await;
        }
    }

    async fn shutdown(&self) {
        self.job_queue.close();
        futures::future::join_all(self.containers.lock().await.drain().map(
            |(job_id, c_name)| async move {
                tracing::debug!("[shutdown({job_id})]: Removing container {c_name:?}");
                Self::finish_container(&job_id, &c_name).await;
            },
        ))
        .await;
    }
}
