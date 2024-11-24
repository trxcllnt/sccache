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

use anyhow::{bail, Context, Error, Result};
use async_trait::async_trait;
use bytes::Buf;
use flate2::read::GzDecoder;
use futures::lock::Mutex;
use sccache::dist::{
    BuildResult, BuilderIncoming, CompileCommand, JobId, OutputData, ProcessOutput, TcCache,
    Toolchain,
};
use sccache::lru_disk_cache::Error as LruError;
use std::collections::{hash_map, HashMap};
use std::hint;
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::ChildStdin;
use uuid::Uuid;

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

// Force remove the container
async fn pot_rm(cid: &str, pot_cmd: &Path) -> Result<()> {
    let mut cmd = tokio::process::Command::new(pot_cmd);
    cmd.args(&["destroy", "-F", "-p", cid])
        .check_run()
        .await
        .context("Failed to force delete container")
}

#[derive(Clone)]
pub struct PotBuilder {
    pot_fs_root: PathBuf,
    clone_from: String,
    pot_cmd: PathBuf,
    pot_clone_args: Vec<String>,
    image_map: Arc<Mutex<HashMap<Toolchain, String>>>,
    container_lists: Arc<Mutex<HashMap<Toolchain, Vec<String>>>>,
    cleanup_thread_count: Arc<AtomicUsize>,
    max_cleanup_thread_count: usize,
    jobserver: sccache::jobserver::Client,
}

impl PotBuilder {
    // TODO: this should accept a unique string, e.g. inode of the tccache directory
    // having locked a pidfile, or at minimum should loudly detect other running
    // instances - pidfile in /tmp
    pub async fn new(
        pot_fs_root: PathBuf,
        clone_from: String,
        pot_cmd: PathBuf,
        pot_clone_args: Vec<String>,
        jobserver: sccache::jobserver::Client,
    ) -> Result<Self> {
        tracing::info!("Creating pot builder");

        let ret = Self {
            jobserver,
            pot_fs_root,
            clone_from,
            pot_cmd,
            pot_clone_args,
            image_map: Arc::new(Mutex::new(HashMap::new())),
            container_lists: Arc::new(Mutex::new(HashMap::new())),
            cleanup_thread_count: Arc::new(AtomicUsize::new(0)),
            max_cleanup_thread_count: num_cpus::get() * 3,
        };
        ret.cleanup().await?;
        Ok(ret)
    }

    // This removes all leftover pots from previous runs
    async fn cleanup(&self) -> Result<()> {
        tracing::info!("Performing initial pot cleanup");
        let mut cmd = tokio::process::Command::new(&self.pot_cmd);
        let mut to_remove = cmd
            .args(&["ls", "-q"])
            .check_stdout_trim()
            .await
            .context("Failed to force delete container")?
            .split('\n')
            .filter(|a| a.starts_with("sccache-builder-") || a.starts_with("sccache-image-"))
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        to_remove.sort();
        for cid in to_remove {
            tracing::trace!("Removing pot {}", cid);
            if let Err(e) = pot_rm(&cid, &self.pot_cmd).await {
                tracing::warn!("Failed to remove container {}: {}", cid, e);
            }
        }
        tracing::info!("Completed initial pot cleanup");
        Ok(())
    }

    // If we have a spare running container, claim it and remove it from the available list,
    // otherwise try and create a new container (possibly creating the Pot image along
    // the way)
    async fn get_container(
        &self,
        job_id: JobId,
        tc: &Toolchain,
        tccache: &Mutex<TcCache>,
    ) -> Result<String> {
        let container = {
            let mut map = self.container_lists.lock().await;
            map.entry(tc.clone()).or_insert_with(Vec::new).pop()
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
                            tracing::info!("[get_container({})]: Creating pot image for {:?} (may block requests)", job_id, tc);
                            let image = Self::make_image(
                                job_id,
                                tc,
                                tccache,
                                &self.pot_fs_root,
                                &self.clone_from,
                                &self.pot_cmd,
                                &self.pot_clone_args,
                            )
                            .await?;
                            e.insert(image.clone());
                            image
                        }
                    }
                };
                Self::start_container(&image, &self.pot_cmd, &self.pot_clone_args).await
            }
        }
    }

    async fn clean_container(cid: &str) -> Result<()> {
        let mut cmd = tokio::process::Command::new("pot");
        cmd.args(&["stop", "-p", cid])
            .check_run()
            .await
            .context("Failed to stop container")?;

        let mut cmd = tokio::process::Command::new("pot");
        cmd.args(&["revert", "-p", cid])
            .check_run()
            .await
            .context("Failed to revert container")?;

        let mut cmd = tokio::process::Command::new("pot");
        cmd.args(&["start", "-p", cid])
            .check_run()
            .await
            .context("Failed to (re)start container")?;
        Ok(())
    }

    // Failing during cleanup is pretty unexpected, but we can still return the successful compile
    // TODO: if too many of these fail, we should mark this builder as faulty
    async fn finish_container(
        job_id: JobId,
        container_lists: Arc<Mutex<HashMap<Toolchain, Vec<String>>>>,
        tc: Toolchain,
        cid: String,
        pot_cmd: &PathBuf,
    ) {
        if let Err(e) = Self::clean_container(&cid).await {
            tracing::info!(
                "[finish_container({})]: Failed to clean container {}: {}",
                job_id,
                cid,
                e
            );
            if let Err(e) = pot_rm(&cid, pot_cmd).await {
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
        if let Some(entry) = container_lists.lock().await.get_mut(&tc) {
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
            if let Err(e) = pot_rm(&cid, pot_cmd).await {
                tracing::warn!(
                    "[finish_container({})]: Failed to remove container {}: {}",
                    job_id,
                    cid,
                    e
                );
            }
        }
    }

    async fn make_image(
        job_id: JobId,
        tc: &Toolchain,
        tccache: &Mutex<TcCache>,
        pot_fs_root: &Path,
        clone_from: &str,
        pot_cmd: &PathBuf,
        pot_clone_args: &[String],
    ) -> Result<String> {
        let imagename = format!("sccache-image-{}", &tc.archive_id);
        tracing::trace!(
            "[make_image({})]: Creating toolchain image: {}",
            job_id,
            imagename
        );
        let mut clone_args: Vec<&str> = ["clone", "-p", &imagename, "-P", clone_from].to_vec();
        clone_args.append(&mut pot_clone_args.iter().map(|s| s as &str).collect());
        let mut cmd = tokio::process::Command::new(pot_cmd);
        cmd.args(&clone_args)
            .check_run()
            .await
            .context("Failed to create pot container")?;

        let mut tccache = tccache.lock().await;
        let toolchain_rdr = match tccache.get(tc) {
            Ok(rdr) => rdr,
            Err(LruError::FileNotInCache) => {
                bail!("expected toolchain {}, but not available", tc.archive_id)
            }
            Err(e) => return Err(Error::from(e).context("failed to get toolchain from cache")),
        };

        tracing::trace!("[make_image({})]: Copying in toolchain", job_id);
        tar::Archive::new(GzDecoder::new(toolchain_rdr))
            .unpack(pot_fs_root.join("jails").join(&imagename).join("m"))
            .or_else(|e| {
                tracing::warn!(
                    "[make_image({})]: Failed to unpack toolchain: {:?}",
                    job_id,
                    e
                );
                tccache
                    .remove(tc)
                    .context("Failed to remove corrupt toolchain")?;
                Err(Error::from(e))
            })?;

        let mut cmd = tokio::process::Command::new(pot_cmd);
        cmd.args(&["snapshot", "-p", &imagename])
            .check_run()
            .await
            .context("Failed to snapshot container after build")?;

        Ok(imagename)
    }

    async fn start_container(
        image: &str,
        pot_cmd: &PathBuf,
        pot_clone_args: &[String],
    ) -> Result<String> {
        let cid = format!("sccache-builder-{}", Uuid::new_v4());
        let mut clone_args: Vec<&str> = ["clone", "-p", &cid, "-P", image].to_vec();
        clone_args.append(&mut pot_clone_args.iter().map(|s| s as &str).collect());
        let mut cmd = tokio::process::Command::new(pot_cmd);
        cmd.args(&clone_args)
            .check_run()
            .await
            .context("Failed to create pot container")?;

        let mut cmd = tokio::process::Command::new(pot_cmd);
        cmd.args(&["snapshot", "-p", &cid])
            .check_run()
            .await
            .context("Failed to snapshotpot container")?;

        let mut cmd = tokio::process::Command::new(pot_cmd);
        cmd.args(&["start", "-p", &cid])
            .check_run()
            .await
            .context("Failed to start container")?;

        Ok(cid.to_string())
    }

    async fn perform_build(
        job_id: JobId,
        compile_command: CompileCommand,
        mut inputs_rdr: std::pin::Pin<&mut (dyn tokio::io::AsyncRead + Send)>,
        output_paths: Vec<String>,
        cid: &str,
        pot_fs_root: &Path,
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

        tracing::trace!("[perform_build({})]: copying in inputs", job_id);
        // not elegant
        // Read into memory because we can't use asyncio in the thread below.
        let mut inputs_buf = vec![];
        inputs_rdr.read_to_end(&mut inputs_buf).await?;
        tar::Archive::new(inputs_buf.reader())
            .unpack(pot_fs_root.join("jails").join(cid).join("m"))
            .context("Failed to unpack inputs to pot")?;

        let CompileCommand {
            executable,
            arguments,
            env_vars,
            cwd,
        } = compile_command;
        let cwd = Path::new(&cwd);

        tracing::trace!("[perform_build({})]: creating output directories", job_id);
        assert!(!output_paths.is_empty());
        let mut cmd = tokio::process::Command::new("jexec");
        cmd.args(&[cid, "mkdir", "-p"]).arg(cwd);
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
        let mut cmd = tokio::process::Command::new("jexec");
        cmd.arg(cid);
        cmd.arg("env");
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
            cmd.arg(env);
        }
        let shell_cmd = "cd \"$1\" && shift && exec \"$@\"";
        cmd.args(&["sh", "-c", shell_cmd]);
        cmd.arg(&executable);
        cmd.arg(cwd);
        cmd.arg(executable);
        cmd.args(&arguments);
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
            let output = tokio::process::Command::new("jexec")
                .args(&[cid, "cat"])
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
impl BuilderIncoming for PotBuilder {
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
        // Guard invoking perform_build until we get a token from the jobserver
        let token = self.jobserver.acquire().await?;
        tracing::debug!(
            "[run_build({})]: Performing build with container {}",
            job_id,
            cid
        );
        let res = Self::perform_build(
            job_id,
            command,
            inputs_rdr,
            outputs,
            &cid,
            &self.pot_fs_root,
        )
        .await;
        // Drop the jobserver token
        drop(token);
        // Unwrap the result
        let res = res.context("Failed to perform build")?;
        tracing::debug!("[run_build({})]: Finishing with container {}", job_id, cid);
        let cloned = self.clone();
        let tc = tc;
        while cloned.cleanup_thread_count.fetch_add(1, Ordering::SeqCst)
            > self.max_cleanup_thread_count
        {
            cloned.cleanup_thread_count.fetch_sub(1, Ordering::SeqCst);
            hint::spin_loop();
        }
        let runtime = tokio::runtime::Handle::current();
        //
        // Don't await the spawn future so cleanup happens in the background.
        //
        // TODO: This seems like many background cleanup threads could occupy
        //       many of the threads in tokio's threadpool. Maybe this should
        //       be awaited? How expensive is `Self::finish_container()`?
        runtime.spawn(async move {
            Self::finish_container(job_id, cloned.container_lists, tc, cid, &cloned.pot_cmd).await;
            cloned.cleanup_thread_count.fetch_sub(1, Ordering::SeqCst);
        });
        tracing::debug!("[run_build({})]: Returning result", job_id);
        Ok(res)
    }
}
