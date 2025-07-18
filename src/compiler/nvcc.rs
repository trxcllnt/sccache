// Copyright 2016 Mozilla Foundation
// SPDX-FileCopyrightText: Copyright (c) 2023 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#![allow(unused_imports, dead_code, unused_variables)]

use crate::compiler::args::*;
use crate::compiler::c::{ArtifactDescriptor, CCompilerImpl, CCompilerKind, ParsedArguments};
use crate::compiler::gcc::ArgData::*;
use crate::compiler::{
    self, gcc, get_compiler_info, write_temp_file, CCompileCommand, Cacheable, CompileCommand,
    CompileCommandImpl, CompilerArguments, Language,
};
use crate::mock_command::{
    exit_status, CommandChild, CommandCreator, CommandCreatorSync, ProcessOutput, ProcessStatus,
    RunCommand,
};
use crate::util::{run_input_output, OsStrExt};
use crate::{counted_array, dist, protocol, server, SCCACHE_TMPDIR};
use async_trait::async_trait;
use bytes::Buf;
use fs::File;
use fs_err as fs;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use log::Level::Trace;
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::future::{Future, IntoFuture};
use std::io::{self, BufRead, Read, Write};
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::{env, process};
use which::which_in;

use crate::errors::*;

static IS_VALID_LINE_RE: Lazy<Regex> = regex_static::lazy_regex!(r"^#\$ (.*)$");
static IS_ENVVAR_LINE_RE: Lazy<Regex> = regex_static::lazy_regex!(r"^([_A-Z]+)=(.*)$");
static HAS_SM_IN_NAME_RE: Lazy<Regex> = regex_static::lazy_regex!(r"^(.*).sm_([0-9A-Za-z]+).(.*)$");
static HAS_COMPUTE_IN_NAME_RE: Lazy<Regex> =
    regex_static::lazy_regex!(r"^(.*).compute_([0-9A-Za-z]+).(.*)$");
static ARG_HAS_FILE_WITH_EXTENSION_RE: Lazy<Regex> = regex_static::lazy_regex!(r"-.*=(.*)");

/// A unit struct on which to implement `CCompilerImpl`.
#[derive(Clone, Debug)]
pub enum NvccHostCompiler {
    Gcc,
    Msvc,
    Nvhpc,
}

#[derive(Clone, Debug)]
pub struct Nvcc {
    pub host_compiler: NvccHostCompiler,
    pub host_compiler_version: Option<String>,
    pub version: Option<String>,
}

#[async_trait]
impl CCompilerImpl for Nvcc {
    fn kind(&self) -> CCompilerKind {
        CCompilerKind::Nvcc
    }
    fn plusplus(&self) -> bool {
        false
    }
    fn version(&self) -> Option<String> {
        let nvcc_ver = self.version.clone().unwrap_or_default();
        let host_ver = self.host_compiler_version.clone().unwrap_or_default();
        let both_ver = [nvcc_ver, host_ver]
            .iter()
            .filter(|x| !x.is_empty())
            .join("-");
        if both_ver.is_empty() {
            None
        } else {
            Some(both_ver)
        }
    }
    fn parse_arguments(
        &self,
        arguments: &[OsString],
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
    ) -> CompilerArguments<ParsedArguments> {
        let mut arguments = arguments.to_vec();

        if let Some(flags) = env_vars
            .iter()
            .find(|(k, _)| k == "NVCC_PREPEND_FLAGS")
            .and_then(|(_, p)| p.to_str())
        {
            arguments = shlex::split(flags)
                .unwrap_or_default()
                .iter()
                .map(|s| s.clone().into_arg_os_string())
                .chain(arguments.iter().cloned())
                .collect::<Vec<_>>();
        }

        if let Some(flags) = env_vars
            .iter()
            .find(|(k, _)| k == "NVCC_APPEND_FLAGS")
            .and_then(|(_, p)| p.to_str())
        {
            arguments.extend(
                shlex::split(flags)
                    .unwrap_or_default()
                    .iter()
                    .map(|s| s.clone().into_arg_os_string()),
            );
        }

        let parsed_args = gcc::parse_arguments(
            &arguments,
            cwd,
            (&gcc::ARGS[..], &ARGS[..]),
            false,
            self.kind(),
        );

        match parsed_args {
            CompilerArguments::Ok(mut parsed_args) => {
                let compile_flag = parsed_args.compilation_flag.as_os_str().into();

                match compile_flag {
                    NvccCompileFlag::Executable => { /* no compile flag is valid */ }
                    _ => {
                        // Add the compilation flag to `parsed_args.common_args` so
                        // it's considered when computing the hash.
                        //
                        // Consider the following cases:
                        //  $ sccache nvcc x.cu -o x.bin
                        //  $ sccache nvcc x.cu -o x.cu.o -c
                        //  $ sccache nvcc x.cu -o x.ptx -ptx
                        //  $ sccache nvcc x.cu -o x.cubin -cubin
                        //
                        // The preprocessor output for all four are identical, so
                        // without including the compilation flag in the hasher's
                        // inputs, the same hash would be generated for all four.
                        parsed_args
                            .common_args
                            .push(parsed_args.compilation_flag.clone());
                    }
                }

                // Canonicalize here so the absolute path to the input is in the
                // preprocessor output instead of paths relative to `cwd`.
                //
                // Since cicc's input is the post-processed source run through cudafe++'s
                // transforms, its cache key is sensitive to the preprocessor output. The
                // preprocessor embeds the name of the input file in comments, so without
                // canonicalizing here, cicc will get cache misses on otherwise identical
                // inputs that should produce cache hits.
                let input = cwd.join(&parsed_args.input);
                if input.exists() {
                    parsed_args.input = dunce::canonicalize(input).unwrap();
                }

                CompilerArguments::Ok(parsed_args)
            }
            CompilerArguments::CannotCache(_, _) | CompilerArguments::NotCompilation => parsed_args,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn preprocess<T>(
        &self,
        creator: &T,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
        may_dist: bool,
        rewrite_includes_only: bool,
        _preprocessor_cache_mode: bool,
    ) -> Result<ProcessOutput>
    where
        T: CommandCreatorSync,
    {
        let mut env_vars = env_vars
            .iter()
            .filter(|(k, _)| k != "NVCC_PREPEND_FLAGS" && k != "NVCC_APPEND_FLAGS")
            .cloned()
            .collect::<Vec<_>>();

        let language = match parsed_args.language {
            Language::C => Ok("c"),
            Language::Cxx => Ok("c++"),
            Language::ObjectiveC => Ok("objective-c"),
            Language::ObjectiveCxx => Ok("objective-c++"),
            Language::Cuda => Ok("cu"),
            _ => Err(anyhow!("PCH not supported by nvcc")),
        }?;

        let (arguments, output_path, _, _) = parsed_to_nvcc_args(cwd, parsed_args);

        // Chain dependency generation and the preprocessor command to emulate a `proper` front end
        if !parsed_args.dependency_args.is_empty() {
            run_input_output(
                {
                    // NVCC doesn't support generating both the dependency information
                    // and the preprocessor output at the same time. So if we have
                    // need for both, we need separate compiler invocations
                    let mut dependency_cmd = creator.clone().new_command_sync(executable);
                    dependency_cmd
                        .current_dir(cwd)
                        .env_clear()
                        .envs(env_vars.clone())
                        .args(&parsed_args.preprocessor_args)
                        .args(&parsed_args.common_args)
                        .arg("-x")
                        .arg(language)
                        .arg(&parsed_args.input)
                        .args(
                            &parsed_args
                                .dependency_args
                                .iter()
                                .map(|arg| match arg.to_str().unwrap_or_default() {
                                    "-MD" | "--generate-dependencies-with-compile" => "-M",
                                    "-MMD" | "--generate-nonsystem-dependencies-with-compile" => {
                                        "-MM"
                                    }
                                    arg => arg,
                                })
                                // protect against duplicate -M and -MM flags after transform
                                .unique()
                                .collect::<Vec<_>>(),
                        );
                    if log_enabled!(Trace) {
                        trace!(
                            "[{}]: dependencies command: {:?}",
                            output_path.display(),
                            dependency_cmd
                        );
                    }
                    dependency_cmd
                },
                None,
            )
            .await?;
        }

        // Use `nvcc --dryrun` to extract all the host and device preprocessor commands.
        // Run each preprocessor command, hash the output, then concatenate all the hashes.
        // We don't care what the preprocessor output is (we can't compile it), we care that
        // it's unique to this input.
        //
        // We have to run all of them, because `nvcc -E` isn't sufficient to capture all
        // possible modifications, e.g.
        // ```
        //
        // #ifndef __CUDA_ARCH__
        // static_assert(false, "not CUDA compiler")
        // #endif
        //
        // __global__ void kernel() {
        //   #if __CUDA_ARCH__ > 700
        //   sm70_logic();
        //   #elseif __CUDA_ARCH__ > 600
        //   sm60_logic();
        //   #else
        //   fallback_logic();
        //   #endif
        // }
        // ```

        let preprocessor_output = {
            let runtime = tokio::runtime::Handle::current();

            let preprocessor_flag = match self.host_compiler {
                NvccHostCompiler::Msvc => "-P",
                _ => "-E",
            }
            .to_owned();

            let preprocessor_commands = select_nvcc_subcommands(
                creator,
                executable,
                cwd,
                &mut env_vars,
                &arguments,
                |_, args| args.contains(&preprocessor_flag),
                &self.host_compiler,
                &output_path,
            )
            .await?
            .into_iter()
            .map(|(_, exe, mut args)| {
                match self.host_compiler {
                    NvccHostCompiler::Msvc => {
                        // Remove -Fi so output goes to stdout
                        if let Some(idx) = args.iter().position(|x| x.starts_with("-Fi")) {
                            args.splice(idx..idx + 1, []);
                        }
                        // Rewrite `-P` to `-E -EP`
                        // msvc requires the `-EP` flag to elide line numbers
                        if let Some(idx) = args.iter().position(|x| x == "-P") {
                            args.splice(idx..idx + 1, []);
                        }
                        if !args.contains(&String::from("-E")) {
                            args.push("-E".into());
                        }
                        if !args.contains(&String::from("-EP")) {
                            args.push("-EP".into());
                        }
                    }
                    NvccHostCompiler::Gcc => {
                        // Remove -o so output goes to stdout
                        if let Some(idx) = args.iter().position(|x| x == "-o") {
                            args.splice(idx..idx + 2, []);
                        }
                        // Add `-P` to elide line numbers
                        // nvc/nvc++ don't support eliding line numbers
                        // non-NVHPC host compilers are presumed to match `gcc` behavior
                        if !args.contains(&String::from("-P")) {
                            args.push("-P".into())
                        }
                    }
                    NvccHostCompiler::Nvhpc => {
                        // Remove -o so output goes to stdout
                        if let Some(idx) = args.iter().position(|x| x == "-o") {
                            args.splice(idx..idx + 2, []);
                        }
                    }
                }

                NvccGeneratedSubcommand {
                    exe,
                    args,
                    cwd: cwd.to_owned(),
                    env_vars: env_vars.clone(),
                    cacheable: Cacheable::No,
                }
            })
            .map(|cmd| run_nvcc_subcommand(cmd, creator, &output_path))
            .map(|fut| async {
                let mut res = fut.await.unwrap_or_else(error_to_output);
                if res.success() {
                    res.stderr = vec![];
                    res.stdout = runtime
                        .spawn_blocking(move || {
                            crate::util::Digest::reader_sync(res.stdout.reader()).unwrap()
                        })
                        .await
                        .unwrap()
                        .into_bytes()
                }
                res
            });

            let outputs = futures::future::join_all(preprocessor_commands).await;

            outputs
                .into_iter()
                .fold(ProcessOutput::default(), |output, result| {
                    match (output.success(), result.success()) {
                        // Propagate errors
                        (false, true) => output,
                        (true, false) => result,
                        _ => aggregate_output(output, result),
                    }
                })
        };

        if preprocessor_output.success() {
            Ok(preprocessor_output)
        } else {
            Err(ProcessError(preprocessor_output).into())
        }
    }

    fn generate_compile_commands<T>(
        &self,
        path_transformer: &mut dist::PathTransformer,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
        rewrite_includes_only: bool,
        hash_key: &str,
    ) -> Result<(
        Box<dyn CompileCommand<T>>,
        Option<dist::CompileCommand>,
        Cacheable,
    )>
    where
        T: CommandCreatorSync,
    {
        generate_compile_commands(
            parsed_args,
            executable,
            cwd,
            env_vars,
            &self.host_compiler,
            hash_key,
        )
        .map(|(command, dist_command, cacheable)| {
            (CCompileCommand::new(command), dist_command, cacheable)
        })
    }
}

fn generate_compile_commands(
    parsed_args: &ParsedArguments,
    executable: &Path,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
    host_compiler: &NvccHostCompiler,
    hash_key: &str,
) -> Result<(NvccCompileCommand, Option<dist::CompileCommand>, Cacheable)> {
    let env_vars = env_vars
        .iter()
        .filter(|(k, _)| k != "NVCC_PREPEND_FLAGS" && k != "NVCC_APPEND_FLAGS")
        .cloned()
        .collect::<Vec<_>>();

    let (arguments, output_path, keep_dir, num_parallel) = parsed_to_nvcc_args(cwd, parsed_args);

    // Build nvcc's internal files in `$TMPDIR/$hash_key` so the paths are
    // stable across compilations. This is important because this path ends
    // up in the preprocessed output, so using random tmpdir paths leads to
    // erroneous cache misses.
    let out_dir = SCCACHE_TMPDIR.as_ref()?.join("nvcc").join({
        // Combine `hash_key` with the output path in case
        // the same file is concurrently built to separate
        // output paths.
        let mut m = crate::util::Digest::new();
        m.update(hash_key.as_bytes());
        m.update(cwd.join(&output_path).as_os_str().as_encoded_bytes());
        m.finish()
    });

    fs::create_dir_all(&out_dir).ok();

    let compile_flag = parsed_args.compilation_flag.as_os_str().into();

    // Only cache compilations that produce object files.
    let cacheable = if compile_flag == NvccCompileFlag::Device {
        Cacheable::Yes
    } else {
        Cacheable::No
    };

    let command = NvccCompileCommand {
        out_dir,
        keep_dir,
        num_parallel,
        executable: executable.to_owned(),
        arguments,
        compile_flag,
        env_vars,
        cwd: cwd.to_owned(),
        host_compiler: host_compiler.clone(),
        // Only here so we can include it in logs
        output_path,
    };

    Ok((command, None, cacheable))
}

fn parsed_to_nvcc_args(
    cwd: &Path,
    parsed_args: &ParsedArguments,
) -> (
    Vec<OsString>,   // nvcc arguments
    PathBuf,         // output path
    Option<PathBuf>, // keep_dir
    usize,           // num_parallel
) {
    let mut unhashed_args = parsed_args.unhashed_args.clone();

    let keep_dir = {
        let mut keep = false;
        let mut keep_dir = None;
        // Remove all occurrences of `-keep` and `-keep-dir`, but save the keep dir for copying to later
        loop {
            if let Some(idx) = unhashed_args
                .iter()
                .position(|x| x == "-keep-dir" || x == "--keep-dir")
            {
                let dir = PathBuf::from(unhashed_args[idx + 1].as_os_str());
                let dir = cwd.join(dir);
                unhashed_args.splice(idx..(idx + 2), []);
                keep_dir = Some(dir);
                continue;
            } else if let Some(idx) = unhashed_args.iter().position(|x| {
                x == "-keep" || x == "--keep" || x == "-save-temps" || x == "--save-temps"
            }) {
                keep = true;
                unhashed_args.splice(idx..(idx + 1), []);
                if keep_dir.is_none() {
                    keep_dir = Some(cwd.to_path_buf())
                }
                continue;
            }
            break;
        }
        // Match nvcc behavior where intermediate files are kept if:
        // * Only `-keep` is specified (files copied to cwd)
        // * Both `-keep -keep-dir=<dir>` are specified (files copied to <dir>)
        // nvcc does _not_ keep intermediate files if `-keep-dir=` is specified without `-keep`
        keep.then_some(()).and(keep_dir)
    };

    let num_parallel = {
        let mut num_parallel = 1;
        // Remove all occurrences of `-t=` or `--threads` because it's incompatible with --dryrun
        // Prefer the last occurrence of `-t=` or `--threads` to match nvcc behavior
        loop {
            if let Some(idx) = unhashed_args.iter().position(|x| x.starts_with("-t")) {
                let arg = unhashed_args.get(idx);
                if let Some(arg) = arg.and_then(|arg| arg.to_str()) {
                    let range = if arg.contains('=') {
                        3..arg.len()
                    } else {
                        2..arg.len()
                    };
                    if let Ok(arg) = arg[range].parse::<usize>() {
                        num_parallel = arg;
                    }
                }
                unhashed_args.splice(idx..(idx + 1), []);
                continue;
            }
            if let Some(idx) = unhashed_args.iter().position(|x| x == "--threads") {
                let arg = unhashed_args.get(idx + 1);
                if let Some(arg) = arg.and_then(|arg| arg.to_str()) {
                    if let Ok(arg) = arg.parse::<usize>() {
                        num_parallel = arg;
                    }
                }
                unhashed_args.splice(idx..(idx + 2), []);
                continue;
            }
            break;
        }
        num_parallel
    };

    let mut args = vec![];

    if let Some(lang) = gcc::language_to_gcc_arg(parsed_args.language) {
        args.extend(vec!["-x".into(), lang.into()])
    }

    let output = &parsed_args
        .outputs
        .get("obj")
        .context("Missing object file output")
        .unwrap()
        .path;

    args.extend(vec![
        "-o".into(),
        // Canonicalize the output path if the compile flag indicates we won't
        // produce an object file. Since we run cicc and ptxas in a temp dir,
        // but we run the host compiler in `cwd` (the dir from which sccache was
        // executed), cicc/ptxas `-o` argument should point at the real out path
        // that's potentially relative to `cwd`.
        if matches!(
            parsed_args.compilation_flag.as_os_str().into(),
            NvccCompileFlag::Device | NvccCompileFlag::Executable
        ) {
            output.clone().into()
        } else {
            cwd.join(output).into()
        },
    ]);

    args.extend_from_slice(&parsed_args.preprocessor_args);
    args.extend_from_slice(&unhashed_args);
    args.extend_from_slice(&parsed_args.common_args);
    args.extend_from_slice(&parsed_args.arch_args);
    if parsed_args.double_dash_input {
        args.push("--".into());
    }

    args.push(parsed_args.input.as_path().into());

    (args, output.clone(), keep_dir, num_parallel)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum NvccCompileFlag {
    Cubin,
    Device,
    Executable,
    Fatbin,
    LtoIR,
    OptixIR,
    Ptx,
}

impl From<&OsStr> for NvccCompileFlag {
    fn from(flag: &OsStr) -> Self {
        match flag.to_str() {
            // compile to executable
            Some("") => NvccCompileFlag::Executable,
            // compile to object
            Some("-c") | Some("--compile")
            // compile to object with -rdc=true
            | Some("-dc") | Some("--device-c")
            // compile to object with -rdc=false
            | Some("-dw") | Some("--device-w")
            => NvccCompileFlag::Device,
            Some("-cubin") | Some("--cubin") => NvccCompileFlag::Cubin,
            Some("-fatbin") | Some("--fatbin") => NvccCompileFlag::Fatbin,
            Some("-ltoir") | Some("--ltoir") => NvccCompileFlag::LtoIR,
            Some("-optix-ir") | Some("--optix-ir") => NvccCompileFlag::OptixIR,
            Some("-ptx") | Some("--ptx") => NvccCompileFlag::Ptx,
            _ => unreachable!()
        }
    }
}

#[derive(Clone, Debug)]
struct NvccCompileCommand {
    pub out_dir: PathBuf,
    pub keep_dir: Option<PathBuf>,
    pub num_parallel: usize,
    pub executable: PathBuf,
    pub arguments: Vec<OsString>,
    pub compile_flag: NvccCompileFlag,
    pub env_vars: Vec<(OsString, OsString)>,
    pub cwd: PathBuf,
    pub host_compiler: NvccHostCompiler,
    pub output_path: PathBuf,
}

#[async_trait]
impl CompileCommandImpl for NvccCompileCommand {
    fn get_executable(&self) -> PathBuf {
        self.executable.clone()
    }
    fn get_arguments(&self) -> Vec<OsString> {
        self.arguments.clone()
    }
    fn get_env_vars(&self) -> Vec<(OsString, OsString)> {
        self.env_vars.clone()
    }
    fn get_cwd(&self) -> PathBuf {
        self.cwd.clone()
    }

    async fn execute<T>(
        &self,
        service: &server::SccacheService<T>,
        creator: &T,
    ) -> Result<ProcessOutput>
    where
        T: CommandCreatorSync,
    {
        let NvccCompileCommand {
            out_dir,
            keep_dir,
            num_parallel,
            executable,
            arguments,
            compile_flag,
            env_vars,
            cwd,
            host_compiler,
            output_path,
        } = self;

        let (mut nvcc_internal_files, nvcc_subcommand_groups) =
            group_nvcc_subcommands_by_compilation_stage(
                creator,
                executable,
                arguments,
                compile_flag,
                cwd,
                out_dir.as_path(),
                keep_dir.clone(),
                env_vars,
                host_compiler,
                output_path,
            )
            .await?;

        let mut maybe_keep_temps_then_clean = || {
            // Move and/or delete nvcc's internal files.
            //
            // If the caller passed `-keep` or `-keep-dir`, copy the internal
            // files to the requested location. We do this because we override
            // `-keep` and `-keep-dir` in our `nvcc --dryrun` call.
            //
            // Renames the files back to the original names nvcc gave them.
            let temps_kept = {
                if let Some(keep_dir) = keep_dir.as_ref() {
                    // Ensure `--keep-dir` exists
                    fs::create_dir_all(keep_dir)?;

                    // Rename files back to original nvcc names
                    for (curr, prev) in nvcc_internal_files.drain() {
                        let src = out_dir.join(curr);
                        let dst = out_dir.join(prev);
                        if dst == src || !src.exists() {
                            continue;
                        }
                        trace!(
                            "[{}]: maybe_keep_temps_then_clean rename {src:?} -> {dst:?}",
                            output_path.display(),
                        );
                        fs::rename(src, dst)?;
                    }

                    // Move intermediate files to `--keep-dir`
                    if let Ok(entries) = fs::read_dir(out_dir) {
                        for entry in entries {
                            let entry = entry?;
                            let src = entry.path();
                            let dst = keep_dir.join(entry.file_name());
                            if dst == src || !src.exists() {
                                continue;
                            }
                            trace!(
                                "[{}]: maybe_keep_temps_then_clean move {src:?} -> {dst:?}",
                                output_path.display()
                            );
                            fs::rename(src, dst)?;
                        }
                    }
                }
                Ok(())
            };

            // Delete sccache internal intermediate files dir
            temps_kept
                .and(fs::remove_dir_all(out_dir))
                .map_err(anyhow::Error::new)
        };

        let mut output = ProcessOutput::default();

        let n = nvcc_subcommand_groups.len();
        let cuda_front_end_range = if n > 0 { 0..1 } else { 0..0 };
        let device_compile_range = if n > 2 { 1..n - 1 } else { 0..0 };

        let num_parallel = device_compile_range.len().min(*num_parallel).max(1);

        service.stats.lock().await.decrement_pending_compilations();

        let run_command_groups =
            |mut output: ProcessOutput, chunks: Vec<Vec<Vec<NvccGeneratedSubcommand>>>| async {
                for chunk in chunks {
                    let results = futures::future::join_all(chunk.into_iter().map(|commands| {
                        run_nvcc_subcommands_group(service, creator, cwd, commands, output_path)
                    }))
                    .await;

                    for result in results {
                        output = aggregate_output(output, result.unwrap_or_else(error_to_output));
                    }

                    if !output.success() {
                        output.stdout.shrink_to_fit();
                        output.stderr.shrink_to_fit();
                        return Err(anyhow!(ProcessError(output)));
                    }
                }
                Ok(output)
            };

        let mut cudafe_commands_group = nvcc_subcommand_groups;
        let mut device_compile_groups = cudafe_commands_group.split_off(cuda_front_end_range.len());
        let final_host_compiler_group = device_compile_groups.split_off(device_compile_range.len());

        // Run the `cudafe++` command group first
        if !cudafe_commands_group.is_empty() {
            output = run_command_groups(output, vec![cudafe_commands_group])
                .await
                .or_else(|err| maybe_keep_temps_then_clean().and(Err(err)))?;
        }

        // Compile multiple device architectures in parallel when `nvcc -t=N` is specified
        if !device_compile_groups.is_empty() {
            output = run_command_groups(output, {
                let mut chunks = vec![];
                let mut head = device_compile_groups;
                loop {
                    if head.is_empty() {
                        break;
                    }
                    let tail = head.split_off(head.len().min(num_parallel));
                    chunks.push(head);
                    head = tail;
                }
                chunks
            })
            .await
            .or_else(|err| maybe_keep_temps_then_clean().and(Err(err)))?;
        }

        // Run the final host compile and link command group
        if !final_host_compiler_group.is_empty() {
            output = run_command_groups(output, vec![final_host_compiler_group])
                .await
                .or_else(|err| maybe_keep_temps_then_clean().and(Err(err)))?;
        }

        output.stdout.shrink_to_fit();
        output.stderr.shrink_to_fit();
        maybe_keep_temps_then_clean()?;
        Ok(output)
    }
}

#[derive(Clone, Debug)]
pub struct NvccGeneratedSubcommand {
    pub exe: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env_vars: Vec<(OsString, OsString)>,
    pub cacheable: Cacheable,
}

#[allow(clippy::too_many_arguments)]
async fn group_nvcc_subcommands_by_compilation_stage<T>(
    creator: &T,
    executable: &Path,
    arguments: &[OsString],
    compile_flag: &NvccCompileFlag,
    cwd: &Path,
    out: &Path,
    keep_dir: Option<PathBuf>,
    env_vars: &[(OsString, OsString)],
    host_compiler: &NvccHostCompiler,
    output_path: &Path,
) -> Result<(HashMap<String, String>, Vec<Vec<NvccGeneratedSubcommand>>)>
where
    T: CommandCreatorSync,
{
    // Run `nvcc --dryrun` twice to ensure the commands are correct
    // relative to the directory where they're run.
    //
    // All the "nvcc" commands (cudafe++, cicc, ptxas, nvlink, fatbinary)
    // are run in the temp dir, so their arguments should be relative to
    // the temp dir, e.g. `cudafe++ [...] "x.cpp4.ii"`
    //
    // All the host compiler invocations are run in the original `cwd` where
    // sccache was invoked. Arguments will be relative to the cwd, except
    // any arguments that reference nvcc-generated files should be absolute
    // to the temp dir, e.g. `gcc -E [...] x.cu -o /out/dir/x.cpp4.ii`

    // Roughly equivalent to:
    // ```shell
    //   cat <(nvcc --dryrun --keep                                       \
    //       | nl -n ln -s ' ' -w 1                                       \
    //       | grep -P    "^[0-9]+ (cicc|ptxas|cudafe|nvlink|fatbinary)") \
    //                                                                    \
    //       <(nvcc --dryrun --keep --keep-dir /out/dir                   \
    //       | nl -n ln -s ' ' -w 1                                       \
    //       | grep -P -v "^[0-9]+ (cicc|ptxas|cudafe|nvlink|fatbinary)") \
    //                                                                    \
    //   | sort -k 1n
    // ```

    let mut env_vars_1 = env_vars.to_vec();
    let mut env_vars_2 = env_vars.to_vec();

    let is_nvcc_exe = |exe: &str, _: &[String]| {
        matches!(exe, "cicc" | "ptxas" | "cudafe++" | "nvlink" | "fatbinary")
    };

    let (mut nvcc_commands, mut host_commands) = futures::future::try_join(
        // Get the nvcc compile command lines with paths relative to `out`
        select_nvcc_subcommands(
            creator,
            executable,
            cwd,
            &mut env_vars_1,
            arguments,
            is_nvcc_exe,
            host_compiler,
            output_path,
        ),
        // Get the host compile command lines with paths relative to `cwd` and absolute paths to `out`
        select_nvcc_subcommands(
            creator,
            executable,
            cwd,
            &mut env_vars_2,
            &[arguments, &["--keep-dir".into(), out.into()][..]].concat(),
            |exe, args| !is_nvcc_exe(exe, args),
            host_compiler,
            output_path,
        ),
    )
    .await?;

    drop(env_vars_2);
    let env_vars = env_vars_1;

    //
    // Remap nvcc's generated file names to deterministic names.
    // nvcc generates different file names depending on whether it's compiling one vs. many archs.
    //
    // For example, both of these commands generate PTX for sm60, so we should get a cicc cache hit:
    // 1. `nvcc -x cu -c x.cu -o x.cu.o -gencode=arch=compute_60,code=[compute_60,sm_60]`
    // 2. `nvcc -x cu -c x.cu -o x.cu.o -gencode=arch=compute_60,code=[sm_60] -gencode=arch=compute_70,code=[compute_70,sm_70]`
    //
    // The first command generates:
    // ```
    // cicc --gen_c_file_name x.cudafe1.c \
    //      --stub_file_name x.cudafe1.stub.c \
    //      --gen_device_file_name x.cudafe1.gpu \
    //      -o x.ptx
    // ```
    //
    // The second command generates:
    // ```
    // cicc --gen_c_file_name x.compute_60.cudafe1.c \
    //      --stub_file_name x.compute_60.cudafe1.stub.c \
    //      --gen_device_file_name x.compute_60.cudafe1.gpu \
    //      -o x.compute_60.ptx
    // ```
    //
    // The second command yields a false-positive cache miss because the names are different.
    //
    // This matters because CI jobs will often compile .cu files in "many-arch" mode, but devs
    // who have just one GPU locally prefer to only compile for their specific GPU arch. It is
    // preferrable if they can reuse the PTX populated by "many-arch" CI jobs.
    //
    // So to avoid this, we detect these "single-arch" compilations and rewrite the names to
    // match what nvcc generates for "many-arch" compilations.
    //
    let mut nvcc_internal_files = HashMap::<String, String>::new();
    if let Some(arch) = find_last_compute_arch(&nvcc_commands) {
        host_commands = remap_generated_filenames(
            &arch,
            compile_flag,
            &host_commands,
            &mut nvcc_internal_files,
        );
        nvcc_commands = remap_generated_filenames(
            &arch,
            compile_flag,
            &nvcc_commands,
            &mut nvcc_internal_files,
        );
    }

    let output_flag = "-o".to_owned();
    let gen_module_id_file_flag = "--gen_module_id_file".to_owned();
    let gen_c_file_name_flag = "--gen_c_file_name".to_owned();
    let gen_device_file_name_flag = "--gen_device_file_name".to_owned();
    let module_id_file_name_flag = "--module_id_file_name".to_owned();
    let stub_file_name_flag = "--stub_file_name".to_owned();

    let mut cudafe_has_gen_module_id_file_flag = false;

    // Now zip the two lists of commands again by sorting on original line index.
    // Transform to tuples that include the dir in which each command should run.
    let mut all_commands = nvcc_commands
        .iter()
        // Run cudafe++, nvlink, cicc, ptxas, and fatbinary in `out`
        .map(|(idx, exe, args)| (idx, out, exe, args))
        .chain(
            host_commands
                .iter()
                // Run host preprocessing and compilation steps in `cwd`
                .map(|(idx, exe, args)| (idx, cwd, exe, args)),
        )
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .map(|(_, dir, exe, args)| (dir.to_owned(), exe.to_owned(), args.to_owned()))
        .collect::<Vec<_>>();

    // First pass over commands because in CTK < 12.0, `cudafe++` is at the end of the commands list,
    // but we need to set `cudafe_has_gen_module_id_file_flag` in order to adjust the cicc commands.
    for (dir, exe, args) in all_commands.iter_mut() {
        if let Some("cudafe++") = exe.file_stem().and_then(|s| s.to_str()) {
            // Fix for CTK < 12.0:
            // Add `--gen_module_id_file` if the cudafe++ args include `--module_id_file_name`
            if let Some(idx) = args.iter().position(|x| x == &module_id_file_name_flag) {
                if !args.contains(&gen_module_id_file_flag) {
                    // Insert `--gen_module_id_file` just before `--module_id_file_name` to match nvcc behavior
                    args.splice(idx..idx, [gen_module_id_file_flag.clone()]);
                }
            }
            cudafe_has_gen_module_id_file_flag = args.contains(&gen_module_id_file_flag);
        }
    }

    // Create groups of commands that should be run sequential relative to each other,
    // but can optionally be run in parallel to other groups if the user requested via
    // `nvcc --threads`.

    let preprocessor_flag = OsString::from(match host_compiler {
        NvccHostCompiler::Msvc => "-P",
        _ => "-E",
    });

    let mut cuda_front_end_group = Vec::<(ParsedArguments, NvccGeneratedSubcommand)>::new();
    let mut final_assembly_group = Vec::<(ParsedArguments, NvccGeneratedSubcommand)>::new();
    let mut device_compile_groups =
        HashMap::<PathBuf, Vec<(ParsedArguments, NvccGeneratedSubcommand)>>::new();

    fn get_device_compile_group<'a>(
        device_compile_groups: &'a mut HashMap<
            PathBuf,
            Vec<(ParsedArguments, NvccGeneratedSubcommand)>,
        >,
        path: &Path,
    ) -> Option<&'a mut Vec<(ParsedArguments, NvccGeneratedSubcommand)>> {
        if !device_compile_groups.contains_key(path) {
            trace!("Creating device compile group: {path:?}");
            device_compile_groups.insert(path.to_owned(), vec![]);
        } else {
            trace!("Retrieving device compile group: {path:?}");
        }
        device_compile_groups.get_mut(path)
    }

    let env_vars_no_preprocessor_cache_mode = env_vars
        .iter()
        .filter(|(key, _)| key != "SCCACHE_DIRECT")
        .chain([("SCCACHE_DIRECT".into(), "false".into())].iter())
        .cloned()
        .collect::<Vec<_>>();

    for (dir, exe, args) in all_commands.iter_mut() {
        if let Some((pa, env_vars, cacheable, group)) =
            match exe.file_stem().and_then(|s| s.to_str()) {
                // fatbinary and nvlink are not cacheable
                Some("fatbinary") | Some("nvlink") => Some((
                    parse_args_simple(args, dir),
                    env_vars_no_preprocessor_cache_mode.clone(),
                    Cacheable::No,
                    &mut final_assembly_group,
                )),
                // cicc and ptxas are cacheable
                Some("cicc") => Some(parse_args_simple(args, dir))
                    .map(|pa| {
                        fixup_cicc_args(
                            pa,
                            &keep_dir,
                            compile_flag,
                            cudafe_has_gen_module_id_file_flag,
                        )
                    })
                    .and_then(|pa| {
                        get_device_compile_group(&mut device_compile_groups, &pa.input)
                            .map(|group| (pa, group))
                    })
                    .map(|(pa, group)| {
                        (
                            pa,
                            env_vars_no_preprocessor_cache_mode.clone(),
                            Cacheable::Yes,
                            group,
                        )
                    }),
                Some("ptxas") => {
                    // ptxas ... <input>
                    // ptxas ... <input> -o <output>
                    Some(parse_args_simple(args, dir))
                        // Ignore ptxas invocations without an output or input
                        .filter(|pa| pa.outputs.contains_key("obj"))
                        .filter(|pa| !pa.input.as_os_str().is_empty())
                        .and_then(|pa| {
                            // Search for the prior corresponding cicc call
                            device_compile_groups
                                .values()
                                .find_map(|cmds| {
                                    cmds.iter().rev().find_map(|(cicc, _)| {
                                        cicc.outputs
                                            .get("obj")
                                            .filter(|o| o.path == pa.input)
                                            .map(|_| cicc.input.clone())
                                    })
                                })
                                // Otherwise create a new device compile group
                                .or_else(|| Some(pa.input.clone()))
                                .map(|input| (pa, input))
                        })
                        .and_then(|(pa, input)| {
                            get_device_compile_group(&mut device_compile_groups, &input)
                                .map(|group| (pa, group))
                        })
                        .map(|(pa, group)| {
                            (
                                pa,
                                env_vars_no_preprocessor_cache_mode.clone(),
                                Cacheable::Yes,
                                group,
                            )
                        })
                }
                // cudafe++ _must be_ cached, because the `.module_id` file is unique to each invocation (new in CTK 12.8)
                Some("cudafe++") => Some((
                    parse_args_simple(args, dir),
                    env_vars_no_preprocessor_cache_mode.clone(),
                    Cacheable::Yes,
                    &mut cuda_front_end_group,
                )),
                _ => {
                    Some(parse_args_simple(args, dir))
                        // All generated host compiler commands include one of these defines.
                        // If one of these isn't present, this command is either a new binary
                        // in the CTK that we don't know about, or a line like `rm x_dlink.reg.c`
                        // that nvcc generates in certain cases.
                        .filter(|pa| {
                            pa.common_args.iter().any(|arg| {
                                arg.starts_with("-D__CUDA_ARCH__")
                                    || arg.starts_with("-D__CUDA_ARCH_LIST__")
                                    || arg.starts_with("-D__CUDACC__")
                                    || arg.starts_with("-D__CUDACC_VER")
                                    || arg.starts_with("-D__CUDACC_RDC__")
                                    || arg.starts_with("-D__NVCC__")
                                    || arg.starts_with("-lcudart")
                                    || arg.starts_with("-lcudadevrt")
                            })
                        })
                        .and_then(|pa| {
                            pa.outputs
                                .get("obj")
                                .map(|o| o.path.clone())
                                .map(|o| (pa, o))
                        })
                        .and_then(|(pa, output)| {
                            // Each host preprocessor step represents the start of a new command group
                            if pa.common_args.contains(&preprocessor_flag) {
                                // If the output file ends with...
                                // * .cpp4.ii - cudafe++ input
                                // * .cpp1.ii - cicc/ptxas input
                                if output
                                    .file_name()
                                    .expect("output is a named file")
                                    .ends_with(".cpp4.ii")
                                {
                                    Some((
                                        pa,
                                        env_vars.clone(),
                                        Cacheable::No,
                                        &mut cuda_front_end_group,
                                    ))
                                } else {
                                    get_device_compile_group(&mut device_compile_groups, &output)
                                        .map(|group| (pa, env_vars.clone(), Cacheable::No, group))
                                }
                            } else if compile_flag == &NvccCompileFlag::Executable {
                                // If building an executable, cache the host compiler objects.
                                Some((
                                    pa,
                                    env_vars.clone(),
                                    Cacheable::Yes,
                                    &mut final_assembly_group,
                                ))
                            } else {
                                // Otherwise don't cache, because the object will be cached using
                                // the outer nvcc hash_key.
                                Some((
                                    pa,
                                    env_vars
                                        .iter()
                                        .chain(&[("SCCACHE_NO_CACHE".into(), "1".into())])
                                        .cloned()
                                        .collect::<Vec<_>>(),
                                    Cacheable::Yes,
                                    &mut final_assembly_group,
                                ))
                            }
                        })
                }
            }
        {
            if log_enabled!(log::Level::Trace) {
                trace!(
                    "[{}]: transformed nvcc command: {}",
                    output_path.display(),
                    format!(
                        "{} && {}",
                        shlex::try_join(["cd", &format!("{}", dir.display())])?,
                        shlex::try_join(
                            std::iter::once(&exe.clone().into_os_string())
                                .chain(pa.common_args.iter())
                                .map(|s| s.to_str().unwrap())
                        )?
                    )
                );
            }

            let args = dist::osstrings_to_strings(&pa.common_args).unwrap_or_default();

            group.push((
                pa,
                NvccGeneratedSubcommand {
                    exe: exe.clone(),
                    args,
                    cwd: dir.to_owned(),
                    env_vars,
                    cacheable,
                },
            ));
        }
    }

    let command_groups = std::iter::empty()
        .chain([cuda_front_end_group].into_iter())
        .chain(device_compile_groups.into_values())
        .chain([final_assembly_group].into_iter())
        .map(|xs| xs.into_iter().map(|(_, c)| c).collect::<Vec<_>>())
        .collect::<Vec<_>>();

    Ok((nvcc_internal_files, command_groups))
}

fn is_nvcc_exe(exe: &OsStr) -> bool {
    matches!(
        exe.to_str(),
        Some("cicc") | Some("ptxas") | Some("cudafe++") | Some("nvlink") | Some("fatbinary")
    )
}

counted_array!(static SIMPLE_ARGS: [ArgInfo<gcc::ArgData>; _] = [
    take_arg!("-Fi", PathBuf, Concatenated, Output),
    take_arg!("-Fo", PathBuf, Concatenated, Output),
    take_arg!("-o", PathBuf, Separated, Output),
    take_arg!("-olto", OsString, Separated, PassThrough),
]);

fn parse_args_simple(args: &[String], cwd: &Path) -> ParsedArguments {
    let (input, output, args) = parse_input_output(
        dist::strings_to_osstrings(args).into_iter(),
        &SIMPLE_ARGS[..],
        |data| match data {
            Output(p) => Some(p),
            _ => None,
        },
    );

    let input = input.map(|p| cwd.join(p)).unwrap_or_default();
    let mut outputs = HashMap::new();
    if let Some(path) = output.map(|p| cwd.join(p)) {
        outputs.insert(
            "obj",
            ArtifactDescriptor {
                path,
                optional: false,
                must_be_non_empty: false,
            },
        );
    }

    trace!("parse_args_simple: input={input:?}, outputs={outputs:?} args={args:?}");

    ParsedArguments {
        input,
        outputs,
        common_args: args,
        ..Default::default()
    }
}

fn fixup_cicc_args(
    mut pa: ParsedArguments,
    keep_dir: &Option<PathBuf>,
    compile_flag: &NvccCompileFlag,
    cudafe_has_gen_module_id_file_flag: bool,
) -> ParsedArguments {
    let args = &mut pa.common_args;

    // Fix for CTK < 12.0:
    if let Some(idx) = args.iter().position(|x| x == "--module_id_file_name") {
        // Remove `--gen_module_id_file` if cudafe++ already does it
        if cudafe_has_gen_module_id_file_flag {
            if let Some(idx) = args.iter().position(|x| x == "--gen_module_id_file") {
                args.splice(idx..idx + 1, []);
            }
        } else if !args.contains(&OsString::from("--gen_module_id_file")) {
            // Otherwise if `--module_id_file_name` exists, insert `--gen_module_id_file`
            args.splice(idx..idx, ["--gen_module_id_file".into()]);
        }
    }

    // Add or remove these flags:
    // * `--gen_c_file_name test_a.compute_XX.cudafe1.c`
    // * `--stub_file_name test_a.compute_XX.cudafe1.stub.c`
    // * `--gen_device_file_name test_a.compute_XX.cudafe1.gpu` (optional)
    //
    // This ensures the same `cicc` command is generated regardless
    // of whether the compilation flag is `-c`, `-ptx`, or `-cubin`

    // pa.input is absolute, so use the original form here instead
    let input = pa
        .input
        .file_name()
        .and_then(|name| name.to_str())
        .expect("input is a named file");
    let mut input_index = args
        .iter()
        .position(|arg| arg.ends_with(input))
        .expect("input is in args");
    // foo.compute_52.cpp1.ii -> foo.compute_52.cpp1
    let input = Path::new(&args[input_index]);
    let input = if input.extension().filter(|&e| e == "ii").is_some() {
        input.with_extension("")
    } else {
        input.to_owned()
    };

    let is_compile = matches!(
        compile_flag,
        NvccCompileFlag::Device | NvccCompileFlag::Executable
    );

    for (flag, path) in [
        (
            "--stub_file_name",
            is_compile.then(|| input.with_extension("cudafe1.stub.c")),
        ),
        // Only generate `.cudafe1.{c,gpu}` files when `-keep` is on.
        //
        // nvcc's cicc commands to output them by default,
        // but as far as I can tell, they aren't by any other part
        // of the compilation pipeline.
        //
        // These files can be large (`.gpu` can be hundreds of MiB),
        // making them really expensive to cache and/or ship around
        // sccache-dist servers unnecessarily.
        //
        // `nvcc -h` says users can do `nvcc -c x.gpu -o x.cubin`
        // to compile a `.gpu` file directly to a cubin with cicc,
        // but I'm not sure how the user would get the `.gpu` file
        // without doing `nvcc -c x.cu -o x.o -keep` first.
        (
            "--gen_c_file_name",
            keep_dir
                .as_ref()
                .filter(|_| is_compile)
                .map(|_| input.with_extension("cudafe1.c")),
        ),
        (
            "--gen_device_file_name",
            keep_dir
                .as_ref()
                .filter(|_| is_compile)
                .map(|_| input.with_extension("cudafe1.gpu")),
        ),
    ] {
        match (path, args.iter().position(|x| x == flag)) {
            (Some(path), None) => {
                // Insert the flag and value before the input path
                args.splice(input_index..input_index, [flag.into(), path.into()]);
                input_index += 2;
            }
            (None, Some(flag_index)) => {
                // Remove the flag and value
                args.splice(flag_index..flag_index + 2, []);
                if flag_index < input_index {
                    input_index -= 2;
                }
            }
            _ => {}
        }
    }

    pa
}

#[allow(clippy::too_many_arguments)]
async fn select_nvcc_subcommands<T, F>(
    creator: &T,
    executable: &Path,
    cwd: &Path,
    env_vars: &mut Vec<(OsString, OsString)>,
    arguments: &[OsString],
    select_subcommand: F,
    host_compiler: &NvccHostCompiler,
    output_path: &Path,
) -> Result<Vec<(usize, PathBuf, Vec<String>)>>
where
    F: Fn(&str, &[String]) -> bool,
    T: CommandCreatorSync,
{
    if log_enabled!(log::Level::Trace) {
        trace!(
            "[{}]: nvcc dryrun command: {}",
            output_path.display(),
            format!(
                "{} && {}",
                shlex::try_join(["cd", &format!("{}", cwd.display())])?,
                shlex::try_join(
                    std::iter::once(&format!("{}", executable.display()))
                        .chain(&dist::osstrings_to_strings(arguments).unwrap_or_default())
                        .map(|s| s.as_str())
                        .chain(["--dryrun", "--keep"])
                )?
            )
        );
    }

    let mut nvcc_dryrun_cmd = creator.clone().new_command_sync(executable);

    nvcc_dryrun_cmd
        .args(&[arguments, &["--dryrun".into(), "--keep".into()][..]].concat())
        .env_clear()
        .current_dir(cwd)
        .envs(env_vars.to_vec());

    let nvcc_dryrun_output = run_input_output(nvcc_dryrun_cmd, None).await?;

    let mut dryrun_env_vars = Vec::<(OsString, OsString)>::new();
    let mut dryrun_env_vars_re_map = HashMap::<String, regex::Regex>::new();

    let mut lines = Vec::<(usize, PathBuf, Vec<String>)>::new();

    #[cfg(unix)]
    let reader = std::io::BufReader::new(&nvcc_dryrun_output.stderr[..]);
    #[cfg(windows)]
    let reader = std::io::BufReader::new(&nvcc_dryrun_output.stdout[..]);

    for pair in reader.lines().enumerate() {
        let (idx, line) = pair;
        // Select lines that match the `#$ ` prefix from nvcc --dryrun
        let line = match select_valid_dryrun_lines(&IS_VALID_LINE_RE, &line?) {
            Ok(line) => line,
            // Ignore lines that don't start with `#$ `. For some reason, nvcc
            // on Windows prints the name of the input file without the prefix
            Err(err) => continue,
        };

        let maybe_exe_and_args = fold_env_vars_or_split_into_exe_and_args(
            &IS_ENVVAR_LINE_RE,
            &mut dryrun_env_vars,
            &mut dryrun_env_vars_re_map,
            cwd,
            &line,
            host_compiler,
        )?;

        let (exe, args) = match maybe_exe_and_args {
            Some(exe_and_args) => exe_and_args,
            _ => continue,
        };

        match exe.file_stem().and_then(|s| s.to_str()) {
            None => continue,
            Some(exe_name) => {
                if select_subcommand(exe_name, &args) {
                    lines.push((idx, exe, args));
                }
            }
        }
    }

    for pair in dryrun_env_vars {
        env_vars.splice(
            if let Some(idx) = env_vars.iter().position(|(k, _)| *k == pair.0) {
                idx..idx + 1
            } else {
                env_vars.len()..env_vars.len()
            },
            [pair],
        );
    }

    Ok(lines)
}

fn select_valid_dryrun_lines(re: &Regex, line: &str) -> Result<String> {
    match re.captures(line) {
        Some(caps) => {
            let (_, [rest]) = caps.extract();
            Ok(rest.to_string())
        }
        _ => Err(anyhow!("nvcc error: {:?}", line)),
    }
}

fn fold_env_vars_or_split_into_exe_and_args(
    re: &Regex,
    env_vars: &mut Vec<(OsString, OsString)>,
    env_var_re_map: &mut HashMap<String, regex::Regex>,
    cwd: &Path,
    line: &str,
    host_compiler: &NvccHostCompiler,
) -> Result<Option<(PathBuf, Vec<String>)>> {
    fn envvar_in_shell_format(var: &str) -> String {
        if cfg!(target_os = "windows") {
            format!("%{var}%") // %CICC_PATH%
        } else {
            format!("${var}") // $CICC_PATH
        }
    }

    fn envvar_in_shell_format_re(var: &str) -> Regex {
        Regex::new(
            &(if cfg!(target_os = "windows") {
                regex::escape(&envvar_in_shell_format(var))
            } else {
                regex::escape(&envvar_in_shell_format(var)) + r"[^\w]"
            }),
        )
        .unwrap()
    }

    // Intercept the environment variable lines and add them to the env_vars list
    if let Some(var) = re.captures(line) {
        let (_, [var, val]) = var.extract();

        env_var_re_map
            .entry(var.to_owned())
            .or_insert_with_key(|var| envvar_in_shell_format_re(var));

        env_vars.push((var.into(), val.into()));

        return Ok(None);
    }

    // The rest of the lines are subcommands, so parse into a vec of [cmd, args..]

    let mut line = if cfg!(target_os = "windows") {
        let line = line.replace("\"\"", "\"");
        match host_compiler {
            NvccHostCompiler::Msvc => line.replace(" -E ", " -P ").replace(" > ", " -Fi"),
            _ => line.to_owned(),
        }
    } else {
        line.to_owned()
    };

    // Expand envvars in nvcc subcommands, i.e. "$CICC_PATH/cicc ..." or "%CICC_PATH%/cicc"
    if let Some(env_vars) = dist::osstring_tuples_to_strings(env_vars) {
        for (var, val) in env_vars {
            if let Some(re) = env_var_re_map.get(&var) {
                if re.is_match(&line) {
                    line = line.replace(&envvar_in_shell_format(&var), &val);
                }
            }
        }
    }

    trace!("nvcc subcommand: {line}");

    let args = match shlex::split(&line) {
        Some(args) => args,
        None => return Err(anyhow!("Could not parse shell line")),
    };

    let (exe, args) = match args.split_first() {
        Some(exe_and_args) => exe_and_args,
        None => return Err(anyhow!("Could not split shell line")),
    };

    let env_path = env_vars
        .iter()
        .find(|(k, _)| k == "PATH")
        .map(|(_, p)| p.to_owned())
        .unwrap();

    let exe = which_in(exe, env_path.into(), cwd)?;

    Ok(Some((exe.clone(), args.to_vec())))
}

fn find_last_compute_arch(lines: &[(usize, PathBuf, Vec<String>)]) -> Option<String> {
    for (_, _, args) in lines.iter().rev() {
        if let Some(idx) = args.iter().position(|arg| arg == "-arch") {
            if let Some(val) = args.get(idx + 1) {
                if let Some((_, arch)) = val.split_once('_') {
                    return Some(arch.to_owned());
                }
            }
        }
    }
    None
}

fn remap_generated_filenames(
    last_arch: &str,
    compile_flag: &NvccCompileFlag,
    lines: &[(usize, PathBuf, Vec<String>)],
    nvcc_internal_files: &mut HashMap<String, String>,
) -> Vec<(usize, PathBuf, Vec<String>)> {
    let extensions = [
        "_dlink.fatbin.c",
        "_dlink.fatbin",
        "_dlink.o",
        "_dlink.reg.c",
        ".cpp1.ii",
        ".cpp4.ii",
        ".cubin",
        ".cudafe1.c",
        ".cudafe1.cpp",
        ".cudafe1.gpu",
        ".cudafe1.stub.c",
        ".fatbin.c",
        ".fatbin",
        ".module_id",
        ".ltoir",
        ".optixir",
        ".ptx",
    ];

    let mut extensions_to_rename = vec![
        ".cpp1.ii",
        ".cudafe1.c",
        ".cudafe1.cpp",
        ".cudafe1.gpu",
        ".cudafe1.stub.c",
    ];

    match compile_flag {
        // Rewrite PTX names if the compile flag is `-cubin` or `-ltoir`
        NvccCompileFlag::Cubin | NvccCompileFlag::LtoIR => {
            extensions_to_rename.push(".ptx");
        }
        // Rewrite both PTX and cubin names if the compile flag is `-c` or `-fatbin`
        NvccCompileFlag::Device | NvccCompileFlag::Executable | NvccCompileFlag::Fatbin => {
            extensions_to_rename.push(".ptx");
            extensions_to_rename.push(".cubin");
            extensions_to_rename.push(".ltoir");
            extensions_to_rename.push(".optixir");
        }
        _ => {}
    }

    lines
        .iter()
        .map(|(idx, exe, args)| {
            (
                *idx,
                exe.clone(),
                args.iter()
                    .map(|arg| {
                        // Special case for MSVC's preprocess output file name flag
                        let arg_is_msvc_preprocessor_output = arg.starts_with("-Fi");

                        let arg = if arg_is_msvc_preprocessor_output {
                            arg.trim_start_matches("-Fi").to_owned()
                        } else {
                            arg.to_owned()
                        };

                        // If the argument doesn't start with `-` and is a file that
                        // ends in one of these extensions, rename the file to a
                        // stable name that includes the compute architecture.
                        let maybe_extension = if !arg.starts_with('-') {
                            extensions.iter().find(|ext| arg.ends_with(*ext)).copied()
                        } else {
                            None
                        };

                        // If the argument is a file that ends in one of the above extensions:
                        // * If it's our first time seeing this file, compute a stable name for it
                        // * If we've seen this file before, lookup its stable name in the hash map
                        //
                        // This ensures stable names are in cudafe++ output and #include directives,
                        // eliminating one source of false-positive cache misses.
                        let arg = match maybe_extension {
                            // nvcc generates different cubin names under different conditions:
                            // 1. `x.cubin` with one `-gencode` arg and *not* embedding sm_XX PTX
                            // 2. `x.sm_XX.cubin` with one `-gencode` arg and embedding sm_XX PTX
                            // 2. `x.compute_XX.cubin` with multiple `-gencode` args and *not* embedding sm_XX PTX
                            // 3. `x.compute_XX.sm_XX.cubin` with multiple `-gencode` args and embedding sm_XX PTX
                            //
                            // Since the output cubin is identical, rewrite the first three forms
                            // to match the fourth form so we get more cache hits.
                            Some(".cubin") => {
                                nvcc_internal_files
                                    .entry(arg)
                                    .or_insert_with_key(|arg| {
                                        if !extensions_to_rename
                                            .iter()
                                            .any(|ext| arg.ends_with(*ext))
                                        {
                                            return arg.to_owned();
                                        }
                                        let mut path = PathBuf::from(arg);

                                        let cubin_arch = path
                                            .file_name()
                                            .and_then(|name| name.to_str())
                                            .and_then(|name| {
                                                let mut pair = if HAS_SM_IN_NAME_RE.is_match(name) {
                                                    name.split(".sm_")
                                                } else if HAS_COMPUTE_IN_NAME_RE.is_match(name) {
                                                    name.split(".compute_")
                                                } else {
                                                    return None;
                                                };
                                                // Ignore everything before `.sm_` or `.compute_`
                                                let _ = pair.next().unwrap();
                                                // Take everything after `.sm_` or `_.compute`, i.e. `{arch}.cubin`
                                                let s = pair.next().unwrap();
                                                // This is the arch number
                                                let (s, _) = s.split_once('.').unwrap();
                                                Some(s)
                                            })
                                            .unwrap_or(last_arch)
                                            .to_owned();

                                        // Add the `sm_{arch}` component if necessary
                                        if let Some(name) = path
                                            .file_name()
                                            .and_then(|name| name.to_str())
                                            .and_then(|name| {
                                                (!HAS_SM_IN_NAME_RE.is_match(name)).then_some(name)
                                            })
                                        {
                                            // test_a.cubin -> test_a
                                            // test_a.compute_60.cubin -> (test_a.compute_60, cubin)
                                            let name = name.strip_suffix(".cubin").unwrap();
                                            // test_a.cubin -> test_a.sm_60.cubin
                                            // test_a.compute_60.cubin -> test_a.compute_60.sm_60.cubin
                                            let name = format!("{name}.sm_{cubin_arch}.cubin");
                                            path.set_file_name(name);
                                        }

                                        // Add the `compute_{arch}` component if necessary
                                        if let Some(name) = path
                                            .file_name()
                                            .and_then(|name| name.to_str())
                                            .and_then(|name| {
                                                (!HAS_COMPUTE_IN_NAME_RE.is_match(name))
                                                    .then_some(name)
                                            })
                                        {
                                            // test_a.sm_60.cubin -> test_a
                                            let ext = format!(".sm_{cubin_arch}.cubin");
                                            let name = name.strip_suffix(&ext).unwrap();
                                            // test_a.sm_60.cubin -> test_a.compute_60.sm_60.cubin
                                            let name = format!("{name}.compute_{cubin_arch}{ext}");
                                            path.set_file_name(name);
                                        }
                                        path.into_os_string().into_string().unwrap()
                                    })
                                    .to_owned()
                            }
                            Some(ext) => {
                                nvcc_internal_files
                                    .entry(arg)
                                    .or_insert_with_key(|arg| {
                                        if !extensions_to_rename
                                            .iter()
                                            .any(|ext| arg.ends_with(*ext))
                                        {
                                            return arg.to_owned();
                                        }
                                        let mut path = PathBuf::from(arg);
                                        // Add the `compute_{arch}` component if necessary
                                        if let Some(name) = path
                                            .file_name()
                                            .and_then(|name| name.to_str())
                                            .and_then(|name| {
                                                (!HAS_COMPUTE_IN_NAME_RE.is_match(name))
                                                    .then_some(name)
                                            })
                                        {
                                            // test_a.cudafe1.c -> test_a
                                            let name = name.strip_suffix(ext).unwrap();
                                            // test_a.cudafe1.c -> test_a.compute_60.cudafe1.c
                                            let name = format!("{name}.compute_{last_arch}{ext}");
                                            path.set_file_name(name);
                                        }
                                        path.into_os_string().into_string().unwrap()
                                    })
                                    .to_owned()
                            }
                            None => {
                                // If the argument isn't a file name with one of our extensions,
                                // it may _reference_ files we've renamed. Go through and replace
                                // all old names with their new stable names.
                                //
                                // Sort by string length descending so we don't accidentally replace
                                // `test_a.cudafe1.cpp` with the new name for `test_a.cudafe1.c`.
                                //
                                // For example, if we have these renames:
                                //
                                //   test_a.cudafe1.cpp -> test_a.compute_70.cudafe1.cpp
                                //   test_a.cudafe1.c   -> test_a.compute_70.cudafe1.c
                                //
                                // `test_a.cudafe1.cpp` should be replaced with
                                // `test_a.compute_70.cudafe1.cpp`, not
                                // `test_a.compute_70.cudafe1.c`
                                //
                                let mut arg = arg.clone();
                                for (old, new) in nvcc_internal_files
                                    .iter()
                                    .sorted_by(|a, b| b.0.len().cmp(&a.0.len()))
                                {
                                    arg = arg.replace(old, new);
                                }
                                arg
                            }
                        };

                        if arg_is_msvc_preprocessor_output {
                            format!("-Fi{arg}")
                        } else {
                            arg
                        }
                    })
                    .collect::<Vec<_>>()
                    .iter()
                    .map(|arg| {
                        // If the argument matches the form `-<arg>=<val>` and `val` is a
                        // filename or path that ends in one of these extensions, extract
                        // and track the filename as an internal nvcc file
                        if let Some(groups) = ARG_HAS_FILE_WITH_EXTENSION_RE.captures(arg.as_str())
                        {
                            let (_, [name]) = groups.extract();
                            if !nvcc_internal_files.contains_key(name)
                                && extensions.iter().any(|ext| name.ends_with(*ext))
                            {
                                nvcc_internal_files.insert(name.to_owned(), name.to_owned());
                            }
                        }
                        arg.to_owned()
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>()
}

async fn run_nvcc_subcommand<T>(
    cmd: NvccGeneratedSubcommand,
    creator: &T,
    output_path: &Path,
) -> Result<ProcessOutput>
where
    T: CommandCreatorSync,
{
    let NvccGeneratedSubcommand {
        exe,
        args,
        cwd,
        env_vars,
        ..
    } = cmd;

    if log_enabled!(log::Level::Trace) {
        trace!(
            "[{}]: run_nvcc_subcommand: {}",
            output_path.display(),
            format!(
                "{} && {}",
                shlex::try_join(["cd", &format!("{}", cwd.display())])?,
                shlex::try_join(
                    std::iter::once(&format!("{}", exe.display()))
                        .chain(args.iter())
                        .map(|s| s.as_str())
                )?
            )
        );
    }

    let mut cmd = creator.clone().new_command_sync(exe);

    cmd.args(&args)
        .current_dir(cwd)
        .env_clear()
        .envs(env_vars.to_vec());

    run_input_output(cmd, None).await
}

async fn run_nvcc_subcommands_group<T>(
    service: &server::SccacheService<T>,
    creator: &T,
    cwd: &Path,
    commands: Vec<NvccGeneratedSubcommand>,
    output_path: &Path,
) -> Result<ProcessOutput>
where
    T: CommandCreatorSync,
{
    let mut output = ProcessOutput::default();

    for cmd in commands {
        let out = match cmd.cacheable {
            Cacheable::No => run_nvcc_subcommand(cmd, creator, output_path)
                .await
                .unwrap_or_else(error_to_output),
            Cacheable::Yes => {
                if log_enabled!(log::Level::Trace) {
                    trace!(
                        "[{}]: run_commands_sequential: {}",
                        output_path.display(),
                        format!(
                            "{} && {}",
                            shlex::try_join(["cd", &format!("{}", cmd.cwd.display())])?,
                            shlex::try_join(
                                std::iter::once(&format!("{}", cmd.exe.display()))
                                    .chain(cmd.args.iter())
                                    .map(|s| s.as_str())
                            )?
                        )
                    );
                }

                let NvccGeneratedSubcommand {
                    exe,
                    args,
                    cwd,
                    env_vars,
                    ..
                } = &cmd;

                let srvc = service.clone();
                let args = dist::strings_to_osstrings(args);

                match srvc
                    .compiler_info(exe.clone(), cwd.clone(), &args, env_vars)
                    .await
                {
                    Err(err) => error_to_output(err),
                    Ok(compiler) => match compiler.parse_arguments(&args, cwd, env_vars) {
                        CompilerArguments::NotCompilation => {
                            run_nvcc_subcommand(cmd, creator, output_path)
                                .await
                                .unwrap_or_else(error_to_output)
                        }
                        CompilerArguments::CannotCache(why, extra_info) => {
                            error_to_output(extra_info.map_or_else(
                                || anyhow!("Cannot cache({}): {:?} {:?}", why, exe, args),
                                |desc| {
                                    anyhow!("Cannot cache({}, {}): {:?} {:?}", why, desc, exe, args)
                                },
                            ))
                        }
                        CompilerArguments::Ok(hasher) => srvc
                            .start_compile_task(compiler, hasher, args, cmd.cwd, cmd.env_vars)
                            .await
                            .map_or_else(error_to_output, result_to_output),
                    },
                }
            }
        };

        output = aggregate_output(output, out);

        if !output.success() {
            break;
        }
    }

    Ok(output)
}

fn aggregate_output(lhs: ProcessOutput, rhs: ProcessOutput) -> ProcessOutput {
    ProcessOutput::new(
        std::cmp::max(!lhs.success() as i64, !rhs.success() as i64),
        [lhs.stdout, rhs.stdout].concat(),
        [lhs.stderr, rhs.stderr].concat(),
    )
}

fn error_to_output(err: Error) -> ProcessOutput {
    match err.downcast::<ProcessError>() {
        Ok(ProcessError(out)) => out,
        Err(err) => ProcessOutput::new(1, vec![], err.to_string().into_bytes()),
    }
}

fn result_to_output(res: protocol::CompileFinished) -> ProcessOutput {
    ProcessOutput::new(
        !res.output.success() as i64,
        res.output.stdout,
        res.output.stderr,
    )
}

counted_array!(pub static ARGS: [ArgInfo<gcc::ArgData>; _] = [
    //todo: refactor show_includes into dependency_args
    take_arg!("--Ofast-compile", OsString, CanBeSeparated, PassThrough),
    take_arg!("--Ofast-compile=", OsString, Concatenated, PassThrough),
    take_arg!("--Werror", OsString, CanBeSeparated('='), PreprocessorArgument),
    take_arg!("--archive-options options", OsString, CanBeSeparated('='), PassThrough),
    flag!("--compile", DoCompilation),
    take_arg!("--compiler-bindir", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--compiler-options", OsString, CanBeSeparated('='), PreprocessorArgument),
    take_arg!("--compress-mode", OsString, CanBeSeparated, PassThrough),
    take_arg!("--compress-mode=", OsString, Concatenated, PassThrough),
    flag!("--cubin", DoCompilation),
    flag!("--cuda", NotCompilationFlag),
    take_arg!("--default-stream", OsString, CanBeSeparated('='), PassThrough),
    flag!("--device-c", DoCompilation),
    flag!("--device-debug", PassThroughFlag),
    flag!("--device-link", NotCompilationFlag),
    flag!("--device-w", DoCompilation),
    take_arg!("--diag-suppress", OsString, CanBeSeparated('='), PassThrough),
    flag!("--dlink-time-opt", PassThroughFlag),
    take_arg!("--dopt", OsString, CanBeSeparated, PassThrough),
    take_arg!("--dopt=", OsString, Concatenated, PassThrough),
    flag!("--expt-extended-lambda", PreprocessorArgumentFlag),
    flag!("--expt-relaxed-constexpr", PreprocessorArgumentFlag),
    flag!("--extended-lambda", PreprocessorArgumentFlag),
    flag!("--extensible-whole-program", PassThroughFlag),
    flag!("--fatbin", DoCompilation),
    take_arg!("--fdevice-time-trace", OsString, CanBeSeparated, TooHard),
    take_arg!("--fdevice-time-trace=", OsString, Concatenated, TooHard),
    flag!("--gen-opt-lto", PassThroughFlag),
    take_arg!("--generate-code", OsString, CanBeSeparated('='), PassThrough),
    flag!("--generate-dependencies-with-compile", NeedDepTarget),
    flag!("--generate-nonsystem-dependencies-with-compile", NeedDepTarget),
    take_arg!("--gpu-architecture", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--gpu-code", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--include-path", PathBuf, CanBeSeparated('='), PreprocessorArgumentPath),
    flag!("--keep", UnhashedFlag),
    take_arg!("--keep-dir", OsString, CanBeSeparated('='), Unhashed),
    take_arg!("--linker-options", OsString, CanBeSeparated('='), PassThrough),
    flag!("--lto", NotCompilationFlag),
    flag!("--ltoir", DoCompilation),
    take_arg!("--maxrregcount", OsString, CanBeSeparated('='), PassThrough),
    flag!("--no-compress", PassThroughFlag),
    flag!("--no-host-device-initializer-list", PreprocessorArgumentFlag),
    take_arg!("--nvlink-options", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--options-file", PathBuf, CanBeSeparated('='), ExtraHashFile),
    flag!("--optix-ir", DoCompilation),
    flag!("--profile", PassThroughFlag),
    flag!("--ptx", DoCompilation),
    take_arg!("--ptxas-options", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--relocatable-device-code", OsString, CanBeSeparated('='), PreprocessorArgument),
    flag!("--save-temps", UnhashedFlag),
    take_arg!("--split-compile", OsString, CanBeSeparated, Unhashed),
    take_arg!("--split-compile-extended", OsString, CanBeSeparated, Unhashed),
    take_arg!("--split-compile-extended=", OsString, Concatenated, Unhashed),
    take_arg!("--split-compile=", OsString, Concatenated, Unhashed),
    take_arg!("--system-include", PathBuf, CanBeSeparated('='), PreprocessorArgumentPath),
    take_arg!("--threads", OsString, CanBeSeparated('='), Unhashed),
    take_arg!("--time", OsString, CanBeSeparated, TooHard),
    take_arg!("--time=", OsString, Concatenated, TooHard),
    take_arg!("--x", OsString, CanBeSeparated('='), Language),

    flag!("-G", PassThroughFlag),
    take_arg!("-Ofc", OsString, CanBeSeparated, PassThrough),
    take_arg!("-Ofc=", OsString, Concatenated, PassThrough),
    take_arg!("-Werror", OsString, CanBeSeparated('='), PreprocessorArgument),
    take_arg!("-Xarchive", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-Xcompiler", OsString, CanBeSeparated('='), PreprocessorArgument),
    take_arg!("-Xlinker", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-Xnvlink", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-Xptxas", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-arch", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-ccbin", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-code", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-compress-mode", OsString, CanBeSeparated, PassThrough),
    take_arg!("-compress-mode=", OsString, Concatenated, PassThrough),
    flag!("-cubin", DoCompilation),
    flag!("-cuda", NotCompilationFlag),
    flag!("-dc", DoCompilation),
    take_arg!("-default-stream", OsString, CanBeSeparated('='), PassThrough),
    flag!("-dlink", NotCompilationFlag),
    flag!("-dlto", PassThroughFlag),
    take_arg!("-dopt", OsString, CanBeSeparated, PassThrough),
    take_arg!("-dopt=", OsString, Concatenated, PassThrough),
    flag!("-dw", DoCompilation),
    flag!("-ewp", PassThroughFlag),
    flag!("-expt-extended-lambda", PreprocessorArgumentFlag),
    flag!("-expt-relaxed-constexpr", PreprocessorArgumentFlag),
    flag!("-extended-lambda", PreprocessorArgumentFlag),
    flag!("-fatbin", DoCompilation),
    take_arg!("-fdevice-time-trace", OsString, CanBeSeparated, TooHard),
    take_arg!("-fdevice-time-trace=", OsString, Concatenated, TooHard),
    flag!("-gen-opt-lto", PassThroughFlag),
    take_arg!("-gencode", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-isystem", PathBuf, CanBeSeparated('='), PreprocessorArgumentPath),
    flag!("-keep", UnhashedFlag),
    take_arg!("-keep-dir", OsString, CanBeSeparated('='), Unhashed),
    flag!("-lto", PassThroughFlag),
    flag!("-ltoir", DoCompilation),
    take_arg!("-maxrregcount", OsString, CanBeSeparated('='), PassThrough),
    flag!("-no-compress", PassThroughFlag),
    flag!("-nohdinitlist", PreprocessorArgumentFlag),
    flag!("-optix-ir", DoCompilation),
    flag!("-pg", PassThroughFlag),
    flag!("-ptx", DoCompilation),
    take_arg!("-rdc", OsString, CanBeSeparated('='), PreprocessorArgument),
    flag!("-save-temps", UnhashedFlag),
    take_arg!("-split-compile", OsString, CanBeSeparated, Unhashed),
    take_arg!("-split-compile-extended", OsString, CanBeSeparated, Unhashed),
    take_arg!("-split-compile-extended=", OsString, Concatenated, Unhashed),
    take_arg!("-split-compile=", OsString, Concatenated, Unhashed),
    take_arg!("-t", OsString, CanBeSeparated, Unhashed),
    take_arg!("-t=", OsString, Concatenated, Unhashed),
    take_arg!("-time", OsString, CanBeSeparated, TooHard),
    take_arg!("-time=", OsString, Concatenated, TooHard),
    take_arg!("-x", OsString, CanBeSeparated('='), Language),
]);

#[cfg(test)]
mod test {
    use super::*;
    use crate::compiler::gcc;
    use crate::compiler::*;
    use crate::mock_command::*;
    use crate::test::utils::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn parse_arguments_gcc(arguments: Vec<String>) -> CompilerArguments<ParsedArguments> {
        let arguments = arguments.iter().map(OsString::from).collect::<Vec<_>>();
        Nvcc {
            host_compiler: NvccHostCompiler::Gcc,
            host_compiler_version: None,
            version: None,
        }
        .parse_arguments(&arguments, ".".as_ref(), &[])
    }
    fn parse_arguments_msvc(arguments: Vec<String>) -> CompilerArguments<ParsedArguments> {
        let arguments = arguments.iter().map(OsString::from).collect::<Vec<_>>();
        Nvcc {
            host_compiler: NvccHostCompiler::Msvc,
            host_compiler_version: None,
            version: None,
        }
        .parse_arguments(&arguments, ".".as_ref(), &[])
    }
    fn parse_arguments_nvc(arguments: Vec<String>) -> CompilerArguments<ParsedArguments> {
        let arguments = arguments.iter().map(OsString::from).collect::<Vec<_>>();
        Nvcc {
            host_compiler: NvccHostCompiler::Nvhpc,
            host_compiler_version: None,
            version: None,
        }
        .parse_arguments(&arguments, ".".as_ref(), &[])
    }

    macro_rules! parses {
        ( $( $s:expr ),* ) => {
            match parse_arguments_gcc(vec![ $( $s.to_string(), )* ]) {
                CompilerArguments::Ok(a) => a,
                o => panic!("Got unexpected parse result: {:?}", o),
            }
        }
    }
    macro_rules! parses_msvc {
        ( $( $s:expr ),* ) => {
            match parse_arguments_msvc(vec![ $( $s.to_string(), )* ]) {
                CompilerArguments::Ok(a) => a,
                o => panic!("Got unexpected parse result: {:?}", o),
            }
        }
    }
    macro_rules! parses_nvc {
        ( $( $s:expr ),* ) => {
            match parse_arguments_nvc(vec![ $( $s.to_string(), )* ]) {
                CompilerArguments::Ok(a) => a,
                o => panic!("Got unexpected parse result: {:?}", o),
            }
        }
    }

    #[test]
    fn test_parse_arguments_simple_c() {
        let a = parses!("-c", "foo.c", "-o", "foo.o");
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::C, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-c"], a.common_args);
    }

    #[test]
    fn test_parse_arguments_simple_cu_gcc() {
        let a = parses!("-c", "foo.cu", "-o", "foo.o");
        assert_eq!(Some("foo.cu"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-c"], a.common_args);
    }

    #[test]
    fn test_parse_arguments_simple_cu_nvc() {
        let a = parses_nvc!("-c", "foo.cu", "-o", "foo.o");
        assert_eq!(Some("foo.cu"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-c"], a.common_args);
    }

    fn test_parse_arguments_simple_cu_msvc() {
        let a = parses_msvc!("-c", "foo.cu", "-o", "foo.o");
        assert_eq!(Some("foo.cu"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-c"], a.common_args);
    }

    #[test]
    fn test_parse_arguments_ccbin_no_path() {
        let a = parses!("-ccbin=gcc", "-c", "foo.cu", "-o", "foo.o");
        assert_eq!(Some("foo.cu"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-ccbin", "gcc", "-c"], a.common_args);
    }

    #[test]
    fn test_parse_arguments_ccbin_dir() {
        let a = parses!("-ccbin=/usr/bin/", "-c", "foo.cu", "-o", "foo.o");
        assert_eq!(Some("foo.cu"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-ccbin", "/usr/bin/", "-c"], a.common_args);
    }

    #[test]
    fn test_parse_threads_argument_simple_cu() {
        let a = parses!(
            "-t1",
            "-t=2",
            "-t",
            "3",
            "--threads=1",
            "--threads=2",
            "-c",
            "foo.cu",
            "-o",
            "foo.o"
        );
        assert_eq!(Some("foo.cu"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(
            ovec!["-t1", "-t=2", "-t3", "--threads", "1", "--threads", "2"],
            a.unhashed_args
        );
    }

    #[test]
    fn test_parse_arguments_simple_c_as_cu() {
        let a = parses!("-x", "cu", "-c", "foo.c", "-o", "foo.o");
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-c"], a.common_args);
    }

    #[test]
    fn test_parse_arguments_dc_compile_flag() {
        let a = parses!("-x", "cu", "-dc", "foo.c", "-o", "foo.o");
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_eq!(Some("-dc"), a.compilation_flag.to_str());
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-dc"], a.common_args);
    }

    #[test]
    fn test_parse_arguments_fatbin_compile_flag() {
        let a = parses!("-x", "cu", "-fatbin", "foo.c", "-o", "foo.o");
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_eq!(Some("-fatbin"), a.compilation_flag.to_str());
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-fatbin"], a.common_args);
    }

    #[test]
    fn test_parse_arguments_cubin_compile_flag() {
        let a = parses!("-x", "cu", "-cubin", "foo.c", "-o", "foo.o");
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_eq!(Some("-cubin"), a.compilation_flag.to_str());
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-cubin"], a.common_args);
    }

    #[test]
    fn test_parse_arguments_values() {
        let a = parses!(
            "-c",
            "foo.cpp",
            "-fabc",
            "-I",
            "include-file",
            "-o",
            "foo.o",
            "--include-path",
            "include-file",
            "-isystem=/system/include/file",
            "-Werror",
            "cross-execution-space-call",
            "-Werror=all-warnings"
        );
        assert_eq!(Some("foo.cpp"), a.input.to_str());
        assert_eq!(Language::Cxx, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert_eq!(
            ovec![
                "-Iinclude-file",
                "--include-path",
                "include-file",
                "-isystem",
                "/system/include/file",
                "-Werror",
                "cross-execution-space-call",
                "-Werror",
                "all-warnings"
            ],
            a.preprocessor_args
        );
        assert!(a.dependency_args.is_empty());
        assert_eq!(ovec!["-fabc", "-c"], a.common_args);
    }

    #[test]
    fn test_parse_md_mt_flags_cu() {
        let a = parses!(
            "-x", "cu", "-c", "foo.c", "-fabc", "-MD", "-MT", "foo.o", "-MF", "foo.o.d", "-o",
            "foo.o"
        );
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_eq!(Some("-c"), a.compilation_flag.to_str());
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert_eq!(
            ovec!["-MD", "-MF", "foo.o.d", "-MT", "foo.o"],
            a.dependency_args
        );
        assert_eq!(ovec!["-fabc", "-c"], a.common_args);
    }

    #[test]
    fn test_parse_generate_code_flags() {
        let a = parses!(
            "-x",
            "cu",
            "--generate-code=arch=compute_61,code=sm_61",
            "-c",
            "foo.c",
            "-o",
            "foo.o"
        );
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(
            ovec!["--generate-code", "arch=compute_61,code=sm_61", "-c"],
            a.common_args
        );
    }

    #[test]
    fn test_parse_pass_to_host_flags() {
        let a = parses!(
            "-x=cu",
            "--generate-code=arch=compute_60,code=[sm_60,sm_61]",
            "-Xnvlink=--suppress-stack-size-warning",
            "-Xcompiler",
            "-fPIC,-fno-common",
            "-Xcompiler=-fvisibility=hidden",
            "-Xcompiler=-Wall,-Wno-unknown-pragmas,-Wno-unused-local-typedefs",
            "-Xcudafe",
            "--display_error_number",
            "-c",
            "foo.c",
            "-o",
            "foo.o"
        );
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert_eq!(
            ovec![
                "-Xcompiler",
                "-fPIC,-fno-common",
                "-Xcompiler",
                "-fvisibility=hidden",
                "-Xcompiler",
                "-Wall,-Wno-unknown-pragmas,-Wno-unused-local-typedefs"
            ],
            a.preprocessor_args
        );
        assert_eq!(
            ovec![
                "--generate-code",
                "arch=compute_60,code=[sm_60,sm_61]",
                "-Xnvlink",
                "--suppress-stack-size-warning",
                "-Xcudafe",
                "--display_error_number",
                "-c"
            ],
            a.common_args
        );
    }

    #[test]
    fn test_parse_no_capturing_of_xcompiler() {
        let a = parses!(
            "-x=cu",
            "-forward-unknown-to-host-compiler",
            "--expt-relaxed-constexpr",
            "-Xcompiler",
            "-pthread",
            "-std=c++14",
            "-c",
            "foo.c",
            "-o",
            "foo.o"
        );
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert_eq!(
            ovec!["--expt-relaxed-constexpr", "-Xcompiler", "-pthread"],
            a.preprocessor_args
        );
        assert_eq!(
            ovec!["-forward-unknown-to-host-compiler", "-std=c++14", "-c"],
            a.common_args
        );
    }

    #[test]
    fn test_parse_dlink_is_not_compilation() {
        assert_eq!(
            CompilerArguments::NotCompilation,
            parse_arguments_gcc(stringvec![
                "-forward-unknown-to-host-compiler",
                "--generate-code=arch=compute_50,code=[compute_50,sm_50,sm_52]",
                "-dlink",
                "main.cu.o",
                "-o",
                "device_link.o"
            ])
        );
        assert_eq!(
            CompilerArguments::NotCompilation,
            parse_arguments_nvc(stringvec![
                "-forward-unknown-to-host-compiler",
                "--generate-code=arch=compute_50,code=[compute_50,sm_50,sm_52]",
                "-dlink",
                "main.cu.o",
                "-o",
                "device_link.o"
            ])
        );
    }

    #[test]
    fn test_parse_cant_cache_flags() {
        assert_eq!(
            CompilerArguments::CannotCache("-E", None),
            parse_arguments_gcc(stringvec!["-x", "cu", "-c", "foo.c", "-o", "foo.o", "-E"])
        );
        assert_eq!(
            CompilerArguments::CannotCache("-E", None),
            parse_arguments_msvc(stringvec!["-x", "cu", "-c", "foo.c", "-o", "foo.o", "-E"])
        );
        assert_eq!(
            CompilerArguments::CannotCache("-E", None),
            parse_arguments_nvc(stringvec!["-x", "cu", "-c", "foo.c", "-o", "foo.o", "-E"])
        );

        assert_eq!(
            CompilerArguments::CannotCache("-M", None),
            parse_arguments_gcc(stringvec!["-x", "cu", "-c", "foo.c", "-o", "foo.o", "-M"])
        );
        assert_eq!(
            CompilerArguments::CannotCache("-M", None),
            parse_arguments_msvc(stringvec!["-x", "cu", "-c", "foo.c", "-o", "foo.o", "-M"])
        );
        assert_eq!(
            CompilerArguments::CannotCache("-M", None),
            parse_arguments_nvc(stringvec!["-x", "cu", "-c", "foo.c", "-o", "foo.o", "-M"])
        );

        // nvcc arg parsing is very permissive, so all these are valid and should yield CannotCache
        for arg in [
            "-fdevice-time-trace",
            "--fdevice-time-trace",
            "-time",
            "--time",
        ] {
            // {-,--}fdevice-time-trace
            assert_eq!(
                CompilerArguments::CannotCache(arg, None),
                parse_arguments_gcc(stringvec![arg])
            );
            // {-,--}fdevice-time-trace -
            assert_eq!(
                CompilerArguments::CannotCache(arg, None),
                parse_arguments_msvc(stringvec![arg, "-"])
            );
            // {-,--}fdevice-time-trace-
            assert_eq!(
                CompilerArguments::CannotCache(arg, None),
                parse_arguments_msvc(stringvec![format!("{arg}-")])
            );
            // {-,--}fdevice-time-trace flamegraph.json
            assert_eq!(
                CompilerArguments::CannotCache(arg, None),
                parse_arguments_nvc(stringvec![arg, "flamegraph.json"])
            );
        }

        for arg_with_separator in [
            "-fdevice-time-trace=",
            "--fdevice-time-trace=",
            "-time=",
            "--time=",
        ] {
            // {-,--}fdevice-time-trace=-
            assert_eq!(
                CompilerArguments::CannotCache(arg_with_separator, None),
                parse_arguments_msvc(stringvec![format!("{arg_with_separator}-")])
            );
            // {-,--}fdevice-time-trace=flamegraph.json
            assert_eq!(
                CompilerArguments::CannotCache(arg_with_separator, None),
                parse_arguments_nvc(stringvec![format!("{arg_with_separator}flamegraph.json")])
            );
        }
    }

    #[test]
    fn test_parse_device_debug_flag_short_cu() {
        let a = parses!(
            "-x", "cu", "-c", "foo.c", "-G", "-MD", "-MT", "foo.o", "-MF", "foo.o.d", "-o", "foo.o"
        );
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_eq!(Some("-c"), a.compilation_flag.to_str());
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert_eq!(
            ovec!["-MD", "-MF", "foo.o.d", "-MT", "foo.o"],
            a.dependency_args
        );
        assert_eq!(ovec!["-G", "-c"], a.common_args);
    }

    #[test]
    fn test_parse_device_debug_flag_long_cu() {
        let a = parses!(
            "-x",
            "cu",
            "-c",
            "foo.c",
            "--device-debug",
            "-MD",
            "-MT",
            "foo.o",
            "-MF",
            "foo.o.d",
            "-o",
            "foo.o"
        );
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cuda, a.language);
        assert_eq!(Some("-c"), a.compilation_flag.to_str());
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            )
        );
        assert_eq!(
            ovec!["-MD", "-MF", "foo.o.d", "-MT", "foo.o"],
            a.dependency_args
        );
        assert_eq!(ovec!["--device-debug", "-c"], a.common_args);
    }
}
