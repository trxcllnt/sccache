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
    exit_status, CommandChild, CommandCreator, CommandCreatorSync, ExitStatusValue, RunCommand,
};
use crate::util::{run_input_output, OsStrExt};
use crate::{counted_array, dist, protocol, server};
use async_trait::async_trait;
use fs::File;
use fs_err as fs;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use log::Level::Trace;
use regex::Regex;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::future::{Future, IntoFuture};
use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::process;
use tokio::io::AsyncBufReadExt;
use which::which_in;

use crate::errors::*;

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
        self.version.clone()
    }
    fn parse_arguments(
        &self,
        arguments: &[OsString],
        cwd: &Path,
    ) -> CompilerArguments<ParsedArguments> {
        let mut arguments = arguments.to_vec();

        if let Ok(flags) = std::env::var("NVCC_PREPEND_FLAGS") {
            arguments = shlex::split(&flags)
                .unwrap_or_default()
                .iter()
                .map(|s| s.clone().into_arg_os_string())
                .chain(arguments.iter().cloned())
                .collect::<Vec<_>>();
        }

        if let Ok(flags) = std::env::var("NVCC_APPEND_FLAGS") {
            arguments.extend(
                shlex::split(&flags)
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
                match parsed_args.compilation_flag.to_str() {
                    Some("") => { /* no compile flag is valid */ }
                    Some(flag) => {
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
                        parsed_args.common_args.push(flag.into());
                    }
                    _ => unreachable!(),
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
    ) -> Result<process::Output>
    where
        T: CommandCreatorSync,
    {
        let env_vars = env_vars
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

        let initialize_cmd_and_args = || {
            let mut command = creator.clone().new_command_sync(executable);
            command
                .current_dir(cwd)
                .env_clear()
                .envs(env_vars.clone())
                .args(&parsed_args.preprocessor_args)
                .args(&parsed_args.common_args)
                .arg("-x")
                .arg(language)
                .arg(&parsed_args.input);
            command
        };

        let dependencies_command = || {
            // NVCC doesn't support generating both the dependency information
            // and the preprocessor output at the same time. So if we have
            // need for both, we need separate compiler invocations
            let mut dependency_cmd = initialize_cmd_and_args();
            dependency_cmd.args(
                &parsed_args
                    .dependency_args
                    .iter()
                    .map(|arg| match arg.to_str().unwrap_or_default() {
                        "-MD" | "--generate-dependencies-with-compile" => "-M",
                        "-MMD" | "--generate-nonsystem-dependencies-with-compile" => "-MM",
                        arg => arg,
                    })
                    // protect against duplicate -M and -MM flags after transform
                    .unique()
                    .collect::<Vec<_>>(),
            );
            if log_enabled!(Trace) {
                trace!("dependencies command: {:?}", dependency_cmd);
            }
            dependency_cmd
        };

        let preprocessor_command = || {
            let mut preprocess_cmd = initialize_cmd_and_args();
            // NVCC only supports `-E` when it comes after preprocessor and common flags.
            preprocess_cmd.arg("-E");
            preprocess_cmd.arg(match self.host_compiler {
                // nvc/nvc++ don't support eliding line numbers
                NvccHostCompiler::Nvhpc => "",
                // msvc requires the `-EP` flag to elide line numbers
                NvccHostCompiler::Msvc => "-Xcompiler=-EP",
                // other host compilers are presumed to match `gcc` behavior
                NvccHostCompiler::Gcc => "-Xcompiler=-P",
            });
            if log_enabled!(Trace) {
                trace!("preprocessor command: {:?}", preprocess_cmd);
            }
            preprocess_cmd
        };

        // Chain dependency generation and the preprocessor command to emulate a `proper` front end
        if !parsed_args.dependency_args.is_empty() {
            run_input_output(dependencies_command(), None).await?;
        }

        run_input_output(preprocessor_command(), None).await
    }

    fn generate_compile_commands<T>(
        &self,
        path_transformer: &mut dist::PathTransformer,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
        rewrite_includes_only: bool,
    ) -> Result<(
        Box<dyn CompileCommand<T>>,
        Option<dist::CompileCommand>,
        Cacheable,
    )>
    where
        T: CommandCreatorSync,
    {
        generate_compile_commands(parsed_args, executable, cwd, env_vars).map(
            |(command, dist_command, cacheable)| {
                (CCompileCommand::new(command), dist_command, cacheable)
            },
        )
    }
}

pub fn generate_compile_commands(
    parsed_args: &ParsedArguments,
    executable: &Path,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
) -> Result<(NvccCompileCommand, Option<dist::CompileCommand>, Cacheable)> {
    let env_vars = env_vars
        .iter()
        .filter(|(k, _)| k != "NVCC_PREPEND_FLAGS" && k != "NVCC_APPEND_FLAGS")
        .cloned()
        .collect::<Vec<_>>();

    let temp_dir = tempfile::Builder::new()
        .prefix("sccache_nvcc")
        .tempdir()
        .unwrap()
        .into_path();

    let mut arguments: Vec<OsString> = vec![
        "--dryrun".into(),
        // Tell nvcc to produce deterministic file names
        "--keep".into(),
    ];

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
                let dir = if dir.is_absolute() {
                    dir
                } else {
                    cwd.join(dir)
                };
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
            if let Some(idx) = unhashed_args.iter().position(|x| x.starts_with("-t=")) {
                let arg = unhashed_args.get(idx);
                if let Some(arg) = arg.and_then(|arg| arg.to_str()) {
                    if let Ok(arg) = arg[3..arg.len()].parse::<usize>() {
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

    if let Some(lang) = gcc::language_to_gcc_arg(parsed_args.language) {
        arguments.extend(vec!["-x".into(), lang.into()])
    }

    let output = parsed_args
        .outputs
        .get("obj")
        .context("Missing object file output")
        .unwrap()
        .path
        .clone();

    if parsed_args.compilation_flag == "-c" {
        arguments.push(parsed_args.compilation_flag.clone());
    }

    arguments.extend(vec!["-o".into(), output.clone().into()]);

    arguments.extend_from_slice(&parsed_args.preprocessor_args);
    arguments.extend_from_slice(&unhashed_args);
    arguments.extend_from_slice(&parsed_args.common_args);
    arguments.extend_from_slice(&parsed_args.arch_args);
    if parsed_args.double_dash_input {
        arguments.push("--".into());
    }

    // Canonicalize here so the absolute path to the input is in the files
    // generated by the preprocessor instead of paths relative to the cwd.
    //
    // Since cicc's input is the post-processed source run through cudafe++'s
    // transforms, its cache key is sensitive to the preprocessor output. The
    // preprocessor embeds the name of the input file in comments, so without
    // canonicalizing here, cicc will get cache misses on otherwise identical
    // input that should produce a cache hit.

    arguments.push(
        (if parsed_args.input.is_absolute() {
            parsed_args.input.clone()
        } else {
            cwd.join(&parsed_args.input).canonicalize().unwrap()
        })
        .into(),
    );

    let command = NvccCompileCommand {
        out_path: output.clone(),
        temp_dir,
        keep_dir,
        num_parallel,
        executable: executable.to_owned(),
        arguments,
        env_vars,
        cwd: cwd.to_owned(),
    };

    Ok((command, None, Cacheable::Yes))
}

#[derive(Clone, Debug)]
pub struct NvccCompileCommand {
    pub out_path: PathBuf,
    pub temp_dir: PathBuf,
    pub keep_dir: Option<PathBuf>,
    pub num_parallel: usize,
    pub executable: PathBuf,
    pub arguments: Vec<OsString>,
    pub env_vars: Vec<(OsString, OsString)>,
    pub cwd: PathBuf,
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
    ) -> Result<process::Output>
    where
        T: CommandCreatorSync,
    {
        let NvccCompileCommand {
            out_path,
            temp_dir,
            keep_dir,
            num_parallel,
            executable,
            arguments,
            env_vars,
            cwd,
        } = self;

        let mut env_vars = env_vars.to_vec();

        let (nvcc_subcommand_groups, old_to_new_name_map) =
            group_nvcc_subcommands_by_compilation_stage(
                creator,
                executable,
                arguments,
                cwd,
                temp_dir.as_path(),
                keep_dir.clone(),
                &mut env_vars,
            )
            .await?;

        let maybe_keep_temps_then_clean = || {
            // If the caller passed `-keep` or `-keep-dir`, copy the
            // temp files to the requested location. We do this because we
            // override `-keep` and `-keep-dir` in our `nvcc --dryrun` call.
            let maybe_keep_temps = keep_dir.as_ref().and_then(|dst| {
                fs::create_dir_all(dst)
                    .and_then(|_| fs::read_dir(temp_dir))
                    .and_then(|files| {
                        files
                            .filter_map(|path| path.ok())
                            .filter_map(|path| {
                                path.file_name()
                                    .to_str()
                                    .map(|file| (path.path(), file.to_owned()))
                            })
                            .try_fold((), |res, (path, file)| fs::rename(path, dst.join(file)))
                    })
                    .ok()
            });

            // If the compilation flag was `-ptx`, `-cubin`, or `-dc`,
            // copy and rename the output file to the expected location.
            //
            // This is necessary in the case where we renamed the ptx and
            // cubin files to more deterministic names to aid caching.
            //
            //
            // It's important _not_ to canonicalize `out_path` before this
            // point because the original `parsed_args.output` is the key in
            // the `old_to_new_name_map`.
            let maybe_copy_renamed_output = out_path
                .to_str()
                .and_then(|old| old_to_new_name_map.get(old))
                .and_then(|new| {
                    fs::copy(
                        temp_dir.join(new),
                        // output can either be absolute or relative to `cwd`
                        if out_path.is_absolute() {
                            out_path.clone()
                        } else {
                            cwd.join(out_path)
                        },
                    )
                    .ok()
                })
                .or(Some(0));

            maybe_keep_temps
                .and(maybe_copy_renamed_output)
                .map_or_else(
                    || fs::remove_dir_all(temp_dir).ok(),
                    |_| fs::remove_dir_all(temp_dir).ok(),
                )
                .unwrap_or(());
        };

        let mut output = process::Output {
            status: process::ExitStatus::default(),
            stdout: vec![],
            stderr: vec![],
        };

        let n = nvcc_subcommand_groups.len();
        let cuda_front_end_range = if n < 1 { 0..0 } else { 0..1 };
        let device_compile_range = if n < 2 { 0..0 } else { 1..n - 1 };
        let final_assembly_range = if n < 3 { 0..0 } else { n - 1..n };

        let num_parallel = device_compile_range.len().min(*num_parallel).max(1);

        for command_group_chunks in [
            nvcc_subcommand_groups[cuda_front_end_range].chunks(1),
            // compile multiple device architectures in parallel when `nvcc -t=N` is specified
            nvcc_subcommand_groups[device_compile_range].chunks(num_parallel),
            nvcc_subcommand_groups[final_assembly_range].chunks(1),
        ] {
            for command_groups in command_group_chunks {
                let results = futures::future::join_all(command_groups.iter().map(|commands| {
                    run_nvcc_subcommands_group(service, creator, cwd, &env_vars, commands)
                }))
                .await;

                for result in results {
                    aggregate_output(&mut output, result);
                }

                if output
                    .status
                    .code()
                    .and_then(|c| (c != 0).then_some(c))
                    .is_some()
                {
                    maybe_keep_temps_then_clean();
                    return Err(ProcessError(output).into());
                }
            }
        }

        maybe_keep_temps_then_clean();

        Ok(output)
    }
}

#[derive(Clone, Debug)]
pub struct NvccGeneratedSubcommand {
    pub exe: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub cacheable: Cacheable,
}

async fn group_nvcc_subcommands_by_compilation_stage<T>(
    creator: &T,
    executable: &Path,
    arguments: &[OsString],
    cwd: &Path,
    temp_dir: &Path,
    keep_dir: Option<PathBuf>,
    env_vars: &mut Vec<(OsString, OsString)>,
) -> Result<(Vec<Vec<NvccGeneratedSubcommand>>, HashMap<String, String>)>
where
    T: CommandCreatorSync,
{
    if log_enabled!(log::Level::Trace) {
        trace!(
            "nvcc dryrun cmd: {:?}",
            [
                vec![executable.as_os_str().to_owned()],
                arguments.to_owned()
            ]
            .concat()
            .join(OsStr::new(" "))
        );
    }

    let mut nvcc_dryrun_cmd = creator
        .clone()
        .new_command_sync(executable)
        .env_clear()
        .args(arguments)
        .envs(env_vars.to_vec())
        .current_dir(cwd)
        .stdin(process::Stdio::null())
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::piped())
        .spawn()
        .await?;

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
        cwd: &Path,
        line: &str,
    ) -> Option<(PathBuf, Vec<String>)> {
        // Intercept the environment variable lines and add them to the env_vars list
        if let Some(var) = re.captures(line) {
            let (_, [var, val]) = var.extract();

            let loc = if let Some(idx) = env_vars.iter().position(|(key, _)| key == var) {
                idx..idx + 1
            } else {
                env_vars.len()..env_vars.len()
            };

            let mut pair = (var.into(), val.into());
            // Handle the special `_SPACE_= ` line
            if val != " " {
                pair.1 = val
                    .trim()
                    .split(' ')
                    .map(|x| x.trim_start_matches('\"').trim_end_matches('\"'))
                    .collect::<Vec<_>>()
                    .join(" ")
                    .into();
            }
            env_vars.splice(loc, [pair]);
            return None;
        }

        // The rest of the lines are subcommands, so parse into a vec of [cmd, args..]

        let mut line = line.to_owned();

        // Expand envvars in nvcc subcommands, i.e. "$CICC_PATH/cicc ..."
        if let Some(env_vars) = dist::osstring_tuples_to_strings(env_vars) {
            for (key, val) in env_vars {
                let var = "$".to_owned() + &key;
                line = line.replace(&var, &val);
            }
        }

        let args = shlex::split(&line)?;
        let (exe, args) = args.split_first()?;

        let env_path = env_vars
            .iter()
            .find(|(k, _)| k == "PATH")
            .map(|(_, p)| p.to_owned())
            .unwrap();

        if let Ok(exe) = which_in(exe, env_path.into(), cwd) {
            return Some((exe.clone(), args.to_vec()));
        }

        None
    }

    fn remap_generated_filenames(
        args: &mut [String],
        old_to_new: &mut HashMap<String, String>,
        new_to_old: &mut HashMap<String, String>,
        ext_counts: &mut HashMap<String, i32>,
    ) {
        for arg in &mut args[..] {
            let maybe_ext = (!arg.starts_with('-'))
                .then(|| {
                    [
                        ".ptx",
                        ".cubin",
                        ".cpp1.ii",
                        ".cudafe1.c",
                        ".cudafe1.cpp",
                        ".cudafe1.gpu",
                        ".cudafe1.stub.c",
                    ]
                    .iter()
                    .find(|ext| arg.ends_with(*ext))
                    .copied()
                })
                .unwrap_or(None);
            match maybe_ext {
                Some(ext) => {
                    let old = arg.clone();
                    *arg = if !old_to_new.contains_key(&old) {
                        // Get or initialize count for the extension
                        if !ext_counts.contains_key(ext) {
                            // Initialize to 0
                            ext_counts.insert(ext.into(), 0).or(Some(0))
                        } else {
                            // Update the count
                            ext_counts.get_mut(ext).map(|c| {
                                *c += 1;
                                *c
                            })
                        }
                        .and_then(|count| {
                            let new = count.to_string() + ext;
                            None.or(old_to_new.insert(old.clone(), new.clone()))
                                .or(new_to_old.insert(new.clone(), old.clone()))
                                .or(Some(new.clone()))
                        })
                    } else {
                        old_to_new.get(&old).map(|p| p.to_owned())
                    }
                    .unwrap_or(old)
                }
                None => {
                    for (old_name, new_name) in old_to_new.iter() {
                        *arg = arg.replace(old_name, new_name);
                    }
                }
            }
        }
    }

    let nvcc_subcommand_groups = nvcc_dryrun_cmd.take_stderr();
    let nvcc_subcommand_groups = async move {
        match nvcc_subcommand_groups {
            None => Err(anyhow!("failed to read stderr")),
            Some(stderr) => {
                let remap_filenames = keep_dir.is_none();
                let is_valid_line_re = Regex::new(r"^#\$ (.*)$").unwrap();
                let is_envvar_line_re = Regex::new(r"^([_A-Z]+)=(.*)$").unwrap();

                let mut command_groups: Vec<Vec<NvccGeneratedSubcommand>> = vec![];
                let mut old_to_new = HashMap::<String, String>::new();
                let mut new_to_old = HashMap::<String, String>::new();
                let mut ext_counts = HashMap::<String, i32>::new();

                let tmp_name =
                    |name: &String| temp_dir.join(name).into_os_string().into_string().ok();

                let reader = tokio::io::BufReader::new(stderr);
                let mut lines = reader.lines();

                while let Some(line) = lines.next_line().await? {
                    // Select lines that match the `#$ ` prefix from nvcc --dryrun
                    let line = select_valid_dryrun_lines(&is_valid_line_re, &line)?;
                    let maybe_exe_and_args = fold_env_vars_or_split_into_exe_and_args(
                        &is_envvar_line_re,
                        env_vars,
                        cwd,
                        &line,
                    );
                    let (exe, mut args) = match maybe_exe_and_args {
                        Some(exe_and_args) => exe_and_args,
                        None => continue,
                    };

                    // Remap nvcc's generated file names to deterministic names
                    if remap_filenames {
                        remap_generated_filenames(
                            &mut args,
                            &mut old_to_new,
                            &mut new_to_old,
                            &mut ext_counts,
                        );
                    }

                    // * cicc and ptxas are cacheable
                    // * cudafe++ and fatbinary are not cacheable
                    // * Run cudafe++, cicc, ptxas, and fatbinary in `temp_dir`
                    // * Run host preprocessing and compilation steps in `cwd`
                    let (dir, cacheable) = match exe.file_name().and_then(|s| s.to_str()) {
                        Some("cicc") | Some("ptxas") => (temp_dir, Cacheable::Yes),
                        Some("cudafe++") => (temp_dir, Cacheable::No),
                        Some("fatbinary") => {
                            // The fatbinary command represents the start of the last group
                            command_groups.push(vec![]);
                            (temp_dir, Cacheable::No)
                        }
                        // Otherwise this is a host compiler command
                        _ => {
                            if args.contains(&"-E".to_owned()) {
                                // Each preprocessor step represents the start of a new command group
                                command_groups.push(vec![]);
                                // Rewrite the output path of the preprocessor result to be the tempdir,
                                // not the cwd where sccache was invoked. cudafe++ embeds the path to the
                                // device stub as an #include in the file that produces the final object,
                                // so it must be a deterministic path and name to get cache hits.
                                let idx = args.len() - 1;
                                if let Some(path) = args.get(idx).and_then(tmp_name) {
                                    args[idx] = path;
                                }
                                // Do not run preprocessor calls through sccache
                                (cwd, Cacheable::No)
                            } else {
                                // Rewrite the input path of the file compiled by the host compiler into
                                // the final object. This is the other half of the process described above.
                                let idx = args.len() - 3;
                                if let Some(path) = args.get(idx).and_then(tmp_name) {
                                    args[idx] = path;
                                }
                                (cwd, Cacheable::Yes)
                            }
                        }
                    };

                    // Initialize the first group in case the first command isn't a call to the host preprocessor,
                    // i.e. `nvcc -o test.o -c test.c`
                    if command_groups.is_empty() {
                        command_groups.push(vec![]);
                    }

                    match command_groups.last_mut() {
                        None => {}
                        Some(group) => {
                            group.push(NvccGeneratedSubcommand {
                                exe,
                                args,
                                cwd: dir.into(),
                                cacheable,
                            });
                        }
                    };
                }

                Ok((command_groups, old_to_new))
            }
        }
    };

    let output = nvcc_dryrun_cmd
        .wait_with_output()
        .map_err(|e| anyhow!(e.to_string()));

    match futures::future::try_join(output, nvcc_subcommand_groups).await {
        Err(err) => Err(ProcessError(error_to_output(err)).into()),
        Ok((output, nvcc_subcommand_groups)) => {
            if output.status.code().unwrap_or(0) == 0 {
                Ok(nvcc_subcommand_groups)
            } else {
                Err(ProcessError(output).into())
            }
        }
    }
}

async fn run_nvcc_subcommands_group<T>(
    service: &server::SccacheService<T>,
    creator: &T,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
    commands: &[NvccGeneratedSubcommand],
) -> Result<process::Output>
where
    T: CommandCreatorSync,
{
    let mut output = process::Output {
        status: process::ExitStatus::default(),
        stdout: vec![],
        stderr: vec![],
    };

    for cmd in commands {
        let NvccGeneratedSubcommand {
            exe,
            args,
            cwd,
            cacheable,
        } = cmd;

        if log_enabled!(log::Level::Trace) {
            trace!(
                "run_commands_sequential cwd={:?}, cmd={:?}",
                cwd,
                [
                    vec![exe.clone().into_os_string().into_string().unwrap()],
                    args.iter()
                        .map(|x| shlex::try_quote(x).unwrap().to_string())
                        .collect::<Vec<_>>()
                ]
                .concat()
                .join(" ")
            );
        }

        let out = match cacheable {
            Cacheable::No => {
                let mut cmd = creator.clone().new_command_sync(exe);

                cmd.args(args)
                    .current_dir(cwd)
                    .env_clear()
                    .envs(env_vars.to_vec());

                run_input_output(cmd, None)
                    .await
                    .unwrap_or_else(error_to_output)
            }
            Cacheable::Yes => {
                let args = dist::strings_to_osstrings(args);

                match service
                    .compiler_info(exe.clone(), cwd.to_owned(), &args, env_vars)
                    .await
                {
                    Err(err) => error_to_output(err),
                    Ok(c) => match c.parse_arguments(&args, cwd, env_vars) {
                        CompilerArguments::NotCompilation => Err(anyhow!("Not compilation")),
                        CompilerArguments::CannotCache(why, extra_info) => Err(extra_info
                            .map_or_else(
                                || anyhow!("Cannot cache({}): {:?} {:?}", why, exe, args),
                                |desc| {
                                    anyhow!("Cannot cache({}, {}): {:?} {:?}", why, desc, exe, args)
                                },
                            )),
                        CompilerArguments::Ok(hasher) => {
                            service
                                .start_compile_task(
                                    c,
                                    hasher,
                                    args,
                                    cwd.to_owned(),
                                    env_vars.to_owned(),
                                )
                                .await
                        }
                    }
                    .map_or_else(error_to_output, compile_result_to_output),
                }
            }
        };

        aggregate_output(&mut output, Ok(out));

        if output.status.code().unwrap_or(0) != 0 {
            break;
        }
    }

    Ok(output)
}

fn aggregate_output(acc: &mut process::Output, res: Result<process::Output>) {
    let out = res.unwrap_or_else(error_to_output);
    acc.status = exit_status(std::cmp::max(
        acc.status.code().unwrap_or(0),
        out.status.code().unwrap_or(0),
    ) as ExitStatusValue);
    acc.stdout.extend(out.stdout);
    acc.stderr.extend(out.stderr);
}

fn error_to_output(err: Error) -> process::Output {
    match err.downcast::<ProcessError>() {
        Ok(ProcessError(out)) => out,
        Err(err) => process::Output {
            status: exit_status(1 as ExitStatusValue),
            stdout: vec![],
            stderr: err.to_string().into_bytes(),
        },
    }
}

fn compile_result_to_output(res: protocol::CompileFinished) -> process::Output {
    process::Output {
        status: exit_status(res.retcode.or(res.signal).unwrap_or(0) as ExitStatusValue),
        stdout: res.stdout,
        stderr: res.stderr,
    }
}

counted_array!(pub static ARGS: [ArgInfo<gcc::ArgData>; _] = [
    //todo: refactor show_includes into dependency_args
    take_arg!("--Werror", OsString, CanBeSeparated('='), PreprocessorArgument),
    take_arg!("--archive-options options", OsString, CanBeSeparated('='), PassThrough),
    flag!("--compile", DoCompilation),
    take_arg!("--compiler-bindir", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--compiler-options", OsString, CanBeSeparated('='), PreprocessorArgument),
    flag!("--cubin", DoCompilation),
    flag!("--device-c", DoCompilation),
    flag!("--device-w", DoCompilation),
    flag!("--expt-extended-lambda", PreprocessorArgumentFlag),
    flag!("--expt-relaxed-constexpr", PreprocessorArgumentFlag),
    flag!("--extended-lambda", PreprocessorArgumentFlag),
    flag!("--fatbin", DoCompilation),
    take_arg!("--generate-code", OsString, CanBeSeparated('='), PassThrough),
    flag!("--generate-dependencies-with-compile", NeedDepTarget),
    flag!("--generate-nonsystem-dependencies-with-compile", NeedDepTarget),
    take_arg!("--gpu-architecture", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--gpu-code", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--include-path", PathBuf, CanBeSeparated('='), PreprocessorArgumentPath),
    flag!("--keep", UnhashedFlag),
    take_arg!("--keep-dir", OsString, CanBeSeparated('='), Unhashed),
    take_arg!("--linker-options", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--maxrregcount", OsString, CanBeSeparated('='), PassThrough),
    flag!("--no-host-device-initializer-list", PreprocessorArgumentFlag),
    take_arg!("--nvlink-options", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--options-file", PathBuf, CanBeSeparated('='), ExtraHashFile),
    flag!("--optix-ir", DoCompilation),
    flag!("--ptx", DoCompilation),
    take_arg!("--ptxas-options", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--relocatable-device-code", OsString, CanBeSeparated('='), PreprocessorArgument),
    flag!("--save-temps", UnhashedFlag),
    take_arg!("--system-include", PathBuf, CanBeSeparated('='), PreprocessorArgumentPath),
    take_arg!("--threads", OsString, CanBeSeparated('='), Unhashed),

    take_arg!("-Werror", OsString, CanBeSeparated('='), PreprocessorArgument),
    take_arg!("-Xarchive", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-Xcompiler", OsString, CanBeSeparated('='), PreprocessorArgument),
    take_arg!("-Xlinker", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-Xnvlink", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-Xptxas", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-arch", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-ccbin", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-code", OsString, CanBeSeparated('='), PassThrough),
    flag!("-cubin", DoCompilation),
    flag!("-dc", DoCompilation),
    flag!("-dw", DoCompilation),
    flag!("-expt-extended-lambda", PreprocessorArgumentFlag),
    flag!("-expt-relaxed-constexpr", PreprocessorArgumentFlag),
    flag!("-extended-lambda", PreprocessorArgumentFlag),
    flag!("-fatbin", DoCompilation),
    take_arg!("-gencode", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-isystem", PathBuf, CanBeSeparated('='), PreprocessorArgumentPath),
    flag!("-keep", UnhashedFlag),
    take_arg!("-keep-dir", OsString, CanBeSeparated('='), Unhashed),
    take_arg!("-maxrregcount", OsString, CanBeSeparated('='), PassThrough),
    flag!("-nohdinitlist", PreprocessorArgumentFlag),
    flag!("-optix-ir", DoCompilation),
    flag!("-ptx", DoCompilation),
    take_arg!("-rdc", OsString, CanBeSeparated('='), PreprocessorArgument),
    flag!("-save-temps", UnhashedFlag),
    take_arg!("-t", OsString, CanBeSeparated('='), Unhashed),
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
            version: None,
        }
        .parse_arguments(&arguments, ".".as_ref())
    }
    fn parse_arguments_msvc(arguments: Vec<String>) -> CompilerArguments<ParsedArguments> {
        let arguments = arguments.iter().map(OsString::from).collect::<Vec<_>>();
        Nvcc {
            host_compiler: NvccHostCompiler::Msvc,
            version: None,
        }
        .parse_arguments(&arguments, ".".as_ref())
    }
    fn parse_arguments_nvc(arguments: Vec<String>) -> CompilerArguments<ParsedArguments> {
        let arguments = arguments.iter().map(OsString::from).collect::<Vec<_>>();
        Nvcc {
            host_compiler: NvccHostCompiler::Nvhpc,
            version: None,
        }
        .parse_arguments(&arguments, ".".as_ref())
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
                    optional: false
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(ovec!["-ccbin", "/usr/bin/", "-c"], a.common_args);
    }

    #[test]
    fn test_parse_threads_argument_simple_cu() {
        let a = parses!(
            "-t=1",
            "-t",
            "2",
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
                    optional: false
                }
            )
        );
        assert!(a.preprocessor_args.is_empty());
        assert_eq!(
            ovec!["-t=1", "-t=2", "--threads", "1", "--threads", "2"],
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
    }
}
