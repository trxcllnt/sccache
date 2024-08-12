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
    self,
    gcc, get_compiler_info, write_temp_file, Cacheable, CompileCommand, CCompileCommand, CompileCommandImpl, CompilerArguments, Language,
};
use crate::mock_command::{exit_status, CommandCreator, CommandCreatorSync, CommandChild, RunCommand};
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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
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
        let parsed_args = gcc::parse_arguments(
            arguments,
            cwd,
            (&gcc::ARGS[..], &ARGS[..]),
            false,
            self.kind(),
        );

        match parsed_args {
            CompilerArguments::Ok(pargs) => {
                if pargs.compilation_flag != "-c" {
                    let mut new_args = pargs.clone();
                    new_args.common_args.push(pargs.compilation_flag);
                    return CompilerArguments::Ok(new_args);
                }
                CompilerArguments::Ok(pargs)
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
            command.args(&parsed_args.preprocessor_args);
            command.args(&parsed_args.common_args);
            //We need to add "-rdc=true" if we are compiling with `-dc`
            //So that the preprocessor has the correct implicit defines
            if parsed_args.compilation_flag == "-dc" {
                command.arg("-rdc=true");
            }
            command.arg("-x").arg(language).arg(&parsed_args.input);

            command
        };

        let dep_before_preprocessor = || {
            //NVCC doesn't support generating both the dependency information
            //and the preprocessor output at the same time. So if we have
            //need for both we need separate compiler invocations
            let mut dep_cmd = initialize_cmd_and_args();
            let mut transformed_deps = vec![];
            for item in parsed_args.dependency_args.iter() {
                if item == "-MD" {
                    transformed_deps.push(OsString::from("-M"));
                } else if item == "-MMD" {
                    transformed_deps.push(OsString::from("-MM"));
                } else {
                    transformed_deps.push(item.clone());
                }
            }
            dep_cmd
                .args(&transformed_deps)
                .env_clear()
                .envs(env_vars.to_vec())
                .current_dir(cwd);

            if log_enabled!(Trace) {
                trace!("dep-gen command: {:?}", dep_cmd);
            }
            dep_cmd
        };

        trace!("preprocess");
        let mut cmd = initialize_cmd_and_args();

        //NVCC only supports `-E` when it comes after preprocessor
        //and common flags.
        //
        // nvc/nvc++  don't support no line numbers to console
        // msvc requires the `-EP` flag to output no line numbers to console
        // other host compilers are presumed to match `gcc` behavior
        let no_line_num_flag = match self.host_compiler {
            NvccHostCompiler::Nvhpc => "",
            NvccHostCompiler::Msvc => "-Xcompiler=-EP",
            NvccHostCompiler::Gcc => "-Xcompiler=-P",
        };
        cmd.arg("-E")
            .arg(no_line_num_flag)
            .env_clear()
            .envs(env_vars.to_vec())
            .current_dir(cwd);
        if log_enabled!(Trace) {
            trace!("preprocess: {:?}", cmd);
        }

        //Need to chain the dependency generation and the preprocessor
        //to emulate a `proper` front end
        if !parsed_args.dependency_args.is_empty() {
            let first = run_input_output(dep_before_preprocessor(), None);
            let second = run_input_output(cmd, None);
            // TODO: If we need to chain these to emulate a frontend, shouldn't
            // we explicitly wait on the first one before starting the second one?
            // (rather than via which drives these concurrently)
            let (_f, s) = futures::future::try_join(first, second).await?;
            Ok(s)
        } else {
            run_input_output(cmd, None).await
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
    ) -> Result<(Box<dyn CompileCommand<T>>, Option<dist::CompileCommand>, Cacheable)>
    where
        T: CommandCreatorSync
    {
        generate_compile_commands(
            parsed_args,
            executable,
            cwd,
            env_vars,
        )
        .map(|(command, dist_command, cacheable)| (
            CCompileCommand::new(command),
            dist_command,
            cacheable
        ))
    }
}

pub fn generate_compile_commands(
    parsed_args: &ParsedArguments,
    executable: &Path,
    cwd: &Path,
    env_vars: &[(OsString, OsString)]
) -> Result<(NvccCompileCommand, Option<dist::CompileCommand>, Cacheable)> {

    let tempdir = tempfile::Builder::new().prefix("sccache_nvcc").tempdir().unwrap().into_path();

    let mut arguments: Vec<OsString> = vec![
        "--dryrun".into(),
        // Tell nvcc to produce deterministic file names
        "--keep".into(),
    ];

    let mut unhashed_args = parsed_args.unhashed_args.clone();

    let keepdir = {
        let mut keepdir = None;
        // Remove all occurrences of `-keep` and `-keep-dir`, but save the keep dir for copying to later
        loop {
            if let Some(idx) = unhashed_args.iter().position(|x| x == "-keep-dir" || x == "--keep-dir") {
                let dir = PathBuf::from(unhashed_args[idx + 1].as_os_str());
                let dir = if dir.is_absolute() { dir } else { cwd.join(dir) };
                unhashed_args.splice(idx..(idx + 2), []);
                keepdir = Some(dir);
                continue;
            } else if let Some(idx) = unhashed_args.iter().position(|x| x == "-keep" || x == "--keep" || x == "-save-temps" || x == "--save-temps") {
                unhashed_args.splice(idx..(idx + 1), []);
                if keepdir.is_none() {
                    keepdir = Some(cwd.to_path_buf())
                }
                continue;
            }
            break;
        };
        keepdir
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
                unhashed_args.splice(idx..(idx+1), []);
                continue;
            }
            if let Some(idx) = unhashed_args.iter().position(|x| x == "--threads") {
                let arg = unhashed_args.get(idx+1);
                if let Some(arg) = arg.and_then(|arg| arg.to_str()) {
                    if let Ok(arg) = arg.parse::<usize>() {
                        num_parallel = arg;
                    }
                }
                unhashed_args.splice(idx..(idx+2), []);
                continue;
            }
            break;
        }
        num_parallel
    };

    if let Some(lang) = gcc::language_to_gcc_arg(parsed_args.language) {
        arguments.extend(vec!["-x".into(), lang.into()])
    }

    let mut preprocessor_args = vec![];

    let preprocessor_args_len = parsed_args.preprocessor_args.len();
    let mut preprocessor_args_iter = parsed_args.preprocessor_args.iter().cloned().enumerate();
    while let Some((i, arg)) = preprocessor_args_iter.next() {
        let len = arg.len();
        let arg2 = arg.clone();
        let str = arg.into_string().unwrap();

        let starts_with_flag = |flag: &str| {
            if str.starts_with(flag) {
                let suf = &str[flag.len()..len];
                let p = PathBuf::from(suf);
                let p = if p.is_absolute() { p } else { cwd.join(p) };
                return Some((OsStr::new(flag).to_os_string(), p.as_os_str().to_owned()));
            }
            None
        };

        let mut flag_with_path = |flag: &str| {
            if str == flag {
                if let Some((_, suf)) = preprocessor_args_iter.next() {
                    let p = PathBuf::from(suf);
                    let p = if p.is_absolute() { p } else { cwd.join(p) };
                    return Some((OsStr::new(flag).to_os_string(), p.as_os_str().to_owned()));
                }
            }
            None
        };

        if let Some((flag, path)) = starts_with_flag("-F")
                        .or_else(|| starts_with_flag("-I"))
                        .or_else(|| flag_with_path("-idirafter"))
                        .or_else(|| flag_with_path("-iframework"))
                        .or_else(|| flag_with_path("-imacros"))
                        .or_else(|| flag_with_path("-imultilib"))
                        .or_else(|| flag_with_path("-include"))
                        .or_else(|| flag_with_path("--include-path"))
                        .or_else(|| flag_with_path("-iprefix"))
                        .or_else(|| flag_with_path("-iquote"))
                        .or_else(|| flag_with_path("-isysroot"))
                        .or_else(|| flag_with_path("-isystem"))
                        .or_else(|| flag_with_path("--system-include"))
                        .or_else(|| flag_with_path("-iwithprefix"))
                        .or_else(|| flag_with_path("-iwithprefixbefore")) {
            preprocessor_args.push(flag);
            preprocessor_args.push(path);
        } else {
            preprocessor_args.push(arg2);
        }
    }

    let output = parsed_args.outputs
        .get("obj")
        .context("Missing object file output").unwrap()
        .path.clone();

    arguments.extend(vec![
        parsed_args.compilation_flag.clone(),
        "-o".into(),
        output.into(),
    ]);

    arguments.extend_from_slice(&preprocessor_args);
    arguments.extend_from_slice(&unhashed_args);
    arguments.extend_from_slice(&parsed_args.common_args);
    arguments.extend_from_slice(&parsed_args.arch_args);
    if parsed_args.double_dash_input {
        arguments.push("--".into());
    }

    // Canonicalize the input path so the absolute path is in the files generated
    // by cudafe++, instead of a path relative to the tempdir where cudafe++ ran.
    arguments.push((
        if parsed_args.input.is_absolute() {
            parsed_args.input.clone()
        } else {
            cwd.join(parsed_args.input.clone()).canonicalize().unwrap()
        }
    ).into());

    let command = NvccCompileCommand {
        tempdir,
        keepdir,
        num_parallel,
        executable: executable.to_owned(),
        arguments,
        env_vars: env_vars.to_vec(),
        cwd: cwd.to_owned(),
    };

    Ok((command, None, Cacheable::Yes))
}

#[derive(Clone, Debug)]
pub struct NvccCompileCommand {
    pub tempdir: PathBuf,
    pub keepdir: Option<PathBuf>,
    pub num_parallel: usize,
    pub executable: PathBuf,
    pub arguments: Vec<OsString>,
    pub env_vars: Vec<(OsString, OsString)>,
    pub cwd: PathBuf,
}

#[derive(Clone, Debug)]
pub struct NvccGeneratedCommand {
    pub exe: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub cacheable: Cacheable,
}

#[async_trait]
impl CompileCommandImpl for NvccCompileCommand {

    fn get_executable(&self) -> PathBuf { self.executable.clone() }
    fn get_arguments(&self) -> Vec<OsString> { self.arguments.clone() }
    fn get_env_vars(&self) -> Vec<(OsString, OsString)> { self.env_vars.clone() }
    fn get_cwd(&self) -> PathBuf { self.cwd.clone() }

    async fn execute<T>(&self, service: &server::SccacheService<T>, creator: &T) -> Result<process::Output>
    where
        T: CommandCreatorSync
    {
        let NvccCompileCommand {
            tempdir: temp,
            keepdir: keep,
            num_parallel,
            executable,
            arguments,
            env_vars,
            cwd,
        } = self;

        fn aggregate_output(acc: process::Output, res: Result<process::Output>) -> process::Output {
            match res {
                Ok(out) => {
                    process::Output {
                        status: exit_status(*[
                            acc.status.code().unwrap_or(0),
                            out.status.code().unwrap_or(0)
                        ].iter().max().unwrap_or(&0)),
                        stdout: [acc.stdout, out.stdout].concat(),
                        stderr: [acc.stderr, out.stderr].concat(),
                    }
                },
                Err(err) => {
                    process::Output {
                        status: exit_status(1),
                        stdout: vec![],
                        stderr: [acc.stderr, err.to_string().into_bytes()].concat(),
                    }
                },
            }
        }

        let maybe_keep_temps_then_clean = || {
            keep.as_ref()
                .and_then(|dst|
                    fs::create_dir_all(dst)
                        .and_then(|_| fs::read_dir(temp))
                        .and_then(|files| files
                            .filter_map(|path| path.ok())
                            .filter_map(|path| path.file_name().to_str().map(|file| (path.path(), file.to_owned())))
                            .try_fold((), |res, (path, file)| fs::rename(path, dst.join(file)))
                        )
                        .ok()
                )
                .map_or_else(
                    | | fs::remove_dir_all(temp).ok(),
                    |_| fs::remove_dir_all(temp).ok(),
                )
                .unwrap_or(());
        };

        let mut env_vars = env_vars.to_vec();

        let grouped_subcommands = get_nvcc_subcommand_groups(
            creator,
            executable,
            arguments,
            cwd,
            temp.as_path(),
            keep.clone(),
            &mut env_vars
        ).await?;

        let env_path = env_vars.iter()
            .find(|(k, _)| k == "PATH")
            .map(|(_, p)| p.to_owned())
            .unwrap();

        let mut output = process::Output {
            status: process::ExitStatus::default(),
            stdout: vec![],
            stderr: vec![],
        };

        let num_groups = grouped_subcommands.len();
        let cuda_front_end_range = if num_groups < 1 { 0..0 } else { 0..1 };
        let device_compile_range = if num_groups < 2 { 0..0 } else { 1..num_groups-1 };
        let final_assembly_range = if num_groups < 3 { 0..0 } else { num_groups-1..num_groups };

        let num_parallel_device_compiles = device_compile_range.len().min(*num_parallel).max(1);

        for command_group_chunks in [
            grouped_subcommands[cuda_front_end_range].chunks(1),
            grouped_subcommands[device_compile_range].chunks(num_parallel_device_compiles),
            grouped_subcommands[final_assembly_range].chunks(1),
        ] {
            for command_groups in command_group_chunks {

                let results = futures::future::join_all(
                    command_groups.iter().map(|commands|
                        run_nvcc_subcommands(service, creator, cwd, &env_path, &env_vars, commands)
                    )
                )
                .await;

                for result in results {
                    output = aggregate_output(output, result);
                }

                if output.status.code().and_then(|c| (c != 0).then_some(c)).is_some() {
                    maybe_keep_temps_then_clean();
                    return Err(ProcessError(output).into());
                }
            }
        }

        maybe_keep_temps_then_clean();

        Ok(output)
    }
}

async fn get_nvcc_subcommand_groups<T>(
    creator: &T,
    executable: &Path,
    arguments: &[OsString],
    cwd: &Path,
    tmp: &Path,
    keepdir: Option<PathBuf>,
    env_vars: &mut Vec<(OsString, OsString)>,
) -> Result<Vec<Vec<NvccGeneratedCommand>>>
where
    T: CommandCreatorSync
{

    if log_enabled!(log::Level::Trace) {
        trace!("nvcc dryrun cmd: {:?}", [
            vec![executable.as_os_str().to_owned()],
            arguments.to_owned()
        ].concat().join(OsStr::new(" ")));
    }

    let mut nvcc_dryrun_cmd = creator.clone().new_command_sync(executable)
        .env_clear()
        .args(arguments)
        .envs(env_vars.to_vec())
        .current_dir(cwd)
        .stdin(process::Stdio::null())
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::piped())
        .spawn()
        .await?;

    fn select_lines_with_hash_dollar_space(line: String) -> Result<String> {
        let re = Regex::new(r"^#\$ (.*)$").unwrap();
        match re.captures(line.as_str()) {
            Some(caps) => {
                let (_, [rest]) = caps.extract();
                Ok(rest.to_string())
            },
            _ => Err(anyhow!("nvcc error: {:?}", line)),
        }
    }

    fn select_nvcc_env_vars(
        env_vars: &mut Vec<(OsString, OsString)>,
        line: &str
    ) -> Option<String> {
        if let Some(var) = Regex::new(r"^([_A-Z]+)=(.*)$").unwrap().captures(line) {
            let (_, [var, val]) = var.extract();

            let loc = if let Some(idx) = env_vars.iter().position(|(key, _)| key == var) {
                idx..idx+1
            } else {
                env_vars.len()..env_vars.len()
            };

            let mut pair = (var.into(), val.into());
            // Handle the special `_SPACE_= ` line
            if val != " " {
                pair.1 = val.trim()
                    .split(" ")
                    .map(|x| x
                        .trim_start_matches("\"")
                        .trim_end_matches("\""))
                    .collect::<Vec<_>>()
                    .join(" ")
                    .into();
            }
            env_vars.splice(loc, [pair]);
            return None;
        }
        Some(line.to_string())
    }

    let nvcc_subcommand_groups = nvcc_dryrun_cmd.take_stderr();
    let nvcc_subcommand_groups = async move {
        match nvcc_subcommand_groups {
            None => Err(anyhow!("failed to read stderr")),
            Some(mut stderr) => {

                let mut buf = Vec::new();

                stderr
                    .read_to_end(&mut buf)
                    .await
                    .context("failed to read stderr")?;

                let cursor = io::Cursor::new(buf);
                let reader = io::BufReader::new(cursor);
                let remap_filenames = keepdir.is_none();

                let nvcc_subcommand_groups = reader.lines()
                    .map(|line| line.map_err(|e| anyhow!(e.to_string())))
                    // Select lines that match the `#$ ` prefix from nvcc --dryrun
                    .map(|line| line.and_then(select_lines_with_hash_dollar_space))
                    // Intercept the environment variable lines and add them to the env_vars list
                    .filter_map_ok(|line: String| select_nvcc_env_vars(env_vars, &line))
                    // The rest of the lines are subcommands, so parse into a vec of [cmd, args..]
                    .filter_map_ok(|line| {
                        let args = shlex::split(&line)?;
                        let (exe, args) = args.split_first()?;
                        Some((exe.clone(), args.to_vec()))
                    })
                    .fold_ok((
                        vec![],
                        HashMap::<String, String>::new(),
                        HashMap::<String, i32>::new()
                    ), |(mut command_groups, mut old_to_new, mut ext_counts), (exe, mut args)| {

                        // Remap nvcc's generated file names to deterministic names
                        if remap_filenames {
                            for arg in &mut args[..] {
                                match (!arg.starts_with("-"))
                                    .then(||
                                        [
                                            ".ptx",
                                            ".cubin",
                                            ".cpp1.ii",
                                            ".cudafe1.c",
                                            ".cudafe1.cpp",
                                            ".cudafe1.gpu",
                                            ".cudafe1.stub.c",
                                        ].iter().find(|ext| arg.ends_with(*ext)).cloned()
                                    ).unwrap_or(None)
                                {
                                    Some(ext) => {
                                        let old = arg.clone();
                                        *arg = if !old_to_new.contains_key(&old) {
                                            // Get or initialize count for the extension
                                            if !ext_counts.contains_key(ext) {
                                                // Initialize to 0
                                                ext_counts.insert(ext.into(), 0).or(Some(0))
                                            } else {
                                                // Update the count
                                                ext_counts.get_mut(ext).map(|c| { *c += 1; *c })
                                            }
                                            .and_then(|count| {
                                                let new = count.to_string() + ext;
                                                old_to_new.insert(old.clone(), new.clone()).or(Some(new.clone()))
                                            })
                                        } else {
                                            old_to_new.get(&old).map(|p| p.to_owned())
                                        }
                                        .unwrap_or(old);
                                    },
                                    None => {
                                        for (old_name, new_name) in old_to_new.iter() {
                                            *arg = arg.replace(old_name, new_name);
                                        }
                                    }
                                }
                            }
                        }

                        let tmp_name = |name: &String| tmp.join(name).into_os_string().into_string().ok();

                        let (dir, cacheable) = match exe.as_str() {
                            // cicc, ptxas are cacheable
                            "cicc" | "ptxas" => (tmp, Cacheable::Yes),
                            // cudafe++ and fatbinary are not cacheable
                            "cudafe++" => (tmp, Cacheable::No),
                            "fatbinary" => {
                                // The fatbinary command represents the start of the last group
                                command_groups.push(vec![]);
                                (tmp, Cacheable::No)
                            },
                            // Otherwise this is a host compiler command
                            _ => {
                                if args.contains(&"-E".to_owned()) {
                                    // Each preprocessor command represents the start of a new command group
                                    command_groups.push(vec![]);
                                    // Rewrite the output path of the preprocessor result to be the tempdir,
                                    // not the cwd where sccache was invoked. cudafe++ embeds the path to the
                                    // device stub as an #include in the source compiled into the final object,
                                    // so it must be a deterministic path and name in order to get cache hits.
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
                            },
                        };

                        // Initialize the first group if the first command isn't a call to the host compiler preprocessor
                        if command_groups.is_empty() {
                            command_groups.push(vec![]);
                        }

                        let idx = command_groups.len() - 1;

                        command_groups[idx].push(NvccGeneratedCommand {
                            exe,
                            args,
                            cwd: dir.into(),
                            cacheable
                        });

                        (command_groups, old_to_new, ext_counts)
                    })
                    .map(|t| t.0);

                nvcc_subcommand_groups
            }
        }
    };

    let output = nvcc_dryrun_cmd.wait_with_output().map_err(|e| anyhow!(e.to_string()));
    let (output, nvcc_subcommand_groups) = futures::future::try_join(output, nvcc_subcommand_groups).await?;

    let code = output.status.code().unwrap();
    if code == 0 {
        Ok(nvcc_subcommand_groups)
    } else {
        Err(ProcessError(output).into())
    }
}

async fn run_nvcc_subcommands<T>(
    service: &server::SccacheService<T>,
    creator: &T,
    cwd: &Path,
    env_path: &OsStr,
    env_vars: &[(OsString, OsString)],
    commands: &[NvccGeneratedCommand],
) -> Result<process::Output>
where
    T: CommandCreatorSync
{
    let mut output = process::Output {
        status: process::ExitStatus::default(),
        stdout: vec![],
        stderr: vec![],
    };

    for cmd in commands {
        let NvccGeneratedCommand {
            exe,
            args,
            cwd,
            cacheable,
        } = cmd;

        let exe = which_in(exe, env_path.into(), cwd)?;

        if log_enabled!(log::Level::Trace) {
            debug!("run_commands_sequential cwd={:?}, cmd={:?}", cwd, [
                vec![exe.clone().into_os_string().into_string().unwrap()],
                args.iter().map(|x| shlex::try_quote(x).unwrap().to_string()).collect::<Vec<_>>()
            ].concat().join(" "));
        }

        let out = match cacheable {
            Cacheable::No => {
                let mut cmd = creator.clone().new_command_sync(exe);
                cmd.args(args)
                    .env_clear()
                    .current_dir(cwd)
                    .envs(env_vars.to_vec());
                run_input_output(cmd, None)
                    .map_ok_or_else(
                        |err| match err.downcast::<ProcessError>() {
                            Ok(ProcessError(out)) => out,
                            Err(err) => process::Output {
                                status: exit_status(1),
                                stdout: vec![],
                                stderr: err.to_string().into_bytes(),
                            }
                        },
                        |out| out
                    )
                    .await
                },
            Cacheable::Yes => {

                use futures::future;
                use protocol::Response::*;
                use protocol::CompileResponse::*;

                let args = args.iter().map(|x| OsStr::new(x).to_os_string()).collect::<Vec<_>>();

                service
                    .clone()
                    .handle_compile(protocol::Compile {
                        exe: exe.into(),
                        cwd: cwd.into(),
                        args,
                        env_vars: [
                            env_vars,
                            // Disable preprocessor cache mode
                            &[("SCCACHE_DIRECT".into(), "false".into())]
                        ].concat(),
                    })
                    .map_ok_or_else(
                        Err,
                        |res| match res {
                            server::Message::WithoutBody(res) => Err(anyhow!(
                                match res {
                                    Compile(UnhandledCompile) => "Unhandled compile".into(),
                                    Compile(UnsupportedCompiler(msg)) => format!("Unsupported compiler: {:?}", msg),
                                    _ => "Unknown response".into(),
                                }
                            )),
                            server::Message::WithBody(_, body) => Ok(body
                                .filter_map(|res| async move {
                                    match res {
                                        Ok(CompileFinished(output)) => Some(output),
                                        _ => None
                                    }
                                })
                                .take(1)
                                .fold(
                                    Err(anyhow!("Empty response body")),
                                    |opt, out| async { Ok(out) }
                                )
                            )
                        },
                    )
                    .try_flatten()
                    .map_ok_or_else(
                        |err| process::Output {
                            status: exit_status(1),
                            stdout: vec![],
                            stderr: err.to_string().into_bytes(),
                        },
                        |out| process::Output {
                            status: exit_status(
                                out.retcode.or(out.signal).unwrap_or(0)
                            ),
                            stdout: out.stdout,
                            stderr: out.stderr,
                        }
                    ).await
            },
        };

        output.status = out.status;
        output.stdout = [output.stdout, out.stdout].concat();
        output.stderr = [output.stderr, out.stderr].concat();

        if output.status.code().unwrap_or(0) != 0 {
            break;
        }
    }

    Ok(output)
}

counted_array!(pub static ARGS: [ArgInfo<gcc::ArgData>; _] = [
    //todo: refactor show_includes into dependency_args
    take_arg!("--Werror", OsString, CanBeSeparated('='), PreprocessorArgument),
    take_arg!("--archive-options options", OsString, CanBeSeparated('='), PassThrough),
    flag!("--compile", DoCompilation),
    take_arg!("--compiler-bindir", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--compiler-options", OsString, CanBeSeparated('='), PreprocessorArgument),
    flag!("--cubin", DoCompilation),
    flag!("--expt-extended-lambda", PreprocessorArgumentFlag),
    flag!("--expt-relaxed-constexpr", PreprocessorArgumentFlag),
    flag!("--extended-lambda", PreprocessorArgumentFlag),
    flag!("--fatbin", DoCompilation),
    take_arg!("--generate-code", OsString, CanBeSeparated('='), PassThrough),
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
        assert!(a.common_args.is_empty());
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
        assert!(a.common_args.is_empty());
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
        assert!(a.common_args.is_empty());
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
        assert!(a.common_args.is_empty());
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
        assert_eq!(ovec!["-ccbin", "gcc"], a.common_args);
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
        assert_eq!(ovec!["-ccbin", "/usr/bin/"], a.common_args);
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
        assert!(a.common_args.is_empty());
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
        assert_eq!(ovec!["-fabc"], a.common_args);
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
        assert_eq!(ovec!["-fabc"], a.common_args);
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
            ovec!["--generate-code", "arch=compute_61,code=sm_61"],
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
                "--display_error_number"
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
            ovec!["-forward-unknown-to-host-compiler", "-std=c++14"],
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
