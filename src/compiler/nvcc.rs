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
                let output_path = cwd.join(
                    &parsed_args
                        .outputs
                        .get("obj")
                        .context("Missing object file output")
                        .unwrap()
                        .path,
                );

                trace!(
                    "[{}]: dependencies command: {:?}",
                    output_path.display(),
                    dependency_cmd
                );
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
                let output_path = cwd.join(
                    &parsed_args
                        .outputs
                        .get("obj")
                        .context("Missing object file output")
                        .unwrap()
                        .path,
                );

                trace!(
                    "[{}]: preprocessor command: {:?}",
                    output_path.display(),
                    preprocess_cmd
                );
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

    let env_vars = env_vars
        .iter()
        .filter(|(k, _)| k != "NVCC_PREPEND_FLAGS" && k != "NVCC_APPEND_FLAGS")
        .cloned()
        .collect::<Vec<_>>();

    let mut arguments = vec![];

    if let Some(lang) = gcc::language_to_gcc_arg(parsed_args.language) {
        arguments.extend(vec!["-x".into(), lang.into()])
    }

    let output = &parsed_args
        .outputs
        .get("obj")
        .context("Missing object file output")
        .unwrap()
        .path;

    // Build nvcc's internal files in `$TMPDIR/$hash_key` so the paths are
    // stable across compilations. This is important because this path ends
    // up in the preprocessed output, so using random tmpdir paths leads to
    // erroneous cache misses.
    let out_dir = env::temp_dir().join("sccache_nvcc").join({
        // Combine `hash_key` with the output path in case
        // the same file is concurrently built to separate
        // output paths.
        let mut m = crate::util::Digest::new();
        m.update(hash_key.as_bytes());
        m.update(cwd.join(output).as_os_str().as_encoded_bytes());
        m.finish()
    });
    fs::create_dir_all(&out_dir).ok();

    let compile_flag = match parsed_args.compilation_flag.to_str() {
        Some("") // compile to executable
        | Some("-c") | Some("--compile") // compile to object
        | Some("-dc") | Some("--device-c") // compile to object with -rdc=true
        | Some("-dw") | Some("--device-w") // compile to object with -rdc=false
        => NvccCompileFlag::Device,
        Some("-cubin") | Some("--cubin") => NvccCompileFlag::Cubin,
        Some("-ptx") | Some("--ptx") => NvccCompileFlag::Ptx,
        Some("-cuda") | Some("--cuda") => NvccCompileFlag::Preprocess,
        Some("-fatbin") | Some("--fatbin") => NvccCompileFlag::Fatbin,
        _ => unreachable!()
    };

    arguments.extend(vec![
        "-o".into(),
        // Canonicalize the output path if the compile flag indicates we won't
        // produce an object file. Since we run cicc and ptxas in a temp dir,
        // but we run the host compiler in `cwd` (the dir from which sccache was
        // executed), cicc/ptxas `-o` argument should point at the real out path
        // that's potentially relative to `cwd`.
        if compile_flag == NvccCompileFlag::Device {
            output.clone().into()
        } else {
            cwd.join(output).into()
        },
    ]);

    arguments.extend_from_slice(&parsed_args.preprocessor_args);
    arguments.extend_from_slice(&unhashed_args);
    arguments.extend_from_slice(&parsed_args.common_args);
    arguments.extend_from_slice(&parsed_args.arch_args);
    if parsed_args.double_dash_input {
        arguments.push("--".into());
    }

    // Canonicalize here so the absolute path to the input is in the
    // preprocessor output instead of paths relative to `cwd`.
    //
    // Since cicc's input is the post-processed source run through cudafe++'s
    // transforms, its cache key is sensitive to the preprocessor output. The
    // preprocessor embeds the name of the input file in comments, so without
    // canonicalizing here, cicc will get cache misses on otherwise identical
    // inputs that should produce cache hits.
    arguments.push(cwd.join(&parsed_args.input).canonicalize().unwrap().into());

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
        output_path: cwd.join(output),
    };

    Ok((
        command,
        None,
        // Never assume the outer `nvcc` call is cacheable. We must decompose the nvcc call into
        // its constituent subcommands with `--dryrun` and only cache the final build product.
        //
        // Always decomposing `nvcc --dryrun` is the only way to ensure caching nvcc invocations
        // is fully sound, because the `nvcc -E` preprocessor output is not sufficient to detect
        // all source code changes.
        //
        // Specifically, `nvcc -E` always defines __CUDA_ARCH__, which means changes to host-only
        // code guarded by an `#ifndef __CUDA_ARCH__` will _not_ be captured in `nvcc -E` output.
        Cacheable::No,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum NvccCompileFlag {
    Cubin,
    Device,
    Fatbin,
    Preprocess,
    Ptx,
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
    ) -> Result<process::Output>
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
            if let Some(dst) = keep_dir {
                if fs::create_dir_all(dst).is_ok() {
                    nvcc_internal_files
                        .drain()
                        .filter_map(|(orig, path)| {
                            let path = out_dir.join(path);
                            if path.exists() {
                                PathBuf::from(orig)
                                    .file_name()
                                    .map(|name| (path, name.to_owned()))
                            } else {
                                None
                            }
                        })
                        .try_fold((), |_, (src, name)| fs::rename(src, dst.join(name)).ok());
                }
            }
            fs::remove_dir_all(out_dir).ok();
        };

        let mut output = process::Output {
            status: process::ExitStatus::default(),
            stdout: vec![],
            stderr: vec![],
        };

        let n = nvcc_subcommand_groups.len();
        let cuda_front_end_range = if n > 0 { 0..1 } else { 0..0 };
        let final_assembly_range = if n > 1 { n - 1..n } else { 0..0 };
        let device_compile_range = if n > 2 { 1..n - 1 } else { 0..0 };

        let num_parallel = device_compile_range.len().min(*num_parallel).max(1);

        for command_group_chunks in [
            nvcc_subcommand_groups[cuda_front_end_range].chunks(1),
            // compile multiple device architectures in parallel when `nvcc -t=N` is specified
            nvcc_subcommand_groups[device_compile_range].chunks(num_parallel),
            nvcc_subcommand_groups[final_assembly_range].chunks(1),
        ] {
            for command_groups in command_group_chunks {
                let results = futures::future::join_all(command_groups.iter().map(|commands| {
                    run_nvcc_subcommands_group(service, creator, cwd, commands, output_path)
                }))
                .await;

                for result in results {
                    output = aggregate_output(output, result.unwrap_or_else(error_to_output));
                }

                if !output.status.success() {
                    output.stdout.shrink_to_fit();
                    output.stderr.shrink_to_fit();
                    maybe_keep_temps_then_clean();
                    return Err(ProcessError(output).into());
                }
            }
        }

        output.stdout.shrink_to_fit();
        output.stderr.shrink_to_fit();
        maybe_keep_temps_then_clean();
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

    let is_nvcc_exe =
        |exe: &str| matches!(exe, "cicc" | "ptxas" | "cudafe++" | "nvlink" | "fatbinary");

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
            |exe| !is_nvcc_exe(exe),
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

    let preprocessor_flag = match host_compiler {
        NvccHostCompiler::Msvc => "-P",
        _ => "-E",
    }
    .to_owned();

    let mut cuda_front_end_group = Vec::<NvccGeneratedSubcommand>::new();
    let mut final_assembly_group = Vec::<NvccGeneratedSubcommand>::new();
    let mut device_compile_groups = HashMap::<String, Vec<NvccGeneratedSubcommand>>::new();

    for (dir, exe, args) in all_commands.iter_mut() {
        if let (env_vars, cacheable, Some(group)) = match exe.file_stem().and_then(|s| s.to_str()) {
            // fatbinary and nvlink are not cacheable
            Some("fatbinary") | Some("nvlink") => (
                env_vars.clone(),
                Cacheable::No,
                Some(&mut final_assembly_group),
            ),
            // cicc and ptxas are cacheable
            Some("cicc") => {
                // Fix for CTK < 12.0:
                // Remove `--gen_module_id_file` if cudafe++ already does it
                if cudafe_has_gen_module_id_file_flag {
                    if let Some(idx) = args.iter().position(|x| x == &gen_module_id_file_flag) {
                        args.splice(idx..idx + 1, []);
                    }
                }
                // Add these flags if they're missing:
                // * `--gen_c_file_name test_a.compute_XX.cudafe1.c`
                // * `--stub_file_name test_a.compute_XX.cudafe1.stub.c`
                // * `--gen_device_file_name test_a.compute_XX.cudafe1.gpu`
                //
                // This ensures the same `cicc` command is generated regardless
                // of whether the compilation flag is `-c`, `-ptx`, or `-cubin`

                // e.g. test_a.compute_XX.cpp1.ii
                let mut nidx = args.len() - 3;
                let name = args[nidx].clone();
                // test_a.compute_XX.cpp1.ii -> test_a.compute_XX
                let name = name.split(".cpp1.ii").next().unwrap();

                for (flag, name) in [
                    (&gen_c_file_name_flag, format!("{name}.cudafe1.c")),
                    (&stub_file_name_flag, format!("{name}.cudafe1.stub.c")),
                    (&gen_device_file_name_flag, format!("{name}.cudafe1.gpu")),
                ] {
                    if !args.contains(flag) {
                        args.splice(nidx..nidx, [flag.clone(), name]);
                        nidx = args.len() - 3;
                    }
                }

                let group = device_compile_groups.get_mut(&args[args.len() - 3]);
                (env_vars.clone(), Cacheable::Yes, group)
            }
            Some("ptxas") => {
                let group = device_compile_groups.values_mut().find(|cmds| {
                    if let Some(cicc) = cmds.last() {
                        if let Some(cicc_out) = cicc.args.last() {
                            return cicc_out == &args[args.len() - 3];
                        }
                    }
                    false
                });
                (env_vars.clone(), Cacheable::Yes, group)
            }
            // cudafe++ _must be_ cached, because the `.module_id` file is unique to each invocation (new in CTK 12.8)
            Some("cudafe++") => (
                env_vars.clone(),
                Cacheable::Yes,
                Some(&mut cuda_front_end_group),
            ),
            _ => {
                // All generated host compiler commands include one of these defines.
                // If one of these isn't present, this command is either a new binary
                // in the CTK that we don't know about, or a line like `rm x_dlink.reg.c`
                // that nvcc generates in certain cases.
                if !args.iter().any(|arg| {
                    arg.starts_with("-D__CUDA_ARCH__")
                        || arg.starts_with("-D__CUDA_ARCH_LIST__")
                        || arg.starts_with("-D__CUDACC__")
                        || arg.starts_with("-D__CUDACC_VER")
                        || arg.starts_with("-D__NVCC__")
                        || arg.starts_with("-lcudart")
                        || arg.starts_with("-lcudadevrt")
                }) {
                    continue;
                }
                if args.contains(&preprocessor_flag) {
                    // Each preprocessor step represents the start of a new command group
                    if let Some(out_file) = if cfg!(target_os = "windows") {
                        args.iter()
                            .find(|x| x.starts_with("-Fi"))
                            .and_then(|x| x.strip_prefix("-Fi"))
                    } else {
                        args.iter()
                            .position(|x| x == "-o")
                            .and_then(|i| args.get(i + 1).map(|o| o.as_str()))
                    }
                    .map(PathBuf::from)
                    .and_then(|out_path| {
                        out_path
                            .file_name()
                            .and_then(|out_name| out_name.to_str())
                            .map(|out_name| out_name.to_owned())
                    })
                    .and_then(|out_name| {
                        // If the output file ends with...
                        // * .cpp1.ii - cicc/ptxas input
                        // * .cpp4.ii - cudafe++ input
                        if out_name.ends_with(".cpp1.ii") {
                            Some(out_name.to_owned())
                        } else {
                            None
                        }
                    }) {
                        let new_device_compile_group = vec![];
                        device_compile_groups.insert(out_file.clone(), new_device_compile_group);
                        (
                            env_vars.clone(),
                            Cacheable::No,
                            device_compile_groups.get_mut(&out_file),
                        )
                    } else {
                        (
                            env_vars.clone(),
                            Cacheable::No,
                            Some(&mut cuda_front_end_group),
                        )
                    }
                } else {
                    // Cache the host compiler calls, since we've marked the outer `nvcc` call
                    // as non-cacheable. This ensures `sccache nvcc ...` _always_ decomposes the
                    // nvcc call into its constituent subcommands with `--dryrun`, but only caches
                    // the final build product once.
                    //
                    // Always decomposing `nvcc --dryrun` is the only way to ensure caching nvcc invocations
                    // is fully sound, because the `nvcc -E` preprocessor output is not sufficient to detect
                    // all source code changes.
                    //
                    // Specifically, `nvcc -E` always defines __CUDA_ARCH__, which means changes to host-only
                    // code guarded by an `#ifndef __CUDA_ARCH__` will _not_ be captured in `nvcc -E` output.
                    (
                        env_vars
                            .iter()
                            .chain(
                                [
                                    // HACK: This compilation will look like a C/C++ compilation,
                                    // but we want to report it in the stats as a CUDA compilation.
                                    // The SccacheService API doesn't have a great way to specify this
                                    // case, so we set a special envvar here that it can read when the
                                    // compilation is finished.
                                    ("__SCCACHE_THIS_IS_A_CUDA_COMPILATION__".into(), "".into()),
                                ]
                                .iter(),
                            )
                            .cloned()
                            .collect::<Vec<_>>(),
                        Cacheable::Yes,
                        Some(&mut final_assembly_group),
                    )
                }
            }
        } {
            if log_enabled!(log::Level::Trace) {
                trace!(
                    "[{}]: transformed nvcc command: \"{}\"",
                    output_path.display(),
                    [
                        &[format!("cd {} &&", dir.to_string_lossy()).to_string()],
                        &[exe.to_str().unwrap_or_default().to_string()][..],
                        &args[..]
                    ]
                    .concat()
                    .join(" ")
                );
            }

            group.push(NvccGeneratedSubcommand {
                exe: exe.clone(),
                args: args.clone(),
                cwd: dir.to_owned(),
                env_vars,
                cacheable,
            });
        }
    }

    let mut command_groups = vec![];

    command_groups.push(cuda_front_end_group);
    command_groups.extend(device_compile_groups.into_values());
    command_groups.push(final_assembly_group);

    Ok((nvcc_internal_files, command_groups))
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
    F: Fn(&str) -> bool,
    T: CommandCreatorSync,
{
    if log_enabled!(log::Level::Trace) {
        trace!(
            "[{}]: nvcc dryrun command: {:?}",
            output_path.display(),
            [
                &[executable.to_str().unwrap_or_default().to_string()][..],
                &dist::osstrings_to_strings(arguments).unwrap_or_default()[..],
                &["--dryrun".into(), "--keep".into()][..]
            ]
            .concat()
            .join(" ")
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
                if select_subcommand(exe_name) {
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
            format!("%{}%", var) // %CICC_PATH%
        } else {
            format!("${}", var) // $CICC_PATH
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
        let line = line
            .replace("\"\"", "\"")
            .replace(r"\\?\", "")
            .replace('\\', "/")
            .replace(r"//?/", "");
        match host_compiler {
            NvccHostCompiler::Msvc => line.replace(" -E ", " -P ").replace(" > ", " -Fi"),
            _ => line,
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
        ".ptx",
    ];

    let should_rename = compile_flag != &NvccCompileFlag::Preprocess;
    let mut extensions_to_rename = vec![
        ".cpp1.ii",
        ".cudafe1.c",
        ".cudafe1.cpp",
        ".cudafe1.gpu",
        ".cudafe1.stub.c",
    ];

    match compile_flag {
        // Rewrite PTX names if the compile flag is `-cubin`
        NvccCompileFlag::Cubin => {
            extensions_to_rename.push(".ptx");
        }
        // Rewrite both PTX and cubin names if the compile flag is `-c` or `-fatbin`
        NvccCompileFlag::Device | NvccCompileFlag::Fatbin => {
            extensions_to_rename.push(".ptx");
            extensions_to_rename.push(".cubin");
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
                                        if !should_rename
                                            || !extensions_to_rename
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
                                        if !should_rename
                                            || !extensions_to_rename
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
                            format!("-Fi{}", arg)
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

async fn run_nvcc_subcommands_group<T>(
    service: &server::SccacheService<T>,
    creator: &T,
    cwd: &Path,
    commands: &[NvccGeneratedSubcommand],
    output_path: &Path,
) -> Result<process::Output>
where
    T: CommandCreatorSync,
{
    let mut output = process::Output {
        status: process::ExitStatus::default(),
        stdout: vec![],
        stderr: vec![],
    };

    async fn run_subcommand<T>(cmd: &NvccGeneratedSubcommand, creator: &T) -> process::Output
    where
        T: CommandCreatorSync,
    {
        let NvccGeneratedSubcommand {
            exe,
            args,
            cwd,
            env_vars,
            cacheable,
        } = cmd;

        let mut cmd = creator.clone().new_command_sync(exe);

        cmd.args(args)
            .current_dir(cwd)
            .env_clear()
            .envs(env_vars.to_vec());

        run_input_output(cmd, None)
            .await
            .unwrap_or_else(error_to_output)
    }

    for cmd in commands {
        let NvccGeneratedSubcommand {
            exe,
            args,
            cwd,
            env_vars,
            cacheable,
        } = cmd;

        if log_enabled!(log::Level::Trace) {
            trace!(
                "[{}]: run_commands_sequential cwd={:?}, cmd=\"{}\"",
                output_path.display(),
                cwd,
                [
                    vec![exe.clone().into_os_string().into_string().unwrap()],
                    args.to_vec()
                ]
                .concat()
                .join(" ")
            );
        }

        let out = match cacheable {
            Cacheable::No => run_subcommand(cmd, creator).await,
            Cacheable::Yes => {
                let srvc = service.clone();
                let args = dist::strings_to_osstrings(args);

                match srvc
                    .compiler_info(exe.clone(), cwd.to_owned(), &args, env_vars)
                    .await
                {
                    Err(err) => error_to_output(err),
                    Ok(compiler) => match compiler.parse_arguments(&args, cwd, env_vars) {
                        CompilerArguments::NotCompilation => run_subcommand(cmd, creator).await,
                        CompilerArguments::CannotCache(why, extra_info) => {
                            error_to_output(extra_info.map_or_else(
                                || anyhow!("Cannot cache({}): {:?} {:?}", why, exe, args),
                                |desc| {
                                    anyhow!("Cannot cache({}, {}): {:?} {:?}", why, desc, exe, args)
                                },
                            ))
                        }
                        CompilerArguments::Ok(hasher) => srvc
                            .start_compile_task(
                                compiler,
                                hasher,
                                args,
                                cwd.to_owned(),
                                env_vars
                                    .iter()
                                    .filter(|(key, _)| key != "SCCACHE_DIRECT")
                                    .chain([("SCCACHE_DIRECT".into(), "false".into())].iter())
                                    .cloned()
                                    .collect::<Vec<_>>(),
                            )
                            .await
                            .map_or_else(error_to_output, result_to_output),
                    },
                }
            }
        };

        output = aggregate_output(output, out);

        if !output.status.success() {
            break;
        }
    }

    Ok(output)
}

fn aggregate_output(lhs: process::Output, rhs: process::Output) -> process::Output {
    process::Output {
        status: exit_status(std::cmp::max(
            !lhs.status.success() as ExitStatusValue,
            !rhs.status.success() as ExitStatusValue,
        )),
        stdout: [lhs.stdout, rhs.stdout].concat(),
        stderr: [lhs.stderr, rhs.stderr].concat(),
    }
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

fn result_to_output(res: protocol::CompileFinished) -> process::Output {
    process::Output {
        status: exit_status(
            res.retcode
                .or(res.signal)
                .map_or(0, |c| (c != 0) as ExitStatusValue),
        ),
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
    take_arg!("--default-stream", OsString, CanBeSeparated('='), PassThrough),
    flag!("--device-c", DoCompilation),
    flag!("--device-debug", PassThroughFlag),
    flag!("--device-w", DoCompilation),
    take_arg!("--diag-suppress", OsString, CanBeSeparated('='), PassThrough),
    flag!("--expt-extended-lambda", PreprocessorArgumentFlag),
    flag!("--expt-relaxed-constexpr", PreprocessorArgumentFlag),
    flag!("--extended-lambda", PreprocessorArgumentFlag),
    flag!("--fatbin", DoCompilation),
    take_arg!("--fdevice-time-trace", OsString, CanBeSeparated, TooHard),
    take_arg!("--fdevice-time-trace=", OsString, Concatenated, TooHard),
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
    take_arg!("--time", OsString, CanBeSeparated, TooHard),
    take_arg!("--time=", OsString, Concatenated, TooHard),
    take_arg!("--x", OsString, CanBeSeparated('='), Language),

    flag!("-G", PassThroughFlag),
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
    take_arg!("-default-stream", OsString, CanBeSeparated('='), PassThrough),
    flag!("-dw", DoCompilation),
    flag!("-expt-extended-lambda", PreprocessorArgumentFlag),
    flag!("-expt-relaxed-constexpr", PreprocessorArgumentFlag),
    flag!("-extended-lambda", PreprocessorArgumentFlag),
    flag!("-fatbin", DoCompilation),
    take_arg!("-fdevice-time-trace", OsString, CanBeSeparated, TooHard),
    take_arg!("-fdevice-time-trace=", OsString, Concatenated, TooHard),
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
                    optional: false
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
                    optional: false
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
                    optional: false
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
