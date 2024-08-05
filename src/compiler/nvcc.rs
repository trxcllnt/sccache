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
    gcc, write_temp_file, Cacheable, CompileCommand, CompilerArguments, Language,
};
use crate::mock_command::{CommandCreator, CommandCreatorSync, RunCommand};
use crate::util::{run_input_output, OsStrExt};
use crate::{counted_array, dist};
use async_trait::async_trait;
use fs::File;
use fs_err as fs;
use log::Level::Trace;
use regex::Regex;
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::process;
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

    #[allow(clippy::manual_try_fold)]
    fn generate_compile_commands(
        &self,
        path_transformer: &mut dist::PathTransformer,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
        rewrite_includes_only: bool,
    ) -> Result<(CompileCommand, Option<dist::CompileCommand>, Cacheable)> {

        let tempdir = tempfile::Builder::new().prefix("sccache-nvcc").tempdir()?.into_path();

        let mut arguments: Vec<OsString> = vec![
            "--keep".into(),
            "--dryrun".into(),
        ];

        let mut unhashed_args = parsed_args.unhashed_args.clone();
        let keep_files = {
            if let Some(idx) = unhashed_args.iter().position(|x| x == "-keep" || x == "--keep" || x == "-save-temps" || x == "--save-temps") {
                unhashed_args.splice(idx..(idx+1), []);
                trace!("--keep, unhashed_args: {:?}", unhashed_args);
                Some(cwd.to_path_buf())
            } else if let Some(idx) = unhashed_args.iter().position(|x| x == "-keep-dir" || x == "--keep-dir") {
                let dir = PathBuf::from(unhashed_args[idx+1].as_os_str());
                let dir = if dir.is_absolute() { dir } else { cwd.join(dir) };
                unhashed_args.splice(idx..(idx+2), []);
                trace!("--keep-dir: {:?}, unhashed_args: {:?}", dir, unhashed_args);
                Some(dir)
            } else {
                None
            }
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

        let output = match parsed_args.outputs.get("obj") {
            Some(obj) => obj.path.clone(),
            None => return Err(anyhow!("Missing object file output")),
        };

        let output = if output.is_absolute() {
            output
        } else {
            cwd.join(output)
        };

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

        arguments.push(parsed_args.input.canonicalize()?.into());

        let mut nvcc_env_vars = env_vars.to_vec();

        get_nvcc_subcommand_groups(
            executable,
            &arguments,
            tempdir.as_path(),
            &mut nvcc_env_vars
        )
            .and_then(|cmds| {

                let (cmd_exe, cmd_arg, cmd_pre) = if cfg!(target_os = "windows") {
                    ("cmd", "/C", "")
                } else {
                    ("sh", "-c", "exec ")
                };

                let exec_cmd = |cmd: &str| -> Result<i32> {
                    let cmd = (cmd_pre.to_owned() + cmd).to_owned();
                    trace!("executing nvcc subcommand (cwd={:?}): `{:?} {:?} '{:?}'`", tempdir.as_path(), cmd_exe, cmd_arg, cmd);
                    std::process::Command::new(cmd_exe)
                        .env_clear()
                        .current_dir(tempdir.as_path())
                        .envs(nvcc_env_vars.clone())
                        .args(vec![cmd_arg.into(), cmd])
                        .stdin(process::Stdio::null())
                        .spawn()
                        .and_then(|proc| proc.wait_with_output())
                        .map_err(|e| anyhow!(e.to_string()))
                        .and_then(|output| {
                            match output.status.code() {
                                Some(code) => {
                                    if code == 0 {
                                        Ok(code)
                                    } else {
                                        Err(ProcessError(output).into())
                                    }
                                },
                                _ => Err(ProcessError(output).into())
                            }
                        })
                };

                let cmds_flat = cmds.iter().flatten().cloned().collect::<Vec<_>>();

                let res1 = cmds_flat.iter().fold(Ok(0), |ret, cmd| ret.and_then(|_| exec_cmd(cmd)));
                let res2 = {
                    let src = tempdir.as_path();
                    match keep_files {
                        Some(dst) => {
                            fs::create_dir_all(dst.as_path()).and_then(|_|
                                fs::read_dir(src)?
                                    .filter_map(|p| p.ok())
                                    .fold(Ok(()), |res, p| res.and_then(|_|
                                        fs::rename(p.path(), dst.join(p.file_name()))
                                    ))
                            )
                        },
                        None => Ok(())
                    }
                };
                let res3 = fs::remove_dir_all(tempdir.as_path());

                res1.and(res2.map_err(|e| anyhow!(e.to_string())))
                    .and(res3.map_err(|e| anyhow!(e.to_string())))
                    .map(|_| (CompileCommand {
                        cwd: cwd.into(),
                        env_vars: nvcc_env_vars.to_owned(),
                        executable: cmd_exe.into(),
                        arguments: vec![
                            cmd_arg.into(),
                            "".into()
                        ],
                    }, None, Cacheable::Yes))
        })
    }
}

#[allow(clippy::manual_try_fold)]
fn get_nvcc_subcommand_groups(
    executable: &Path,
    arguments: &[OsString],
    cwd: &Path,
    env_vars: &mut Vec<(OsString, OsString)>,
) -> Result<Vec<Vec<String>>> {

    let sccache_bin = std::env::current_exe().unwrap();
    let sccache_str = sccache_bin.to_str().unwrap();

    let mut env_path: OsString = std::env::var("PATH").unwrap().into();
    for (key, val) in env_vars.iter() {
        if key == "PATH" {
            env_path = val.clone();
            break;
        }
    }

    trace!("nvcc dryrun cmd: {:?} {:?}", executable, arguments);

    let mut nvcc_dryrun_cmd = std::process::Command::new(executable)
        .env_clear()
        .args(arguments)
        .envs(env_vars.to_vec())
        .current_dir(cwd)
        .stdin(process::Stdio::null())
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::piped())
        .spawn()
        .unwrap();

    fn select_lines_with_hash_dollar_space(line: io::Result<String>) -> Result<String> {
        match line {
            Ok(line) => {
                let re = Regex::new(r"^#\$ (.*)$").unwrap();
                match re.captures(line.as_str()) {
                    Some(caps) => {
                        let (_, [rest]) = caps.extract();
                        Ok(rest.to_string())
                    },
                    _ => Err(anyhow!("nvcc error: {:?}", line)),
                }
            },
            Err(e) => Err(e.into())
        }
    }

    fn select_nvcc_env_vars(
        env_vars: &mut Vec<(OsString, OsString)>,
        env_path: &mut OsString,
        line: &str
    ) -> Result<Option<String>> {
        if let Some(var) = Regex::new(r"^([_A-Z]+)=(.*)$").unwrap().captures(line) {
            let (_, [var, val]) = var.extract();
            if var == "PATH" {
                env_path.clear();
                env_path.push(val);
            }
            // Handle the special `_SPACE_= ` line
            if val == " " {
                env_vars.extend(vec![(var.into(), val.into())]);
            } else {
                env_vars.extend(vec![(
                    var.into(),
                    val.trim()
                        .split(" ")
                        .map(|x| x
                            .trim_start_matches("\"")
                            .trim_end_matches("\""))
                        .collect::<Vec<_>>()
                        .join(" ")
                        .into()
                )]);
            }
            return Ok(None);
        }
        Ok(Some(line.to_string()))
    }

    fn skip_rm_fatbin(line: &str) -> Option<String> {
        if Regex::new(r"^rm .*$").unwrap().is_match(line) {
            return None;
        }
        Some(line.to_string())
    }

    fn passthrough_host_compiler_preprocessor(line: &str) -> Option<String> {
        if Regex::new(r".* -E .*$").unwrap().is_match(line) {
            return Some(line.to_string())
        }
        None
    }

    fn prefix_device_compiler_calls(sccache_str: &str, line: &str) -> Option<String> {
        if Regex::new(r"^(cicc|ptxas) (.*)$").unwrap().is_match(line) {
            return Some(sccache_str.to_owned() + " " + line);
        }
        None
    }

    fn passthrough_cudafe_and_fatbinary(line: &str) -> Option<String> {
        if Regex::new(r"^(cudafe\+\+|fatbinary) (.*)$").unwrap().is_match(line) {
            return Some(line.to_string());
        }
        None
    }

    let stderr = nvcc_dryrun_cmd.stderr.as_mut().unwrap();
    let reader = io::BufReader::new(stderr);
    let nvcc_subcommand_groups = reader
        .lines()
        .map(|line| {
            // Ok(line)
            // Select lines that match the `#$ ` prefix from nvcc --dryrun
            select_lines_with_hash_dollar_space(line)
            // Select the envvar lines and add them to the env_vars list
            .and_then(|line| select_nvcc_env_vars(env_vars, &mut env_path, &line))
            .map(|line|
                // Ignore the `rm *.fatbin` line
                line.and_then(|line| skip_rm_fatbin(&line))
                    .and_then(|line| None
                        // Pass through host compiler preprocessor calls
                        .or_else(|| passthrough_host_compiler_preprocessor(&line))
                        // Prefix `cicc` and `ptxas` invocations with `sccache`
                        .or_else(|| prefix_device_compiler_calls(sccache_str, &line))
                        // Pass through `cudafe++` and `fatbinary` calls
                        .or_else(|| passthrough_cudafe_and_fatbinary(&line))
                        // Prefix non-preprocessor host compiler invocations with `sccache`
                        .or_else(|| Some(sccache_str.to_owned() + " " + line.as_str())))
            )
        })
        .fold(Ok(vec![]), |acc, res| {
            acc.and_then(|mut groups| {
                res.map(|line| {
                    line.map_or(groups.clone(), |line| {
                        if Regex::new(r".* -E .*$").unwrap().is_match(line.as_str()) {
                            groups.push(vec![]);
                        }
                        let idx = groups.len() - 1;
                        groups[idx].push(line);
                        groups
                    })
                })
            })
        });

    match nvcc_dryrun_cmd.wait_with_output() {
        Ok(output) => {
            let code = output.status.code().unwrap();
            if code == 0 {
                nvcc_subcommand_groups
            } else {
                Err(ProcessError(output).into())
            }
        },
        Err(err) => Err(anyhow!(err.to_string()))
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
