// Copyright 2016 Mozilla Foundation
// SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
use crate::compiler::{Cacheable, ColorMode, CompileCommand, CompilerArguments, Language};
use crate::compiler::c::{ArtifactDescriptor, CCompilerImpl, CCompilerKind, ParsedArguments};
use crate::{counted_array, dist};

use crate::mock_command::{CommandCreator, CommandCreatorSync, RunCommand};

use async_trait::async_trait;

use std::fs;
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process;

use crate::errors::*;

/// A unit struct on which to implement `CCompilerImpl`.
#[derive(Clone, Debug)]
pub struct Cicc {
    pub version: Option<String>,
}

#[async_trait]
impl CCompilerImpl for Cicc {
    fn kind(&self) -> CCompilerKind {
        CCompilerKind::Cicc
    }
    fn plusplus(&self) -> bool { true }
    fn version(&self) -> Option<String> {
        self.version.clone()
    }
    fn parse_arguments(&self, arguments: &[OsString], _cwd: &Path) -> CompilerArguments<ParsedArguments> {
        parse_arguments(arguments, Language::Ptx, &ARGS[..])
    }
    #[allow(clippy::too_many_arguments)]
    async fn preprocess<T>(
        &self,
        _creator: &T,
        _executable: &Path,
        parsed_args: &ParsedArguments,
        _cwd: &Path,
        _env_vars: &[(OsString, OsString)],
        _may_dist: bool,
        _rewrite_includes_only: bool,
        _preprocessor_cache_mode: bool,
    ) -> Result<process::Output>
    where
        T: CommandCreatorSync,
    {
        preprocess(parsed_args).await
    }
    fn generate_compile_commands(
        &self,
        path_transformer: &mut dist::PathTransformer,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
        _rewrite_includes_only: bool,
    ) -> Result<(CompileCommand, Option<dist::CompileCommand>, Cacheable)> {
        generate_compile_commands(
            path_transformer,
            executable,
            parsed_args,
            cwd,
            env_vars
        )
    }
}

pub fn parse_arguments<S>(
    arguments: &[OsString],
    language: Language,
    arg_info: S,
) -> CompilerArguments<ParsedArguments>
where
    S: SearchableArgInfo<ArgData>,
{
    let mut take_next = false;
    let mut inputs = vec![];
    let mut input = OsString::new();
    let mut outputs = HashMap::new();

    let mut common_args = vec![];
    let mut unhashed_args = vec![];

    for arg in ArgsIter::new(arguments.iter().cloned(), arg_info) {
        match arg {
            Ok(arg) => {
                let args = match arg.get_data() {
                    Some(PassThrough(_)) => {
                        take_next = false;
                        &mut common_args
                    },
                    Some(Input(o)) => {
                        take_next = false;
                        let path = o.to_path_buf();
                        inputs.push(path);
                        &mut unhashed_args
                    }
                    Some(Output(o)) => {
                        take_next = false;
                        let path = o.to_path_buf();
                        if let Some(lang) = Language::from_file_name(path.as_path()) {
                            outputs.insert(
                                lang.as_str(),
                                ArtifactDescriptor { path, optional: false }
                            );
                        }
                        match arg.flag_str() {
                            Some("-o") => continue,
                            _ => &mut unhashed_args
                        }
                    },
                    Some(Unhashed(_)) => {
                        take_next = false;
                        &mut unhashed_args
                    },
                    None => match arg {
                        Argument::Raw(ref p) => {
                            if take_next {
                                take_next = false;
                                &mut common_args
                            } else {
                                input.clone_from(p);
                                continue
                            }
                        }
                        Argument::UnknownFlag(ref p) => {
                            let s = p.to_string_lossy();
                            take_next = s.starts_with('-');
                            &mut common_args
                        },
                        _ => unreachable!(),
                    },
                };
                args.extend(arg.iter_os_strings());
            },
            _ => continue,
        };
    }

    CompilerArguments::Ok(ParsedArguments {
        input: input.into(),
        outputs,
        double_dash_input: false,
        language,
        compilation_flag: OsString::new(),
        depfile: None,
        dependency_args: vec![],
        preprocessor_args: vec![],
        common_args,
        arch_args: vec![],
        unhashed_args,
        extra_dist_files: inputs,
        extra_hash_files: vec![],
        msvc_show_includes: false,
        profile_generate: false,
        color_mode: ColorMode::Off,
        suppress_rewrite_includes_only: false,
        too_hard_for_preprocessor_cache_mode: None
    })
}

pub async fn preprocess(
    parsed_args: &ParsedArguments,
) -> Result<process::Output>
{
    std::fs::read(&parsed_args.input)
        .map_err(|e| { anyhow::Error::new(e) })
        .map(|s| {
            process::Output {
                status: process::ExitStatus::default(),
                stdout: s.to_vec(),
                stderr: vec![],
            }
        })
}

pub fn generate_compile_commands(
    path_transformer: &mut dist::PathTransformer,
    executable: &Path,
    parsed_args: &ParsedArguments,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
) -> Result<(CompileCommand, Option<dist::CompileCommand>, Cacheable)> {
    // Unused arguments
    #[cfg(not(feature = "dist-client"))]
    {
        let _ = path_transformer;
    }

    trace!("compile");

    let lang_str = &parsed_args.language.as_str();
    let out_file = match parsed_args.outputs.get(lang_str) {
        Some(obj) => &obj.path,
        None => return Err(anyhow!("Missing {:?} file output", lang_str)),
    };

    let mut arguments: Vec<OsString> = vec![];
    arguments.extend_from_slice(&parsed_args.common_args);
    arguments.extend_from_slice(&parsed_args.unhashed_args);
    arguments.extend(vec![
        (&parsed_args.input).into(),
        "-o".into(),
        out_file.into()
    ]);

    let command = CompileCommand {
        executable: executable.to_owned(),
        arguments,
        env_vars: env_vars.to_owned(),
        cwd: cwd.to_owned(),
    };

    #[cfg(not(feature = "dist-client"))]
    let dist_command = None;
    #[cfg(feature = "dist-client")]
    let dist_command = (|| {
        let mut arguments: Vec<String> = vec![];
        arguments.extend(dist::osstrings_to_strings(&parsed_args.common_args)?);
        arguments.extend(dist::osstrings_to_strings(&parsed_args.unhashed_args)?);
        arguments.extend(vec![
            path_transformer.as_dist(&parsed_args.input)?,
            "-o".into(),
            path_transformer.as_dist(out_file)?,
        ]);
        Some(dist::CompileCommand {
            executable: path_transformer.as_dist(executable.canonicalize().unwrap().as_path())?,
            arguments,
            env_vars: dist::osstring_tuples_to_strings(env_vars)?,
            cwd: path_transformer.as_dist_abs(cwd)?,
        })
    })();

    Ok((command, dist_command, Cacheable::Yes))
}

ArgData! { pub
    PassThrough(OsString),
    Input(PathBuf),
    Output(PathBuf),
    Unhashed(OsString),
}

use self::ArgData::*;

counted_array!(pub static ARGS: [ArgInfo<ArgData>; _] = [
    // These are always randomly generated/different between nvcc invocations
    take_arg!("--gen_c_file_name", OsString, Separated, Unhashed),
    take_arg!("--gen_device_file_name", OsString, Separated, Unhashed),
    take_arg!("--include_file_name", OsString, Separated, Unhashed),
    take_arg!("--module_id_file_name", PathBuf, Separated, Input),
    take_arg!("--stub_file_name", PathBuf, Separated, Output),
    take_arg!("-o", PathBuf, Separated, Output),
]);
