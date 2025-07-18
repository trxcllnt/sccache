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

use crate::compiler::args::*;
use crate::compiler::c::{ArtifactDescriptor, CCompilerImpl, CCompilerKind, ParsedArguments};
use crate::compiler::{
    CCompileCommand, Cacheable, ColorMode, CompileCommand, CompilerArguments, Language,
    SingleCompileCommand,
};
use crate::{counted_array, dist, util::OsStrExt};

use crate::mock_command::{CommandCreatorSync, ProcessOutput};

use async_trait::async_trait;

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
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
    fn plusplus(&self) -> bool {
        true
    }
    fn version(&self) -> Option<String> {
        self.version.clone()
    }
    fn parse_arguments(
        &self,
        arguments: &[OsString],
        cwd: &Path,
        _env_vars: &[(OsString, OsString)],
    ) -> CompilerArguments<ParsedArguments> {
        parse_arguments(arguments, cwd, Language::Ptx, &ARGS[..])
    }
    #[allow(clippy::too_many_arguments)]
    async fn preprocess<T>(
        &self,
        _creator: &T,
        _executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        _env_vars: &[(OsString, OsString)],
        _may_dist: bool,
        _rewrite_includes_only: bool,
        _preprocessor_cache_mode: bool,
    ) -> Result<ProcessOutput>
    where
        T: CommandCreatorSync,
    {
        preprocess(cwd, parsed_args).await
    }
    fn generate_compile_commands<T>(
        &self,
        path_transformer: &mut dist::PathTransformer,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
        _rewrite_includes_only: bool,
        _hash_key: &str,
    ) -> Result<(
        Box<dyn CompileCommand<T>>,
        Option<dist::CompileCommand>,
        Cacheable,
    )>
    where
        T: CommandCreatorSync,
    {
        generate_compile_commands(
            path_transformer,
            executable,
            parsed_args,
            cwd,
            env_vars,
            "-o",
        )
        .map(|(command, dist_command, cacheable)| {
            (CCompileCommand::new(command), dist_command, cacheable)
        })
    }
}

fn should_take_next(arg: &OsStr) -> bool {
    let arg = match *arg.as_encoded_bytes() {
        [b'-', b'-', ..] => arg.split_prefix("--").unwrap_or_default(),
        [b'-', ..] => arg.split_prefix("-").unwrap_or_default(),
        _ => return false,
    };

    // return false on single-letter flags: `-` and `-o`
    !arg.is_empty()
        // return false on --foo=bar
        && !arg.contains("=")
        // return false on -f64.123
        && arg.to_str().and_then(|s| {
            None.or_else(|| s.parse::<f64>().ok().map(|_| true))
                .or_else(|| s.parse::<i128>().ok().map(|_| true))
                .or_else(|| s.parse::<u128>().ok().map(|_| true))
        }).is_none()
}

pub fn parse_arguments<S>(
    arguments: &[OsString],
    cwd: &Path,
    language: Language,
    arg_info: S,
) -> CompilerArguments<ParsedArguments>
where
    S: SearchableArgInfo<ArgData>,
{
    let args = arguments.to_vec();
    let mut input = None;

    let mut take_next = false;
    let mut outputs = HashMap::new();
    let mut extra_dist_files = vec![];
    let mut gen_module_id_file = false;
    let mut module_id_file_name = Option::<PathBuf>::None;

    let mut common_args = vec![];
    let mut unhashed_args = vec![];

    for arg in ArgsIter::new(args.iter().cloned(), arg_info) {
        match arg {
            Ok(arg) => {
                let args = match arg.get_data() {
                    Some(ExtraOutput(path)) => {
                        take_next = false;
                        if let Some(flag) = arg.flag_str() {
                            outputs.insert(
                                flag,
                                ArtifactDescriptor {
                                    path: path.to_owned(),
                                    optional: false,
                                    must_be_non_empty: false,
                                },
                            );
                        }
                        &mut common_args
                    }
                    Some(GenModuleIdFileFlag) => {
                        take_next = false;
                        gen_module_id_file = true;
                        &mut common_args
                    }
                    Some(ModuleIdFileName(path)) => {
                        take_next = false;
                        module_id_file_name = Some(cwd.join(path));
                        &mut common_args
                    }
                    Some(Output(path)) => {
                        take_next = false;
                        outputs.insert(
                            "obj",
                            ArtifactDescriptor {
                                path: path.to_owned(),
                                optional: false,
                                must_be_non_empty: false,
                            },
                        );
                        continue;
                    }
                    Some(PassThroughFlag) | Some(PassThrough(_)) => {
                        take_next = false;
                        &mut common_args
                    }
                    Some(Unhashed(_)) => {
                        take_next = false;
                        &mut unhashed_args
                    }
                    None => match arg {
                        Argument::Raw(ref p) => {
                            trace!("{language:?} raw (take_next={take_next}): {p:?}");
                            if take_next {
                                take_next = should_take_next(p);
                                &mut common_args
                            } else {
                                if input.is_none() {
                                    trace!("{language:?} input: {p:?}");
                                    input = Some(p.clone());
                                }
                                continue;
                            }
                        }
                        Argument::UnknownFlag(ref p) => {
                            take_next = should_take_next(p);
                            trace!("{language:?} unknown (take_next={take_next}): {p:?}");
                            &mut common_args
                        }
                        _ => unreachable!(),
                    },
                };
                let disposition = match arg.flag_str() {
                    Some(s) if s.len() == 2 => NormalizedDisposition::Concatenated,
                    _ => NormalizedDisposition::Separated,
                };
                args.extend(arg.normalize(disposition).iter_os_strings());
            }
            _ => continue,
        };
    }

    let gen_module_id_file = gen_module_id_file
        && (outputs.contains_key("--gen_c_file_name") || outputs.contains_key("--stub_file_name"));

    match (gen_module_id_file, module_id_file_name) {
        (true, Some(path)) => {
            outputs.insert(
                "--module_id_file_name",
                ArtifactDescriptor {
                    path,
                    optional: false,
                    must_be_non_empty: true,
                },
            );
        }
        (false, Some(path)) if path.exists() => {
            extra_dist_files.push(path.clone());
        }
        _ => {}
    }

    let input = match input {
        Some(i) => i,
        // We can't cache compilation without an input.
        None => return CompilerArguments::CannotCache("no input file", None),
    };

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
        extra_dist_files: extra_dist_files.clone(),
        extra_hash_files: extra_dist_files,
        msvc_show_includes: false,
        profile_generate: false,
        color_mode: ColorMode::Off,
        suppress_rewrite_includes_only: false,
        too_hard_for_preprocessor_cache_mode: None,
    })
}

pub async fn preprocess(cwd: &Path, parsed_args: &ParsedArguments) -> Result<ProcessOutput> {
    std::fs::read(cwd.join(&parsed_args.input))
        .map_err(anyhow::Error::new)
        .map(|s| {
            process::Output {
                status: process::ExitStatus::default(),
                stdout: s,
                stderr: vec![],
            }
            .into()
        })
}

pub fn generate_compile_commands(
    path_transformer: &mut dist::PathTransformer,
    executable: &Path,
    parsed_args: &ParsedArguments,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
    output_flag: &str,
) -> Result<(
    SingleCompileCommand,
    Option<dist::CompileCommand>,
    Cacheable,
)> {
    // Unused arguments
    #[cfg(not(feature = "dist-client"))]
    {
        let _ = path_transformer;
    }

    let input = parsed_args.input.as_path();
    let output = match parsed_args.outputs.get("obj") {
        Some(obj) => &obj.path,
        None => {
            return Err(anyhow!(
                "Missing {} file output",
                parsed_args.language.as_str()
            ))
        }
    };

    let command = SingleCompileCommand {
        arguments: [
            &parsed_args.common_args[..],
            &parsed_args.unhashed_args[..],
            &(if output_flag == "-o" {
                [input.into(), output_flag.into(), output.into()]
            } else {
                [output_flag.into(), output.into(), input.into()]
            }),
        ]
        .concat(),
        cwd: cwd.to_owned(),
        env_vars: env_vars.to_owned(),
        executable: executable.to_owned(),
    };

    if log_enabled!(log::Level::Trace) {
        trace!(
            "[{}]: {} command: cd {cwd:?} && {command}",
            output.display(),
            parsed_args.language.as_str(),
        );
    }

    Ok((
        command,
        #[cfg(not(feature = "dist-client"))]
        None,
        #[cfg(feature = "dist-client")]
        (|| {
            let command = dist::CompileCommand {
                arguments: [
                    &dist::osstrings_to_strings(&parsed_args.common_args)?[..],
                    // Don't send unhashed args (i.e. `-split-compile`) to the build cluster
                    // &dist::osstrings_to_strings(&parsed_args.unhashed_args)?[..],
                    &(if output_flag == "-o" {
                        [
                            path_transformer.as_dist(input)?,
                            output_flag.into(),
                            path_transformer.as_dist(output)?,
                        ]
                    } else {
                        [
                            output_flag.into(),
                            path_transformer.as_dist(output)?,
                            path_transformer.as_dist(input)?,
                        ]
                    }),
                ]
                .concat(),
                cwd: path_transformer.as_dist_abs(cwd)?,
                env_vars: dist::osstring_tuples_to_strings(env_vars)?,
                executable: path_transformer
                    .as_dist(dunce::canonicalize(executable).ok()?.as_path())?,
            };

            if log_enabled!(log::Level::Trace) {
                trace!(
                    "[{}]: {} dist_command: cd {cwd:?} && {command}",
                    output.display(),
                    parsed_args.language.as_str(),
                );
            }

            Some(command)
        })(),
        Cacheable::Yes,
    ))
}

ArgData! { pub
    ExtraOutput(PathBuf),
    GenModuleIdFileFlag,
    ModuleIdFileName(PathBuf),
    Output(PathBuf),
    PassThroughFlag,
    PassThrough(OsString),
    Unhashed(OsString),
}

use self::ArgData::*;

counted_array!(pub static ARGS: [ArgInfo<ArgData>; _] = [
    flag!("--emit-optix-ir", PassThroughFlag),
    take_arg!("--gen_c_file_name", PathBuf, Separated, ExtraOutput),
    take_arg!("--gen_device_file_name", PathBuf, Separated, ExtraOutput),
    flag!("--gen_module_id_file", GenModuleIdFileFlag),
    take_arg!("--include_file_name", OsString, Separated, PassThrough),
    take_arg!("--module_id_file_name", PathBuf, Separated, ModuleIdFileName),
    take_arg!("--stub_file_name", PathBuf, Separated, ExtraOutput),
    flag!("-lto", PassThroughFlag),
    take_arg!("-o", PathBuf, Separated, Output),
    take_arg!("-olto", PathBuf, Separated, ExtraOutput),
]);
