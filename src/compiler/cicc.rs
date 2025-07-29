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
use crate::compiler::c::{
    ArtifactDescriptor, CCompilerImpl, CCompilerKind, ParsedArguments, PreprocessorOutput,
};
use crate::compiler::{
    CCompileCommand, Cacheable, CompileCommand, CompilerArguments, Language, SingleCompileCommand,
};
use crate::{counted_array, dist, util::OsStrExt};

use crate::mock_command::CommandCreatorSync;

use async_trait::async_trait;
use itertools::Itertools;

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::{debug_if_trace, errors::*};

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
        _service: &crate::server::SccacheService<T>,
        _creator: &T,
        _executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        _env_vars: &[(OsString, OsString)],
        _rewrite_includes_only: bool,
        _generate_dependencies: bool,
        _include_line_numbers: bool,
    ) -> Result<PreprocessorOutput>
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

pub fn parse_arguments<S>(
    arguments: &[OsString],
    cwd: &Path,
    language: Language,
    arg_info: S,
) -> CompilerArguments<ParsedArguments>
where
    S: Clone + SearchableArgInfo<ArgData>,
{
    // Initial parse just to get the input and output
    let (input, output, mut args) = parse_input_output(
        arguments.iter().cloned(),
        arg_info.clone(),
        |data| match data {
            Output(p) => Some(p),
            _ => None,
        },
    );

    if let Some(input) = input.as_ref().and_then(|s| s.to_str()) {
        if let Some(idx) = args.iter().position(|arg| arg.ends_with(input)) {
            args.splice(idx..idx + 1, []);
        }
    }

    if let Some(output) = output.as_ref().and_then(|s| s.to_str()) {
        if let Some((idx, arg)) = args.iter().find_position(|arg| arg.ends_with(output)) {
            if arg.starts_with(output) {
                // -o <output>
                // --module_id_file_name <module_id>
                args.splice(idx - 1..idx + 1, []);
            } else {
                // -Fi<output>
                // -Fo<output>
                args.splice(idx..idx + 1, []);
            }
        }
    }

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

    let (common_args, unhashed_args, gen_module_id_file, module_id_file_name) =
        ArgsIter::new(args.into_iter(), arg_info)
            .filter_map(|arg| arg.ok())
            .fold(
                (vec![], vec![], false, None),
                |(
                    mut common_args,
                    mut unhashed_args,
                    mut gen_module_id_file,
                    mut module_id_file_name,
                ),
                 arg| {
                    let args = if let Some(Unhashed(_)) = arg.get_data() {
                        &mut unhashed_args
                    } else {
                        &mut common_args
                    };

                    args.extend(
                        arg.get_data()
                            .map(|data| match data {
                                ExtraOutput(path) => {
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
                                    None
                                }
                                GenModuleIdFileFlag => {
                                    gen_module_id_file = true;
                                    None
                                }
                                ModuleIdFileName(path) => {
                                    module_id_file_name = Some(cwd.join(path));
                                    None
                                }
                                _ => None,
                            })
                            .and_then(|arg| arg)
                            .map_or_else(|| arg, |arg| arg)
                            .iter_os_strings(),
                    );

                    (
                        common_args,
                        unhashed_args,
                        gen_module_id_file,
                        module_id_file_name,
                    )
                },
            );

    let mut extra_dist_files = vec![];

    let gen_module_id_file = gen_module_id_file
        && ["--gen_c_file_name", "--stub_file_name"]
            .iter()
            .any(|k| outputs.contains_key(k));

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
        language,
        common_args,
        unhashed_args,
        extra_dist_files: extra_dist_files.clone(),
        extra_hash_files: extra_dist_files,
        ..Default::default()
    })
}

pub async fn preprocess(cwd: &Path, parsed_args: &ParsedArguments) -> Result<PreprocessorOutput> {
    let path = cwd.join(&parsed_args.input);
    let file = tokio::fs::File::open(&path).await?.into_std().await;
    Ok(PreprocessorOutput::File(fs_err::File::from_parts(
        file, path,
    )))
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
            &[output_flag.into(), output.into(), input.into()],
        ]
        .concat(),
        cwd: cwd.to_owned(),
        env_vars: env_vars.to_owned(),
        executable: executable.to_owned(),
    };

    debug_if_trace!(
        "[{}]: {} command: {command}",
        parsed_args.output_pretty(),
        parsed_args.language.as_str(),
    );

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
                    &[
                        output_flag.into(),
                        path_transformer.as_dist(output)?,
                        path_transformer.as_dist(input)?,
                    ],
                ]
                .concat(),
                cwd: path_transformer.as_dist_abs(cwd)?,
                env_vars: dist::osstring_tuples_to_strings(env_vars)?,
                executable: path_transformer
                    .as_dist(dunce::canonicalize(executable).ok()?.as_path())?,
            };

            debug_if_trace!(
                "[{}]: {} dist_command: {command}",
                parsed_args.output_pretty(),
                parsed_args.language.as_str(),
            );

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
