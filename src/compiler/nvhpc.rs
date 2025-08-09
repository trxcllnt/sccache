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

use crate::compiler::{
    args::*,
    c::{CCompilerImpl, CCompilerKind, ParsedArguments, PreprocessorOutput},
    gcc::{self, ArgData::*},
    Cacheable, CompileCommandImpl, CompilerArguments,
};
use crate::mock_command::CommandCreatorSync;
use crate::{counted_array, dist};
use async_trait::async_trait;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use tempfile::TempPath;

use crate::errors::*;

/// A unit struct on which to implement `CCompilerImpl`.
#[derive(Clone, Debug)]
pub struct Nvhpc {
    /// true iff this is nvc++.
    pub nvcplusplus: bool,
    pub version: Option<String>,
}

#[async_trait]
impl CCompilerImpl for Nvhpc {
    fn kind(&self) -> CCompilerKind {
        CCompilerKind::Nvhpc
    }
    fn plusplus(&self) -> bool {
        self.nvcplusplus
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
        gcc::parse_arguments(
            arguments,
            cwd,
            (&gcc::ARGS[..], &ARGS[..]),
            self.nvcplusplus,
            self.kind(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn preprocess<T>(
        &self,
        _service: &crate::server::SccacheService<T>,
        creator: &T,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
        rewrite_includes_only: bool,
        generate_dependencies: bool,
        include_line_numbers: bool,
    ) -> Result<PreprocessorOutput>
    where
        T: CommandCreatorSync,
    {
        // Chain the dependency generation and preprocessor commands to emulate a `proper` front end
        let dependencies = if generate_dependencies || !parsed_args.dependency_args.is_empty() {
            if let Some((depfile, _)) = self
                .generate_dependencies(creator, executable, parsed_args, cwd, env_vars)
                .await?
            {
                Some(gcc::parse_dependencies(parsed_args, cwd, &depfile).await?)
            } else {
                None
            }
        } else {
            None
        };

        let extra_preprocessor_flags = [
            // nvc++ must be directed to output to stdout
            vec!["-o".into(), "-".into()],
            if include_line_numbers {
                vec![]
            } else {
                vec!["-P".into()]
            },
        ]
        .concat();

        gcc::preprocess(
            creator,
            executable,
            // nvc++ only preprocesses when there's no dependency flags
            &ParsedArguments {
                dependency_args: vec![],
                ..parsed_args.clone()
            },
            cwd,
            env_vars,
            self.kind(),
            rewrite_includes_only,
            false, // generate_dependencies
            &extra_preprocessor_flags,
        )
        .await
        .map(|res| {
            if let PreprocessorOutput::File(_) = res {
                return res;
            }
            let out = match res {
                PreprocessorOutput::Output(out)
                | PreprocessorOutput::OutputWithDepedencies(out, _) => out,
                _ => unreachable!(),
            };
            if let Some(deps) = dependencies {
                PreprocessorOutput::OutputWithDepedencies(out, deps)
            } else {
                PreprocessorOutput::Output(out)
            }
        })
    }

    async fn generate_dependencies<T>(
        &self,
        creator: &T,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
    ) -> Result<Option<(PathBuf, Option<TempPath>)>>
    where
        T: CommandCreatorSync,
    {
        gcc::generate_dependencies(creator, executable, parsed_args, cwd, env_vars, self.kind())
            .await
            .map(Some)
    }

    fn generate_compile_commands(
        &self,
        path_transformer: &mut dist::PathTransformer,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
        rewrite_includes_only: bool,
        _hash_key: &str,
    ) -> Result<(
        impl CompileCommandImpl,
        Option<dist::CompileCommand>,
        Cacheable,
    )> {
        gcc::generate_compile_commands(
            path_transformer,
            executable,
            parsed_args,
            cwd,
            env_vars,
            self.kind(),
            rewrite_includes_only,
        )
    }
}

counted_array!(pub static ARGS: [ArgInfo<gcc::ArgData>; _] = [
    //todo: refactor show_includes into dependency_args
    take_arg!("--gcc-toolchain", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("--include-path", PathBuf, CanBeSeparated, PreprocessorArgumentPath),
    take_arg!("--linker-options", OsString, CanBeSeparated, PassThrough),
    flag!("--nvcchost", PassThroughFlag),
    take_arg!("--preinclude", PathBuf, CanBeSeparated, PreprocessorArgumentPath),
    take_arg!("--system-include-path", PathBuf, CanBeSeparated, PreprocessorArgumentPath),

    take_arg!("-Mconcur", OsString, CanBeSeparated('='), PassThrough),
    flag!("-Mnostdlib", PreprocessorArgumentFlag),
    take_arg!("-Werror", OsString, CanBeSeparated, PreprocessorArgument),
    take_arg!("-Xcompiler", OsString, CanBeSeparated('='), PreprocessorArgument),
    take_arg!("-Xfatbinary", OsString, CanBeSeparated, PassThrough),
    take_arg!("-Xlinker", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-Xnvlink", OsString, CanBeSeparated, PassThrough),
    take_arg!("-Xptxas", OsString, CanBeSeparated, PassThrough),
    take_arg!("-acc", OsString, CanBeSeparated('='), PassThrough),
    flag!("-acclibs", PassThroughFlag),
    take_arg!("-c++", OsString, Concatenated, Standard),
    flag!("-c++libs", PassThroughFlag),
    flag!("-cuda", PreprocessorArgumentFlag),
    flag!("-cudaforlibs", PassThroughFlag),
    take_arg!("-cudalib", OsString, CanBeSeparated('='), PassThrough),
    flag!("-fortranlibs", PassThroughFlag),
    flag!("-gopt", PassThroughFlag),
    take_arg!("-gpu", OsString, Concatenated('='), PassThrough),
    take_arg!("-mcmodel", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-mcpu", OsString, CanBeSeparated('='), PassThrough),
    flag!("-noswitcherror", PassThroughFlag),
    take_arg!("-ta", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-target", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-tp", OsString, CanBeSeparated('='), PassThrough),
    take_arg!("-x", OsString, CanBeSeparated('='), Language)
]);

#[cfg(test)]
mod test {
    use super::*;
    use crate::compiler::c::ArtifactDescriptor;
    use crate::compiler::{Language, *};

    fn parse_arguments_(arguments: Vec<String>) -> CompilerArguments<ParsedArguments> {
        let arguments = arguments.iter().map(OsString::from).collect::<Vec<_>>();
        Nvhpc {
            nvcplusplus: false,
            version: None,
        }
        .parse_arguments(&arguments, ".".as_ref(), &[])
    }

    macro_rules! parses {
        ( $( $s:expr ),* ) => {
            match parse_arguments_(vec![ $( $s.to_string(), )* ]) {
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
        assert!(a.common_args.is_empty());
    }

    #[test]
    fn test_parse_arguments_simple_cxx() {
        let a = parses!("-c", "foo.cxx", "-o", "foo.o");
        assert_eq!(Some("foo.cxx"), a.input.to_str());
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
        assert!(a.preprocessor_args.is_empty());
        assert!(a.common_args.is_empty());
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
            "-isystem",
            "/system/include/file",
            "-gpu=ccnative",
            "-Werror",
            "an_error"
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
                "an_error"
            ],
            a.preprocessor_args
        );
        assert!(a.dependency_args.is_empty());
        assert_eq!(ovec!["-fabc", "-gpu=ccnative"], a.common_args);
    }

    #[test]
    fn test_parse_md_mt_flags_cxx() {
        let a = parses!(
            "-x", "c++", "-c", "foo.c", "-fabc", "-MD", "-MT", "foo.o", "-MF", "foo.o.d", "-o",
            "foo.o"
        );
        assert_eq!(Some("foo.c"), a.input.to_str());
        assert_eq!(Language::Cxx, a.language);
        assert_eq!(Some("-c"), a.compilation_flag.to_str());
        assert_eq!(Some("foo.o.d"), a.depfile.unwrap().to_str());
        assert_map_contains!(
            a.outputs,
            (
                "obj",
                ArtifactDescriptor {
                    path: "foo.o".into(),
                    optional: false,
                    must_be_non_empty: false,
                }
            ),
            (
                "dep",
                ArtifactDescriptor {
                    path: "foo.o.d".into(),
                    optional: false,
                    must_be_non_empty: false,
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
            "c++",
            "-cuda",
            "-gpu=cc60,cc70",
            "-c",
            "foo.c",
            "-o",
            "foo.o"
        );
        assert_eq!(Some("foo.c"), a.input.to_str());
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
        assert_eq!(ovec!["-cuda"], a.preprocessor_args);
        assert_eq!(ovec!["-gpu=cc60,cc70"], a.common_args);
    }

    #[test]
    fn test_parse_generate_code_mem_flags() {
        let a = parses!(
            "-x",
            "c++",
            "-cuda",
            "-gpu=mem:separate",
            "-c",
            "foo.c",
            "-o",
            "foo.o"
        );
        assert_eq!(Some("foo.c"), a.input.to_str());
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
        assert_eq!(ovec!["-cuda"], a.preprocessor_args);
        assert_eq!(ovec!["-gpu=mem:separate"], a.common_args);
    }

    #[test]
    fn test_parse_cant_cache_flags() {
        assert_eq!(
            CompilerArguments::CannotCache("-E", None),
            parse_arguments_(stringvec!["-c", "foo.c", "-o", "foo.o", "-E"])
        );
        assert_eq!(
            CompilerArguments::CannotCache("-M", None),
            parse_arguments_(stringvec!["-c", "foo.c", "-o", "foo.o", "-M"])
        );
    }
}
