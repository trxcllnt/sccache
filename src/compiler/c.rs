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

use crate::cache::{FileObjectSource, PreprocessorCacheModeConfig, Storage};
use crate::compiler::preprocessor_cache::preprocessor_cache_entry_hash_key;
use crate::compiler::{
    Cacheable, ColorMode, Compilation, CompileCommand, Compiler, CompilerArguments, CompilerHasher,
    CompilerKind, HashResult, Language,
};
#[cfg(feature = "dist-client")]
use crate::compiler::{DistPackagers, NoopOutputsRewriter};
use crate::dist;
#[cfg(feature = "dist-client")]
use crate::dist::pkg::{self, InputsWriter};
use crate::mock_command::{CommandCreatorSync, ProcessOutput};
use crate::util::{hash_all, Digest, HashToDigest, TimeMacroFinder};
use async_trait::async_trait;
use bytes::Buf;
use fs_err as fs;
use itertools::Itertools;
use once_cell::sync::Lazy;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::hash::Hash;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::errors::*;

use super::preprocessor_cache::PreprocessorCacheEntry;
use super::CacheControl;

/// A generic implementation of the `Compiler` trait for C/C++ compilers.
#[derive(Clone)]
pub struct CCompiler<I>
where
    I: CCompilerImpl,
{
    executable: PathBuf,
    executable_digest: String,
    compiler: I,
}

/// A generic implementation of the `CompilerHasher` trait for C/C++ compilers.
#[derive(Debug, Clone)]
pub struct CCompilerHasher<I>
where
    I: CCompilerImpl,
{
    parsed_args: ParsedArguments,
    executable: PathBuf,
    executable_digest: String,
    compiler: I,
}

/// Artifact produced by a C/C++ compiler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactDescriptor {
    /// Path to the artifact.
    pub path: PathBuf,
    /// Whether the artifact is an optional object file.
    pub optional: bool,
    /// Whether the artifact size must be greater than 0 bytes.
    pub must_be_non_empty: bool,
}

/// The results of parsing a compiler commandline.
#[allow(dead_code)]
#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct ParsedArguments {
    /// The input source file.
    pub input: PathBuf,
    /// Whether to prepend the input with `--`
    pub double_dash_input: bool,
    /// The type of language used in the input source file.
    pub language: Language,
    /// The flag required to compile for the given language
    pub compilation_flag: OsString,
    /// The file in which to generate dependencies.
    pub depfile: Option<PathBuf>,
    /// Output files and whether it's optional, keyed by a simple name, like "obj".
    pub outputs: HashMap<&'static str, ArtifactDescriptor>,
    /// Commandline arguments for dependency generation.
    pub dependency_args: Vec<OsString>,
    /// Commandline arguments for the preprocessor (not including common_args).
    pub preprocessor_args: Vec<OsString>,
    /// Commandline arguments for the preprocessor or the compiler.
    pub common_args: Vec<OsString>,
    /// Commandline arguments for the compiler that specify the architecture given
    pub arch_args: Vec<OsString>,
    /// Commandline arguments for the preprocessor or the compiler that don't affect the computed hash.
    pub unhashed_args: Vec<OsString>,
    /// Extra unhashed files that need to be sent along with dist compiles.
    pub extra_dist_files: Vec<PathBuf>,
    /// Extra files that need to have their contents hashed.
    pub extra_hash_files: Vec<PathBuf>,
    /// Whether or not the `-showIncludes` argument is passed on MSVC
    pub msvc_show_includes: bool,
    /// Whether the compilation is generating profiling or coverage data.
    pub profile_generate: bool,
    /// The color mode.
    pub color_mode: ColorMode,
    /// arguments are incompatible with rewrite_includes_only
    pub suppress_rewrite_includes_only: bool,
    /// Arguments are incompatible with preprocessor cache mode
    pub too_hard_for_preprocessor_cache_mode: Vec<OsString>,
}

impl ParsedArguments {
    pub fn output_pretty(&self) -> Cow<'_, str> {
        self.outputs
            .get("obj")
            .map(|o| o.path.as_os_str())
            .and_then(|s| s.to_str().map(Cow::Borrowed))
            .unwrap_or(Cow::Borrowed("Unknown filename"))
    }
}

/// A generic implementation of the `Compilation` trait for C/C++ compilers.
#[derive(Debug)]
struct CCompilation<T: CommandCreatorSync, I: CCompilerImpl> {
    service: crate::server::SccacheService<T>,
    creator: T,
    parsed_args: ParsedArguments,
    executable: PathBuf,
    compiler: I,
    cwd: PathBuf,
    env_vars: Vec<(OsString, OsString)>,
    rewrite_includes_only: bool,
}

/// Supported C compilers.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CCompilerKind {
    /// GCC
    Gcc,
    /// clang
    Clang,
    /// Diab
    Diab,
    /// Microsoft Visual C++
    Msvc,
    /// NVIDIA CUDA compiler
    Nvcc,
    /// NVIDIA CUDA front-end
    CudaFE,
    /// NVIDIA CUDA optimizer and PTX generator
    Cicc,
    /// NVIDIA CUDA PTX assembler
    Ptxas,
    /// NVIDIA hpc c, c++ compiler
    Nvhpc,
    /// Tasking VX
    TaskingVX,
}

impl From<&str> for CCompilerKind {
    fn from(kind: &str) -> Self {
        match kind {
            "clang" | "clang++" | "clang-cl" => CCompilerKind::Clang,
            "diab" => CCompilerKind::Diab,
            "gcc" | "g++" => CCompilerKind::Gcc,
            "cl" => CCompilerKind::Msvc,
            "nvcc" => CCompilerKind::Nvcc,
            "cudafe++" => CCompilerKind::CudaFE,
            "cicc" => CCompilerKind::Cicc,
            "ptxas" => CCompilerKind::Ptxas,
            "nvc" | "nvc++" => CCompilerKind::Nvhpc,
            "taskingvx" => CCompilerKind::TaskingVX,
            _ => unreachable!(),
        }
    }
}

impl From<CCompilerKind> for CompilerKind {
    fn from(kind: CCompilerKind) -> Self {
        CompilerKind::C(kind)
    }
}

impl From<&CCompilerKind> for CompilerKind {
    fn from(kind: &CCompilerKind) -> Self {
        CompilerKind::C(kind.clone())
    }
}

#[derive(Debug)]
pub enum PreprocessorOutput {
    // cudafe++, cicc, and ptxas return this. Their output is always
    // saved to an intermediate file, and no preprocessor caching should
    // be done on them.
    File(fs::File),
    Output(ProcessOutput),
    OutputWithDepedencies(ProcessOutput, Vec<PathBuf>),
}

/// An interface to a specific C compiler.
#[async_trait]
pub trait CCompilerImpl: Clone + fmt::Debug + Send + Sync + 'static {
    /// Return the kind of compiler.
    fn kind(&self) -> CCompilerKind;
    /// Return true iff this is g++ or clang++.
    fn plusplus(&self) -> bool;
    /// Return the compiler version reported by the compiler executable.
    fn version(&self) -> Option<String>;
    /// Determine whether `arguments` are supported by this compiler.
    fn parse_arguments(
        &self,
        arguments: &[OsString],
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
    ) -> CompilerArguments<ParsedArguments>;
    /// Run the C preprocessor with the specified set of arguments.
    #[allow(clippy::too_many_arguments)]
    async fn preprocess<T>(
        &self,
        service: &crate::server::SccacheService<T>,
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
        T: CommandCreatorSync;
    /// Generate a command that can be used to invoke the C compiler to perform
    /// the compilation.
    #[allow(clippy::too_many_arguments)]
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
        T: CommandCreatorSync;
}

impl<I> CCompiler<I>
where
    I: CCompilerImpl,
{
    pub async fn new(compiler: I, executable: PathBuf) -> Result<CCompiler<I>> {
        let digest = Digest::file(&executable).await?;

        Ok(CCompiler {
            executable,
            executable_digest: {
                if let Some(version) = compiler.version() {
                    let mut m = Digest::new();
                    m.update(digest.as_bytes());
                    m.update(version.as_bytes());
                    m.finish()
                } else {
                    digest
                }
            },
            compiler,
        })
    }

    fn extract_rocm_arg(args: &ParsedArguments, flag: &str) -> Option<PathBuf> {
        args.common_args.iter().find_map(|arg| match arg.to_str() {
            Some(sarg) if sarg.starts_with(flag) => {
                Some(PathBuf::from(sarg[arg.len()..].to_string()))
            }
            _ => None,
        })
    }

    fn extract_rocm_env(env_vars: &[(OsString, OsString)], name: &str) -> Option<PathBuf> {
        env_vars.iter().find_map(|(k, v)| match v.to_str() {
            Some(path) if k == name => Some(PathBuf::from(path.to_string())),
            _ => None,
        })
    }

    // See https://clang.llvm.org/docs/HIPSupport.html for details regarding the
    // order in which the environment variables and command-line arguments control the
    // directory to search for bitcode libraries.
    fn search_hip_device_libs(
        args: &ParsedArguments,
        env_vars: &[(OsString, OsString)],
    ) -> Vec<PathBuf> {
        let rocm_path_arg: Option<PathBuf> = Self::extract_rocm_arg(args, "--rocm-path=");
        let hip_device_lib_path_arg: Option<PathBuf> =
            Self::extract_rocm_arg(args, "--hip-device-lib-path=");
        let rocm_path_env: Option<PathBuf> = Self::extract_rocm_env(env_vars, "ROCM_PATH");
        let hip_device_lib_path_env: Option<PathBuf> =
            Self::extract_rocm_env(env_vars, "HIP_DEVICE_LIB_PATH");

        let hip_device_lib_path: PathBuf = hip_device_lib_path_arg
            .or(hip_device_lib_path_env)
            .or(rocm_path_arg.map(|path| path.join("amdgcn").join("bitcode")))
            .or(rocm_path_env.map(|path| path.join("amdgcn").join("bitcode")))
            // This is the default location in official AMD packages and containers.
            .unwrap_or(PathBuf::from("/opt/rocm/amdgcn/bitcode"));

        hip_device_lib_path
            .read_dir()
            .ok()
            .map(|f| {
                let mut device_libs = f
                    .flatten()
                    .filter(|f| f.path().extension().is_some_and(|ext| ext == "bc"))
                    .map(|f| f.path())
                    .collect::<Vec<_>>();
                device_libs.sort_unstable();
                device_libs
            })
            .unwrap_or_default()
    }

    pub fn compiler(&self) -> &I {
        &self.compiler
    }
}

impl<T: CommandCreatorSync, I: CCompilerImpl> Compiler<T> for CCompiler<I> {
    fn kind(&self) -> CompilerKind {
        CompilerKind::C(self.compiler.kind())
    }
    fn version(&self) -> Option<String> {
        self.compiler.version()
    }
    #[cfg(feature = "dist-client")]
    fn get_toolchain_packager(&self) -> Box<dyn pkg::ToolchainPackager> {
        Box::new(CToolchainPackager {
            env_vars: vec![],
            executable: self.executable.clone(),
            kind: self.compiler.kind(),
        })
    }
    fn parse_arguments(
        &self,
        arguments: &[OsString],
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
    ) -> CompilerArguments<Box<dyn CompilerHasher<T> + 'static>> {
        match self.compiler.parse_arguments(arguments, cwd, env_vars) {
            CompilerArguments::Ok(mut args) => {
                // Handle SCCACHE_EXTRAFILES
                for (k, v) in env_vars.iter() {
                    if k.as_os_str() == OsStr::new("SCCACHE_EXTRAFILES") {
                        args.extra_hash_files.extend(std::env::split_paths(&v))
                    }
                }

                // Handle cache invalidation for the ROCm device bitcode libraries. Every HIP
                // object links in some LLVM bitcode libraries (.bc files), so in some sense
                // every HIP object compilation has an direct dependency on those bitcode
                // libraries.
                //
                // The bitcode libraries are unlikely to change **except** when a ROCm version
                // changes, so for correctness we should take these bitcode libraries into
                // account by adding them to `extra_hash_files`.
                //
                // In reality, not every available bitcode library is needed, but that is
                // too much to handle on our side so we just hash every bitcode library we find.
                if args.language == Language::Hip {
                    args.extra_hash_files
                        .extend(Self::search_hip_device_libs(&args, env_vars))
                }

                CompilerArguments::Ok(Box::new(CCompilerHasher {
                    parsed_args: args,
                    executable: self.executable.clone(),
                    executable_digest: self.executable_digest.clone(),
                    compiler: self.compiler.clone(),
                }))
            }
            CompilerArguments::CannotCache(why, extra_info) => {
                CompilerArguments::CannotCache(why, extra_info)
            }
            CompilerArguments::NotCompilation => CompilerArguments::NotCompilation,
        }
    }

    fn box_clone(&self) -> Box<dyn Compiler<T>> {
        Box::new((*self).clone())
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send + Sync> {
        self
    }
}

#[derive(PartialEq)]
enum PreprocessorCacheLookup {
    Disabled,
    Hit(String),
    Miss(String),
}

impl<I> CCompilerHasher<I>
where
    I: CCompilerImpl,
{
    async fn preprocessor_cache_lookup(
        &self,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
        extra_hashes: &[String],
        cache_control: &CacheControl,
        storage: &dyn Storage,
    ) -> Result<PreprocessorCacheLookup> {
        let CCompilerHasher {
            parsed_args,
            executable_digest,
            compiler,
            ..
        } = self;

        let out_pretty = parsed_args.output_pretty();
        let preprocessor_cache_mode_config = storage.preprocessor_cache_mode_config();

        if !use_preprocessor_cache_mode(parsed_args, env_vars, &preprocessor_cache_mode_config) {
            return Ok(PreprocessorCacheLookup::Disabled);
        }

        // Create an argument vector containing all the preprocessor args to use in creating a hash key
        let mut preprocessor_args = parsed_args.preprocessor_args.clone();
        // If the dependency args change, we need to re-run the preprocessor to generate them
        preprocessor_args.extend_from_slice(&parsed_args.dependency_args[..]);
        // common_args is used in preprocessing too
        preprocessor_args.extend_from_slice(&parsed_args.common_args[..]);
        preprocessor_args.extend_from_slice(&parsed_args.arch_args[..]);

        let preprocessor_key = preprocessor_cache_entry_hash_key(
            executable_digest,
            parsed_args.language,
            &preprocessor_args,
            extra_hashes,
            env_vars,
            &cwd.join(&parsed_args.input),
            compiler.plusplus(),
            &preprocessor_cache_mode_config,
        )?
        .filter(|_| matches!(cache_control, CacheControl::Default));

        if let Some(preprocessor_key) = preprocessor_key {
            if let Some(mut preprocessor_cache_entry) = storage
                .get_preprocessor_cache_entry(&preprocessor_key)
                .await
            {
                let (hit, updated, preprocessor_cache_entry) = tokio::runtime::Handle::current()
                    .spawn_blocking(move || {
                        let mut updated = false;
                        let hit = preprocessor_cache_entry
                            .lookup_result_digest(preprocessor_cache_mode_config, &mut updated);
                        Ok::<(Option<String>, bool, PreprocessorCacheEntry), anyhow::Error>((
                            hit,
                            updated,
                            preprocessor_cache_entry,
                        ))
                    })
                    .await??;

                let mut update_failed = false;
                if updated {
                    // Time macros have been found, we need to update
                    // the preprocessor cache entry. See [`PreprocessorCacheEntry::result_matches`].
                    debug!("[{out_pretty}]: Preprocessor cache updated because of time macros: {preprocessor_key}");

                    if let Err(e) = storage
                        .put_preprocessor_cache_entry(&preprocessor_key, preprocessor_cache_entry)
                        .await
                    {
                        debug!("[{out_pretty}]: Failed to update preprocessor cache: {}", e);
                        update_failed = true;
                    }
                }

                if !update_failed {
                    if let Some(key) = hit {
                        debug!(
                            "[{out_pretty}]: Preprocessor cache hit: {preprocessor_key} -> {key}"
                        );
                        return Ok(PreprocessorCacheLookup::Hit(key));
                    }
                }
            }
            debug!("[{out_pretty}]: Preprocessor cache miss: {preprocessor_key}");
            return Ok(PreprocessorCacheLookup::Miss(preprocessor_key));
        }

        Ok(PreprocessorCacheLookup::Disabled)
    }
}

#[async_trait]
impl<T, I> CompilerHasher<T> for CCompilerHasher<I>
where
    T: CommandCreatorSync,
    I: CCompilerImpl,
{
    fn get_executable(&self) -> PathBuf {
        self.executable.clone()
    }

    async fn generate_hash_key(
        self: Box<Self>,
        service: &crate::server::SccacheService<T>,
        creator: &T,
        cwd: PathBuf,
        env_vars: Vec<(OsString, OsString)>,
        pool: &tokio::runtime::Handle,
        rewrite_includes_only: bool,
        storage: Arc<dyn Storage>,
        cache_control: CacheControl,
    ) -> Result<HashResult<T>> {
        let start_of_compilation = std::time::SystemTime::now();
        let CCompilerHasher { parsed_args, .. } = self.as_ref();

        let extra_hashes = if parsed_args.extra_hash_files.is_empty() {
            vec![]
        } else {
            hash_all(&parsed_args.extra_hash_files, pool).await?
        };

        // Try to look for a cached preprocessing step for this compilation request.
        let preprocessor_cache_mode_config = storage.preprocessor_cache_mode_config();
        let mut preprocessor_cache_lookup = self
            .preprocessor_cache_lookup(
                &cwd,
                &env_vars,
                &extra_hashes,
                &cache_control,
                storage.as_ref(),
            )
            .await?;

        let kind = CompilerKind::C(self.compiler.kind());
        let lang = <CCompilerHasher<I> as CompilerHasher<T>>::language(&*self);

        let CCompilerHasher {
            parsed_args,
            executable,
            executable_digest,
            compiler,
            ..
        } = *self;

        match preprocessor_cache_lookup {
            PreprocessorCacheLookup::Hit(_) => {
                service
                    .stats
                    .lock()
                    .await
                    .preprocessor_cache_hits
                    .increment(&kind, &lang);
            }
            PreprocessorCacheLookup::Miss(_) => {
                service
                    .stats
                    .lock()
                    .await
                    .preprocessor_cache_misses
                    .increment(&kind, &lang);
            }
            _ => {}
        };

        let key = if let PreprocessorCacheLookup::Hit(key) = preprocessor_cache_lookup {
            key
        } else {
            let result = compiler
                .preprocess(
                    service,
                    creator,
                    &executable,
                    &parsed_args,
                    &cwd,
                    &env_vars,
                    rewrite_includes_only,
                    // Generate dependencies if we're going to read them below
                    preprocessor_cache_lookup != PreprocessorCacheLookup::Disabled,
                    // include line numbers when `-fprofile-generate` is enabled
                    // to guarantee the profile data embedded in the cached object
                    // matches the line numbers in this source file
                    parsed_args.profile_generate,
                )
                .await;

            let out_pretty = parsed_args.output_pretty();

            let mut preprocessor_output = match result {
                Ok(out) => out,
                Err(err) => {
                    let outputs = &parsed_args.outputs;
                    // Errors remove all traces of potential output.
                    trace!("[{out_pretty}]: removing files {:?}", &outputs);

                    let v = outputs.values().try_for_each(|output| {
                        let path = cwd.join(&output.path);
                        match fs::metadata(&path) {
                            // File exists, remove it.
                            Ok(_) => fs::remove_file(&path),
                            _ => Ok(()),
                        }
                    });

                    if v.is_err() {
                        warn!(
                            "[{out_pretty}]: Could not remove files after preprocessing failed: {:?}",
                            &outputs
                        );
                    }

                    match err.downcast::<ProcessError>() {
                        Ok(ProcessError(mut output)) => {
                            debug!(
                                "[{out_pretty}]: preprocessor returned error (code={:?}, desc={:?})",
                                output.code(),
                                output.desc()
                            );
                            // Drop the stdout since it's the preprocessor output,
                            // just hand back stderr and the exit status.
                            output.stdout = vec![];
                            bail!(ProcessError(output));
                        }
                        Err(err) => {
                            warn!("[{out_pretty}]: preprocessor failed: {err:?}");
                            return Err(err);
                        }
                    }
                }
            };

            // Create an argument vector containing both common and arch args, to
            // use in creating a hash key
            let mut common_and_arch_args = parsed_args.common_args.clone();
            common_and_arch_args.extend_from_slice(&parsed_args.arch_args[..]);

            match preprocessor_output {
                PreprocessorOutput::File(ref res) => hash_key(
                    &executable_digest,
                    parsed_args.language,
                    &common_and_arch_args,
                    &extra_hashes,
                    &env_vars,
                    &mut fs::File::open(res.path())?,
                    compiler.plusplus(),
                ),
                PreprocessorOutput::Output(ref mut res) => hash_key(
                    &executable_digest,
                    parsed_args.language,
                    &common_and_arch_args,
                    &extra_hashes,
                    &env_vars,
                    &mut res.stdout.reader(),
                    compiler.plusplus(),
                ),
                PreprocessorOutput::OutputWithDepedencies(ref mut res, dependencies) => {
                    // Remember include files needed in this preprocessing step
                    let included_files = if let PreprocessorCacheLookup::Miss(_) =
                        preprocessor_cache_lookup
                    {
                        let paths_and_digests = dependencies.into_iter().map(|path| {
                            tokio::runtime::Handle::current().spawn_blocking(
                                move || -> Result<Option<(PathBuf, String)>> {
                                    let file = fs::File::open(&path).map_err(anyhow::Error::new)?;
                                    let (digest, finder) =
                                        if preprocessor_cache_mode_config.ignore_time_macros {
                                            (Digest::reader_sync(file)?, TimeMacroFinder::new())
                                        } else {
                                            Digest::reader_sync_time_macros(file)?
                                        };
                                    if finder.found_time() {
                                        Ok(None)
                                    } else {
                                        Ok(Some((path, digest)))
                                    }
                                },
                            )
                        });

                        futures::future::try_join_all(paths_and_digests)
                            .await?
                            .into_iter()
                            .filter_map_ok(|x| x)
                            .fold_ok(HashMap::new(), |mut deps, (path, digest)| {
                                deps.insert(path, digest);
                                deps
                            })
                    } else {
                        Ok(HashMap::new())
                    };

                    let included_files = match included_files {
                        Ok(included_files) => included_files,
                        Err(err) => {
                            debug!("[{out_pretty}]: Disabling preprocessor cache mode: {err}");
                            preprocessor_cache_lookup = PreprocessorCacheLookup::Disabled;
                            HashMap::new()
                        }
                    };

                    let key = hash_key(
                        &executable_digest,
                        parsed_args.language,
                        &common_and_arch_args,
                        &extra_hashes,
                        &env_vars,
                        &mut res.stdout.reader(),
                        compiler.plusplus(),
                    );

                    // Cache the preprocessing step
                    if let PreprocessorCacheLookup::Miss(preprocessor_key) =
                        preprocessor_cache_lookup
                    {
                        if !included_files.is_empty() {
                            let mut preprocessor_cache_entry = PreprocessorCacheEntry::new();
                            let mut included_files = included_files
                                .into_iter()
                                .map(|(path, digest)| (digest, path))
                                .collect::<Vec<_>>();
                            included_files.sort_unstable_by(|a, b| a.1.cmp(&b.1));
                            preprocessor_cache_entry.add_result(
                                start_of_compilation,
                                &preprocessor_key,
                                &key,
                                included_files,
                            );

                            if let Err(e) = storage
                                .put_preprocessor_cache_entry(
                                    &preprocessor_key,
                                    preprocessor_cache_entry,
                                )
                                .await
                            {
                                debug!(
                                    "[{out_pretty}]: Failed to update preprocessor cache: {}",
                                    e
                                );
                            }
                        }
                    }

                    key
                }
            }
        };

        // A compiler binary may be a symlink to another and so has the same digest, but that means
        // the toolchain will not contain the correct path to invoke the compiler! Add the compiler
        // executable path to try and prevent this
        let weak_toolchain_key = format!("{}-{}", executable.to_string_lossy(), executable_digest);

        Ok(HashResult {
            key,
            compilation: Box::new(CCompilation {
                service: service.to_owned(),
                creator: creator.to_owned(),
                compiler,
                cwd,
                env_vars,
                executable,
                parsed_args,
                rewrite_includes_only,
            }),
            weak_toolchain_key,
        })
    }

    fn color_mode(&self) -> ColorMode {
        self.parsed_args.color_mode
    }

    fn output_pretty(&self) -> Cow<'_, str> {
        self.parsed_args.output_pretty()
    }

    fn box_clone(&self) -> Box<dyn CompilerHasher<T>> {
        Box::new((*self).clone())
    }

    fn language(&self) -> Language {
        self.parsed_args.language
    }
}

fn use_preprocessor_cache_mode(
    parsed_args: &ParsedArguments,
    env_vars: &[(OsString, OsString)],
    preprocessor_cache_mode_config: &PreprocessorCacheModeConfig,
) -> bool {
    let out_pretty = parsed_args.output_pretty();

    // Try to look for a cached preprocessing step for this compilation
    // request.

    if !parsed_args.too_hard_for_preprocessor_cache_mode.is_empty() {
        trace!(
            "[{out_pretty}]: Cannot use preprocessor cache because of {:?}",
            parsed_args
                .too_hard_for_preprocessor_cache_mode
                .join(OsStr::new(" "))
        );
    }

    let can_use_preprocessor_cache_mode = preprocessor_cache_mode_config
        .use_preprocessor_cache_mode
        && parsed_args.too_hard_for_preprocessor_cache_mode.is_empty();

    // Allow overrides from the env
    let use_preprocessor_cache_mode = env_vars
        .iter()
        .find_map(|(k, v)| {
            if k == "SCCACHE_DIRECT" {
                v.to_str().map(|s| s.to_lowercase())
            } else {
                None
            }
        })
        .map(|v| !matches!(v.as_str(), "false" | "off" | "0" | ""))
        .unwrap_or(can_use_preprocessor_cache_mode);

    if can_use_preprocessor_cache_mode && !use_preprocessor_cache_mode {
        trace!("[{out_pretty}]: Disabling preprocessor cache because SCCACHE_DIRECT=false");
    }

    use_preprocessor_cache_mode
}

impl<T: CommandCreatorSync, I: CCompilerImpl> Compilation<T> for CCompilation<T, I> {
    fn generate_compile_commands(
        &self,
        path_transformer: &mut dist::PathTransformer,
        rewrite_includes_only: bool,
        hash_key: &str,
    ) -> Result<(
        Box<dyn CompileCommand<T>>,
        Option<dist::CompileCommand>,
        Cacheable,
    )> {
        let CCompilation {
            ref parsed_args,
            ref executable,
            ref compiler,
            ref cwd,
            ref env_vars,
            ..
        } = *self;

        compiler.generate_compile_commands(
            path_transformer,
            executable,
            parsed_args,
            cwd,
            env_vars,
            rewrite_includes_only,
            hash_key,
        )
    }

    #[cfg(feature = "dist-client")]
    fn into_dist_packagers(self: Box<Self>) -> Result<DistPackagers> {
        trace!(
            "Dist inputs: {:?}",
            std::iter::once(&self.parsed_args.input)
                .chain(self.parsed_args.extra_dist_files.iter())
                .chain(self.parsed_args.extra_hash_files.iter())
                .unique()
                .collect::<Vec<_>>()
        );

        let toolchain_packager = Box::new(CToolchainPackager {
            kind: self.compiler.kind(),
            env_vars: self.env_vars.to_owned(),
            executable: self.executable.to_owned(),
        });

        let outputs_rewriter = Box::new(NoopOutputsRewriter);

        Ok((self, toolchain_packager, outputs_rewriter))
    }

    fn outputs<'a>(&'a self) -> Box<dyn Iterator<Item = FileObjectSource> + 'a> {
        Box::new(
            self.parsed_args
                .outputs
                .iter()
                .map(|(k, output)| FileObjectSource {
                    key: k.to_string(),
                    path: output.path.clone(),
                    optional: output.optional,
                    must_be_non_empty: output.must_be_non_empty,
                }),
        )
    }
}

#[cfg(feature = "dist-client")]
#[async_trait]
impl<T: CommandCreatorSync, I: CCompilerImpl> pkg::InputsPackager for CCompilation<T, I> {
    async fn write_inputs(
        self: Box<Self>,
        path_transformer: &mut dist::PathTransformer,
        compressor: Box<dyn InputsWriter>,
    ) -> Result<()> {
        let CCompilation {
            service,
            creator,
            cwd,
            executable,
            compiler,
            env_vars,
            parsed_args,
            rewrite_includes_only,
        } = *self;

        // Preprocess again but this time with line numbers
        let preprocessor_output = compiler
            .preprocess(
                &service,
                &creator,
                &executable,
                &parsed_args,
                &cwd,
                &env_vars,
                rewrite_includes_only,
                false, // generate_dependencies
                true,  // include_line_numbers
            )
            .await?;

        let mut builder = tar::Builder::new(compressor);
        let mut path_transformer = path_transformer.clone();

        tokio::runtime::Handle::current()
            .spawn_blocking(move || -> Result<_> {
                {
                    let input_path = cwd.join(&parsed_args.input);
                    let input_path = pkg::simplify_path(&input_path)?;
                    let dist_input_path =
                        path_transformer.as_dist(&input_path).with_context(|| {
                            format!("unable to transform input path {}", input_path.display())
                        })?;

                    match preprocessor_output {
                        PreprocessorOutput::File(file) => {
                            use crate::dist::pkg::tar_safe_path;
                            builder.append_path_with_name(
                                file.path(),
                                tar_safe_path(&dist_input_path),
                            )?;
                        }
                        PreprocessorOutput::Output(output)
                        | PreprocessorOutput::OutputWithDepedencies(output, _) => {
                            let (mut file_header, dist_input_path) =
                                pkg::make_tar_header(&input_path, &dist_input_path)?;
                            file_header.set_size(output.stdout.len() as u64); // The metadata is from non-preprocessed
                            file_header.set_cksum();
                            builder.append_data(
                                &mut file_header,
                                dist_input_path,
                                output.stdout.as_slice(),
                            )?;
                        }
                    }
                }

                let extra_dist_files = parsed_args.extra_dist_files;
                let extra_hash_files = parsed_args.extra_hash_files;

                for input_path in extra_hash_files.iter().chain(extra_dist_files.iter()) {
                    let input_path = pkg::simplify_path(input_path)?;

                    if !super::CAN_DIST_DYLIBS
                        && input_path
                            .extension()
                            .is_some_and(|ext| ext == std::env::consts::DLL_EXTENSION)
                    {
                        bail!(
                            "Cannot distribute dylib input {} on this platform",
                            input_path.display()
                        )
                    }

                    let dist_input_path =
                        path_transformer.as_dist(&input_path).with_context(|| {
                            format!("unable to transform input path {}", input_path.display())
                        })?;

                    let mut file = io::BufReader::new(fs::File::open(&input_path)?);
                    let mut output = vec![];
                    io::copy(&mut file, &mut output)?;

                    let (mut file_header, dist_input_path) =
                        pkg::make_tar_header(&input_path, &dist_input_path)?;
                    file_header.set_size(output.len() as u64);
                    file_header.set_cksum();
                    builder.append_data(&mut file_header, dist_input_path, &*output)?;
                }

                // Finish archive
                let _ = builder.into_inner()?.finish()?;

                Ok(())
            })
            .await?
    }
}

#[cfg(feature = "dist-client")]
#[allow(unused)]
struct CToolchainPackager {
    env_vars: Vec<(OsString, OsString)>,
    executable: PathBuf,
    kind: CCompilerKind,
}

#[cfg(feature = "dist-client")]
#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64"),
))]
#[async_trait]
#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64"),
))]
impl pkg::ToolchainPackager for CToolchainPackager {
    async fn write_pkg(self: Box<Self>, f: fs::File) -> Result<String> {
        use std::os::unix::ffi::OsStringExt;

        debug!("Generating toolchain {}", self.executable.display());
        let mut package_builder = pkg::ToolchainPackageBuilder::new();
        package_builder.add_common()?;

        // Helper to use -print-file-name and -print-prog-name to look up
        // files by path.
        let named_file = |kind: &str, name: &str| -> Option<PathBuf> {
            let mut output = std::process::Command::new(&self.executable)
                .arg(format!("-print-{kind}-name={name}"))
                .output()
                .ok()?;
            debug!(
                "find named {} {} output:\n{}\n===\n{}",
                kind,
                name,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            if !output.status.success() {
                debug!("exit failure");
                return None;
            }

            // Remove the trailing newline (if present)
            if output.stdout.last() == Some(&b'\n') {
                output.stdout.pop();
            }

            // Create our PathBuf from the raw bytes.  Assume that relative
            // paths can be found via PATH.
            let path: PathBuf = OsString::from_vec(output.stdout).into();
            if path.is_absolute() {
                Some(path)
            } else {
                which::which(path).ok()
            }
        };

        // Helper to add a named file/program by to the package.
        // We ignore the case where the file doesn't exist, as we don't need it.
        let add_named_prog =
            |builder: &mut pkg::ToolchainPackageBuilder, name: &str| -> Result<()> {
                if let Some(path) = named_file("prog", name) {
                    builder.add_executable_and_deps(&self.env_vars, path)?;
                }
                Ok(())
            };
        let add_named_file =
            |builder: &mut pkg::ToolchainPackageBuilder, name: &str| -> Result<()> {
                if let Some(path) = named_file("file", name) {
                    builder.add_file(path)?;
                }
                Ok(())
            };

        let mut add_default_files = || -> Result<()> {
            // Add basic |as| and |objcopy| programs.
            add_named_prog(&mut package_builder, "as")?;
            add_named_prog(&mut package_builder, "objcopy")?;

            // Linker configuration.
            if Path::new("/etc/ld.so.conf").is_file() {
                package_builder.add_file("/etc/ld.so.conf".into())?;
            }
            Ok(())
        };

        // Compiler-specific handling
        match self.kind {
            CCompilerKind::Clang => {
                add_default_files()?;
                package_builder.add_executable_and_deps(&self.env_vars, self.executable.clone())?;
                // Clang uses internal header files, so add them.
                if let Some(limits_h) = named_file("file", "include/limits.h") {
                    info!("limits_h = {}", limits_h.display());
                    package_builder.add_dir_contents(limits_h.parent().unwrap())?;
                }
            }

            CCompilerKind::Gcc => {
                // Various external programs / files which may be needed by gcc
                add_default_files()?;
                package_builder.add_executable_and_deps(&self.env_vars, self.executable.clone())?;
                add_named_prog(&mut package_builder, "cc1")?;
                add_named_prog(&mut package_builder, "cc1plus")?;
                add_named_file(&mut package_builder, "specs")?;
                add_named_file(&mut package_builder, "liblto_plugin.so")?;
            }

            CCompilerKind::Nvhpc => {
                // Various programs called by the nvc/nvc++ front end.
                let _ = add_default_files();

                // Handle NVHPC's symlinks of `mpic++ -> bin/env.sh` where `env.sh` is
                // a wrapper to the real `mpic++` executable (or symlink) in `bin/.bin/mpic++`
                if let Err(orig_err) =
                    package_builder.add_executable_and_deps(&self.env_vars, self.executable.clone())
                {
                    let exe_dir = self.executable.parent().expect("exe should have a parent");

                    let dot_bin_dir = if let Ok(path) = self.executable.read_link() {
                        exe_dir.join(path).parent().map(|p| p.join(".bin"))
                    } else {
                        Some(exe_dir).map(|p| p.join(".bin"))
                    };

                    if let Some(real_exe) = dot_bin_dir
                        .and_then(|p| self.executable.file_name().map(|name| p.join(name)))
                    {
                        if !real_exe.exists() {
                            return Err(orig_err);
                        }
                        package_builder.add_link(&real_exe, &self.executable)?;
                        package_builder.add_executable_and_deps(&self.env_vars, real_exe)?;
                    }
                }

                let mut cfg_paths = vec![];
                let mut dir_paths = vec![];
                let mut dir_contents = vec![];
                let mut exe_paths = vec![];

                if let Ok(as_path) = which::which("as") {
                    exe_paths.push(as_path);
                }

                if let Some(bin_dir) = self.executable.parent() {
                    cfg_paths.push(bin_dir.join("nvcrc"));
                    cfg_paths.push(bin_dir.join(".nvcrc"));
                    cfg_paths.push(bin_dir.join("nvc++rc"));
                    cfg_paths.push(bin_dir.join(".nvc++rc"));
                    cfg_paths.push(bin_dir.join("localrc"));
                    cfg_paths.push(bin_dir.join("makelocalrc"));
                    dir_contents.push(bin_dir.join("rcfiles"));

                    let path = bin_dir.join("tools");
                    if path.exists() && path.is_dir() {
                        exe_paths.push(path.join("acclnk"));
                        exe_paths.push(path.join("append"));
                        exe_paths.push(path.join("cpp1"));
                        exe_paths.push(path.join("cpp2"));
                        exe_paths.push(path.join("nvcpfe"));
                        exe_paths.push(path.join("nvdd"));
                    }

                    if let Some(comp_dir) = bin_dir.parent() {
                        let path = comp_dir.join("share").join("llvm").join("bin");
                        if path.exists() && path.is_dir() {
                            exe_paths.push(path.join("llc"));
                            exe_paths.push(path.join("opt"));
                            exe_paths.push(path.join("llvm-as"));
                            exe_paths.push(path.join("llvm-link"));
                            exe_paths.push(path.join("llvm-mc"));
                        }
                    }
                }

                let _ = std::process::Command::new(&self.executable)
                    .arg("-show")
                    .output()
                    .and_then(|output| {
                        use bytes::Buf;
                        use std::io::BufRead;
                        output.stdout.reader().lines().try_for_each(|line| {
                            let mut line = line?;
                            if line.starts_with("CUDAROOT")
                                || line.starts_with("DEFCPPINC")
                                || line.starts_with("DEFSTDINC")
                                || line.starts_with("GCCINC")
                                || line.starts_with("GPPDIR")
                                || line.starts_with("STDINC")
                                || line.starts_with("SYSTEMINC")
                            {
                                dir_paths.extend_from_slice(
                                    line.find('=')
                                        .map(|idx| {
                                            line.split_off(idx + 1)
                                                .trim_start()
                                                .trim_end()
                                                .split(' ')
                                                .map(PathBuf::from)
                                                .collect::<Vec<_>>()
                                        })
                                        .unwrap_or(vec![])
                                        .as_slice(),
                                );
                                return Ok(());
                            }
                            if line.starts_with("HOMELOCALRC") || line.starts_with("MYLOCALRC") {
                                line.find('=').and_then(|idx| {
                                    let str = line.split_off(idx + 1);
                                    let str = str.trim_start().trim_end();
                                    if !str.is_empty() {
                                        cfg_paths.push(PathBuf::from(str));
                                    }
                                    Option::<()>::None
                                });
                                return Ok(());
                            }
                            if line.starts_with("COMPINCDIRFULL") {
                                dir_contents.extend_from_slice(
                                    line.find('=')
                                        .map(|idx| {
                                            line.split_off(idx + 1)
                                                .trim_start()
                                                .trim_end()
                                                .split(' ')
                                                .map(PathBuf::from)
                                                .collect::<Vec<_>>()
                                        })
                                        .unwrap_or(vec![])
                                        .as_slice(),
                                );
                                return Ok(());
                            }
                            Ok(())
                        })
                    });

                for path in cfg_paths {
                    if path.exists() && path.is_file() {
                        if let Ok(path) = dunce::canonicalize(&path) {
                            let _ = package_builder.add_file(path);
                        }
                    }
                }

                for path in dir_paths {
                    if path.exists() && path.is_dir() {
                        if let Ok(path) = dunce::canonicalize(&path) {
                            let _ = package_builder.add_dir(path);
                        }
                    }
                }

                for path in dir_contents {
                    if path.exists() && path.is_dir() {
                        let _ = package_builder.add_dir_contents(&path);
                    }
                }

                for path in exe_paths {
                    if path.exists() && path.is_file() {
                        let _ = package_builder.add_executable_and_deps(&self.env_vars, path);
                    }
                }
            }

            _ => {
                package_builder.add_executable_and_deps(&self.env_vars, self.executable.clone())?;
            }
        }

        // Bundle into a compressed tarfile.
        package_builder.into_compressed_tar(f).await
    }
}

/// The cache is versioned by the inputs to `hash_key`.
pub const CACHE_VERSION: &[u8] = b"11";

/// Environment variables that are factored into the cache key.
static CACHED_ENV_VARS: Lazy<HashSet<&'static OsStr>> = Lazy::new(|| {
    [
        // SCCACHE_C_CUSTOM_CACHE_BUSTER has no particular meaning behind it,
        // serving as a way for the user to factor custom data into the hash.
        // One can set it to different values for different invocations
        // to prevent cache reuse between them.
        "SCCACHE_C_CUSTOM_CACHE_BUSTER",
        "MACOSX_DEPLOYMENT_TARGET",
        "IPHONEOS_DEPLOYMENT_TARGET",
        "TVOS_DEPLOYMENT_TARGET",
        "WATCHOS_DEPLOYMENT_TARGET",
        "SDKROOT",
        "CCC_OVERRIDE_OPTIONS",
    ]
    .iter()
    .map(OsStr::new)
    .collect()
});

/// Compute the hash key of `compiler` compiling `preprocessor_output` with `args`.
pub fn hash_key<R: io::Read>(
    compiler_digest: &str,
    language: Language,
    arguments: &[OsString],
    extra_hashes: &[String],
    env_vars: &[(OsString, OsString)],
    preprocessor_output: &mut R,
    plusplus: bool,
) -> String {
    // If you change any of the inputs to the hash, you should change `CACHE_VERSION`.
    let mut m = Digest::new();
    m.update(compiler_digest.as_bytes());
    // clang and clang++ have different behavior despite being byte-for-byte identical binaries, so
    // we have to incorporate that into the hash as well.
    m.update(&[plusplus as u8]);
    m.update(CACHE_VERSION);
    m.update(language.as_str().as_bytes());
    for arg in arguments {
        arg.hash(&mut HashToDigest { digest: &mut m });
    }
    for hash in extra_hashes {
        m.update(hash.as_bytes());
    }

    for (var, val) in env_vars.iter() {
        if CACHED_ENV_VARS.contains(var.as_os_str()) {
            var.hash(&mut HashToDigest { digest: &mut m });
            m.update(&b"="[..]);
            val.hash(&mut HashToDigest { digest: &mut m });
        }
    }
    let _ = m.update_from_reader_sync(preprocessor_output);
    m.finish()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_same_content() {
        let args = ovec!["a", "b", "c"];
        const PREPROCESSED: &[u8] = b"hello world";
        assert_eq!(
            hash_key(
                "abcd",
                Language::C,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            ),
            hash_key(
                "abcd",
                Language::C,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            )
        );
    }

    #[test]
    fn test_plusplus_differs() {
        let args = ovec!["a", "b", "c"];
        const PREPROCESSED: &[u8] = b"hello world";
        assert_neq!(
            hash_key(
                "abcd",
                Language::C,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            ),
            hash_key(
                "abcd",
                Language::C,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                true
            )
        );
    }

    #[test]
    fn test_header_differs() {
        let args = ovec!["a", "b", "c"];
        const PREPROCESSED: &[u8] = b"hello world";
        assert_neq!(
            hash_key(
                "abcd",
                Language::C,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            ),
            hash_key(
                "abcd",
                Language::CHeader,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            )
        );
    }

    #[test]
    fn test_plusplus_header_differs() {
        let args = ovec!["a", "b", "c"];
        const PREPROCESSED: &[u8] = b"hello world";
        assert_neq!(
            hash_key(
                "abcd",
                Language::Cxx,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                true
            ),
            hash_key(
                "abcd",
                Language::CxxHeader,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                true
            )
        );
    }

    #[test]
    fn test_hash_key_executable_contents_differs() {
        let args = ovec!["a", "b", "c"];
        const PREPROCESSED: &[u8] = b"hello world";
        assert_neq!(
            hash_key(
                "abcd",
                Language::C,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            ),
            hash_key(
                "wxyz",
                Language::C,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            )
        );
    }

    #[test]
    fn test_hash_key_args_differs() {
        let digest = "abcd";
        let abc = ovec!["a", "b", "c"];
        let xyz = ovec!["x", "y", "z"];
        let ab = ovec!["a", "b"];
        let a = ovec!["a"];
        const PREPROCESSED: &[u8] = b"hello world";
        assert_neq!(
            hash_key(
                digest,
                Language::C,
                &abc,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            ),
            hash_key(
                digest,
                Language::C,
                &xyz,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            )
        );

        assert_neq!(
            hash_key(
                digest,
                Language::C,
                &abc,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            ),
            hash_key(
                digest,
                Language::C,
                &ab,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            )
        );

        assert_neq!(
            hash_key(
                digest,
                Language::C,
                &abc,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            ),
            hash_key(
                digest,
                Language::C,
                &a,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            )
        );
    }

    #[test]
    fn test_hash_key_preprocessed_content_differs() {
        let args = ovec!["a", "b", "c"];
        assert_neq!(
            hash_key(
                "abcd",
                Language::C,
                &args,
                &[],
                &[],
                &mut b"hello world"[..].reader(),
                false
            ),
            hash_key(
                "abcd",
                Language::C,
                &args,
                &[],
                &[],
                &mut b"goodbye"[..].reader(),
                false
            )
        );
    }

    #[test]
    fn test_hash_key_env_var_differs() {
        let args = ovec!["a", "b", "c"];
        let digest = "abcd";
        const PREPROCESSED: &[u8] = b"hello world";
        for var in CACHED_ENV_VARS.iter() {
            let h1 = hash_key(
                digest,
                Language::C,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false,
            );
            let vars = vec![(OsString::from(var), OsString::from("something"))];
            let h2 = hash_key(
                digest,
                Language::C,
                &args,
                &[],
                &vars,
                &mut PREPROCESSED[..].reader(),
                false,
            );
            let vars = vec![(OsString::from(var), OsString::from("something else"))];
            let h3 = hash_key(
                digest,
                Language::C,
                &args,
                &[],
                &vars,
                &mut PREPROCESSED[..].reader(),
                false,
            );
            assert_neq!(h1, h2);
            assert_neq!(h2, h3);
        }
    }

    #[test]
    fn test_extra_hash_data() {
        let args = ovec!["a", "b", "c"];
        let digest = "abcd";
        const PREPROCESSED: &[u8] = b"hello world";
        let extra_data = stringvec!["hello", "world"];

        assert_neq!(
            hash_key(
                digest,
                Language::C,
                &args,
                &extra_data,
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            ),
            hash_key(
                digest,
                Language::C,
                &args,
                &[],
                &[],
                &mut PREPROCESSED[..].reader(),
                false
            )
        );
    }

    #[test]
    fn test_language_from_file_name() {
        fn t(extension: &str, expected: Language) {
            let path_str = format!("input.{extension}");
            let path = Path::new(&path_str);
            let actual = Language::from_file_name(path);
            assert_eq!(actual, Some(expected));
        }

        t("c", Language::C);

        t("C", Language::Cxx);
        t("cc", Language::Cxx);
        t("cp", Language::Cxx);
        t("cpp", Language::Cxx);
        t("CPP", Language::Cxx);
        t("cxx", Language::Cxx);
        t("c++", Language::Cxx);

        t("h", Language::GenericHeader);

        t("hh", Language::CxxHeader);
        t("H", Language::CxxHeader);
        t("hp", Language::CxxHeader);
        t("hxx", Language::CxxHeader);
        t("hpp", Language::CxxHeader);
        t("HPP", Language::CxxHeader);
        t("h++", Language::CxxHeader);
        t("tcc", Language::CxxHeader);

        t("m", Language::ObjectiveC);

        t("M", Language::ObjectiveCxx);
        t("mm", Language::ObjectiveCxx);

        t("cu", Language::Cuda);
        t("hip", Language::Hip);
    }

    #[test]
    fn test_language_from_file_name_none() {
        fn t(extension: &str) {
            let path_str = format!("input.{extension}");
            let path = Path::new(&path_str);
            let actual = Language::from_file_name(path);
            let expected = None;
            assert_eq!(actual, expected);
        }

        // gcc parses file-extensions as case-sensitive
        t("Cp");
        t("Cpp");
        t("Hp");
        t("Hpp");
        t("Mm");
        t("Cu");
    }
}
