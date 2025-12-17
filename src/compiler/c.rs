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

use crate::{
    cache::{Cache, FileObjectSource, PreprocessorCacheModeConfig, Storage},
    compiler::{
        Cacheable, ColorMode, Compilation, CompileCommand, CompileCommandImpl, Compiler,
        CompilerArguments, CompilerHasher, CompilerKind, HashResult, Language,
        preprocessor_cache::preprocessor_cache_entry_hash_key,
    },
    dist,
    mock_command::{CommandCreatorSync, ProcessOutput},
    util::{Digest, HashToDigest, TimeMacroFinder, hash_all},
};

#[cfg(feature = "dist-client")]
use crate::{
    compiler::{DistPackagers, NoopOutputsRewriter},
    dist::pkg::{self, InputsWriter},
};

use async_trait::async_trait;
use bytes::Buf;
use fs_err as fs;
use itertools::Itertools;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    ffi::{OsStr, OsString},
    fmt,
    hash::Hash,
    io,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};
use tempfile::TempPath;

use crate::errors::*;

use super::CacheControl;
use super::preprocessor_cache::PreprocessorCacheEntry;

/// A generic implementation of the `Compiler` trait for C/C++ compilers.
#[derive(Clone)]
pub struct CCompiler<I>
where
    I: CCompilerImpl,
{
    executable: PathBuf,
    #[cfg(test)]
    pub executable_digest: String,
    #[cfg(not(test))]
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
#[derive(Clone, Debug)]
struct CCompilation<T: CommandCreatorSync, I: CCompilerImpl> {
    service: crate::server::SccacheService<T>,
    creator: T,
    parsed_args: ParsedArguments,
    is_locally_preprocessed: bool,
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

    /// Run the C preprocessor to generate the dependencies file.
    async fn generate_dependencies<T>(
        &self,
        creator: &T,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
    ) -> Result<Option<(PathBuf, Option<TempPath>)>>
    where
        T: CommandCreatorSync;

    /// Generate a command that can be used to invoke the C compiler to perform
    /// the compilation.
    #[allow(clippy::too_many_arguments)]
    fn generate_compile_commands(
        &self,
        path_transformer: &mut dist::PathTransformer,
        executable: &Path,
        parsed_args: &ParsedArguments,
        cwd: &Path,
        env_vars: &[(OsString, OsString)],
        rewrite_includes_only: bool,
        hash_key: &str,
    ) -> Result<(
        impl CompileCommandImpl,
        Option<dist::CompileCommand>,
        Cacheable,
    )>;
}

impl<I> CCompiler<I>
where
    I: CCompilerImpl,
{
    pub async fn new(
        compiler: I,
        executable: PathBuf,
        extra_hash_files: Vec<PathBuf>,
    ) -> Result<CCompiler<I>> {
        let mut digests = vec![];
        for path in std::iter::once(&executable).chain(extra_hash_files.iter()) {
            if path.exists() {
                digests.push(Digest::file(path).await?);
            }
        }

        let mut digest = Digest::new();
        for hash in digests {
            digest.update(hash.as_bytes());
        }

        Ok(CCompiler {
            executable,
            executable_digest: {
                if let Some(version) = compiler.version() {
                    digest.update(version.as_bytes());
                }
                digest.finish()
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
            parsed_args: Default::default(),
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
                        args.extra_hash_files.extend(std::env::split_paths(&v));
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
                        .extend(Self::search_hip_device_libs(&args, env_vars));
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

/// Return the preprocessor cache entry for a given preprocessor key,
/// if it exists.
/// Only applicable when using preprocessor cache mode.
async fn get_preprocessor_cache_entry(
    storage: &dyn Storage,
    key: &str,
) -> Result<Cache<PreprocessorCacheEntry>> {
    match storage.get(key).await {
        Err(err) => Err(err),
        Ok(Cache::Miss) => Ok(Cache::Miss),
        Ok(Cache::Hit(buf)) => Ok(Cache::Hit(
            PreprocessorCacheEntry::deserialize_from(buf.reader()).await?,
        )),
    }
}

/// Insert a preprocessor cache entry at the given preprocessor key,
/// overwriting the entry if it exists.
/// Only applicable when using preprocessor cache mode.
async fn put_preprocessor_cache_entry(
    storage: &dyn Storage,
    key: &str,
    preprocessor_cache_entry: PreprocessorCacheEntry,
) -> Result<()> {
    storage
        .put(key, preprocessor_cache_entry.serialize_into().await?)
        .await
        .map(|_| ())
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
        .filter(|_| !matches!(cache_control, CacheControl::ForceNoCache));

        if let Some(preprocessor_key) = preprocessor_key {
            if matches!(cache_control, CacheControl::ForceRecache) {
                debug!("[{out_pretty}]: Preprocessor forced re-cache: {preprocessor_key}");
                return Ok(PreprocessorCacheLookup::Miss(preprocessor_key));
            }

            if let Cache::Hit(mut preprocessor_cache_entry) =
                get_preprocessor_cache_entry(storage, &preprocessor_key).await?
            {
                let (hit, updated, preprocessor_cache_entry) =
                    tokio::task::spawn_blocking(move || {
                        let mut updated = false;
                        let hit = preprocessor_cache_entry
                            .lookup_result_digest(&preprocessor_cache_mode_config, &mut updated);
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
                    debug!(
                        "[{out_pretty}]: Preprocessor cache updated because of time macros: {preprocessor_key}"
                    );

                    if let Err(e) = put_preprocessor_cache_entry(
                        storage,
                        &preprocessor_key,
                        preprocessor_cache_entry,
                    )
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
        _pool: &tokio::runtime::Handle,
        rewrite_includes_only: bool,
        storage: Arc<dyn Storage>,
        cache_control: CacheControl,
    ) -> Result<HashResult<T>> {
        let start_of_compilation = std::time::SystemTime::now();

        let CCompilerHasher {
            compiler,
            executable,
            executable_digest,
            parsed_args,
            ..
        } = self.as_ref();

        // A compiler binary may be a symlink to another and so has the same digest, but that means
        // the toolchain will not contain the correct path to invoke the compiler! Add the compiler
        // executable path to try and prevent this
        let weak_toolchain_key = format!("{}-{}", executable.to_string_lossy(), executable_digest);

        // Skip preprocessing if we're not going to do any cache reads/writes
        // In this mode, the cache key doesn't matter, so return empty string
        if CacheControl::ForceNoCache == cache_control {
            return Ok(HashResult {
                key: String::new(),
                compilation: Box::new(CCompilation {
                    compiler: self.compiler,
                    creator: creator.to_owned(),
                    cwd,
                    env_vars,
                    executable: self.executable,
                    is_locally_preprocessed: false,
                    parsed_args: self.parsed_args,
                    rewrite_includes_only,
                    service: service.to_owned(),
                }),
                weak_toolchain_key,
            });
        }

        let extra_hashes = if parsed_args.extra_hash_files.is_empty() {
            vec![]
        } else {
            hash_all(&parsed_args.extra_hash_files).await?
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

        match preprocessor_cache_lookup {
            PreprocessorCacheLookup::Hit(_) => {
                service
                    .stats
                    .lock()
                    .await
                    .preprocessor_cache_hits
                    .increment(
                        &CompilerKind::C(compiler.kind()),
                        &<CCompilerHasher<I> as CompilerHasher<T>>::language(&*self),
                    );
            }
            PreprocessorCacheLookup::Miss(_) => {
                service
                    .stats
                    .lock()
                    .await
                    .preprocessor_cache_misses
                    .increment(
                        &CompilerKind::C(compiler.kind()),
                        &<CCompilerHasher<I> as CompilerHasher<T>>::language(&*self),
                    );
            }
            _ => {}
        };

        // Skip preprocessing if it's a preprocessor cache hit
        if let PreprocessorCacheLookup::Hit(key) = preprocessor_cache_lookup {
            return Ok(HashResult {
                key,
                compilation: Box::new(CCompilation {
                    compiler: self.compiler,
                    creator: creator.to_owned(),
                    cwd,
                    env_vars,
                    executable: self.executable,
                    is_locally_preprocessed: false,
                    parsed_args: self.parsed_args,
                    rewrite_includes_only,
                    service: service.to_owned(),
                }),
                weak_toolchain_key,
            });
        }

        let result = compiler
            .preprocess(
                service,
                creator,
                executable,
                parsed_args,
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
                trace!("[{out_pretty}]: removing files {outputs:?}");

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
                        "[{out_pretty}]: Could not remove files after preprocessing failed: {outputs:?}"
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

        let key = match preprocessor_output {
            PreprocessorOutput::File(ref res) => hash_key(
                executable_digest,
                parsed_args.language,
                &common_and_arch_args,
                &extra_hashes,
                &env_vars,
                &mut fs::File::open(res.path())?,
                compiler.plusplus(),
            ),
            PreprocessorOutput::Output(ref mut res) => hash_key(
                executable_digest,
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
                        tokio::task::spawn_blocking(move || -> Result<Option<(PathBuf, String)>> {
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
                        })
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
                    executable_digest,
                    parsed_args.language,
                    &common_and_arch_args,
                    &extra_hashes,
                    &env_vars,
                    &mut res.stdout.reader(),
                    compiler.plusplus(),
                );

                // Cache the preprocessing step
                if let PreprocessorCacheLookup::Miss(preprocessor_key) = preprocessor_cache_lookup {
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

                        if let Err(err) = put_preprocessor_cache_entry(
                            storage.as_ref(),
                            &preprocessor_key,
                            preprocessor_cache_entry,
                        )
                        .await
                        {
                            debug!("[{out_pretty}]: Failed to update preprocessor cache: {err}");
                        }
                    }
                }

                key
            }
        };

        Ok(HashResult {
            key,
            compilation: Box::new(CCompilation {
                compiler: self.compiler,
                creator: creator.to_owned(),
                cwd,
                env_vars,
                executable: self.executable,
                is_locally_preprocessed: true,
                parsed_args: self.parsed_args,
                rewrite_includes_only,
                service: service.to_owned(),
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
            "[{out_pretty}]: Cannot use preprocessor cache because {:?}",
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

#[derive(Debug, Clone)]
struct CCompilerCommand<C, I, T>
where
    C: CCompilerImpl,
    I: CompileCommandImpl,
    T: CommandCreatorSync,
{
    cmd: I,
    compilation: CCompilation<T, C>,
}

impl<C, I, T> CCompilerCommand<C, I, T>
where
    C: CCompilerImpl,
    I: CompileCommandImpl,
    T: CommandCreatorSync,
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(cmd: I, compilation: CCompilation<T, C>) -> Box<dyn CompileCommand<T>> {
        Box::new(CCompilerCommand { cmd, compilation }) as Box<dyn CompileCommand<T>>
    }
}

#[async_trait]
impl<C, I, T> CompileCommand<T> for CCompilerCommand<C, I, T>
where
    C: CCompilerImpl,
    I: CompileCommandImpl,
    T: CommandCreatorSync,
{
    fn get_executable(&self) -> PathBuf {
        self.cmd.get_executable()
    }
    fn get_arguments(&self) -> Vec<OsString> {
        self.cmd.get_arguments()
    }
    fn get_env_vars(&self) -> Vec<(OsString, OsString)> {
        self.cmd.get_env_vars()
    }
    fn get_cwd(&self) -> PathBuf {
        self.cmd.get_cwd()
    }

    async fn execute(
        &self,
        service: &crate::server::SccacheService<T>,
        creator: &T,
        active: crate::server::SccacheGaugeIncrement,
    ) -> Result<ProcessOutput> {
        let out = self.cmd.execute(service, creator, active).await?;
        // Ensure the dependency file exists
        self.compilation.generate_dependencies(creator).await?;
        Ok(out)
    }

    fn box_clone(&self) -> Box<dyn CompileCommand<T>> {
        CCompilerCommand::<C, I, T>::new(self.cmd.clone(), self.compilation.clone())
    }
}

#[async_trait]
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
            parsed_args,
            executable,
            compiler,
            cwd,
            env_vars,
            ..
        } = self;

        compiler
            .generate_compile_commands(
                path_transformer,
                executable,
                parsed_args,
                cwd,
                env_vars,
                rewrite_includes_only,
                hash_key,
            )
            .map(|(command, dist_command, cacheable)| {
                (
                    CCompilerCommand::new(command, self.clone()),
                    dist_command,
                    cacheable,
                )
            })
    }

    #[allow(dead_code)]
    async fn generate_dependencies(&self, creator: &T) -> Result<()> {
        let CCompilation {
            parsed_args,
            executable,
            compiler,
            cwd,
            env_vars,
            ..
        } = self;

        // Ensure the depfile exists if it is required and doesn't already.
        //
        // When not configured for dist-compile, the depfile is either created
        // by the preprocessor, generated during compile, or restored from
        // cache.
        //
        // If we're using preprocessor cache mode with sccache-dist, it's possible
        // to get a preprocessor cache hit, an object cache miss (i.e. changed from
        // remote to local caching), and then dist-compile. However, dist-compile
        // doesn't generate dependency files because it compiles the preprocessed
        // source. Preprocessor-cache mode means we skip calling the preprocessor,
        // so we have to generate the dependency file after the fact.
        if let Some(depfile) = parsed_args.depfile.as_ref() {
            if !depfile.exists() {
                compiler
                    .generate_dependencies(creator, executable, parsed_args, cwd, env_vars)
                    .await?;
            }
        }
        Ok(())
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
            parsed_args: self.parsed_args.to_owned(),
        });

        let outputs_rewriter = Box::new(NoopOutputsRewriter);

        Ok((self, toolchain_packager, outputs_rewriter))
    }

    fn is_locally_preprocessed(&self) -> bool {
        self.is_locally_preprocessed
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
            ..
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
                true, // generate_dependencies
                true, // include_line_numbers
            )
            .await?;

        use std::collections::BTreeMap;
        use std::path::PathBuf;

        let mut builder = tar::Builder::new(compressor);
        let mut path_transformer = path_transformer.clone();
        let mut symlinks = BTreeMap::<PathBuf, PathBuf>::new();

        tokio::task::spawn_blocking(move || -> Result<_> {
            let dependencies = {
                let input_path = cwd.join(&parsed_args.input);
                let input_path = pkg::tarify_path(&mut symlinks, &input_path)?;

                match preprocessor_output {
                    PreprocessorOutput::File(file) => {
                        builder.append_path_with_name(
                            file.path(),
                            path_transformer
                                .as_dist(&input_path)
                                .map(pkg::tar_safe_path)
                                .with_context(|| {
                                    format!(
                                        "unable to transform input path {}",
                                        input_path.display()
                                    )
                                })?,
                        )?;
                        vec![]
                    }
                    PreprocessorOutput::OutputWithDepedencies(output, dependencies) => {
                        let dist_input_path = path_transformer
                            .as_dist_input_path(&input_path)
                            .with_context(|| {
                                format!("unable to transform input path {}", input_path.display())
                            })?;
                        let (mut file_header, dist_input_path) =
                            pkg::make_tar_header(&input_path, &dist_input_path)?;
                        file_header.set_size(output.stdout.len() as u64); // The metadata is from non-preprocessed
                        file_header.set_cksum();
                        builder.append_data(
                            &mut file_header,
                            dist_input_path,
                            output.stdout.as_slice(),
                        )?;
                        dependencies
                    }
                    _ => unreachable!(),
                }
            };

            let extra_dist_files = parsed_args.extra_dist_files;
            let extra_hash_files = parsed_args.extra_hash_files;

            for extra_path in dependencies
                .iter()
                .chain(extra_hash_files.iter())
                .chain(extra_dist_files.iter())
            {
                let extra_path = pkg::tarify_path(&mut symlinks, extra_path)?;

                if !super::CAN_DIST_DYLIBS
                    && extra_path
                        .extension()
                        .is_some_and(|ext| ext == std::env::consts::DLL_EXTENSION)
                {
                    bail!(
                        "Cannot distribute dylib input {} on this platform",
                        extra_path.display()
                    )
                }

                builder.append_path_with_name(
                    &extra_path,
                    path_transformer
                        .as_dist(&extra_path)
                        .map(pkg::tar_safe_path)
                        .with_context(|| {
                            format!("unable to transform input path {}", extra_path.display())
                        })?,
                )?;
            }

            for (from_path, to_path) in symlinks.iter() {
                let mut header = tar::Header::new_gnu();
                header.set_size(0);
                header.set_mtime(0);
                header.set_entry_type(tar::EntryType::Symlink);
                // Leave `to_path` as absolute, assuming the tar will
                // be used in a chroot-like environment.
                builder.append_link(&mut header, pkg::tar_safe_path(from_path), to_path)?;
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
    parsed_args: ParsedArguments,
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
    async fn package(&self) -> Result<Arc<dyn pkg::PackagedToolchain>> {
        use std::os::unix::ffi::OsStringExt;

        debug!("Generating toolchain {}", self.executable.display());
        let mut package_builder = pkg::ToolchainPackaged::new();
        package_builder.add_common()?;

        // Helper to use -print-file-name and -print-prog-name to look up
        // files by path.
        let named_file = |arg: &str| -> Option<PathBuf> {
            let mut output = std::process::Command::new(&self.executable)
                .arg(arg)
                .output()
                .ok()?;
            debug!(
                "{} {arg} output:\n{}\n===\n{}",
                self.executable.display(),
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
        let add_named_prog = |builder: &mut pkg::ToolchainPackaged, name: &str| -> Result<()> {
            if let Some(path) = named_file(&format!("-print-prog-name={name}")) {
                builder.add_executable_and_deps(&self.env_vars, &path)?;
            }
            Ok(())
        };
        let add_named_file = |builder: &mut pkg::ToolchainPackaged, name: &str| -> Result<()> {
            if let Some(path) = named_file(&format!("-print-file-name={name}")) {
                builder.add_file(&self.env_vars, path)?;
            }
            Ok(())
        };

        let mut add_default_files = || -> Result<()> {
            // Add basic |as| and |objcopy| programs.
            add_named_prog(&mut package_builder, "as")?;
            add_named_prog(&mut package_builder, "objcopy")?;

            // Linker configuration.
            if Path::new("/etc/ld.so.conf").is_file() {
                package_builder.add_file(&self.env_vars, "/etc/ld.so.conf".into())?;
            }
            Ok(())
        };

        // Compiler-specific handling
        match self.kind {
            CCompilerKind::Clang => {
                add_default_files()?;
                package_builder.add_executable_and_deps(&self.env_vars, &self.executable)?;
                // Clang uses internal header files, so add them.
                if let Some(limits_h) = named_file("-print-file-name=include/limits.h") {
                    info!("limits_h = {}", limits_h.display());
                    package_builder.add_dir_contents(&self.env_vars, limits_h.parent().unwrap())?;
                }
            }

            CCompilerKind::Gcc => {
                // Various external programs / files which may be needed by gcc
                add_default_files()?;
                package_builder.add_executable_and_deps(&self.env_vars, &self.executable)?;
                add_named_prog(&mut package_builder, "cc1")?;
                add_named_prog(&mut package_builder, "cc1plus")?;
                add_named_file(&mut package_builder, "liblto_plugin.so")?;
                // Add gcc implicit specfiles
                let jobserver = crate::jobserver::Client::new_num(1);
                let mut creator = crate::mock_command::ProcessCommandCreator::new(&jobserver);
                for path in crate::compiler::gcc::Gcc::read_implicit_specfiles(
                    &mut creator,
                    &self.executable,
                    &[],
                    &self.env_vars,
                    "-v",
                )
                .await?
                {
                    package_builder.add_file(&self.env_vars, path)?;
                }
            }

            CCompilerKind::Nvhpc => {
                // Various programs called by the nvc/nvc++ front end.
                let _ = add_default_files();

                // Handle NVHPC's mpicc/mpic++ compiler wrappers
                let executable = if let Some(executable) = named_file("-showme:command") {
                    package_builder.add_executable_and_deps(&self.env_vars, &executable)?;

                    // Handle NVHPC's symlinks of `mpic++ -> bin/env.sh` where `env.sh`
                    // is a wrapper to the real `mpic++` executable at `bin/.bin/mpic++`
                    if let Err(orig_err) =
                        package_builder.add_executable_and_deps(&self.env_vars, &self.executable)
                    {
                        if let Some(exe_dir) = self.executable.parent() {
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
                                // Symlink `bin/mpic++` -> `bin/.bin/mpic++`
                                package_builder.add_link(&real_exe, &self.executable)?;
                                package_builder
                                    .add_executable_and_deps(&self.env_vars, &real_exe)?;
                            }
                        } else {
                            return Err(orig_err);
                        }
                    }
                    executable
                } else {
                    // If `-showme:command` fails, self.executable is nvc/nvc++
                    package_builder.add_executable_and_deps(&self.env_vars, &self.executable)?;
                    self.executable.clone()
                };

                let (contents, dirs, mut files): (Vec<PathBuf>, Vec<PathBuf>, Vec<PathBuf>) = {
                    use futures::{AsyncBufReadExt, TryStreamExt, future, io::BufReader};
                    use std::process::Stdio;
                    use tokio_util::compat::TokioAsyncReadCompatExt;

                    let mut child = tokio::process::Command::new(&executable)
                        .arg("-show")
                        .stdin(Stdio::null())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .spawn()?;

                    let info = child.stdout.take().unwrap();
                    let info = BufReader::new(info.compat());

                    // DEFCPPINC      These dirs just need to exist
                    // DEFSTDINC      These dirs just need to exist
                    // GCCINC         These dirs just need to exist
                    // GPPDIR         These dirs just need to exist
                    // STDINC         These dirs just need to exist
                    // SYSTEMINC      These dirs just need to exist
                    // NVCOMPILER                                         =/opt/nvidia/hpc_sdk/Linux_x86_64/25.7
                    // COMPBIN        Compiler binary directory           =/opt/nvidia/hpc_sdk/Linux_x86_64/25.7/compilers/bin
                    // COMPLIB        Compiler library directory          =/opt/nvidia/hpc_sdk/Linux_x86_64/25.7/compilers/lib
                    // CCOMPDIR       Directory containing the C compiler =/opt/nvidia/hpc_sdk/Linux_x86_64/25.7/compilers/bin/tools
                    // CPPCOMPDIR     Directory containing the C compiler =/opt/nvidia/hpc_sdk/Linux_x86_64/25.7/compilers/bin/tools
                    // COMPINCDIRFULL                                     =/opt/nvidia/hpc_sdk/Linux_x86_64/25.7/compilers/include /opt/nvidia/hpc_sdk/Linux_x86_64/25.7/compilers/include-stdexec
                    // CUDAROOT                                           =/opt/nvidia/hpc_sdk/Linux_x86_64/25.7/cuda/12.9
                    // CUDADIR                                            =/opt/nvidia/hpc_sdk/Linux_x86_64/25.7/cuda/12.9/bin
                    // CUDALIBDIR                                         =/opt/nvidia/hpc_sdk/Linux_x86_64/25.7/cuda/12.9/lib64
                    // LLVMBINDIR     Directory containing LLVM tools     =/opt/nvidia/hpc_sdk/Linux_x86_64/25.7/compilers/share/llvm/bin

                    info.lines()
                        .try_filter(|line| future::ready(line.len() >= 14 && line.contains('=')))
                        .try_filter_map(|mut line| {
                            future::ok(match &line[0..14] {
                                "CCOMPDIR      " | // .
                                "COMPBIN       " | // .
                                "COMPINCDIRFULL" | // .
                                "COMPLIB       " | // .
                                "CPPCOMPDIR    " | // .
                                "CUDADIR       " | // .
                                "CUDALIBDIR    " | // .
                                "CUDAROOT      " | // .
                                "DEFCPPINC     " | // .
                                "DEFSTDINC     " | // .
                                "GCCINC        " | // .
                                "GPPDIR        " | // .
                                "LLVMBINDIR    " | // .
                                "NVCOMPILER    " | // .
                                "STDINC        " | // .
                                "SYSTEMINC     " => {
                                    line.find('=')
                                        .map(|idx| {
                                            line.split_off(idx + 1)
                                                .trim()
                                                .split(' ')
                                                .map(PathBuf::from)
                                                .collect::<Vec<_>>()
                                        })
                                        .map(|dirs| (line, dirs))
                                }
                                _ => None,
                            })
                        })
                        .try_filter_map(|(line, dirs)| {
                            let mut contents = vec![];
                            let mut files = vec![];

                            match &line[0..14] {
                                "NVCOMPILER    " => {
                                    files.extend(dirs.iter().flat_map(|root| {
                                        use crate::util::OsStrExt;
                                        walkdir::WalkDir::new(root)
                                            .follow_links(true)
                                            .same_file_system(true)
                                            .into_iter()
                                            .filter_map_ok(|e| {
                                                if e.file_type().is_file()
                                                {
                                                    let name = e.file_name();
                                                    None
                                                        // rcfiles
                                                        .or(name.ends_with("rc").then_some(true))
                                                        .or(name.ends_with(".bc").then_some(true))
                                                        // mpic++-wrapper-data.txt
                                                        .or(name.ends_with("-wrapper-data.txt").then_some(true))
                                                        .or(name.ends_with("help-opal-wrapper.txt").then_some(true))
                                                        .map(|_| e.into_path())
                                                } else {
                                                    None
                                                }
                                            })
                                            .filter_map(|r| r.ok())
                                    }));
                                }
                                "COMPBIN       " => {
                                    contents.extend(dirs.iter().map(|root| root.join("rcfiles")));
                                }
                                "COMPLIB       " => {
                                    files.extend(dirs.iter().flat_map(|root| {
                                        [
                                            "acc_init_link_cuda.o",
                                            "cuda_init_register_end.o",
                                            "init_pgpf.o",
                                            "libmem.il",
                                            "nvhpc.ld",
                                            "nvhpc.syms",
                                        ]
                                        .iter()
                                        .map(|name| root.join(name))
                                    }));
                                }
                                "CCOMPDIR      " | // .
                                "CPPCOMPDIR    " => {
                                    // Add everything under `compilers/bin/tools`
                                    contents.extend_from_slice(dirs.as_slice());
                                }
                                "COMPINCDIRFULL" => {
                                    // NVHPC uses internal LLVM header files, so add them.
                                    contents.extend_from_slice(dirs.as_slice());
                                }
                                "CUDAROOT      " => {
                                    files.extend(dirs.iter().map(|root| root.join("nvvm/bin/cicc")));
                                    files.extend(
                                        dirs.iter().flat_map(|root| {
                                            [
                                                "include/cuda.h",
                                                "nvvm/lib64/libnvvm.so"
                                            ]
                                            .iter()
                                            .map(|name| root.join(name))
                                        })
                                    );
                                }
                                "CUDADIR       " => {
                                    files.extend(dirs.iter().flat_map(|root| {
                                        [
                                            "bin2c",
                                            "cudafe++",
                                            "fatbinary",
                                            "nvlink",
                                            "nvprune",
                                            "ptxas",
                                        ]
                                        .iter()
                                        .map(|name| root.join(name))
                                    }));
                                    files.extend(dirs.iter().map(|root| root.join("nvcc.profile")));
                                }
                                "CUDALIBDIR    " => {}
                                "LLVMBINDIR    " => {
                                    files.extend(dirs.iter().flat_map(|root| {
                                        ["llc", "opt", "llvm-as", "llvm-link", "llvm-mc"]
                                            .iter()
                                            .map(|name| root.join(name))
                                    }));
                                }
                                _ => {}
                            }

                            future::ok(Some((contents, dirs, files)))
                        })
                        .try_fold((vec![], vec![], vec![]), |mut acc, res| {
                            future::ok({
                                acc.0.extend(res.0);
                                acc.1.extend(res.1);
                                acc.2.extend(res.2);
                                acc
                            })
                        })
                        .await?
                };

                if let Ok(as_path) = which::which("as") {
                    files.push(as_path);
                }

                for path in dirs.into_iter().filter(|p| p.exists()) {
                    if path.is_dir() {
                        let _ = package_builder
                            .add_dir(path.clone())
                            .map_err(|e| trace!("add_dir error {path:?}: {e:?}"));
                    }
                }

                for path in files.into_iter().filter(|p| p.exists()) {
                    if path.is_file() || path.is_symlink() {
                        let _ = package_builder
                            .add_file(&self.env_vars, path.clone())
                            .map_err(|e| trace!("add_file error {path:?}: {e:?}"));
                    }
                }

                for path in contents.into_iter().filter(|p| p.exists()) {
                    if path.is_dir() {
                        let _ = package_builder
                            .add_dir_contents(&self.env_vars, &path)
                            .map_err(|e| trace!("add_dir_contents error {path:?}: {e:?}"));
                    }
                }
            }

            _ => {
                package_builder.add_executable_and_deps(&self.env_vars, &self.executable)?;
            }
        }

        // Return the builder so the archive can be lazily created, depending
        // on whether the scheduler reports it already has the toolchain or not
        Ok(Arc::new(package_builder))
    }
}

/// The cache is versioned by the inputs to `hash_key`.
pub const CACHE_VERSION: &[u8] = b"11";

/// Environment variables that are factored into the cache key.
static CACHED_ENV_VARS: LazyLock<HashSet<&'static OsStr>> = LazyLock::new(|| {
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
    use crate::{
        cache::StorageKind,
        config::{CacheModeConfig, CacheType, Config, DiskCacheConfig},
    };

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

    #[test]
    fn test_read_write_local_preprocessor_cache() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .unwrap();

        // Use disk cache.
        let tempdir = crate::util::normal_tempdir()
            .context("Failed to create tempdir")
            .unwrap();

        let cache_dir = tempdir.path().join("cache");
        fs::create_dir(&cache_dir).unwrap();

        let make_config = |rw_mode| Config {
            caches: vec![CacheType::Disk(DiskCacheConfig {
                dir: cache_dir.clone(),
                rw_mode,
                ..DiskCacheConfig::default()
            })],
            ..Default::default()
        };

        // Test Read Write
        {
            let caches = make_config(CacheModeConfig::ReadWrite).caches;
            runtime.block_on(async {
                let storage = StorageKind::Preprocessor.create(&caches).await.unwrap();
                put_preprocessor_cache_entry(
                    storage.as_ref(),
                    "test1",
                    PreprocessorCacheEntry::default(),
                )
                .await
                .unwrap();
            });
        }

        // Test Read-only
        {
            let caches = make_config(CacheModeConfig::ReadOnly).caches;
            runtime.block_on(async {
                let storage = StorageKind::Preprocessor.create(&caches).await.unwrap();
                assert_eq!(
                    put_preprocessor_cache_entry(
                        storage.as_ref(),
                        "test1",
                        PreprocessorCacheEntry::default()
                    )
                    .await
                    .unwrap_err()
                    .to_string(),
                    "Cannot write to read-only storage"
                );
            });
        }
    }
}
