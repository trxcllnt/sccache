#[cfg(feature = "dist-client")]
pub use self::client::ClientToolchains;
#[cfg(feature = "dist-server")]
pub use self::server::ServerToolchains;

#[cfg(feature = "dist-client")]
mod client {

    use anyhow::{bail, Context, Error, Result};
    use fs_err as fs;
    use std::collections::{HashMap, HashSet};
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use crate::config;
    use crate::dist::pkg::ToolchainPackager;
    use crate::dist::Toolchain;
    use crate::lru_disk_cache::Error as LruError;
    use crate::lru_disk_cache::LruDiskCache;
    use crate::util::Digest;

    fn path_key(path: &Path) -> Result<String> {
        file_key(fs::File::open(path)?)
    }

    fn file_key<R: Read>(rdr: R) -> Result<String> {
        Digest::reader_sync(rdr)
    }

    #[derive(Clone, Debug)]
    pub struct CustomToolchain {
        archive: PathBuf,
        compiler_executable: String,
    }

    // TODO: possibly shouldn't be public
    pub struct ClientToolchains {
        cache_dir: PathBuf,
        cache: Mutex<LruDiskCache>,
        // Lookup from dist toolchain -> path to custom toolchain archive
        custom_toolchain_archives: Mutex<HashMap<Toolchain, PathBuf>>,
        // Lookup from local path -> toolchain details
        // The Option<Toolchain> could be populated on startup, but it's lazy for efficiency
        custom_toolchain_paths: Mutex<HashMap<PathBuf, (CustomToolchain, Option<Toolchain>)>>,
        // Toolchains configured to not be distributed
        disabled_toolchains: HashSet<PathBuf>,
        // Local machine mapping from 'weak' hashes to strong toolchain hashes
        // - Weak hashes are what sccache uses to determine if a compiler has changed
        //   on the local machine - they're fast and 'good enough' (assuming we trust
        //   the local machine), but not safe if other users can update the cache.
        // - Strong hashes (or 'archive ids') are the hash of the complete compiler contents that
        //   will be sent over the wire for use in distributed compilation - it is assumed
        //   that if two of them match, the contents of a compiler archive cannot
        //   have been tampered with
        weak_map: Mutex<HashMap<String, String>>,
    }

    impl ClientToolchains {
        pub fn new(
            cache_dir: &Path,
            cache_size: u64,
            toolchain_configs: &[config::DistToolchainConfig],
        ) -> Result<Self> {
            let cache_dir = cache_dir.to_owned();
            fs::create_dir_all(&cache_dir).context(format!(
                "failed to create top level toolchain cache dir: {}",
                cache_dir.display()
            ))?;

            let toolchain_creation_dir = cache_dir.join("toolchain_tmp");
            if toolchain_creation_dir.exists() {
                fs::remove_dir_all(&toolchain_creation_dir).context(format!(
                    "failed to clean up temporary toolchain creation directory: {}",
                    toolchain_creation_dir.display()
                ))?
            }
            fs::create_dir(&toolchain_creation_dir).context(format!(
                "failed to create temporary toolchain creation directory: {}",
                toolchain_creation_dir.display()
            ))?;

            let weak_map_path = cache_dir.join("weak_map.json");
            if !weak_map_path.exists() {
                fs::File::create(&weak_map_path)
                    .and_then(|mut f| f.write_all(b"{}"))
                    .context(format!(
                        "failed to create new toolchain weak map file: {}",
                        weak_map_path.display()
                    ))?
            }
            let weak_map = fs::File::open(&weak_map_path)
                .map_err(Error::from)
                .and_then(|f| serde_json::from_reader(f).map_err(Error::from))
                .context(format!(
                    "failed to load toolchain weak map: {}",
                    weak_map_path.display()
                ))?;

            let tc_cache_dir = cache_dir.join("tc");
            let cache = LruDiskCache::new(&tc_cache_dir, cache_size)
                .map(Mutex::new)
                .context("failed to initialise a toolchain cache")?;

            // Load in toolchain configuration
            let mut custom_toolchain_paths = HashMap::new();
            let mut disabled_toolchains = HashSet::new();
            for ct in toolchain_configs.iter() {
                match ct {
                    config::DistToolchainConfig::PathOverride {
                        compiler_executable,
                        archive,
                        archive_compiler_executable,
                    } => {
                        debug!(
                            "Registering custom toolchain for {}",
                            compiler_executable.display()
                        );
                        let custom_tc = CustomToolchain {
                            archive: archive.clone(),
                            compiler_executable: archive_compiler_executable.clone(),
                        };
                        if custom_toolchain_paths
                            .insert(compiler_executable.clone(), (custom_tc, None))
                            .is_some()
                        {
                            bail!("Multiple toolchains for {}", compiler_executable.display())
                        }
                        if disabled_toolchains.contains(compiler_executable) {
                            bail!(
                                "Override for toolchain {} conflicts with it being disabled",
                                compiler_executable.display()
                            )
                        }
                    }
                    config::DistToolchainConfig::NoDist {
                        compiler_executable,
                    } => {
                        debug!("Disabling toolchain {}", compiler_executable.display());
                        if !disabled_toolchains.insert(compiler_executable.clone()) {
                            bail!(
                                "Disabled toolchain {} multiple times",
                                compiler_executable.display()
                            )
                        }
                        if custom_toolchain_paths.contains_key(compiler_executable) {
                            bail!(
                                "Override for toolchain {} conflicts with it being disabled",
                                compiler_executable.display()
                            )
                        }
                    }
                }
            }
            let custom_toolchain_paths = Mutex::new(custom_toolchain_paths);

            Ok(Self {
                cache_dir,
                cache,
                custom_toolchain_archives: Mutex::new(HashMap::new()),
                custom_toolchain_paths,
                disabled_toolchains,
                // TODO: shouldn't clear on restart, but also should have some
                // form of pruning
                weak_map: Mutex::new(weak_map),
            })
        }

        // Get the bytes of a toolchain tar
        // TODO: by this point the toolchain should be known to exist
        pub fn get_toolchain(&self, tc: &Toolchain) -> Result<Option<fs::File>> {
            // TODO: be more relaxed about path casing and slashes on Windows
            let file = if let Some(custom_tc_archive) =
                self.custom_toolchain_archives.lock().unwrap().get(tc)
            {
                fs::File::open(custom_tc_archive).with_context(|| {
                    format!(
                        "could not open file for toolchain {}",
                        custom_tc_archive.display()
                    )
                })?
            } else {
                match self.cache.lock().unwrap().get_file(&tc.archive_id) {
                    Ok(file) => file,
                    Err(LruError::FileNotInCache) => return Ok(None),
                    Err(e) => return Err(e).context("error while retrieving toolchain from cache"),
                }
            };
            Ok(Some(file))
        }
        // If the toolchain doesn't already exist, create it and insert into the cache
        pub fn put_toolchain(
            &self,
            compiler_path: &Path,
            weak_key: &str,
            toolchain_packager: Box<dyn ToolchainPackager>,
        ) -> Result<(Toolchain, Option<(String, PathBuf)>)> {
            if self.disabled_toolchains.contains(compiler_path) {
                bail!(
                    "Toolchain distribution for {} is disabled",
                    compiler_path.display()
                )
            }
            if let Some(tc_and_paths) = self.get_custom_toolchain(compiler_path) {
                debug!("Using custom toolchain for {:?}", compiler_path);
                let (tc, compiler_path, archive) = tc_and_paths?;
                return Ok((tc, Some((compiler_path, archive))));
            }
            // Only permit one toolchain creation at a time. Not an issue if there are multiple attempts
            // to create the same toolchain, just a waste of time
            let mut cache = self.cache.lock().unwrap();
            if let Some(archive_id) = self.weak_to_strong(weak_key) {
                trace!("Using cached toolchain {} -> {}", weak_key, archive_id);
                return Ok((Toolchain { archive_id }, None));
            }
            debug!("Weak key {} appears to be new", weak_key);
            let tmpfile = tempfile::NamedTempFile::new_in(self.cache_dir.join("toolchain_tmp"))?;
            toolchain_packager
                .write_pkg(fs_err::File::from_parts(tmpfile.reopen()?, tmpfile.path()))
                .context("Could not package toolchain")?;
            let tc = Toolchain {
                archive_id: path_key(tmpfile.path())?,
            };
            cache.insert_file(&tc.archive_id, tmpfile.path())?;
            self.record_weak(weak_key.to_owned(), tc.archive_id.clone())?;
            Ok((tc, None))
        }

        pub fn get_custom_toolchain(
            &self,
            compiler_path: &Path,
        ) -> Option<Result<(Toolchain, String, PathBuf)>> {
            match self
                .custom_toolchain_paths
                .lock()
                .unwrap()
                .get_mut(compiler_path)
            {
                Some((custom_tc, Some(tc))) => Some(Ok((
                    tc.clone(),
                    custom_tc.compiler_executable.clone(),
                    custom_tc.archive.clone(),
                ))),
                Some((custom_tc, maybe_tc @ None)) => {
                    let archive_id = match path_key(&custom_tc.archive) {
                        Ok(archive_id) => archive_id,
                        Err(e) => return Some(Err(e)),
                    };
                    let tc = Toolchain { archive_id };
                    *maybe_tc = Some(tc.clone());
                    // If this entry already exists, someone has two custom toolchains with the same strong hash
                    if let Some(old_path) = self
                        .custom_toolchain_archives
                        .lock()
                        .unwrap()
                        .insert(tc.clone(), custom_tc.archive.clone())
                    {
                        // Log a warning if the user has identical toolchains at two different locations - it's
                        // not strictly wrong, but it is a bit odd
                        if old_path != custom_tc.archive {
                            warn!(
                                "Detected interchangeable toolchain archives at {} and {}",
                                old_path.display(),
                                custom_tc.archive.display()
                            )
                        }
                    }
                    Some(Ok((
                        tc,
                        custom_tc.compiler_executable.clone(),
                        custom_tc.archive.clone(),
                    )))
                }
                None => None,
            }
        }

        fn weak_to_strong(&self, weak_key: &str) -> Option<String> {
            self.weak_map
                .lock()
                .unwrap()
                .get(weak_key)
                .map(String::to_owned)
        }
        fn record_weak(&self, weak_key: String, key: String) -> Result<()> {
            let mut weak_map = self.weak_map.lock().unwrap();
            weak_map.insert(weak_key, key);
            let weak_map_path = self.cache_dir.join("weak_map.json");
            fs::File::create(weak_map_path)
                .map_err(Error::from)
                .and_then(|f| serde_json::to_writer(f, &*weak_map).map_err(Error::from))
                .context("failed to enter toolchain in weak map")
        }
    }

    #[cfg(test)]
    mod test {
        use crate::config;
        use crate::test::utils::create_file;
        use std::io::Write;

        use super::ClientToolchains;

        struct PanicToolchainPackager;
        impl PanicToolchainPackager {
            fn new() -> Box<Self> {
                Box::new(PanicToolchainPackager)
            }
        }
        #[cfg(any(
            all(target_os = "linux", target_arch = "x86_64"),
            all(target_os = "linux", target_arch = "aarch64"),
        ))]
        impl crate::dist::pkg::ToolchainPackager for PanicToolchainPackager {
            fn write_pkg(self: Box<Self>, _f: super::fs::File) -> crate::errors::Result<()> {
                panic!("should not have called packager")
            }
        }

        #[test]
        fn test_client_toolchains_custom() {
            let td = tempfile::Builder::new()
                .prefix("sccache")
                .tempdir()
                .unwrap();

            let ct1 =
                create_file(td.path(), "ct1", |mut f| f.write_all(b"toolchain_contents")).unwrap();

            let client_toolchains = ClientToolchains::new(
                &td.path().join("cache"),
                1024,
                &[config::DistToolchainConfig::PathOverride {
                    compiler_executable: "/my/compiler".into(),
                    archive: ct1.clone(),
                    archive_compiler_executable: "/my/compiler/in_archive".into(),
                }],
            )
            .unwrap();

            let (_tc, newpath) = client_toolchains
                .put_toolchain(
                    "/my/compiler".as_ref(),
                    "weak_key",
                    PanicToolchainPackager::new(),
                )
                .unwrap();
            assert!(newpath.unwrap() == ("/my/compiler/in_archive".to_string(), ct1));
        }

        #[test]
        fn test_client_toolchains_custom_multiuse_archive() {
            let td = tempfile::Builder::new()
                .prefix("sccache")
                .tempdir()
                .unwrap();

            let ct1 =
                create_file(td.path(), "ct1", |mut f| f.write_all(b"toolchain_contents")).unwrap();

            let client_toolchains = ClientToolchains::new(
                &td.path().join("cache"),
                1024,
                &[
                    config::DistToolchainConfig::PathOverride {
                        compiler_executable: "/my/compiler".into(),
                        archive: ct1.clone(),
                        archive_compiler_executable: "/my/compiler/in_archive".into(),
                    },
                    // Uses the same archive, but a maps a different external compiler to a different archive compiler
                    config::DistToolchainConfig::PathOverride {
                        compiler_executable: "/my/compiler2".into(),
                        archive: ct1.clone(),
                        archive_compiler_executable: "/my/compiler2/in_archive".into(),
                    },
                    // Uses the same archive, but a maps a different external compiler to the same archive compiler as the first
                    config::DistToolchainConfig::PathOverride {
                        compiler_executable: "/my/compiler3".into(),
                        archive: ct1.clone(),
                        archive_compiler_executable: "/my/compiler/in_archive".into(),
                    },
                ],
            )
            .unwrap();

            let (_tc, newpath) = client_toolchains
                .put_toolchain(
                    "/my/compiler".as_ref(),
                    "weak_key",
                    PanicToolchainPackager::new(),
                )
                .unwrap();
            assert!(newpath.unwrap() == ("/my/compiler/in_archive".to_string(), ct1.clone()));
            let (_tc, newpath) = client_toolchains
                .put_toolchain(
                    "/my/compiler2".as_ref(),
                    "weak_key2",
                    PanicToolchainPackager::new(),
                )
                .unwrap();
            assert!(newpath.unwrap() == ("/my/compiler2/in_archive".to_string(), ct1.clone()));
            let (_tc, newpath) = client_toolchains
                .put_toolchain(
                    "/my/compiler3".as_ref(),
                    "weak_key2",
                    PanicToolchainPackager::new(),
                )
                .unwrap();
            assert!(newpath.unwrap() == ("/my/compiler/in_archive".to_string(), ct1));
        }

        #[test]
        fn test_client_toolchains_nodist() {
            let td = tempfile::Builder::new()
                .prefix("sccache")
                .tempdir()
                .unwrap();

            let client_toolchains = ClientToolchains::new(
                &td.path().join("cache"),
                1024,
                &[config::DistToolchainConfig::NoDist {
                    compiler_executable: "/my/compiler".into(),
                }],
            )
            .unwrap();

            assert!(client_toolchains
                .put_toolchain(
                    "/my/compiler".as_ref(),
                    "weak_key",
                    PanicToolchainPackager::new()
                )
                .is_err());
        }

        #[test]
        fn test_client_toolchains_custom_nodist_conflict() {
            let td = tempfile::Builder::new()
                .prefix("sccache")
                .tempdir()
                .unwrap();

            let ct1 =
                create_file(td.path(), "ct1", |mut f| f.write_all(b"toolchain_contents")).unwrap();

            let client_toolchains = ClientToolchains::new(
                &td.path().join("cache"),
                1024,
                &[
                    config::DistToolchainConfig::PathOverride {
                        compiler_executable: "/my/compiler".into(),
                        archive: ct1,
                        archive_compiler_executable: "/my/compiler".into(),
                    },
                    config::DistToolchainConfig::NoDist {
                        compiler_executable: "/my/compiler".into(),
                    },
                ],
            );
            assert!(client_toolchains.is_err())
        }
    }
}

#[cfg(feature = "dist-server")]
mod server {
    use async_compression::tokio::bufread::GzipDecoder;

    use futures::StreamExt;
    use tokio::io::BufReader;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    use std::ffi::OsStr;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use crate::cache::disk::DiskCache;
    use crate::cache::{cache, Storage};
    use crate::dist::Toolchain;
    use crate::errors::*;

    #[derive(Clone)]
    pub struct ServerToolchains {
        cache: Arc<DiskCache>,
        store: Arc<dyn cache::Storage>,
    }

    impl ServerToolchains {
        pub fn new<P: AsRef<OsStr>>(
            root: P,
            max_size: u64,
            store: Arc<dyn cache::Storage>,
        ) -> Self {
            Self {
                cache: Arc::new(DiskCache::new(
                    root,
                    max_size,
                    Default::default(),
                    crate::cache::CacheMode::ReadWrite,
                )),
                store,
            }
        }

        pub async fn load(&self, tc: &Toolchain) -> Result<PathBuf> {
            let start = std::time::Instant::now();
            let res = loop {
                // Load and cache the deflated toolchain.
                // Inflate, unpack, and cache it in a directory.
                // Return the path to the unpacked toolchain dir.
                match self.load_inflated_toolchain(tc).await {
                    Ok(inflated_path) => break Ok(inflated_path),
                    Err(err) => {
                        tracing::warn!(
                            "ServerToolchains({})]: Error loading toolchain, retrying: {err:?}",
                            &tc.archive_id
                        );
                        continue;
                    }
                }
            };
            // Record toolchain load time
            metrics::histogram!("sccache::server::toolchain::load_time")
                .record(start.elapsed().as_secs_f64());
            res
        }

        async fn load_inflated_toolchain(&self, tc: &Toolchain) -> Result<PathBuf> {
            let start = std::time::Instant::now();
            let res = {
                if let Ok((inflated_path, _)) = self.cache.entry(&tc.archive_id).await {
                    // Return early if the toolchain is already loaded and unpacked
                    Ok(inflated_path)
                } else {
                    async move {
                        // Load the compressed toolchain
                        let deflated_path = self.load_deflated_toolchain(tc).await?;
                        // Compute the toolchain's inflated size
                        let inflated_size =
                            self.load_inflated_toolchain_size(&deflated_path).await?;
                        // Inflate and unpack the toolchain archive
                        let inflated_path = self
                            .unpack_inflated_toolchain(
                                &deflated_path,
                                &tc.archive_id,
                                inflated_size,
                            )
                            .await?;
                        Ok(inflated_path)
                    }
                    .await
                }
            };
            // Record toolchain load inflated size time
            metrics::histogram!("sccache::server::toolchain::load_inflated_time")
                .record(start.elapsed().as_secs_f64());
            res
        }

        async fn load_deflated_toolchain(&self, tc: &Toolchain) -> Result<PathBuf> {
            let start = std::time::Instant::now();
            let deflated_key = format!("{}.tgz", tc.archive_id);
            if !self.cache.has(&deflated_key).await {
                let deflated_size = self.load_deflated_toolchain_size(tc).await?;
                let reader = self.store.get_stream(&tc.archive_id).await?;
                self.cache
                    .put_stream(&deflated_key, deflated_size, std::pin::pin!(reader))
                    .await?;
            }
            let entry = self.cache.entry(&deflated_key).await;
            // Record toolchain load deflated time
            metrics::histogram!("sccache::server::toolchain::load_deflated_time")
                .record(start.elapsed().as_secs_f64());
            Ok(entry?.0)
        }

        async fn load_deflated_toolchain_size(&self, tc: &Toolchain) -> Result<u64> {
            let start = std::time::Instant::now();
            let res = self.store.size(&tc.archive_id).await;
            // Record toolchain load deflated size time
            metrics::histogram!("sccache::server::toolchain::load_deflated_size_time")
                .record(start.elapsed().as_secs_f64());
            res
        }

        async fn load_inflated_toolchain_size(&self, deflated_path: &Path) -> Result<u64> {
            let start = std::time::Instant::now();
            let deflated_file = tokio::fs::File::open(&deflated_path).await?;
            let gunzip_reader = GzipDecoder::new(BufReader::new(deflated_file));
            let inflated_size = async_tar::Archive::new(gunzip_reader.compat())
                .entries()?
                .fold(0, |inflated_size, entry| async move {
                    if let Ok(inflated_entry_size) = entry.and_then(|e| e.header().size()) {
                        inflated_size + inflated_entry_size
                    } else {
                        inflated_size
                    }
                })
                .await;
            // Record toolchain load inflated size time
            metrics::histogram!("sccache::server::toolchain::load_inflated_size_time")
                .record(start.elapsed().as_secs_f64());
            Ok(inflated_size)
        }

        async fn unpack_inflated_toolchain(
            &self,
            deflated_path: &Path,
            inflated_key: &str,
            inflated_size: u64,
        ) -> Result<PathBuf> {
            let start = std::time::Instant::now();
            let res = self
                .cache
                .insert_with(inflated_key, inflated_size, |inflated_path: &Path| {
                    let deflated_path = deflated_path.to_owned();
                    let inflated_path = inflated_path.to_owned();
                    async move {
                        // Ensure the inflated dir exists first
                        tokio::fs::create_dir_all(&inflated_path).await?;
                        let deflated_file = tokio::fs::File::open(&deflated_path).await?;
                        let gunzip_reader = GzipDecoder::new(BufReader::new(deflated_file));
                        let targz_archive = async_tar::Archive::new(gunzip_reader.compat());
                        // Unpack the tgz into the inflated dir
                        targz_archive
                            .unpack(&inflated_path)
                            .await
                            .map(|_| inflated_size)
                    }
                })
                .await
                .map(|(path, _)| path);
            // Record toolchain load inflated time
            metrics::histogram!("sccache::server::toolchain::unpack_inflated_time")
                .record(start.elapsed().as_secs_f64());
            res
        }
    }
}
