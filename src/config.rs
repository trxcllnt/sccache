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

use crate::cache::CacheMode;
use directories::ProjectDirs;
use fs::File;
use fs_err as fs;
use itertools::Itertools;
#[cfg(any(feature = "dist-client", feature = "dist-server"))]
use serde::ser::Serializer;
use serde::{
    Deserialize, Serialize,
    de::{self, DeserializeOwned, Deserializer},
};
#[cfg(test)]
use serial_test::serial;
use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

pub use crate::cache::PreprocessorCacheModeConfig;
use crate::errors::*;

static CACHED_CONFIG_PATH: LazyLock<PathBuf> = LazyLock::new(CachedConfig::file_config_path);
static CACHED_CONFIG: Mutex<Option<CachedFileConfig>> = Mutex::new(None);

const ORGANIZATION: &str = "Mozilla";
const APP_NAME: &str = "sccache";
const DIST_APP_NAME: &str = "sccache-dist-client";
const TEN_GIGS: u64 = 10 * 1024 * 1024 * 1024;

pub const INSECURE_DIST_CLIENT_TOKEN: &str = "dangerously_insecure_client";

// Unfortunately this means that nothing else can use the sccache cache dir as
// this top level directory is used directly to store sccache cached objects...
pub fn default_disk_cache_dir() -> PathBuf {
    ProjectDirs::from("", ORGANIZATION, APP_NAME)
        .expect("Unable to retrieve disk cache directory")
        .cache_dir()
        .to_owned()
}
// ...whereas subdirectories are used of this one
pub fn default_dist_cache_dir() -> PathBuf {
    ProjectDirs::from("", ORGANIZATION, DIST_APP_NAME)
        .expect("Unable to retrieve dist cache directory")
        .cache_dir()
        .to_owned()
}

fn default_disk_cache_size() -> u64 {
    TEN_GIGS
}
fn default_toolchain_cache_size() -> u64 {
    TEN_GIGS
}

struct StringOrU64Visitor;

impl de::Visitor<'_> for StringOrU64Visitor {
    type Value = u64;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a string with size suffix (like '20G') or a u64")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        parse_size(value).ok_or_else(|| E::custom(format!("Invalid size value: {}", value)))
    }

    fn visit_u64<E>(self, value: u64) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(value)
    }

    fn visit_i64<E>(self, value: i64) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        if value < 0 {
            Err(E::custom("negative values not supported"))
        } else {
            Ok(value as u64)
        }
    }
}

fn deserialize_size_from_str<'de, D>(deserializer: D) -> StdResult<u64, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(StringOrU64Visitor)
}

pub fn parse_size(val: &str) -> Option<u64> {
    let multiplier = match val.chars().last().map(|v| v.to_ascii_uppercase()) {
        Some('K') => 1024,
        Some('M') => 1024 * 1024,
        Some('G') => 1024 * 1024 * 1024,
        Some('T') => 1024 * 1024 * 1024 * 1024,
        _ => 1,
    };
    let val = if multiplier > 1 && !val.is_empty() {
        val.split_at(val.len() - 1).0
    } else {
        val
    };
    u64::from_str(val).ok().map(|size| size * multiplier)
}

#[cfg(any(feature = "dist-client", feature = "dist-server"))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HTTPUrl(reqwest::Url);
#[cfg(any(feature = "dist-client", feature = "dist-server"))]
impl Serialize for HTTPUrl {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.0.as_str())
    }
}
#[cfg(any(feature = "dist-client", feature = "dist-server"))]
impl<'a> Deserialize<'a> for HTTPUrl {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        use serde::de::Error;
        let helper: String = Deserialize::deserialize(deserializer)?;
        let url = parse_http_url(helper).map_err(D::Error::custom)?;
        Ok(HTTPUrl(url))
    }
}
#[cfg(any(feature = "dist-client", feature = "dist-server"))]
fn parse_http_url<K: AsRef<str>>(str: K) -> Result<reqwest::Url> {
    use std::net::SocketAddr;
    let url = str.as_ref();
    let url = if let Ok(sa) = url.parse::<SocketAddr>() {
        warn!("Url {} has no scheme, assuming http", url);
        reqwest::Url::parse(&format!("http://{sa}"))
    } else {
        reqwest::Url::parse(url)
    }?;
    if url.scheme() != "http" && url.scheme() != "https" {
        bail!("url not http or https")
    }
    // TODO: relative url handling just hasn't been implemented and tested
    if url.path() != "/" {
        bail!("url has a relative path (currently unsupported)")
    }
    Ok(url)
}
#[cfg(any(feature = "dist-client", feature = "dist-server"))]
impl HTTPUrl {
    pub fn from_url(u: reqwest::Url) -> Self {
        HTTPUrl(u)
    }
    pub fn to_url(&self) -> reqwest::Url {
        self.0.clone()
    }
}

enum TieredCacheOrder {
    Disk = 0,
    S3 = 1,
    Redis = 2,
    Memcached = 3,
    Gcs = 4,
    Gha = 5,
    Azure = 6,
    Webdav = 7,
    Oss = 8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureCacheConfig {
    #[serde(default)]
    pub connection_string: String,
    #[serde(default)]
    pub container: String,
    #[serde(default)]
    pub key_prefix: String,
    pub preprocessor_cache_mode: Option<PreprocessorCacheModeConfig>,
    #[serde(default = "AzureCacheConfig::default_order")]
    pub order: u64,
}

impl Default for AzureCacheConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl AzureCacheConfig {
    fn default_order() -> u64 {
        TieredCacheOrder::Azure as u64
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(default)]
pub struct DiskCacheConfig {
    pub dir: PathBuf,
    #[serde(deserialize_with = "deserialize_size_from_str")]
    pub size: u64,
    pub preprocessor_cache_mode: PreprocessorCacheModeConfig,
    pub rw_mode: CacheModeConfig,
    #[serde(default = "DiskCacheConfig::default_order")]
    pub order: u64,
}

impl DiskCacheConfig {
    fn default_order() -> u64 {
        TieredCacheOrder::Disk as u64
    }
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        DiskCacheConfig {
            dir: default_disk_cache_dir(),
            size: default_disk_cache_size(),
            preprocessor_cache_mode: PreprocessorCacheModeConfig::activated(),
            rw_mode: CacheModeConfig::ReadWrite,
            order: DiskCacheConfig::default_order(),
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CacheModeConfig {
    #[serde(rename = "READ_ONLY")]
    ReadOnly,
    #[default]
    #[serde(rename = "READ_WRITE")]
    ReadWrite,
}

impl From<CacheModeConfig> for CacheMode {
    fn from(value: CacheModeConfig) -> Self {
        match value {
            CacheModeConfig::ReadOnly => CacheMode::ReadOnly,
            CacheModeConfig::ReadWrite => CacheMode::ReadWrite,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GCSCacheConfig {
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub key_prefix: String,
    pub cred_path: Option<String>,
    pub service_account: Option<String>,
    #[serde(default)]
    pub rw_mode: CacheModeConfig,
    pub credential_url: Option<String>,
    pub preprocessor_cache_mode: Option<PreprocessorCacheModeConfig>,
    #[serde(default = "GCSCacheConfig::default_order")]
    pub order: u64,
}

impl Default for GCSCacheConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl GCSCacheConfig {
    fn default_order() -> u64 {
        TieredCacheOrder::Gcs as u64
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GHACacheConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub key_prefix: String,
    /// Version for gha cache is a namespace. By setting different versions,
    /// we can avoid mixed caches.
    #[serde(default)]
    pub version: String,
    pub preprocessor_cache_mode: Option<PreprocessorCacheModeConfig>,
    #[serde(default = "GHACacheConfig::default_order")]
    pub order: u64,
}

impl Default for GHACacheConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl GHACacheConfig {
    fn default_order() -> u64 {
        TieredCacheOrder::Gha as u64
    }
}

/// Memcached's default value of expiration is 10800s (3 hours), which is too
/// short for use case of sccache.
///
/// We increase the default expiration to 86400s (1 day) to balance between
/// memory consumpation and cache hit rate.
///
/// Please change this value freely if we have a better choice.
const DEFAULT_MEMCACHED_CACHE_EXPIRATION: u32 = 86400;

fn default_memcached_cache_expiration() -> u32 {
    DEFAULT_MEMCACHED_CACHE_EXPIRATION
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemcachedCacheConfig {
    #[serde(default)]
    #[serde(alias = "endpoint")]
    pub url: String,

    /// Username to authenticate with.
    pub username: Option<String>,

    /// Password to authenticate with.
    pub password: Option<String>,

    /// the expiration time in seconds.
    ///
    /// Default to 24 hours (86400)
    /// Up to 30 days (2592000)
    #[serde(default = "default_memcached_cache_expiration")]
    pub expiration: u32,

    #[serde(default)]
    pub key_prefix: String,

    /// The maximum number of connections allowed.
    ///
    /// Default to 10
    pub connection_pool_max_size: Option<u32>,

    pub preprocessor_cache_mode: Option<PreprocessorCacheModeConfig>,

    #[serde(default = "MemcachedCacheConfig::default_order")]
    pub order: u64,
}

impl Default for MemcachedCacheConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl MemcachedCacheConfig {
    fn default_order() -> u64 {
        TieredCacheOrder::Memcached as u64
    }
}

/// redis has no default TTL - all caches live forever
///
/// We keep the TTL as 0 here as redis does
///
/// Please change this value freely if we have a better choice.
const DEFAULT_REDIS_CACHE_TTL: u64 = 0;
pub const DEFAULT_REDIS_DB: u32 = 0;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisCacheConfig {
    /// The single-node redis endpoint.
    /// Mutually exclusive with `cluster_endpoints`.
    pub endpoint: Option<String>,

    /// The redis cluster endpoints.
    /// Mutually exclusive with `endpoint`.
    pub cluster_endpoints: Option<String>,

    /// The redis reader endpoint(s).
    pub reader_endpoints: Option<String>,

    /// Username to authenticate with.
    pub username: Option<String>,

    /// Password to authenticate with.
    pub password: Option<String>,

    /// The redis URL.
    /// Deprecated in favor of `endpoint`.
    pub url: Option<String>,

    /// the db number to use
    ///
    /// Default to 0
    #[serde(default)]
    pub db: u32,

    /// the ttl (expiration) time in seconds.
    ///
    /// Default to infinity (0)
    #[serde(default, alias = "expiration")]
    pub ttl: u64,

    /// The maximum number of connections allowed.
    ///
    /// Default to 10
    pub connection_pool_max_size: Option<u32>,

    #[serde(default)]
    pub key_prefix: String,

    pub preprocessor_cache_mode: Option<PreprocessorCacheModeConfig>,

    #[serde(default = "RedisCacheConfig::default_order")]
    pub order: u64,
}

impl Default for RedisCacheConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl RedisCacheConfig {
    fn default_order() -> u64 {
        TieredCacheOrder::Redis as u64
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebdavCacheConfig {
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub key_prefix: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub preprocessor_cache_mode: Option<PreprocessorCacheModeConfig>,
    #[serde(default = "WebdavCacheConfig::default_order")]
    pub order: u64,
}

impl Default for WebdavCacheConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl WebdavCacheConfig {
    fn default_order() -> u64 {
        TieredCacheOrder::Webdav as u64
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3CacheConfig {
    #[serde(default)]
    pub bucket: String,
    pub region: Option<String>,
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default)]
    pub no_credentials: bool,
    pub endpoint: Option<String>,
    pub use_ssl: Option<bool>,
    pub server_side_encryption: Option<bool>,
    pub enable_virtual_host_style: Option<bool>,
    pub preprocessor_cache_mode: Option<PreprocessorCacheModeConfig>,
    #[serde(default = "S3CacheConfig::default_order")]
    pub order: u64,
}

impl Default for S3CacheConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl S3CacheConfig {
    fn default_order() -> u64 {
        TieredCacheOrder::S3 as u64
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OSSCacheConfig {
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub key_prefix: String,
    pub endpoint: Option<String>,
    #[serde(default)]
    pub no_credentials: bool,
    pub preprocessor_cache_mode: Option<PreprocessorCacheModeConfig>,
    #[serde(default = "OSSCacheConfig::default_order")]
    pub order: u64,
}

impl Default for OSSCacheConfig {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl OSSCacheConfig {
    fn default_order() -> u64 {
        TieredCacheOrder::Oss as u64
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheType {
    Azure(AzureCacheConfig),
    Disk(DiskCacheConfig),
    GCS(GCSCacheConfig),
    GHA(GHACacheConfig),
    Memcached(MemcachedCacheConfig),
    Redis(RedisCacheConfig),
    S3(S3CacheConfig),
    Webdav(WebdavCacheConfig),
    OSS(OSSCacheConfig),
}

impl CacheType {
    pub fn order(&self) -> u64 {
        match self {
            Self::Azure(cfg) => cfg.order,
            Self::Disk(cfg) => cfg.order,
            Self::GCS(cfg) => cfg.order,
            Self::GHA(cfg) => cfg.order,
            Self::Memcached(cfg) => cfg.order,
            Self::Redis(cfg) => cfg.order,
            Self::S3(cfg) => cfg.order,
            Self::Webdav(cfg) => cfg.order,
            Self::OSS(cfg) => cfg.order,
        }
    }
    pub fn preprocessor_cache_mode(&self) -> Option<PreprocessorCacheModeConfig> {
        match self {
            Self::Azure(cfg) => cfg.preprocessor_cache_mode.clone(),
            Self::Disk(cfg) => Some(cfg.preprocessor_cache_mode.clone()),
            Self::GCS(cfg) => cfg.preprocessor_cache_mode.clone(),
            Self::GHA(cfg) => cfg.preprocessor_cache_mode.clone(),
            Self::Memcached(cfg) => cfg.preprocessor_cache_mode.clone(),
            Self::Redis(cfg) => cfg.preprocessor_cache_mode.clone(),
            Self::S3(cfg) => cfg.preprocessor_cache_mode.clone(),
            Self::Webdav(cfg) => cfg.preprocessor_cache_mode.clone(),
            Self::OSS(cfg) => cfg.preprocessor_cache_mode.clone(),
        }
    }
}

impl PartialOrd for CacheType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CacheType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order().cmp(&other.order())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CacheConfigs {
    pub azure: Option<AzureCacheConfig>,
    pub disk: Option<DiskCacheConfig>,
    pub gcs: Option<GCSCacheConfig>,
    pub gha: Option<GHACacheConfig>,
    pub memcached: Option<MemcachedCacheConfig>,
    pub redis: Option<RedisCacheConfig>,
    pub s3: Option<S3CacheConfig>,
    pub webdav: Option<WebdavCacheConfig>,
    pub oss: Option<OSSCacheConfig>,
}

impl CacheConfigs {
    /// Override self with any existing fields from other
    fn merge(mut self, other: Self) -> Self {
        let CacheConfigs {
            azure,
            disk,
            gcs,
            gha,
            memcached,
            redis,
            s3,
            webdav,
            oss,
        } = other;

        if azure.is_some() {
            self.azure = azure;
        }
        if disk.is_some() {
            self.disk = disk;
        }
        if gcs.is_some() {
            self.gcs = gcs;
        }
        if gha.is_some() {
            self.gha = gha;
        }
        if memcached.is_some() {
            self.memcached = memcached;
        }
        if redis.is_some() {
            self.redis = redis;
        }
        if s3.is_some() {
            self.s3 = s3;
        }
        if webdav.is_some() {
            self.webdav = webdav;
        }

        if oss.is_some() {
            self.oss = oss;
        }

        self
    }
}

// Return a list of caches in the order determined by the user
impl From<CacheConfigs> for Vec<CacheType> {
    fn from(value: CacheConfigs) -> Self {
        [
            value.disk.map(CacheType::Disk),
            value.azure.map(CacheType::Azure),
            value.gcs.map(CacheType::GCS),
            value.gha.map(CacheType::GHA),
            value.memcached.map(CacheType::Memcached),
            value.redis.map(CacheType::Redis),
            value.s3.map(CacheType::S3),
            value.webdav.map(CacheType::Webdav),
            value.oss.map(CacheType::OSS),
        ]
        .into_iter()
        .flatten()
        .sorted()
        .collect::<Vec<_>>()
    }
}

impl From<Vec<CacheType>> for CacheConfigs {
    fn from(configs: Vec<CacheType>) -> Self {
        configs
            .into_iter()
            .fold(Self::default(), |caches, cache| caches.merge(cache.into()))
    }
}

impl From<CacheType> for CacheConfigs {
    fn from(cache: CacheType) -> Self {
        let mut caches = Self::default();
        match cache {
            CacheType::Azure(c) => caches.azure = Some(c),
            CacheType::Disk(c) => caches.disk = Some(c),
            CacheType::GCS(c) => caches.gcs = Some(c),
            CacheType::GHA(c) => caches.gha = Some(c),
            CacheType::Memcached(c) => caches.memcached = Some(c),
            CacheType::Redis(c) => caches.redis = Some(c),
            CacheType::S3(c) => caches.s3 = Some(c),
            CacheType::Webdav(c) => caches.webdav = Some(c),
            CacheType::OSS(c) => caches.oss = Some(c),
        }
        caches
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub enum DistToolchainConfig {
    #[serde(rename = "no_dist")]
    NoDist { compiler_executable: PathBuf },
    #[serde(rename = "path_override")]
    PathOverride {
        compiler_executable: PathBuf,
        archive: PathBuf,
        archive_compiler_executable: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum DistAuth {
    #[serde(rename = "token")]
    Token { token: String },
    #[serde(rename = "oauth2_code_grant_pkce")]
    Oauth2CodeGrantPKCE {
        client_id: String,
        auth_url: String,
        token_url: String,
    },
    #[serde(rename = "oauth2_implicit")]
    Oauth2Implicit { client_id: String, auth_url: String },
}

impl DistAuth {
    fn with_env_or_config(&self) -> Self {
        let mut env_or_cfg_token = env::var("SCCACHE_DIST_AUTH_TOKEN");
        let mut env_or_cfg_client_id = env::var("SCCACHE_DIST_AUTH_CLIENT_ID");
        let mut env_or_cfg_auth_url = env::var("SCCACHE_DIST_AUTH_URL");
        let mut env_or_cfg_token_url = env::var("SCCACHE_DIST_AUTH_TOKEN_URL");

        match self {
            Self::Token { token } => {
                env_or_cfg_token = env_or_cfg_token.or(Ok(token.clone()));
            }
            Self::Oauth2CodeGrantPKCE {
                client_id,
                auth_url,
                token_url,
            } => {
                env_or_cfg_client_id = env_or_cfg_client_id.or(Ok(client_id.clone()));
                env_or_cfg_auth_url = env_or_cfg_auth_url.or(Ok(auth_url.clone()));
                env_or_cfg_token_url = env_or_cfg_token_url.or(Ok(token_url.clone()));
            }
            Self::Oauth2Implicit {
                client_id,
                auth_url,
            } => {
                env_or_cfg_client_id = env_or_cfg_client_id.or(Ok(client_id.clone()));
                env_or_cfg_auth_url = env_or_cfg_auth_url.or(Ok(auth_url.clone()));
            }
        }

        let into_token = || {
            env_or_cfg_token
                .map(|token| Self::Token { token })
                .unwrap_or(self.clone())
        };

        let into_oauth2_code_grant_pkce = || {
            env_or_cfg_client_id
                .iter()
                .zip(env_or_cfg_auth_url.iter())
                .zip(env_or_cfg_token_url.iter())
                .map(
                    |((client_id, auth_url), token_url)| Self::Oauth2CodeGrantPKCE {
                        client_id: client_id.clone(),
                        auth_url: auth_url.clone(),
                        token_url: token_url.clone(),
                    },
                )
                .next_back()
                .unwrap_or(self.clone())
        };

        let into_oauth2_implicit = || {
            env_or_cfg_client_id
                .iter()
                .zip(env_or_cfg_auth_url.iter())
                .map(|(client_id, auth_url)| Self::Oauth2Implicit {
                    client_id: client_id.clone(),
                    auth_url: auth_url.clone(),
                })
                .next_back()
                .unwrap_or(self.clone())
        };

        if let Ok(auth_type) = env::var("SCCACHE_DIST_AUTH_TYPE") {
            match auth_type.as_ref() {
                "token" => into_token(),
                "oauth2_code_grant_pkce" => into_oauth2_code_grant_pkce(),
                "oauth2_implicit" => into_oauth2_implicit(),
                _ => {
                    warn!("Unknown SCCACHE_DIST_AUTH_TYPE {auth_type:?}");
                    self.clone()
                }
            }
        } else {
            match self {
                Self::Token { .. } => into_token(),
                Self::Oauth2CodeGrantPKCE { .. } => into_oauth2_code_grant_pkce(),
                Self::Oauth2Implicit { .. } => into_oauth2_implicit(),
            }
        }
    }
}

// Convert a type = "mozilla" immediately into an actual oauth configuration
// https://github.com/serde-rs/serde/issues/595 could help if implemented
impl<'a> Deserialize<'a> for DistAuth {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        #[serde(tag = "type")]
        pub enum Helper {
            #[serde(rename = "token")]
            Token { token: String },
            #[serde(rename = "oauth2_code_grant_pkce")]
            Oauth2CodeGrantPKCE {
                client_id: String,
                auth_url: String,
                token_url: String,
            },
            #[serde(rename = "oauth2_implicit")]
            Oauth2Implicit { client_id: String, auth_url: String },
        }

        let helper: Helper = Deserialize::deserialize(deserializer)?;

        Ok(match helper {
            Helper::Token { token } => DistAuth::Token { token },
            Helper::Oauth2CodeGrantPKCE {
                client_id,
                auth_url,
                token_url,
            } => DistAuth::Oauth2CodeGrantPKCE {
                client_id,
                auth_url,
                token_url,
            },
            Helper::Oauth2Implicit {
                client_id,
                auth_url,
            } => DistAuth::Oauth2Implicit {
                client_id,
                auth_url,
            },
        })
    }
}

impl Default for DistAuth {
    fn default() -> Self {
        DistAuth::Token {
            token: INSECURE_DIST_CLIENT_TOKEN.to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct DistNetworking {
    pub connect_timeout: u32,
    pub request_timeout: u32,
    pub connection_pool: bool,
    pub keepalive: DistNetworkingKeepalive,
}

impl DistNetworking {
    pub fn with_env_or_config(&self) -> Self {
        Self {
            connect_timeout: number_from_env_var("SCCACHE_DIST_CONNECT_TIMEOUT")
                .map(|val| val.unwrap_or(self.connect_timeout))
                .unwrap_or(self.connect_timeout),
            request_timeout: number_from_env_var("SCCACHE_DIST_REQUEST_TIMEOUT")
                .map(|val| val.unwrap_or(self.request_timeout))
                .unwrap_or(self.request_timeout),
            connection_pool: bool_from_env_var("SCCACHE_DIST_CONNECTION_POOL")
                .map(|val| val.unwrap_or(self.connection_pool))
                .unwrap_or(self.connection_pool),
            keepalive: self.keepalive.with_env_or_config(),
        }
    }
}

impl Default for DistNetworking {
    fn default() -> Self {
        Self {
            // Default timeout for connections to an sccache-dist server
            connect_timeout: 30,
            // Default timeout for compile requests to an sccache-dist server.
            // Users should set their load balancer's idle timeout to match or
            // exceed this value.
            request_timeout: 600,
            // Default to using reqwest's HTTP/2 connection pool
            connection_pool: true,
            keepalive: Default::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct DistNetworkingKeepalive {
    pub enabled: bool,
    pub timeout: u64,
    pub interval: u64,
}

impl DistNetworkingKeepalive {
    pub fn with_env_or_config(&self) -> Self {
        Self {
            enabled: bool_from_env_var("SCCACHE_DIST_KEEPALIVE_ENABLED")
                .map(|val| val.unwrap_or(self.enabled))
                .unwrap_or(self.enabled),
            interval: number_from_env_var("SCCACHE_DIST_KEEPALIVE_INTERVAL")
                .map(|val| val.unwrap_or(self.interval))
                .unwrap_or(self.interval),
            timeout: number_from_env_var("SCCACHE_DIST_KEEPALIVE_TIMEOUT")
                .map(|val| val.unwrap_or(self.timeout))
                .unwrap_or(self.timeout),
        }
    }
}

impl Default for DistNetworkingKeepalive {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: 20,
            timeout: 600,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct DistConfig {
    pub auth: DistAuth,
    pub cache_dir: PathBuf,
    pub fallback_to_local_compile: bool,
    // f64 to allow `max_retries = inf`, i.e. never fallback to local compile
    pub max_retries: f64,
    pub net: DistNetworking,
    pub rewrite_includes_only: bool,
    #[cfg(any(feature = "dist-client", feature = "dist-server"))]
    pub scheduler_url: Option<HTTPUrl>,
    #[cfg(not(any(feature = "dist-client", feature = "dist-server")))]
    pub scheduler_url: Option<String>,
    pub toolchains: Vec<DistToolchainConfig>,
    #[serde(deserialize_with = "deserialize_size_from_str")]
    pub toolchain_cache_size: u64,
}

impl DistConfig {
    pub fn with_env_or_config(&self) -> Self {
        Self {
            auth: self.auth.with_env_or_config(),
            net: self.net.with_env_or_config(),
            #[cfg(any(feature = "dist-client", feature = "dist-server"))]
            scheduler_url: env::var("SCCACHE_DIST_SCHEDULER_URL")
                .map_err(Into::into)
                .and_then(parse_http_url)
                .map(HTTPUrl::from_url)
                .ok()
                .or(self.scheduler_url.clone()),
            fallback_to_local_compile: bool_from_env_var("SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE")
                .map(|val| val.unwrap_or(self.fallback_to_local_compile))
                .unwrap_or(self.fallback_to_local_compile),
            max_retries: number_from_env_var("SCCACHE_DIST_MAX_RETRIES")
                .map(|val| val.unwrap_or(self.max_retries))
                .unwrap_or(self.max_retries),
            rewrite_includes_only: bool_from_env_var("SCCACHE_DIST_REWRITE_INCLUDES_ONLY")
                .map(|val| val.unwrap_or(self.rewrite_includes_only))
                .unwrap_or(self.rewrite_includes_only),
            ..self.clone()
        }
    }
}

impl Default for DistConfig {
    fn default() -> Self {
        Self {
            auth: Default::default(),
            cache_dir: default_dist_cache_dir(),
            fallback_to_local_compile: true,
            max_retries: 0f64,
            net: Default::default(),
            rewrite_includes_only: false,
            scheduler_url: Default::default(),
            toolchains: Default::default(),
            toolchain_cache_size: default_toolchain_cache_size(),
        }
    }
}

// TODO: fields only pub for tests
#[derive(Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub cache: CacheConfigs,
    pub dist: DistConfig,
    pub server_startup_timeout_ms: Option<u64>,
}

// If the file doesn't exist or we can't read it, log the issue and proceed. If the
// config exists but doesn't parse then something is wrong - return an error.
pub fn try_read_config_file<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    debug!("Attempting to read config file at {:?}", path);
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            debug!("Couldn't open config file: {}", e);
            return Ok(None);
        }
    };

    let mut string = String::new();
    match file.read_to_string(&mut string) {
        Ok(_) => (),
        Err(e) => {
            warn!("Failed to read config file: {}", e);
            return Ok(None);
        }
    }

    let res = if path.extension().is_some_and(|e| e == "json") {
        serde_json::from_str(&string)
            .with_context(|| format!("Failed to load json config file from {}", path.display()))?
    } else {
        toml::from_str(&string)
            .with_context(|| format!("Failed to load toml config file from {}", path.display()))?
    };

    Ok(Some(res))
}

#[derive(Debug)]
pub struct EnvConfig {
    cache: CacheConfigs,
}

fn key_prefix_from_env_var(env_var_name: &str) -> Option<String> {
    env::var(env_var_name)
        .ok()
        .as_ref()
        .map(|s| s.trim_end_matches('/'))
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned())
}

pub fn number_from_env_var<A: std::str::FromStr>(env_var_name: &str) -> Option<Result<A>>
where
    <A as FromStr>::Err: std::fmt::Debug,
{
    let value = env::var(env_var_name).ok()?;

    value
        .parse::<A>()
        .map_err(|err| anyhow!("{env_var_name} value is invalid: {err:?}"))
        .into()
}

pub fn bool_from_env_var(env_var_name: &str) -> Result<Option<bool>> {
    env::var(env_var_name)
        .ok()
        .map(|value| match value.to_lowercase().as_str() {
            "true" | "on" | "1" => Ok(true),
            "false" | "off" | "0" => Ok(false),
            _ => bail!(
                "{} must be 'true', 'on', '1', 'false', 'off' or '0'.",
                env_var_name
            ),
        })
        .transpose()
}

fn config_from_env<'a>(envvar_prefix: impl Into<Option<&'a str>>) -> Result<EnvConfig> {
    let envvar_prefix = envvar_prefix.into().unwrap_or("SCCACHE_").to_owned();
    let envvar = |key: &str| envvar_prefix.clone() + key;

    // ======= AWS =======
    let s3 = if let Some(bucket) = env::var(envvar("BUCKET"))
        .ok()
        .and_then(|bucket| (!bucket.is_empty()).then_some(bucket))
    {
        let region = env::var(envvar("REGION")).ok();
        let no_credentials = bool_from_env_var(&envvar("S3_NO_CREDENTIALS"))?.unwrap_or(false);
        let use_ssl = bool_from_env_var(&envvar("S3_USE_SSL"))?;
        let server_side_encryption = bool_from_env_var(&envvar("S3_SERVER_SIDE_ENCRYPTION"))?;
        let endpoint = env::var(envvar("ENDPOINT")).ok();
        let key_prefix = key_prefix_from_env_var(&envvar("S3_KEY_PREFIX")).unwrap_or_default();
        let enable_virtual_host_style = bool_from_env_var(&envvar("S3_ENABLE_VIRTUAL_HOST_STYLE"))?;

        Some(S3CacheConfig {
            bucket,
            region,
            no_credentials,
            key_prefix,
            endpoint,
            use_ssl,
            server_side_encryption,
            enable_virtual_host_style,
            preprocessor_cache_mode: Some(PreprocessorCacheModeConfig {
                use_preprocessor_cache_mode: bool_from_env_var(&envvar(
                    "S3_USE_PREPROCESSOR_CACHE_MODE",
                ))?
                .unwrap_or_default(),
                key_prefix: key_prefix_from_env_var(&envvar("S3_PREPROCESSOR_CACHE_KEY_PREFIX"))
                    .unwrap_or_else(PreprocessorCacheModeConfig::default_key_prefix),
            }),
            order: number_from_env_var(&envvar("S3_CACHE_ORDER"))
                .unwrap_or(Ok(S3CacheConfig::default_order()))
                .unwrap_or(S3CacheConfig::default_order()),
        })
    } else {
        None
    };

    if s3.as_ref().map(|s3| s3.no_credentials).unwrap_or_default()
        && (env::var_os("AWS_ACCESS_KEY_ID").is_some()
            || env::var_os("AWS_SECRET_ACCESS_KEY").is_some())
    {
        bail!(
            "If setting S3 credentials, {} must not be set.",
            envvar("S3_NO_CREDENTIALS")
        );
    }

    // ======= redis =======
    let redis = match (
        env::var(envvar("REDIS")).ok(),
        env::var(envvar("REDIS_ENDPOINT")).ok(),
        env::var(envvar("REDIS_CLUSTER_ENDPOINTS")).ok(),
        env::var(envvar("REDIS_READER_ENDPOINTS")).ok(),
    ) {
        (None, None, None, None) => None,
        (url, endpoint, cluster_endpoints, reader_endpoints) => {
            let db = number_from_env_var(&envvar("REDIS_DB"))
                .transpose()?
                .unwrap_or(DEFAULT_REDIS_DB);

            let username = env::var(envvar("REDIS_USERNAME")).ok();
            let password = env::var(envvar("REDIS_PASSWORD")).ok();

            let ttl = number_from_env_var(&envvar("REDIS_EXPIRATION"))
                .or_else(|| number_from_env_var(&envvar("REDIS_TTL")))
                .transpose()?
                .unwrap_or(DEFAULT_REDIS_CACHE_TTL);

            let connection_pool_max_size =
                number_from_env_var(&envvar("REDIS_CONNECTION_POOL_SIZE"))
                    .transpose()?
                    .unwrap_or(10);

            let key_prefix =
                key_prefix_from_env_var(&envvar("REDIS_KEY_PREFIX")).unwrap_or_default();

            Some(RedisCacheConfig {
                url,
                endpoint,
                cluster_endpoints,
                reader_endpoints,
                username,
                password,
                db,
                ttl,
                key_prefix,
                connection_pool_max_size: Some(connection_pool_max_size),
                preprocessor_cache_mode: Some(PreprocessorCacheModeConfig {
                    use_preprocessor_cache_mode: bool_from_env_var(&envvar(
                        "REDIS_USE_PREPROCESSOR_CACHE_MODE",
                    ))?
                    .unwrap_or_default(),
                    key_prefix: key_prefix_from_env_var(&envvar(
                        "REDIS_PREPROCESSOR_CACHE_KEY_PREFIX",
                    ))
                    .unwrap_or_else(PreprocessorCacheModeConfig::default_key_prefix),
                }),
                order: number_from_env_var(&envvar("REDIS_CACHE_ORDER"))
                    .unwrap_or(Ok(RedisCacheConfig::default_order()))
                    .unwrap_or(RedisCacheConfig::default_order()),
            })
        }
    };

    if env::var_os(envvar("REDIS_EXPIRATION")).is_some()
        && env::var_os(envvar("REDIS_TTL")).is_some()
    {
        bail!(
            "You mustn't set both {} and {}. Use only one.",
            envvar("REDIS_EXPIRATION"),
            envvar("REDIS_TTL")
        );
    }

    // ======= memcached =======
    let memcached = if let Ok(url) =
        env::var(envvar("MEMCACHED")).or_else(|_| env::var(envvar("MEMCACHED_ENDPOINT")))
    {
        let username = env::var(envvar("MEMCACHED_USERNAME")).ok();
        let password = env::var(envvar("MEMCACHED_PASSWORD")).ok();

        let expiration = number_from_env_var(&envvar("MEMCACHED_EXPIRATION"))
            .transpose()?
            .unwrap_or(DEFAULT_MEMCACHED_CACHE_EXPIRATION);

        let connection_pool_max_size =
            number_from_env_var(&envvar("MEMCACHED_CONNECTION_POOL_SIZE"))
                .transpose()?
                .unwrap_or(10);

        let key_prefix =
            key_prefix_from_env_var(&envvar("MEMCACHED_KEY_PREFIX")).unwrap_or_default();

        Some(MemcachedCacheConfig {
            url,
            username,
            password,
            expiration,
            key_prefix,
            connection_pool_max_size: Some(connection_pool_max_size),
            preprocessor_cache_mode: Some(PreprocessorCacheModeConfig {
                use_preprocessor_cache_mode: bool_from_env_var(&envvar(
                    "MEMCACHED_USE_PREPROCESSOR_CACHE_MODE",
                ))?
                .unwrap_or_default(),
                key_prefix: key_prefix_from_env_var(&envvar(
                    "MEMCACHED_PREPROCESSOR_CACHE_KEY_PREFIX",
                ))
                .unwrap_or_else(PreprocessorCacheModeConfig::default_key_prefix),
            }),
            order: number_from_env_var(&envvar("MEMCACHED_CACHE_ORDER"))
                .unwrap_or(Ok(MemcachedCacheConfig::default_order()))
                .unwrap_or(MemcachedCacheConfig::default_order()),
        })
    } else {
        None
    };

    if env::var_os(envvar("MEMCACHED")).is_some()
        && env::var_os(envvar("MEMCACHED_ENDPOINT")).is_some()
    {
        bail!(
            "You mustn't set both {} and {}. Please, use only {}.",
            envvar("MEMCACHED"),
            envvar("MEMCACHED_ENDPOINT"),
            envvar("MEMCACHED_ENDPOINT")
        );
    }

    // ======= GCP/GCS =======
    if (env::var(envvar("GCS_CREDENTIALS_URL")).is_ok()
        || env::var(envvar("GCS_OAUTH_URL")).is_ok()
        || env::var(envvar("GCS_KEY_PATH")).is_ok())
        && env::var(envvar("GCS_BUCKET")).is_err()
    {
        bail!(
            "If setting GCS credentials, {} and an auth mechanism need to be set.",
            envvar("GCS_BUCKET")
        );
    }

    let gcs = env::var(envvar("GCS_BUCKET")).ok().map(|bucket| {
        let key_prefix = key_prefix_from_env_var(&envvar("GCS_KEY_PREFIX")).unwrap_or_default();

        if env::var(envvar("GCS_OAUTH_URL")).is_ok() {
            eprintln!("{} has been deprecated", envvar("GCS_OAUTH_URL"));
            eprintln!("if you intend to use vm metadata for auth, please set correct service account instead");
        }

        let credential_url = env::var(envvar("GCS_CREDENTIALS_URL")).ok();

        let cred_path = env::var(envvar("GCS_KEY_PATH")).ok();
        let service_account = env::var(envvar("GCS_SERVICE_ACCOUNT")).ok();

        let rw_mode = match env::var(envvar("GCS_RW_MODE")).as_ref().map(String::as_str) {
            Ok("READ_ONLY") => CacheModeConfig::ReadOnly,
            Ok("READ_WRITE") => CacheModeConfig::ReadWrite,
            // TODO: unsure if these should warn during the configuration loading
            // or at the time when they're actually used to connect to GCS
            Ok(_) => {
                warn!("Invalid {} -- defaulting to READ_ONLY.",
                envvar("GCS_RW_MODE"));
                CacheModeConfig::ReadOnly
            }
            _ => {
                warn!("No {} specified -- defaulting to READ_ONLY.", envvar("GCS_RW_MODE"));
                CacheModeConfig::ReadOnly
            }
        };

        GCSCacheConfig {
            bucket,
            key_prefix,
            cred_path,
            service_account,
            rw_mode,
            credential_url,
            preprocessor_cache_mode: Some(PreprocessorCacheModeConfig {
                use_preprocessor_cache_mode: bool_from_env_var(&envvar(
                    "GCS_USE_PREPROCESSOR_CACHE_MODE",
                ))
                .unwrap_or(None)
                .unwrap_or_default(),
                key_prefix: key_prefix_from_env_var(&envvar("GCS_PREPROCESSOR_CACHE_KEY_PREFIX")) //
                    .unwrap_or_else(PreprocessorCacheModeConfig::default_key_prefix),
            }),
            order: number_from_env_var(&envvar("GCS_CACHE_ORDER"))
                .unwrap_or(Ok(GCSCacheConfig::default_order()))
                .unwrap_or(GCSCacheConfig::default_order()),
        }
    });

    // ======= GHA =======
    let gha = if let Ok(version) = env::var(envvar("GHA_VERSION")) {
        // If SCCACHE_GHA_VERSION has been set, we don't need to check
        // SCCACHE_GHA_ENABLED's value anymore.
        Some(GHACacheConfig {
            enabled: true,
            version,
            key_prefix: key_prefix_from_env_var(&envvar("GHA_CACHE_KEY_PREFIX"))
                .unwrap_or_default(),
            preprocessor_cache_mode: Some(PreprocessorCacheModeConfig {
                use_preprocessor_cache_mode: bool_from_env_var(&envvar(
                    "GHA_USE_PREPROCESSOR_CACHE_MODE",
                ))?
                .unwrap_or_default(),
                key_prefix: key_prefix_from_env_var(&envvar("GHA_PREPROCESSOR_CACHE_KEY_PREFIX"))
                    .unwrap_or_else(PreprocessorCacheModeConfig::default_key_prefix),
            }),
            order: number_from_env_var(&envvar("GHA_CACHE_ORDER"))
                .unwrap_or(Ok(GHACacheConfig::default_order()))
                .unwrap_or(GHACacheConfig::default_order()),
        })
    } else if bool_from_env_var(&envvar("GHA_ENABLED"))?.unwrap_or(false) {
        // If only SCCACHE_GHA_ENABLED has been set to the true value, enable with
        // default version.
        Some(GHACacheConfig {
            enabled: true,
            version: "".to_string(),
            key_prefix: key_prefix_from_env_var(&envvar("GHA_CACHE_KEY_PREFIX"))
                .unwrap_or_default(),
            preprocessor_cache_mode: Some(PreprocessorCacheModeConfig {
                use_preprocessor_cache_mode: bool_from_env_var(&envvar(
                    "GHA_USE_PREPROCESSOR_CACHE_MODE",
                ))?
                .unwrap_or_default(),
                key_prefix: key_prefix_from_env_var(&envvar("GHA_PREPROCESSOR_CACHE_KEY_PREFIX"))
                    .unwrap_or_else(PreprocessorCacheModeConfig::default_key_prefix),
            }),
            order: number_from_env_var(&envvar("GHA_CACHE_ORDER"))
                .unwrap_or(Ok(GHACacheConfig::default_order()))
                .unwrap_or(GHACacheConfig::default_order()),
        })
    } else {
        None
    };

    // ======= Azure =======
    let azure = if let (Ok(connection_string), Ok(container)) = (
        env::var(envvar("AZURE_CONNECTION_STRING")),
        env::var(envvar("AZURE_BLOB_CONTAINER")),
    ) {
        let key_prefix = key_prefix_from_env_var(&envvar("AZURE_KEY_PREFIX")).unwrap_or_default();
        Some(AzureCacheConfig {
            connection_string,
            container,
            key_prefix,
            preprocessor_cache_mode: Some(PreprocessorCacheModeConfig {
                use_preprocessor_cache_mode: bool_from_env_var(&envvar(
                    "AZURE_USE_PREPROCESSOR_CACHE_MODE",
                ))?
                .unwrap_or_default(),
                key_prefix: key_prefix_from_env_var(&envvar("AZURE_PREPROCESSOR_CACHE_KEY_PREFIX"))
                    .unwrap_or_else(PreprocessorCacheModeConfig::default_key_prefix),
            }),
            order: number_from_env_var(&envvar("AZURE_CACHE_ORDER"))
                .unwrap_or(Ok(AzureCacheConfig::default_order()))
                .unwrap_or(AzureCacheConfig::default_order()),
        })
    } else {
        None
    };

    // ======= WebDAV =======
    let webdav = if let Ok(endpoint) = env::var(envvar("WEBDAV_ENDPOINT")) {
        let key_prefix = key_prefix_from_env_var(&envvar("WEBDAV_KEY_PREFIX")).unwrap_or_default();
        let username = env::var(envvar("WEBDAV_USERNAME")).ok();
        let password = env::var(envvar("WEBDAV_PASSWORD")).ok();
        let token = env::var(envvar("WEBDAV_TOKEN")).ok();

        Some(WebdavCacheConfig {
            endpoint,
            key_prefix,
            username,
            password,
            token,
            preprocessor_cache_mode: Some(PreprocessorCacheModeConfig {
                use_preprocessor_cache_mode: bool_from_env_var(&envvar(
                    "WEBDAV_USE_PREPROCESSOR_CACHE_MODE",
                ))?
                .unwrap_or_default(),
                key_prefix: key_prefix_from_env_var(&envvar(
                    "WEBDAV_PREPROCESSOR_CACHE_KEY_PREFIX",
                ))
                .unwrap_or_else(PreprocessorCacheModeConfig::default_key_prefix),
            }),
            order: number_from_env_var(&envvar("WEBDAV_CACHE_ORDER"))
                .unwrap_or(Ok(WebdavCacheConfig::default_order()))
                .unwrap_or(WebdavCacheConfig::default_order()),
        })
    } else {
        None
    };

    // ======= OSS =======
    let oss = if let Ok(bucket) = env::var(envvar("OSS_BUCKET")) {
        let endpoint = env::var(envvar("OSS_ENDPOINT")).ok();
        let key_prefix = key_prefix_from_env_var(&envvar("OSS_KEY_PREFIX")).unwrap_or_default();

        let no_credentials = bool_from_env_var(&envvar("OSS_NO_CREDENTIALS"))?.unwrap_or(false);

        Some(OSSCacheConfig {
            bucket,
            endpoint,
            key_prefix,
            no_credentials,
            preprocessor_cache_mode: Some(PreprocessorCacheModeConfig {
                use_preprocessor_cache_mode: bool_from_env_var(&envvar(
                    "OSS_USE_PREPROCESSOR_CACHE_MODE",
                ))?
                .unwrap_or_default(),
                key_prefix: key_prefix_from_env_var(&envvar("OSS_PREPROCESSOR_CACHE_KEY_PREFIX"))
                    .unwrap_or_else(PreprocessorCacheModeConfig::default_key_prefix),
            }),
            order: number_from_env_var(&envvar("OSS_CACHE_ORDER"))
                .unwrap_or(Ok(OSSCacheConfig::default_order()))
                .unwrap_or(OSSCacheConfig::default_order()),
        })
    } else {
        None
    };

    if oss
        .as_ref()
        .map(|oss| oss.no_credentials)
        .unwrap_or_default()
        && (env::var_os("ALIBABA_CLOUD_ACCESS_KEY_ID").is_some()
            || env::var_os("ALIBABA_CLOUD_ACCESS_KEY_SECRET").is_some())
    {
        bail!(
            "If setting OSS credentials, {} must not be set.",
            envvar("OSS_NO_CREDENTIALS")
        );
    }

    // ======= Local =======
    let disk_dir = env::var_os(envvar("DIR"))
        .or(env::var_os(envvar("DISK_CACHE_PATH")))
        .map(PathBuf::from);
    let disk_sz = env::var(envvar("CACHE_SIZE"))
        .or(env::var(envvar("DISK_CACHE_SIZE")))
        .ok()
        .and_then(|v| parse_size(&v));
    let disk_order = number_from_env_var(&envvar("DISK_CACHE_ORDER")).and_then(|o| o.ok());

    let mut preprocessor_cache_mode = PreprocessorCacheModeConfig::activated();
    let preprocessor_mode_overridden = if let Some(value) = bool_from_env_var(&envvar("DIRECT"))? {
        preprocessor_cache_mode.use_preprocessor_cache_mode = value;
        true
    } else if let Some(value) = bool_from_env_var(&envvar("DISK_USE_PREPROCESSOR_CACHE_MODE"))? {
        preprocessor_cache_mode.use_preprocessor_cache_mode = value;
        true
    } else {
        false
    };

    if preprocessor_cache_mode.use_preprocessor_cache_mode
        && env::var(envvar("DISK_PREPROCESSOR_CACHE_KEY_PREFIX")).is_ok()
    {
        preprocessor_cache_mode.key_prefix =
            key_prefix_from_env_var(&envvar("DISK_PREPROCESSOR_CACHE_KEY_PREFIX"))
                .unwrap_or(preprocessor_cache_mode.key_prefix);
    }

    let (disk_rw_mode, disk_rw_mode_overridden) = match env::var(envvar("LOCAL_RW_MODE"))
        .as_ref()
        .map(String::as_str)
    {
        Ok("READ_ONLY") => (CacheModeConfig::ReadOnly, true),
        Ok("READ_WRITE") => (CacheModeConfig::ReadWrite, true),
        Ok(_) => {
            warn!(
                "Invalid {} -- defaulting to READ_WRITE.",
                envvar("LOCAL_RW_MODE")
            );
            (CacheModeConfig::ReadWrite, false)
        }
        _ => (CacheModeConfig::ReadWrite, false),
    };

    let any_overridden = disk_dir.is_some()
        || disk_sz.is_some()
        || disk_order.is_some()
        || preprocessor_mode_overridden
        || disk_rw_mode_overridden;
    let disk = if any_overridden {
        Some(DiskCacheConfig {
            dir: disk_dir.unwrap_or_else(default_disk_cache_dir),
            size: disk_sz.unwrap_or_else(default_disk_cache_size),
            preprocessor_cache_mode,
            rw_mode: disk_rw_mode,
            order: disk_order.unwrap_or(DiskCacheConfig::default_order()),
        })
    } else {
        None
    };

    let cache = CacheConfigs {
        azure,
        disk,
        gcs,
        gha,
        memcached,
        redis,
        s3,
        webdav,
        oss,
    };

    Ok(EnvConfig { cache })
}

// The directories crate changed the location of `config_dir` on macos in version 3,
// so we also check the config in `preference_dir` (new in that version), which
// corresponds to the old location, for compatibility with older setups.
fn config_file(env_var: &str, leaf: &str) -> PathBuf {
    if let Some(env_value) = env::var_os(env_var) {
        return env_value.into();
    }
    let dirs =
        ProjectDirs::from("", ORGANIZATION, APP_NAME).expect("Unable to get config directory");
    // If the new location exists, use that.
    let path = dirs.config_dir().join(leaf);
    if path.exists() {
        return path;
    }
    // If the old location exists, use that.
    let path = dirs.preference_dir().join(leaf);
    if path.exists() {
        return path;
    }
    // Otherwise, use the new location.
    dirs.config_dir().join(leaf)
}

#[derive(Debug, Default, PartialEq)]
pub struct Config {
    pub caches: Vec<CacheType>,
    pub dist: DistConfig,
    pub server_startup_timeout: Option<Duration>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let env_conf = config_from_env("SCCACHE_")?;

        let file_conf_path = config_file("SCCACHE_CONF", "config");
        let file_conf = try_read_config_file(&file_conf_path)
            .context("Failed to load config file")?
            .unwrap_or_default();

        let mut conf = Self::from_env_and_file_configs(env_conf, file_conf);

        conf.dist = conf.dist.with_env_or_config();

        Ok(conf)
    }

    fn from_env_and_file_configs(env_conf: EnvConfig, file_conf: FileConfig) -> Self {
        let FileConfig {
            dist,
            server_startup_timeout_ms,
            ..
        } = file_conf;

        let server_startup_timeout = server_startup_timeout_ms.map(Duration::from_millis);

        let caches = {
            CacheConfigs::default()
                .merge(file_conf.cache)
                .merge(env_conf.cache)
                .into()
        };

        Self {
            caches,
            dist,
            server_startup_timeout,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct CachedDistConfig {
    pub auth_tokens: HashMap<String, String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct CachedFileConfig {
    pub dist: CachedDistConfig,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CachedConfig(());

impl CachedConfig {
    pub fn load() -> Result<Self> {
        let mut cached_file_config = CACHED_CONFIG.lock().unwrap();

        if cached_file_config.is_none() {
            let cfg = Self::load_file_config().context("Unable to initialise cached config")?;
            *cached_file_config = Some(cfg);
        }
        Ok(CachedConfig(()))
    }
    pub fn reload() -> Result<Self> {
        {
            let mut cached_file_config = CACHED_CONFIG.lock().unwrap();
            *cached_file_config = None;
        };
        Self::load()
    }
    pub fn with<F: FnOnce(&CachedFileConfig) -> T, T>(&self, f: F) -> T {
        let cached_file_config = CACHED_CONFIG.lock().unwrap();
        let cached_file_config = cached_file_config.as_ref().unwrap();

        f(cached_file_config)
    }
    pub fn with_mut<F: FnOnce(&mut CachedFileConfig)>(&self, f: F) -> Result<()> {
        let mut cached_file_config = CACHED_CONFIG.lock().unwrap();
        let cached_file_config = cached_file_config.as_mut().unwrap();

        let mut new_config = cached_file_config.clone();
        f(&mut new_config);
        Self::save_file_config(&new_config)?;
        *cached_file_config = new_config;
        Ok(())
    }

    fn file_config_path() -> PathBuf {
        config_file("SCCACHE_CACHED_CONF", "cached-config")
    }
    fn load_file_config() -> Result<CachedFileConfig> {
        let file_conf_path = &*CACHED_CONFIG_PATH;

        if !file_conf_path.exists() {
            let file_conf_dir = file_conf_path
                .parent()
                .expect("Cached conf file has no parent directory");
            if !file_conf_dir.is_dir() {
                fs::create_dir_all(file_conf_dir)
                    .context("Failed to create dir to hold cached config")?;
            }
            Self::save_file_config(&Default::default()).with_context(|| {
                format!(
                    "Unable to create cached config file at {}",
                    file_conf_path.display()
                )
            })?;
        }
        try_read_config_file(file_conf_path)
            .context("Failed to load cached config file")?
            .with_context(|| format!("Failed to load from {}", file_conf_path.display()))
    }
    fn save_file_config(c: &CachedFileConfig) -> Result<()> {
        let file_conf_path = &*CACHED_CONFIG_PATH;
        let mut file = File::create(file_conf_path).context("Could not open config for writing")?;
        file.write_all(toml::to_string(c).unwrap().as_bytes())
            .map_err(Into::into)
    }
}

#[cfg(feature = "dist-server")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MessageBroker {
    #[serde(rename = "amqp")]
    AMQP(String),
    #[serde(rename = "redis")]
    Redis(String),
}

#[cfg(feature = "dist-server")]
impl MessageBroker {
    fn from_env() -> Option<MessageBroker> {
        None.or(std::env::var("AMQP_ADDR").ok().map(MessageBroker::AMQP))
            .or(std::env::var("REDIS_ADDR").ok().map(MessageBroker::Redis))
    }
}

#[cfg(feature = "dist-server")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DogStatsDAggregationMode {
    #[serde(rename = "aggressive")]
    Aggressive,
    #[serde(rename = "conservative")]
    Conservative,
}

#[cfg(feature = "dist-server")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DogStatsDMetricsConfig {
    pub remote_addr: String,
    // Write timeout (in milliseconds) for forwarding metrics to dogstatsd
    pub write_timeout: Option<u64>,
    // Maximum payload length for forwarding metrics
    pub maximum_payload_length: Option<usize>,
    // The aggregation mode for the exporter
    pub aggregation_mode: Option<DogStatsDAggregationMode>,
    // The flush interval of the aggregator
    pub flush_interval: Option<u64>,
    // Whether or not to enable telemetry for the exporter
    pub telemetry: Option<bool>,
    // Whether or not to enable histogram sampling
    pub histogram_sampling: Option<bool>,
    // The reservoir size for histogram sampling
    pub histogram_reservoir_size: Option<usize>,
    // Whether or not to send histograms as distributions
    pub histograms_as_distributions: Option<bool>,
}

#[cfg(feature = "dist-server")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
#[serde(deny_unknown_fields)]
pub enum PrometheusMetricsConfig {
    #[serde(rename = "bind")]
    ListenAddr { addr: Option<std::net::SocketAddr> },
    #[serde(rename = "path")]
    ListenPath { path: Option<String> },
    #[serde(rename = "push")]
    PushGateway {
        endpoint: String,
        // Interval (in milliseconds) to push metrics to prometheus
        interval: Option<u64>,
        username: Option<String>,
        password: Option<String>,
        http_method: Option<String>,
    },
}

#[cfg(feature = "dist-server")]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetricsConfigs {
    pub dogstatsd: Option<DogStatsDMetricsConfig>,
    pub prometheus: Option<PrometheusMetricsConfig>,
}

#[cfg(feature = "dist-server")]
pub mod scheduler {
    use std::env;
    use std::path::PathBuf;
    use std::{net::SocketAddr, str::FromStr};

    use crate::config::DistNetworkingKeepalive;
    use crate::errors::*;

    use serde::{Deserialize, Serialize};

    use super::{
        CacheConfigs, CacheModeConfig, CacheType, DiskCacheConfig, MessageBroker, MetricsConfigs,
        config_from_env, default_disk_cache_dir, default_disk_cache_size, number_from_env_var,
        try_read_config_file,
    };

    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "type")]
    #[serde(deny_unknown_fields)]
    pub enum ClientAuth {
        #[default]
        #[serde(rename = "DANGEROUSLY_INSECURE")]
        Insecure,
        #[serde(rename = "token")]
        Token { token: String },
        #[serde(rename = "jwt_validate")]
        JwtValidate {
            audience: String,
            issuer: String,
            jwks_url: String,
            claims: Option<Vec<String>>,
        },
        #[serde(rename = "proxy_token")]
        ProxyToken {
            url: String,
            cache_secs: Option<u64>,
            decode: Option<ProxyTokenDecodeConfig>,
            rate_limit_on_error_count: Option<usize>,
            rate_limit_on_error_window_size_secs: Option<u64>,
        },
    }

    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "type")]
    #[serde(deny_unknown_fields)]
    pub enum ProxyTokenDecodeConfig {
        #[default]
        None,
        #[serde(rename = "jwt")]
        JwtDecoder {
            audience: String,
            issuer: String,
            jwks_url: String,
            claims: Option<Vec<String>>,
        },
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(default)]
    #[serde(deny_unknown_fields)]
    pub struct FileConfig {
        pub client_auth: ClientAuth,
        #[serde(default = "Config::default_heartbeat_interval_ms")]
        pub heartbeat_interval_ms: u64,
        #[serde(default = "Config::default_job_time_limit")]
        pub job_time_limit: u32,
        #[serde(default = "Config::default_jobs_storage")]
        pub jobs: CacheConfigs,
        pub keepalive: DistNetworkingKeepalive,
        #[serde(default = "Config::default_max_body_size")]
        pub max_body_size: usize,
        pub max_concurrent_streams: Option<u32>,
        pub message_broker: Option<MessageBroker>,
        pub metrics: MetricsConfigs,
        #[serde(default = "Config::default_public_addr")]
        pub public_addr: SocketAddr,
        #[serde(default = "Config::default_scheduler_id")]
        pub scheduler_id: String,
        #[serde(default = "Config::default_shutdown_timeout")]
        pub shutdown_timeout: u64,
        #[serde(default = "Config::default_toolchains_storage")]
        pub toolchains: CacheConfigs,
    }

    impl Default for FileConfig {
        fn default() -> Self {
            Self {
                client_auth: ClientAuth::Insecure,
                heartbeat_interval_ms: Config::default_heartbeat_interval_ms(),
                job_time_limit: Config::default_job_time_limit(),
                jobs: Config::default_jobs_storage(),
                keepalive: Default::default(),
                max_body_size: Config::default_max_body_size(),
                max_concurrent_streams: None,
                message_broker: None,
                metrics: Default::default(),
                public_addr: SocketAddr::from_str("0.0.0.0:10500").unwrap(),
                scheduler_id: Config::default_scheduler_id(),
                shutdown_timeout: Config::default_shutdown_timeout(),
                toolchains: Config::default_toolchains_storage(),
            }
        }
    }

    #[derive(Debug)]
    pub struct Config {
        pub client_auth: ClientAuth,
        pub heartbeat_interval_ms: u64,
        pub job_time_limit: u32,
        pub jobs: Vec<CacheType>,
        pub keepalive: DistNetworkingKeepalive,
        pub max_body_size: usize,
        pub max_concurrent_streams: Option<u32>,
        pub message_broker: Option<MessageBroker>,
        pub metrics: MetricsConfigs,
        pub public_addr: SocketAddr,
        pub scheduler_id: String,
        pub shutdown_timeout: u64,
        pub toolchains: Vec<CacheType>,
    }

    impl Default for Config {
        fn default() -> Self {
            FileConfig::default().into()
        }
    }

    impl Config {
        pub fn default_jobs_storage() -> CacheConfigs {
            CacheConfigs {
                disk: Some(DiskCacheConfig {
                    dir: default_disk_cache_dir().join("jobs"),
                    rw_mode: CacheModeConfig::ReadWrite,
                    size: default_disk_cache_size(),
                    ..Default::default()
                }),
                ..Default::default()
            }
        }
        pub fn default_toolchains_storage() -> CacheConfigs {
            CacheConfigs {
                disk: Some(DiskCacheConfig {
                    dir: default_disk_cache_dir().join("toolchains"),
                    rw_mode: CacheModeConfig::ReadWrite,
                    size: default_disk_cache_size(),
                    ..Default::default()
                }),
                ..Default::default()
            }
        }
        // Default to 15s
        fn default_heartbeat_interval_ms() -> u64 {
            15000
        }
        pub fn default_job_time_limit() -> u32 {
            600
        }
        pub fn default_max_body_size() -> usize {
            // 1GiB should be enough for toolchains and compile inputs, right?
            1024 * 1024 * 1024
        }
        pub fn default_public_addr() -> SocketAddr {
            SocketAddr::from_str("0.0.0.0:10500").unwrap()
        }
        pub fn default_scheduler_id() -> String {
            uuid::Uuid::new_v4().as_simple().to_string()
        }
        pub fn default_shutdown_timeout() -> u64 {
            10
        }
        pub fn load(conf_path: Option<PathBuf>) -> Result<Self> {
            let FileConfig {
                client_auth,
                heartbeat_interval_ms,
                job_time_limit,
                jobs,
                keepalive,
                max_body_size,
                max_concurrent_streams,
                message_broker,
                metrics,
                public_addr,
                scheduler_id,
                shutdown_timeout,
                toolchains,
            } = conf_path
                .map(|path| {
                    let conf = try_read_config_file::<FileConfig>(&path)
                        .context("Failed to load scheduler config file");
                    match conf {
                        Ok(conf) => conf.unwrap_or_default(),
                        Err(err) => {
                            warn!("{err:?}");
                            Default::default()
                        }
                    }
                })
                .unwrap_or_default();

            let jobs = CacheConfigs::default()
                .merge(jobs)
                .merge(config_from_env("SCCACHE_DIST_JOBS_")?.cache);

            let toolchains = CacheConfigs::default()
                .merge(toolchains)
                .merge(config_from_env("SCCACHE_DIST_TOOLCHAINS_")?.cache);

            let heartbeat_interval_ms =
                number_from_env_var("SCCACHE_DIST_SCHEDULER_HEARTBEAT_INTERVAL")
                    .transpose()?
                    .unwrap_or(heartbeat_interval_ms);

            let job_time_limit = number_from_env_var("SCCACHE_DIST_JOB_TIME_LIMIT_SECS")
                .transpose()?
                .unwrap_or(job_time_limit);

            let max_body_size = number_from_env_var("SCCACHE_DIST_MAX_BODY_SIZE")
                .transpose()?
                .unwrap_or(max_body_size);

            let max_concurrent_streams = number_from_env_var("SCCACHE_DIST_MAX_CONCURRENT_STREAMS")
                .transpose()
                .unwrap_or(max_concurrent_streams);

            let message_broker = MessageBroker::from_env().or(message_broker);

            let scheduler_id = env::var("SCCACHE_DIST_SCHEDULER_ID")
                .ok()
                .unwrap_or(scheduler_id);

            let shutdown_timeout = number_from_env_var("SCCACHE_DIST_SHUTDOWN_TIMEOUT")
                .transpose()?
                .unwrap_or(shutdown_timeout);

            Ok(Self {
                client_auth,
                heartbeat_interval_ms,
                job_time_limit,
                jobs: jobs.into(),
                keepalive: keepalive.with_env_or_config(),
                max_body_size,
                max_concurrent_streams,
                message_broker,
                metrics,
                public_addr,
                scheduler_id,
                shutdown_timeout,
                toolchains: toolchains.into(),
            })
        }

        pub fn into_file(self) -> FileConfig {
            self.into()
        }
    }

    impl From<Config> for FileConfig {
        fn from(scheduler_config: Config) -> Self {
            Self {
                client_auth: scheduler_config.client_auth,
                heartbeat_interval_ms: scheduler_config.heartbeat_interval_ms,
                job_time_limit: scheduler_config.job_time_limit,
                jobs: scheduler_config.jobs.into(),
                keepalive: scheduler_config.keepalive,
                max_body_size: scheduler_config.max_body_size,
                max_concurrent_streams: scheduler_config.max_concurrent_streams,
                message_broker: scheduler_config.message_broker,
                metrics: scheduler_config.metrics,
                public_addr: scheduler_config.public_addr,
                scheduler_id: scheduler_config.scheduler_id,
                shutdown_timeout: scheduler_config.shutdown_timeout,
                toolchains: scheduler_config.toolchains.into(),
            }
        }
    }

    impl From<FileConfig> for Config {
        fn from(scheduler_config: FileConfig) -> Self {
            Self {
                client_auth: scheduler_config.client_auth,
                heartbeat_interval_ms: scheduler_config.heartbeat_interval_ms,
                job_time_limit: scheduler_config.job_time_limit,
                jobs: scheduler_config.jobs.into(),
                keepalive: scheduler_config.keepalive,
                max_body_size: scheduler_config.max_body_size,
                max_concurrent_streams: scheduler_config.max_concurrent_streams,
                message_broker: scheduler_config.message_broker,
                metrics: scheduler_config.metrics,
                public_addr: scheduler_config.public_addr,
                scheduler_id: scheduler_config.scheduler_id,
                shutdown_timeout: scheduler_config.shutdown_timeout,
                toolchains: scheduler_config.toolchains.into(),
            }
        }
    }
}

#[cfg(feature = "dist-server")]
pub mod server {
    use super::{
        CacheConfigs, CacheModeConfig, CacheType, DiskCacheConfig, MessageBroker, MetricsConfigs,
        PrometheusMetricsConfig, config_from_env, default_disk_cache_dir, default_disk_cache_size,
        number_from_env_var, try_read_config_file,
    };
    use serde::{Deserialize, Serialize};
    use std::env;
    use std::path::PathBuf;

    use crate::errors::*;

    const TEN_GIGS: u64 = 10 * 1024 * 1024 * 1024;
    fn default_toolchain_cache_size() -> u64 {
        TEN_GIGS
    }

    const DEFAULT_POT_CLONE_FROM: &str = "sccache-template";
    const DEFAULT_POT_FS_ROOT: &str = "/opt/pot";
    const DEFAULT_POT_CMD: &str = "pot";
    const DEFAULT_POT_CLONE_ARGS: &[&str] = &["-i", "lo0|127.0.0.2"];

    fn default_pot_clone_from() -> String {
        DEFAULT_POT_CLONE_FROM.to_string()
    }

    fn default_pot_fs_root() -> PathBuf {
        DEFAULT_POT_FS_ROOT.into()
    }

    fn default_pot_cmd() -> PathBuf {
        DEFAULT_POT_CMD.into()
    }

    fn default_pot_clone_args() -> Vec<String> {
        DEFAULT_POT_CLONE_ARGS
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "type")]
    #[serde(deny_unknown_fields)]
    pub enum BuilderType {
        #[default]
        #[serde(rename = "docker")]
        Docker,
        #[serde(rename = "overlay")]
        Overlay {
            build_dir: PathBuf,
            bwrap_path: PathBuf,
        },
        #[serde(rename = "pot")]
        Pot {
            #[serde(default = "default_pot_fs_root")]
            pot_fs_root: PathBuf,
            #[serde(default = "default_pot_clone_from")]
            clone_from: String,
            #[serde(default = "default_pot_cmd")]
            pot_cmd: PathBuf,
            #[serde(default = "default_pot_clone_args")]
            pot_clone_args: Vec<String>,
        },
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(default)]
    #[serde(deny_unknown_fields)]
    pub struct FileConfig {
        pub builder: BuilderType,
        pub cache_dir: PathBuf,
        #[serde(default = "Config::default_heartbeat_interval_ms")]
        pub heartbeat_interval_ms: u64,
        #[serde(default = "Config::default_jobs_storage")]
        pub jobs: CacheConfigs,
        #[serde(default = "Config::default_max_per_core_load")]
        pub max_per_core_load: f64,
        #[serde(default = "Config::default_max_per_core_prefetch")]
        pub max_per_core_prefetch: f64,
        pub message_broker: Option<MessageBroker>,
        pub metrics: MetricsConfigs,
        #[serde(default = "Config::default_server_id")]
        pub server_id: String,
        #[serde(default = "Config::default_shutdown_timeout")]
        pub shutdown_timeout: u64,
        #[serde(default = "Config::default_toolchain_cache_size")]
        pub toolchain_cache_size: u64,
        #[serde(default = "Config::default_toolchains_storage")]
        pub toolchains: CacheConfigs,
    }

    impl Default for FileConfig {
        fn default() -> Self {
            Self {
                builder: BuilderType::Docker,
                cache_dir: Default::default(),
                heartbeat_interval_ms: Config::default_heartbeat_interval_ms(),
                jobs: Config::default_jobs_storage(),
                max_per_core_load: Config::default_max_per_core_load(),
                max_per_core_prefetch: Config::default_max_per_core_prefetch(),
                message_broker: None,
                metrics: Default::default(),
                server_id: Config::default_server_id(),
                shutdown_timeout: Config::default_shutdown_timeout(),
                toolchain_cache_size: Config::default_toolchain_cache_size(),
                toolchains: Config::default_toolchains_storage(),
            }
        }
    }

    #[derive(Debug)]
    pub struct Config {
        pub builder: BuilderType,
        pub cache_dir: PathBuf,
        pub heartbeat_interval_ms: u64,
        pub jobs: Vec<CacheType>,
        pub max_per_core_load: f64,
        pub max_per_core_prefetch: f64,
        pub message_broker: Option<MessageBroker>,
        pub metrics: MetricsConfigs,
        pub server_id: String,
        pub shutdown_timeout: u64,
        pub toolchain_cache_size: u64,
        pub toolchains: Vec<CacheType>,
    }

    impl Default for Config {
        fn default() -> Self {
            FileConfig::default().into()
        }
    }

    impl Config {
        pub fn default_jobs_storage() -> CacheConfigs {
            CacheConfigs {
                disk: Some(DiskCacheConfig {
                    dir: default_disk_cache_dir().join("jobs"),
                    rw_mode: CacheModeConfig::ReadWrite,
                    size: default_disk_cache_size(),
                    ..Default::default()
                }),
                ..Default::default()
            }
        }
        pub fn default_toolchains_storage() -> CacheConfigs {
            CacheConfigs {
                disk: Some(DiskCacheConfig {
                    dir: default_disk_cache_dir().join("toolchains"),
                    rw_mode: CacheModeConfig::ReadWrite,
                    size: default_disk_cache_size(),
                    ..Default::default()
                }),
                ..Default::default()
            }
        }
        // Default to 15s
        fn default_heartbeat_interval_ms() -> u64 {
            15000
        }
        // Default to 1
        fn default_max_per_core_load() -> f64 {
            1.0
        }
        // Default to 1
        fn default_max_per_core_prefetch() -> f64 {
            1.0
        }
        fn default_server_id() -> String {
            uuid::Uuid::new_v4().as_simple().to_string()
        }
        fn default_shutdown_timeout() -> u64 {
            10
        }
        fn default_toolchain_cache_size() -> u64 {
            default_toolchain_cache_size()
        }
        pub fn load(conf_path: Option<PathBuf>) -> Result<Self> {
            let FileConfig {
                message_broker,
                builder,
                cache_dir,
                heartbeat_interval_ms,
                jobs,
                max_per_core_load,
                max_per_core_prefetch,
                metrics,
                server_id,
                shutdown_timeout,
                toolchain_cache_size,
                toolchains,
            } = conf_path
                .map(|path| {
                    let conf = try_read_config_file::<FileConfig>(&path)
                        .context("Failed to load server config file");
                    match conf {
                        Ok(conf) => conf.unwrap_or_default(),
                        Err(err) => {
                            warn!("{err:?}");
                            Default::default()
                        }
                    }
                })
                .unwrap_or_default();

            if let Some(PrometheusMetricsConfig::ListenPath { .. }) = metrics.prometheus {
                return Err(anyhow!(
                    "Invalid config `metrics.prometheus.type=\"path\"`. Choose `type = \"addr\"` or `type = \"push\"`"
                ));
            }

            let builder = if let Ok(builder_type) = env::var("SCCACHE_DIST_BUILDER_TYPE") {
                match builder_type.as_ref() {
                    "docker" => BuilderType::Docker,
                    "overlay" => BuilderType::Overlay {
                        build_dir: env::var("SCCACHE_DIST_BUILD_DIR").map(|p| p.into())?,
                        bwrap_path: env::var("SCCACHE_DIST_BWRAP_PATH").map(|p| p.into())?,
                    },
                    "pot" => BuilderType::Pot {
                        clone_from: default_pot_clone_from(),
                        pot_clone_args: default_pot_clone_args(),
                        pot_cmd: env::var("SCCACHE_DIST_POT_CMD")
                            .map(|p| p.into())
                            .unwrap_or_else(|_| default_pot_cmd()),
                        pot_fs_root: env::var("SCCACHE_DIST_BUILD_DIR")
                            .map(|p| p.into())
                            .unwrap_or_else(|_| default_pot_fs_root()),
                    },
                    _ => builder,
                }
            } else {
                builder
            };

            let builder = match builder {
                BuilderType::Overlay {
                    build_dir,
                    bwrap_path,
                } => BuilderType::Overlay {
                    build_dir: env::var("SCCACHE_DIST_BUILD_DIR")
                        .map(|p| p.into())
                        .unwrap_or(build_dir),
                    bwrap_path: env::var("SCCACHE_DIST_BWRAP_PATH")
                        .map(|p| p.into())
                        .unwrap_or(bwrap_path),
                },
                BuilderType::Pot {
                    clone_from,
                    pot_cmd,
                    pot_fs_root,
                    pot_clone_args,
                } => BuilderType::Pot {
                    clone_from,
                    pot_clone_args,
                    pot_cmd: env::var("SCCACHE_DIST_POT_CMD")
                        .map(|p| p.into())
                        .unwrap_or(pot_cmd),
                    pot_fs_root: env::var("SCCACHE_DIST_BUILD_DIR")
                        .map(|p| p.into())
                        .unwrap_or(pot_fs_root),
                },
                builder => builder,
            };

            let jobs = CacheConfigs::default()
                .merge(jobs)
                .merge(config_from_env("SCCACHE_DIST_JOBS_")?.cache);

            let toolchains = CacheConfigs::default()
                .merge(toolchains)
                .merge(config_from_env("SCCACHE_DIST_TOOLCHAINS_")?.cache);

            let heartbeat_interval_ms =
                number_from_env_var("SCCACHE_DIST_SERVER_HEARTBEAT_INTERVAL")
                    .transpose()?
                    .unwrap_or(heartbeat_interval_ms);

            let max_per_core_load = number_from_env_var("SCCACHE_DIST_MAX_PER_CORE_LOAD")
                .transpose()?
                .unwrap_or(max_per_core_load);

            let max_per_core_prefetch = number_from_env_var("SCCACHE_DIST_MAX_PER_CORE_PREFETCH")
                .transpose()?
                .unwrap_or(max_per_core_prefetch);

            let message_broker = MessageBroker::from_env().or(message_broker);

            let server_id = env::var("SCCACHE_DIST_SERVER_ID").ok().unwrap_or(server_id);

            let shutdown_timeout = number_from_env_var("SCCACHE_DIST_SHUTDOWN_TIMEOUT")
                .transpose()?
                .unwrap_or(shutdown_timeout);

            let toolchain_cache_size = number_from_env_var("SCCACHE_DIST_TOOLCHAIN_CACHE_SIZE")
                .transpose()?
                .unwrap_or(toolchain_cache_size);

            Ok(Self {
                builder,
                cache_dir,
                heartbeat_interval_ms,
                jobs: jobs.into(),
                max_per_core_load,
                max_per_core_prefetch,
                message_broker,
                metrics,
                server_id,
                shutdown_timeout,
                toolchain_cache_size,
                toolchains: toolchains.into(),
            })
        }

        pub fn into_file(self) -> FileConfig {
            self.into()
        }
    }

    impl From<Config> for FileConfig {
        fn from(server_config: Config) -> Self {
            Self {
                builder: server_config.builder,
                cache_dir: server_config.cache_dir,
                heartbeat_interval_ms: server_config.heartbeat_interval_ms,
                jobs: server_config.jobs.into(),
                max_per_core_load: server_config.max_per_core_load,
                max_per_core_prefetch: server_config.max_per_core_prefetch,
                message_broker: server_config.message_broker,
                metrics: server_config.metrics,
                server_id: server_config.server_id,
                shutdown_timeout: server_config.shutdown_timeout,
                toolchain_cache_size: server_config.toolchain_cache_size,
                toolchains: server_config.toolchains.into(),
            }
        }
    }

    impl From<FileConfig> for Config {
        fn from(server_config: FileConfig) -> Self {
            Self {
                builder: server_config.builder,
                cache_dir: server_config.cache_dir,
                heartbeat_interval_ms: server_config.heartbeat_interval_ms,
                jobs: server_config.jobs.into(),
                max_per_core_load: server_config.max_per_core_load,
                max_per_core_prefetch: server_config.max_per_core_prefetch,
                message_broker: server_config.message_broker,
                metrics: server_config.metrics,
                server_id: server_config.server_id,
                shutdown_timeout: server_config.shutdown_timeout,
                toolchain_cache_size: server_config.toolchain_cache_size,
                toolchains: server_config.toolchains.into(),
            }
        }
    }
}

#[test]
fn test_parse_size() {
    assert_eq!(None, parse_size(""));
    assert_eq!(None, parse_size("bogus value"));
    assert_eq!(Some(100), parse_size("100"));
    assert_eq!(Some(2048), parse_size("2K"));
    assert_eq!(Some(2048), parse_size("2k"));
    assert_eq!(Some(10 * 1024 * 1024), parse_size("10M"));
    assert_eq!(Some(TEN_GIGS), parse_size("10G"));
    assert_eq!(Some(1024 * TEN_GIGS), parse_size("10T"));
}

#[test]
fn test_cache_ordering() {
    let disk = CacheType::Disk(DiskCacheConfig::default());
    let s3 = CacheType::S3(S3CacheConfig::default());
    let redis = CacheType::Redis(RedisCacheConfig::default());
    let memcached = CacheType::Memcached(MemcachedCacheConfig::default());
    let gcs = CacheType::GCS(GCSCacheConfig::default());
    let gha = CacheType::GHA(GHACacheConfig::default());
    let azure = CacheType::Azure(AzureCacheConfig::default());
    let webdav = CacheType::Webdav(WebdavCacheConfig::default());
    let oss = CacheType::OSS(OSSCacheConfig::default());

    assert_eq!(disk.order(), DiskCacheConfig::default_order());
    assert_eq!(s3.order(), S3CacheConfig::default_order());
    assert_eq!(redis.order(), RedisCacheConfig::default_order());
    assert_eq!(memcached.order(), MemcachedCacheConfig::default_order());
    assert_eq!(gcs.order(), GCSCacheConfig::default_order());
    assert_eq!(gha.order(), GHACacheConfig::default_order());
    assert_eq!(azure.order(), AzureCacheConfig::default_order());
    assert_eq!(webdav.order(), WebdavCacheConfig::default_order());
    assert_eq!(oss.order(), OSSCacheConfig::default_order());

    let mut caches = vec![oss, redis, gha, memcached, gcs, s3, webdav, disk, azure];
    caches.sort();

    while let Some(cache) = caches.pop() {
        for other in caches.iter() {
            assert_eq!(cache.cmp(other), std::cmp::Ordering::Greater);
        }
    }
}

#[test]
fn config_overrides() {
    let env_conf = EnvConfig {
        cache: CacheConfigs {
            azure: Some(AzureCacheConfig {
                connection_string: String::new(),
                container: String::new(),
                key_prefix: String::new(),
                ..Default::default()
            }),
            disk: Some(DiskCacheConfig {
                dir: "/env-cache".into(),
                size: 5,
                rw_mode: CacheModeConfig::ReadWrite,
                ..Default::default()
            }),
            redis: Some(RedisCacheConfig {
                endpoint: Some("myotherredisurl".to_owned()),
                ttl: 24 * 3600,
                key_prefix: "/redis/prefix".into(),
                db: 10,
                username: Some("user".to_owned()),
                password: Some("secret".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        },
    };

    let file_conf = FileConfig {
        cache: CacheConfigs {
            disk: Some(DiskCacheConfig {
                dir: "/file-cache".into(),
                size: 15,
                rw_mode: CacheModeConfig::ReadWrite,
                ..Default::default()
            }),
            memcached: Some(MemcachedCacheConfig {
                url: "memurl".to_owned(),
                expiration: 24 * 3600,
                key_prefix: String::new(),
                ..Default::default()
            }),
            redis: Some(RedisCacheConfig {
                url: Some("myredisurl".to_owned()),
                ttl: 25 * 3600,
                key_prefix: String::new(),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };

    assert_eq!(
        Config::from_env_and_file_configs(env_conf, file_conf),
        Config {
            caches: vec![
                CacheType::Disk(DiskCacheConfig {
                    dir: "/env-cache".into(),
                    size: 5,
                    rw_mode: CacheModeConfig::ReadWrite,
                    ..Default::default()
                }),
                CacheType::Redis(RedisCacheConfig {
                    endpoint: Some("myotherredisurl".to_owned()),
                    ttl: 24 * 3600,
                    key_prefix: "/redis/prefix".into(),
                    db: 10,
                    username: Some("user".to_owned()),
                    password: Some("secret".to_owned()),
                    ..Default::default()
                }),
                CacheType::Memcached(MemcachedCacheConfig {
                    url: "memurl".to_owned(),
                    expiration: 24 * 3600,
                    key_prefix: String::new(),
                    ..Default::default()
                }),
                CacheType::Azure(AzureCacheConfig {
                    connection_string: String::new(),
                    container: String::new(),
                    key_prefix: String::new(),
                    ..Default::default()
                }),
            ],
            ..Default::default()
        }
    );
}

#[test]
#[serial]
#[cfg(feature = "s3")]
fn test_s3_no_credentials_conflict() {
    unsafe {
        env::set_var("SCCACHE_S3_NO_CREDENTIALS", "true");
        env::set_var("SCCACHE_BUCKET", "my-bucket");
        env::set_var("AWS_ACCESS_KEY_ID", "aws-access-key-id");
        env::set_var("AWS_SECRET_ACCESS_KEY", "aws-secret-access-key");
    }

    let cfg = config_from_env("SCCACHE_");

    unsafe {
        env::remove_var("SCCACHE_S3_NO_CREDENTIALS");
        env::remove_var("SCCACHE_BUCKET");
        env::remove_var("AWS_ACCESS_KEY_ID");
        env::remove_var("AWS_SECRET_ACCESS_KEY");
    }

    let error = cfg.unwrap_err();
    assert_eq!(
        "If setting S3 credentials, SCCACHE_S3_NO_CREDENTIALS must not be set.",
        error.to_string()
    );
}

#[test]
#[serial]
fn test_s3_no_credentials_invalid() {
    unsafe {
        env::set_var("SCCACHE_S3_NO_CREDENTIALS", "yes");
        env::set_var("SCCACHE_BUCKET", "my-bucket");
    }

    let cfg = config_from_env("SCCACHE_");

    unsafe {
        env::remove_var("SCCACHE_S3_NO_CREDENTIALS");
        env::remove_var("SCCACHE_BUCKET");
    }

    let error = cfg.unwrap_err();
    assert_eq!(
        "SCCACHE_S3_NO_CREDENTIALS must be 'true', 'on', '1', 'false', 'off' or '0'.",
        error.to_string()
    );
}

#[test]
#[serial]
fn test_s3_no_credentials_valid_true() {
    unsafe {
        env::set_var("SCCACHE_S3_NO_CREDENTIALS", "true");
        env::set_var("SCCACHE_BUCKET", "my-bucket");
    }

    let cfg = config_from_env("SCCACHE_");

    unsafe {
        env::remove_var("SCCACHE_S3_NO_CREDENTIALS");
        env::remove_var("SCCACHE_BUCKET");
    }

    let env_cfg = cfg.unwrap();
    match env_cfg.cache.s3 {
        Some(S3CacheConfig {
            ref bucket,
            no_credentials,
            ..
        }) => {
            assert_eq!(bucket, "my-bucket");
            assert!(no_credentials);
        }
        None => unreachable!(),
    };
}

#[test]
#[serial]
fn test_s3_no_credentials_valid_false() {
    unsafe {
        env::set_var("SCCACHE_S3_NO_CREDENTIALS", "false");
        env::set_var("SCCACHE_BUCKET", "my-bucket");
    }

    let cfg = config_from_env("SCCACHE_");

    unsafe {
        env::remove_var("SCCACHE_S3_NO_CREDENTIALS");
        env::remove_var("SCCACHE_BUCKET");
    }

    let env_cfg = cfg.unwrap();
    match env_cfg.cache.s3 {
        Some(S3CacheConfig {
            ref bucket,
            no_credentials,
            ..
        }) => {
            assert_eq!(bucket, "my-bucket");
            assert!(!no_credentials);
        }
        None => unreachable!(),
    };
}

#[test]
#[serial]
#[cfg(feature = "gcs")]
fn test_gcs_service_account() {
    unsafe {
        env::set_var("SCCACHE_S3_NO_CREDENTIALS", "false");
        env::set_var("SCCACHE_GCS_BUCKET", "my-bucket");
        env::set_var("SCCACHE_GCS_SERVICE_ACCOUNT", "my@example.com");
        env::set_var("SCCACHE_GCS_RW_MODE", "READ_WRITE");
    }

    let cfg = config_from_env("SCCACHE_");

    unsafe {
        env::remove_var("SCCACHE_GCS_BUCKET");
        env::remove_var("SCCACHE_GCS_SERVICE_ACCOUNT");
        env::remove_var("SCCACHE_GCS_RW_MODE");
    }

    let env_cfg = cfg.unwrap();
    match env_cfg.cache.gcs {
        Some(GCSCacheConfig {
            ref bucket,
            service_account,
            rw_mode,
            ..
        }) => {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(service_account, Some("my@example.com".to_string()));
            assert_eq!(rw_mode, CacheModeConfig::ReadWrite);
        }
        None => unreachable!(),
    };
}

#[test]
fn client_toml_parse() {
    const CONFIG_STR: &str = r#"
server_startup_timeout_ms = 10000

[dist]
# where to find the scheduler
scheduler_url = "http://1.2.3.4:10600"
# a set of prepackaged toolchains
toolchains = []
# the maximum size of the toolchain cache in bytes
toolchain_cache_size = 5368709120
cache_dir = "/home/user/.cache/sccache-dist-client"

[dist.auth]
type = "token"
token = "secrettoken"

[cache.azure]
connection_string = "connection_string"
container = "container"
key_prefix = "key_prefix"

[cache.disk]
dir = "/tmp/.cache/sccache"
size = 7516192768 # 7 GiBytes

[cache.gcs]
rw_mode = "READ_ONLY"
# rw_mode = "READ_WRITE"
cred_path = "/psst/secret/cred"
bucket = "bucket"
key_prefix = "prefix"
service_account = "example_service_account"

[cache.gha]
enabled = true
version = "sccache"

[cache.memcached]
# Deprecated alias for `endpoint`
# url = "127.0.0.1:11211"
endpoint = "tcp://127.0.0.1:11211"
# Username and password for authentication
username = "user"
password = "passwd"
expiration = 90000
key_prefix = "/custom/prefix/if/need"

[cache.redis]
url = "redis://user:passwd@1.2.3.4:6379/?db=1"
endpoint = "redis://127.0.0.1:6379"
cluster_endpoints = "tcp://10.0.0.1:6379,redis://10.0.0.2:6379"
username = "another_user"
password = "new_passwd"
db = 12
expiration = 86400
key_prefix = "/my/redis/cache"

[cache.s3]
bucket = "name"
region = "us-east-2"
endpoint = "s3-us-east-1.amazonaws.com"
use_ssl = true
key_prefix = "s3prefix"
no_credentials = true
server_side_encryption = false

[cache.webdav]
endpoint = "http://127.0.0.1:8080"
key_prefix = "webdavprefix"
username = "webdavusername"
password = "webdavpassword"
token = "webdavtoken"

[cache.oss]
bucket = "name"
endpoint = "oss-us-east-1.aliyuncs.com"
key_prefix = "ossprefix"
no_credentials = true
"#;

    let file_config: FileConfig = toml::from_str(CONFIG_STR).expect("Is valid toml.");
    assert_eq!(
        file_config,
        FileConfig {
            cache: CacheConfigs {
                // azure: None, // TODO not sure how to represent a unit struct in TOML Some(AzureCacheConfig),
                azure: Some(AzureCacheConfig {
                    connection_string: "connection_string".into(),
                    container: "container".into(),
                    key_prefix: "key_prefix".into(),
                    ..Default::default()
                }),
                disk: Some(DiskCacheConfig {
                    dir: PathBuf::from("/tmp/.cache/sccache"),
                    size: 7 * 1024 * 1024 * 1024,
                    rw_mode: CacheModeConfig::ReadWrite,
                    ..Default::default()
                }),
                gcs: Some(GCSCacheConfig {
                    bucket: "bucket".to_owned(),
                    cred_path: Some("/psst/secret/cred".to_string()),
                    service_account: Some("example_service_account".to_string()),
                    rw_mode: CacheModeConfig::ReadOnly,
                    key_prefix: "prefix".into(),
                    credential_url: None,
                    ..Default::default()
                }),
                gha: Some(GHACacheConfig {
                    enabled: true,
                    version: "sccache".to_string(),
                    ..Default::default()
                }),
                redis: Some(RedisCacheConfig {
                    url: Some("redis://user:passwd@1.2.3.4:6379/?db=1".to_owned()),
                    endpoint: Some("redis://127.0.0.1:6379".to_owned()),
                    cluster_endpoints: Some("tcp://10.0.0.1:6379,redis://10.0.0.2:6379".to_owned()),
                    username: Some("another_user".to_owned()),
                    password: Some("new_passwd".to_owned()),
                    db: 12,
                    ttl: 24 * 3600,
                    key_prefix: "/my/redis/cache".into(),
                    ..Default::default()
                }),
                memcached: Some(MemcachedCacheConfig {
                    url: "tcp://127.0.0.1:11211".to_owned(),
                    username: Some("user".to_owned()),
                    password: Some("passwd".to_owned()),
                    expiration: 25 * 3600,
                    key_prefix: "/custom/prefix/if/need".into(),
                    ..Default::default()
                }),
                s3: Some(S3CacheConfig {
                    bucket: "name".to_owned(),
                    region: Some("us-east-2".to_owned()),
                    endpoint: Some("s3-us-east-1.amazonaws.com".to_owned()),
                    use_ssl: Some(true),
                    key_prefix: "s3prefix".into(),
                    no_credentials: true,
                    server_side_encryption: Some(false),
                    enable_virtual_host_style: None,
                    ..Default::default()
                }),
                webdav: Some(WebdavCacheConfig {
                    endpoint: "http://127.0.0.1:8080".to_string(),
                    key_prefix: "webdavprefix".into(),
                    username: Some("webdavusername".to_string()),
                    password: Some("webdavpassword".to_string()),
                    token: Some("webdavtoken".to_string()),
                    ..Default::default()
                }),
                oss: Some(OSSCacheConfig {
                    bucket: "name".to_owned(),
                    endpoint: Some("oss-us-east-1.aliyuncs.com".to_owned()),
                    key_prefix: "ossprefix".into(),
                    no_credentials: true,
                    ..Default::default()
                }),
            },
            dist: DistConfig {
                auth: DistAuth::Token {
                    token: "secrettoken".to_owned()
                },
                cache_dir: PathBuf::from("/home/user/.cache/sccache-dist-client"),
                rewrite_includes_only: false,
                toolchains: vec![],
                toolchain_cache_size: 5368709120,
                #[cfg(any(feature = "dist-client", feature = "dist-server"))]
                scheduler_url: Some(
                    parse_http_url("http://1.2.3.4:10600")
                        .map(|url| { HTTPUrl::from_url(url) })
                        .expect("Scheduler url must be valid url str")
                ),
                #[cfg(not(any(feature = "dist-client", feature = "dist-server")))]
                scheduler_url: Some("http://1.2.3.4:10600".to_owned()),
                ..Default::default()
            },
            server_startup_timeout_ms: Some(10000),
        }
    )
}

#[test]
#[cfg(feature = "dist-server")]
fn scheduler_toml_parse() {
    use std::net::SocketAddr;

    const CONFIG_STR: &str = r#"
# The socket address the scheduler will listen on. It's strongly recommended
# to listen on localhost and put a HTTPS server in front of it.
public_addr = "127.0.0.1:10500"
# The address of the AMQP broker
message_broker.amqp = "amqp://127.0.0.1:5672//"
scheduler_id = "scheduler-1"
# Don't allow jobs to run for longer than 20 minutes
job_time_limit = 1200

[metrics.prometheus]
type = "push"
endpoint = "http://127.0.0.1:9091/metrics/job/scheduler"
interval = 1000
username = "sccache"
password = "sccache"

[jobs.redis]
endpoint = "redis://127.0.0.1:6379"
expiration = 3600
key_prefix = "/sccache-dist-jobs"

[toolchains.s3]
bucket = "sccache"
region = "auto"
endpoint = "192.168.1.69:9000"
use_ssl = false
no_credentials = false
key_prefix = "sccache-dist-toolchains"
"#;

    let scheduler_config: scheduler::FileConfig =
        toml::from_str(CONFIG_STR).expect("Is valid toml.");
    assert_eq!(
        scheduler_config,
        scheduler::FileConfig {
            public_addr: SocketAddr::from_str("127.0.0.1:10500").unwrap(),
            job_time_limit: 1200,
            message_broker: Some(MessageBroker::AMQP("amqp://127.0.0.1:5672//".into())),
            metrics: MetricsConfigs {
                prometheus: Some(PrometheusMetricsConfig::PushGateway {
                    endpoint: "http://127.0.0.1:9091/metrics/job/scheduler".into(),
                    interval: Some(1000),
                    username: Some("sccache".into()),
                    password: Some("sccache".into()),
                    http_method: None,
                }),
                ..Default::default()
            },
            jobs: CacheConfigs {
                redis: Some(RedisCacheConfig {
                    endpoint: Some("redis://127.0.0.1:6379".into()),
                    ttl: 3600,
                    key_prefix: "/sccache-dist-jobs".into(),
                    ..RedisCacheConfig::default()
                }),
                ..CacheConfigs::default()
            },
            toolchains: CacheConfigs {
                s3: Some(S3CacheConfig {
                    bucket: "sccache".into(),
                    region: Some("auto".into()),
                    endpoint: Some("192.168.1.69:9000".into()),
                    use_ssl: Some(false),
                    no_credentials: false,
                    key_prefix: "sccache-dist-toolchains".into(),
                    ..S3CacheConfig::default()
                }),
                ..CacheConfigs::default()
            },
            scheduler_id: "scheduler-1".into(),
            ..Default::default()
        }
    )
}

#[test]
#[cfg(feature = "dist-server")]
fn server_toml_parse() {
    use server::BuilderType;
    const CONFIG_STR: &str = r#"
# This is where client toolchains will be stored.
cache_dir = "/tmp/toolchains"
# Dedicate (nproc * 1.25) CPUs to building
max_per_core_load = 1.25
# Prefetch (nproc * 1) jobs
max_per_core_prefetch = 1
server_id = "server-1"
# The maximum size of the toolchain cache, in bytes.
# If unspecified the default is 10GB.
toolchain_cache_size = 10737418240

[builder]
type = "overlay"
# The directory under which a sandboxed filesystem will be created for builds.
build_dir = "/tmp/build"
# The path to the bubblewrap version 0.3.0+ `bwrap` binary.
bwrap_path = "/usr/bin/bwrap"

[message_broker]
amqp = "amqp://127.0.0.1:5672//"

[metrics.prometheus]
type = "push"
endpoint = "http://127.0.0.1:9091/metrics/job/server"
interval = 1000
username = "sccache"
password = "sccache"

[jobs.redis]
endpoint = "redis://127.0.0.1:6379"
expiration = 3600
key_prefix = "/sccache-dist-jobs"

[toolchains.s3]
bucket = "sccache"
region = "auto"
endpoint = "192.168.1.69:9000"
use_ssl = false
no_credentials = false
key_prefix = "sccache-dist-toolchains"
"#;

    let server_config: server::FileConfig = toml::from_str(CONFIG_STR).expect("Is valid toml.");
    assert_eq!(
        server_config,
        server::FileConfig {
            builder: BuilderType::Overlay {
                build_dir: PathBuf::from("/tmp/build"),
                bwrap_path: PathBuf::from("/usr/bin/bwrap"),
            },
            cache_dir: PathBuf::from("/tmp/toolchains"),
            max_per_core_load: 1.25,
            max_per_core_prefetch: 1.0,
            message_broker: Some(MessageBroker::AMQP("amqp://127.0.0.1:5672//".into())),
            metrics: MetricsConfigs {
                prometheus: Some(PrometheusMetricsConfig::PushGateway {
                    endpoint: "http://127.0.0.1:9091/metrics/job/server".into(),
                    interval: Some(1000),
                    username: Some("sccache".into()),
                    password: Some("sccache".into()),
                    http_method: None,
                }),
                ..Default::default()
            },
            jobs: CacheConfigs {
                redis: Some(RedisCacheConfig {
                    endpoint: Some("redis://127.0.0.1:6379".into()),
                    ttl: 3600,
                    key_prefix: "/sccache-dist-jobs".into(),
                    ..RedisCacheConfig::default()
                }),
                ..CacheConfigs::default()
            },
            toolchains: CacheConfigs {
                s3: Some(S3CacheConfig {
                    bucket: "sccache".into(),
                    region: Some("auto".into()),
                    endpoint: Some("192.168.1.69:9000".into()),
                    use_ssl: Some(false),
                    no_credentials: false,
                    key_prefix: "sccache-dist-toolchains".into(),
                    ..S3CacheConfig::default()
                }),
                ..CacheConfigs::default()
            },
            server_id: "server-1".into(),
            toolchain_cache_size: 10737418240,
            ..Default::default()
        }
    )
}

#[test]
fn human_units_parse() {
    const CONFIG_STR: &str = r#"
[dist]
toolchain_cache_size = "5g"

[cache.disk]
size = "7g"
"#;

    let file_config: FileConfig = toml::from_str(CONFIG_STR).expect("Is valid toml.");
    assert_eq!(
        file_config,
        FileConfig {
            cache: CacheConfigs {
                disk: Some(DiskCacheConfig {
                    size: 7 * 1024 * 1024 * 1024,
                    ..Default::default()
                }),
                ..Default::default()
            },
            dist: DistConfig {
                toolchain_cache_size: 5 * 1024 * 1024 * 1024,
                ..Default::default()
            },
            server_startup_timeout_ms: None,
        }
    );
}
