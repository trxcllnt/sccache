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
use once_cell::sync::Lazy;
#[cfg(any(feature = "dist-client", feature = "dist-server"))]
use serde::ser::Serializer;
use serde::{
    de::{DeserializeOwned, Deserializer},
    Deserialize, Serialize,
};
#[cfg(test)]
use serial_test::serial;
use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;

pub use crate::cache::PreprocessorCacheModeConfig;
use crate::errors::*;

static CACHED_CONFIG_PATH: Lazy<PathBuf> = Lazy::new(CachedConfig::file_config_path);
static CACHED_CONFIG: Mutex<Option<CachedFileConfig>> = Mutex::new(None);

const ORGANIZATION: &str = "Mozilla";
const APP_NAME: &str = "sccache";
const DIST_APP_NAME: &str = "sccache-dist-client";
const TEN_GIGS: u64 = 10 * 1024 * 1024 * 1024;

const MOZILLA_OAUTH_PKCE_CLIENT_ID: &str = "F1VVD6nRTckSVrviMRaOdLBWIk1AvHYo";
// The sccache audience is an API set up in auth0 for sccache to allow 7 day expiry,
// the openid scope allows us to query the auth0 /userinfo endpoint which contains
// group information due to Mozilla rules.
const MOZILLA_OAUTH_PKCE_AUTH_URL: &str =
    "https://auth.mozilla.auth0.com/authorize?audience=sccache&scope=openid%20profile";
const MOZILLA_OAUTH_PKCE_TOKEN_URL: &str = "https://auth.mozilla.auth0.com/oauth/token";

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

pub fn parse_size(val: &str) -> Option<u64> {
    let multiplier = match val.chars().last() {
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
        let url = parse_http_url(&helper).map_err(D::Error::custom)?;
        Ok(HTTPUrl(url))
    }
}
#[cfg(any(feature = "dist-client", feature = "dist-server"))]
fn parse_http_url(url: &str) -> Result<reqwest::Url> {
    use std::net::SocketAddr;
    let url = if let Ok(sa) = url.parse::<SocketAddr>() {
        warn!("Url {} has no scheme, assuming http", url);
        reqwest::Url::parse(&format!("http://{}", sa))
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureCacheConfig {
    pub connection_string: String,
    pub container: String,
    pub key_prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(default)]
pub struct DiskCacheConfig {
    pub dir: PathBuf,
    // TODO: use deserialize_with to allow human-readable sizes in toml
    pub size: u64,
    pub preprocessor_cache_mode: PreprocessorCacheModeConfig,
    pub rw_mode: CacheModeConfig,
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        DiskCacheConfig {
            dir: default_disk_cache_dir(),
            size: default_disk_cache_size(),
            preprocessor_cache_mode: PreprocessorCacheModeConfig::activated(),
            rw_mode: CacheModeConfig::ReadWrite,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CacheModeConfig {
    #[serde(rename = "READ_ONLY")]
    ReadOnly,
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
    pub bucket: String,
    pub key_prefix: String,
    pub cred_path: Option<String>,
    pub service_account: Option<String>,
    pub rw_mode: CacheModeConfig,
    pub credential_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GHACacheConfig {
    pub enabled: bool,
    /// Version for gha cache is a namespace. By setting different versions,
    /// we can avoid mixed caches.
    pub version: String,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MemcachedCacheConfig {
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
}

/// redis has no default TTL - all caches live forever
///
/// We keep the TTL as 0 here as redis does
///
/// Please change this value freely if we have a better choice.
const DEFAULT_REDIS_CACHE_TTL: u64 = 0;
pub const DEFAULT_REDIS_DB: u32 = 0;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RedisCacheConfig {
    /// The single-node redis endpoint.
    /// Mutually exclusive with `cluster_endpoints`.
    pub endpoint: Option<String>,

    /// The redis cluster endpoints.
    /// Mutually exclusive with `endpoint`.
    pub cluster_endpoints: Option<String>,

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

    #[serde(default)]
    pub key_prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebdavCacheConfig {
    pub endpoint: String,
    #[serde(default)]
    pub key_prefix: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3CacheConfig {
    pub bucket: String,
    pub region: Option<String>,
    #[serde(default)]
    pub key_prefix: String,
    pub no_credentials: bool,
    pub endpoint: Option<String>,
    pub use_ssl: Option<bool>,
    pub server_side_encryption: Option<bool>,
    pub enable_virtual_host_style: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OSSCacheConfig {
    pub bucket: String,
    #[serde(default)]
    pub key_prefix: String,
    pub endpoint: Option<String>,
    pub no_credentials: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheType {
    Azure(AzureCacheConfig),
    GCS(GCSCacheConfig),
    GHA(GHACacheConfig),
    Memcached(MemcachedCacheConfig),
    Redis(RedisCacheConfig),
    S3(S3CacheConfig),
    Webdav(WebdavCacheConfig),
    OSS(OSSCacheConfig),
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
    /// Return cache type in an arbitrary but
    /// consistent ordering
    fn into_fallback(self) -> (Option<CacheType>, DiskCacheConfig) {
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
        } = self;

        let cache_type = s3
            .map(CacheType::S3)
            .or_else(|| redis.map(CacheType::Redis))
            .or_else(|| memcached.map(CacheType::Memcached))
            .or_else(|| gcs.map(CacheType::GCS))
            .or_else(|| gha.map(CacheType::GHA))
            .or_else(|| azure.map(CacheType::Azure))
            .or_else(|| webdav.map(CacheType::Webdav))
            .or_else(|| oss.map(CacheType::OSS));

        let fallback = disk.unwrap_or_default();

        (cache_type, fallback)
    }

    /// Override self with any existing fields from other
    fn merge(&mut self, other: Self) {
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
            self.azure = azure
        }
        if disk.is_some() {
            self.disk = disk
        }
        if gcs.is_some() {
            self.gcs = gcs
        }
        if gha.is_some() {
            self.gha = gha
        }
        if memcached.is_some() {
            self.memcached = memcached
        }
        if redis.is_some() {
            self.redis = redis
        }
        if s3.is_some() {
            self.s3 = s3
        }
        if webdav.is_some() {
            self.webdav = webdav
        }

        if oss.is_some() {
            self.oss = oss
        }
    }
}

impl From<CacheType> for CacheConfigs {
    fn from(cache_type: CacheType) -> Self {
        match cache_type {
            CacheType::Azure(opts) => Self {
                azure: Some(opts),
                ..Default::default()
            },
            CacheType::GCS(opts) => Self {
                gcs: Some(opts),
                ..Default::default()
            },
            CacheType::GHA(opts) => Self {
                gha: Some(opts),
                ..Default::default()
            },
            CacheType::Memcached(opts) => Self {
                memcached: Some(opts),
                ..Default::default()
            },
            CacheType::Redis(opts) => Self {
                redis: Some(opts),
                ..Default::default()
            },
            CacheType::S3(opts) => Self {
                s3: Some(opts),
                ..Default::default()
            },
            CacheType::Webdav(opts) => Self {
                webdav: Some(opts),
                ..Default::default()
            },
            CacheType::OSS(opts) => Self {
                oss: Some(opts),
                ..Default::default()
            },
        }
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
            #[serde(rename = "mozilla")]
            Mozilla,
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
            Helper::Mozilla => DistAuth::Oauth2CodeGrantPKCE {
                client_id: MOZILLA_OAUTH_PKCE_CLIENT_ID.to_owned(),
                auth_url: MOZILLA_OAUTH_PKCE_AUTH_URL.to_owned(),
                token_url: MOZILLA_OAUTH_PKCE_TOKEN_URL.to_owned(),
            },
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
    pub connect_timeout: u64,
    pub request_timeout: u64,
    pub connection_pool: bool,
    pub keepalive: DistNetworkingKeepalive,
}

impl Default for DistNetworking {
    fn default() -> Self {
        Self {
            // Default timeout for connections to an sccache-dist server
            connect_timeout: 5,
            // Default timeout for compile requests to an sccache-dist server.
            // Users should set their load balancer's idle timeout to match or
            // exceed this value.
            request_timeout: 600,
            // Default to not using reqwest's connection pool
            connection_pool: false,
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

impl Default for DistNetworkingKeepalive {
    fn default() -> Self {
        Self {
            enabled: false,
            timeout: 60,
            interval: 20,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct DistConfig {
    pub auth: DistAuth,
    pub cache_dir: PathBuf,
    // f64 to allow `max_retries = inf`, i.e. never fallback to local compile
    pub max_retries: f64,
    pub net: DistNetworking,
    pub rewrite_includes_only: bool,
    #[cfg(any(feature = "dist-client", feature = "dist-server"))]
    pub scheduler_url: Option<HTTPUrl>,
    #[cfg(not(any(feature = "dist-client", feature = "dist-server")))]
    pub scheduler_url: Option<String>,
    pub toolchains: Vec<DistToolchainConfig>,
    pub toolchain_cache_size: u64,
}

impl Default for DistConfig {
    fn default() -> Self {
        Self {
            auth: Default::default(),
            cache_dir: default_dist_cache_dir(),
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
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
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

fn key_prefix_from_env_var(env_var_name: &str) -> String {
    env::var(env_var_name)
        .ok()
        .as_ref()
        .map(|s| s.trim_end_matches('/'))
        .filter(|s| !s.is_empty())
        .unwrap_or_default()
        .to_owned()
}

fn number_from_env_var<A: std::str::FromStr>(env_var_name: &str) -> Option<Result<A>>
where
    <A as FromStr>::Err: std::fmt::Debug,
{
    let value = env::var(env_var_name).ok()?;

    value
        .parse::<A>()
        .map_err(|err| anyhow!("{env_var_name} value is invalid: {err:?}"))
        .into()
}

fn bool_from_env_var(env_var_name: &str) -> Result<Option<bool>> {
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
    let s3 = if let Ok(bucket) = env::var(envvar("BUCKET")) {
        let region = env::var(envvar("REGION")).ok();
        let no_credentials = bool_from_env_var(&envvar("S3_NO_CREDENTIALS"))?.unwrap_or(false);
        let use_ssl = bool_from_env_var(&envvar("S3_USE_SSL"))?;
        let server_side_encryption = bool_from_env_var(&envvar("S3_SERVER_SIDE_ENCRYPTION"))?;
        let endpoint = env::var(envvar("ENDPOINT")).ok();
        let key_prefix = key_prefix_from_env_var(&envvar("S3_KEY_PREFIX"));
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
    ) {
        (None, None, None) => None,
        (url, endpoint, cluster_endpoints) => {
            let db = number_from_env_var(&envvar("REDIS_DB"))
                .transpose()?
                .unwrap_or(DEFAULT_REDIS_DB);

            let username = env::var(envvar("REDIS_USERNAME")).ok();
            let password = env::var(envvar("REDIS_PASSWORD")).ok();

            let ttl = number_from_env_var(&envvar("REDIS_EXPIRATION"))
                .or_else(|| number_from_env_var(&envvar("REDIS_TTL")))
                .transpose()?
                .unwrap_or(DEFAULT_REDIS_CACHE_TTL);

            let key_prefix = key_prefix_from_env_var(&envvar("REDIS_KEY_PREFIX"));

            Some(RedisCacheConfig {
                url,
                endpoint,
                cluster_endpoints,
                username,
                password,
                db,
                ttl,
                key_prefix,
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

        let key_prefix = key_prefix_from_env_var(&envvar("MEMCACHED_KEY_PREFIX"));

        Some(MemcachedCacheConfig {
            url,
            username,
            password,
            expiration,
            key_prefix,
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
        let key_prefix = key_prefix_from_env_var(&envvar("GCS_KEY_PREFIX"));

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
        }
    });

    // ======= GHA =======
    let gha = if let Ok(version) = env::var(envvar("GHA_VERSION")) {
        // If SCCACHE_GHA_VERSION has been set, we don't need to check
        // SCCACHE_GHA_ENABLED's value anymore.
        Some(GHACacheConfig {
            enabled: true,
            version,
        })
    } else if bool_from_env_var(&envvar("GHA_ENABLED"))?.unwrap_or(false) {
        // If only SCCACHE_GHA_ENABLED has been set to the true value, enable with
        // default version.
        Some(GHACacheConfig {
            enabled: true,
            version: "".to_string(),
        })
    } else {
        None
    };

    // ======= Azure =======
    let azure = if let (Ok(connection_string), Ok(container)) = (
        env::var(envvar("AZURE_CONNECTION_STRING")),
        env::var(envvar("AZURE_BLOB_CONTAINER")),
    ) {
        let key_prefix = key_prefix_from_env_var(&envvar("AZURE_KEY_PREFIX"));
        Some(AzureCacheConfig {
            connection_string,
            container,
            key_prefix,
        })
    } else {
        None
    };

    // ======= WebDAV =======
    let webdav = if let Ok(endpoint) = env::var(envvar("WEBDAV_ENDPOINT")) {
        let key_prefix = key_prefix_from_env_var(&envvar("WEBDAV_KEY_PREFIX"));
        let username = env::var(envvar("WEBDAV_USERNAME")).ok();
        let password = env::var(envvar("WEBDAV_PASSWORD")).ok();
        let token = env::var(envvar("WEBDAV_TOKEN")).ok();

        Some(WebdavCacheConfig {
            endpoint,
            key_prefix,
            username,
            password,
            token,
        })
    } else {
        None
    };

    // ======= OSS =======
    let oss = if let Ok(bucket) = env::var(envvar("OSS_BUCKET")) {
        let endpoint = env::var(envvar("OSS_ENDPOINT")).ok();
        let key_prefix = key_prefix_from_env_var(&envvar("OSS_KEY_PREFIX"));

        let no_credentials = bool_from_env_var(&envvar("OSS_NO_CREDENTIALS"))?.unwrap_or(false);

        Some(OSSCacheConfig {
            bucket,
            endpoint,
            key_prefix,
            no_credentials,
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
    let disk_dir = env::var_os(envvar("DIR")).map(PathBuf::from);
    let disk_sz = env::var(envvar("CACHE_SIZE"))
        .ok()
        .and_then(|v| parse_size(&v));

    let mut preprocessor_mode_config = PreprocessorCacheModeConfig::activated();
    let preprocessor_mode_overridden = if let Some(value) = bool_from_env_var(&envvar("DIRECT"))? {
        preprocessor_mode_config.use_preprocessor_cache_mode = value;
        true
    } else {
        false
    };

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
        || preprocessor_mode_overridden
        || disk_rw_mode_overridden;
    let disk = if any_overridden {
        Some(DiskCacheConfig {
            dir: disk_dir.unwrap_or_else(default_disk_cache_dir),
            size: disk_sz.unwrap_or_else(default_disk_cache_size),
            preprocessor_cache_mode: preprocessor_mode_config,
            rw_mode: disk_rw_mode,
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
    pub cache: Option<CacheType>,
    pub fallback_cache: DiskCacheConfig,
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

        conf.dist.max_retries = number_from_env_var("SCCACHE_DIST_MAX_RETRIES")
            .transpose()?
            .unwrap_or(conf.dist.max_retries);

        conf.dist.net.connect_timeout = number_from_env_var("SCCACHE_DIST_CONNECT_TIMEOUT")
            .transpose()?
            .unwrap_or(conf.dist.net.connect_timeout);

        conf.dist.net.request_timeout = number_from_env_var("SCCACHE_DIST_REQUEST_TIMEOUT")
            .transpose()?
            .unwrap_or(conf.dist.net.request_timeout);

        conf.dist.net.connection_pool = bool_from_env_var("SCCACHE_DIST_CONNECTION_POOL")?
            .unwrap_or(conf.dist.net.connection_pool);

        conf.dist.net.keepalive.enabled = bool_from_env_var("SCCACHE_DIST_KEEPALIVE_ENABLED")?
            .unwrap_or(conf.dist.net.keepalive.enabled);

        conf.dist.net.keepalive.interval = number_from_env_var("SCCACHE_DIST_KEEPALIVE_INTERVAL")
            .transpose()?
            .unwrap_or(conf.dist.net.keepalive.interval);

        conf.dist.net.keepalive.timeout = number_from_env_var("SCCACHE_DIST_KEEPALIVE_TIMEOUT")
            .transpose()?
            .unwrap_or(conf.dist.net.keepalive.timeout);

        Ok(conf)
    }

    fn from_env_and_file_configs(env_conf: EnvConfig, file_conf: FileConfig) -> Self {
        let mut conf_caches: CacheConfigs = Default::default();

        let FileConfig {
            cache,
            dist,
            server_startup_timeout_ms,
        } = file_conf;
        conf_caches.merge(cache);

        let server_startup_timeout = server_startup_timeout_ms.map(Duration::from_millis);

        let EnvConfig { cache } = env_conf;
        conf_caches.merge(cache);

        let (caches, fallback_cache) = conf_caches.into_fallback();
        Self {
            cache: caches,
            fallback_cache,
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
            *cached_file_config = Some(cfg)
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
                    .context("Failed to create dir to hold cached config")?
            }
            Self::save_file_config(&Default::default()).with_context(|| {
                format!(
                    "Unable to create cached config file at {}",
                    file_conf_path.display()
                )
            })?
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
#[derive(Clone, Debug, Default)]
pub struct StorageConfig {
    pub storage: Option<CacheType>,
    pub fallback: DiskCacheConfig,
}

#[cfg(feature = "dist-server")]
impl From<CacheConfigs> for StorageConfig {
    fn from(conf: CacheConfigs) -> Self {
        let (storage, fallback) = conf.into_fallback();
        Self { storage, fallback }
    }
}

#[cfg(feature = "dist-server")]
impl From<StorageConfig> for CacheConfigs {
    fn from(conf: StorageConfig) -> Self {
        conf.storage
            .map(|x| x.clone().into())
            .unwrap_or_else(|| Self {
                disk: Some(conf.fallback),
                ..Default::default()
            })
    }
}

#[cfg(feature = "dist-server")]
pub mod scheduler {
    use std::env;
    use std::path::PathBuf;
    use std::{net::SocketAddr, str::FromStr};

    use crate::errors::*;

    use serde::{Deserialize, Serialize};

    use super::{
        config_from_env, default_disk_cache_dir, default_disk_cache_size, number_from_env_var,
        try_read_config_file, CacheConfigs, CacheModeConfig, DiskCacheConfig, MessageBroker,
        MetricsConfigs, StorageConfig,
    };

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(tag = "type")]
    #[serde(deny_unknown_fields)]
    pub enum ClientAuth {
        #[serde(rename = "DANGEROUSLY_INSECURE")]
        Insecure,
        #[serde(rename = "token")]
        Token { token: String },
        #[serde(rename = "jwt_validate")]
        JwtValidate {
            audience: String,
            issuer: String,
            jwks_url: String,
        },
        #[serde(rename = "mozilla")]
        Mozilla { required_groups: Vec<String> },
        #[serde(rename = "proxy_token")]
        ProxyToken {
            url: String,
            cache_secs: Option<u64>,
        },
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(default)]
    #[serde(deny_unknown_fields)]
    pub struct FileConfig {
        pub client_auth: ClientAuth,
        pub job_time_limit: Option<u32>,
        pub jobs: CacheConfigs,
        pub max_body_size: Option<usize>,
        pub message_broker: Option<MessageBroker>,
        pub metrics: MetricsConfigs,
        pub public_addr: SocketAddr,
        pub scheduler_id: Option<String>,
        pub toolchains: CacheConfigs,
    }

    impl Default for FileConfig {
        fn default() -> Self {
            Self {
                client_auth: ClientAuth::Insecure,
                job_time_limit: None,
                jobs: CacheConfigs {
                    disk: Some(DiskCacheConfig {
                        dir: default_disk_cache_dir().join("jobs"),
                        preprocessor_cache_mode: Default::default(),
                        rw_mode: CacheModeConfig::ReadWrite,
                        size: default_disk_cache_size(),
                    }),
                    ..Default::default()
                },
                max_body_size: None,
                message_broker: None,
                metrics: Default::default(),
                public_addr: SocketAddr::from_str("0.0.0.0:10500").unwrap(),
                scheduler_id: None,
                toolchains: CacheConfigs {
                    disk: Some(DiskCacheConfig {
                        dir: default_disk_cache_dir().join("toolchains"),
                        preprocessor_cache_mode: Default::default(),
                        rw_mode: CacheModeConfig::ReadWrite,
                        size: default_disk_cache_size(),
                    }),
                    ..Default::default()
                },
            }
        }
    }

    #[derive(Debug)]
    pub struct Config {
        pub client_auth: ClientAuth,
        pub job_time_limit: u32,
        pub jobs: StorageConfig,
        pub max_body_size: usize,
        pub message_broker: Option<MessageBroker>,
        pub metrics: MetricsConfigs,
        pub public_addr: SocketAddr,
        pub scheduler_id: String,
        pub toolchains: StorageConfig,
    }

    impl Config {
        pub fn load(conf_path: Option<PathBuf>) -> Result<Self> {
            let FileConfig {
                client_auth,
                job_time_limit,
                jobs,
                max_body_size,
                message_broker,
                metrics,
                public_addr,
                scheduler_id,
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

            let mut jobs_storage = CacheConfigs::default();
            jobs_storage.merge(jobs);
            jobs_storage.merge(config_from_env("SCCACHE_DIST_JOBS_")?.cache);

            let mut toolchains_storage = CacheConfigs::default();
            toolchains_storage.merge(toolchains);
            toolchains_storage.merge(config_from_env("SCCACHE_DIST_TOOLCHAINS_")?.cache);

            let job_time_limit = number_from_env_var("SCCACHE_DIST_JOB_TIME_LIMIT_SECS")
                .transpose()?
                .or(job_time_limit)
                .unwrap_or(600);

            let max_body_size = number_from_env_var("SCCACHE_DIST_MAX_BODY_SIZE")
                .transpose()?
                .or(max_body_size)
                // 1GiB should be enough for toolchains and compile inputs, right?
                .unwrap_or(1024 * 1024 * 1024);

            let message_broker = MessageBroker::from_env().or(message_broker);

            let scheduler_id = env::var("SCCACHE_DIST_SCHEDULER_ID")
                .ok()
                .or(scheduler_id)
                .unwrap_or(uuid::Uuid::new_v4().simple().to_string());

            Ok(Self {
                client_auth,
                job_time_limit,
                jobs: jobs_storage.into(),
                max_body_size,
                message_broker,
                metrics,
                public_addr,
                scheduler_id,
                toolchains: toolchains_storage.into(),
            })
        }

        pub fn into_file(self) -> FileConfig {
            self.into()
        }
    }

    impl From<Config> for FileConfig {
        fn from(scheduler_config: Config) -> Self {
            Self {
                client_auth: scheduler_config.client_auth.clone(),
                job_time_limit: Some(scheduler_config.job_time_limit),
                jobs: scheduler_config.jobs.into(),
                max_body_size: Some(scheduler_config.max_body_size),
                message_broker: scheduler_config.message_broker,
                metrics: scheduler_config.metrics,
                public_addr: scheduler_config.public_addr,
                scheduler_id: Some(scheduler_config.scheduler_id),
                toolchains: scheduler_config.toolchains.into(),
            }
        }
    }
}

#[cfg(feature = "dist-server")]
pub mod server {
    use super::{
        config_from_env, default_disk_cache_dir, default_disk_cache_size, number_from_env_var,
        try_read_config_file, CacheConfigs, CacheModeConfig, DiskCacheConfig, MessageBroker,
        MetricsConfigs, PrometheusMetricsConfig, StorageConfig,
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

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    #[serde(tag = "type")]
    #[serde(deny_unknown_fields)]
    pub enum BuilderType {
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

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(tag = "type")]
    #[serde(deny_unknown_fields)]
    pub enum SchedulerAuth {
        #[serde(rename = "DANGEROUSLY_INSECURE")]
        Insecure,
        #[serde(rename = "jwt_token")]
        JwtToken { token: String },
        #[serde(rename = "token")]
        Token { token: String },
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(default)]
    #[serde(deny_unknown_fields)]
    pub struct FileConfig {
        pub builder: BuilderType,
        pub cache_dir: PathBuf,
        pub heartbeat_interval_ms: Option<u64>,
        pub jobs: CacheConfigs,
        pub max_per_core_load: Option<f64>,
        pub max_per_core_prefetch: Option<f64>,
        pub message_broker: Option<MessageBroker>,
        pub metrics: MetricsConfigs,
        pub server_id: Option<String>,
        pub toolchain_cache_size: Option<u64>,
        pub toolchains: CacheConfigs,
    }

    impl Default for FileConfig {
        fn default() -> Self {
            Self {
                builder: BuilderType::Docker,
                cache_dir: Default::default(),
                heartbeat_interval_ms: None,
                jobs: CacheConfigs {
                    disk: Some(DiskCacheConfig {
                        dir: default_disk_cache_dir().join("jobs"),
                        preprocessor_cache_mode: Default::default(),
                        rw_mode: CacheModeConfig::ReadWrite,
                        size: default_disk_cache_size(),
                    }),
                    ..Default::default()
                },
                max_per_core_load: None,
                max_per_core_prefetch: None,
                message_broker: None,
                metrics: Default::default(),
                server_id: None,
                toolchain_cache_size: None,
                toolchains: CacheConfigs {
                    disk: Some(DiskCacheConfig {
                        dir: default_disk_cache_dir().join("toolchains"),
                        preprocessor_cache_mode: Default::default(),
                        rw_mode: CacheModeConfig::ReadWrite,
                        size: default_disk_cache_size(),
                    }),
                    ..Default::default()
                },
            }
        }
    }

    #[derive(Debug)]
    pub struct Config {
        pub builder: BuilderType,
        pub cache_dir: PathBuf,
        pub heartbeat_interval_ms: u64,
        pub jobs: StorageConfig,
        pub max_per_core_load: f64,
        pub max_per_core_prefetch: f64,
        pub message_broker: Option<MessageBroker>,
        pub metrics: MetricsConfigs,
        pub server_id: String,
        pub toolchain_cache_size: u64,
        pub toolchains: StorageConfig,
    }

    impl Config {
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
                return Err(anyhow!("Invalid config `metrics.prometheus.type=\"path\"`. Choose `type = \"addr\"` or `type = \"push\"`"));
            }

            let mut jobs_storage = CacheConfigs::default();
            jobs_storage.merge(jobs);
            jobs_storage.merge(config_from_env("SCCACHE_DIST_JOBS_")?.cache);

            let mut toolchains_storage = CacheConfigs::default();
            toolchains_storage.merge(toolchains);
            toolchains_storage.merge(config_from_env("SCCACHE_DIST_TOOLCHAINS_")?.cache);

            let heartbeat_interval_ms =
                number_from_env_var("SCCACHE_DIST_SERVER_HEARTBEAT_INTERVAL")
                    .transpose()?
                    .or(heartbeat_interval_ms)
                    // Default to 5s
                    .unwrap_or(5000);

            let max_per_core_load = number_from_env_var("SCCACHE_DIST_MAX_PER_CORE_LOAD")
                .transpose()?
                .or(max_per_core_load)
                // Default to 1.1
                .unwrap_or(1.1f64);

            let max_per_core_prefetch = number_from_env_var("SCCACHE_DIST_MAX_PER_CORE_PREFETCH")
                .transpose()?
                .or(max_per_core_prefetch)
                // Default to 1
                .unwrap_or(1f64);

            let message_broker = MessageBroker::from_env().or(message_broker);

            let server_id = env::var("SCCACHE_DIST_SERVER_ID")
                .ok()
                .or(server_id)
                .unwrap_or(uuid::Uuid::new_v4().simple().to_string());

            let toolchain_cache_size = number_from_env_var("SCCACHE_DIST_TOOLCHAIN_CACHE_SIZE")
                .transpose()?
                .or(toolchain_cache_size)
                .unwrap_or(default_toolchain_cache_size());

            Ok(Self {
                builder,
                cache_dir,
                heartbeat_interval_ms,
                jobs: jobs_storage.into(),
                max_per_core_load,
                max_per_core_prefetch,
                message_broker,
                metrics,
                server_id,
                toolchain_cache_size,
                toolchains: toolchains_storage.into(),
            })
        }

        pub fn into_file(self) -> FileConfig {
            self.into()
        }
    }

    impl From<Config> for FileConfig {
        fn from(server_config: Config) -> Self {
            Self {
                builder: server_config.builder.clone(),
                cache_dir: server_config.cache_dir.clone(),
                heartbeat_interval_ms: Some(server_config.heartbeat_interval_ms),
                jobs: server_config.jobs.into(),
                max_per_core_load: Some(server_config.max_per_core_load),
                max_per_core_prefetch: Some(server_config.max_per_core_prefetch),
                message_broker: server_config.message_broker,
                metrics: server_config.metrics,
                server_id: Some(server_config.server_id),
                toolchain_cache_size: Some(server_config.toolchain_cache_size),
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
    assert_eq!(Some(10 * 1024 * 1024), parse_size("10M"));
    assert_eq!(Some(TEN_GIGS), parse_size("10G"));
    assert_eq!(Some(1024 * TEN_GIGS), parse_size("10T"));
}

#[test]
fn config_overrides() {
    let env_conf = EnvConfig {
        cache: CacheConfigs {
            azure: Some(AzureCacheConfig {
                connection_string: String::new(),
                container: String::new(),
                key_prefix: String::new(),
            }),
            disk: Some(DiskCacheConfig {
                dir: "/env-cache".into(),
                size: 5,
                preprocessor_cache_mode: Default::default(),
                rw_mode: CacheModeConfig::ReadWrite,
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
                preprocessor_cache_mode: Default::default(),
                rw_mode: CacheModeConfig::ReadWrite,
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
        dist: Default::default(),
        server_startup_timeout_ms: None,
    };

    assert_eq!(
        Config::from_env_and_file_configs(env_conf, file_conf),
        Config {
            cache: Some(CacheType::Redis(RedisCacheConfig {
                endpoint: Some("myotherredisurl".to_owned()),
                ttl: 24 * 3600,
                key_prefix: "/redis/prefix".into(),
                db: 10,
                username: Some("user".to_owned()),
                password: Some("secret".to_owned()),
                ..Default::default()
            }),),
            fallback_cache: DiskCacheConfig {
                dir: "/env-cache".into(),
                size: 5,
                preprocessor_cache_mode: Default::default(),
                rw_mode: CacheModeConfig::ReadWrite,
            },
            dist: Default::default(),
            server_startup_timeout: None,
        }
    );
}

#[test]
#[serial]
#[cfg(feature = "s3")]
fn test_s3_no_credentials_conflict() {
    env::set_var("SCCACHE_S3_NO_CREDENTIALS", "true");
    env::set_var("SCCACHE_BUCKET", "my-bucket");
    env::set_var("AWS_ACCESS_KEY_ID", "aws-access-key-id");
    env::set_var("AWS_SECRET_ACCESS_KEY", "aws-secret-access-key");

    let cfg = config_from_env("SCCACHE_");

    env::remove_var("SCCACHE_S3_NO_CREDENTIALS");
    env::remove_var("SCCACHE_BUCKET");
    env::remove_var("AWS_ACCESS_KEY_ID");
    env::remove_var("AWS_SECRET_ACCESS_KEY");

    let error = cfg.unwrap_err();
    assert_eq!(
        "If setting S3 credentials, SCCACHE_S3_NO_CREDENTIALS must not be set.",
        error.to_string()
    );
}

#[test]
#[serial]
fn test_s3_no_credentials_invalid() {
    env::set_var("SCCACHE_S3_NO_CREDENTIALS", "yes");
    env::set_var("SCCACHE_BUCKET", "my-bucket");

    let cfg = config_from_env("SCCACHE_");

    env::remove_var("SCCACHE_S3_NO_CREDENTIALS");
    env::remove_var("SCCACHE_BUCKET");

    let error = cfg.unwrap_err();
    assert_eq!(
        "SCCACHE_S3_NO_CREDENTIALS must be 'true', 'on', '1', 'false', 'off' or '0'.",
        error.to_string()
    );
}

#[test]
#[serial]
fn test_s3_no_credentials_valid_true() {
    env::set_var("SCCACHE_S3_NO_CREDENTIALS", "true");
    env::set_var("SCCACHE_BUCKET", "my-bucket");

    let cfg = config_from_env("SCCACHE_");

    env::remove_var("SCCACHE_S3_NO_CREDENTIALS");
    env::remove_var("SCCACHE_BUCKET");

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
    env::set_var("SCCACHE_S3_NO_CREDENTIALS", "false");
    env::set_var("SCCACHE_BUCKET", "my-bucket");

    let cfg = config_from_env("SCCACHE_");

    env::remove_var("SCCACHE_S3_NO_CREDENTIALS");
    env::remove_var("SCCACHE_BUCKET");

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
    env::set_var("SCCACHE_S3_NO_CREDENTIALS", "false");
    env::set_var("SCCACHE_GCS_BUCKET", "my-bucket");
    env::set_var("SCCACHE_GCS_SERVICE_ACCOUNT", "my@example.com");
    env::set_var("SCCACHE_GCS_RW_MODE", "READ_WRITE");

    let cfg = config_from_env("SCCACHE_");

    env::remove_var("SCCACHE_GCS_BUCKET");
    env::remove_var("SCCACHE_GCS_SERVICE_ACCOUNT");
    env::remove_var("SCCACHE_GCS_RW_MODE");

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
fn full_toml_parse() {
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


#[cache.azure]
# does not work as it appears

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
                azure: None, // TODO not sure how to represent a unit struct in TOML Some(AzureCacheConfig),
                disk: Some(DiskCacheConfig {
                    dir: PathBuf::from("/tmp/.cache/sccache"),
                    size: 7 * 1024 * 1024 * 1024,
                    preprocessor_cache_mode: PreprocessorCacheModeConfig::activated(),
                    rw_mode: CacheModeConfig::ReadWrite,
                }),
                gcs: Some(GCSCacheConfig {
                    bucket: "bucket".to_owned(),
                    cred_path: Some("/psst/secret/cred".to_string()),
                    service_account: Some("example_service_account".to_string()),
                    rw_mode: CacheModeConfig::ReadOnly,
                    key_prefix: "prefix".into(),
                    credential_url: None,
                }),
                gha: Some(GHACacheConfig {
                    enabled: true,
                    version: "sccache".to_string()
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
                }),
                memcached: Some(MemcachedCacheConfig {
                    url: "tcp://127.0.0.1:11211".to_owned(),
                    username: Some("user".to_owned()),
                    password: Some("passwd".to_owned()),
                    expiration: 25 * 3600,
                    key_prefix: "/custom/prefix/if/need".into(),
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
                }),
                webdav: Some(WebdavCacheConfig {
                    endpoint: "http://127.0.0.1:8080".to_string(),
                    key_prefix: "webdavprefix".into(),
                    username: Some("webdavusername".to_string()),
                    password: Some("webdavpassword".to_string()),
                    token: Some("webdavtoken".to_string()),
                }),
                oss: Some(OSSCacheConfig {
                    bucket: "name".to_owned(),
                    endpoint: Some("oss-us-east-1.aliyuncs.com".to_owned()),
                    key_prefix: "ossprefix".into(),
                    no_credentials: true,
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
            max_per_core_load: Some(1.25),
            max_per_core_prefetch: Some(1.0),
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
            server_id: Some("server-1".into()),
            toolchain_cache_size: Some(10737418240),
            ..Default::default()
        }
    )
}
