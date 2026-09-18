// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
    config::{
        Loadable, Valid,
        cache::{Azure, AzureAuth, Cache, Caches},
        dist::{Keepalive, MessageBroker, Metrics},
        utils::{
            _Ignored, DeserializeListOfStrings, DeserializeMapOfStringsToStrings,
            deserialize_string_or_list, deserialize_string_or_seq_to_map,
        },
    },
    errors::*,
};

use itertools::Itertools;
use serde::{
    Deserialize, Serialize, de,
    ser::{self, SerializeMap},
};
use std::{collections::HashMap, ffi::OsStr, fmt, net::SocketAddr, path::Path};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub jobs: Caches,

    #[serde(default)]
    pub toolchains: Caches,

    #[serde(default = "defaults::default_auth", alias = "client_auth")]
    pub auth: Vec<Auth>,

    #[serde(default = "defaults::default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,

    #[serde(default = "defaults::default_job_time_limit")]
    pub job_time_limit_secs: u32,

    #[serde(default)]
    pub keepalive: Keepalive,

    #[serde(default = "defaults::default_max_body_size")]
    pub max_body_size: usize,

    #[serde(default)]
    pub max_concurrent_streams: Option<u32>,

    #[serde(default)]
    pub message_broker: Option<MessageBroker>,

    #[serde(default)]
    pub metrics: Metrics,

    #[serde(default = "defaults::default_public_addr")]
    pub public_addr: SocketAddr,

    #[serde(alias = "scheduler_id", default)]
    pub id: String,

    #[serde(default = "defaults::default_shutdown_timeout")]
    pub shutdown_timeout_secs: u64,
}

impl Config {
    pub fn load<P: AsRef<Path>, O: Into<Option<P>>>(path: O) -> crate::errors::Result<Self> {
        Self::from_envs_and_path(std::env::vars_os(), path).map(|mut conf| {
            if conf.id.is_empty() {
                conf.id = defaults::default_scheduler_id();
            }
            conf
        })
    }
}

impl_add_for_config! { Config }

// Delegate Default to #[serde(default)]
impl Default for Config {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl Valid for Config {
    fn validate_merged(mut self, orig: &Self) -> Result<Self> {
        for (name, orig_cache) in orig.jobs.configs.iter() {
            if !self.jobs.configs.contains_key(name) {
                self.jobs.configs.insert(name.clone(), orig_cache.clone());
            }
        }

        for (name, orig_cache) in orig.toolchains.configs.iter() {
            if !self.toolchains.configs.contains_key(name) {
                self.toolchains
                    .configs
                    .insert(name.clone(), orig_cache.clone());
            }
        }

        self.jobs.validate_storage_levels(
            if self.jobs.multilevel.chain.as_deref() == orig.jobs.multilevel.chain.as_deref() {
                "jobs.multilevel.chain"
            } else {
                "SCCACHE_DIST_JOBS_MULTILEVEL_CHAIN"
            },
        )?;

        self.toolchains.validate_storage_levels(
            if self.toolchains.multilevel.chain.as_deref()
                == orig.toolchains.multilevel.chain.as_deref()
            {
                "toolchains.multilevel.chain"
            } else {
                "SCCACHE_DIST_TOOLCHAINS_MULTILEVEL_CHAIN"
            },
        )?;

        Ok(self)
    }

    fn validate_file(self) -> Result<Self> {
        self.jobs.validate_storage_levels("jobs.multilevel.chain")?;

        self.toolchains
            .validate_storage_levels("toolchains.multilevel.chain")?;

        Ok(self)
    }

    fn validate_vars(self) -> Result<Self> {
        self.jobs
            .validate_storage_levels("SCCACHE_DIST_JOBS_MULTILEVEL_CHAIN")?;

        self.toolchains
            .validate_storage_levels("SCCACHE_DIST_TOOLCHAINS_MULTILEVEL_CHAIN")?;

        Ok(self)
    }
}

impl Loadable<Self> for Config {
    fn env_prefix<'a>() -> Option<&'a str> {
        Some("SCCACHE_DIST")
    }

    fn from_vars<I, S>(vars: I) -> Result<Self>
    where
        I: IntoIterator<Item = (S, S)>,
        S: AsRef<OsStr>,
    {
        let mut conf = Self::from_vars_impl(vars)?;

        // Remove Azure configs that don't have an auth mechanism
        conf.jobs.configs.retain(|_, cache| match cache {
            Cache::Azure(Azure { auth, .. }) => !matches!(auth, AzureAuth::None),
            _ => true,
        });

        conf.toolchains.configs.retain(|_, cache| match cache {
            Cache::Azure(Azure { auth, .. }) => !matches!(auth, AzureAuth::None),
            _ => true,
        });

        Ok(conf)
    }

    fn select_vars<Iter, S>(vars: Iter) -> Result<impl IntoIterator<Item = (String, String)>>
    where
        Iter: IntoIterator<Item = (S, S)>,
        S: AsRef<OsStr>,
    {
        use crate::util::os_str_to_str;

        let prefix_ = Self::env_prefix().map_or_else(
            || "SCCACHE_DIST_".into(),
            |prefix| {
                if prefix.ends_with("_") {
                    prefix.into()
                } else {
                    format!("{prefix}_")
                }
            },
        );

        let metrics_ = ["DOGSTATSD_", "PROMETHEUS_"];

        let vars = vars
            .into_iter()
            .map(|(key, val)| -> Result<Vec<(String, String)>> {
                let key = os_str_to_str(key.as_ref())?;
                let val = os_str_to_str(val.as_ref())?;

                if key == "AMQP_ADDR" {
                    return Ok(vec![
                        ("MESSAGE_BROKER".into(), "amqp".into()),
                        ("MESSAGE_BROKER_ADDR".into(), val.into_owned()),
                    ]);
                } else if key == "REDIS_ADDR" {
                    return Ok(vec![
                        ("MESSAGE_BROKER".into(), "redis".into()),
                        ("MESSAGE_BROKER_ADDR".into(), val.into_owned()),
                    ]);
                }

                // Only take vars that start with `SCCACHE_DIST_`
                let key = if let Some(key) = key.strip_prefix(&prefix_) {
                    key
                } else {
                    return Ok(vec![]);
                };

                let key = match key {
                    // SCCACHE_DIR -> SCCACHE_CACHE_DISK_DIR
                    "DIR" => "DISK_DIR",
                    // SCCACHE_CACHE_SIZE -> SCCACHE_CACHE_DISK_SIZE
                    "CACHE_SIZE" => "DISK_SIZE",
                    // SCCACHE_BUCKET -> SCCACHE_CACHE_S3_BUCKET
                    "BUCKET" => "S3_BUCKET",
                    // SCCACHE_REGION -> SCCACHE_CACHE_S3_REGION
                    "REGION" => "S3_REGION",
                    // SCCACHE_REDIS -> SCCACHE_CACHE_REDIS_URL
                    "REDIS" => "REDIS_URL",
                    // SCCACHE_MEMCACHED -> SCCACHE_CACHE_MEMCACHED_URL
                    "MEMCACHED" => "MEMCACHED_URL",
                    _ => key,
                };

                let res = if let Some(key) = key.strip_prefix("SCHEDULER_") {
                    // SCCACHE_DIST_SCHEDULER_ID -> SCCACHE_DIST_ID
                    // SCCACHE_DIST_SCHEDULER_HEARTBEAT_INTERVAL -> SCCACHE_DIST_HEARTBEAT_INTERVAL_SECS
                    if matches!(key, "HEARTBEAT_INTERVAL") {
                        let key = format!("{key}_SECS");
                        vec![(key, val.into_owned())]
                    } else {
                        let key = key.to_owned();
                        vec![(key, val.into_owned())]
                    }
                } else if matches!(key, "JOB_TIME_LIMIT" | "SHUTDOWN_TIMEOUT") {
                    let key = format!("{key}_SECS");
                    vec![(key, val.into_owned())]
                } else if matches!(key, "AUTH_TYPE") {
                    // SCCACHE_DIST_AUTH_TYPE -> SCCACHE_DIST_AUTH
                    let key = "AUTH".into();
                    vec![(key, val.into_owned())]
                } else if matches!(key, "METRICS_TYPE") {
                    // SCCACHE_DIST_METRICS_TYPE -> SCCACHE_DIST_METRICS
                    let key = "METRICS".to_owned();
                    vec![(key, val.into_owned())]
                } else if let Some((kind, mut key)) = metrics_.iter().find_map(|kind_| {
                    if let Some(key) = key.strip_prefix(kind_) {
                        kind_.strip_suffix("_").map(|kind| (kind, key))
                    } else {
                        None
                    }
                }) {
                    if kind == "PROMETHEUS" {
                        // SCCACHE_DIST_PROMETHEUS_PUSH_ENDPOINT -> SCCACHE_DIST_METRICS_ENDPOINT
                        // SCCACHE_DIST_PROMETHEUS_PUSH_INTERVAL -> SCCACHE_DIST_METRICS_INTERVAL
                        // SCCACHE_DIST_PROMETHEUS_PUSH_USERNAME -> SCCACHE_DIST_METRICS_USERNAME
                        // SCCACHE_DIST_PROMETHEUS_PUSH_PASSWORD -> SCCACHE_DIST_METRICS_PASSWORD
                        // SCCACHE_DIST_PROMETHEUS_PUSH_HTTP_METHOD -> SCCACHE_DIST_METRICS_HTTP_METHOD
                        if let Some(k) = key.strip_prefix("PUSH_") {
                            key = k;
                        }
                        // SCCACHE_DIST_PROMETHEUS_LISTEN_ADDR -> SCCACHE_DIST_METRICS_ADDR
                        else if let Some(k) = key.strip_prefix("LISTEN_") {
                            key = k;
                        }
                    }

                    // SCCACHE_DIST_DOGSTATSD_ADDR -> SCCACHE_DIST_METRICS_ADDR
                    // SCCACHE_DIST_PROMETHEUS_TYPE -> SCCACHE_DIST_METRICS_TYPE
                    // SCCACHE_DIST_PROMETHEUS_ADDR -> SCCACHE_DIST_METRICS_ADDR
                    // SCCACHE_DIST_PROMETHEUS_IDLE_TIMEOUT_SECS -> SCCACHE_DIST_METRICS_IDLE_TIMEOUT_SECS
                    // etc.
                    let key = format!("METRICS_{key}");
                    vec![(key, val.into_owned())]
                } else {
                    let key = key.to_owned();
                    vec![(key, val.into_owned())]
                };

                Ok(res)
            })
            .flatten_ok()
            .try_collect::<_, Vec<_>, _>()?;

        if !vars.is_empty() {
            // trace! because envvars might include secrets.
            // SCCACHE_LOG=debug is the lowest recommended log level in CI.
            trace!(
                "Building scheduler config from env:\n{}",
                vars.iter()
                    .map(|(k, v)| format!("{prefix_}{k} = \"{v}\""))
                    .join("\n")
            );
        }

        Ok(vars)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Auth {
    #[default]
    Insecure,
    Token {
        token: String,
    },
    Jwt(JWTDecode),
    ProxyToken {
        url: String,
        cache_secs: Option<u64>,
        decode: Option<ProxyTokenDecode>,
        rate_limit_on_error_count: Option<usize>,
        rate_limit_on_error_window_size_secs: Option<u64>,
    },
}

impl Serialize for Auth {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            Auth::Insecure => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "DANGEROUSLY_INSECURE")?;
                map.end()
            }
            Auth::Token { token } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "token")?;
                map.serialize_entry("token", token)?;
                map.end()
            }
            Auth::Jwt(jwt) => {
                let mut map = serializer.serialize_map(Some(6))?;
                map.serialize_entry("type", "jwt")?;
                map.serialize_entry("audience", &jwt.audience)?;
                map.serialize_entry("issuer", &jwt.issuer)?;
                map.serialize_entry("jwks_url", &jwt.jwks_url)?;
                map.serialize_entry("claims", &jwt.claims)?;
                map.serialize_entry("leeway", &jwt.leeway)?;
                map.end()
            }
            Auth::ProxyToken {
                url,
                cache_secs,
                decode,
                rate_limit_on_error_count,
                rate_limit_on_error_window_size_secs,
            } => {
                let mut map = serializer.serialize_map(Some(6))?;
                map.serialize_entry("type", "proxy_token")?;
                map.serialize_entry("url", url)?;
                map.serialize_entry("cache_secs", cache_secs)?;
                map.serialize_entry("decode", decode)?;
                map.serialize_entry("rate_limit_on_error_count", rate_limit_on_error_count)?;
                map.serialize_entry(
                    "rate_limit_on_error_window_size_secs",
                    rate_limit_on_error_window_size_secs,
                )?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Auth {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct AuthVisitor;

        impl<'de> de::Visitor<'de> for AuthVisitor {
            type Value = Auth;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("sccache-dist authentication configuration")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                let mut type_ = None;
                let mut token = None;
                let mut audience = None;
                let mut issuer = None;
                let mut jwks_url = None;
                let mut claims = None;
                let mut leeway = None;
                let mut url = None;
                let mut cache_secs = None;
                let mut decode = None;
                let mut rate_limit_on_error_count = None;
                let mut rate_limit_on_error_window_size_secs = None;

                while let Ok(Some(name)) = map.next_key::<String>() {
                    match name.as_str() {
                        "type" => {
                            type_ = Some(map.next_value::<String>()?);
                        }
                        "token" => {
                            token = Some(map.next_value::<String>()?);
                        }
                        "audience" => {
                            audience = Some(map.next_value::<DeserializeListOfStrings>()?.into());
                        }
                        "issuer" => {
                            issuer = Some(map.next_value::<DeserializeListOfStrings>()?.into());
                        }
                        "jwks_url" => {
                            jwks_url = Some(map.next_value::<DeserializeListOfStrings>()?.into());
                        }
                        "claims" => {
                            claims =
                                Some(map.next_value::<DeserializeMapOfStringsToStrings>()?.into());
                        }
                        "leeway" => {
                            leeway = Some(map.next_value::<u64>()?);
                        }
                        "url" => {
                            url = Some(map.next_value::<String>()?);
                        }
                        "cache_secs" => {
                            cache_secs = map.next_value::<Option<u64>>()?;
                        }
                        "decode" => {
                            decode = map.next_value::<Option<ProxyTokenDecode>>()?;
                        }
                        "rate_limit_on_error_count" => {
                            rate_limit_on_error_count = map.next_value::<Option<usize>>()?;
                        }
                        "rate_limit_on_error_window_size_secs" => {
                            rate_limit_on_error_window_size_secs =
                                map.next_value::<Option<u64>>()?;
                        }
                        "jwks"
                        | "cache"
                        | "rate_limit"
                        | "rate_limit_on"
                        | "rate_limit_on_error"
                        | "rate_limit_on_error_window"
                        | "rate_limit_on_error_window_size" => {
                            let _ = map.next_value::<_Ignored>();
                        }
                        name => {
                            return Err(de::Error::unknown_field(
                                name,
                                &[
                                    "type",
                                    "token",
                                    "audience",
                                    "issuer",
                                    "jwks_url",
                                    "claims",
                                    "leeway",
                                    "url",
                                    "cache_secs",
                                    "decode",
                                    "rate_limit_on_error_count",
                                    "rate_limit_on_error_window_size_secs",
                                ],
                            ));
                        }
                    }
                }

                let type_ = if type_.is_none() {
                    if audience.is_some() && issuer.is_some() && jwks_url.is_some() {
                        Some("jwt")
                    } else if url.is_some() {
                        Some("proxy_token")
                    } else if token.is_some() {
                        Some("token")
                    } else {
                        Some("DANGEROUSLY_INSECURE")
                    }
                } else {
                    type_.as_deref()
                };

                match type_.unwrap_or("DANGEROUSLY_INSECURE") {
                    "token" => Ok(Auth::Token {
                        token: token
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("token")))?,
                    }),
                    "jwt" | "jwt_validate" => Ok(Auth::Jwt(JWTDecode {
                        audience: audience
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("audience")))?,
                        issuer: issuer
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("issuer")))?,
                        jwks_url: jwks_url
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("jwks_url")))?,
                        claims: claims.unwrap_or_else(HashMap::new),
                        leeway: leeway.unwrap_or_else(defaults::default_jwt_decode_leeway),
                    })),
                    "proxy_token" => Ok(Auth::ProxyToken {
                        url: url
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("url")))?,
                        cache_secs: cache_secs.or(Some(300)),
                        decode,
                        rate_limit_on_error_count,
                        rate_limit_on_error_window_size_secs,
                    }),
                    _ => Ok(Auth::default()),
                }
            }
        }

        deserializer.deserialize_map(AuthVisitor)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct JWTDecode {
    #[serde(default, deserialize_with = "deserialize_string_or_list")]
    pub audience: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_list")]
    pub issuer: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_list")]
    pub jwks_url: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_seq_to_map")]
    pub claims: HashMap<String, String>,
    #[serde(default = "defaults::default_jwt_decode_leeway")]
    pub leeway: u64,
}

// Delegate Default to #[serde(default)]
impl Default for JWTDecode {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ProxyTokenDecode {
    #[default]
    None,
    Jwt(JWTDecode),
}

impl Serialize for ProxyTokenDecode {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            ProxyTokenDecode::None => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "none")?;
                map.end()
            }
            ProxyTokenDecode::Jwt(jwt) => {
                let mut map = serializer.serialize_map(Some(6))?;
                map.serialize_entry("type", "jwt")?;
                map.serialize_entry("audience", &jwt.audience)?;
                map.serialize_entry("issuer", &jwt.issuer)?;
                map.serialize_entry("jwks_url", &jwt.jwks_url)?;
                map.serialize_entry("claims", &jwt.claims)?;
                map.serialize_entry("leeway", &jwt.leeway)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ProxyTokenDecode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct ProxyTokenDecodeVisitor;

        impl<'de> de::Visitor<'de> for ProxyTokenDecodeVisitor {
            type Value = ProxyTokenDecode;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("sccache-dist authentication configuration")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                let mut type_ = None;
                let mut audience = None;
                let mut issuer = None;
                let mut jwks_url = None;
                let mut claims = None;
                let mut leeway = None;

                while let Ok(Some(name)) = map.next_key::<String>() {
                    match name.as_str() {
                        "type" => {
                            type_ = Some(map.next_value::<String>()?);
                        }
                        "audience" => {
                            audience = Some(map.next_value::<DeserializeListOfStrings>()?.into());
                        }
                        "issuer" => {
                            issuer = Some(map.next_value::<DeserializeListOfStrings>()?.into());
                        }
                        "jwks_url" => {
                            jwks_url = Some(map.next_value::<DeserializeListOfStrings>()?.into());
                        }
                        "claims" => {
                            claims =
                                Some(map.next_value::<DeserializeMapOfStringsToStrings>()?.into());
                        }
                        "leeway" => {
                            leeway = Some(map.next_value::<u64>()?);
                        }
                        "jwks" => {
                            let _ = map.next_value::<_Ignored>();
                        }
                        name => {
                            return Err(de::Error::unknown_field(
                                name,
                                &["type", "audience", "issuer", "jwks_url", "claims", "leeway"],
                            ));
                        }
                    }
                }

                let type_ = if type_.is_none() {
                    if audience.is_some() && issuer.is_some() && jwks_url.is_some() {
                        Some("jwt")
                    } else {
                        Some("none")
                    }
                } else {
                    type_.as_deref()
                };

                match type_.unwrap_or("none") {
                    "jwt" | "jwt_validate" => Ok(ProxyTokenDecode::Jwt(JWTDecode {
                        audience: audience
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("audience")))?,
                        issuer: issuer
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("issuer")))?,
                        jwks_url: jwks_url
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("jwks_url")))?,
                        claims: claims.unwrap_or_else(HashMap::new),
                        leeway: leeway.unwrap_or_else(defaults::default_jwt_decode_leeway),
                    })),
                    _ => Ok(ProxyTokenDecode::default()),
                }
            }
        }

        deserializer.deserialize_map(ProxyTokenDecodeVisitor)
    }
}

pub mod defaults {
    use super::*;

    use std::str::FromStr;

    pub use crate::config::dist::defaults::{default_heartbeat_interval, default_shutdown_timeout};

    pub fn default_auth() -> Vec<Auth> {
        vec![Auth::Insecure]
    }

    pub fn default_job_time_limit() -> u32 {
        // 10 minutes
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

    pub fn default_jwt_decode_leeway() -> u64 {
        60
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::{cache::*, dist::*};

    use std::str::FromStr;

    #[test]
    fn config_from_vars_amqp_addr() -> Result<()> {
        drop(env_logger::try_init());

        assert_eq!(
            Config::from_vars([("AMQP_ADDR", "amqp://127.0.0.1:5672//"),])?.message_broker,
            Some(MessageBroker::AMQP("amqp://127.0.0.1:5672//".into()))
        );

        Ok(())
    }

    #[test]
    fn config_from_vars_redis_addr() -> Result<()> {
        drop(env_logger::try_init());

        assert_eq!(
            Config::from_vars([("REDIS_ADDR", "redis://127.0.0.1:6379"),])?.message_broker,
            Some(MessageBroker::Redis("redis://127.0.0.1:6379".into()))
        );

        Ok(())
    }

    #[test]
    fn config_from_vars() -> Result<()> {
        drop(env_logger::try_init());

        assert_eq!(
            Config::from_vars([
                ("SCCACHE_DIST_MESSAGE_BROKER", "amqp"),
                (
                    "SCCACHE_DIST_MESSAGE_BROKER_ADDR",
                    "amqp://127.0.0.1:5672//"
                ),
                ("SCCACHE_DIST_SCHEDULER_ID", "scheduler-1"),
                ("SCCACHE_DIST_PUBLIC_ADDR", "127.0.0.1:10500"),
                ("SCCACHE_DIST_JOB_TIME_LIMIT_SECS", "1200"),
                ("SCCACHE_DIST_METRICS_TYPE", "prometheus"),
                ("SCCACHE_DIST_PROMETHEUS_TYPE", "push"),
                (
                    "SCCACHE_DIST_PROMETHEUS_ENDPOINT",
                    "http://127.0.0.1:9091/metrics/job/scheduler-1"
                ),
                ("SCCACHE_DIST_PROMETHEUS_IDLE_TIMEOUT_SECS", "10"),
                ("SCCACHE_DIST_PROMETHEUS_PUSH_INTERVAL_MS", "1000"),
                ("SCCACHE_DIST_PROMETHEUS_PUSH_USERNAME", "sccache"),
                ("SCCACHE_DIST_PROMETHEUS_PUSH_PASSWORD", "sccache"),
                ("SCCACHE_DIST_PROMETHEUS_PUSH_HTTP_METHOD", "post"),
                ("SCCACHE_DIST_JOBS_REDIS_URL", "redis://127.0.0.1:6379"),
                ("SCCACHE_DIST_JOBS_REDIS_TTL", "3600"),
                ("SCCACHE_DIST_JOBS_REDIS_KEY_PREFIX", "/sccache-dist-jobs"),
                ("SCCACHE_DIST_TOOLCHAINS_S3_BUCKET", "sccache"),
                ("SCCACHE_DIST_TOOLCHAINS_S3_REGION", "auto"),
                ("SCCACHE_DIST_TOOLCHAINS_S3_ENDPOINT", "192.168.1.69:9000"),
                ("SCCACHE_DIST_TOOLCHAINS_S3_USE_SSL", "false"),
                ("SCCACHE_DIST_TOOLCHAINS_S3_NO_CREDENTIALS", "false"),
                (
                    "SCCACHE_DIST_TOOLCHAINS_S3_KEY_PREFIX",
                    "sccache-dist-toolchains"
                ),
            ])?,
            Config {
                id: "scheduler-1".into(),
                public_addr: SocketAddr::from_str("127.0.0.1:10500").unwrap(),
                job_time_limit_secs: 1200,
                message_broker: Some(MessageBroker::AMQP("amqp://127.0.0.1:5672//".into())),
                metrics: Metrics::Prometheus(Prometheus::PushGateway {
                    endpoint: "http://127.0.0.1:9091/metrics/job/scheduler-1".into(),
                    interval_ms: 1000,
                    username: Some("sccache".into()),
                    password: Some("sccache".into()),
                    http_method: Some("post".into()),
                    idle_timeout_secs: Some(10)
                }),
                jobs: vec![
                    Redis {
                        ttl: 3600,
                        key_prefix: "/sccache-dist-jobs".into(),
                        ..Redis::from_url("redis://127.0.0.1:6379")
                    }
                    .into()
                ]
                .into(),
                toolchains: vec![
                    S3 {
                        region: Some("auto".into()),
                        endpoint: Some("192.168.1.69:9000".into()),
                        use_ssl: Some(false),
                        no_credentials: false,
                        key_prefix: "sccache-dist-toolchains".into(),
                        ..S3::from_bucket("sccache")
                    }
                    .into()
                ]
                .into(),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[test]
    fn config_from_toml() -> Result<()> {
        drop(env_logger::try_init());

        use std::net::SocketAddr;

        let config = Config::from_toml(
            r#"
            # The socket address the scheduler will listen on. It's strongly recommended
            # to listen on localhost and put a HTTPS server in front of it.
            public_addr = "127.0.0.1:10500"

            id = "scheduler-1"

            # Don't allow jobs to run for longer than 20 minutes
            job_time_limit_secs = 1200

            # The address of the AMQP broker
            message_broker.amqp = "amqp://127.0.0.1:5672//"

            [[client_auth]]
            type = "token"
            token = "secrettoken"

            [[client_auth]]
            type = "jwt_validate"
            audience = "token.mycompany.com"
            issuer = "https://token.mycompany.com"
            jwks_url = "https://token.mycompany.com/.well-known/jwks"

            [client_auth.claims]
            enterprise_id = "12345"
            actor = "*"
            repository = "*"
            workflow = "*"

            [[client_auth]]
            type = "proxy_token"
            url = "https://token.mycompany.com/12345?audience=sts.amazonaws.com"
            cache_secs = 300

            [client_auth.decode]
            type = "jwt"
            audience = "sts.amazonaws.com"
            issuer = "https://token.mycompany.com"
            jwks_url = "https://token.mycompany.com/.well-known/jwks"

            [client_auth.decode.claims]
            actor = "*"
            username = "*"

            [metrics.prometheus]
            type = "push"
            endpoint = "http://127.0.0.1:9091/metrics/job/scheduler-1"
            interval_ms = 1000
            username = "sccache"
            password = "sccache"
            http_method = "post"
            idle_timeout_secs = 10

            [jobs.redis]
            url = "redis://127.0.0.1:6379"
            expiration = 3600
            key_prefix = "/sccache-dist-jobs"

            [toolchains.s3]
            bucket = "sccache"
            region = "auto"
            endpoint = "192.168.1.69:9000"
            use_ssl = false
            no_credentials = false
            key_prefix = "sccache-dist-toolchains"
            "#,
        )?;

        assert_eq!(
            config,
            Config {
                id: "scheduler-1".into(),
                auth: vec![
                    Auth::Token {
                        token: "secrettoken".into()
                    },
                    Auth::Jwt(JWTDecode {
                        audience: vec!["token.mycompany.com".into()],
                        issuer: vec!["https://token.mycompany.com".into()],
                        jwks_url: vec!["https://token.mycompany.com/.well-known/jwks".into()],
                        claims: [
                            ("enterprise_id", "12345"),
                            ("actor", "*"),
                            ("repository", "*"),
                            ("workflow", "*"),
                        ]
                        .into_iter()
                        .map(|(k, v)| (k.into(), v.into()))
                        .collect(),
                        ..Default::default()
                    }),
                    Auth::ProxyToken {
                        url: "https://token.mycompany.com/12345?audience=sts.amazonaws.com".into(),
                        cache_secs: Some(300),
                        decode: Some(ProxyTokenDecode::Jwt(JWTDecode {
                            audience: vec!["sts.amazonaws.com".into()],
                            issuer: vec!["https://token.mycompany.com".into()],
                            jwks_url: vec!["https://token.mycompany.com/.well-known/jwks".into()],
                            claims: [("actor", "*"), ("username", "*")]
                                .into_iter()
                                .map(|(k, v)| (k.into(), v.into()))
                                .collect(),
                            ..Default::default()
                        })),
                        rate_limit_on_error_count: None,
                        rate_limit_on_error_window_size_secs: None,
                    }
                ],
                public_addr: SocketAddr::from_str("127.0.0.1:10500").unwrap(),
                job_time_limit_secs: 1200,
                message_broker: Some(MessageBroker::AMQP("amqp://127.0.0.1:5672//".into())),
                metrics: Metrics::Prometheus(Prometheus::PushGateway {
                    endpoint: "http://127.0.0.1:9091/metrics/job/scheduler-1".into(),
                    interval_ms: 1000,
                    username: Some("sccache".into()),
                    password: Some("sccache".into()),
                    http_method: Some("post".into()),
                    idle_timeout_secs: Some(10)
                }),
                jobs: vec![
                    Redis {
                        ttl: 3600,
                        key_prefix: "/sccache-dist-jobs".into(),
                        ..Redis::from_url("redis://127.0.0.1:6379")
                    }
                    .into()
                ]
                .into(),
                toolchains: vec![
                    S3 {
                        region: Some("auto".into()),
                        endpoint: Some("192.168.1.69:9000".into()),
                        use_ssl: Some(false),
                        no_credentials: false,
                        key_prefix: "sccache-dist-toolchains".into(),
                        ..S3::from_bucket("sccache")
                    }
                    .into()
                ]
                .into(),
                ..Default::default()
            }
        );

        Ok(())
    }
}
