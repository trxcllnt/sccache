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
use crate::config::{
    dist::Keepalive,
    utils::{_Ignored, deserialize_bool, deserialize_size_from_str},
};

use serde::{
    Deserialize, Serialize, de,
    ser::{self, SerializeMap},
};
use serde_with::{TryFromInto, serde_as};
use std::{fmt, path::PathBuf};

#[serde_as]
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Config {
    // dist-client authentication configuration
    #[serde(default)]
    pub auth: Auth,

    /// Local toolchains cache dir
    #[serde(default = "defaults::default_dist_cache_dir")]
    pub cache_dir: PathBuf,

    /// Whether to fallback to local compile
    #[serde(
        default = "defaults::default_true",
        deserialize_with = "deserialize_bool"
    )]
    pub fallback_to_local_compile: bool,

    // `max_retries = 0` = never retry failed dist-compiles
    // `max_retries = 1` = retry failed dist-compiles once
    // `max_retries = inf` = retry all failed dist-compiles (never compile locally)
    #[serde(default)]
    #[serde_as(as = "TryFromInto<f64>")]
    pub max_retries: MaxRetries,

    // Configuration for the dist reqwest client
    #[serde(default)]
    pub net: Networking,

    #[serde(default, deserialize_with = "deserialize_bool")]
    pub rewrite_includes_only: bool,

    #[cfg(any(feature = "dist-client", feature = "dist-server"))]
    #[serde(default, alias = "scheduler_url")]
    pub url: Option<crate::config::utils::HTTPUrl>,

    #[cfg(not(any(feature = "dist-client", feature = "dist-server")))]
    #[serde(default, alias = "scheduler_url")]
    pub url: Option<String>,

    /// Custom toolchains
    #[serde(default)]
    pub toolchains: Vec<Toolchain>,

    #[serde(
        default = "defaults::default_disk_cache_size",
        deserialize_with = "deserialize_size_from_str"
    )]
    pub toolchain_cache_size: u64,
}

// Delegate Default to #[serde(default)]
impl Default for Config {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Auth {
    Token {
        token: String,
    },
    Oauth2CodeGrantPKCE {
        client_id: String,
        auth_url: String,
        token_url: String,
    },
    Oauth2Implicit {
        client_id: String,
        auth_url: String,
    },
}

impl Serialize for Auth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            Auth::Token { token } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "token")?;
                map.serialize_entry("token", token)?;
                map.end()
            }
            Auth::Oauth2CodeGrantPKCE {
                client_id,
                auth_url,
                token_url,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("type", "oauth2_code_grant_pkce")?;
                map.serialize_entry("client_id", client_id)?;
                map.serialize_entry("auth_url", auth_url)?;
                map.serialize_entry("token_url", token_url)?;
                map.end()
            }
            Auth::Oauth2Implicit {
                client_id,
                auth_url,
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "oauth2_implicit")?;
                map.serialize_entry("client_id", client_id)?;
                map.serialize_entry("auth_url", auth_url)?;
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
                f.write_str("dist-client authentication configuration")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                let mut type_ = None;
                let mut token = None;
                let mut client_id = None;
                let mut auth_url = None;
                let mut token_url = None;

                while let Ok(Some(name)) = map.next_key::<String>() {
                    match name.as_str() {
                        "type" => {
                            type_ = Some(map.next_value::<String>()?);
                        }
                        "auth" => {
                            let _ = map.next_value::<_Ignored>();
                        }
                        "auth_url" => {
                            auth_url = Some(map.next_value::<String>()?);
                        }
                        "client" => {
                            let _ = map.next_value::<_Ignored>();
                        }
                        "client_id" => {
                            client_id = Some(map.next_value::<String>()?);
                        }
                        "token" => {
                            token = Some(map.next_value::<String>()?);
                        }
                        "token_url" => {
                            token_url = Some(map.next_value::<String>()?);
                        }
                        name => {
                            return Err(de::Error::unknown_field(
                                name,
                                &["type", "token", "client_id", "auth_url", "token_url"],
                            ));
                        }
                    }
                }

                let type_ = if type_.is_none() {
                    if client_id.is_some() && auth_url.is_some() && token_url.is_some() {
                        Some("oauth2_code_grant_pkce")
                    } else if client_id.is_some() && auth_url.is_some() {
                        Some("oauth2_implicit")
                    } else if token.is_some() {
                        Some("token")
                    } else {
                        None
                    }
                } else {
                    type_.as_deref()
                };

                match type_.unwrap_or("none") {
                    "token" => Ok(Auth::Token {
                        token: token
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("token")))?,
                    }),
                    "oauth2_code_grant_pkce" => Ok(Auth::Oauth2CodeGrantPKCE {
                        client_id: client_id
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("client_id")))?,
                        auth_url: auth_url
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("auth_url")))?,
                        token_url: token_url
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("token_url")))?,
                    }),
                    "oauth2_implicit" => Ok(Auth::Oauth2Implicit {
                        client_id: client_id
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("client_id")))?,
                        auth_url: auth_url
                            .map(Ok)
                            .unwrap_or_else(|| Err(de::Error::missing_field("auth_url")))?,
                    }),
                    _ => Ok(Auth::default()),
                }
            }
        }

        deserializer.deserialize_map(AuthVisitor)
    }
}

impl Default for Auth {
    fn default() -> Self {
        Auth::Token {
            token: crate::config::INSECURE_DIST_CLIENT_TOKEN.to_owned(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MaxRetries {
    Inf,
    Count(u32),
}

impl Default for MaxRetries {
    fn default() -> Self {
        Self::Count(0)
    }
}

impl From<f64> for MaxRetries {
    fn from(count: f64) -> Self {
        if count.is_infinite() {
            MaxRetries::Inf
        } else {
            MaxRetries::Count(count.clamp(0f64, u32::MAX as f64) as u32)
        }
    }
}

impl From<MaxRetries> for f64 {
    fn from(max_retries: MaxRetries) -> Self {
        match max_retries {
            MaxRetries::Inf => f64::INFINITY,
            MaxRetries::Count(c) => c as f64,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Networking {
    pub connect_timeout: u32,
    pub request_timeout: u32,
    #[serde(default, deserialize_with = "deserialize_bool")]
    pub connection_pool: bool,
    pub max_connections: u32,
    pub keepalive: Keepalive,
}

impl Default for Networking {
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
            // Default to using 8 separate reqwest clients to maximize HTTP/2 throughput
            max_connections: 8,
            keepalive: Default::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Toolchain {
    NoDist {
        compiler_executable: PathBuf,
    },
    PathOverride {
        compiler_executable: PathBuf,
        archive: PathBuf,
        archive_compiler_executable: String,
    },
}

impl Serialize for Toolchain {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            Toolchain::NoDist {
                compiler_executable,
            } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "no_dist")?;
                map.serialize_entry("compiler_executable", compiler_executable)?;
                map.end()
            }
            Toolchain::PathOverride {
                compiler_executable,
                archive,
                archive_compiler_executable,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("type", "path_override")?;
                map.serialize_entry("compiler_executable", compiler_executable)?;
                map.serialize_entry("archive", archive)?;
                map.serialize_entry("archive_compiler_executable", archive_compiler_executable)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Toolchain {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct ToolchainVisitor;

        impl<'de> de::Visitor<'de> for ToolchainVisitor {
            type Value = Toolchain;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("dist-client authentication configuration")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                let mut type_ = None;
                let mut archive = None;
                let mut compiler_executable = None;
                let mut archive_compiler_executable = None;

                while let Ok(Some(name)) = map.next_key::<String>() {
                    match name.as_str() {
                        "type" => {
                            type_ = Some(map.next_value::<String>()?);
                        }
                        "archive" => {
                            archive = Some(map.next_value::<PathBuf>()?);
                        }
                        "compiler" => {
                            let _ = map.next_value::<_Ignored>();
                        }
                        "compiler_executable" => {
                            compiler_executable = Some(map.next_value::<PathBuf>()?);
                        }
                        "archive_compiler" => {
                            let _ = map.next_value::<_Ignored>();
                        }
                        "archive_compiler_executable" => {
                            archive_compiler_executable = Some(map.next_value::<String>()?);
                        }
                        name => {
                            return Err(de::Error::unknown_field(
                                name,
                                &[
                                    "type",
                                    "compiler_executable",
                                    "archive",
                                    "archive_compiler_executable",
                                ],
                            ));
                        }
                    }
                }

                let type_ = if type_.is_none() {
                    if compiler_executable.is_some()
                        && archive.is_some()
                        && archive_compiler_executable.is_some()
                    {
                        Some("path_override")
                    } else if compiler_executable.is_some() {
                        Some("no_dist")
                    } else {
                        None
                    }
                } else {
                    type_.as_deref()
                };

                match type_.unwrap_or("none") {
                    "no_dist" => {
                        return Ok(Toolchain::NoDist {
                            compiler_executable: compiler_executable.map(Ok).unwrap_or_else(
                                || Err(de::Error::missing_field("compiler_executable")),
                            )?,
                        });
                    }
                    "path_override" => {
                        return Ok(Toolchain::PathOverride {
                            archive: archive
                                .map(Ok)
                                .unwrap_or_else(|| Err(de::Error::missing_field("archive")))?,
                            compiler_executable: compiler_executable.map(Ok).unwrap_or_else(
                                || Err(de::Error::missing_field("compiler_executable")),
                            )?,
                            archive_compiler_executable: archive_compiler_executable
                                .map(Ok)
                                .unwrap_or_else(|| {
                                    Err(de::Error::missing_field("archive_compiler_executable"))
                                })?,
                        });
                    }
                    name => Err(de::Error::unknown_variant(
                        name,
                        &["no_dist", "path_override"],
                    )),
                }
            }
        }

        deserializer.deserialize_map(ToolchainVisitor)
    }
}

pub mod defaults {
    pub use crate::config::defaults::{
        default_disk_cache_size, default_dist_cache_dir, default_true,
    };
}
