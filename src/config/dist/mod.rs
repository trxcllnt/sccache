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

use crate::config::utils::{_Ignored, deserialize_bool};

pub mod client;
pub mod scheduler;
pub mod server;

use serde::{
    Deserialize, Serialize, de,
    ser::{self, SerializeMap},
};
use std::{collections::HashSet, fmt, time::Duration};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Keepalive {
    #[serde(default, deserialize_with = "deserialize_bool")]
    pub enabled: bool,
    pub timeout: u64,
    pub interval: u64,
}

impl Default for Keepalive {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: 20,
            timeout: 600,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MessageBroker {
    AMQP(MessageBrokerAddr),
    Redis(MessageBrokerAddr),
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct MessageBrokerAddr {
    pub addr: String,
}

impl<'de> Deserialize<'de> for MessageBrokerAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct MessageBrokerAddrVisitor;

        impl<'de> de::Visitor<'de> for MessageBrokerAddrVisitor {
            type Value = MessageBrokerAddr;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("sccache-dist message broker address")
            }

            fn visit_str<E>(self, addr: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(MessageBrokerAddr {
                    addr: addr.to_owned(),
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut addr = None;

                while let Some(name) = map.next_key::<String>()? {
                    match name.as_str() {
                        "addr" => {
                            addr = Some(map.next_value()?);
                        }
                        name => {
                            return Err(de::Error::unknown_field(name, &["addr"]));
                        }
                    }
                }

                if let Some(addr) = addr {
                    Ok(MessageBrokerAddr { addr })
                } else {
                    Err(de::Error::missing_field("addr"))
                }
            }
        }

        deserializer.deserialize_map(MessageBrokerAddrVisitor)
    }
}

impl From<MessageBrokerAddr> for String {
    fn from(MessageBrokerAddr { addr }: MessageBrokerAddr) -> Self {
        addr
    }
}

impl From<String> for MessageBrokerAddr {
    fn from(addr: String) -> Self {
        Self { addr }
    }
}

impl<'a> From<&'a str> for MessageBrokerAddr {
    fn from(addr: &'a str) -> Self {
        Self { addr: addr.into() }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Metrics {
    #[default]
    None,
    Dogstatsd(DogStatsD),
    Prometheus(Prometheus),
}

impl Serialize for Metrics {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            Metrics::None => serializer.serialize_str("none"),
            Metrics::Dogstatsd(dogstatsd) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("dogstatsd", dogstatsd)?;
                map.end()
            }
            Metrics::Prometheus(prometheus) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("prometheus", prometheus)?;
                map.end()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DogStatsD {
    #[serde(alias = "remote_addr")]
    pub addr: String,

    // Write timeout (in milliseconds) for forwarding metrics to dogstatsd
    #[serde(default)]
    pub write_timeout_ms: Option<u64>,

    // Maximum payload length (in bytes) for forwarding metrics
    #[serde(default, alias = "maximum_payload_length")]
    pub maximum_payload_length_bytes: Option<usize>,

    // The aggregation mode for the exporter
    #[serde(default)]
    pub aggregation_mode: Option<DogStatsDAggregationMode>,

    // The flush interval of the aggregator
    #[serde(default, alias = "flush_interval")]
    pub flush_interval_ms: Option<u64>,

    // Whether or not to enable telemetry for the exporter
    #[serde(default)]
    pub telemetry: Option<bool>,

    // Whether or not to enable histogram sampling
    #[serde(default)]
    pub histogram_sampling: Option<bool>,

    // The reservoir size (in bytes) for histogram sampling
    #[serde(default, alias = "histogram_reservoir_size")]
    pub histogram_reservoir_size_bytes: Option<usize>,

    // Whether or not to send histograms as distributions
    #[serde(default)]
    pub histograms_as_distributions: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DogStatsDAggregationMode {
    Aggressive,
    Conservative,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Prometheus {
    // #[serde(rename = "bind")]
    ListenAddr {
        addr: Option<std::net::SocketAddr>,
        idle_timeout_secs: Option<u64>,
    },
    // #[serde(rename = "path")]
    ListenPath {
        path: Option<String>,
        idle_timeout_secs: Option<u64>,
    },
    // #[serde(rename = "push")]
    PushGateway {
        endpoint: String,
        // Interval (in milliseconds) to push metrics to prometheus
        // #[serde(default = "defaults::default_prometheus_push_gateway_interval")]
        interval_ms: u64,
        username: Option<String>,
        password: Option<String>,
        http_method: Option<String>,
        idle_timeout_secs: Option<u64>,
    },
}

impl Serialize for Prometheus {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            Prometheus::ListenAddr {
                addr,
                idle_timeout_secs,
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "bind")?;
                map.serialize_entry("addr", addr)?;
                map.serialize_entry("idle_timeout_secs", idle_timeout_secs)?;
                map.end()
            }
            Prometheus::ListenPath {
                path,
                idle_timeout_secs,
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "path")?;
                map.serialize_entry("path", path)?;
                map.serialize_entry("idle_timeout_secs", idle_timeout_secs)?;
                map.end()
            }
            Prometheus::PushGateway {
                endpoint,
                interval_ms,
                username,
                password,
                http_method,
                idle_timeout_secs,
            } => {
                let mut map = serializer.serialize_map(Some(7))?;
                map.serialize_entry("type", "push")?;
                map.serialize_entry("endpoint", endpoint)?;
                map.serialize_entry("interval_ms", interval_ms)?;
                map.serialize_entry("username", username)?;
                map.serialize_entry("password", password)?;
                map.serialize_entry("http_method", http_method)?;
                map.serialize_entry("idle_timeout_secs", idle_timeout_secs)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Prometheus {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct PrometheusVisitor;

        impl<'de> de::Visitor<'de> for PrometheusVisitor {
            type Value = Prometheus;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("sccache-dist metrics prometheus configuration")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                let mut type_ = None;
                let mut addr = None;
                let mut idle_timeout_secs = None;
                let mut path = None;
                let mut endpoint = None;
                let mut interval_ms = None;
                let mut username = None;
                let mut password = None;
                let mut http_method = None;

                let to_ignore = [
                    "type",
                    "addr",
                    "idle_timeout_secs",
                    "path",
                    "endpoint",
                    "interval_ms",
                    "username",
                    "password",
                    "http_method",
                ]
                .into_iter()
                .map(|name| name.split_once('_').map(|(p, _)| p).unwrap_or(name))
                .collect::<HashSet<_>>();

                while let Ok(Some(name)) = map.next_key::<String>() {
                    match name.as_str() {
                        "type" => {
                            type_ = Some(map.next_value::<String>()?);
                        }
                        "addr" | "listen_addr" => {
                            addr = map.next_value()?;
                        }
                        "idle_timeout" => {
                            idle_timeout_secs = map.next_value::<u64>().ok();
                        }
                        "idle_timeout_secs" => {
                            idle_timeout_secs = map.next_value()?;
                        }
                        "path" | "listen_path" => {
                            path = map.next_value()?;
                        }
                        "endpoint" => {
                            endpoint = map.next_value()?;
                        }
                        "interval_ms" => {
                            interval_ms = map.next_value()?;
                        }
                        "username" => {
                            username = map.next_value()?;
                        }
                        "password" => {
                            password = map.next_value()?;
                        }
                        "http_method" => {
                            http_method = map.next_value()?;
                        }
                        name => {
                            if to_ignore.iter().any(|prefix| name.starts_with(prefix)) {
                                let _ = map.next_value::<_Ignored>();
                            } else {
                                return Err(de::Error::unknown_field(
                                    name,
                                    &[
                                        "type",
                                        "addr",
                                        "idle_timeout_secs",
                                        "path",
                                        "endpoint",
                                        "interval_ms",
                                        "username",
                                        "password",
                                        "http_method",
                                    ],
                                ));
                            }
                        }
                    }
                }

                let prom = match type_.as_deref().unwrap_or("none") {
                    "bind" if let Some(addr) = addr => Prometheus::ListenAddr {
                        addr,
                        idle_timeout_secs,
                    },
                    "path" if let Some(path) = path => Prometheus::ListenPath {
                        path,
                        idle_timeout_secs,
                    },
                    "push" if let Some(endpoint) = endpoint => Prometheus::PushGateway {
                        endpoint,
                        interval_ms: interval_ms
                            .unwrap_or_else(defaults::default_prometheus_push_gateway_interval),
                        username,
                        password,
                        http_method,
                        idle_timeout_secs,
                    },
                    type_ => {
                        return Err(de::Error::unknown_variant(type_, &["bind", "path", "push"]));
                    }
                };

                Ok(prom)
            }
        }

        deserializer.deserialize_map(PrometheusVisitor)
    }
}

pub mod defaults {
    use super::*;

    // Default to 15s
    pub fn default_heartbeat_interval() -> u64 {
        Duration::from_secs(1500).as_secs()
    }

    pub fn default_shutdown_timeout() -> u64 {
        Duration::from_secs(10).as_secs()
    }

    pub fn default_prometheus_push_gateway_interval() -> u64 {
        Duration::from_secs(10).as_secs()
    }
}
