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

use crate::config::utils::deserialize_bool;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub mod client;
#[cfg(feature = "dist-server")]
pub mod scheduler;
#[cfg(feature = "dist-server")]
pub mod server;

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

#[cfg(feature = "dist-server")]
pub use dist_server::*;

#[cfg(feature = "dist-server")]
mod dist_server {
    use super::*;
    use serde::de;
    use std::fmt;

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

    #[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
    #[serde(rename_all = "lowercase")]
    pub enum Metrics {
        #[default]
        None,
        Dogstatsd(DogStatsD),
        Prometheus(Prometheus),
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

    #[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
    #[serde(tag = "type", rename_all = "lowercase")]
    pub enum Prometheus {
        #[serde(rename = "bind")]
        ListenAddr {
            addr: Option<std::net::SocketAddr>,
            idle_timeout_secs: Option<u64>,
        },
        #[serde(rename = "path")]
        ListenPath {
            path: Option<String>,
            idle_timeout_secs: Option<u64>,
        },
        #[serde(rename = "push")]
        PushGateway {
            endpoint: String,
            // Interval (in milliseconds) to push metrics to prometheus
            #[serde(
                alias = "interval",
                default = "defaults::default_prometheus_push_gateway_interval"
            )]
            interval_ms: u64,
            username: Option<String>,
            password: Option<String>,
            http_method: Option<String>,
            idle_timeout_secs: Option<u64>,
        },
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
