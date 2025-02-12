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

use std::{
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    config::{
        DogStatsDAggregationMode, DogStatsDMetricsConfig, MetricsConfigs, PrometheusMetricsConfig,
    },
    errors::*,
};
use metrics_exporter_dogstatsd::{AggregationMode, DogStatsDBuilder};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

fn merge_labels(
    global_labels: &[(String, String)],
    local_labels: &[(&str, &str)],
) -> Vec<(String, String)> {
    let mut all_labels = vec![];
    for (k, v) in global_labels.iter() {
        all_labels.push((k.to_owned(), v.to_owned()));
    }
    for &(k, v) in local_labels.iter() {
        all_labels.push((k.to_owned(), v.to_owned()));
    }
    all_labels
}

pub struct IncrementRecorder {
    name: String,
    labels: Vec<(String, String)>,
}

impl Drop for IncrementRecorder {
    fn drop(&mut self) {
        metrics::counter!(self.name.clone(), &self.labels).increment(1);
    }
}

pub struct HistoRecorder {
    name: String,
    value: f64,
    labels: Vec<(String, String)>,
}

impl Drop for HistoRecorder {
    fn drop(&mut self) {
        metrics::histogram!(self.name.clone(), &self.labels).record(self.value);
    }
}

pub struct TimeRecorder {
    name: String,
    start: Instant,
    labels: Vec<(String, String)>,
}

impl Drop for TimeRecorder {
    fn drop(&mut self) {
        metrics::histogram!(self.name.clone(), &self.labels).record(self.start.elapsed());
    }
}

#[derive(Clone)]
pub struct Metrics {
    global_labels: Arc<Vec<(String, String)>>,
    inner: Arc<dyn MetricsInner>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            global_labels: Default::default(),
            inner: Arc::new(NoopMetrics {}),
        }
    }
}

impl Metrics {
    pub fn new(config: MetricsConfigs, global_labels: Vec<(String, String)>) -> Result<Self> {
        if let Some(config) = config.dogstatsd {
            Self::new_dogstatsd(config, global_labels)
        } else if let Some(config) = config.prometheus {
            Self::new_prometheus(config, global_labels)
        } else {
            Ok(Self {
                global_labels: Arc::new(global_labels),
                inner: Arc::new(NoopMetrics {}),
            })
        }
    }

    fn new_dogstatsd(
        config: DogStatsDMetricsConfig,
        global_labels: Vec<(String, String)>,
    ) -> Result<Self> {
        Ok(Self {
            global_labels: Arc::new(global_labels),
            inner: Arc::new(DogStatsDMetrics::new(config)?),
        })
    }

    fn new_prometheus(
        config: PrometheusMetricsConfig,
        global_labels: Vec<(String, String)>,
    ) -> Result<Self> {
        Ok(Self {
            global_labels: Arc::new(vec![]),
            inner: Arc::new(PrometheusMetrics::new(config, global_labels)?),
        })
    }

    pub fn labels(&self, labels: &[(&str, &str)]) -> Vec<(String, String)> {
        self.global_labels
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .chain(labels.iter().map(|&(k, v)| (k.to_owned(), v.to_owned())))
            .collect::<Vec<_>>()
    }

    pub fn render(&self) -> String {
        self.inner.render()
    }

    pub fn listen_path(&self) -> Option<String> {
        self.inner.listen_path()
    }

    pub fn count<'a>(&self, name: &'a str, labels: &'a [(&'a str, &'a str)]) -> IncrementRecorder {
        IncrementRecorder {
            name: name.to_owned(),
            labels: merge_labels(self.global_labels.as_ref(), labels),
        }
    }

    pub fn histo<T: metrics::IntoF64>(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        value: T,
    ) -> HistoRecorder {
        HistoRecorder {
            name: name.to_owned(),
            value: value.into_f64(),
            labels: merge_labels(self.global_labels.as_ref(), labels),
        }
    }

    pub fn timer(&self, name: &str, labels: &[(&str, &str)]) -> TimeRecorder {
        TimeRecorder {
            name: name.to_owned(),
            start: Instant::now(),
            labels: merge_labels(self.global_labels.as_ref(), labels),
        }
    }
}

trait MetricsInner: Send + Sync {
    fn render(&self) -> String;
    fn listen_path(&self) -> Option<String>;
}

struct NoopMetrics {}

impl MetricsInner for NoopMetrics {
    fn render(&self) -> String {
        String::new()
    }
    fn listen_path(&self) -> Option<String> {
        None
    }
}

struct DogStatsDMetrics {}

impl DogStatsDMetrics {
    pub fn new(config: DogStatsDMetricsConfig) -> Result<Self> {
        let mut builder = DogStatsDBuilder::default();

        builder = builder.with_remote_address(config.remote_addr)?;

        if let Some(write_timeout) = config.write_timeout {
            builder = builder.with_write_timeout(Duration::from_millis(write_timeout));
        }
        if let Some(maximum_payload_length) = config.maximum_payload_length {
            builder = builder.with_maximum_payload_length(maximum_payload_length)?;
        }
        if let Some(aggregation_mode) = config.aggregation_mode {
            builder = builder.with_aggregation_mode(match aggregation_mode {
                DogStatsDAggregationMode::Aggressive => AggregationMode::Aggressive,
                DogStatsDAggregationMode::Conservative => AggregationMode::Conservative,
            });
        }
        if let Some(flush_interval) = config.flush_interval {
            builder = builder.with_flush_interval(Duration::from_millis(flush_interval));
        }
        if let Some(telemetry) = config.telemetry {
            builder = builder.with_telemetry(telemetry);
        }
        if let Some(histogram_sampling) = config.histogram_sampling {
            builder = builder.with_histogram_sampling(histogram_sampling);
        }
        if let Some(histogram_reservoir_size) = config.histogram_reservoir_size {
            builder = builder.with_histogram_reservoir_size(histogram_reservoir_size);
        }
        if let Some(histograms_as_distributions) = config.histograms_as_distributions {
            builder = builder.send_histograms_as_distributions(histograms_as_distributions);
        }

        metrics::set_global_recorder(builder.build()?)?;

        Ok(Self {})
    }
}

impl MetricsInner for DogStatsDMetrics {
    fn render(&self) -> String {
        String::new()
    }
    fn listen_path(&self) -> Option<String> {
        None
    }
}

struct PrometheusMetrics {
    inner: PrometheusHandle,
    listen_path: Option<String>,
}

impl PrometheusMetrics {
    pub fn new(
        config: PrometheusMetricsConfig,
        global_labels: Vec<(String, String)>,
    ) -> Result<Self> {
        let builder = global_labels
            .iter()
            .fold(PrometheusBuilder::new(), |builder, (key, val)| {
                builder.add_global_label(key, val)
            });

        let (recorder, exporter, listen_path) = match config {
            PrometheusMetricsConfig::ListenAddr { ref addr } => {
                let addr = addr.unwrap_or(SocketAddr::from_str("0.0.0.0:9000")?);
                tracing::info!("Listening for metrics at {addr}");
                let (recorder, exporter) = builder.with_http_listener(addr).build()?;
                (recorder, exporter, None)
            }
            PrometheusMetricsConfig::ListenPath { ref path } => {
                let path = path.clone().unwrap_or("/metrics".to_owned());
                tracing::info!("Listening for metrics at {path}");
                let (recorder, exporter) = builder.build()?;
                (recorder, exporter, Some(path))
            }
            PrometheusMetricsConfig::PushGateway {
                ref endpoint,
                ref interval,
                ref username,
                ref password,
            } => {
                let interval = Duration::from_millis(interval.unwrap_or(10_000));
                tracing::info!(
                    "Pushing metrics to {endpoint} every {}s",
                    interval.as_secs_f64()
                );
                let (recorder, exporter) = builder
                    .set_bucket_count(std::num::NonZeroU32::new(3).unwrap())
                    .with_push_gateway(endpoint, interval, username.clone(), password.clone())?
                    .build()?;
                (recorder, exporter, None)
            }
        };

        let handle = recorder.handle();

        metrics::set_global_recorder(recorder)?;

        tokio::spawn(exporter);

        Ok(Self {
            inner: handle,
            listen_path,
        })
    }
}

impl MetricsInner for PrometheusMetrics {
    fn render(&self) -> String {
        self.inner.render()
    }
    fn listen_path(&self) -> Option<String> {
        self.listen_path.clone()
    }
}
