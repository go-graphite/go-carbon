//! Go-compatible application collectors; no Go-runtime measurements are fabricated.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts,
    Registry, TextEncoder, exponential_buckets,
};

use crate::{app::App, config::Config};

pub struct Metrics {
    pub registry: Registry,
    pub tcp_received: Option<IntCounter>,
    pub write_lag: Histogram,
    pub carbonserver: Option<ServerMetrics>,
}

pub struct ServerMetrics {
    pub requests: IntCounterVec,
    pub cache_requests: IntCounterVec,
    pub durations: Histogram,
    pub cache_durations: HistogramVec,
    pub disk_requests: IntCounter,
    pub cancelled_requests: IntCounter,
    pub timeout_requests: IntCounter,
    pub disk_wait: Histogram,
    pub returned_metrics: IntCounter,
    pub returned_points: IntCounter,
}

impl Metrics {
    pub fn new(config: &Config) -> prometheus::Result<Self> {
        config
            .prometheus
            .validate(config.carbonserver.enabled)
            .map_err(prometheus::Error::Msg)?;
        let registry = Registry::new_custom(None, Some(config.prometheus.labels.clone()))?;
        let tcp_received = if config.tcp.enabled {
            let counter = IntCounter::new(
                "metrics_received_tcp_total",
                "Counter of metrics received via the TCP endpoint.",
            )?;
            registry.register(Box::new(counter.clone()))?;
            Some(counter)
        } else {
            None
        };
        let write_lag = Histogram::with_opts(
            HistogramOpts::new(
                "out_of_order_write_lag_exp",
                "Lag for incoming datapoints (exponential buckets)",
            )
            .buckets(exponential_buckets(0.001, 2.0, 30)?),
        )?;
        registry.register(Box::new(write_lag.clone()))?;
        let carbonserver = if config.carbonserver.enabled {
            Some(ServerMetrics::new(&registry)?)
        } else {
            None
        };
        let build = IntGauge::with_opts(
            Opts::new("carbon_rs_build_info", "Build information for carbon-rs.")
                .const_label("version", env!("CARGO_PKG_VERSION")),
        )?;
        build.set(1);
        registry.register(Box::new(build))?;
        #[cfg(target_os = "linux")]
        {
            registry.register(Box::new(
                prometheus::process_collector::ProcessCollector::for_self(),
            ))?;
            registry.register(Box::new(crate::linux_process::ExtraProcessMetrics::new()?))?;
        }
        Ok(Self {
            registry,
            tcp_received,
            write_lag,
            carbonserver,
        })
    }
}

impl ServerMetrics {
    fn new(registry: &Registry) -> prometheus::Result<Self> {
        let counter = |name, help| -> prometheus::Result<IntCounter> {
            let metric = IntCounter::new(name, help)?;
            registry.register(Box::new(metric.clone()))?;
            Ok(metric)
        };
        let histogram = |name, help| -> prometheus::Result<Histogram> {
            let metric = Histogram::with_opts(
                HistogramOpts::new(name, help).buckets(exponential_buckets(0.001, 2.0, 20)?),
            )?;
            registry.register(Box::new(metric.clone()))?;
            Ok(metric)
        };
        let requests = IntCounterVec::new(
            Opts::new(
                "http_requests_total",
                "How many HTTP requests processed, partitioned by status code and handler",
            ),
            &["code", "handler"],
        )?;
        let cache_requests = IntCounterVec::new(
            Opts::new(
                "cache_requests_total",
                "Cache counts, partitioned by type and hit/miss",
            ),
            &["type", "hit"],
        )?;
        let cache_durations = HistogramVec::new(
            HistogramOpts::new(
                "cache_duration_seconds_exp",
                "Time spent in cache (exponential buckets)",
            )
            .buckets(exponential_buckets(0.001, 2.0, 20)?),
            &["type"],
        )?;
        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(cache_requests.clone()))?;
        registry.register(Box::new(cache_durations.clone()))?;
        Ok(Self {
            requests,
            cache_requests,
            cache_durations,
            durations: histogram(
                "http_request_duration_seconds_exp",
                "Duration of HTTP requests (exponential buckets)",
            )?,
            disk_requests: counter("disk_requests_total", "Number of times disk has been hit")?,
            cancelled_requests: counter(
                "cancelled_requests_total",
                "Number of times a request has been cancelled",
            )?,
            timeout_requests: counter(
                "timeout_requests_total",
                "Number of times a request has been timeout",
            )?,
            disk_wait: histogram(
                "disk_wait_seconds_exp",
                "Duration of disk wait times (exponential buckets)",
            )?,
            returned_metrics: counter("returned_metrics_total", "Number of metrics returned")?,
            returned_points: counter("returned_points_total", "Number of points returned")?,
        })
    }

    pub fn cache_request(&self, kind: &str, hit: bool) {
        self.cache_requests
            .with_label_values(&[kind, if hit { "true" } else { "false" }])
            .inc();
    }

    pub(crate) fn request<'a>(&'a self, handler: &'a str) -> RequestMetrics<'a> {
        RequestMetrics {
            metrics: self,
            handler,
            start: Instant::now(),
            completed: false,
        }
    }
}

pub(crate) struct RequestMetrics<'a> {
    metrics: &'a ServerMetrics,
    handler: &'a str,
    start: Instant,
    completed: bool,
}
impl RequestMetrics<'_> {
    pub(crate) fn finish(mut self, status: StatusCode) {
        self.completed = true;
        self.metrics
            .requests
            .with_label_values(&[status.as_str(), self.handler])
            .inc();
    }
}
impl Drop for RequestMetrics<'_> {
    fn drop(&mut self) {
        self.metrics
            .durations
            .observe(self.start.elapsed().as_secs_f64());
        if !self.completed {
            self.metrics.cancelled_requests.inc();
        }
    }
}

/// Go exposes Prometheus on pprof.listen, independently of carbonserver.
pub fn router(app: Arc<App>) -> Router {
    match &app.prometheus {
        Some(metrics) => Router::new()
            .route(&app.config.prometheus.endpoint, get(scrape))
            .with_state(metrics.clone()),
        None => Router::new(),
    }
}

async fn scrape(State(metrics): State<Arc<Metrics>>) -> Response {
    let encoder = TextEncoder::new();
    let mut output = Vec::new();
    match encoder.encode(&metrics.registry.gather(), &mut output) {
        Ok(()) => ([(header::CONTENT_TYPE, encoder.format_type())], output).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}
