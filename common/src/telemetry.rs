// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Process-wide tracing initialization shared by every binary.
//!
//! Always installs a formatting subscriber honoring `RUST_LOG` (default `info`).
//! When built with the **`otlp`** feature *and* `OTEL_EXPORTER_OTLP_ENDPOINT` is
//! set, it additionally exports spans over OTLP to a collector (Tempo / Jaeger /
//! the OpenTelemetry Collector), tagged with `service.name`. The feature is off
//! by default, so the standard build pulls none of the OpenTelemetry
//! dependencies and behaves exactly as a plain `fmt` subscriber.
//!
//! See `docs/design/observability.md`.

use tracing_subscriber::EnvFilter;

fn default_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Initialize tracing for `service_name`. Call once, early in `main`.
pub fn init(service_name: &str) {
    #[cfg(feature = "otlp")]
    {
        if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            match otlp::init(service_name, &endpoint) {
                Ok(()) => return,
                Err(e) => eprintln!("otlp init failed ({e:#}); using fmt-only logging"),
            }
        }
    }
    let _ = service_name;
    init_fmt_only();
}

/// Install the plain formatting subscriber (the default everywhere).
fn init_fmt_only() {
    tracing_subscriber::fmt().with_env_filter(default_filter()).with_target(false).init();
}

#[cfg(feature = "otlp")]
mod otlp {
    use super::default_filter;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::{trace as sdktrace, Resource};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    /// Build an OTLP span exporter + a `tracing` layer and install the
    /// subscriber (fmt + OpenTelemetry).
    pub fn init(service_name: &str, endpoint: &str) -> anyhow::Result<()> {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()?;

        let provider = sdktrace::TracerProvider::builder()
            .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                service_name.to_string(),
            )]))
            .build();

        let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "aetheldb");
        opentelemetry::global::set_tracer_provider(provider);

        tracing_subscriber::registry()
            .with(default_filter())
            .with(tracing_subscriber::fmt::layer().with_target(false))
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .init();
        Ok(())
    }
}
