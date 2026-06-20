// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The `/metrics` endpoint serves registered counters in the Prometheus text
//! exposition format.

use common::metrics::serve_metrics;
use prometheus::register_int_counter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn metrics_endpoint_serves_prometheus_text() {
    let counter = register_int_counter!("aethel_test_counter_total", "a test counter").unwrap();
    counter.inc_by(7);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_metrics(listener));

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);

    assert!(text.contains("200 OK"), "should be an HTTP 200 response");
    assert!(text.contains("text/plain"), "Prometheus exposition content type");
    assert!(
        text.contains("aethel_test_counter_total 7"),
        "metric value should be exposed; got:\n{text}"
    );
}
