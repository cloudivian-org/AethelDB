// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! A Kubernetes-native [`Activator`](crate::activator::Activator).
//!
//! Each tenant's compute is a Kubernetes **Deployment** (e.g. `compute-<tenant>`).
//! Scale-to-zero is then just scaling that Deployment: `start` patches its scale
//! subresource to one replica, `stop` to zero. The proxy connects to the
//! tenant's `Service`, which load-balances to the (now-running) Pod.
//!
//! Gated behind the `kubernetes` cargo feature so the default build stays light.
//! The proxy needs RBAC to `patch` `deployments` and `deployments/scale` in its
//! namespace — see `deploy/k8s/proxy-rbac.yaml`.

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Config};
use tracing::info;

use crate::activator::Activator;

/// Render a compute Deployment name for `tenant` from a template containing the
/// literal token `{tenant}`.
pub fn render_name(template: &str, tenant: &str) -> String {
    template.replace("{tenant}", tenant)
}

/// Scales a per-tenant compute Deployment via the Kubernetes API.
pub struct KubeActivator {
    client: Client,
    namespace: String,
    name_template: String,
}

impl KubeActivator {
    /// Build from the ambient cluster config — the in-cluster service account
    /// when running in a Pod, or the local kubeconfig in development.
    pub async fn try_default(
        namespace: impl Into<String>,
        name_template: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let client = Client::try_default().await?;
        Ok(KubeActivator { client, namespace: namespace.into(), name_template: name_template.into() })
    }

    /// Build against an explicit cluster config (used by tests pointing at a
    /// mock API server).
    pub fn from_config(
        config: Config,
        namespace: impl Into<String>,
        name_template: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let client = Client::try_from(config)?;
        Ok(KubeActivator { client, namespace: namespace.into(), name_template: name_template.into() })
    }

    async fn scale(&self, tenant: &str, replicas: i32) -> anyhow::Result<()> {
        let name = render_name(&self.name_template, tenant);
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
        let patch = serde_json::json!({ "spec": { "replicas": replicas } });
        api.patch_scale(&name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .map_err(|e| anyhow::anyhow!("scaling deployment {name}: {e}"))?;
        info!(tenant, deployment = %name, replicas, "scaled compute Deployment");
        Ok(())
    }
}

#[async_trait]
impl Activator for KubeActivator {
    async fn start(&self, tenant: &str) -> anyhow::Result<()> {
        self.scale(tenant, 1).await
    }

    async fn stop(&self, tenant: &str) -> anyhow::Result<()> {
        self.scale(tenant, 0).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn render_name_substitutes_tenant() {
        assert_eq!(render_name("compute-{tenant}", "shop"), "compute-shop");
        assert_eq!(render_name("pg-{tenant}-svc", "acme"), "pg-acme-svc");
        assert_eq!(render_name("fixed", "x"), "fixed");
    }

    type Captured = Arc<Mutex<Vec<(String, String, String)>>>;

    /// A mock Kubernetes API server that records every request and answers each
    /// with a canned `Scale` object (whatever sequence the client uses).
    async fn mock_api() -> (std::net::SocketAddr, Captured) {
        let captured: Captured = Arc::new(Mutex::new(Vec::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cap = captured.clone();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(p) => p,
                    Err(_) => return,
                };
                let cap = cap.clone();
                tokio::spawn(async move {
                    // One request per connection is enough for our purposes.
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 1024];
                    let header_end = loop {
                        let n = match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break p;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let mut lines = head.split("\r\n");
                    let req_line = lines.next().unwrap_or("");
                    let mut parts = req_line.split_whitespace();
                    let method = parts.next().unwrap_or("").to_string();
                    let path = parts.next().unwrap_or("").to_string();
                    let clen: usize = lines
                        .clone()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            if k.trim().eq_ignore_ascii_case("content-length") {
                                v.trim().parse().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0);
                    let mut body = buf[header_end + 4..].to_vec();
                    while body.len() < clen {
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => body.extend_from_slice(&chunk[..n]),
                        }
                    }
                    cap.lock().unwrap().push((
                        method,
                        path,
                        String::from_utf8_lossy(&body).to_string(),
                    ));

                    let scale = r#"{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"compute-shop","namespace":"aetheldb"},"spec":{"replicas":1},"status":{"replicas":1}}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        scale.len(),
                        scale
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });
        (addr, captured)
    }

    /// End-to-end against a **real** cluster. Skipped unless `AETHEL_K8S_TEST` is
    /// set; `deploy/k8s/verify-activator.sh` provisions a kind cluster + a
    /// `compute-shop` Deployment and runs it. Verifies start/stop actually scale
    /// the Deployment via the live API.
    #[tokio::test]
    async fn scales_a_real_deployment() {
        if std::env::var("AETHEL_K8S_TEST").is_err() {
            eprintln!("skipping real-cluster test: set AETHEL_K8S_TEST (see deploy/k8s/verify-activator.sh)");
            return;
        }
        let ns = "aetheldb";
        let activator = KubeActivator::try_default(ns, "compute-{tenant}").await.unwrap();
        let api: Api<Deployment> =
            Api::namespaced(Client::try_default().await.unwrap(), ns);

        let replicas = |d: &Deployment| d.spec.as_ref().and_then(|s| s.replicas);

        activator.start("shop").await.expect("start should scale up");
        assert_eq!(replicas(&api.get("compute-shop").await.unwrap()), Some(1), "started -> 1 replica");

        activator.stop("shop").await.expect("stop should scale down");
        assert_eq!(replicas(&api.get("compute-shop").await.unwrap()), Some(0), "stopped -> 0 replicas");
    }

    #[tokio::test]
    async fn start_scales_the_deployment_to_one() {
        let (addr, captured) = mock_api().await;
        let config = Config::new(format!("http://{addr}").parse().unwrap());
        let activator = KubeActivator::from_config(config, "aetheldb", "compute-{tenant}").unwrap();

        activator.start("shop").await.expect("start should scale up");

        // Find the PATCH to the deployment's scale subresource (the path may
        // carry a trailing empty query string).
        let reqs = captured.lock().unwrap().clone();
        let patch = reqs
            .iter()
            .find(|(m, p, _)| m == "PATCH" && p.contains("/apis/apps/v1/namespaces/aetheldb/deployments/compute-shop/scale"))
            .unwrap_or_else(|| panic!("no scale PATCH; saw: {reqs:?}"));
        assert!(patch.2.contains("\"replicas\":1"), "body scales to 1: {}", patch.2);
    }
}
