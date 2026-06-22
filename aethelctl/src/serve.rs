// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The `aethelctl serve` web console.
//!
//! A small blocking HTTP server (`tiny_http`) that serves an embedded
//! single-page app and a JSON API. The API **proxies the control plane** (so the
//! browser never needs the token), renders **deploy dry-runs**, and — only when
//! started with `--allow-apply` — **streams a real `helm` apply**. It runs on
//! localhost by default and acts with the operator's own `kubectl` context, like
//! `helm` or `k9s`.

use std::process::{Command, Stdio};

use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use tiny_http::{Header, Method, Response, Server, StatusCode};

use crate::deploy::{self, DeployOpts};
use crate::{databases, Client};

/// The embedded console SPA.
const INDEX_HTML: &str = include_str!("../console/index.html");

type Reply = (u16, String, &'static str);
const JSON: &str = "application/json";

/// Runtime configuration for the console.
pub struct ServeCfg {
    pub control_url: String,
    /// Allow the GUI to run a *real* `helm` apply (not just a dry-run preview).
    pub allow_apply: bool,
    /// Optional Grafana base URL to embed metrics panels in the Overview.
    pub grafana_url: Option<String>,
    /// The client-facing endpoint (proxy `host:port`) shown in connection strings.
    pub client_endpoint: String,
}

/// Serve the console on `listen`, proxying control-plane calls to the page server.
pub fn serve(listen: &str, cfg: ServeCfg, token: Option<String>) -> Result<()> {
    let server = Server::http(listen).map_err(|e| anyhow!("binding {listen}: {e}"))?;
    let client = Client::new(cfg.control_url.clone(), token);
    println!("AethelDB console → http://{listen}");
    println!("  control plane: {}", cfg.control_url);
    println!("  apply enabled: {}", cfg.allow_apply);

    for mut req in server.incoming_requests() {
        let method = req.method().clone();
        let url = req.url().to_string();
        let path = url.split('?').next().unwrap_or("").to_string();

        // The real apply streams helm's output as it runs — handled specially.
        if method == Method::Post && path == "/api/deploy/apply" {
            let body = read_body(&mut req);
            stream_apply(req, &body, cfg.allow_apply);
            continue;
        }

        let body = read_body(&mut req);
        let (status, payload, ctype) = route(&method, &url, &path, &body, &client, &cfg);
        let header = Header::from_bytes(&b"Content-Type"[..], ctype.as_bytes()).unwrap();
        let _ = req
            .respond(Response::from_string(payload).with_status_code(status).with_header(header));
    }
    Ok(())
}

fn read_body(req: &mut tiny_http::Request) -> String {
    let mut s = String::new();
    let _ = std::io::Read::read_to_string(req.as_reader(), &mut s);
    s
}

fn route(
    method: &Method,
    url: &str,
    path: &str,
    body: &str,
    client: &Client,
    cfg: &ServeCfg,
) -> Reply {
    match (method, path) {
        (Method::Get, "/") | (Method::Get, "/index.html") => {
            (200, INDEX_HTML.to_string(), "text/html; charset=utf-8")
        }
        (Method::Get, "/api/status") => api(status(client, cfg)),
        (Method::Get, "/api/databases") => api(list_databases(client, cfg)),
        (Method::Post, "/api/databases") => api(create_database(client, cfg, body)),
        (Method::Get, "/api/tenants") => {
            api(client.list_tenants().map(|t| json!({ "tenants": t })))
        }
        (Method::Post, "/api/tenants") => api(do_create_tenant(client, body)),
        (Method::Get, "/api/timelines") => {
            let tenant = query_param(url, "tenant");
            api(client.list_timelines(tenant.as_deref()).map(|t| json!({ "timelines": t })))
        }
        (Method::Post, "/api/timelines") => api(do_create_timeline(client, body)),
        (Method::Post, "/api/branches") => api(do_branch(client, body)),
        (Method::Post, "/api/gc") => api(do_gc(client, body)),
        (Method::Post, "/api/deploy/preview") => api(deploy_preview(body)),
        (Method::Post, "/api/deploy/command") => api(deploy_command(body)),
        _ => (404, json!({ "error": "not found" }).to_string(), JSON),
    }
}

/// Turn a `Result<Value>` into an HTTP reply (200 or 400 with `{error}`).
fn api(r: Result<Value>) -> Reply {
    match r {
        Ok(v) => (200, v.to_string(), JSON),
        Err(e) => (400, json!({ "error": format!("{e:#}") }).to_string(), JSON),
    }
}

fn status(client: &Client, cfg: &ServeCfg) -> Result<Value> {
    client.healthz()?;
    Ok(json!({
        "server": cfg.control_url,
        "databases": databases::load().len(),
        "allowApply": cfg.allow_apply,
        "grafanaUrl": cfg.grafana_url,
        "clientEndpoint": cfg.client_endpoint,
    }))
}

const ROOT_TIMELINE: &str = "00000000000000000000000000000000";

/// Provision a database by name: derive its tenant, create it + a root timeline
/// on the control plane (idempotent), and record the name locally.
fn create_database(client: &Client, cfg: &ServeCfg, body: &str) -> Result<Value> {
    let name = field(&parse(body), "name")?.to_string();
    let db = databases::upsert(&name)?;
    ignore_conflict(client.create_tenant(&db.id))?;
    ignore_conflict(client.create_timeline(ROOT_TIMELINE, Some(&db.id)))?;
    Ok(json!({
        "name": db.name,
        "id": db.id,
        "connection": databases::connection_string(&db.name, &cfg.client_endpoint),
        "status": "active",
    }))
}

/// List provisioned databases, tagging each with a live status from the control
/// plane (active when its tenant exists, else pending).
fn list_databases(client: &Client, cfg: &ServeCfg) -> Result<Value> {
    let live = client.list_tenants().unwrap_or_default();
    let dbs: Vec<Value> = databases::load()
        .into_iter()
        .map(|d| {
            let status = if live.contains(&d.id) { "active" } else { "pending" };
            json!({
                "name": d.name,
                "id": d.id,
                "connection": databases::connection_string(&d.name, &cfg.client_endpoint),
                "status": status,
            })
        })
        .collect();
    Ok(json!({ "databases": dbs, "endpoint": cfg.client_endpoint }))
}

/// Treat an "already exists" (HTTP 409) as success — provisioning is idempotent.
fn ignore_conflict(r: Result<Value>) -> Result<()> {
    match r {
        Ok(_) => Ok(()),
        Err(e) => {
            let m = format!("{e:#}");
            if m.contains("409") || m.to_lowercase().contains("exists") {
                Ok(())
            } else {
                Err(e)
            }
        }
    }
}

fn do_create_tenant(client: &Client, body: &str) -> Result<Value> {
    client.create_tenant(field(&parse(body), "id")?)
}
fn do_create_timeline(client: &Client, body: &str) -> Result<Value> {
    let b = parse(body);
    client.create_timeline(field(&b, "id")?, opt(&b, "tenant"))
}
fn do_branch(client: &Client, body: &str) -> Result<Value> {
    let b = parse(body);
    let lsn = b.get("lsn").and_then(|v| v.as_u64()).unwrap_or(0);
    client.branch(field(&b, "timeline")?, field(&b, "parent")?, lsn, opt(&b, "tenant"))
}
fn do_gc(client: &Client, body: &str) -> Result<Value> {
    let b = parse(body);
    let horizon = b.get("horizon_lsn").and_then(|v| v.as_u64()).unwrap_or(0);
    client.gc(horizon, opt(&b, "tenant"))
}

fn deploy_preview(body: &str) -> Result<Value> {
    let opts = deploy_opts(&parse(body), true);
    let output = deploy::deploy_capture(&opts, None)?;
    Ok(json!({ "ok": true, "command": deploy::command_preview(&opts), "output": output }))
}
fn deploy_command(body: &str) -> Result<Value> {
    let opts = deploy_opts(&parse(body), false);
    Ok(json!({ "command": deploy::command_preview(&opts) }))
}

/// Stream a real `helm upgrade --install` to the client as it runs. Gated on
/// `--allow-apply` so a console started read-only can never mutate a cluster.
fn stream_apply(req: tiny_http::Request, body: &str, allow_apply: bool) {
    if !allow_apply {
        let payload =
            json!({ "error": "apply is disabled; restart `aethelctl serve` with --allow-apply" })
                .to_string();
        let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
        let _ =
            req.respond(Response::from_string(payload).with_status_code(403).with_header(header));
        return;
    }

    let opts = deploy_opts(&parse(body), false);
    let chart = match deploy::extract_chart_dir() {
        Ok(c) => c,
        Err(e) => {
            let _ =
                req.respond(Response::from_string(format!("error: {e:#}")).with_status_code(500));
            return;
        }
    };
    // Merge stderr into stdout via a shell so the browser sees one stream.
    let cmd = format!("helm {} 2>&1", deploy::helm_args(&opts, &chart).join(" "));
    let child = Command::new("sh").arg("-c").arg(&cmd).stdout(Stdio::piped()).spawn();
    let mut child = match child {
        Ok(c) => c,
        Err(e) => {
            let _ = req.respond(Response::from_string(format!("failed to run helm: {e}")));
            return;
        }
    };
    let stdout = child.stdout.take().unwrap();
    let header =
        Header::from_bytes(&b"Content-Type"[..], &b"text/plain; charset=utf-8"[..]).unwrap();
    // data_length = None → chunked: tiny_http streams stdout as helm produces it.
    let resp = Response::new(StatusCode(200), vec![header], stdout, None, None);
    let _ = req.respond(resp);
    let _ = child.wait(); // reap once the stream (and helm) finish
}

/// Map the console's deploy form to `DeployOpts` + chart `--set` overrides.
fn deploy_opts(b: &Value, dry_run: bool) -> DeployOpts {
    let s =
        |k: &str| b.get(k).and_then(|v| v.as_str()).filter(|s| !s.is_empty()).map(str::to_owned);
    let flag = |k: &str| b.get(k).and_then(|v| v.as_bool()).unwrap_or(false);
    let num = |k: &str, d: i64| b.get(k).and_then(|v| v.as_i64()).unwrap_or(d);
    let cloud = s("cloud").unwrap_or_default();

    let sets = vec![
        format!("safekeeper.replicas={}", num("safekeeperReplicas", 1)),
        format!("autoscaling.proxy.enabled={}", flag("autoscaling")),
        format!("autoscaling.proxy.minReplicas={}", num("autoMin", 2)),
        format!("autoscaling.proxy.maxReplicas={}", num("autoMax", 10)),
        format!("podDisruptionBudget.safekeeper.enabled={}", flag("pdb")),
        format!("podDisruptionBudget.proxy.enabled={}", flag("pdb")),
        format!("topologySpread.enabled={}", flag("spread")),
        format!("pooling.enabled={}", flag("pooling")),
    ];

    DeployOpts {
        release: s("release").unwrap_or_else(|| "aethel".into()),
        namespace: s("namespace").unwrap_or_else(|| "aethel".into()),
        values_files: vec![],
        sets,
        object_store_url: s("objectStoreUrl"),
        image_repo: s("imageRepo"),
        image_tag: s("imageTag"),
        expose: flag("expose") || !cloud.is_empty(),
        wait: flag("wait"),
        dry_run,
    }
}

// ---- small helpers ----
fn parse(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or(Value::Null)
}
fn field<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key).and_then(|x| x.as_str()).ok_or_else(|| anyhow!("missing field `{key}`"))
}
fn opt<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(|x| x.as_str()).filter(|s| !s.is_empty())
}
fn query_param(url: &str, key: &str) -> Option<String> {
    url.split_once('?')?.1.split('&').find_map(|kv| {
        let (k, val) = kv.split_once('=')?;
        (k == key).then(|| val.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deploy_opts_map_cloud_and_toggles() {
        let body = json!({
            "cloud": "aws", "namespace": "ns", "release": "r",
            "objectStoreUrl": "s3://b", "imageRepo": "me/x", "imageTag": "v1",
            "safekeeperReplicas": 3, "autoscaling": true, "autoMin": 2, "autoMax": 9,
            "pdb": true, "spread": true, "pooling": false, "expose": false
        });
        let o = deploy_opts(&body, true);
        assert_eq!(o.namespace, "ns");
        assert_eq!(o.object_store_url.as_deref(), Some("s3://b"));
        assert!(o.expose, "a cloud target should expose the proxy");
        assert!(o.dry_run);
        let joined = o.sets.join(" ");
        assert!(joined.contains("safekeeper.replicas=3"));
        assert!(joined.contains("autoscaling.proxy.enabled=true"));
        assert!(joined.contains("autoscaling.proxy.maxReplicas=9"));
        assert!(joined.contains("topologySpread.enabled=true"));
    }

    #[test]
    fn query_param_extracts_tenant() {
        assert_eq!(query_param("/api/timelines?tenant=abc", "tenant"), Some("abc".into()));
        assert_eq!(query_param("/api/timelines", "tenant"), None);
    }
}
