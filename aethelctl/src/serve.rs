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
    /// Optional proxy compute-control API base URL (for running state + start/stop).
    pub proxy_url: Option<String>,
    /// Optional Prometheus base URL (for per-database metrics charts).
    pub prometheus_url: Option<String>,
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
    // Dynamic: POST /api/databases/<name>/start|stop (compute lifecycle).
    if *method == Method::Post {
        if let Some(rest) = path.strip_prefix("/api/databases/") {
            if let Some(name) = rest.strip_suffix("/start") {
                return api(database_action(cfg, name, true));
            }
            if let Some(name) = rest.strip_suffix("/stop") {
                return api(database_action(cfg, name, false));
            }
            if let Some(name) = rest.strip_suffix("/branch") {
                return api(branch_database(client, cfg, name, body));
            }
            if let Some(name) = rest.strip_suffix("/restore") {
                return api(restore_database(cfg, name, body));
            }
            if let Some(name) = rest.strip_suffix("/keepwarm") {
                return api(database_warm(cfg, name, true));
            }
            if let Some(name) = rest.strip_suffix("/nokeepwarm") {
                return api(database_warm(cfg, name, false));
            }
        }
    }
    // Dynamic: GET /api/databases/<name>/metrics (per-database charts).
    if *method == Method::Get {
        if let Some(rest) = path.strip_prefix("/api/databases/") {
            if let Some(name) = rest.strip_suffix("/metrics") {
                return api(database_metrics(cfg, name));
            }
        }
    }
    // Dynamic: DELETE /api/databases/<name> (deprovision).
    if *method == Method::Delete {
        if let Some(name) = path.strip_prefix("/api/databases/") {
            if !name.contains('/') {
                return api(delete_database(client, cfg, name));
            }
        }
    }
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
    let running = compute_states(cfg).values().filter(|&&r| r).count();
    Ok(json!({
        "server": cfg.control_url,
        "databases": databases::load().len(),
        "running": running,
        "computeControl": cfg.proxy_url.is_some(),
        "metrics": cfg.prometheus_url.is_some(),
        "allowApply": cfg.allow_apply,
        "grafanaUrl": cfg.grafana_url,
        "clientEndpoint": cfg.client_endpoint,
    }))
}

/// Per-database time-series for the console charts, queried from Prometheus over
/// the last hour. Returns native series the SPA renders as sparklines — the
/// scale-to-zero / branching signals Aurora can't show. Requires `--prometheus-url`.
fn database_metrics(cfg: &ServeCfg, name: &str) -> Result<Value> {
    let prom = cfg.prometheus_url.as_ref().ok_or_else(|| {
        anyhow!("metrics are not configured; start `aethelctl serve` with --prometheus-url")
    })?;
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let start = end.saturating_sub(3600);
    let sel = format!("{{database=\"{name}\"}}");
    let series = |metric: &str, rate: bool| -> Vec<Value> {
        let expr = if rate {
            format!("rate(aethel_proxy_database_{metric}{sel}[5m])")
        } else {
            format!("aethel_proxy_database_{metric}{sel}")
        };
        prom_range(prom, &expr, start, end)
    };
    Ok(json!({
        "database": name,
        "window": 3600,
        "series": {
            "connections": series("connections_total", true),
            "active": series("active_connections", false),
            "computeUp": series("compute_up", false),
            "wakes": series("wakes_total", true),
        },
    }))
}

/// Run a Prometheus `query_range` and return `[[unix_ts, value], …]` (empty on
/// any error — the chart just shows no data, never breaks the page).
fn prom_range(prom: &str, expr: &str, start: u64, end: u64) -> Vec<Value> {
    let url = format!("{}/api/v1/query_range", prom.trim_end_matches('/'));
    let resp = ureq::get(&url)
        .query("query", expr)
        .query("start", &start.to_string())
        .query("end", &end.to_string())
        .query("step", "60")
        .call();
    let Ok(resp) = resp else { return vec![] };
    let Ok(v) = resp.into_json::<Value>() else { return vec![] };
    v.pointer("/data/result/0/values")
        .and_then(|x| x.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|pair| {
                    let arr = pair.as_array()?;
                    let t = arr.first()?.as_f64()?;
                    let val = arr.get(1)?.as_str()?.parse::<f64>().ok()?;
                    Some(json!([t, val]))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Per-tenant running state from the proxy's compute-control API (empty if no
/// proxy URL is configured or it's unreachable).
fn compute_states(cfg: &ServeCfg) -> std::collections::HashMap<String, bool> {
    let mut map = std::collections::HashMap::new();
    if let Some(base) = &cfg.proxy_url {
        let url = format!("{}/tenants", base.trim_end_matches('/'));
        if let Ok(resp) = ureq::get(&url).call() {
            if let Ok(v) = resp.into_json::<Value>() {
                if let Some(arr) = v.get("tenants").and_then(|t| t.as_array()) {
                    for t in arr {
                        if let (Some(name), Some(running)) = (
                            t.get("tenant").and_then(|n| n.as_str()),
                            t.get("running").and_then(|r| r.as_bool()),
                        ) {
                            map.insert(name.to_string(), running);
                        }
                    }
                }
            }
        }
    }
    map
}

/// Per-tenant **keep-warm** state from the proxy (empty if no proxy/unreachable).
fn keep_warm_states(cfg: &ServeCfg) -> std::collections::HashMap<String, bool> {
    let mut map = std::collections::HashMap::new();
    if let Some(base) = &cfg.proxy_url {
        let url = format!("{}/tenants", base.trim_end_matches('/'));
        if let Ok(resp) = ureq::get(&url).call() {
            if let Ok(v) = resp.into_json::<Value>() {
                if let Some(arr) = v.get("tenants").and_then(|t| t.as_array()) {
                    for t in arr {
                        if let (Some(name), Some(kw)) = (
                            t.get("tenant").and_then(|n| n.as_str()),
                            t.get("keepWarm").and_then(|r| r.as_bool()),
                        ) {
                            map.insert(name.to_string(), kw);
                        }
                    }
                }
            }
        }
    }
    map
}

/// Start (`start=true`) or hibernate a database's compute via the proxy.
fn database_action(cfg: &ServeCfg, name: &str, start: bool) -> Result<Value> {
    let base = cfg.proxy_url.as_ref().ok_or_else(|| {
        anyhow!("compute control is not configured; start `aethelctl serve` with --proxy-url")
    })?;
    let action = if start { "start" } else { "stop" };
    let url = format!("{}/tenants/{}/{}", base.trim_end_matches('/'), name, action);
    ureq::post(&url).call().map_err(|e| anyhow!("compute {action} for {name} failed: {e}"))?;
    Ok(json!({ "name": name, "running": start }))
}

/// Mark a database keep-warm (never scale to zero → zero cold start) or clear it.
fn database_warm(cfg: &ServeCfg, name: &str, on: bool) -> Result<Value> {
    let base = cfg.proxy_url.as_ref().ok_or_else(|| {
        anyhow!("compute control is not configured; start `aethelctl serve` with --proxy-url")
    })?;
    let action = if on { "keepwarm" } else { "nokeepwarm" };
    let url = format!("{}/tenants/{}/{}", base.trim_end_matches('/'), name, action);
    ureq::post(&url).call().map_err(|e| anyhow!("keep-warm {action} for {name} failed: {e}"))?;
    Ok(json!({ "name": name, "keepWarm": on }))
}

const ROOT_TIMELINE: &str = "00000000000000000000000000000000";

/// Provision a database by name: derive its tenant, create it + a root timeline
/// on the control plane (idempotent), register a proxy route so its connection
/// string works immediately, and record the name locally.
fn create_database(client: &Client, cfg: &ServeCfg, body: &str) -> Result<Value> {
    let name = field(&parse(body), "name")?.to_string();
    let db = databases::upsert(&name)?;
    ignore_conflict(client.create_tenant(&db.id))?;
    ignore_conflict(client.create_timeline(ROOT_TIMELINE, Some(&db.id)))?;
    proxy_route(cfg, &db.name, true); // automatic routing (best-effort)
    Ok(json!({
        "name": db.name,
        "id": db.id,
        "connection": databases::connection_string(&db.name, &cfg.client_endpoint),
        "status": "active",
    }))
}

/// Deprovision a database: delete its tenant on the control plane, deregister its
/// proxy route, and forget the name locally. Idempotent.
fn delete_database(client: &Client, cfg: &ServeCfg, name: &str) -> Result<Value> {
    let db = databases::load()
        .into_iter()
        .find(|d| d.name == name)
        .ok_or_else(|| anyhow!("unknown database {name}"))?;
    let _ = client.delete_tenant(&db.id); // ignore "unknown" — idempotent
    proxy_route(cfg, name, false);
    databases::remove(name)?;
    Ok(json!({ "deleted": name }))
}

/// Create a **recovery branch** of a database — a copy-on-write snapshot at an
/// LSN restore point. This is point-in-time recovery surfaced per database: the
/// branch shares history up to `lsn` and diverges after.
fn branch_database(client: &Client, _cfg: &ServeCfg, name: &str, body: &str) -> Result<Value> {
    let db = databases::load()
        .into_iter()
        .find(|d| d.name == name)
        .ok_or_else(|| anyhow!("unknown database {name}"))?;
    let b = parse(body);
    let branch = b
        .get("branch")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("restore")
        .to_string();
    let lsn = b.get("lsn").and_then(|v| v.as_u64()).unwrap_or(0);
    let timeline = databases::id_from_name(&format!("{name}/{branch}"));
    ignore_conflict(client.branch(&timeline, ROOT_TIMELINE, lsn, Some(&db.id)))?;
    databases::add_branch(name, &branch, &timeline, lsn)?;
    Ok(json!({ "database": name, "branch": branch, "timeline": timeline, "lsn": lsn }))
}

/// Restore a database to a restore point (or back to live). Records the timeline
/// locally and **pins compute to it on the proxy** (which hibernates the database
/// so the next connection wakes serving from that timeline) — so an in-place
/// restore takes effect end-to-end, not just as a recorded intent.
fn restore_database(cfg: &ServeCfg, name: &str, body: &str) -> Result<Value> {
    let timeline = parse(body)
        .get("timeline")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty() && *s != "live")
        .map(str::to_owned);
    databases::set_current(name, timeline.clone())?;
    proxy_pin(cfg, name, timeline.as_deref()); // best-effort end-to-end effect
    Ok(json!({ "database": name, "current": timeline }))
}

/// Pin (`Some`) or unpin (`None`) a database's compute timeline on the proxy so a
/// restore takes effect on the next wake. Best-effort — a no-op without a proxy.
fn proxy_pin(cfg: &ServeCfg, name: &str, timeline: Option<&str>) {
    if let Some(base) = &cfg.proxy_url {
        let base = base.trim_end_matches('/');
        let url = match timeline {
            Some(tl) => format!("{base}/tenants/{name}/pin/{tl}"),
            None => format!("{base}/tenants/{name}/unpin"),
        };
        let _ = ureq::post(&url).call();
    }
}

/// Register (`add = true`) or deregister a tenant route on the proxy. Best-effort
/// — does nothing when no proxy URL is configured.
fn proxy_route(cfg: &ServeCfg, name: &str, add: bool) {
    if let Some(base) = &cfg.proxy_url {
        let url = format!("{}/tenants/{}", base.trim_end_matches('/'), name);
        let _ = if add { ureq::post(&url).call() } else { ureq::request("DELETE", &url).call() };
    }
}

/// List provisioned databases, tagging each with its provisioning status (from
/// the control plane) and its compute state (running / hibernated, from the
/// proxy — `unmanaged` when no compute route exists).
fn list_databases(client: &Client, cfg: &ServeCfg) -> Result<Value> {
    let live = client.list_tenants().unwrap_or_default();
    let compute = compute_states(cfg);
    let warm = keep_warm_states(cfg);
    let dbs: Vec<Value> = databases::load()
        .into_iter()
        .map(|d| {
            let status = if live.contains(&d.id) { "active" } else { "pending" };
            let compute = match compute.get(&d.name) {
                Some(true) => "running",
                Some(false) => "hibernated",
                None => "unmanaged",
            };
            let keep_warm = warm.get(&d.name).copied().unwrap_or(false);
            // Friendly name of the timeline the database currently serves from.
            let current = match &d.current {
                None => "live".to_string(),
                Some(tl) => d
                    .branches
                    .iter()
                    .find(|b| &b.timeline == tl)
                    .map(|b| b.name.clone())
                    .unwrap_or_else(|| "restore".to_string()),
            };
            json!({
                "name": d.name,
                "id": d.id,
                "connection": databases::connection_string(&d.name, &cfg.client_endpoint),
                "status": status,
                "compute": compute,
                "keepWarm": keep_warm,
                "current": current,
                "branches": d.branches,
            })
        })
        .collect();
    Ok(json!({
        "databases": dbs,
        "endpoint": cfg.client_endpoint,
        "computeControl": cfg.proxy_url.is_some(),
    }))
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
