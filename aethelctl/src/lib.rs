// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! A thin client for the AethelDB page-server control plane.
//!
//! `aethelctl` speaks the same HTTP/JSON API as `curl` would (see
//! `pageserver::httpapi`), wrapping it in typed methods. It is a pure client —
//! it adds no behavior to the engine and is unaware of the data plane. When a
//! control token is configured on the server, pass it here and it is sent as a
//! `Authorization: Bearer` header on every request.

pub mod deploy;

use anyhow::{bail, Result};
use serde_json::{json, Value};

/// Default control-plane address (the page server's HTTP API).
pub const DEFAULT_SERVER: &str = "http://127.0.0.1:6403";

/// A connection to a page server's control plane.
pub struct Client {
    base: String,
    token: Option<String>,
    agent: ureq::Agent,
}

impl Client {
    /// Build a client for `base` (e.g. `http://host:6403`), optionally bearing
    /// `token` for auth.
    pub fn new(base: impl Into<String>, token: Option<String>) -> Self {
        let base = base.into().trim_end_matches('/').to_string();
        Client { base, token, agent: ureq::Agent::new() }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    fn request(&self, method: &str, path: &str) -> ureq::Request {
        let req = self.agent.request(method, &self.url(path));
        match &self.token {
            Some(t) => req.set("Authorization", &format!("Bearer {t}")),
            None => req,
        }
    }

    /// Execute a request (optionally with a JSON body), returning the parsed
    /// JSON response or a readable error carrying the server's message.
    fn run(&self, req: ureq::Request, body: Option<Value>) -> Result<Value> {
        let result = match body {
            Some(b) => req.send_json(b),
            None => req.call(),
        };
        match result {
            Ok(resp) => Ok(resp.into_json().unwrap_or(Value::Null)),
            Err(ureq::Error::Status(code, resp)) => {
                let raw = resp.into_string().unwrap_or_default();
                let detail = serde_json::from_str::<Value>(&raw)
                    .ok()
                    .and_then(|v| v.get("error").and_then(|e| e.as_str().map(str::to_owned)))
                    .unwrap_or(raw);
                bail!("server returned {code}: {detail}")
            }
            Err(e) => bail!("request to {} failed: {e}", self.base),
        }
    }

    /// `GET /healthz` — liveness.
    pub fn healthz(&self) -> Result<Value> {
        self.run(self.request("GET", "/healthz"), None)
    }

    /// `GET /v1/tenants` — list tenant ids.
    pub fn list_tenants(&self) -> Result<Vec<String>> {
        let v = self.run(self.request("GET", "/v1/tenants"), None)?;
        Ok(string_array(&v, "tenants"))
    }

    /// `POST /v1/tenants` — create a tenant.
    pub fn create_tenant(&self, id: &str) -> Result<Value> {
        self.run(self.request("POST", "/v1/tenants"), Some(json!({ "id": id })))
    }

    /// `GET /v1/timelines[?tenant=…]` — list timeline ids.
    pub fn list_timelines(&self, tenant: Option<&str>) -> Result<Vec<String>> {
        let path = match tenant {
            Some(t) => format!("/v1/timelines?tenant={t}"),
            None => "/v1/timelines".to_string(),
        };
        let v = self.run(self.request("GET", &path), None)?;
        Ok(string_array(&v, "timelines"))
    }

    /// `POST /v1/timelines` — create a root timeline.
    pub fn create_timeline(&self, id: &str, tenant: Option<&str>) -> Result<Value> {
        let mut body = json!({ "id": id });
        with_tenant(&mut body, tenant);
        self.run(self.request("POST", "/v1/timelines"), Some(body))
    }

    /// `POST /v1/branches` — branch `new` off `parent` at `lsn` (the PITR primitive).
    pub fn branch(&self, new: &str, parent: &str, lsn: u64, tenant: Option<&str>) -> Result<Value> {
        let mut body = json!({ "timeline": new, "parent": parent, "lsn": lsn });
        with_tenant(&mut body, tenant);
        self.run(self.request("POST", "/v1/branches"), Some(body))
    }

    /// `POST /v1/timelines/receive` — attach a WAL receiver to a timeline.
    pub fn receive(
        &self,
        timeline: &str,
        safekeeper: &str,
        start_lsn: u64,
        tenant: Option<&str>,
    ) -> Result<Value> {
        let mut body =
            json!({ "timeline": timeline, "safekeeper": safekeeper, "start_lsn": start_lsn });
        with_tenant(&mut body, tenant);
        self.run(self.request("POST", "/v1/timelines/receive"), Some(body))
    }

    /// `POST /v1/gc` — compact + branch-aware GC below `horizon_lsn`.
    pub fn gc(&self, horizon_lsn: u64, tenant: Option<&str>) -> Result<Value> {
        let mut body = json!({ "horizon_lsn": horizon_lsn });
        with_tenant(&mut body, tenant);
        self.run(self.request("POST", "/v1/gc"), Some(body))
    }
}

fn with_tenant(body: &mut Value, tenant: Option<&str>) {
    if let Some(t) = tenant {
        body["tenant"] = json!(t);
    }
}

fn string_array(v: &Value, key: &str) -> Vec<String> {
    v.get(key)
        .and_then(|a| a.as_array())
        .map(|a| a.iter().filter_map(|s| s.as_str().map(str::to_owned)).collect())
        .unwrap_or_default()
}
