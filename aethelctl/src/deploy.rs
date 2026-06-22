// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Deploy helpers: bring up a local stack (Docker Compose) and install AethelDB
//! onto a Kubernetes cluster (Helm).
//!
//! The Helm chart is **embedded in the binary**, so `aethelctl deploy` works
//! from anywhere — it's extracted to a temp directory and handed to `helm`.
//! These are thin wrappers over `helm` / `docker`; AethelDB itself is untouched.

use std::path::PathBuf;
use std::process::Command;

use anyhow::{bail, Context, Result};

/// The Helm chart, baked into the binary at build time.
static CHART: include_dir::Dir =
    include_dir::include_dir!("$CARGO_MANIFEST_DIR/../deploy/helm/aetheldb");

/// Options for `aethelctl deploy` (a `helm upgrade --install`).
#[derive(Debug, Default, Clone)]
pub struct DeployOpts {
    pub release: String,
    pub namespace: String,
    pub values_files: Vec<String>,
    pub sets: Vec<String>,
    pub object_store_url: Option<String>,
    pub image_repo: Option<String>,
    pub image_tag: Option<String>,
    /// Expose the proxy with a cloud LoadBalancer.
    pub expose: bool,
    pub wait: bool,
    pub dry_run: bool,
}

/// Build the `helm` argument vector (without the leading `helm`). Pure, so it's
/// unit-testable without invoking helm.
pub fn helm_args(opts: &DeployOpts, chart_dir: &str) -> Vec<String> {
    let mut args = vec![
        "upgrade".to_string(),
        "--install".to_string(),
        opts.release.clone(),
        chart_dir.to_string(),
        "--namespace".to_string(),
        opts.namespace.clone(),
        "--create-namespace".to_string(),
    ];
    for f in &opts.values_files {
        args.push("--values".to_string());
        args.push(f.clone());
    }

    let mut sets = opts.sets.clone();
    if let Some(u) = &opts.object_store_url {
        sets.push(format!("objectStore.url={u}"));
    }
    if let Some(r) = &opts.image_repo {
        sets.push(format!("image.repository={r}"));
    }
    if let Some(t) = &opts.image_tag {
        sets.push(format!("image.tag={t}"));
    }
    if opts.expose {
        sets.push("proxy.service.type=LoadBalancer".to_string());
    }
    for s in sets {
        args.push("--set".to_string());
        args.push(s);
    }

    if opts.wait {
        args.push("--wait".to_string());
    }
    if opts.dry_run {
        args.push("--dry-run".to_string());
    }
    args
}

/// Extract the embedded chart to a fresh temp directory and return its path.
fn extract_chart() -> Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("aethelctl-chart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    CHART.extract(&dir).context("extracting the embedded Helm chart")?;
    Ok(dir)
}

/// Run `helm upgrade --install` for the AethelDB chart. When `chart_override` is
/// `None`, the embedded chart is used.
pub fn deploy(opts: &DeployOpts, chart_override: Option<&str>) -> Result<()> {
    let chart_dir = match chart_override {
        Some(c) => c.to_string(),
        None => extract_chart()?.to_string_lossy().into_owned(),
    };
    run("helm", &helm_args(opts, &chart_dir))
}

/// Like [`deploy`], but **captures** helm's output instead of inheriting stdio
/// — used by the web console's deploy preview (always pass `dry_run = true`).
pub fn deploy_capture(opts: &DeployOpts, chart_override: Option<&str>) -> Result<String> {
    let chart_dir = match chart_override {
        Some(c) => c.to_string(),
        None => extract_chart()?.to_string_lossy().into_owned(),
    };
    let out = Command::new("helm")
        .args(helm_args(opts, &chart_dir))
        .output()
        .map_err(|e| anyhow::anyhow!("failed to run `helm` (installed and on PATH?): {e}"))?;
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    let err = String::from_utf8_lossy(&out.stderr);
    if !err.trim().is_empty() {
        text.push_str(&err);
    }
    if !out.status.success() {
        bail!("helm exited with {}:\n{text}", out.status);
    }
    Ok(text)
}

/// The equivalent `aethelctl deploy` / `helm` command line, for display.
pub fn command_preview(opts: &DeployOpts) -> String {
    format!("helm {}", helm_args(opts, "<chart>").join(" "))
}

/// Run `helm uninstall <release> -n <namespace>`.
pub fn uninstall(release: &str, namespace: &str) -> Result<()> {
    run("helm", &["uninstall".into(), release.into(), "--namespace".into(), namespace.into()])
}

/// Run a Docker Compose action (`up` / `down`) against `file`.
pub fn compose(action: &str, file: &str, detach: bool) -> Result<()> {
    let mut args =
        vec!["compose".to_string(), "-f".to_string(), file.to_string(), action.to_string()];
    if action == "up" && detach {
        args.push("-d".to_string());
    }
    run("docker", &args)
}

/// Spawn `program args…`, inheriting stdio, and map a non-zero exit to an error.
fn run(program: &str, args: &[String]) -> Result<()> {
    let status = Command::new(program).args(args).status().map_err(|e| {
        anyhow::anyhow!("failed to run `{program}` (is it installed and on PATH?): {e}")
    })?;
    if !status.success() {
        bail!("`{program}` exited with {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helm_args_compose_values_sets_and_flags() {
        let opts = DeployOpts {
            release: "aethel".into(),
            namespace: "ns".into(),
            values_files: vec!["prod.yaml".into()],
            sets: vec!["safekeeper.replicas=3".into()],
            object_store_url: Some("s3://bucket".into()),
            image_repo: Some("ghcr.io/me/aetheldb".into()),
            image_tag: Some("v0.2.0".into()),
            expose: true,
            wait: true,
            dry_run: true,
        };
        let args = helm_args(&opts, "/tmp/chart");
        let joined = args.join(" ");
        assert!(joined
            .starts_with("upgrade --install aethel /tmp/chart --namespace ns --create-namespace"));
        assert!(joined.contains("--values prod.yaml"));
        assert!(joined.contains("--set safekeeper.replicas=3"));
        assert!(joined.contains("--set objectStore.url=s3://bucket"));
        assert!(joined.contains("--set image.repository=ghcr.io/me/aetheldb"));
        assert!(joined.contains("--set image.tag=v0.2.0"));
        assert!(joined.contains("--set proxy.service.type=LoadBalancer"));
        assert!(joined.contains("--wait"));
        assert!(joined.contains("--dry-run"));
    }

    #[test]
    fn embedded_chart_extracts() {
        let dir = extract_chart().unwrap();
        assert!(dir.join("Chart.yaml").exists());
        assert!(dir.join("templates").join("pageserver.yaml").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
