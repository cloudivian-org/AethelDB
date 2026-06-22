// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Drive a real page-server control plane in-process and exercise the
//! `aethelctl` client against it — the same path the CLI binary uses.

use std::sync::mpsc;

use aethelctl::Client;
use pageserver::TenantManager;

/// Start `serve_http_api` on a background thread (own Tokio runtime) and return
/// its base URL once it's listening.
fn spawn_server(token: Option<&str>) -> String {
    let (tx, rx) = mpsc::channel();
    let token = token.map(str::to_owned);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr).unwrap();
            let mgr = TenantManager::new(1_000, None);
            let tok = token.map(|t| std::sync::Arc::<str>::from(t.as_str()));
            pageserver::serve_http_api(mgr, None, listener, tok).await.unwrap();
        });
    });
    let addr = rx.recv().unwrap();
    format!("http://{addr}")
}

fn hex(n: u8) -> String {
    format!("{n:02x}").repeat(16) // 32 hex chars
}

#[test]
fn cli_drives_the_control_plane() {
    let c = Client::new(spawn_server(None), None);

    // Health.
    c.healthz().expect("healthz");

    // Tenant create + list; duplicate is an error.
    let acme = hex(0xAA);
    c.create_tenant(&acme).expect("create tenant");
    assert!(c.list_tenants().unwrap().contains(&acme));
    assert!(c.create_tenant(&acme).is_err(), "duplicate tenant should fail");

    // Timeline create + branch (the PITR primitive) + list, scoped to the tenant.
    let root = hex(0x00);
    let dev = hex(0x0b);
    c.create_timeline(&root, Some(&acme)).expect("create timeline");
    c.branch(&dev, &root, 100, Some(&acme)).expect("branch");
    let timelines = c.list_timelines(Some(&acme)).unwrap();
    assert!(timelines.contains(&root) && timelines.contains(&dev), "got {timelines:?}");

    // GC reports stats.
    let gc = c.gc(50, Some(&acme)).expect("gc");
    assert!(gc.get("versions_removed").is_some(), "gc result: {gc}");

    // Isolation: another tenant doesn't see acme's branch.
    let other = hex(0xCC);
    c.create_tenant(&other).unwrap();
    assert!(!c.list_timelines(Some(&other)).unwrap().contains(&dev));
}

#[test]
fn token_is_required_when_configured() {
    let base = spawn_server(Some("s3cr3t"));

    // No token / wrong token are rejected.
    assert!(Client::new(base.clone(), None).list_tenants().is_err());
    assert!(Client::new(base.clone(), Some("nope".into())).list_tenants().is_err());

    // The correct token works.
    let ok = Client::new(base, Some("s3cr3t".into()));
    ok.list_tenants().expect("authorized list");
    ok.create_tenant(&hex(0x11)).expect("authorized create");
}
