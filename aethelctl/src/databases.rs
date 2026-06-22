// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! A friendly **database** layer over tenants.
//!
//! Users think in named databases ("orders", "analytics"), not 32-hex tenant
//! ids. This module maps a name to a deterministic tenant id and keeps a small
//! local registry (`~/.aethel/databases.json`) of the names a console/CLI has
//! provisioned — so the engine stays unchanged (it only ever sees ids) while the
//! UI shows names and connection strings.

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A named recovery branch (point-in-time restore point) of a database.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestorePoint {
    pub name: String,
    /// The branch's timeline id (32 hex chars).
    pub timeline: String,
    /// The LSN this restore point branches from.
    pub lsn: u64,
}

/// A provisioned database (a named tenant).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Database {
    pub name: String,
    /// The tenant id (32 hex chars) derived from the name.
    pub id: String,
    /// Named point-in-time restore points (branches).
    #[serde(default)]
    pub branches: Vec<RestorePoint>,
    /// The timeline the database currently serves from (`None` = the live root).
    #[serde(default)]
    pub current: Option<String>,
}

/// Derive a stable 32-hex tenant id from a database name (first 16 bytes of its
/// SHA-256). Deterministic, so provisioning the same name is idempotent.
pub fn id_from_name(name: &str) -> String {
    let digest = Sha256::digest(name.trim().as_bytes());
    digest[..16].iter().map(|b| format!("{b:02x}")).collect()
}

/// A `postgresql://` connection string for `name` against the client `endpoint`
/// (the proxy's `host:port`).
pub fn connection_string(name: &str, endpoint: &str) -> String {
    format!("postgresql://postgres@{endpoint}/{name}")
}

/// Path to the local database registry (overridable via `AETHEL_DB_REGISTRY`).
pub fn registry_path() -> PathBuf {
    if let Ok(p) = std::env::var("AETHEL_DB_REGISTRY") {
        return PathBuf::from(p);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".aethel").join("databases.json")
}

/// Load the registry (an empty list if none exists yet).
pub fn load() -> Vec<Database> {
    match std::fs::read(registry_path()) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Persist the registry.
pub fn save(dbs: &[Database]) -> Result<()> {
    let path = registry_path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    std::fs::write(&path, serde_json::to_vec_pretty(dbs)?)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Forget a database name from the registry, returning its entry if present.
pub fn remove(name: &str) -> Result<Option<Database>> {
    let mut dbs = load();
    if let Some(pos) = dbs.iter().position(|d| d.name == name) {
        let db = dbs.remove(pos);
        save(&dbs)?;
        Ok(Some(db))
    } else {
        Ok(None)
    }
}

/// Record a database name (idempotent), returning its entry.
pub fn upsert(name: &str) -> Result<Database> {
    let name = name.trim().to_string();
    anyhow::ensure!(!name.is_empty(), "database name must not be empty");
    let mut dbs = load();
    if let Some(existing) = dbs.iter().find(|d| d.name == name) {
        return Ok(existing.clone());
    }
    let db =
        Database { id: id_from_name(&name), name: name.clone(), branches: vec![], current: None };
    dbs.push(db.clone());
    save(&dbs)?;
    Ok(db)
}

/// Record a recovery branch (restore point) for a database. Idempotent by name.
pub fn add_branch(db: &str, branch: &str, timeline: &str, lsn: u64) -> Result<()> {
    let mut dbs = load();
    if let Some(d) = dbs.iter_mut().find(|d| d.name == db) {
        if !d.branches.iter().any(|b| b.name == branch) {
            d.branches.push(RestorePoint {
                name: branch.to_string(),
                timeline: timeline.to_string(),
                lsn,
            });
            save(&dbs)?;
        }
    }
    Ok(())
}

/// Set the timeline a database currently serves from (`None` = live root).
pub fn set_current(db: &str, timeline: Option<String>) -> Result<()> {
    let mut dbs = load();
    if let Some(d) = dbs.iter_mut().find(|d| d.name == db) {
        d.current = timeline;
        save(&dbs)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EnvGuard(PathBuf);
    impl EnvGuard {
        fn new(tag: &str) -> Self {
            let p = std::env::temp_dir().join(format!(
                "aethel-dbreg-{}-{}.json",
                tag,
                std::process::id()
            ));
            let _ = std::fs::remove_file(&p);
            std::env::set_var("AETHEL_DB_REGISTRY", &p);
            EnvGuard(p)
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            std::env::remove_var("AETHEL_DB_REGISTRY");
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn id_is_deterministic_32_hex() {
        let a = id_from_name("orders");
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(a, id_from_name("orders"));
        assert_ne!(a, id_from_name("analytics"));
    }

    #[test]
    fn connection_string_format() {
        assert_eq!(
            connection_string("orders", "db.example.com:5432"),
            "postgresql://postgres@db.example.com:5432/orders"
        );
    }

    #[test]
    fn upsert_is_idempotent_and_persists() {
        let _g = EnvGuard::new("upsert");
        let a = upsert("orders").unwrap();
        let b = upsert("orders").unwrap();
        assert_eq!(a, b);
        upsert("analytics").unwrap();
        let dbs = load();
        assert_eq!(dbs.len(), 2);
        assert!(dbs.iter().any(|d| d.name == "orders"));
    }
}
