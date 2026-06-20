// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Object storage abstraction for offloaded layers.
//!
//! Immutable layers are pushed to an S3-compatible object store for durable,
//! cheap, infinite-capacity history. The [`ObjectStore`] trait keeps the page
//! server agnostic about the backend: [`LocalObjectStore`] writes to a local
//! directory (standing in for MinIO / S3 in local development), while a real
//! deployment would provide an S3 implementation behind the same trait.

use std::path::{Path, PathBuf};

use async_trait::async_trait;

/// A minimal key/value object store (put / get / list).
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Store `bytes` under `key`, overwriting any previous value.
    async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<()>;
    /// Fetch the bytes stored under `key`.
    async fn get(&self, key: &str) -> anyhow::Result<Vec<u8>>;
    /// List keys beginning with `prefix`.
    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>>;
}

/// A filesystem-backed object store — the mock MinIO/S3 for local dev.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// Create (if necessary) and use `root` as the object store.
    pub fn new(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(LocalObjectStore { root })
    }

    /// Map an object key to a filesystem path, rejecting traversal.
    fn path_for(&self, key: &str) -> anyhow::Result<PathBuf> {
        anyhow::ensure!(
            !key.contains("..") && !key.starts_with('/'),
            "invalid object key {key:?}"
        );
        Ok(self.root.join(key))
    }
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
        let path = self.path_for(key)?;
        // Do the blocking filesystem work off the async runtime threads.
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // Write to a temp file then rename for atomic visibility.
            let tmp = path.with_extension("tmp");
            std::fs::write(&tmp, &bytes)?;
            std::fs::rename(&tmp, &path)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let path = self.path_for(key)?;
        let bytes = tokio::task::spawn_blocking(move || std::fs::read(path)).await??;
        Ok(bytes)
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let root = self.root.clone();
        let prefix = prefix.to_owned();
        let keys = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<String>> {
            let mut out = Vec::new();
            collect_keys(&root, &root, &prefix, &mut out)?;
            out.sort();
            Ok(out)
        })
        .await??;
        Ok(keys)
    }
}

/// Recursively collect object keys (paths relative to `root`) under `dir`.
fn collect_keys(root: &Path, dir: &Path, prefix: &str, out: &mut Vec<String>) -> anyhow::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_keys(root, &path, prefix, out)?;
        } else if path.extension().map(|e| e == "tmp").unwrap_or(false) {
            continue; // skip in-progress writes
        } else if let Ok(rel) = path.strip_prefix(root) {
            let key = rel.to_string_lossy().replace('\\', "/");
            if key.starts_with(prefix) {
                out.push(key);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);
    impl TempDir {
        fn new(tag: &str) -> Self {
            let p = std::env::temp_dir().join(format!("sp-obj-{}-{}", tag, std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            TempDir(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn put_get_list_round_trip() {
        let dir = TempDir::new("rt");
        let store = LocalObjectStore::new(&dir.0).unwrap();
        store.put("layers/0001.layer", vec![1, 2, 3]).await.unwrap();
        store.put("layers/0002.layer", vec![4, 5]).await.unwrap();

        assert_eq!(store.get("layers/0001.layer").await.unwrap(), vec![1, 2, 3]);
        let keys = store.list("layers/").await.unwrap();
        assert_eq!(keys, vec!["layers/0001.layer", "layers/0002.layer"]);
    }

    #[tokio::test]
    async fn rejects_path_traversal() {
        let dir = TempDir::new("trav");
        let store = LocalObjectStore::new(&dir.0).unwrap();
        assert!(store.put("../escape", vec![0]).await.is_err());
    }
}
