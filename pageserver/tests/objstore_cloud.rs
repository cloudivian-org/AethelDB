// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Round-trip the cloud object store against a real backend.
//!
//! Skipped unless `AETHEL_TEST_OBJSTORE_URL` is set (e.g. `s3://bucket`,
//! `az://container`, `gs://bucket`), with credentials in the standard per-cloud
//! environment variables. This lets the same test verify AWS S3, Azure Blob,
//! Google Cloud Storage, or a local emulator (MinIO / Azurite / fake-gcs-server)
//! without baking any provider into the default `cargo test` run.

use pageserver::objstore::ObjectStore;
use pageserver::CloudObjectStore;

#[tokio::test]
async fn cloud_object_store_round_trip() {
    let Ok(url) = std::env::var("AETHEL_TEST_OBJSTORE_URL") else {
        eprintln!("skipping cloud_object_store_round_trip: set AETHEL_TEST_OBJSTORE_URL to run");
        return;
    };

    let store = CloudObjectStore::from_url(&url).expect("build cloud object store from URL");
    let key = "aethel-test/roundtrip.bin";

    // Clean slate, then put / get / list / delete.
    let _ = store.delete(key).await;
    store.put(key, vec![1, 2, 3, 4]).await.expect("put");
    assert_eq!(store.get(key).await.expect("get"), vec![1, 2, 3, 4]);

    let keys = store.list("aethel-test/").await.expect("list");
    assert!(keys.iter().any(|k| k == key), "listed keys should include {key}: {keys:?}");

    store.delete(key).await.expect("delete");
    // Deleting again is idempotent.
    store.delete(key).await.expect("idempotent delete");
}
