// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Integration test for the real S3 object store against a live MinIO / S3.
//!
//! Skipped unless `AETHEL_S3_ENDPOINT` is set, so it doesn't run in a normal
//! `cargo test`. To run it:
//!
//! ```bash
//! docker run -d -p 9100:9000 -e MINIO_ROOT_USER=minioadmin \
//!   -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
//! # create the bucket "aethel", then:
//! AETHEL_S3_ENDPOINT=http://localhost:9100 cargo test -p pageserver --test s3
//! ```

use pageserver::objstore::{ObjectStore, S3ObjectStore};

#[tokio::test]
async fn s3_put_get_list_delete_against_minio() {
    let endpoint = match std::env::var("AETHEL_S3_ENDPOINT") {
        Ok(e) => e,
        Err(_) => {
            eprintln!("skipping S3 test: set AETHEL_S3_ENDPOINT (e.g. http://localhost:9100)");
            return;
        }
    };
    let bucket = std::env::var("AETHEL_S3_BUCKET").unwrap_or_else(|_| "aethel".to_string());
    let access = std::env::var("AETHEL_S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret = std::env::var("AETHEL_S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());

    let store = S3ObjectStore::new(&endpoint, &bucket, "us-east-1", &access, &secret).unwrap();

    let key = format!("layers/it-{}.layer", std::process::id());
    let body = vec![0xAB, 0xCD, 0xEF, 0x01, 0x02];

    // put + get round-trip.
    store.put(&key, body.clone()).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), body, "object round-trips through S3");

    // list sees it under the prefix.
    let keys = store.list("layers/").await.unwrap();
    assert!(keys.contains(&key), "list returns the stored key");

    // delete removes it, and deleting again is idempotent.
    store.delete(&key).await.unwrap();
    store.delete(&key).await.unwrap();
    let keys = store.list("layers/").await.unwrap();
    assert!(!keys.contains(&key), "deleted object is gone");
}
