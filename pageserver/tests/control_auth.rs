// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The line-oriented control endpoint requires `auth <token>` before any other
//! command when the server is started with a control token.

use std::sync::Arc;

use pageserver::{serve_control, TenantManager};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// Send one command line and read the one-line reply.
async fn cmd(stream: &mut BufReader<TcpStream>, line: &str) -> String {
    stream.write_all(line.as_bytes()).await.unwrap();
    stream.write_all(b"\n").await.unwrap();
    stream.flush().await.unwrap();
    let mut reply = String::new();
    stream.read_line(&mut reply).await.unwrap();
    reply.trim_end().to_string()
}

#[tokio::test]
async fn control_requires_auth_when_token_set() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token: Arc<str> = Arc::from("s3cr3t");
    tokio::spawn(serve_control(TenantManager::new(1_000, None), None, listener, Some(token)));

    let mut s = BufReader::new(TcpStream::connect(addr).await.unwrap());

    // Before authenticating, any real command is rejected.
    assert!(cmd(&mut s, "list").await.starts_with("err unauthorized"));
    // A wrong token is rejected.
    assert!(cmd(&mut s, "auth nope").await.starts_with("err"));
    // The correct token authenticates the connection.
    assert_eq!(cmd(&mut s, "auth s3cr3t").await, "ok authenticated");
    // Now commands work.
    assert!(cmd(&mut s, "list").await.starts_with("ok"));
}
