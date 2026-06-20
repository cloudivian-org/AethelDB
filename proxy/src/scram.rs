// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Proxy-side SCRAM-SHA-256 authentication.
//!
//! When a tenant has a stored SCRAM verifier, the proxy authenticates the client
//! itself — *before* waking compute — so bad credentials are rejected without a
//! cold start (a real scale-to-zero protection). On success the proxy does not
//! send `AuthenticationOk`; it forwards the startup to a `trust`-auth backend on
//! the trusted local network, whose `AuthenticationOk` completes the client's
//! handshake. The backend therefore re-uses the proxy's authentication.
//!
//! This implements RFC 5802 over PostgreSQL's SASL message framing, using
//! `hmac`/`sha2` for the primitives. Only the channel-binding-free `n,,` mode is
//! supported (PostgreSQL's `SCRAM-SHA-256`, not `-PLUS`).

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

type HmacSha256 = Hmac<Sha256>;

/// Errors from the SCRAM exchange. Any of these means authentication failed.
#[derive(Debug, Error)]
pub enum ScramError {
    #[error("io error during SCRAM exchange: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed SCRAM message: {0}")]
    Protocol(&'static str),
    #[error("authentication failed")]
    AuthFailed,
}

/// A stored SCRAM-SHA-256 verifier — enough to *check* a password without
/// knowing it. Mirrors PostgreSQL's `pg_authid.rolpassword` secret.
#[derive(Debug, Clone)]
pub struct ScramSecret {
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

/// PBKDF2-HMAC-SHA256 with a 1-block output (`Hi` from RFC 5802).
fn hi(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut salted = Vec::with_capacity(salt.len() + 4);
    salted.extend_from_slice(salt);
    salted.extend_from_slice(&1u32.to_be_bytes()); // INT(1)
    let mut u = hmac(password, &salted);
    let mut result = u;
    for _ in 1..iterations {
        u = hmac(password, &u);
        for (r, x) in result.iter_mut().zip(u.iter()) {
            *r ^= x;
        }
    }
    result
}

impl ScramSecret {
    /// Derive a verifier from a cleartext password (used to provision a tenant
    /// credential, and in tests).
    pub fn from_password(password: &str, salt: &[u8], iterations: u32) -> Self {
        let salted = hi(password.as_bytes(), salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac(&salted, b"Server Key");
        ScramSecret { iterations, salt: salt.to_vec(), stored_key, server_key }
    }
}

/// Generate a 24-character base64 nonce.
fn make_nonce() -> String {
    let mut buf = [0u8; 18];
    rand::thread_rng().fill_bytes(&mut buf);
    B64.encode(buf)
}

/// Run the SCRAM-SHA-256 exchange as the server over `stream`. Returns `Ok` only
/// if the client proved knowledge of the password behind `secret`.
pub async fn authenticate<S>(stream: &mut S, secret: &ScramSecret) -> Result<(), ScramError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // 1. Offer SCRAM-SHA-256.
    let mut sasl = Vec::new();
    sasl.extend_from_slice(b"SCRAM-SHA-256\0\0");
    write_auth_message(stream, 10, &sasl).await?; // AuthenticationSASL

    // 2. Read the client's first message (SASLInitialResponse).
    let (tag, body) = read_message(stream).await?;
    if tag != b'p' {
        return Err(ScramError::Protocol("expected SASLInitialResponse"));
    }
    let client_first = parse_sasl_initial(&body)?;
    let client_first_bare = strip_gs2_header(&client_first)?;
    let client_nonce = field(client_first_bare, 'r').ok_or(ScramError::Protocol("no client nonce"))?;

    // 3. Send the server-first message.
    let server_nonce = make_nonce();
    let combined_nonce = format!("{client_nonce}{server_nonce}");
    let server_first = format!(
        "r={combined_nonce},s={},i={}",
        B64.encode(&secret.salt),
        secret.iterations
    );
    write_auth_message(stream, 11, server_first.as_bytes()).await?; // SASLContinue

    // 4. Read the client's final message and verify the proof.
    let (tag, body) = read_message(stream).await?;
    if tag != b'p' {
        return Err(ScramError::Protocol("expected SASLResponse"));
    }
    let client_final = std::str::from_utf8(&body).map_err(|_| ScramError::Protocol("bad utf8"))?;
    let proof_b64 = field(client_final, 'p').ok_or(ScramError::Protocol("no client proof"))?;
    let final_nonce = field(client_final, 'r').ok_or(ScramError::Protocol("no nonce"))?;
    if final_nonce != combined_nonce {
        return Err(ScramError::Protocol("nonce mismatch"));
    }
    let client_final_without_proof = client_final
        .rsplit_once(",p=")
        .map(|(head, _)| head)
        .ok_or(ScramError::Protocol("no proof field"))?;

    let auth_message =
        format!("{client_first_bare},{server_first},{client_final_without_proof}");
    let client_signature = hmac(&secret.stored_key, auth_message.as_bytes());
    let proof = B64.decode(proof_b64).map_err(|_| ScramError::Protocol("bad proof base64"))?;
    if proof.len() != 32 {
        return Err(ScramError::Protocol("bad proof length"));
    }
    // ClientKey = ClientProof XOR ClientSignature; verify SHA256(ClientKey).
    let mut client_key = [0u8; 32];
    for i in 0..32 {
        client_key[i] = proof[i] ^ client_signature[i];
    }
    if sha256(&client_key) != secret.stored_key {
        return Err(ScramError::AuthFailed);
    }

    // 5. Send the server signature (AuthenticationSASLFinal). We intentionally
    //    do NOT send AuthenticationOk — the trust backend's will.
    let server_signature = hmac(&secret.server_key, auth_message.as_bytes());
    let server_final = format!("v={}", B64.encode(server_signature));
    write_auth_message(stream, 12, server_final.as_bytes()).await?; // SASLFinal
    Ok(())
}

/// Run the SCRAM-SHA-256 exchange as the *client* over `stream`, proving
/// knowledge of `password`. Useful for tests and for the proxy authenticating to
/// an upstream. Returns `Ok` only if the server's signature also verifies.
pub async fn client_authenticate<S>(stream: &mut S, user: &str, password: &str) -> Result<(), ScramError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Expect AuthenticationSASL.
    let (tag, body) = read_message(stream).await?;
    if tag != b'R' || body.get(0..4) != Some(&10i32.to_be_bytes()) {
        return Err(ScramError::Protocol("expected AuthenticationSASL"));
    }

    // Send SASLInitialResponse.
    let client_nonce = make_nonce();
    let client_first_bare = format!("n={user},r={client_nonce}");
    let client_first = format!("n,,{client_first_bare}");
    let mut init = Vec::new();
    init.extend_from_slice(b"SCRAM-SHA-256\0");
    init.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
    init.extend_from_slice(client_first.as_bytes());
    write_p_message(stream, &init).await?;

    // Read server-first.
    let (_, body) = read_message(stream).await?;
    let server_first =
        std::str::from_utf8(body.get(4..).unwrap_or_default()).map_err(|_| ScramError::Protocol("bad utf8"))?.to_string();
    let combined_nonce = field(&server_first, 'r').ok_or(ScramError::Protocol("no nonce"))?.to_string();
    let salt = B64.decode(field(&server_first, 's').ok_or(ScramError::Protocol("no salt"))?)
        .map_err(|_| ScramError::Protocol("bad salt"))?;
    let iterations: u32 =
        field(&server_first, 'i').and_then(|s| s.parse().ok()).ok_or(ScramError::Protocol("no iterations"))?;

    // Compute the proof.
    let salted = hi(password.as_bytes(), &salt, iterations);
    let client_key = hmac(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let client_final_without_proof = format!("c=biws,r={combined_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");
    let client_signature = hmac(&stored_key, auth_message.as_bytes());
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_signature[i];
    }
    let client_final = format!("{client_final_without_proof},p={}", B64.encode(proof));
    write_p_message(stream, client_final.as_bytes()).await?;

    // Verify the server signature in SASLFinal.
    let (_, body) = read_message(stream).await?;
    let server_final = std::str::from_utf8(body.get(4..).unwrap_or_default()).map_err(|_| ScramError::Protocol("bad utf8"))?;
    let v = field(server_final, 'v').ok_or(ScramError::Protocol("no server signature"))?;
    let server_key = hmac(&salted, b"Server Key");
    if v != B64.encode(hmac(&server_key, auth_message.as_bytes())) {
        return Err(ScramError::AuthFailed);
    }
    Ok(())
}

/// Write a client ('p') message: int32 length (self-inclusive) + body.
async fn write_p_message<S: AsyncWrite + Unpin>(stream: &mut S, body: &[u8]) -> Result<(), ScramError> {
    let len = 4 + body.len();
    let mut msg = Vec::with_capacity(1 + len);
    msg.push(b'p');
    msg.extend_from_slice(&(len as i32).to_be_bytes());
    msg.extend_from_slice(body);
    stream.write_all(&msg).await?;
    stream.flush().await?;
    Ok(())
}

/// Write an `Authentication*` message ('R'): int32 length, int32 subtype, body.
async fn write_auth_message<S: AsyncWrite + Unpin>(
    stream: &mut S,
    subtype: i32,
    body: &[u8],
) -> Result<(), ScramError> {
    let len = 4 + 4 + body.len();
    let mut msg = Vec::with_capacity(1 + len);
    msg.push(b'R');
    msg.extend_from_slice(&(len as i32).to_be_bytes());
    msg.extend_from_slice(&subtype.to_be_bytes());
    msg.extend_from_slice(body);
    stream.write_all(&msg).await?;
    stream.flush().await?;
    Ok(())
}

/// Read one PostgreSQL message: a 1-byte tag, an int32 length (self-inclusive),
/// and the body.
async fn read_message<S: AsyncRead + Unpin>(stream: &mut S) -> Result<(u8, Vec<u8>), ScramError> {
    let mut tag = [0u8; 1];
    stream.read_exact(&mut tag).await?;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = i32::from_be_bytes(len_buf);
    if !(4..=(1 << 20)).contains(&len) {
        return Err(ScramError::Protocol("message length out of range"));
    }
    let mut body = vec![0u8; (len - 4) as usize];
    stream.read_exact(&mut body).await?;
    Ok((tag[0], body))
}

/// Parse a SASLInitialResponse body: mechanism\0 + int32 len + data.
fn parse_sasl_initial(body: &[u8]) -> Result<String, ScramError> {
    let nul = body.iter().position(|&b| b == 0).ok_or(ScramError::Protocol("no mechanism"))?;
    let mechanism = &body[..nul];
    if mechanism != b"SCRAM-SHA-256" {
        return Err(ScramError::Protocol("unsupported SASL mechanism"));
    }
    let rest = &body[nul + 1..];
    if rest.len() < 4 {
        return Err(ScramError::Protocol("truncated SASLInitialResponse"));
    }
    let data_len = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
    let data = &rest[4..];
    if data_len < 0 || data_len as usize != data.len() {
        return Err(ScramError::Protocol("bad SASL data length"));
    }
    String::from_utf8(data.to_vec()).map_err(|_| ScramError::Protocol("bad utf8"))
}

/// Strip the gs2 header (`n,,` or `y,,` etc.) from a client-first message,
/// leaving the bare message `n=...,r=...`.
fn strip_gs2_header(client_first: &str) -> Result<&str, ScramError> {
    // gs2-header is two comma-separated fields then the bare part.
    let mut commas = client_first.match_indices(',');
    let _first = commas.next();
    let second = commas.next().ok_or(ScramError::Protocol("bad gs2 header"))?;
    Ok(&client_first[second.0 + 1..])
}

/// Extract the value of a `key=value` field (single-char key) from a
/// comma-separated SCRAM message.
fn field(msg: &str, key: char) -> Option<&str> {
    let prefix = [key, '='];
    let prefix: String = prefix.iter().collect();
    msg.split(',').find_map(|kv| kv.strip_prefix(&prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn correct_password_authenticates() {
        let secret = ScramSecret::from_password("s3cret", b"a-pinch-of-salt", 4096);
        let (mut server_side, mut client_side) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move { authenticate(&mut server_side, &secret).await });
        let client = client_authenticate(&mut client_side, "tester", "s3cret").await;
        assert!(client.is_ok(), "client: {client:?}");
        assert!(server.await.unwrap().is_ok(), "server should accept the proof");
    }

    #[tokio::test]
    async fn wrong_password_is_rejected() {
        let secret = ScramSecret::from_password("s3cret", b"a-pinch-of-salt", 4096);
        let (mut server_side, mut client_side) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move { authenticate(&mut server_side, &secret).await });
        // Client proves the *wrong* password; its own verification of the
        // server signature also fails, but we assert on the server's verdict.
        let _ = client_authenticate(&mut client_side, "tester", "wrong").await;
        assert!(server.await.unwrap().is_err(), "server must reject a bad proof");
    }
}
