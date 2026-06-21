// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Just enough of the PostgreSQL v3 frontend/backend protocol for the proxy.
//!
//! The proxy only needs to understand the *first* thing a client sends. Unlike
//! every later message, the first packet has no leading type byte; it is:
//!
//! ```text
//! Int32  length (including these 4 bytes)
//! Int32  request code / protocol version
//! ...    payload (for a StartupMessage: NUL-terminated key/value pairs)
//! ```
//!
//! The request code disambiguates the variants we care about: a real
//! `StartupMessage` (protocol 3.0 = `0x00030000`), an `SSLRequest`, a
//! `GSSENCRequest`, or a `CancelRequest`. Everything here operates on byte
//! buffers and is exhaustively unit-tested; the async I/O that fills those
//! buffers lives in [`crate::proxy`].

use std::collections::HashMap;

use thiserror::Error;

/// Magic request code for an `SSLRequest` packet.
pub const SSL_REQUEST_CODE: i32 = 80_877_103;
/// Magic request code for a `GSSENCRequest` packet.
pub const GSS_REQUEST_CODE: i32 = 80_877_104;
/// Magic request code for a `CancelRequest` packet.
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;
/// Protocol version 3.0, the only one modern servers speak.
pub const PROTOCOL_V3: i32 = 0x0003_0000;

/// Upper bound on a startup packet length. Real startup packets are tiny
/// (a few hundred bytes); anything larger is treated as hostile/malformed.
pub const MAX_STARTUP_LEN: usize = 64 * 1024;
/// A length field is at minimum the 4 length bytes + 4 code bytes.
pub const MIN_STARTUP_LEN: usize = 8;

/// Errors from parsing a client's first packet.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("declared startup length {0} is outside the allowed range (8..=65536 bytes)")]
    BadLength(usize),
    #[error("buffer of {got} bytes does not match declared length {declared}")]
    LengthMismatch { declared: usize, got: usize },
    #[error("startup parameters are not properly NUL-terminated")]
    UnterminatedParameters,
    #[error("startup parameter contained invalid UTF-8")]
    InvalidUtf8,
}

/// The classified first message from a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FirstMessage {
    /// A real connection request carrying its protocol version and parameters.
    Startup(StartupMessage),
    /// Client is asking to negotiate TLS before the startup packet.
    SslRequest,
    /// Client is asking to negotiate GSSAPI encryption.
    GssEncRequest,
    /// Client wants to cancel an in-flight query on an existing backend.
    CancelRequest { process_id: i32, secret_key: i32 },
}

/// A parsed `StartupMessage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupMessage {
    /// Protocol version requested (normally [`PROTOCOL_V3`]).
    pub protocol: i32,
    /// Connection parameters such as `user` and `database`.
    pub parameters: HashMap<String, String>,
    /// The original, untouched packet bytes (length prefix included) so the
    /// proxy can replay them verbatim to the backend.
    pub raw: Vec<u8>,
}

impl StartupMessage {
    /// Resolve the tenant this connection targets.
    ///
    /// We key tenants on the requested database name, falling back to the user
    /// name (PostgreSQL itself defaults the database to the user when omitted).
    pub fn tenant(&self) -> Option<&str> {
        self.parameters.get("database").or_else(|| self.parameters.get("user")).map(String::as_str)
    }
}

/// Read a big-endian `i32` from `buf[off..off+4]`.
#[inline]
fn read_i32(buf: &[u8], off: usize) -> i32 {
    i32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

/// Parse a complete first packet (including its 4-byte length prefix).
///
/// `raw` must be exactly the bytes of one packet: the leading length field is
/// validated against the buffer size so a truncated read can never be parsed as
/// a valid message.
pub fn parse_first_message(raw: Vec<u8>) -> Result<FirstMessage, ProtocolError> {
    if raw.len() < MIN_STARTUP_LEN {
        return Err(ProtocolError::BadLength(raw.len()));
    }
    let declared = read_i32(&raw, 0) as usize;
    if !(MIN_STARTUP_LEN..=MAX_STARTUP_LEN).contains(&declared) {
        return Err(ProtocolError::BadLength(declared));
    }
    if declared != raw.len() {
        return Err(ProtocolError::LengthMismatch { declared, got: raw.len() });
    }

    let code = read_i32(&raw, 4);
    match code {
        SSL_REQUEST_CODE => Ok(FirstMessage::SslRequest),
        GSS_REQUEST_CODE => Ok(FirstMessage::GssEncRequest),
        CANCEL_REQUEST_CODE => {
            // CancelRequest payload is exactly two more i32s.
            if raw.len() < 16 {
                return Err(ProtocolError::BadLength(raw.len()));
            }
            Ok(FirstMessage::CancelRequest {
                process_id: read_i32(&raw, 8),
                secret_key: read_i32(&raw, 12),
            })
        }
        protocol => {
            let parameters = parse_parameters(&raw[8..])?;
            Ok(FirstMessage::Startup(StartupMessage { protocol, parameters, raw }))
        }
    }
}

/// Parse the NUL-separated `key\0value\0...\0` parameter block of a startup
/// message. The block is terminated by an empty key (a trailing lone NUL).
fn parse_parameters(mut body: &[u8]) -> Result<HashMap<String, String>, ProtocolError> {
    let mut params = HashMap::new();

    loop {
        // A single trailing NUL (empty key) marks the end of the block.
        match body.first() {
            None => return Err(ProtocolError::UnterminatedParameters),
            Some(0) => return Ok(params),
            Some(_) => {}
        }

        let key = take_cstr(&mut body)?;
        let value = take_cstr(&mut body)?;
        params.insert(key, value);
    }
}

/// Split off the next NUL-terminated string from `body`, advancing the slice
/// past the terminator.
fn take_cstr(body: &mut &[u8]) -> Result<String, ProtocolError> {
    let nul = body.iter().position(|&b| b == 0).ok_or(ProtocolError::UnterminatedParameters)?;
    let s = std::str::from_utf8(&body[..nul]).map_err(|_| ProtocolError::InvalidUtf8)?.to_owned();
    *body = &body[nul + 1..];
    Ok(s)
}

/// Build a backend `ErrorResponse` ('E') message so the proxy can reject a
/// connection (e.g. unknown tenant) with a proper protocol error rather than an
/// abrupt socket close. `code` is a 5-character SQLSTATE.
pub fn error_response(severity: &str, code: &str, message: &str) -> Vec<u8> {
    // Body: a series of (field-type byte, C-string) pairs, then a final NUL.
    let mut body = Vec::with_capacity(severity.len() + code.len() + message.len() + 16);
    for (tag, text) in [(b'S', severity), (b'V', severity), (b'C', code), (b'M', message)] {
        body.push(tag);
        body.extend_from_slice(text.as_bytes());
        body.push(0);
    }
    body.push(0); // terminator

    let mut msg = Vec::with_capacity(body.len() + 5);
    msg.push(b'E');
    // Length covers the 4 length bytes + body, but not the leading type byte.
    msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a StartupMessage byte buffer from `(key, value)` parameters.
    fn build_startup(params: &[(&str, &str)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        for (k, v) in params {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        msg.extend_from_slice(&body);
        msg
    }

    #[test]
    fn parses_startup_and_extracts_tenant() {
        let raw = build_startup(&[("user", "alice"), ("database", "shop")]);
        match parse_first_message(raw.clone()).unwrap() {
            FirstMessage::Startup(s) => {
                assert_eq!(s.protocol, PROTOCOL_V3);
                assert_eq!(s.parameters.get("user").map(String::as_str), Some("alice"));
                assert_eq!(s.tenant(), Some("shop"));
                assert_eq!(s.raw, raw, "raw bytes must be preserved for replay");
            }
            other => panic!("expected Startup, got {other:?}"),
        }
    }

    #[test]
    fn tenant_falls_back_to_user_when_no_database() {
        let raw = build_startup(&[("user", "alice")]);
        let FirstMessage::Startup(s) = parse_first_message(raw).unwrap() else {
            panic!("expected startup");
        };
        assert_eq!(s.tenant(), Some("alice"));
    }

    #[test]
    fn detects_ssl_and_gss_requests() {
        let mut ssl = 8i32.to_be_bytes().to_vec();
        ssl.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
        assert_eq!(parse_first_message(ssl).unwrap(), FirstMessage::SslRequest);

        let mut gss = 8i32.to_be_bytes().to_vec();
        gss.extend_from_slice(&GSS_REQUEST_CODE.to_be_bytes());
        assert_eq!(parse_first_message(gss).unwrap(), FirstMessage::GssEncRequest);
    }

    #[test]
    fn detects_cancel_request() {
        let mut buf = 16i32.to_be_bytes().to_vec();
        buf.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        buf.extend_from_slice(&4242i32.to_be_bytes());
        buf.extend_from_slice(&99i32.to_be_bytes());
        assert_eq!(
            parse_first_message(buf).unwrap(),
            FirstMessage::CancelRequest { process_id: 4242, secret_key: 99 }
        );
    }

    #[test]
    fn rejects_length_mismatch_and_garbage() {
        // Declared length larger than the actual buffer (a truncated read).
        let mut bad = 100i32.to_be_bytes().to_vec();
        bad.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        assert!(matches!(
            parse_first_message(bad),
            Err(ProtocolError::LengthMismatch { declared: 100, .. })
        ));

        // Too short to even hold a length + code.
        assert!(matches!(parse_first_message(vec![0, 0, 0, 4]), Err(ProtocolError::BadLength(_))));
    }

    #[test]
    fn rejects_unterminated_parameters() {
        // Length+code valid, but the parameter block never NUL-terminates.
        let mut body = PROTOCOL_V3.to_be_bytes().to_vec();
        body.extend_from_slice(b"user"); // no NUL
        let mut raw = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        raw.extend_from_slice(&body);
        assert_eq!(parse_first_message(raw), Err(ProtocolError::UnterminatedParameters));
    }

    #[test]
    fn error_response_is_wellformed() {
        let msg = error_response("FATAL", "3D000", "no such tenant");
        assert_eq!(msg[0], b'E');
        let declared = read_i32(&msg, 1) as usize;
        // Declared length excludes the leading type byte.
        assert_eq!(declared, msg.len() - 1);
        // Message must end in the double-NUL (last field's NUL + terminator).
        assert_eq!(&msg[msg.len() - 2..], &[0, 0]);
        assert!(msg.windows(5).any(|w| w == b"3D000"));
    }
}
