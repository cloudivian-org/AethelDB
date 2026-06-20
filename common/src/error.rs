// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The error type shared by the vocabulary crate.
//!
//! Library crates return typed errors (so callers can match on them) rather
//! than `anyhow::Error`. The service binaries are free to convert these into
//! `anyhow::Error` at their boundaries.

use std::result::Result as StdResult;

use thiserror::Error;

/// Errors produced while parsing or manipulating shared types.
#[derive(Debug, Error)]
pub enum Error {
    /// A textual value (LSN, ID, …) was malformed.
    #[error("parse error: {0}")]
    Parse(String),
}

impl Error {
    /// Build a [`Error::Parse`] from anything string-like.
    pub fn parse(msg: impl Into<String>) -> Error {
        Error::Parse(msg.into())
    }
}

/// Convenience alias used throughout the crate.
pub type Result<T> = StdResult<T, Error>;
