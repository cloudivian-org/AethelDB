// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Opaque 16-byte identifiers for tenants and timelines.
//!
//! A *tenant* is one logically isolated database instance (one customer, one
//! local project). A *timeline* is a single branch of that tenant's history —
//! creating a branch (the headline "instant branching" feature) allocates a new
//! [`TimelineId`] that shares all pages up to its branch-point LSN. Both are
//! random 128-bit values rendered as 32 lowercase hex characters, so they are
//! collision-free without coordination and URL-safe in control-plane APIs.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Generate the boilerplate for a 16-byte, hex-encoded identifier newtype.
macro_rules! hex_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub [u8; 16]);

        impl $name {
            /// The all-zero identifier, reserved as a sentinel "none" value.
            pub const ZERO: $name = $name([0u8; 16]);

            /// Construct from raw bytes.
            #[inline]
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                $name(bytes)
            }

            /// Borrow the underlying bytes.
            #[inline]
            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for b in &self.0 {
                    write!(f, "{:02x}", b)?;
                }
                Ok(())
            }
        }

        // Debug prints `TypeName(hex)` so logs are greppable but unambiguous.
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self)
            }
        }

        impl FromStr for $name {
            type Err = Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                if s.len() != 32 {
                    return Err(Error::parse(format!(
                        "{} must be 32 hex chars, got {}",
                        stringify!($name),
                        s.len()
                    )));
                }
                let mut bytes = [0u8; 16];
                for (i, byte) in bytes.iter_mut().enumerate() {
                    let hi = &s[i * 2..i * 2 + 2];
                    *byte = u8::from_str_radix(hi, 16).map_err(|e| {
                        Error::parse(format!("invalid hex in {}: {e}", stringify!($name)))
                    })?;
                }
                Ok($name(bytes))
            }
        }
    };
}

hex_id! {
    /// Identifies one isolated database instance.
    TenantId
}

hex_id! {
    /// Identifies one branch of a tenant's history.
    TimelineId
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_32_lowercase_hex() {
        let id = TenantId::from_bytes([0xAB; 16]);
        let s = id.to_string();
        assert_eq!(s.len(), 32);
        assert_eq!(s, "abababababababababababababababab");
    }

    #[test]
    fn parse_round_trips() {
        let id = TimelineId::from_bytes([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]);
        let parsed: TimelineId = id.to_string().parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn parse_rejects_bad_length_and_chars() {
        assert!("abc".parse::<TenantId>().is_err());
        assert!("zz000000000000000000000000000000".parse::<TenantId>().is_err());
    }
}
