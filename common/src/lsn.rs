// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Log Sequence Number — the 64-bit position of a byte in the WAL stream.
//!
//! Every page version, every safekeeper acknowledgement, and every pageserver
//! lookup is keyed on an LSN. PostgreSQL prints LSNs as two 32-bit hexadecimal
//! halves separated by a slash, e.g. `16/B374D848`; we preserve that exact
//! textual form so values are copy-pasteable between our tooling and `psql`.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// A WAL position. Monotonic within a timeline; 0 is the canonical "invalid" LSN.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Lsn(pub u64);

impl Lsn {
    /// The invalid / zero LSN, mirroring PostgreSQL's `InvalidXLogRecPtr`.
    pub const INVALID: Lsn = Lsn(0);

    /// Raw 64-bit value.
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Returns `true` for the zero (invalid) position.
    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 != 0
    }

    /// Round this LSN down to the start of its containing WAL block.
    ///
    /// PostgreSQL writes the WAL in fixed-size blocks (8 KiB by default); the
    /// safekeeper and pageserver frequently need the block-aligned floor of a
    /// position when slicing the log.
    #[inline]
    pub const fn align_down(self, block_size: u64) -> Lsn {
        Lsn(self.0 - (self.0 % block_size))
    }

    /// Saturating addition of a byte offset, useful when advancing a cursor.
    #[inline]
    pub const fn checked_add(self, bytes: u64) -> Option<Lsn> {
        match self.0.checked_add(bytes) {
            Some(v) => Some(Lsn(v)),
            None => None,
        }
    }
}

impl fmt::Display for Lsn {
    /// Render as `HIGH/LOW` in uppercase hex, exactly like `pg_current_wal_lsn()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl FromStr for Lsn {
    type Err = Error;

    /// Parse the `HIGH/LOW` hex form produced by [`Display`] and by PostgreSQL.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi, lo) = s
            .split_once('/')
            .ok_or_else(|| Error::parse(format!("LSN '{s}' is missing the '/' separator")))?;

        let hi = u32::from_str_radix(hi.trim(), 16)
            .map_err(|e| Error::parse(format!("invalid LSN high half '{hi}': {e}")))?;
        let lo = u32::from_str_radix(lo.trim(), 16)
            .map_err(|e| Error::parse(format!("invalid LSN low half '{lo}': {e}")))?;

        Ok(Lsn(((hi as u64) << 32) | (lo as u64)))
    }
}

impl From<u64> for Lsn {
    #[inline]
    fn from(v: u64) -> Self {
        Lsn(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_matches_postgres_format() {
        // 0x16 << 32 | 0xB374D848 == 0x16B374D848
        let lsn = Lsn(0x0000_0016_B374_D848);
        assert_eq!(lsn.to_string(), "16/B374D848");
    }

    #[test]
    fn parse_round_trips_through_display() {
        for raw in [0u64, 1, 0xB374D848, 0x0000_0016_B374_D848, u64::MAX] {
            let lsn = Lsn(raw);
            let parsed: Lsn = lsn.to_string().parse().expect("round trip");
            assert_eq!(parsed, lsn, "raw={raw:#x}");
        }
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!("not-an-lsn".parse::<Lsn>().is_err());
        assert!("16".parse::<Lsn>().is_err()); // missing separator
        assert!("ZZ/00".parse::<Lsn>().is_err()); // non-hex
    }

    #[test]
    fn validity_and_alignment() {
        assert!(!Lsn::INVALID.is_valid());
        assert!(Lsn(1).is_valid());
        assert_eq!(Lsn(8193).align_down(8192), Lsn(8192));
        assert_eq!(Lsn(8192).align_down(8192), Lsn(8192));
    }
}
