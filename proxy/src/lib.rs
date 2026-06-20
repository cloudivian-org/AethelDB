// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! # proxy — the AethelDB activation proxy (library)
//!
//! This crate implements the scale-to-zero front door described in Step 2. The
//! `aethel-proxy` binary is a thin CLI over the pieces exposed here:
//!
//! * [`protocol`] — PostgreSQL startup-packet parsing and error responses.
//! * [`tenant`] — the tenant registry and per-tenant lifecycle state.
//! * [`activator`] — pluggable start/stop of compute, plus the readiness probe.
//! * [`proxy`] — the connection handler and accept loop.
//! * [`idle`] — the background reaper that scales idle tenants to zero.

pub mod activator;
pub mod idle;
pub mod protocol;
pub mod proxy;
pub mod tenant;

pub use activator::{Activator, CommandActivator, NoopActivator};
pub use idle::ReaperConfig;
pub use proxy::{serve, HealthConfig, Proxy};
pub use tenant::{Registry, TenantState};
