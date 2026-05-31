//! # Restricted SQL Safety Checks
//!
//! Re-exported SQL read-only policy primitives from `mcp-toolkit-policy-core`.
//!
//! ## Rationale
//! Keep postgres server behavior aligned with shared, deterministic policy-core
//! logic while avoiding duplicated classifier implementations.
//!
//! ## Security Boundaries
//! * Classifier behavior is fail-closed.
//! * Error messages are redacted and stable.

pub use mcp_toolkit_policy_core::sql_read_only::{
    RestrictedSqlError, RestrictedSqlErrorCode, classify_restricted_sql, validate_restricted_sql,
};
