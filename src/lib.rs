//! # Postgres MCP Rust Server
//!
//! Rust stdio MCP server that provides PostgreSQL inspection and tuning tools.
//!
//! ## Rationale
//! Deliver a low-latency alternative to the Python server while preserving core
//! tool names and operator-facing behavior.
//!
//! ## Security Boundaries
//! * **Restricted mode**: `execute_sql` is guarded by SQL safety checks.
//! * **Credential handling**: connection URI is read from CLI/env and never logged raw.
//!
//! ## References
//! * `README.md`

pub mod advisor_extension;
pub mod config;
pub mod db;
pub mod server;
pub mod sql_safety;
pub mod startup_coordination;
pub mod startup_dependencies;
pub mod tools;

pub type McpError = rmcp::ErrorData;
