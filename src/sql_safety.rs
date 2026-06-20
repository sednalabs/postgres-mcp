//! # Restricted SQL Safety Checks
//!
//! SQL read-only policy primitives backed by the toolkit policy authority.
//!
//! ## Rationale
//! Keep postgres server behavior aligned with the shared policy-kernel authority
//! while preserving the local error surface consumed by existing tool handlers.
//!
//! ## Security Boundaries
//! * Classifier behavior is fail-closed.
//! * Error messages are redacted and stable.

pub use mcp_toolkit_policy_core::sql_read_only::{RestrictedSqlError, RestrictedSqlErrorCode};
use mcp_toolkit_policy_core::{
    DecisionCode, SQL_POLICY_CONTRACT_VERSION, SqlRestrictedPolicyInput,
};
use mcp_toolkit_policy_runtime::{
    PolicyAuthorityDecision, PolicyRuntimeMode, configured_sql_restricted_policy_authority,
    sql_restricted_policy_authority,
};

/// Evaluates restricted SQL through the configured toolkit policy authority.
pub fn evaluate_restricted_sql_policy(sql: &str) -> PolicyAuthorityDecision {
    let input = SqlRestrictedPolicyInput {
        policy_contract_version: SQL_POLICY_CONTRACT_VERSION.to_string(),
        sql: sql.to_string(),
    };
    configured_sql_restricted_policy_authority().evaluate(&input)
}

/// Evaluates restricted SQL through a specific toolkit policy authority mode.
pub fn evaluate_restricted_sql_policy_with_mode(
    sql: &str,
    runtime_mode: PolicyRuntimeMode,
) -> PolicyAuthorityDecision {
    let input = SqlRestrictedPolicyInput {
        policy_contract_version: SQL_POLICY_CONTRACT_VERSION.to_string(),
        sql: sql.to_string(),
    };
    sql_restricted_policy_authority(runtime_mode).evaluate(&input)
}

/// Validates SQL string against read-only constraints.
pub fn validate_restricted_sql(sql: &str) -> Result<(), String> {
    classify_restricted_sql(sql).map_err(|err| err.message)
}

/// Classifies a SQL statement and returns detailed error information on failure.
pub fn classify_restricted_sql(sql: &str) -> Result<(), RestrictedSqlError> {
    decision_to_restricted_result(evaluate_restricted_sql_policy(sql))
}

fn decision_to_restricted_result(
    decision: PolicyAuthorityDecision,
) -> Result<(), RestrictedSqlError> {
    if decision.allow {
        return Ok(());
    }

    let code = restricted_error_code(decision.code.as_deref());
    Err(RestrictedSqlError {
        code,
        message: restricted_error_message(code).to_string(),
    })
}

fn restricted_error_code(code: Option<&str>) -> RestrictedSqlErrorCode {
    match code.and_then(DecisionCode::parse) {
        Some(DecisionCode::EmptySql) => RestrictedSqlErrorCode::EmptySql,
        Some(DecisionCode::UnterminatedToken) => RestrictedSqlErrorCode::UnterminatedToken,
        Some(DecisionCode::MultipleStatements) => RestrictedSqlErrorCode::MultipleStatements,
        Some(DecisionCode::NotReadOnlyPrefix) => RestrictedSqlErrorCode::NotReadOnlyPrefix,
        Some(DecisionCode::ForbiddenKeyword) => RestrictedSqlErrorCode::ForbiddenKeyword,
        Some(DecisionCode::ForbiddenFunction) => RestrictedSqlErrorCode::ForbiddenFunction,
        Some(DecisionCode::ExplainNotReadOnly) => RestrictedSqlErrorCode::ExplainNotReadOnly,
        Some(DecisionCode::ClassifierUnavailable)
        | Some(DecisionCode::SparkRuntimeUnavailable)
        | Some(DecisionCode::InvalidInput)
        | None
        | Some(_) => RestrictedSqlErrorCode::ClassifierUnavailable,
    }
}

fn restricted_error_message(code: RestrictedSqlErrorCode) -> &'static str {
    match code {
        RestrictedSqlErrorCode::EmptySql => "sql must not be empty",
        RestrictedSqlErrorCode::UnterminatedToken => {
            "restricted mode could not parse SQL lexical surface"
        }
        RestrictedSqlErrorCode::MultipleStatements => {
            "restricted mode allows only a single SQL statement"
        }
        RestrictedSqlErrorCode::NotReadOnlyPrefix => {
            "restricted mode allows only allowlisted SQL prefixes"
        }
        RestrictedSqlErrorCode::ForbiddenKeyword => "restricted mode rejected write/admin SQL",
        RestrictedSqlErrorCode::ForbiddenFunction => {
            "restricted mode rejected unsafe function call"
        }
        RestrictedSqlErrorCode::ExplainNotReadOnly => {
            "restricted mode allows EXPLAIN only for read-only statements"
        }
        RestrictedSqlErrorCode::ClassifierUnavailable => {
            "restricted mode policy classifier unavailable"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_authority_allows_read_only_sql_with_provenance() {
        let decision =
            evaluate_restricted_sql_policy_with_mode("SELECT 1", PolicyRuntimeMode::Rust);

        assert!(decision.allow);
        assert_eq!(decision.runtime_mode, PolicyRuntimeMode::Rust);
        assert_eq!(
            decision.policy_contract_version.as_deref(),
            Some(SQL_POLICY_CONTRACT_VERSION)
        );
        assert_eq!(
            decision.decision_source,
            "mcp_toolkit_policy_runtime.sql_restricted.rust"
        );
    }

    #[test]
    fn classifier_preserves_existing_error_code_surface() {
        let err = classify_restricted_sql("INSERT INTO t VALUES (1)")
            .expect_err("restricted SQL must reject writes");

        assert_eq!(err.code, RestrictedSqlErrorCode::NotReadOnlyPrefix);
        assert_eq!(
            err.message,
            "restricted mode allows only allowlisted SQL prefixes"
        );
    }
}
