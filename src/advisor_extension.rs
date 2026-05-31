//! # Advisor Extension Runtime
//!
//! Provider-neutral external advisor execution support for postgres-mcp.
//!
//! ## Rationale
//! Keep the public server provider-agnostic while allowing optional extension
//! providers to supply advisor recommendations through a stable JSON contract.
//!
//! ## Security Boundaries
//! * Executes only explicit command + args (no shell interpolation).
//! * Enforces bounded attempts and timeout on each external invocation.
//! * Normalizes external output before exposing it to tool payloads.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use crate::config::AdvisorExternalConfig;
use crate::db::{sql_quote_ident, sql_quote_qualified_ident};

const MAX_EXTERNAL_TABLE_NAME_BYTES: usize = 256;
const MAX_EXTERNAL_INDEX_COLUMNS: usize = 16;
const MAX_EXTERNAL_METHOD_BYTES: usize = 64;
const MAX_EXTERNAL_REASON_BYTES: usize = 1_024;
const MAX_EXTERNAL_STOP_REASON_BYTES: usize = 128;
const MAX_EXTERNAL_RECOMMENDATIONS_PER_ATTEMPT: usize = 256;
const MAX_EXTERNAL_TOTAL_RECOMMENDATIONS: usize = 1_024;
const MAX_EXTERNAL_ERRORS_PER_ATTEMPT: usize = 128;
const MAX_EXTERNAL_TOTAL_ERRORS: usize = 512;
const MAX_EXTERNAL_ERROR_ITEM_BYTES: usize = 2_048;
const MAX_EXTERNAL_STDOUT_BYTES: usize = 2 * 1024 * 1024;
const MAX_EXTERNAL_STDERR_BYTES: usize = 256 * 1024;
const DEFAULT_INDEX_METHOD: &str = "btree";

pub const ADVISOR_EXTENSION_PROTOCOL_VERSION: &str = "postgres-advisor-extension/v1";

pub type RunnerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<String, ExternalInvokeError>> + Send + 'a>>;

/// Runs external advisor requests and returns raw JSON output.
pub trait AdvisorExternalRunner: Send + Sync {
    /// Invoke the external advisor process with a serialized JSON request.
    ///
    /// # Errors
    /// Returns [`ExternalInvokeError`] when process startup, execution, timeout,
    /// or decoding fails.
    fn invoke<'a>(
        &'a self,
        request_json: &'a str,
        config: &'a AdvisorExternalConfig,
    ) -> RunnerFuture<'a>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalInvokeErrorKind {
    Timeout,
    Spawn,
    Io,
    NonZeroExit,
    InvalidUtf8,
    OutputTooLarge,
}

#[derive(Debug, Clone)]
pub struct ExternalInvokeError {
    pub kind: ExternalInvokeErrorKind,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct AdvisorExternalFailure {
    pub reason: &'static str,
    pub message: String,
    pub attempt_count: usize,
}

#[derive(Debug, Clone)]
pub struct AdvisorExternalOutcome {
    pub recommendations: Vec<Value>,
    pub errors: Vec<Value>,
    pub attempt_count: usize,
    pub stop_reason: String,
}

#[derive(Debug, Clone)]
pub struct ProcessAdvisorExternalRunner;

impl ProcessAdvisorExternalRunner {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for ProcessAdvisorExternalRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl AdvisorExternalRunner for ProcessAdvisorExternalRunner {
    fn invoke<'a>(
        &'a self,
        request_json: &'a str,
        config: &'a AdvisorExternalConfig,
    ) -> RunnerFuture<'a> {
        Box::pin(async move {
            let command = config
                .command
                .as_deref()
                .ok_or_else(|| ExternalInvokeError {
                    kind: ExternalInvokeErrorKind::Spawn,
                    message: "external advisor command is not configured".to_string(),
                })?;

            let mut cmd = Command::new(command);
            cmd.args(config.args.iter().map(String::as_str))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);

            let mut child = cmd.spawn().map_err(|err| ExternalInvokeError {
                kind: ExternalInvokeErrorKind::Spawn,
                message: format!(
                    "failed to spawn external advisor command {:?}: {err}",
                    command
                ),
            })?;

            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(request_json.as_bytes())
                    .await
                    .map_err(|err| ExternalInvokeError {
                        kind: ExternalInvokeErrorKind::Io,
                        message: format!("failed writing request to external advisor stdin: {err}"),
                    })?;
                stdin
                    .write_all(b"\n")
                    .await
                    .map_err(|err| ExternalInvokeError {
                        kind: ExternalInvokeErrorKind::Io,
                        message: format!("failed finalizing external advisor stdin payload: {err}"),
                    })?;
            }

            let mut stdout_reader = child.stdout.take().ok_or_else(|| ExternalInvokeError {
                kind: ExternalInvokeErrorKind::Io,
                message: "external advisor stdout pipe is unavailable".to_string(),
            })?;
            let mut stderr_reader = child.stderr.take().ok_or_else(|| ExternalInvokeError {
                kind: ExternalInvokeErrorKind::Io,
                message: "external advisor stderr pipe is unavailable".to_string(),
            })?;

            let output = tokio::time::timeout(config.timeout, async {
                let wait_fut = async {
                    child.wait().await.map_err(|err| ExternalInvokeError {
                        kind: ExternalInvokeErrorKind::Io,
                        message: format!(
                            "failed waiting for external advisor command output: {err}"
                        ),
                    })
                };
                let (stdout, stderr, status) = tokio::try_join!(
                    read_stream_capped(
                        &mut stdout_reader,
                        MAX_EXTERNAL_STDOUT_BYTES,
                        "external advisor stdout",
                    ),
                    read_stream_capped(
                        &mut stderr_reader,
                        MAX_EXTERNAL_STDERR_BYTES,
                        "external advisor stderr",
                    ),
                    wait_fut
                )?;
                Ok::<_, ExternalInvokeError>((stdout, stderr, status))
            })
            .await
            .map_err(|_| ExternalInvokeError {
                kind: ExternalInvokeErrorKind::Timeout,
                message: format!(
                    "external advisor command timed out after {}ms",
                    config.timeout.as_millis()
                ),
            })??;

            if !output.2.success() {
                let stderr_present = !output.1.is_empty();
                return Err(ExternalInvokeError {
                    kind: ExternalInvokeErrorKind::NonZeroExit,
                    message: if !stderr_present {
                        format!("external advisor command exited with status {}", output.2)
                    } else {
                        format!(
                            "external advisor command exited with status {} (stderr redacted)",
                            output.2
                        )
                    },
                });
            }

            let stdout = String::from_utf8(output.0).map_err(|err| ExternalInvokeError {
                kind: ExternalInvokeErrorKind::InvalidUtf8,
                message: format!("external advisor stdout is not valid UTF-8: {err}"),
            })?;

            Ok(stdout)
        })
    }
}

async fn read_stream_capped<R>(
    reader: &mut R,
    limit_bytes: usize,
    label: &str,
) -> Result<Vec<u8>, ExternalInvokeError>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let count = reader
            .read(&mut chunk)
            .await
            .map_err(|err| ExternalInvokeError {
                kind: ExternalInvokeErrorKind::Io,
                message: format!("failed reading {label}: {err}"),
            })?;
        if count == 0 {
            break;
        }
        if bytes.len().saturating_add(count) > limit_bytes {
            return Err(ExternalInvokeError {
                kind: ExternalInvokeErrorKind::OutputTooLarge,
                message: format!("{label} exceeded {limit_bytes} bytes"),
            });
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    Ok(bytes)
}

#[must_use]
pub fn default_advisor_external_runner() -> Arc<dyn AdvisorExternalRunner> {
    Arc::new(ProcessAdvisorExternalRunner::new())
}

#[derive(Debug, Serialize)]
struct ExternalAdvisorRequest<'a> {
    protocol_version: &'static str,
    tool_name: &'a str,
    query_count: usize,
    queries: &'a [String],
    max_index_size_mb: i64,
    attempt: usize,
    previous_recommendations: &'a [Value],
}

#[derive(Debug, Deserialize)]
struct ExternalAdvisorResponse {
    #[serde(default)]
    recommendations: Vec<ExternalAdvisorRecommendation>,
    #[serde(default)]
    errors: Vec<Value>,
    #[serde(default)]
    stop_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExternalAdvisorRecommendation {
    table: String,
    columns: Vec<String>,
    #[serde(default)]
    using: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct NormalizedRecommendation {
    table: String,
    columns: Vec<String>,
    using: String,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RecommendationKey {
    table: String,
    columns: Vec<String>,
    using: String,
}

impl NormalizedRecommendation {
    fn key(&self) -> RecommendationKey {
        RecommendationKey {
            table: self.table.clone(),
            columns: self.columns.clone(),
            using: self.using.clone(),
        }
    }

    fn to_json(&self) -> Value {
        let table_ident = sql_quote_qualified_ident(&self.table);
        let cols = self
            .columns
            .iter()
            .map(|c| sql_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let ddl = format!(
            "CREATE INDEX ON {table_ident} USING {} ({cols})",
            self.using
        );
        json!({
            "table": self.table,
            "columns": self.columns,
            "using": self.using,
            "index_definition": ddl,
            "reason": self.reason,
        })
    }
}

pub async fn run_external_advisor_loop(
    runner: &Arc<dyn AdvisorExternalRunner>,
    config: &AdvisorExternalConfig,
    tool_name: &str,
    queries: &[String],
    max_index_size_mb: i64,
) -> Result<AdvisorExternalOutcome, AdvisorExternalFailure> {
    if !config.enabled {
        return Err(AdvisorExternalFailure {
            reason: "external_disabled",
            message: "external advisor mode is disabled".to_string(),
            attempt_count: 0,
        });
    }
    if config.command.is_none() {
        return Err(AdvisorExternalFailure {
            reason: "external_not_configured",
            message: "external advisor command is not configured".to_string(),
            attempt_count: 0,
        });
    }

    let mut recommendation_index: BTreeMap<RecommendationKey, usize> = BTreeMap::new();
    let mut ordered: Vec<NormalizedRecommendation> = Vec::new();
    let mut external_errors = Vec::new();

    for attempt in 1..=config.max_attempts {
        let previous_recommendations = ordered
            .iter()
            .map(NormalizedRecommendation::to_json)
            .collect::<Vec<_>>();
        let request = ExternalAdvisorRequest {
            protocol_version: ADVISOR_EXTENSION_PROTOCOL_VERSION,
            tool_name,
            query_count: queries.len(),
            queries,
            max_index_size_mb,
            attempt,
            previous_recommendations: &previous_recommendations,
        };
        let request_json =
            serde_json::to_string(&request).map_err(|err| AdvisorExternalFailure {
                reason: "external_request_encoding_failed",
                message: format!("failed to encode external advisor request: {err}"),
                attempt_count: attempt,
            })?;
        let raw_output = runner
            .invoke(&request_json, config)
            .await
            .map_err(|err| map_invoke_error(err, attempt))?;

        let parsed: ExternalAdvisorResponse =
            serde_json::from_str(raw_output.trim()).map_err(|err| AdvisorExternalFailure {
                reason: "external_invalid_response",
                message: format!("external advisor returned invalid JSON response: {err}"),
                attempt_count: attempt,
            })?;
        if parsed.recommendations.len() > MAX_EXTERNAL_RECOMMENDATIONS_PER_ATTEMPT {
            return Err(AdvisorExternalFailure {
                reason: "external_invalid_response",
                message: format!(
                    "external advisor returned too many recommendations in one attempt ({} > {})",
                    parsed.recommendations.len(),
                    MAX_EXTERNAL_RECOMMENDATIONS_PER_ATTEMPT
                ),
                attempt_count: attempt,
            });
        }
        if parsed.errors.len() > MAX_EXTERNAL_ERRORS_PER_ATTEMPT {
            return Err(AdvisorExternalFailure {
                reason: "external_invalid_response",
                message: format!(
                    "external advisor returned too many errors in one attempt ({} > {})",
                    parsed.errors.len(),
                    MAX_EXTERNAL_ERRORS_PER_ATTEMPT
                ),
                attempt_count: attempt,
            });
        }

        let mut new_items = 0usize;
        for rec in parsed.recommendations {
            let normalized =
                normalize_external_recommendation(rec).map_err(|err| AdvisorExternalFailure {
                    reason: "external_invalid_response",
                    message: err,
                    attempt_count: attempt,
                })?;
            let key = normalized.key();
            if let Some(existing_idx) = recommendation_index.get(&key).copied() {
                let merged_reason =
                    merge_recommendation_reason(&ordered[existing_idx].reason, &normalized.reason);
                ordered[existing_idx].reason = merged_reason;
            } else {
                new_items += 1;
                if ordered.len() >= MAX_EXTERNAL_TOTAL_RECOMMENDATIONS {
                    return Err(AdvisorExternalFailure {
                        reason: "external_invalid_response",
                        message: format!(
                            "external advisor exceeded total recommendation limit ({})",
                            MAX_EXTERNAL_TOTAL_RECOMMENDATIONS
                        ),
                        attempt_count: attempt,
                    });
                }
                recommendation_index.insert(key, ordered.len());
                ordered.push(normalized);
            }
        }
        for error in parsed.errors {
            if external_errors.len() >= MAX_EXTERNAL_TOTAL_ERRORS {
                return Err(AdvisorExternalFailure {
                    reason: "external_invalid_response",
                    message: format!(
                        "external advisor exceeded total error item limit ({})",
                        MAX_EXTERNAL_TOTAL_ERRORS
                    ),
                    attempt_count: attempt,
                });
            }
            external_errors.push(normalize_external_error_item(error));
        }

        if let Some(stop_reason) = parsed
            .stop_reason
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if stop_reason.len() > MAX_EXTERNAL_STOP_REASON_BYTES {
                return Err(AdvisorExternalFailure {
                    reason: "external_invalid_response",
                    message: format!(
                        "external advisor stop_reason exceeds {} bytes",
                        MAX_EXTERNAL_STOP_REASON_BYTES
                    ),
                    attempt_count: attempt,
                });
            }
            return Ok(AdvisorExternalOutcome {
                recommendations: ordered
                    .iter()
                    .map(NormalizedRecommendation::to_json)
                    .collect(),
                errors: external_errors,
                attempt_count: attempt,
                stop_reason: stop_reason.to_string(),
            });
        }

        if new_items == 0 {
            return Ok(AdvisorExternalOutcome {
                recommendations: ordered
                    .iter()
                    .map(NormalizedRecommendation::to_json)
                    .collect(),
                errors: external_errors,
                attempt_count: attempt,
                stop_reason: "converged".to_string(),
            });
        }

        if attempt == config.max_attempts {
            return Ok(AdvisorExternalOutcome {
                recommendations: ordered
                    .iter()
                    .map(NormalizedRecommendation::to_json)
                    .collect(),
                errors: external_errors,
                attempt_count: attempt,
                stop_reason: "max_attempts".to_string(),
            });
        }
    }

    Err(AdvisorExternalFailure {
        reason: "external_unreachable_state",
        message: "external advisor loop reached an invalid terminal state".to_string(),
        attempt_count: config.max_attempts,
    })
}

fn map_invoke_error(err: ExternalInvokeError, attempt: usize) -> AdvisorExternalFailure {
    let reason = match err.kind {
        ExternalInvokeErrorKind::Timeout => "external_timeout",
        ExternalInvokeErrorKind::Spawn => "external_spawn_failed",
        ExternalInvokeErrorKind::Io => "external_io_failed",
        ExternalInvokeErrorKind::NonZeroExit => "external_non_zero_exit",
        ExternalInvokeErrorKind::InvalidUtf8 => "external_invalid_response",
        ExternalInvokeErrorKind::OutputTooLarge => "external_output_too_large",
    };
    AdvisorExternalFailure {
        reason,
        message: err.message,
        attempt_count: attempt,
    }
}

fn merge_recommendation_reason(current: &str, incoming: &str) -> String {
    let incoming = incoming.trim();
    if incoming.is_empty() || current == incoming {
        return current.to_string();
    }
    if current
        .split(" | ")
        .map(str::trim)
        .any(|reason| reason == incoming)
    {
        return current.to_string();
    }
    let merged = format!("{current} | {incoming}");
    if merged.len() > MAX_EXTERNAL_REASON_BYTES {
        return current.to_string();
    }
    merged
}

fn normalize_external_error_item(value: Value) -> Value {
    let serialized = match serde_json::to_string(&value) {
        Ok(serialized) => serialized,
        Err(_) => {
            return json!({
                "truncated": true,
                "reason": "external_error_serialization_failed",
            });
        }
    };
    if serialized.len() <= MAX_EXTERNAL_ERROR_ITEM_BYTES {
        return value;
    }
    json!({
        "truncated": true,
        "reason": "external_error_item_too_large",
        "original_bytes": serialized.len(),
        "preview": clip_utf8_bytes(&serialized, 512),
    })
}

fn clip_utf8_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut out = String::new();
    for ch in value.chars() {
        if out.len().saturating_add(ch.len_utf8()) > max_bytes {
            break;
        }
        out.push(ch);
    }
    out
}

fn normalize_external_recommendation(
    rec: ExternalAdvisorRecommendation,
) -> Result<NormalizedRecommendation, String> {
    let raw_table = rec.table.trim();
    if raw_table.is_empty() || raw_table.len() > MAX_EXTERNAL_TABLE_NAME_BYTES {
        return Err("external recommendation table must be non-empty and <= 256 bytes".to_string());
    }
    if raw_table.split('.').any(|part| part.trim().is_empty()) {
        return Err(
            "external recommendation table must not contain empty qualification segments"
                .to_string(),
        );
    }
    let table_parts = raw_table.split('.').map(str::trim).collect::<Vec<_>>();
    if table_parts
        .iter()
        .any(|part| part.chars().any(char::is_control))
    {
        return Err(
            "external recommendation table contains invalid control characters".to_string(),
        );
    }
    let table = table_parts.join(".");

    if rec.columns.is_empty() || rec.columns.len() > MAX_EXTERNAL_INDEX_COLUMNS {
        return Err(
            "external recommendation columns must contain between 1 and 16 entries".to_string(),
        );
    }

    let mut dedupe = BTreeSet::new();
    let mut columns = Vec::new();
    for raw_column in rec.columns {
        let column = raw_column.trim().trim_matches('"').to_string();
        if column.is_empty() {
            return Err("external recommendation columns must be non-empty strings".to_string());
        }
        if column.chars().any(char::is_control) {
            return Err(
                "external recommendation columns contain invalid control characters".to_string(),
            );
        }
        let normalized = column.to_ascii_lowercase();
        if dedupe.insert(normalized) {
            columns.push(column);
        }
    }
    if columns.is_empty() {
        return Err("external recommendation columns deduped to an empty set".to_string());
    }

    let using = rec
        .using
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_INDEX_METHOD)
        .to_ascii_lowercase();
    if using.len() > MAX_EXTERNAL_METHOD_BYTES || !is_valid_index_method(&using) {
        return Err(format!(
            "external recommendation using method {:?} is invalid",
            using
        ));
    }

    let reason = rec
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("external advisor recommendation")
        .to_string();
    if reason.len() > MAX_EXTERNAL_REASON_BYTES {
        return Err(format!(
            "external recommendation reason exceeds {} bytes",
            MAX_EXTERNAL_REASON_BYTES
        ));
    }

    Ok(NormalizedRecommendation {
        table,
        columns,
        using,
        reason,
    })
}

fn is_valid_index_method(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() || first == '_' => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
}

#[cfg(test)]
mod tests {
    use super::{
        AdvisorExternalConfig, AdvisorExternalFailure, AdvisorExternalOutcome,
        AdvisorExternalRunner, ExternalInvokeError, ExternalInvokeErrorKind, RunnerFuture,
        run_external_advisor_loop,
    };
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct FakeRunner {
        responses: Mutex<VecDeque<Result<String, ExternalInvokeError>>>,
    }

    impl FakeRunner {
        fn new(items: Vec<Result<String, ExternalInvokeError>>) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from(items)),
            }
        }
    }

    impl AdvisorExternalRunner for FakeRunner {
        fn invoke<'a>(
            &'a self,
            _request_json: &'a str,
            _config: &'a AdvisorExternalConfig,
        ) -> RunnerFuture<'a> {
            Box::pin(async move {
                self.responses
                    .lock()
                    .expect("responses lock")
                    .pop_front()
                    .unwrap_or_else(|| {
                        Err(ExternalInvokeError {
                            kind: ExternalInvokeErrorKind::Io,
                            message: "no fake responses left".to_string(),
                        })
                    })
            })
        }
    }

    fn enabled_config(max_attempts: usize) -> AdvisorExternalConfig {
        AdvisorExternalConfig {
            enabled: true,
            command: Some("/usr/bin/fake".to_string()),
            args: vec!["--json".to_string()],
            timeout: Duration::from_millis(500),
            max_attempts,
            fallback_to_dta: true,
        }
    }

    #[tokio::test]
    async fn external_loop_stops_on_convergence() {
        let runner = Arc::new(FakeRunner::new(vec![
            Ok(
                r#"{"recommendations":[{"table":"users","columns":["email"],"using":"btree","reason":"filter predicate"}],"errors":[]}"#
                    .to_string(),
            ),
            Ok(
                r#"{"recommendations":[{"table":"users","columns":["email"],"using":"btree","reason":"filter predicate"}],"errors":[]}"#
                    .to_string(),
            ),
        ]));
        let result = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(3),
            "analyze_query_indexes",
            &["select * from users where email = $1".to_string()],
            10_000,
        )
        .await
        .expect("external run should succeed");
        assert_eq!(result.stop_reason, "converged");
        assert_eq!(result.attempt_count, 2);
        assert_eq!(result.recommendations.len(), 1);
    }

    #[tokio::test]
    async fn external_loop_honors_stop_reason() {
        let runner = Arc::new(FakeRunner::new(vec![Ok(
            r#"{"recommendations":[],"errors":[],"stop_reason":"provider_done"}"#.to_string(),
        )]));
        let result = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(3),
            "analyze_query_indexes",
            &["select 1".to_string()],
            10_000,
        )
        .await
        .expect("external run should succeed");
        assert_eq!(result.stop_reason, "provider_done");
        assert_eq!(result.attempt_count, 1);
    }

    #[tokio::test]
    async fn external_loop_dedupes_structural_recommendations_when_reason_changes() {
        let runner = Arc::new(FakeRunner::new(vec![
            Ok(
                r#"{"recommendations":[{"table":"users","columns":["email"],"using":"btree","reason":"first reason"}],"errors":[]}"#
                    .to_string(),
            ),
            Ok(
                r#"{"recommendations":[{"table":"users","columns":["email"],"using":"btree","reason":"second reason"}],"errors":[]}"#
                    .to_string(),
            ),
        ]));
        let result = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(3),
            "analyze_query_indexes",
            &["select * from users where email = 'a@example.com'".to_string()],
            10_000,
        )
        .await
        .expect("external run should succeed");
        assert_eq!(result.stop_reason, "converged");
        assert_eq!(result.attempt_count, 2);
        assert_eq!(result.recommendations.len(), 1);
        let merged_reason = result.recommendations[0]
            .get("reason")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        assert!(merged_reason.contains("first reason"));
        assert!(merged_reason.contains("second reason"));
    }

    #[tokio::test]
    async fn external_loop_normalizes_oversized_error_items() {
        let oversized = "x".repeat(4_096);
        let runner = Arc::new(FakeRunner::new(vec![Ok(format!(
            r#"{{"recommendations":[],"errors":[{{"detail":"{}"}}],"stop_reason":"provider_done"}}"#,
            oversized
        ))]));
        let result = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(2),
            "analyze_query_indexes",
            &["select 1".to_string()],
            10_000,
        )
        .await
        .expect("external run should succeed");
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0]["truncated"], true);
        assert_eq!(result.errors[0]["reason"], "external_error_item_too_large");
    }

    #[tokio::test]
    async fn external_loop_maps_timeout_errors() {
        let runner = Arc::new(FakeRunner::new(vec![Err(ExternalInvokeError {
            kind: ExternalInvokeErrorKind::Timeout,
            message: "timed out".to_string(),
        })]));
        let err = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(3),
            "analyze_query_indexes",
            &["select 1".to_string()],
            10_000,
        )
        .await
        .expect_err("timeout should fail");
        assert_eq!(err.reason, "external_timeout");
        assert_eq!(err.attempt_count, 1);
    }

    #[tokio::test]
    async fn external_loop_maps_output_too_large_errors() {
        let runner = Arc::new(FakeRunner::new(vec![Err(ExternalInvokeError {
            kind: ExternalInvokeErrorKind::OutputTooLarge,
            message: "stdout exceeded cap".to_string(),
        })]));
        let err = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(3),
            "analyze_query_indexes",
            &["select 1".to_string()],
            10_000,
        )
        .await
        .expect_err("oversized output should fail");
        assert_eq!(err.reason, "external_output_too_large");
        assert_eq!(err.attempt_count, 1);
    }

    #[tokio::test]
    async fn external_loop_rejects_invalid_json_response() {
        let runner = Arc::new(FakeRunner::new(vec![Ok("not-json".to_string())]));
        let err = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(3),
            "analyze_query_indexes",
            &["select 1".to_string()],
            10_000,
        )
        .await
        .expect_err("invalid json should fail");
        assert_eq!(err.reason, "external_invalid_response");
    }

    #[tokio::test]
    async fn external_loop_rejects_invalid_using_method() {
        let runner = Arc::new(FakeRunner::new(vec![Ok(
            r#"{"recommendations":[{"table":"users","columns":["email"],"using":"btree;drop"}],"errors":[]}"#
                .to_string(),
        )]));
        let err = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(2),
            "analyze_query_indexes",
            &["select 1".to_string()],
            10_000,
        )
        .await
        .expect_err("invalid method should fail");
        assert_eq!(err.reason, "external_invalid_response");
    }

    #[tokio::test]
    async fn external_loop_rejects_empty_qualified_table_segments() {
        let runner = Arc::new(FakeRunner::new(vec![Ok(
            r#"{"recommendations":[{"table":"public..users","columns":["email"],"using":"btree"}],"errors":[]}"#
                .to_string(),
        )]));
        let err = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(2),
            "analyze_query_indexes",
            &["select 1".to_string()],
            10_000,
        )
        .await
        .expect_err("invalid qualified table should fail");
        assert_eq!(err.reason, "external_invalid_response");
    }

    #[tokio::test]
    async fn external_loop_rejects_overlong_stop_reason() {
        let long_stop_reason = "x".repeat(300);
        let runner = Arc::new(FakeRunner::new(vec![Ok(format!(
            r#"{{"recommendations":[],"errors":[],"stop_reason":"{}"}}"#,
            long_stop_reason
        ))]));
        let err = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(2),
            "analyze_query_indexes",
            &["select 1".to_string()],
            10_000,
        )
        .await
        .expect_err("overlong stop_reason should fail");
        assert_eq!(err.reason, "external_invalid_response");
    }

    #[tokio::test]
    async fn external_loop_respects_max_attempts_cap() {
        let runner = Arc::new(FakeRunner::new(vec![
            Ok(
                r#"{"recommendations":[{"table":"users","columns":["email"],"using":"btree"}],"errors":[]}"#
                    .to_string(),
            ),
            Ok(
                r#"{"recommendations":[{"table":"users","columns":["tenant_id"],"using":"btree"}],"errors":[]}"#
                    .to_string(),
            ),
        ]));
        let result = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &enabled_config(2),
            "analyze_query_indexes",
            &["select 1".to_string()],
            10_000,
        )
        .await
        .expect("external run should succeed");
        assert_eq!(result.stop_reason, "max_attempts");
        assert_eq!(result.attempt_count, 2);
        assert_eq!(result.recommendations.len(), 2);
    }

    #[tokio::test]
    async fn external_loop_fails_when_disabled() {
        let runner = Arc::new(FakeRunner::new(vec![]));
        let config = AdvisorExternalConfig::disabled();
        let err = run_external_advisor_loop(
            &(runner as Arc<dyn AdvisorExternalRunner>),
            &config,
            "analyze_query_indexes",
            &["select 1".to_string()],
            10_000,
        )
        .await
        .expect_err("disabled config should fail");
        assert_eq!(err.reason, "external_disabled");
    }

    fn _assert_failure_is_send_sync(_: AdvisorExternalFailure) {}
    fn _assert_outcome_is_send_sync(_: AdvisorExternalOutcome) {}
}
