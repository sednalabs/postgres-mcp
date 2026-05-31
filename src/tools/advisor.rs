use super::*;
use crate::advisor_extension::{AdvisorExternalFailure, run_external_advisor_loop};

const MAX_EXTERNAL_MODEL_FAILURE_MESSAGE_BYTES: usize = 512;
const MAX_WORKLOAD_QUERY_PREVIEW_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdvisorMethod {
    Dta,
    External,
}

impl AdvisorMethod {
    fn parse(raw: &str) -> Result<Self, String> {
        let normalized = raw.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "" | "dta" => Ok(Self::Dta),
            "external" => Ok(Self::External),
            _ => Err(format!(
                "Invalid advisor method {:?}. Expected one of: dta, external.",
                raw
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Dta => "dta",
            Self::External => "external",
        }
    }
}

#[derive(Debug, Clone)]
struct AdvisorExecution {
    method_requested: String,
    method_effective: String,
    fallback_reason: Option<String>,
    fallback_message: Option<String>,
    attempt_count: usize,
    stop_reason: String,
    recommendations: Value,
}

#[rmcp::tool_router(router = tool_router_postgres_advisor, vis = "pub")]
impl PostgresMcp {
    #[tool(
        name = "analyze_workload_indexes",
        description = "Analyze frequently executed queries in the database and recommend optimal indexes",
        execution(task_support = "optional"),
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn analyze_workload_indexes(
        &self,
        Parameters(args): Parameters<AnalyzeWorkloadIndexesArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let max_index_size_mb = args.max_index_size_mb.unwrap_or(10_000).max(1);
        let method_requested = args.method.unwrap_or_else(|| "dta".to_string());
        let method = match AdvisorMethod::parse(&method_requested) {
            Ok(method) => method,
            Err(message) => return Ok(error_result(self, &message, elapsed_ms(started))),
        };

        if let Err(err) = ensure_extension_ready(
            self,
            ExtensionCapability::PgStatStatements,
            "workload_source_unavailable",
        )
        .await
        {
            if err.code != "EXTENSION_UNAVAILABLE" {
                return Ok(extension_check_error_result(
                    self,
                    &err,
                    elapsed_ms(started),
                ));
            }
            return Ok(extension_unavailable_result(
                self,
                ExtensionCapability::PgStatStatements.extension_name(),
                &err.reason,
                &err.message,
                merge_payload(json!({ "recommendations": [] }), &err.details),
                elapsed_ms(started),
            ));
        }

        let version_num = server_version_num(self).await.unwrap_or(0);
        let total_time_col = if version_num >= 130_000 {
            "total_exec_time"
        } else {
            "total_time"
        };
        let avg_time_expr = if version_num >= 130_000 {
            "mean_exec_time"
        } else {
            "mean_time"
        };

        let workload_sql = format!(
            "SELECT query, calls, {avg_time_expr} AS avg_exec_time, {total_time_col} AS total_exec_time FROM pg_stat_statements WHERE calls >= 50 AND {avg_time_expr} >= 5 ORDER BY {total_time_col} DESC LIMIT 100"
        );

        let workload_rows = match self.db.execute_query_readonly(&workload_sql).await {
            Ok(rows) => rows.rows,
            Err(err) => {
                return Ok(extension_runtime_error_result(
                    self,
                    ExtensionCapability::PgStatStatements,
                    "workload_source_unavailable",
                    &err,
                    json!({ "recommendations": [] }),
                    "Error collecting workload from pg_stat_statements",
                    elapsed_ms(started),
                )
                .await);
            }
        };

        let mut queries = Vec::new();
        let mut skipped_queries = Vec::new();
        for row in &workload_rows {
            let Some(raw_query) = row.get("query").and_then(Value::as_str) else {
                continue;
            };
            match classify_workload_query_for_index_advisor(raw_query) {
                Ok(normalized_query) => {
                    if queries.len() < MAX_NUM_INDEX_TUNING_QUERIES {
                        queries.push(normalized_query);
                    }
                }
                Err(skip) => {
                    let query_preview =
                        clip_utf8_bytes(raw_query, MAX_WORKLOAD_QUERY_PREVIEW_BYTES);
                    skipped_queries.push(json!({
                        "query": query_preview,
                        "query_truncated": raw_query.len() > MAX_WORKLOAD_QUERY_PREVIEW_BYTES,
                        "reason": skip.reason,
                        "message": skip.message,
                        "calls": row.get("calls").cloned().unwrap_or(Value::Null),
                        "avg_exec_time": row.get("avg_exec_time").cloned().unwrap_or(Value::Null),
                        "total_exec_time": row.get("total_exec_time").cloned().unwrap_or(Value::Null),
                    }));
                }
            }
        }

        let execution = match execute_advisor_method(
            self,
            method_requested,
            method,
            "analyze_workload_indexes",
            &queries,
            max_index_size_mb,
        )
        .await
        {
            Ok(execution) => execution,
            Err(failure) => {
                return Ok(external_failure_result(self, failure, elapsed_ms(started)));
            }
        };

        Ok(tool_success(
            self,
            build_advisor_payload(
                execution,
                max_index_size_mb,
                json!({
                "workload_queries_scanned": workload_rows.len(),
                "workload_queries_considered": queries.len(),
                "workload_queries_skipped": skipped_queries.len(),
                "skipped_queries": skipped_queries,
                }),
            ),
            elapsed_ms(started),
        ))
    }

    #[tool(
        name = "analyze_query_indexes",
        description = "Analyze a list of (up to 10) SQL queries and recommend optimal indexes",
        execution(task_support = "optional"),
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn analyze_query_indexes(
        &self,
        Parameters(args): Parameters<AnalyzeQueryIndexesArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        if args.queries.is_empty() {
            return Ok(error_result(
                self,
                "Please provide a non-empty list of queries to analyze.",
                elapsed_ms(started),
            ));
        }
        if args.queries.len() > MAX_NUM_INDEX_TUNING_QUERIES {
            return Ok(error_result(
                self,
                &format!(
                    "Please provide a list of up to {MAX_NUM_INDEX_TUNING_QUERIES} queries to analyze."
                ),
                elapsed_ms(started),
            ));
        }
        if args
            .queries
            .iter()
            .any(|query| query.len() > MAX_SQL_INPUT_BYTES)
        {
            return Ok(error_result(
                self,
                &format!("Each query must be <= {MAX_SQL_INPUT_BYTES} bytes"),
                elapsed_ms(started),
            ));
        }

        let max_index_size_mb = args.max_index_size_mb.unwrap_or(10_000).max(1);
        let method_requested = args.method.unwrap_or_else(|| "dta".to_string());
        let method = match AdvisorMethod::parse(&method_requested) {
            Ok(method) => method,
            Err(message) => return Ok(error_result(self, &message, elapsed_ms(started))),
        };

        let execution = match execute_advisor_method(
            self,
            method_requested,
            method,
            "analyze_query_indexes",
            &args.queries,
            max_index_size_mb,
        )
        .await
        {
            Ok(execution) => execution,
            Err(failure) => {
                return Ok(external_failure_result(self, failure, elapsed_ms(started)));
            }
        };

        Ok(tool_success(
            self,
            build_advisor_payload(execution, max_index_size_mb, json!({})),
            elapsed_ms(started),
        ))
    }

    #[tool(
        name = "get_top_queries",
        description = "Reports the slowest or most resource-intensive queries using data from the 'pg_stat_statements' extension.",
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn get_top_queries(
        &self,
        Parameters(args): Parameters<GetTopQueriesArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let sort_by = args
            .sort_by
            .as_deref()
            .unwrap_or("resources")
            .trim()
            .to_ascii_lowercase();
        let limit = args.limit.unwrap_or(10).max(1);

        if let Err(err) = ensure_extension_ready(
            self,
            ExtensionCapability::PgStatStatements,
            "top_queries_source_unavailable",
        )
        .await
        {
            if err.code != "EXTENSION_UNAVAILABLE" {
                return Ok(extension_check_error_result(
                    self,
                    &err,
                    elapsed_ms(started),
                ));
            }
            return Ok(extension_unavailable_result(
                self,
                ExtensionCapability::PgStatStatements.extension_name(),
                &err.reason,
                &err.message,
                merge_payload(json!({ "queries": [] }), &err.details),
                elapsed_ms(started),
            ));
        }

        let version_num = server_version_num(self).await.unwrap_or(0);
        let total_time_col = if version_num >= 130_000 {
            "total_exec_time"
        } else {
            "total_time"
        };
        let mean_time_col = if version_num >= 130_000 {
            "mean_exec_time"
        } else {
            "mean_time"
        };

        let sql = match sort_by.as_str() {
            "mean_time" => format!(
                "SELECT query, calls, {total_time_col}, {mean_time_col}, rows FROM pg_stat_statements ORDER BY {mean_time_col} DESC LIMIT {limit}"
            ),
            "total_time" => format!(
                "SELECT query, calls, {total_time_col}, {mean_time_col}, rows FROM pg_stat_statements ORDER BY {total_time_col} DESC LIMIT {limit}"
            ),
            "resources" => format!(
                "WITH resource_fractions AS (SELECT query, calls, rows, {total_time_col} AS total_exec_time, {mean_time_col} AS mean_exec_time, stddev_exec_time, shared_blks_hit, shared_blks_read, shared_blks_dirtied, wal_bytes, {total_time_col} / NULLIF(SUM({total_time_col}) OVER (), 0) AS total_exec_time_frac, (shared_blks_hit + shared_blks_read) / NULLIF(SUM(shared_blks_hit + shared_blks_read) OVER (), 0) AS shared_blks_accessed_frac, shared_blks_read / NULLIF(SUM(shared_blks_read) OVER (), 0) AS shared_blks_read_frac, shared_blks_dirtied / NULLIF(SUM(shared_blks_dirtied) OVER (), 0) AS shared_blks_dirtied_frac, wal_bytes / NULLIF(SUM(wal_bytes) OVER (), 0) AS total_wal_bytes_frac FROM pg_stat_statements) SELECT * FROM resource_fractions WHERE total_exec_time_frac > 0.05 OR shared_blks_accessed_frac > 0.05 OR shared_blks_read_frac > 0.05 OR shared_blks_dirtied_frac > 0.05 OR total_wal_bytes_frac > 0.05 ORDER BY total_exec_time DESC LIMIT {limit}"
            ),
            _ => {
                return Ok(error_result(
                    self,
                    "Invalid sort criteria. Please use 'resources', 'mean_time', or 'total_time'.",
                    elapsed_ms(started),
                ));
            }
        };

        match self.db.execute_query_readonly(&sql).await {
            Ok(output) => Ok(tool_success(self, json!(output.rows), elapsed_ms(started))),
            Err(err) => Ok(extension_runtime_error_result(
                self,
                ExtensionCapability::PgStatStatements,
                "top_queries_source_unavailable",
                &err,
                json!({ "queries": [] }),
                "Error getting slow queries",
                elapsed_ms(started),
            )
            .await),
        }
    }
}

async fn execute_advisor_method(
    server: &PostgresMcp,
    method_requested: String,
    method: AdvisorMethod,
    tool_name: &str,
    queries: &[String],
    max_index_size_mb: i64,
) -> Result<AdvisorExecution, AdvisorExternalFailure> {
    if queries.is_empty() {
        return Ok(AdvisorExecution {
            method_requested,
            method_effective: method.as_str().to_string(),
            fallback_reason: None,
            fallback_message: None,
            attempt_count: 0,
            stop_reason: "no_queries".to_string(),
            recommendations: json!({
                "recommendations": [],
                "errors": [],
            }),
        });
    }

    match method {
        AdvisorMethod::Dta => {
            let recommendations = analyze_queries_for_indexes(server, queries).await;
            Ok(AdvisorExecution {
                method_requested,
                method_effective: AdvisorMethod::Dta.as_str().to_string(),
                fallback_reason: None,
                fallback_message: None,
                attempt_count: 1,
                stop_reason: "deterministic_single_pass".to_string(),
                recommendations,
            })
        }
        AdvisorMethod::External => {
            match run_external_advisor_loop(
                &server.advisor_external_runner,
                &server.advisor_external,
                tool_name,
                queries,
                max_index_size_mb,
            )
            .await
            {
                Ok(outcome) => Ok(AdvisorExecution {
                    method_requested,
                    method_effective: AdvisorMethod::External.as_str().to_string(),
                    fallback_reason: None,
                    fallback_message: None,
                    attempt_count: outcome.attempt_count,
                    stop_reason: outcome.stop_reason,
                    recommendations: json!({
                        "recommendations": outcome.recommendations,
                        "errors": outcome.errors,
                    }),
                }),
                Err(failure) if server.advisor_external.fallback_to_dta => {
                    let recommendations = analyze_queries_for_indexes(server, queries).await;
                    Ok(AdvisorExecution {
                        method_requested,
                        method_effective: AdvisorMethod::Dta.as_str().to_string(),
                        fallback_reason: Some(failure.reason.to_string()),
                        fallback_message: Some(model_safe_external_message(
                            failure.reason,
                            &failure.message,
                        )),
                        attempt_count: failure.attempt_count,
                        stop_reason: "fallback_to_dta".to_string(),
                        recommendations,
                    })
                }
                Err(failure) => Err(failure),
            }
        }
    }
}

fn external_failure_result(
    server: &PostgresMcp,
    failure: AdvisorExternalFailure,
    elapsed_ms: u64,
) -> CallToolResult {
    let code = match failure.reason {
        "external_disabled" | "external_not_configured" => "ADVISOR_EXTERNAL_UNAVAILABLE",
        _ => "ADVISOR_EXTERNAL_FAILED",
    };
    contract_error(
        server,
        json!({
            "error": model_safe_external_message(failure.reason, &failure.message),
            "code": code,
            "reason": failure.reason,
            "attempt_count": failure.attempt_count,
        }),
        elapsed_ms,
        json!({}),
    )
}

fn model_safe_external_message(reason: &str, message: &str) -> String {
    let normalized = message.split_whitespace().collect::<Vec<_>>().join(" ");
    let fallback = match reason {
        "external_timeout" => "external advisor timed out".to_string(),
        "external_output_too_large" => {
            "external advisor output exceeded configured limit".to_string()
        }
        _ => "external advisor execution failed".to_string(),
    };
    let base = if normalized.is_empty() {
        fallback
    } else {
        normalized
    };
    clip_utf8_bytes(&base, MAX_EXTERNAL_MODEL_FAILURE_MESSAGE_BYTES)
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

fn build_advisor_payload(
    execution: AdvisorExecution,
    max_index_size_mb: i64,
    extra_fields: Value,
) -> Value {
    let method_effective = execution.method_effective;
    let mut payload = json!({
        "method": method_effective.clone(),
        "method_requested": execution.method_requested,
        "method_effective": method_effective,
        "fallback_reason": execution.fallback_reason,
        "fallback_message": execution.fallback_message,
        "attempt_count": execution.attempt_count,
        "stop_reason": execution.stop_reason,
        "max_index_size_mb": max_index_size_mb,
        "recommendations": execution.recommendations,
    });
    if let Some(payload_obj) = payload.as_object_mut()
        && let Some(extra_obj) = extra_fields.as_object()
    {
        for (key, value) in extra_obj {
            payload_obj.insert(key.clone(), value.clone());
        }
    }
    payload
}

#[cfg(test)]
mod tests {
    use super::{
        AdvisorExecution, AdvisorMethod, build_advisor_payload, model_safe_external_message,
    };
    use serde_json::json;

    #[test]
    fn advisor_method_parse_defaults_to_dta() {
        assert_eq!(
            AdvisorMethod::parse("").expect("empty should parse"),
            AdvisorMethod::Dta
        );
        assert_eq!(
            AdvisorMethod::parse("dta").expect("dta should parse"),
            AdvisorMethod::Dta
        );
    }

    #[test]
    fn advisor_method_parse_accepts_external() {
        assert_eq!(
            AdvisorMethod::parse("external").expect("external should parse"),
            AdvisorMethod::External
        );
    }

    #[test]
    fn advisor_method_parse_rejects_unknown_method() {
        let err = AdvisorMethod::parse("provider_x").expect_err("invalid method should fail");
        assert!(err.contains("Expected one of: dta, external"));
    }

    #[test]
    fn build_advisor_payload_merges_execution_and_extra_fields() {
        let payload = build_advisor_payload(
            AdvisorExecution {
                method_requested: "external".to_string(),
                method_effective: "dta".to_string(),
                fallback_reason: Some("external_timeout".to_string()),
                fallback_message: Some("external timed out".to_string()),
                attempt_count: 2,
                stop_reason: "fallback_to_dta".to_string(),
                recommendations: json!({
                    "recommendations": [],
                    "errors": [],
                }),
            },
            10_000,
            json!({
                "workload_queries_scanned": 7
            }),
        );
        assert_eq!(payload["method"], "dta");
        assert_eq!(payload["method_requested"], "external");
        assert_eq!(payload["attempt_count"], 2);
        assert_eq!(payload["workload_queries_scanned"], 7);
    }

    #[test]
    fn model_safe_external_message_normalizes_and_bounds_output() {
        let noisy = "error line 1\n\n   extra spacing\tline 2";
        let normalized = model_safe_external_message("external_non_zero_exit", noisy);
        assert!(!normalized.contains('\n'));
        assert!(!normalized.contains('\t'));
        assert!(normalized.contains("error line 1 extra spacing line 2"));

        let long = "x".repeat(4_096);
        let clipped = model_safe_external_message("external_non_zero_exit", &long);
        assert!(clipped.len() <= super::MAX_EXTERNAL_MODEL_FAILURE_MESSAGE_BYTES);
    }
}
