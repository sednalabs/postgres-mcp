//! # MCP Tool Handlers
//!
//! PostgreSQL tools exposed by postgres-mcp.
//!
//! ## Rationale
//! Preserve the core Python tool surface while optimizing for low startup
//! latency and predictable stdio behavior.
//!
//! ## Security Boundaries
//! * Read-oriented SQL tools enforce read-safe checks before execution.
//! * Identifier/literal interpolation in internal SQL uses explicit quoting.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use regex::Regex;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content};
use rmcp::tool;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;

use crate::config::{ResponseOutputMode, StartupRole};
use crate::db::{
    DbError, QueryColumn, QueryOutput, sql_quote_ident, sql_quote_literal,
    sql_quote_qualified_ident,
};
use crate::server::{ExtensionCapability, ExtensionUnavailableStatus, PostgresMcp};
use crate::sql_safety::classify_restricted_sql;

const MAX_NUM_INDEX_TUNING_QUERIES: usize = 10;
const MAX_SQL_INPUT_BYTES: usize = 128 * 1024;
const MAX_HYPOTHETICAL_INDEXES: usize = 32;
const MAX_HYPOTHETICAL_INDEX_COLUMNS: usize = 16;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_QUALIFIED_IDENTIFIER_BYTES: usize = 256;
const EXTENSION_REFRESH_WAIT_STEP: Duration = Duration::from_millis(20);
const EXTENSION_REFRESH_WAIT_ATTEMPTS: usize = 5;
const CURSOR_PREFIX: &str = "v3";
const CURSOR_SIGNATURE_HEX_LEN: usize = 64;
const VACUUM_HEALTH_SQL: &str = "SELECT n.nspname AS schema_name, c.relname AS table_name, format('%I.%I', n.nspname, c.relname) AS relation, 2146483648 - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid)) AS transactions_left FROM pg_class c INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_class t ON c.reltoastrelid = t.oid WHERE c.relkind = 'r' AND (2146483648 - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid))) < 10000000 ORDER BY transactions_left, schema_name, table_name";
const CONSTRAINT_HEALTH_SQL: &str = "SELECT nsp.nspname AS schema_name, rel.relname AS table_name, format('%I.%I', nsp.nspname, rel.relname) AS relation, con.conname AS constraint_name, fnsp.nspname AS referenced_schema_name, frel.relname AS referenced_table_name, CASE WHEN fnsp.nspname IS NULL OR frel.relname IS NULL THEN NULL ELSE format('%I.%I', fnsp.nspname, frel.relname) END AS referenced_relation FROM pg_catalog.pg_constraint con INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid LEFT JOIN pg_catalog.pg_class frel ON frel.oid = con.confrelid LEFT JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace LEFT JOIN pg_catalog.pg_namespace fnsp ON fnsp.oid = frel.relnamespace WHERE con.convalidated = false ORDER BY schema_name, table_name, constraint_name";

const INSTALL_PG_STAT_STATEMENTS_MESSAGE: &str = "The pg_stat_statements extension is required to report slow queries, but it is not currently installed. You can install it by running: CREATE EXTENSION pg_stat_statements;";
const MAX_CURRENCY_FORMATTED_COLUMNS: usize = 32;
const CURRENCY_SUFFIX: &str = "_formatted";

fn index_method_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-z_][a-z0-9_]*$").expect("index-method regex must compile"))
}

#[derive(Debug, Deserialize, JsonSchema, Default)]
pub struct ListSchemasArgs {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StructuredRowsToolResultSchema {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<BTreeMap<String, Value>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<BTreeMap<String, Value>>,
    pub meta: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StructuredTableDataSchema {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StructuredTableToolResultSchema {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<StructuredTableDataSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<BTreeMap<String, Value>>,
    pub meta: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StructuredObjectToolResultSchema {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<BTreeMap<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<BTreeMap<String, Value>>,
    pub meta: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListObjectsArgs {
    pub schema_name: String,
    #[serde(default)]
    pub object_type: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub name_like: Option<String>,
    #[serde(default)]
    pub name_prefix: Option<String>,
    #[serde(default)]
    pub name_contains: Option<String>,
    #[serde(default)]
    pub name_exact: Option<String>,
    #[serde(default)]
    pub name_pattern: Option<String>,
    #[serde(default)]
    pub include_columns: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryNameFilterMode {
    Exact,
    Prefix,
    Contains,
    Pattern,
}

impl DiscoveryNameFilterMode {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Prefix => "prefix",
            Self::Contains => "contains",
            Self::Pattern => "pattern",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CompiledDiscoveryNameFilter {
    pub mode: DiscoveryNameFilterMode,
    pub source_arg: &'static str,
    pub pattern: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetObjectDetailsArgs {
    pub schema_name: String,
    pub object_name: String,
    #[serde(default)]
    pub object_type: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResponseFormattingMode {
    Currency,
    Markdown,
}

impl ResponseFormattingMode {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "currency" => Some(Self::Currency),
            "markdown" => Some(Self::Markdown),
            _ => None,
        }
    }

    fn validation_message(raw: &str) -> String {
        let trimmed = raw.trim();
        let correction_hint = if trimmed.eq_ignore_ascii_case("compact") {
            " If you wanted compact metadata, use metadata_verbosity=compact or output_mode=data_only."
        } else {
            ""
        };
        format!(
            "response_formatting_mode supports `currency` and compatibility alias `markdown` (normalized to output_mode=table).{} For compact verification loops, use profile=fast_agent. Got {:?}. Example: {{\"output_mode\":\"table\"}}",
            correction_hint, raw,
        )
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteSqlExportFormat {
    Csv,
    Tsv,
    Jsonl,
}

impl ExecuteSqlExportFormat {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Jsonl => "jsonl",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteSqlMetadataVerbosity {
    Compact,
    Standard,
    Full,
}

impl ExecuteSqlMetadataVerbosity {
    const CANONICAL_MODE_LIST: &str = "compact, standard, full";

    fn parse_with_alias(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "compact" | "low" => Some(Self::Compact),
            "standard" => Some(Self::Standard),
            "full" => Some(Self::Full),
            _ => None,
        }
    }

    fn validation_message(raw: &str) -> String {
        format!(
            "metadata_verbosity must be one of [{}] (alias: low -> compact); got {:?}. Example: {{\"metadata_verbosity\":\"compact\"}}",
            Self::CANONICAL_MODE_LIST,
            raw,
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Compact => "compact",
            Self::Standard => "standard",
            Self::Full => "full",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteSqlCountMode {
    None,
    Exact,
    Estimated,
    Async,
}

impl ExecuteSqlCountMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Exact => "exact",
            Self::Estimated => "estimated",
            Self::Async => "async",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteSqlProfile {
    FastAgent,
    HumanDebug,
    HeavyView,
}

impl ExecuteSqlProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FastAgent => "fast_agent",
            Self::HumanDebug => "human_debug",
            Self::HeavyView => "heavy_view",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadQueryProfile {
    Compact,
    Inspect,
}

impl ReadQueryProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Compact => "compact",
            Self::Inspect => "inspect",
        }
    }
}

fn deserialize_optional_execute_sql_metadata_verbosity<'de, D>(
    deserializer: D,
) -> Result<Option<ExecuteSqlMetadataVerbosity>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    raw.map(|value| {
        ExecuteSqlMetadataVerbosity::parse_with_alias(&value).ok_or_else(|| {
            serde::de::Error::custom(ExecuteSqlMetadataVerbosity::validation_message(&value))
        })
    })
    .transpose()
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct ExecuteSqlArgs {
    pub sql: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub params: Option<Vec<Value>>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub max_rows: Option<usize>,
    #[serde(default)]
    pub max_cell_chars: Option<usize>,
    #[serde(
        default,
        deserialize_with = "crate::config::deserialize_optional_response_output_mode"
    )]
    pub output_mode: Option<ResponseOutputMode>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_response_formatting_mode"
    )]
    pub response_formatting_mode: Option<ResponseFormattingMode>,
    #[serde(default)]
    pub currency_columns: Option<Vec<String>>,
    #[serde(default)]
    pub summary_only: bool,
    #[serde(default)]
    pub include_total_row_count: Option<bool>,
    #[serde(default)]
    pub count_mode: Option<ExecuteSqlCountMode>,
    #[serde(default)]
    pub profile: Option<ExecuteSqlProfile>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_execute_sql_metadata_verbosity"
    )]
    pub metadata_verbosity: Option<ExecuteSqlMetadataVerbosity>,
    #[serde(default)]
    pub describe_only: bool,
    #[serde(default)]
    pub export_to_file: bool,
    #[serde(default)]
    pub export_format: Option<ExecuteSqlExportFormat>,
    #[serde(default)]
    #[schemars(range(min = 1, max = 300000))]
    pub statement_timeout_ms: Option<u64>,
    #[serde(default)]
    pub diagnose_on_timeout: Option<bool>,
    #[serde(default)]
    pub preflight_check: Option<bool>,
}

fn deserialize_optional_response_formatting_mode<'de, D>(
    deserializer: D,
) -> Result<Option<ResponseFormattingMode>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    raw.map(|value| {
        ResponseFormattingMode::parse(&value).ok_or_else(|| {
            serde::de::Error::custom(ResponseFormattingMode::validation_message(&value))
        })
    })
    .transpose()
}

#[derive(Debug, Clone, JsonSchema)]
pub struct QueryStartArgs {
    #[serde(flatten)]
    pub execute_sql: ExecuteSqlArgs,
    #[serde(default, rename = "execute_sql")]
    pub execute_sql_nested: Option<ExecuteSqlArgs>,
    #[serde(skip)]
    top_level_summary_only_present: bool,
    #[serde(skip)]
    top_level_describe_only_present: bool,
    #[serde(skip)]
    top_level_export_to_file_present: bool,
}

#[derive(Debug, Deserialize)]
struct QueryStartArgsSerde {
    #[serde(flatten)]
    execute_sql: ExecuteSqlArgs,
    #[serde(default, rename = "execute_sql")]
    execute_sql_nested: Option<ExecuteSqlArgs>,
}

impl<'de> Deserialize<'de> for QueryStartArgs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let (
            top_level_summary_only_present,
            top_level_describe_only_present,
            top_level_export_to_file_present,
        ) = value.as_object().map_or((false, false, false), |object| {
            (
                object.contains_key("summary_only"),
                object.contains_key("describe_only"),
                object.contains_key("export_to_file"),
            )
        });
        let parsed: QueryStartArgsSerde = serde_json::from_value(value)
            .map_err(|err| <D::Error as serde::de::Error>::custom(err.to_string()))?;
        Ok(Self {
            execute_sql: parsed.execute_sql,
            execute_sql_nested: parsed.execute_sql_nested,
            top_level_summary_only_present,
            top_level_describe_only_present,
            top_level_export_to_file_present,
        })
    }
}

impl ExecuteSqlArgs {
    fn is_effectively_empty(&self) -> bool {
        self.sql.is_none()
            && self.session_id.is_none()
            && self.params.is_none()
            && self.cursor.is_none()
            && self.max_rows.is_none()
            && self.max_cell_chars.is_none()
            && self.output_mode.is_none()
            && self.response_formatting_mode.is_none()
            && self.currency_columns.is_none()
            && !self.summary_only
            && self.include_total_row_count.is_none()
            && self.count_mode.is_none()
            && self.profile.is_none()
            && self.metadata_verbosity.is_none()
            && !self.describe_only
            && !self.export_to_file
            && self.export_format.is_none()
            && self.statement_timeout_ms.is_none()
            && self.diagnose_on_timeout.is_none()
            && self.preflight_check.is_none()
    }
}

fn merge_execute_sql_args(
    primary: ExecuteSqlArgs,
    fallback: ExecuteSqlArgs,
    primary_summary_only_explicit: bool,
    primary_describe_only_explicit: bool,
    primary_export_to_file_explicit: bool,
) -> ExecuteSqlArgs {
    let primary_is_empty = primary.is_effectively_empty();
    ExecuteSqlArgs {
        sql: primary.sql.or(fallback.sql),
        session_id: primary.session_id.or(fallback.session_id),
        params: primary.params.or(fallback.params),
        cursor: primary.cursor.or(fallback.cursor),
        max_rows: primary.max_rows.or(fallback.max_rows),
        max_cell_chars: primary.max_cell_chars.or(fallback.max_cell_chars),
        output_mode: primary.output_mode.or(fallback.output_mode),
        response_formatting_mode: primary
            .response_formatting_mode
            .or(fallback.response_formatting_mode),
        currency_columns: primary.currency_columns.or(fallback.currency_columns),
        summary_only: if primary_summary_only_explicit {
            primary.summary_only
        } else if primary_is_empty {
            fallback.summary_only
        } else {
            primary.summary_only
        },
        include_total_row_count: primary
            .include_total_row_count
            .or(fallback.include_total_row_count),
        count_mode: primary.count_mode.or(fallback.count_mode),
        profile: primary.profile.or(fallback.profile),
        metadata_verbosity: primary.metadata_verbosity.or(fallback.metadata_verbosity),
        describe_only: if primary_describe_only_explicit {
            primary.describe_only
        } else if primary_is_empty {
            fallback.describe_only
        } else {
            primary.describe_only
        },
        export_to_file: if primary_export_to_file_explicit {
            primary.export_to_file
        } else if primary_is_empty {
            fallback.export_to_file
        } else {
            primary.export_to_file
        },
        export_format: primary.export_format.or(fallback.export_format),
        statement_timeout_ms: primary
            .statement_timeout_ms
            .or(fallback.statement_timeout_ms),
        diagnose_on_timeout: primary.diagnose_on_timeout.or(fallback.diagnose_on_timeout),
        preflight_check: primary.preflight_check.or(fallback.preflight_check),
    }
}

impl QueryStartArgs {
    pub fn into_execute_sql_args(self) -> ExecuteSqlArgs {
        let Self {
            execute_sql,
            execute_sql_nested,
            top_level_summary_only_present,
            top_level_describe_only_present,
            top_level_export_to_file_present,
        } = self;
        match execute_sql_nested {
            Some(nested)
                if execute_sql.is_effectively_empty()
                    && !top_level_summary_only_present
                    && !top_level_describe_only_present
                    && !top_level_export_to_file_present =>
            {
                nested
            }
            Some(nested) => merge_execute_sql_args(
                execute_sql,
                nested,
                top_level_summary_only_present,
                top_level_describe_only_present,
                top_level_export_to_file_present,
            ),
            None => execute_sql,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryStatusArgs {
    pub job_id: String,
    #[serde(default)]
    #[schemars(range(min = 1, max = 3600000))]
    pub wait_ms: Option<u64>,
    #[serde(default)]
    pub wait_until_terminal: bool,
}

#[derive(Debug, JsonSchema)]
pub struct QueryStartAndWaitArgs {
    #[serde(flatten)]
    pub query_start: QueryStartArgs,
    #[serde(default)]
    #[schemars(range(min = 1, max = 3600000))]
    pub wait_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct QueryStartAndWaitArgsSerde {
    #[serde(flatten)]
    query_start: QueryStartArgs,
    #[serde(default)]
    wait_ms: Option<u64>,
}

impl<'de> Deserialize<'de> for QueryStartAndWaitArgs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let parsed = QueryStartAndWaitArgsSerde::deserialize(deserializer)?;
        Ok(Self {
            query_start: parsed.query_start,
            wait_ms: parsed.wait_ms,
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryCancelArgs {
    pub job_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExplainQueryArgs {
    pub sql: String,
    #[serde(default)]
    pub analyze: bool,
    #[serde(default)]
    pub hypothetical_indexes: Vec<HypotheticalIndexArg>,
}

#[derive(Debug, Deserialize, serde::Serialize, JsonSchema)]
pub struct HypotheticalIndexArg {
    pub table: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub using: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AnalyzeWorkloadIndexesArgs {
    #[serde(default)]
    pub max_index_size_mb: Option<i64>,
    #[serde(default)]
    pub method: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AnalyzeQueryIndexesArgs {
    pub queries: Vec<String>,
    #[serde(default)]
    pub max_index_size_mb: Option<i64>,
    #[serde(default)]
    pub method: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AnalyzeDbHealthArgs {
    #[serde(default)]
    pub health_type: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetTopQueriesArgs {
    #[serde(default)]
    pub sort_by: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum HealthType {
    Index,
    Connection,
    Vacuum,
    Sequence,
    Replication,
    Buffer,
    Constraint,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IndexRecommendation {
    table: String,
    columns: Vec<String>,
    using: String,
    reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkloadQuerySkipReason {
    reason: &'static str,
    message: &'static str,
}

mod advisor;
mod health;
mod query;
mod query_surface;
mod schema;

impl PostgresMcp {
    pub fn tool_router_postgres() -> ToolRouter<PostgresMcp> {
        Self::tool_router_postgres_schema()
            + Self::tool_router_postgres_query()
            + Self::tool_router_postgres_query_surface()
            + Self::tool_router_postgres_health()
            + Self::tool_router_postgres_advisor()
    }
}
fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn canonical_response_meta(server: &PostgresMcp, meta: Value, elapsed_ms: u64) -> Value {
    if let Some(obj) = meta.as_object() {
        let mut payload = obj.clone();
        payload.insert("elapsed_ms".to_string(), json!(elapsed_ms));
        payload.insert(
            "capabilities".to_string(),
            server.startup_capabilities_meta(),
        );
        return Value::Object(payload);
    }
    json!({
        "elapsed_ms": elapsed_ms,
        "capabilities": server.startup_capabilities_meta(),
    })
}

fn contract_success(
    server: &PostgresMcp,
    payload: Value,
    elapsed_ms: u64,
    meta: Value,
) -> CallToolResult {
    CallToolResult::structured(json!({
        "ok": true,
        "data": payload,
        "meta": canonical_response_meta(server, meta, elapsed_ms),
    }))
}

fn contract_error(
    server: &PostgresMcp,
    payload: Value,
    elapsed_ms: u64,
    meta: Value,
) -> CallToolResult {
    let payload = normalize_error_payload_for_role(server.startup_role, payload);
    CallToolResult::structured(json!({
        "ok": false,
        "error": payload,
        "meta": canonical_response_meta(server, meta, elapsed_ms),
    }))
}

pub(crate) fn normalize_error_payload_for_role(startup_role: StartupRole, payload: Value) -> Value {
    let Value::Object(mut object) = payload else {
        return payload;
    };
    if startup_role == StartupRole::Runtime {
        object.remove("detail");
        object.remove("position");
    }
    let detail_level = match startup_role {
        StartupRole::Runtime => "minimal",
        StartupRole::Migrator => "detailed",
    };
    object.insert("detail_level".to_string(), json!(detail_level));
    let retryable = classify_retryable_error(&object);
    object.insert("retryable".to_string(), json!(retryable));
    object.insert(
        "error_class".to_string(),
        json!(classify_error_class(&object)),
    );
    object.insert(
        "fingerprint".to_string(),
        json!(error_fingerprint(&object, detail_level)),
    );
    Value::Object(object)
}

fn classify_retryable_error(payload: &Map<String, Value>) -> bool {
    let code = payload
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let reason = payload
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let sqlstate = payload
        .get("sqlstate")
        .and_then(Value::as_str)
        .unwrap_or_default();

    if matches!(
        code,
        "DB_QUERY_TIMEOUT"
            | "DB_CONNECT_FAILED"
            | "DB_CONNECTION_DRIVER_FAILED"
            | "STARTUP_DB_CONNECT_TIMEOUT"
            | "QUERY_JOB_CAPACITY_REACHED"
    ) {
        return true;
    }
    if sqlstate.starts_with("08")
        || sqlstate == "40001"
        || sqlstate == "40P01"
        || sqlstate == "57P03"
        || sqlstate.starts_with("53")
    {
        return true;
    }
    matches!(
        reason,
        "db_connect_failed" | "db_query_timeout" | "startup_db_connect_timeout"
    )
}

fn classify_error_class(payload: &Map<String, Value>) -> &'static str {
    let code = payload
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let reason = payload
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let sqlstate = payload
        .get("sqlstate")
        .and_then(Value::as_str)
        .unwrap_or_default();

    let timeout_signal = payload
        .get("error")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_lowercase())
        .is_some_and(|value| value.contains("statement timeout"));
    let timeout_detail_signal = payload
        .get("detail")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_lowercase())
        .is_some_and(|value| value.contains("statement timeout"));

    if code == "INVALID_REQUEST" || reason == "invalid_request" || code == "SQL_INPUT_INVALID" {
        return "invalid_request";
    }

    if matches!(
        code,
        "METADATA_ACCESS_DENIED"
            | "RUNTIME_ROLE_DDL_BLOCKED"
            | "STARTUP_DEGRADED_READ_ONLY"
            | "TOOL_CIRCUIT_OPEN"
    ) || matches!(
        reason,
        "metadata_access_denied"
            | "restricted_sql"
            | "startup_role_runtime"
            | "startup_degraded_read_only"
    ) {
        return "policy_denied";
    }

    if code == "QUERY_JOB_NOT_FOUND" || reason == "query_job_not_found" {
        return "not_found";
    }
    if code == "QUERY_JOB_CAPACITY_REACHED" || reason == "query_job_capacity_reached" {
        return "resource_limit";
    }

    if code == "QUERY_CANCELED" || reason == "query_canceled" || sqlstate == "57014" {
        if timeout_signal || timeout_detail_signal || code == "DB_QUERY_TIMEOUT" {
            return "statement_timeout";
        }
        return "client_cancelled";
    }

    if code == "DB_QUERY_TIMEOUT" || timeout_signal || timeout_detail_signal {
        return "statement_timeout";
    }

    if code == "DB_CONNECT_CONFIG_INVALID" || reason == "db_connect_config_invalid" {
        return "configuration";
    }

    if sqlstate.starts_with("08")
        || matches!(
            code,
            "DB_CONNECT_FAILED" | "DB_CONNECTION_DRIVER_FAILED" | "STARTUP_DB_CONNECT_TIMEOUT"
        )
        || matches!(
            reason,
            "db_connect_failed" | "db_connection_driver_failed" | "startup_db_connect_timeout"
        )
    {
        return "transient_network";
    }

    if sqlstate == "42501" {
        return "permission";
    }
    if sqlstate == "42601" {
        return "syntax";
    }
    if sqlstate == "42P01" || sqlstate == "42703" {
        return "object_not_found";
    }
    if sqlstate == "40001" || sqlstate == "40P01" {
        return "serialization";
    }
    if sqlstate.starts_with("53") || sqlstate == "57P03" {
        return "resource_limit";
    }
    if sqlstate.starts_with("42") {
        return "syntax_or_semantic";
    }

    "unknown"
}

fn error_fingerprint(payload: &Map<String, Value>, detail_level: &str) -> String {
    let code = payload
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let reason = payload
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let sqlstate = payload
        .get("sqlstate")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(code.as_bytes());
    hasher.update([0u8]);
    hasher.update(reason.as_bytes());
    hasher.update([0u8]);
    hasher.update(sqlstate.as_bytes());
    hasher.update([0u8]);
    hasher.update(detail_level.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    let short = &hex[..12];
    format!("err_{short}")
}

fn error_result(server: &PostgresMcp, message: &str, elapsed_ms: u64) -> CallToolResult {
    let payload = json!({
        "error": message,
        "code": "INVALID_REQUEST",
        "reason": "invalid_request",
    });
    contract_error(server, payload, elapsed_ms, json!({}))
}

fn policy_error_result(
    server: &PostgresMcp,
    code: &str,
    message: &str,
    reason: &str,
    elapsed_ms: u64,
) -> CallToolResult {
    contract_error(
        server,
        json!({
            "error": message,
            "code": code,
            "reason": reason,
        }),
        elapsed_ms,
        json!({}),
    )
}

fn db_error_result(
    server: &PostgresMcp,
    context: &str,
    err: &DbError,
    elapsed_ms: u64,
) -> CallToolResult {
    contract_error(
        server,
        json!({
            "error": format!("{context}: {err}"),
            "code": err.code(),
            "reason": err.reason(),
            "sqlstate": err.sqlstate(),
            "detail": err.detail(),
            "hint": err.hint(),
            "position": err.position(),
        }),
        elapsed_ms,
        json!({}),
    )
}

fn extension_check_error_result(
    server: &PostgresMcp,
    err: &ExtensionCheckError,
    elapsed_ms: u64,
) -> CallToolResult {
    let mut payload = json!({
        "error": err.message,
        "code": err.code,
        "reason": err.reason,
    });
    if let Some(map) = payload.as_object_mut()
        && let Some(details) = err.details.as_object()
    {
        for (key, value) in details {
            map.insert(key.clone(), value.clone());
        }
    }
    contract_error(server, payload, elapsed_ms, json!({}))
}

fn extension_unavailable_result(
    server: &PostgresMcp,
    extension: &str,
    reason: &str,
    message: &str,
    mut payload: Value,
    elapsed_ms: u64,
) -> CallToolResult {
    if !payload.is_object() {
        payload = json!({});
    }
    if let Some(map) = payload.as_object_mut() {
        map.insert("message".to_string(), json!(message));
        map.insert("code".to_string(), json!("EXTENSION_UNAVAILABLE"));
        map.insert("reason".to_string(), json!(reason));
        map.insert("extension".to_string(), json!(extension));
    }
    contract_error(server, payload, elapsed_ms, json!({}))
}

fn merge_payload(mut payload: Value, details: &Value) -> Value {
    let Some(payload_obj) = payload.as_object_mut() else {
        return payload;
    };
    let Some(details_obj) = details.as_object() else {
        return payload;
    };
    for (key, value) in details_obj {
        payload_obj.insert(key.clone(), value.clone());
    }
    payload
}

fn rows_to_tuple_rows(rows: &[Map<String, Value>], columns: &[QueryColumn]) -> Value {
    let values = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|col| row.get(&col.name).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Value::Array(values.into_iter().map(Value::Array).collect())
}

fn resolve_effective_output_mode(
    output: &QueryOutput,
    requested_mode: ResponseOutputMode,
    auto_tabular_mode: ResponseOutputMode,
) -> ResponseOutputMode {
    match requested_mode {
        ResponseOutputMode::Auto => {
            if output.rows.len() == 1 && output.columns.len() == 1 {
                ResponseOutputMode::Scalar
            } else {
                auto_tabular_mode
            }
        }
        mode => mode,
    }
}

fn duplicate_column_aliases(columns: &[QueryColumn]) -> Vec<String> {
    const DUPLICATE_SUFFIX_MARKER: &str = "__dup";
    let column_name_set = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    let auto_aliased_names = columns
        .iter()
        .filter_map(|column| {
            let (base_name, suffix) = column.name.rsplit_once(DUPLICATE_SUFFIX_MARKER)?;
            let suffix_number = suffix.parse::<usize>().ok()?;
            if suffix_number < 2 || base_name.is_empty() || !column_name_set.contains(base_name) {
                return None;
            }
            Some(column.name.clone())
        })
        .collect::<Vec<_>>();
    auto_aliased_names
}

fn duplicate_column_alias_hints(columns: &[QueryColumn]) -> Vec<String> {
    let auto_aliased_names = duplicate_column_aliases(columns);
    if auto_aliased_names.is_empty() {
        return Vec::new();
    }

    vec![format!(
        "duplicate output column names were auto-aliased ({}) to preserve deterministic keys; prefer explicit SQL aliases or output_mode='rows_safe'/'tuples'",
        auto_aliased_names.join(", ")
    )]
}

fn column_name_safety_meta(columns: &[QueryColumn]) -> Value {
    let aliased_columns = duplicate_column_aliases(columns);
    json!({
        "object_row_safe": true,
        "duplicate_columns_aliased": !aliased_columns.is_empty(),
        "aliased_columns": aliased_columns,
        "strategy": "suffix_alias",
    })
}

fn response_data_by_mode(output: &QueryOutput, output_mode: ResponseOutputMode) -> Value {
    match output_mode {
        ResponseOutputMode::Auto => {
            unreachable!("output_mode auto must be resolved before response shaping")
        }
        ResponseOutputMode::Rows | ResponseOutputMode::RowsSafe => Value::Array(
            output
                .rows
                .iter()
                .map(|row| Value::Object(row.clone()))
                .collect(),
        ),
        ResponseOutputMode::Tuples => rows_to_tuple_rows(&output.rows, &output.columns),
        ResponseOutputMode::Scalar => {
            if output.columns.is_empty() || output.rows.is_empty() {
                Value::Null
            } else {
                output
                    .rows
                    .first()
                    .and_then(|row| row.get(&output.columns[0].name))
                    .cloned()
                    .unwrap_or(Value::Null)
            }
        }
        ResponseOutputMode::DataOnly => rows_to_tuple_rows(&output.rows, &output.columns),
    }
}

fn escape_markdown_table_cell(raw: &str) -> String {
    raw.replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('\r', "")
        .replace('\n', "<br>")
}

fn render_markdown_cell_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(raw) => raw.to_string(),
        Value::Number(raw) => raw.to_string(),
        Value::String(raw) => raw.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn render_query_output_markdown(output: &QueryOutput, output_mode: ResponseOutputMode) -> String {
    match output_mode {
        ResponseOutputMode::Auto => {
            unreachable!("output_mode auto must be resolved before markdown rendering")
        }
        ResponseOutputMode::Scalar => {
            if output.columns.is_empty() || output.rows.is_empty() {
                String::new()
            } else {
                output
                    .rows
                    .first()
                    .and_then(|row| row.get(&output.columns[0].name))
                    .map(render_markdown_cell_value)
                    .unwrap_or_default()
            }
        }
        ResponseOutputMode::Rows
        | ResponseOutputMode::RowsSafe
        | ResponseOutputMode::Tuples
        | ResponseOutputMode::DataOnly => {
            if output.columns.is_empty() {
                return format!("{} rows", output.rows.len());
            }

            let header = output
                .columns
                .iter()
                .map(|column| escape_markdown_table_cell(&column.name))
                .collect::<Vec<_>>()
                .join(" | ");
            let separator = std::iter::repeat_n("---", output.columns.len())
                .collect::<Vec<_>>()
                .join(" | ");

            let mut lines = vec![format!("| {header} |"), format!("| {separator} |")];
            for row in &output.rows {
                let rendered = output
                    .columns
                    .iter()
                    .map(|column| {
                        escape_markdown_table_cell(
                            &row.get(&column.name)
                                .map(render_markdown_cell_value)
                                .unwrap_or_default(),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                lines.push(format!("| {rendered} |"));
            }
            if output.rows.is_empty() {
                lines.push(String::new());
                lines.push("0 rows".to_string());
            }
            lines.join("\n")
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CellClipMeta {
    max_cell_chars: usize,
    clipped_cells: usize,
}

#[allow(clippy::too_many_arguments)]
fn response_meta_for_rows(
    output: &QueryOutput,
    output_mode: ResponseOutputMode,
    summary_only: bool,
    row_count_total: Option<usize>,
    query_hash: Option<&str>,
    next_cursor: Option<String>,
    cursor_offset: Option<usize>,
    next_offset: Option<usize>,
    cell_clip_meta: Option<CellClipMeta>,
    query_hints: &[String],
) -> Value {
    let row_count_returned = output.rows.len();
    let row_count_mode = if row_count_total.is_some() {
        "count_exact"
    } else {
        "page_window"
    };
    let row_count_total = row_count_total.unwrap_or(row_count_returned);
    let has_more = next_cursor.is_some();
    let truncated = has_more;
    let next_offset = if has_more { next_offset } else { None };
    let cell_clipping = if let Some(meta) = cell_clip_meta {
        json!({
            "enabled": true,
            "max_cell_chars": meta.max_cell_chars,
            "clipped_cells": meta.clipped_cells,
            "applied": meta.clipped_cells > 0,
        })
    } else {
        json!({
            "enabled": false,
            "max_cell_chars": null,
            "clipped_cells": 0,
            "applied": false,
        })
    };
    json!({
        "output_mode": output_mode.as_str(),
        "summary_only": summary_only,
        "row_count_mode": row_count_mode,
        "row_count_total": row_count_total,
        "row_count_returned": row_count_returned,
        "returned_rows": row_count_returned,
        "has_more": has_more,
        "truncated": truncated,
        "cursor_offset": cursor_offset,
        "next_cursor": next_cursor,
        "next_offset": next_offset,
        "query_hash": query_hash,
        "columns": output.columns,
        "cell_clipping": cell_clipping,
        "query_hints": query_hints,
        "column_name_safety": column_name_safety_meta(&output.columns),
    })
}

fn is_cents_like_identifier(name: &str) -> bool {
    let lowered = name.to_ascii_lowercase();
    lowered.ends_with("_cents")
}

fn parse_cents_value(raw: &Value) -> Option<i128> {
    match raw {
        Value::Number(number) => number
            .as_i64()
            .map(i128::from)
            .or_else(|| number.as_u64().map(|value| value as i128))
            .or_else(|| {
                number.as_f64().and_then(|value| {
                    if value.is_finite() && value.fract() == 0.0 {
                        Some(value as i128)
                    } else {
                        None
                    }
                })
            }),
        Value::String(raw) => {
            let normalized = raw.replace('_', "");
            normalized.trim().parse::<i128>().ok()
        }
        _ => None,
    }
}

fn format_cents_value_as_currency(value: &Value) -> Option<Value> {
    let cents = parse_cents_value(value)?;
    let is_negative = cents < 0;
    let absolute = cents.abs();
    let major = absolute / 100;
    let minor = absolute % 100;
    let formatted = format!(
        "{}{}.{:02}",
        if is_negative { "-" } else { "" },
        major,
        minor
    );
    Some(Value::String(formatted))
}

fn formatted_currency_column_name(base: &str, used: &mut BTreeSet<String>) -> String {
    let mut formatted = format!("{base}{CURRENCY_SUFFIX}");
    let mut suffix = 1usize;
    while used.contains(&formatted) {
        formatted = format!("{base}{CURRENCY_SUFFIX}_{suffix}");
        suffix = suffix.saturating_add(1);
    }
    used.insert(formatted.clone());
    formatted
}

fn apply_currency_display_mode(
    output: &QueryOutput,
    mode: Option<ResponseFormattingMode>,
    explicit_columns: &[String],
) -> (QueryOutput, Vec<String>) {
    if mode != Some(ResponseFormattingMode::Currency) {
        return (output.clone(), Vec::new());
    }

    let explicit_columns = explicit_columns
        .iter()
        .map(|column| column.trim().to_ascii_lowercase())
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>();
    let mut selected_columns = Vec::new();
    for col in &output.columns {
        let lowered = col.name.to_ascii_lowercase();
        let should_format =
            explicit_columns.contains(&lowered) || is_cents_like_identifier(&col.name);
        if should_format {
            selected_columns.push(col.name.clone());
        }
        if selected_columns.len() >= MAX_CURRENCY_FORMATTED_COLUMNS {
            break;
        }
    }

    if selected_columns.is_empty() {
        return (output.clone(), Vec::new());
    }

    let mut used_names = output
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<BTreeSet<_>>();
    let mut formatted_column_pairs = Vec::new();
    for column_name in &selected_columns {
        let formatted_name = formatted_currency_column_name(column_name, &mut used_names);
        formatted_column_pairs.push((column_name.clone(), formatted_name.clone()));
    }

    let mut formatted_columns = output.columns.clone();
    formatted_columns.extend(formatted_column_pairs.iter().map(|(_, formatted_name)| {
        QueryColumn {
            name: formatted_name.to_string(),
            pg_type: "text".to_string(),
            nullable: Some(true),
        }
    }));

    let mut formatted_rows = Vec::with_capacity(output.rows.len());
    for row in &output.rows {
        let mut formatted_row = row.clone();
        for (source_name, formatted_name) in &formatted_column_pairs {
            let formatted = row
                .get(source_name)
                .and_then(format_cents_value_as_currency)
                .unwrap_or(Value::Null);
            formatted_row.insert(formatted_name.clone(), formatted);
        }
        formatted_rows.push(formatted_row);
    }

    let formatted_output = QueryOutput {
        rows: formatted_rows,
        columns: formatted_columns,
        rows_affected: None,
    };

    let formatted_names = formatted_column_pairs
        .iter()
        .map(|(_, formatted_name)| formatted_name.clone())
        .collect();
    (formatted_output, formatted_names)
}

#[allow(clippy::too_many_arguments)]
fn response_contract_rows(
    server: &PostgresMcp,
    output: &QueryOutput,
    output_mode: ResponseOutputMode,
    summary_only: bool,
    elapsed_ms: u64,
    row_count_total: Option<usize>,
    query_hash: Option<&str>,
    next_cursor: Option<String>,
    cursor_offset: Option<usize>,
    next_offset: Option<usize>,
    cell_clip_meta: Option<CellClipMeta>,
    query_hints: &[String],
) -> CallToolResult {
    let payload = if summary_only {
        Value::Null
    } else {
        response_data_by_mode(output, output_mode)
    };
    contract_success(
        server,
        payload,
        elapsed_ms,
        response_meta_for_rows(
            output,
            output_mode,
            summary_only,
            row_count_total,
            query_hash,
            next_cursor,
            cursor_offset,
            next_offset,
            cell_clip_meta,
            query_hints,
        ),
    )
}

#[derive(Debug, PartialEq, Eq)]
struct PaginationCursor {
    query_hash: String,
    offset: usize,
    expires_at_epoch_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PaginationCursorScope {
    ExecuteSql,
    ListObjects,
}

impl PaginationCursorScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExecuteSql => "execute_sql",
            Self::ListObjects => "list_objects",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PaginationCursorDecodeError {
    Invalid,
    QueryMismatch,
    Expired,
}

fn pagination_cursor_now_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn pagination_cursor_signature(server: &PostgresMcp, payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(server.pagination_cursor_signing_key.as_ref());
    hasher.update(payload.as_bytes());
    hasher.update(server.pagination_cursor_signing_key.as_ref());
    let digest = hasher.finalize();
    format!("{digest:x}")
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (left_byte, right_byte) in left.iter().zip(right.iter()) {
        diff |= *left_byte ^ *right_byte;
    }
    diff == 0
}

fn encode_pagination_cursor(
    server: &PostgresMcp,
    scope: PaginationCursorScope,
    query_hash: &str,
    offset: usize,
) -> String {
    let expires_at_epoch_seconds = pagination_cursor_now_epoch_seconds()
        .saturating_add(server.pagination_cursor_ttl.as_secs().max(1));
    let signed_payload = format!(
        "{CURSOR_PREFIX}:{}:{query_hash}:{offset}:{expires_at_epoch_seconds}",
        scope.as_str()
    );
    let signature = pagination_cursor_signature(server, &signed_payload);
    format!("{signed_payload}:{signature}")
}

fn decode_pagination_cursor(
    server: &PostgresMcp,
    scope: PaginationCursorScope,
    expected_query_hash: &str,
    cursor: &str,
) -> Result<PaginationCursor, PaginationCursorDecodeError> {
    let parts = cursor.splitn(6, ':').collect::<Vec<_>>();
    let [
        prefix,
        raw_scope,
        query_hash,
        offset_str,
        expires_at_str,
        raw_signature,
    ] = parts.as_slice()
    else {
        return Err(PaginationCursorDecodeError::Invalid);
    };
    if *prefix != CURSOR_PREFIX {
        return Err(PaginationCursorDecodeError::Invalid);
    }
    if *raw_scope != scope.as_str() {
        return Err(PaginationCursorDecodeError::Invalid);
    }
    if query_hash.is_empty() || query_hash.len() != 16 {
        return Err(PaginationCursorDecodeError::Invalid);
    }
    let offset = offset_str
        .parse::<usize>()
        .map_err(|_| PaginationCursorDecodeError::Invalid)?;
    let expires_at_epoch_seconds = expires_at_str
        .parse::<u64>()
        .map_err(|_| PaginationCursorDecodeError::Invalid)?;
    let signature = raw_signature.to_ascii_lowercase();
    if signature.len() != CURSOR_SIGNATURE_HEX_LEN
        || !signature.as_bytes().iter().all(u8::is_ascii_hexdigit)
    {
        return Err(PaginationCursorDecodeError::Invalid);
    }
    let signed_payload = format!(
        "{CURSOR_PREFIX}:{}:{query_hash}:{offset}:{expires_at_epoch_seconds}",
        scope.as_str()
    );
    let expected_signature = pagination_cursor_signature(server, &signed_payload);
    if !constant_time_eq(signature.as_bytes(), expected_signature.as_bytes()) {
        return Err(PaginationCursorDecodeError::Invalid);
    }
    if *query_hash != expected_query_hash {
        return Err(PaginationCursorDecodeError::QueryMismatch);
    }
    if pagination_cursor_now_epoch_seconds() > expires_at_epoch_seconds {
        return Err(PaginationCursorDecodeError::Expired);
    }
    Ok(PaginationCursor {
        query_hash: query_hash.to_string(),
        offset,
        expires_at_epoch_seconds,
    })
}

fn canonicalize_sql(sql: &str) -> String {
    sql.trim().trim_end_matches(';').trim().to_string()
}

fn response_page_hash(sql: &str) -> String {
    let data = canonicalize_sql(sql).to_ascii_lowercase();
    let digest = Sha256::digest(data.as_bytes());
    let hex = format!("{digest:x}");
    hex[..16].to_string()
}

fn response_page_hash_for_params(sql: &str, params: &[Value]) -> String {
    if params.is_empty() {
        return response_page_hash(sql);
    }
    let canonical_sql = canonicalize_sql(sql).to_ascii_lowercase();
    let mut hasher = Sha256::new();
    hasher.update(canonical_sql.as_bytes());
    hasher.update(b"\0params\0");
    let params_bytes = serde_json::to_vec(params).unwrap_or_default();
    hasher.update(params_bytes);
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    hex[..16].to_string()
}

fn sql_fingerprint_string_literal_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"'(?:''|[^'])*'").expect("sql fingerprint string literal regex must compile")
    })
}

fn sql_fingerprint_numeric_literal_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\b\d+(?:\.\d+)?\b")
            .expect("sql fingerprint numeric literal regex must compile")
    })
}

fn sql_fingerprint_whitespace_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\s+").expect("sql fingerprint whitespace regex must compile"))
}

fn sql_fingerprint_dollar_delimiter_len(input: &[u8], start: usize) -> Option<usize> {
    if input.get(start).copied() != Some(b'$') {
        return None;
    }
    let mut i = start + 1;
    if input.get(i).copied() == Some(b'$') {
        return Some(2);
    }
    let first = input.get(i).copied()?;
    if !matches!(first, b'a'..=b'z' | b'_') {
        return None;
    }
    i += 1;
    while let Some(byte) = input.get(i).copied() {
        match byte {
            b'a'..=b'z' | b'0'..=b'9' | b'_' => i += 1,
            b'$' => return Some(i - start + 1),
            _ => return None,
        }
    }
    None
}

fn mask_dollar_quoted_literals(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len());
    let mut i = 0usize;
    while i < bytes.len() {
        let Some(delimiter_len) = sql_fingerprint_dollar_delimiter_len(bytes, i) else {
            output.push(bytes[i] as char);
            i += 1;
            continue;
        };
        let delimiter = &bytes[i..i + delimiter_len];
        let mut cursor = i + delimiter_len;
        let mut close_at = None;
        while cursor + delimiter_len <= bytes.len() {
            if &bytes[cursor..cursor + delimiter_len] == delimiter {
                close_at = Some(cursor);
                break;
            }
            cursor += 1;
        }
        if let Some(close_index) = close_at {
            output.push('?');
            i = close_index + delimiter_len;
        } else {
            output.push(bytes[i] as char);
            i += 1;
        }
    }
    output
}

fn normalize_sql_for_fingerprint(sql: &str) -> String {
    let canonical = canonicalize_sql(sql).to_ascii_lowercase();
    let masked_dollar_quoted = mask_dollar_quoted_literals(&canonical);
    let masked_strings =
        sql_fingerprint_string_literal_re().replace_all(&masked_dollar_quoted, "?");
    let masked_numbers = sql_fingerprint_numeric_literal_re().replace_all(&masked_strings, "?");
    let normalized = sql_fingerprint_whitespace_re().replace_all(&masked_numbers, " ");
    normalized.trim().to_string()
}

fn query_fingerprint(sql: &str) -> String {
    let normalized = normalize_sql_for_fingerprint(sql);
    let digest = Sha256::digest(normalized.as_bytes());
    let hex = format!("{digest:x}");
    format!("qf_{}", &hex[..16])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlLexState {
    Normal,
    SingleQuote,
    DoubleQuote,
    LineComment,
    BlockComment,
}

fn is_identifier_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn top_level_keyword_tokens(sql: &str) -> Vec<String> {
    let canonical = canonicalize_sql(sql);
    let bytes = canonical.as_bytes();
    let mut tokens = Vec::new();
    let mut state = SqlLexState::Normal;
    let mut paren_depth = 0usize;
    let mut i = 0usize;

    while i < bytes.len() {
        match state {
            SqlLexState::Normal => {
                if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    state = SqlLexState::LineComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    state = SqlLexState::BlockComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'\'' {
                    state = SqlLexState::SingleQuote;
                    i += 1;
                    continue;
                }
                if bytes[i] == b'"' {
                    state = SqlLexState::DoubleQuote;
                    i += 1;
                    continue;
                }
                if bytes[i] == b'(' {
                    paren_depth = paren_depth.saturating_add(1);
                    i += 1;
                    continue;
                }
                if bytes[i] == b')' {
                    paren_depth = paren_depth.saturating_sub(1);
                    i += 1;
                    continue;
                }
                if paren_depth == 0 && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_') {
                    let start = i;
                    i += 1;
                    while i < bytes.len() && is_identifier_token_byte(bytes[i]) {
                        i += 1;
                    }
                    tokens.push(canonical[start..i].to_ascii_lowercase());
                    continue;
                }
                i += 1;
            }
            SqlLexState::SingleQuote => {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    state = SqlLexState::Normal;
                }
                i += 1;
            }
            SqlLexState::DoubleQuote => {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    state = SqlLexState::Normal;
                }
                i += 1;
            }
            SqlLexState::LineComment => {
                if bytes[i] == b'\n' {
                    state = SqlLexState::Normal;
                }
                i += 1;
            }
            SqlLexState::BlockComment => {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    state = SqlLexState::Normal;
                    i += 2;
                    continue;
                }
                i += 1;
            }
        }
    }

    tokens
}

fn leading_statement_keyword(sql: &str) -> Option<String> {
    let tokens = top_level_keyword_tokens(sql);
    let first = tokens.first()?;
    if first == "with" {
        return tokens.into_iter().skip(1).find(|token| {
            matches!(
                token.as_str(),
                "select" | "values" | "table" | "insert" | "update" | "delete" | "merge"
            )
        });
    }
    Some(first.to_string())
}

fn is_select_like(sql: &str) -> bool {
    matches!(
        leading_statement_keyword(sql).as_deref(),
        Some("select" | "values" | "table")
    )
}

fn should_paginate_execute_sql(sql: &str) -> bool {
    is_select_like(sql)
}

fn resolve_execute_sql_page_size(
    default_page_size: usize,
    requested_max_rows: Option<usize>,
) -> usize {
    requested_max_rows.unwrap_or(default_page_size).max(1)
}

fn select_star_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)\bselect\s+\*").expect("select-star regex must compile"))
}

fn pre_execution_hints_for_sql(sql: &str) -> Vec<String> {
    let canonical = canonicalize_sql(sql);
    if canonical.is_empty() || !is_select_like(&canonical) {
        return Vec::new();
    }

    let lowered = canonical.to_ascii_lowercase();
    let has_bound = lowered.contains(" limit ")
        || lowered.ends_with(" limit")
        || lowered.contains(" fetch first ")
        || lowered.contains(" fetch next ");
    if has_bound {
        return Vec::new();
    }

    let is_scalar_aggregate = ["count(", "sum(", "avg(", "min(", "max("]
        .iter()
        .any(|marker| lowered.contains(marker));
    if is_scalar_aggregate {
        return Vec::new();
    }

    if select_star_re().is_match(&canonical) {
        return vec!["Query appears unbounded (SELECT * without LIMIT). Consider adding LIMIT, selecting specific columns, or using aggregate/scalar output.".to_string()];
    }

    Vec::new()
}

fn unbound_param_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\$[1-9][0-9]*").expect("unbound-parameter regex must compile"))
}

fn starts_with_any_keyword(sql: &str, keywords: &[&str]) -> bool {
    keywords.iter().any(|keyword| sql.starts_with(keyword))
}

fn normalize_optional_name_filter(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|v| !v.is_empty())
}

fn ensure_no_dangling_like_escape(value: &str, arg_name: &str) -> Result<(), String> {
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' && chars.next().is_none() {
            return Err(format!(
                "{arg_name} must not end with an unfinished escape ('\\\\')"
            ));
        }
    }
    Ok(())
}

fn contains_unescaped_wildcard(value: &str, arg_name: &str) -> Result<bool, String> {
    let mut has_wildcard = false;
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if chars.next().is_none() {
                return Err(format!(
                    "{arg_name} must not end with an unfinished escape ('\\\\')"
                ));
            }
            continue;
        }
        if matches!(ch, '%' | '_') {
            has_wildcard = true;
        }
    }
    Ok(has_wildcard)
}

pub(super) fn escape_like_pattern(term: &str) -> String {
    let mut escaped = String::with_capacity(term.len());
    for ch in term.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '%' => escaped.push_str("\\%"),
            '_' => escaped.push_str("\\_"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn sql_quote_e_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '\'' => escaped.push_str("\\'"),
            _ => escaped.push(ch),
        }
    }
    format!("E'{escaped}'")
}

pub(super) fn ilike_literal_predicate(column_expr: &str, pattern: &str) -> String {
    format!(
        "{column_expr} ILIKE {} ESCAPE E'\\\\'",
        sql_quote_e_literal(pattern)
    )
}

pub(super) fn compile_discovery_name_filter(
    name_like: Option<&str>,
    name_prefix: Option<&str>,
    name_contains: Option<&str>,
    name_exact: Option<&str>,
    name_pattern: Option<&str>,
) -> Result<Option<CompiledDiscoveryNameFilter>, String> {
    let name_like = normalize_optional_name_filter(name_like);
    let name_prefix = normalize_optional_name_filter(name_prefix);
    let name_contains = normalize_optional_name_filter(name_contains);
    let name_exact = normalize_optional_name_filter(name_exact);
    let name_pattern = normalize_optional_name_filter(name_pattern);

    let provided_count = [
        name_like.is_some(),
        name_prefix.is_some(),
        name_contains.is_some(),
        name_exact.is_some(),
        name_pattern.is_some(),
    ]
    .into_iter()
    .filter(|value| *value)
    .count();
    if provided_count > 1 {
        return Err(
            "only one name filter may be provided: name_like, name_prefix, name_contains, name_exact, name_pattern"
                .to_string(),
        );
    }

    if let Some(value) = name_exact {
        return Ok(Some(CompiledDiscoveryNameFilter {
            mode: DiscoveryNameFilterMode::Exact,
            source_arg: "name_exact",
            pattern: escape_like_pattern(value),
        }));
    }

    if let Some(value) = name_prefix {
        return Ok(Some(CompiledDiscoveryNameFilter {
            mode: DiscoveryNameFilterMode::Prefix,
            source_arg: "name_prefix",
            pattern: format!("{}%", escape_like_pattern(value)),
        }));
    }

    if let Some(value) = name_contains {
        return Ok(Some(CompiledDiscoveryNameFilter {
            mode: DiscoveryNameFilterMode::Contains,
            source_arg: "name_contains",
            pattern: format!("%{}%", escape_like_pattern(value)),
        }));
    }

    if let Some(value) = name_pattern {
        ensure_no_dangling_like_escape(value, "name_pattern")?;
        return Ok(Some(CompiledDiscoveryNameFilter {
            mode: DiscoveryNameFilterMode::Pattern,
            source_arg: "name_pattern",
            pattern: value.to_string(),
        }));
    }

    let Some(value) = name_like else {
        return Ok(None);
    };
    if contains_unescaped_wildcard(value, "name_like")? {
        Ok(Some(CompiledDiscoveryNameFilter {
            mode: DiscoveryNameFilterMode::Pattern,
            source_arg: "name_like",
            pattern: value.to_string(),
        }))
    } else {
        Ok(Some(CompiledDiscoveryNameFilter {
            mode: DiscoveryNameFilterMode::Contains,
            source_arg: "name_like",
            pattern: format!("%{}%", escape_like_pattern(value)),
        }))
    }
}

fn is_internal_introspection_query(sql: &str) -> bool {
    let sql = sql.to_ascii_lowercase();
    [
        "pg_catalog.",
        "information_schema.",
        "pg_toast.",
        "pg_stat_statements",
        "pg_stat_activity",
        "pg_stat_user_",
        "pg_available_extensions",
        "pg_extension",
    ]
    .iter()
    .any(|marker| sql.contains(marker))
}

fn classify_workload_query_for_index_advisor(
    query: &str,
) -> Result<String, WorkloadQuerySkipReason> {
    let canonical = canonicalize_sql(query);
    if canonical.is_empty() {
        return Err(WorkloadQuerySkipReason {
            reason: "empty_query",
            message: "query text is empty",
        });
    }
    if canonical.len() > MAX_SQL_INPUT_BYTES {
        return Err(WorkloadQuerySkipReason {
            reason: "oversized_query",
            message: "query exceeds configured maximum payload size",
        });
    }

    if unbound_param_re().is_match(&canonical) {
        return Err(WorkloadQuerySkipReason {
            reason: "unbound_params",
            message: "query contains bind placeholders that cannot be replayed safely",
        });
    }

    if is_internal_introspection_query(&canonical) {
        return Err(WorkloadQuerySkipReason {
            reason: "internal_query",
            message: "internal introspection query ignored for user-index recommendations",
        });
    }

    let lower = canonical.to_ascii_lowercase();
    if starts_with_any_keyword(
        &lower,
        &[
            "show ",
            "set ",
            "reset ",
            "discard ",
            "explain ",
            "prepare ",
            "deallocate ",
            "listen ",
            "unlisten ",
            "notify ",
            "create ",
            "alter ",
            "drop ",
            "truncate ",
            "vacuum ",
            "reindex ",
            "grant ",
            "revoke ",
            "comment ",
            "analyze ",
            "cluster ",
            "refresh ",
        ],
    ) {
        return Err(WorkloadQuerySkipReason {
            reason: "ddl_ignored",
            message: "non-actionable DDL/admin statement ignored",
        });
    }

    if !is_select_like(&canonical) {
        return Err(WorkloadQuerySkipReason {
            reason: "non_read_statement",
            message: "only read workload statements are eligible for index recommendations",
        });
    }

    if classify_restricted_sql(&canonical).is_err() {
        return Err(WorkloadQuerySkipReason {
            reason: "ddl_ignored",
            message: "query rejected by SQL safety policy for workload replay",
        });
    }

    Ok(canonical)
}

fn wrap_for_row_count(sql: &str) -> String {
    format!(
        "SELECT COUNT(*) AS row_count_total FROM ({}) AS _postgres_mcp_results",
        canonicalize_sql(sql)
    )
}

fn wrap_for_page(sql: &str, offset: usize, limit: usize) -> String {
    format!(
        "SELECT * FROM ({}) AS _postgres_mcp_results LIMIT {} OFFSET {}",
        canonicalize_sql(sql),
        limit,
        offset
    )
}

fn parse_usize_from_json(value: &Value) -> Option<usize> {
    match value {
        Value::Number(number) => {
            if let Some(u64_value) = number.as_u64() {
                return Some(u64_value as usize);
            }
            if let Some(i64_value) = number.as_i64() {
                return usize::try_from(i64_value).ok();
            }
            if let Some(f64_value) = number.as_f64()
                && f64_value >= 0.0
                && f64_value.is_finite()
                && f64_value.fract() == 0.0
            {
                return Some(f64_value as usize);
            }
            None
        }
        Value::String(raw) => {
            let normalized = raw.trim().replace('_', "");
            normalized.parse::<usize>().ok()
        }
        _ => None,
    }
}

fn extract_row_count(output: &QueryOutput) -> Option<usize> {
    let row = output.rows.first()?;
    row.values().find_map(parse_usize_from_json)
}

fn clip_string_cell(raw: &str, max_cell_chars: usize) -> Option<String> {
    let length = raw.chars().count();
    if length <= max_cell_chars {
        return None;
    }

    let clipped = if max_cell_chars <= 3 {
        raw.chars().take(max_cell_chars).collect::<String>()
    } else {
        let head = raw.chars().take(max_cell_chars - 3).collect::<String>();
        format!("{head}...")
    };
    Some(clipped)
}

fn clip_query_output_cells(output: &QueryOutput, max_cell_chars: usize) -> (QueryOutput, usize) {
    let mut clipped_cells = 0usize;
    let mut rows = Vec::with_capacity(output.rows.len());
    for row in &output.rows {
        let mut clipped_row = Map::with_capacity(row.len());
        for (column, value) in row {
            let clipped_value = match value {
                Value::String(raw) => {
                    if let Some(clipped) = clip_string_cell(raw, max_cell_chars) {
                        clipped_cells += 1;
                        Value::String(clipped)
                    } else {
                        value.clone()
                    }
                }
                _ => value.clone(),
            };
            clipped_row.insert(column.clone(), clipped_value);
        }
        rows.push(clipped_row);
    }
    (
        QueryOutput {
            rows,
            columns: output.columns.clone(),
            rows_affected: None,
        },
        clipped_cells,
    )
}

fn query_response_error(
    message: &str,
    code: &str,
    reason: &str,
    elapsed_ms: u64,
    server: &PostgresMcp,
) -> CallToolResult {
    contract_error(
        server,
        json!({
            "error": message,
            "code": code,
            "reason": reason,
        }),
        elapsed_ms,
        json!({}),
    )
}

#[allow(clippy::too_many_arguments)]
fn query_success(
    server: &PostgresMcp,
    output: &QueryOutput,
    output_mode: ResponseOutputMode,
    elapsed_ms: u64,
    row_count_total: Option<usize>,
    query_hash: Option<&str>,
    next_cursor: Option<String>,
    cursor_offset: Option<usize>,
    next_offset: Option<usize>,
    max_cell_chars: Option<usize>,
    mut query_hints: Vec<String>,
    summary_only: bool,
    emit_markdown_text: bool,
) -> CallToolResult {
    let (clipped_output, cell_clip_meta) =
        if let Some(max_cell_chars) = max_cell_chars.filter(|value| *value > 0) {
            let (clipped_output, clipped_cells) = clip_query_output_cells(output, max_cell_chars);
            (
                clipped_output,
                Some(CellClipMeta {
                    max_cell_chars,
                    clipped_cells,
                }),
            )
        } else {
            (output.clone(), None)
        };
    let resolved_output_mode = resolve_effective_output_mode(
        &clipped_output,
        output_mode,
        server.response_output_mode_auto_tabular.as_output_mode(),
    );
    query_hints.extend(duplicate_column_alias_hints(&clipped_output.columns));
    let summary_only = summary_only;

    let mut result = response_contract_rows(
        server,
        &clipped_output,
        resolved_output_mode,
        summary_only,
        elapsed_ms,
        row_count_total,
        query_hash,
        next_cursor,
        cursor_offset,
        next_offset,
        cell_clip_meta,
        &query_hints,
    );
    if emit_markdown_text && !summary_only {
        result.content = vec![Content::text(render_query_output_markdown(
            &clipped_output,
            resolved_output_mode,
        ))];
    }
    result
}

fn tool_success(server: &PostgresMcp, payload: Value, elapsed_ms: u64) -> CallToolResult {
    contract_success(server, payload, elapsed_ms, json!({}))
}

fn validate_sql_size(sql: &str, field_name: &str) -> Result<(), String> {
    let bytes = sql.len();
    if bytes > MAX_SQL_INPUT_BYTES {
        return Err(format!(
            "{field_name} exceeds maximum size of {MAX_SQL_INPUT_BYTES} bytes (received {bytes})"
        ));
    }
    Ok(())
}

fn extract_query_plan_value(rows: &[Map<String, Value>]) -> Option<Value> {
    let row = rows.first()?;
    let value = row.get("QUERY PLAN")?;
    if value.is_array() {
        return Some(value.clone());
    }
    if let Some(text) = value.as_str()
        && let Ok(parsed) = serde_json::from_str::<Value>(text)
    {
        return Some(parsed);
    }
    Some(value.clone())
}

#[derive(Debug, Clone)]
struct ExtensionCheckError {
    code: String,
    reason: String,
    message: String,
    details: Value,
}

fn guard_error_to_extension_check(
    err: mcp_toolkit_policy_runtime::CapabilityGuardError,
) -> ExtensionCheckError {
    ExtensionCheckError {
        code: err.code,
        reason: err.reason,
        message: err.message,
        details: json!({}),
    }
}

async fn extension_runtime_error_result(
    server: &PostgresMcp,
    extension: ExtensionCapability,
    unavailable_reason: &str,
    err: &DbError,
    unavailable_payload: Value,
    generic_context: &str,
    elapsed_ms: u64,
) -> CallToolResult {
    match recheck_extension_after_runtime_error(server, extension, unavailable_reason, err).await {
        Ok(Some(ext_err)) => extension_unavailable_result(
            server,
            extension.extension_name(),
            &ext_err.reason,
            &ext_err.message,
            merge_payload(unavailable_payload, &ext_err.details),
            elapsed_ms,
        ),
        Ok(None) => db_error_result(server, generic_context, err, elapsed_ms),
        Err(check_err) => extension_check_error_result(server, &check_err, elapsed_ms),
    }
}

async fn ensure_extension_ready(
    server: &PostgresMcp,
    extension: ExtensionCapability,
    unavailable_reason: &str,
) -> Result<(), ExtensionCheckError> {
    use mcp_toolkit_policy_runtime::CapabilityRefreshState;

    if let Some(cached) = server.extension_unavailable_cache.get_fresh(&extension) {
        return Err(extension_unavailable_error_from_cached(
            extension.extension_name(),
            unavailable_reason,
            cached,
        ));
    }

    for _ in 0..2 {
        match server
            .extension_guard
            .begin_refresh(extension)
            .map_err(guard_error_to_extension_check)?
        {
            CapabilityRefreshState::FreshSuccess => {
                server.extension_unavailable_cache.clear(&extension);
                return Ok(());
            }
            CapabilityRefreshState::StartRefresh => {
                return refresh_extension_status(server, extension, unavailable_reason).await;
            }
            CapabilityRefreshState::RefreshInFlight => {
                if let Some(err) =
                    wait_for_refresh_outcome(server, extension, unavailable_reason).await?
                {
                    return Err(err);
                }
            }
        }
    }

    if server
        .extension_guard
        .has_fresh_success(&extension)
        .map_err(guard_error_to_extension_check)?
    {
        server.extension_unavailable_cache.clear(&extension);
        return Ok(());
    }

    if let Some(cached) = server.extension_unavailable_cache.get_fresh(&extension) {
        return Err(extension_unavailable_error_from_cached(
            extension.extension_name(),
            unavailable_reason,
            cached,
        ));
    }

    Err(extension_check_in_progress_error(
        extension.extension_name(),
    ))
}

struct ExtensionProbeStatus {
    installed: bool,
    reason: &'static str,
    message: String,
}

fn extension_unavailable_error(
    extension_name: &str,
    unavailable_reason: &str,
    guard_reason: &str,
    message: String,
) -> ExtensionCheckError {
    ExtensionCheckError {
        code: "EXTENSION_UNAVAILABLE".to_string(),
        reason: unavailable_reason.to_string(),
        message,
        details: json!({
            "guard_reason": guard_reason,
            "extension": extension_name,
        }),
    }
}

fn extension_unavailable_error_from_cached(
    extension_name: &str,
    unavailable_reason: &str,
    cached: ExtensionUnavailableStatus,
) -> ExtensionCheckError {
    extension_unavailable_error(
        extension_name,
        unavailable_reason,
        cached.guard_reason,
        cached.message,
    )
}

fn extension_check_in_progress_error(extension_name: &str) -> ExtensionCheckError {
    ExtensionCheckError {
        code: "EXTENSION_CHECK_IN_PROGRESS".to_string(),
        reason: "extension_check_in_progress".to_string(),
        message: format!(
            "Extension readiness check for {extension_name} is in progress; retry shortly."
        ),
        details: json!({
            "extension": extension_name,
            "retry_after_ms": (EXTENSION_REFRESH_WAIT_STEP.as_millis() as usize) * EXTENSION_REFRESH_WAIT_ATTEMPTS,
        }),
    }
}

async fn refresh_extension_status(
    server: &PostgresMcp,
    extension: ExtensionCapability,
    unavailable_reason: &str,
) -> Result<(), ExtensionCheckError> {
    let status_result = probe_extension_status(server, extension.extension_name()).await;
    let is_success = matches!(&status_result, Ok(status) if status.installed);
    server
        .extension_guard
        .complete_refresh(extension, is_success)
        .map_err(guard_error_to_extension_check)?;

    let status = status_result?;
    if status.installed {
        server.extension_unavailable_cache.clear(&extension);
        return Ok(());
    }

    server
        .extension_unavailable_cache
        .record(extension, status.reason, status.message.clone());
    Err(extension_unavailable_error(
        extension.extension_name(),
        unavailable_reason,
        status.reason,
        status.message,
    ))
}

async fn wait_for_refresh_outcome(
    server: &PostgresMcp,
    extension: ExtensionCapability,
    unavailable_reason: &str,
) -> Result<Option<ExtensionCheckError>, ExtensionCheckError> {
    for _ in 0..EXTENSION_REFRESH_WAIT_ATTEMPTS {
        if server
            .extension_guard
            .has_fresh_success(&extension)
            .map_err(guard_error_to_extension_check)?
        {
            server.extension_unavailable_cache.clear(&extension);
            return Ok(None);
        }
        if let Some(cached) = server.extension_unavailable_cache.get_fresh(&extension) {
            return Ok(Some(extension_unavailable_error_from_cached(
                extension.extension_name(),
                unavailable_reason,
                cached,
            )));
        }
        sleep(EXTENSION_REFRESH_WAIT_STEP).await;
    }
    Ok(None)
}

fn is_extension_missing_runtime_error(extension: ExtensionCapability, err: &DbError) -> bool {
    is_extension_missing_signature(extension, err.sqlstate(), err.message())
}

fn is_extension_missing_signature(
    extension: ExtensionCapability,
    sqlstate: Option<&str>,
    message: &str,
) -> bool {
    let message = message.to_ascii_lowercase();
    match extension {
        ExtensionCapability::PgStatStatements => {
            matches!(sqlstate, Some("42P01")) && message.contains("pg_stat_statements")
        }
        ExtensionCapability::Hypopg => {
            matches!(sqlstate, Some("42883"))
                && (message.contains("hypopg_reset") || message.contains("hypopg_create_index"))
        }
    }
}

async fn recheck_extension_after_runtime_error(
    server: &PostgresMcp,
    extension: ExtensionCapability,
    unavailable_reason: &str,
    err: &DbError,
) -> Result<Option<ExtensionCheckError>, ExtensionCheckError> {
    if !is_extension_missing_runtime_error(extension, err) {
        return Ok(None);
    }

    server
        .extension_guard
        .invalidate(&extension)
        .map_err(guard_error_to_extension_check)?;

    let status = probe_extension_status(server, extension.extension_name()).await?;
    if status.installed {
        server
            .extension_guard
            .record_success(extension)
            .map_err(guard_error_to_extension_check)?;
        server.extension_unavailable_cache.clear(&extension);
        return Ok(None);
    }

    server
        .extension_unavailable_cache
        .record(extension, status.reason, status.message.clone());
    Ok(Some(extension_unavailable_error(
        extension.extension_name(),
        unavailable_reason,
        status.reason,
        status.message,
    )))
}

async fn probe_extension_status(
    server: &PostgresMcp,
    extension_name: &str,
) -> Result<ExtensionProbeStatus, ExtensionCheckError> {
    let installed_sql = format!(
        "SELECT extversion FROM pg_extension WHERE extname = {}",
        sql_quote_literal(extension_name)
    );
    let installed_output = server
        .db
        .execute_query_readonly(&installed_sql)
        .await
        .map_err(|err| ExtensionCheckError {
            code: "EXTENSION_CHECK_FAILED".to_string(),
            reason: "extension_probe_failed".to_string(),
            message: format!("Failed checking extension {extension_name}: {err}"),
            details: json!({ "extension": extension_name, "phase": "installed_probe" }),
        })?;
    if !installed_output.rows.is_empty() {
        return Ok(ExtensionProbeStatus {
            installed: true,
            reason: "extension_installed",
            message: format!("The {extension_name} extension is already installed."),
        });
    }

    let available_sql = format!(
        "SELECT default_version FROM pg_available_extensions WHERE name = {}",
        sql_quote_literal(extension_name)
    );
    let available_output = server
        .db
        .execute_query_readonly(&available_sql)
        .await
        .map_err(|err| ExtensionCheckError {
            code: "EXTENSION_CHECK_FAILED".to_string(),
            reason: "extension_probe_failed".to_string(),
            message: format!("Failed checking extension {extension_name}: {err}"),
            details: json!({ "extension": extension_name, "phase": "available_probe" }),
        })?;
    if !available_output.rows.is_empty() {
        let message = if extension_name == ExtensionCapability::PgStatStatements.extension_name() {
            INSTALL_PG_STAT_STATEMENTS_MESSAGE.to_string()
        } else {
            format!(
                "The {extension_name} extension is available but not installed. Install with: CREATE EXTENSION {extension_name};"
            )
        };
        return Ok(ExtensionProbeStatus {
            installed: false,
            reason: "extension_not_installed",
            message,
        });
    }

    Ok(ExtensionProbeStatus {
        installed: false,
        reason: "extension_not_available",
        message: format!(
            "The {extension_name} extension is not available on this PostgreSQL server."
        ),
    })
}

async fn server_version_num(server: &PostgresMcp) -> Option<i64> {
    let output = server
        .db
        .execute_query_readonly("SHOW server_version_num")
        .await
        .ok()?;
    let row = output.rows.first()?;
    row.get("server_version_num")?.as_i64()
}

fn parse_health_types(input: &str) -> Result<HashSet<HealthType>, crate::McpError> {
    let mut types = HashSet::new();
    for token in input.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        match token.to_ascii_lowercase().as_str() {
            "index" => {
                types.insert(HealthType::Index);
            }
            "connection" => {
                types.insert(HealthType::Connection);
            }
            "vacuum" => {
                types.insert(HealthType::Vacuum);
            }
            "sequence" => {
                types.insert(HealthType::Sequence);
            }
            "replication" => {
                types.insert(HealthType::Replication);
            }
            "buffer" => {
                types.insert(HealthType::Buffer);
            }
            "constraint" => {
                types.insert(HealthType::Constraint);
            }
            "all" => {
                types.extend([
                    HealthType::Index,
                    HealthType::Connection,
                    HealthType::Vacuum,
                    HealthType::Sequence,
                    HealthType::Replication,
                    HealthType::Buffer,
                    HealthType::Constraint,
                ]);
            }
            other => {
                return Err(crate::McpError::invalid_params(
                    format!("Invalid health type: {other}"),
                    None,
                ));
            }
        }
    }

    if types.is_empty() {
        types.extend([
            HealthType::Index,
            HealthType::Connection,
            HealthType::Vacuum,
            HealthType::Sequence,
            HealthType::Replication,
            HealthType::Buffer,
            HealthType::Constraint,
        ]);
    }

    Ok(types)
}

async fn run_index_health(server: &PostgresMcp) -> Value {
    let invalid_sql = "SELECT schemaname AS schema, relname AS table, indexrelname AS index_name FROM pg_stat_user_indexes ui JOIN pg_index i ON ui.indexrelid = i.indexrelid WHERE NOT i.indisvalid ORDER BY schemaname, relname, indexrelname";
    let duplicate_sql = "SELECT schemaname AS schema, tablename AS table, indexdef, COUNT(*) AS duplicates FROM pg_indexes GROUP BY schemaname, tablename, indexdef HAVING COUNT(*) > 1 ORDER BY duplicates DESC, tablename";
    let unused_sql = "SELECT schemaname AS schema, relname AS table, indexrelname AS index_name, idx_scan AS scans, pg_relation_size(i.indexrelid) AS size_bytes FROM pg_stat_user_indexes ui JOIN pg_index i ON ui.indexrelid = i.indexrelid WHERE NOT i.indisunique AND idx_scan <= 50 ORDER BY size_bytes DESC, relname";
    let bloat_sql = "WITH stats AS (SELECT schemaname AS schema, relname AS table, indexrelname AS index_name, pg_relation_size(indexrelid) AS index_bytes, idx_scan AS scans FROM pg_stat_user_indexes) SELECT * FROM stats WHERE index_bytes >= 104857600 ORDER BY index_bytes DESC";

    json!({
        "invalid": query_rows_or_error(server, invalid_sql).await,
        "duplicate": query_rows_or_error(server, duplicate_sql).await,
        "unused": query_rows_or_error(server, unused_sql).await,
        "bloat_candidates": query_rows_or_error(server, bloat_sql).await,
    })
}

async fn run_connection_health(server: &PostgresMcp) -> Value {
    let sql = "SELECT (SELECT COUNT(*) FROM pg_stat_activity) AS total_connections, (SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'idle in transaction') AS idle_connections";
    query_rows_or_error(server, sql).await
}

async fn run_vacuum_health(server: &PostgresMcp) -> Value {
    query_rows_or_error(server, VACUUM_HEALTH_SQL).await
}

async fn run_sequence_health(server: &PostgresMcp) -> Value {
    let sql = "SELECT schemaname AS schema, sequencename AS sequence, data_type, last_value, max_value, ROUND((last_value::numeric / NULLIF(max_value::numeric, 0)) * 100, 2) AS percent_used FROM pg_sequences WHERE last_value IS NOT NULL AND max_value IS NOT NULL AND (last_value::numeric / NULLIF(max_value::numeric, 0)) > 0.9 ORDER BY percent_used DESC";
    query_rows_or_error(server, sql).await
}

async fn run_replication_health(server: &PostgresMcp) -> Value {
    let is_replica = query_rows_or_error(server, "SELECT pg_is_in_recovery() AS is_replica").await;
    let lag = query_rows_or_error(server, "SELECT CASE WHEN NOT pg_is_in_recovery() THEN 0 ELSE EXTRACT(EPOCH FROM NOW() - pg_last_xact_replay_timestamp()) END AS replication_lag_seconds").await;
    let slots = query_rows_or_error(
        server,
        "SELECT slot_name, database, active FROM pg_replication_slots ORDER BY slot_name",
    )
    .await;
    let replication_clients = query_rows_or_error(
        server,
        "SELECT pid, application_name, state, sync_state FROM pg_stat_replication ORDER BY pid",
    )
    .await;

    json!({
        "is_replica": is_replica,
        "replication_lag": lag,
        "replication_slots": slots,
        "replication_clients": replication_clients,
    })
}

async fn run_buffer_health(server: &PostgresMcp) -> Value {
    let index_sql = "SELECT (SUM(idx_blks_hit)::numeric / NULLIF(SUM(idx_blks_hit + idx_blks_read), 0)) AS rate FROM pg_statio_user_indexes";
    let table_sql = "SELECT (SUM(heap_blks_hit)::numeric / NULLIF(SUM(heap_blks_hit + heap_blks_read), 0)) AS rate FROM pg_statio_user_tables";

    json!({
        "index_hit_rate": query_rows_or_error(server, index_sql).await,
        "table_hit_rate": query_rows_or_error(server, table_sql).await,
    })
}

async fn run_constraint_health(server: &PostgresMcp) -> Value {
    query_rows_or_error(server, CONSTRAINT_HEALTH_SQL).await
}

async fn query_rows_or_error(server: &PostgresMcp, sql: &str) -> Value {
    match server.db.execute_query_readonly(sql).await {
        Ok(rows) => json!(rows.rows),
        Err(err) => json!({
            "error": err.to_string(),
            "code": err.code(),
            "reason": err.reason(),
            "sqlstate": err.sqlstate(),
        }),
    }
}

async fn analyze_queries_for_indexes(server: &PostgresMcp, queries: &[String]) -> Value {
    let mut recommendations = BTreeSet::new();
    let mut errors = Vec::new();

    for query in queries {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Err(err) = classify_restricted_sql(trimmed) {
            errors.push(json!({
                "query": trimmed,
                "error": err.message,
                "code": err.code.as_str(),
            }));
            continue;
        }

        let explain_sql = format!("EXPLAIN (FORMAT JSON) {trimmed}");
        let output = match server.db.execute_query_readonly(&explain_sql).await {
            Ok(output) => output,
            Err(err) => {
                errors.push(json!({
                    "query": trimmed,
                    "error": err.to_string(),
                    "code": err.code(),
                    "reason": err.reason(),
                    "sqlstate": err.sqlstate(),
                }));
                continue;
            }
        };

        let Some(plan) = extract_query_plan_value(&output.rows) else {
            errors.push(json!({ "query": trimmed, "error": "missing QUERY PLAN" }));
            continue;
        };

        collect_plan_recommendations(&plan, &mut recommendations);
    }

    let recs = recommendations
        .into_iter()
        .map(|rec| {
            let table_ident = sql_quote_qualified_ident(&rec.table);
            let cols = rec
                .columns
                .iter()
                .map(|c| sql_quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            let ddl = format!("CREATE INDEX ON {table_ident} USING {} ({cols})", rec.using);
            json!({
                "table": rec.table,
                "columns": rec.columns,
                "using": rec.using,
                "index_definition": ddl,
                "reason": rec.reason,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "recommendations": recs,
        "errors": errors,
    })
}

fn collect_plan_recommendations(plan_value: &Value, out: &mut BTreeSet<IndexRecommendation>) {
    let plans: Vec<&Value> = match plan_value {
        Value::Array(items) => items.iter().collect(),
        _ => vec![plan_value],
    };

    for item in plans {
        if let Some(plan) = item.get("Plan") {
            walk_plan_node(plan, out);
        }
    }
}

fn walk_plan_node(node: &Value, out: &mut BTreeSet<IndexRecommendation>) {
    let node_type = node
        .get("Node Type")
        .and_then(Value::as_str)
        .unwrap_or_default();

    if node_type == "Seq Scan" || node_type == "Bitmap Heap Scan" {
        let relation = node
            .get("Relation Name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        if !relation.is_empty() {
            let mut columns = BTreeSet::new();
            for key in ["Filter", "Recheck Cond", "Index Cond", "Join Filter"] {
                if let Some(expr) = node.get(key).and_then(Value::as_str) {
                    for col in extract_candidate_columns(expr) {
                        columns.insert(col);
                    }
                }
            }

            if !columns.is_empty() {
                let cols = columns.into_iter().collect::<Vec<_>>();
                out.insert(IndexRecommendation {
                    table: relation,
                    columns: cols,
                    using: "btree".to_string(),
                    reason: format!("{node_type} with filter predicates"),
                });
            }
        }
    }

    if let Some(children) = node.get("Plans").and_then(Value::as_array) {
        for child in children {
            walk_plan_node(child, out);
        }
    }
}

fn extract_candidate_columns(expr: &str) -> Vec<String> {
    fn candidate_column_re() -> &'static Regex {
        static RE: OnceLock<Regex> = OnceLock::new();
        RE.get_or_init(|| {
            Regex::new(
                r"(?i)\b((?:[a-z_][a-z0-9_]*\.)?[a-z_][a-z0-9_]*)\b(?:\s*\)\s*)*\s*(?:->>|->|#>>|#>|@>|<@|\?\||\?&|\?|=|<>|!=|>=|<=|>|<|~~\*?|!~~\*?|IN\b|LIKE\b|ILIKE\b|BETWEEN\b|IS\b)",
            )
            .expect("candidate-column regex must compile")
        })
    }

    fn cast_suffix_re() -> &'static Regex {
        static RE: OnceLock<Regex> = OnceLock::new();
        RE.get_or_init(|| {
            Regex::new(r"(?i)::\s*(?:[a-z_][a-z0-9_]*\.)*[a-z_][a-z0-9_]*(?:\[\])*")
                .expect("cast-suffix regex must compile")
        })
    }

    fn is_stopword(value: &str) -> bool {
        matches!(
            value,
            "and"
                | "or"
                | "not"
                | "null"
                | "is"
                | "true"
                | "false"
                | "case"
                | "when"
                | "then"
                | "else"
                | "in"
                | "like"
                | "ilike"
                | "between"
                | "as"
        )
    }

    let scrubbed = cast_suffix_re().replace_all(expr, "");

    let mut cols = BTreeSet::new();
    for cap in candidate_column_re().captures_iter(&scrubbed) {
        let col = cap
            .get(1)
            .map(|m| {
                m.as_str()
                    .rsplit('.')
                    .next()
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_ascii_lowercase()
            })
            .unwrap_or_default();
        if col.is_empty() || is_stopword(col.as_str()) {
            continue;
        }
        cols.insert(col);
    }

    cols.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::query_surface::response_page_hash_for_read_context;
    use super::{
        CONSTRAINT_HEALTH_SQL, CURSOR_SIGNATURE_HEX_LEN, CellClipMeta, ExtensionCapability,
        HealthType, MAX_SQL_INPUT_BYTES, PaginationCursorDecodeError, PaginationCursorScope,
        ReadQueryProfile, ResponseFormattingMode, VACUUM_HEALTH_SQL, apply_currency_display_mode,
        classify_workload_query_for_index_advisor, clip_query_output_cells,
        decode_pagination_cursor, encode_pagination_cursor, ensure_extension_ready,
        extension_check_in_progress_error, extract_candidate_columns,
        is_extension_missing_signature, normalize_error_payload_for_role,
        pagination_cursor_now_epoch_seconds, pagination_cursor_signature, parse_health_types,
        pre_execution_hints_for_sql, query_fingerprint, query_success,
        resolve_execute_sql_page_size, response_meta_for_rows, response_page_hash,
        response_page_hash_for_params, run_constraint_health, run_vacuum_health,
        should_paginate_execute_sql, validate_sql_size,
    };
    use crate::config::{
        AccessMode, ResponseAutoTabularMode, ResponseMode, ResponseOutputMode, StartupRole,
    };
    use crate::db::{DbEngine, QueryColumn, QueryOutput};
    use crate::server::{PostgresMcp, sanitize_tool_schemas_for_mcp};
    use mcp_toolkit_policy_runtime::CapabilityRefreshState;
    use mcp_toolkit_testing::assert_tool_schema_snapshot;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::env;
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;
    use tokio::sync::Mutex;

    fn extension_transition_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn cursor_test_server() -> PostgresMcp {
        PostgresMcp::with_response_contract(
            Arc::new(DbEngine::new(
                None,
                AccessMode::Unrestricted,
                false,
                Some(Duration::from_secs(15)),
                Some(Duration::from_secs(10)),
                Some(Duration::from_secs(2)),
            )),
            ResponseMode::V2,
            ResponseOutputMode::Auto,
            ResponseAutoTabularMode::Rows,
            200,
        )
    }

    #[test]
    fn extract_columns_simple() {
        let cols = extract_candidate_columns("(user_id = 1) AND status = 'active'");
        assert!(cols.contains(&"user_id".to_string()));
        assert!(cols.contains(&"status".to_string()));
    }

    #[test]
    fn extract_columns_handles_qualified_and_casted_identifiers() {
        let cols = extract_candidate_columns("u.created_at::date >= NOW() AND u.user_id = $1");
        assert!(cols.contains(&"created_at".to_string()));
        assert!(cols.contains(&"user_id".to_string()));
    }

    #[test]
    fn extract_columns_handles_json_and_array_operators() {
        let cols = extract_candidate_columns(
            "metadata->>'status' = 'active' AND tags @> ARRAY['pro'] AND payload ? 'tier'",
        );
        assert!(cols.contains(&"metadata".to_string()));
        assert!(cols.contains(&"tags".to_string()));
        assert!(cols.contains(&"payload".to_string()));
    }

    #[test]
    fn extract_columns_handles_wrapped_expressions() {
        let cols = extract_candidate_columns(
            "LOWER(email) LIKE 'a%' AND COALESCE(deleted_at, created_at) IS NULL",
        );
        assert!(cols.contains(&"email".to_string()));
        assert!(cols.contains(&"created_at".to_string()));
    }

    #[test]
    fn workload_filter_accepts_actionable_read_query() {
        let accepted = classify_workload_query_for_index_advisor(
            " SELECT * FROM public.users WHERE status = 'ready'; ",
        )
        .expect("read workload query should be accepted");
        assert_eq!(
            accepted,
            "SELECT * FROM public.users WHERE status = 'ready'"
        );
    }

    #[test]
    fn workload_filter_skips_ddl_with_taxonomy_reason() {
        let skipped = classify_workload_query_for_index_advisor("CREATE TABLE demo(id int)")
            .expect_err("DDL should be skipped");
        assert_eq!(skipped.reason, "ddl_ignored");
    }

    #[test]
    fn workload_filter_skips_unbound_parameters_with_taxonomy_reason() {
        let skipped =
            classify_workload_query_for_index_advisor("SELECT * FROM public.users WHERE id = $1")
                .expect_err("queries with unresolved bind placeholders should be skipped");
        assert_eq!(skipped.reason, "unbound_params");
    }

    #[test]
    fn workload_filter_skips_internal_queries_with_taxonomy_reason() {
        let skipped = classify_workload_query_for_index_advisor(
            "SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r'",
        )
        .expect_err("internal catalog queries should be skipped");
        assert_eq!(skipped.reason, "internal_query");
    }

    #[test]
    fn workload_filter_skips_oversized_queries() {
        let oversized = format!("SELECT '{}'", "x".repeat(MAX_SQL_INPUT_BYTES + 1));
        let skipped = classify_workload_query_for_index_advisor(&oversized)
            .expect_err("oversized workload queries should be skipped");
        assert_eq!(skipped.reason, "oversized_query");
    }

    #[test]
    fn execute_sql_page_size_defaults_to_server_cap() {
        assert_eq!(resolve_execute_sql_page_size(200, None), 200);
    }

    #[test]
    fn execute_sql_page_size_uses_explicit_override() {
        assert_eq!(resolve_execute_sql_page_size(200, Some(25)), 25);
        assert_eq!(resolve_execute_sql_page_size(200, Some(0)), 1);
    }

    #[test]
    fn execute_sql_pagination_applies_to_select_like_queries_for_all_response_modes() {
        assert!(should_paginate_execute_sql(
            "select * from public.operator_review_queue"
        ));
        assert!(should_paginate_execute_sql(
            "with x as (select 1) select * from x"
        ));
        assert!(should_paginate_execute_sql(
            "with recursive x(n) as (values (1)) select * from x"
        ));
        assert!(should_paginate_execute_sql("values (1), (2), (3)"));
        assert!(should_paginate_execute_sql(
            "table public.operator_review_queue"
        ));
        assert!(should_paginate_execute_sql(
            "with x as (select 1 as id) table x"
        ));
    }

    #[test]
    fn execute_sql_pagination_skips_non_select_statements() {
        assert!(!should_paginate_execute_sql(
            "update public.operator_review_queue set reason = 'x'"
        ));
        assert!(!should_paginate_execute_sql(
            "delete from public.operator_review_queue"
        ));
        assert!(!should_paginate_execute_sql(
            "with picked as (select id from public.operator_review_queue limit 1) update public.operator_review_queue q set reason = 'x' from picked where q.id = picked.id"
        ));
    }

    #[test]
    fn pagination_cursor_roundtrip_preserves_query_hash_and_offset() {
        let server = cursor_test_server();
        let query_hash =
            response_page_hash("SELECT id FROM public.operator_review_queue ORDER BY id");
        let cursor =
            encode_pagination_cursor(&server, PaginationCursorScope::ExecuteSql, &query_hash, 10);
        let decoded = decode_pagination_cursor(
            &server,
            PaginationCursorScope::ExecuteSql,
            &query_hash,
            &cursor,
        )
        .expect("cursor should decode");

        assert_eq!(decoded.query_hash, query_hash);
        assert_eq!(decoded.offset, 10);
        assert!(
            decoded.expires_at_epoch_seconds >= pagination_cursor_now_epoch_seconds(),
            "decoded cursor expiry should be in the future"
        );
    }

    #[test]
    fn pagination_cursor_rejects_malformed_tokens() {
        let server = cursor_test_server();
        let query_hash =
            response_page_hash("SELECT id FROM public.operator_review_queue ORDER BY id");
        assert_eq!(
            decode_pagination_cursor(
                &server,
                PaginationCursorScope::ExecuteSql,
                &query_hash,
                "not-a-cursor"
            ),
            Err(PaginationCursorDecodeError::Invalid)
        );
        assert_eq!(
            decode_pagination_cursor(
                &server,
                PaginationCursorScope::ExecuteSql,
                &query_hash,
                "v3:execute_sql:short:10:1735689600:abcd"
            ),
            Err(PaginationCursorDecodeError::Invalid)
        );
        assert_eq!(
            decode_pagination_cursor(
                &server,
                PaginationCursorScope::ExecuteSql,
                &query_hash,
                "v3:execute_sql:84c44961e673fe5d:not-a-number:1735689600:abcd"
            ),
            Err(PaginationCursorDecodeError::Invalid)
        );
    }

    #[test]
    fn pagination_cursor_advances_deterministically_by_page_length() {
        let server = cursor_test_server();
        let query_hash =
            response_page_hash("SELECT id FROM public.operator_review_queue ORDER BY id");
        let first_cursor =
            encode_pagination_cursor(&server, PaginationCursorScope::ExecuteSql, &query_hash, 10);
        let first_page = decode_pagination_cursor(
            &server,
            PaginationCursorScope::ExecuteSql,
            &query_hash,
            &first_cursor,
        )
        .expect("first cursor should decode");
        let next_cursor = encode_pagination_cursor(
            &server,
            PaginationCursorScope::ExecuteSql,
            &query_hash,
            first_page.offset + 10,
        );
        let second_page = decode_pagination_cursor(
            &server,
            PaginationCursorScope::ExecuteSql,
            &query_hash,
            &next_cursor,
        )
        .expect("next cursor should decode");

        assert_eq!(first_page.offset, 10);
        assert_eq!(second_page.offset, 20);
        assert_eq!(first_page.query_hash, second_page.query_hash);
    }

    #[test]
    fn pagination_cursor_rejects_query_mismatch_deterministically() {
        let server = cursor_test_server();
        let original_hash =
            response_page_hash("SELECT id FROM public.operator_review_queue ORDER BY id");
        let mismatched_hash =
            response_page_hash("SELECT id FROM public.operator_review_queue ORDER BY id DESC");
        let cursor = encode_pagination_cursor(
            &server,
            PaginationCursorScope::ExecuteSql,
            &original_hash,
            25,
        );
        assert_eq!(
            decode_pagination_cursor(
                &server,
                PaginationCursorScope::ExecuteSql,
                &mismatched_hash,
                &cursor,
            ),
            Err(PaginationCursorDecodeError::QueryMismatch)
        );
    }

    #[test]
    fn response_page_hash_for_params_matches_sql_only_when_params_empty() {
        let sql = "SELECT id FROM public.operator_review_queue ORDER BY id";
        assert_eq!(
            response_page_hash(sql),
            response_page_hash_for_params(sql, &[])
        );
    }

    #[test]
    fn pagination_cursor_rejects_param_mismatch_deterministically() {
        let server = cursor_test_server();
        let sql = "SELECT id FROM public.operator_review_queue WHERE id = $1 ORDER BY id";
        let original_hash = response_page_hash_for_params(sql, &[json!(1)]);
        let mismatched_hash = response_page_hash_for_params(sql, &[json!(2)]);
        let cursor = encode_pagination_cursor(
            &server,
            PaginationCursorScope::ExecuteSql,
            &original_hash,
            25,
        );
        assert_eq!(
            decode_pagination_cursor(
                &server,
                PaginationCursorScope::ExecuteSql,
                &mismatched_hash,
                &cursor,
            ),
            Err(PaginationCursorDecodeError::QueryMismatch)
        );
    }

    #[test]
    fn pagination_cursor_rejects_profile_mismatch_deterministically() {
        let server = cursor_test_server();
        let sql = "SELECT id FROM public.operator_review_queue ORDER BY id";
        let compact_hash = response_page_hash_for_read_context(
            sql,
            &[],
            Some("ps_a"),
            Some(ReadQueryProfile::Compact),
        );
        let inspect_hash = response_page_hash_for_read_context(
            sql,
            &[],
            Some("ps_a"),
            Some(ReadQueryProfile::Inspect),
        );
        let cursor = encode_pagination_cursor(
            &server,
            PaginationCursorScope::ExecuteSql,
            &compact_hash,
            25,
        );
        assert_eq!(
            decode_pagination_cursor(
                &server,
                PaginationCursorScope::ExecuteSql,
                &inspect_hash,
                &cursor,
            ),
            Err(PaginationCursorDecodeError::QueryMismatch)
        );
    }

    #[test]
    fn query_fingerprint_redacts_literals_but_preserves_query_shape() {
        let first =
            query_fingerprint("SELECT id FROM users WHERE org_id = 42 AND email = 'alice@x.com'");
        let second =
            query_fingerprint("SELECT id FROM users WHERE org_id = 9001 AND email = 'bob@y.net'");
        assert_eq!(first, second);
        assert!(first.starts_with("qf_"));
        assert_eq!(first.len(), 19);
    }

    #[test]
    fn query_fingerprint_changes_when_statement_shape_changes() {
        let base = query_fingerprint("SELECT id FROM users WHERE org_id = 42");
        let changed =
            query_fingerprint("SELECT id FROM users WHERE org_id = 42 ORDER BY created_at DESC");
        assert_ne!(base, changed);
    }

    #[test]
    fn query_fingerprint_redacts_dollar_quoted_literals() {
        let first = query_fingerprint("SELECT $$secret-token-123$$::text");
        let second = query_fingerprint("SELECT $$different-secret-999$$::text");
        assert_eq!(first, second);
        assert!(first.starts_with("qf_"));
    }

    #[test]
    fn query_fingerprint_redacts_tagged_dollar_quoted_literals_with_inner_double_dollar() {
        let first = query_fingerprint("SELECT $tag$abc$$def$tag$::text");
        let second = query_fingerprint("SELECT $tag$uvw$$xyz$tag$::text");
        assert_eq!(first, second);
        assert!(first.starts_with("qf_"));
    }

    #[test]
    fn pagination_cursor_rejects_scope_mismatch_and_tamper() {
        let server = cursor_test_server();
        let query_hash =
            response_page_hash("SELECT id FROM public.operator_review_queue ORDER BY id");
        let cursor =
            encode_pagination_cursor(&server, PaginationCursorScope::ExecuteSql, &query_hash, 5);
        assert_eq!(
            decode_pagination_cursor(
                &server,
                PaginationCursorScope::ListObjects,
                &query_hash,
                &cursor,
            ),
            Err(PaginationCursorDecodeError::Invalid)
        );
        let tampered_cursor = cursor.replacen(":5:", ":6:", 1);
        assert_eq!(
            decode_pagination_cursor(
                &server,
                PaginationCursorScope::ExecuteSql,
                &query_hash,
                &tampered_cursor,
            ),
            Err(PaginationCursorDecodeError::Invalid)
        );
    }

    #[test]
    fn pagination_cursor_rejects_expired_tokens() {
        let mut server = cursor_test_server();
        server.pagination_cursor_ttl = Duration::from_secs(1);
        let query_hash =
            response_page_hash("SELECT id FROM public.operator_review_queue ORDER BY id");
        let cursor =
            encode_pagination_cursor(&server, PaginationCursorScope::ExecuteSql, &query_hash, 1);
        let parts = cursor.splitn(6, ':').collect::<Vec<_>>();
        let [prefix, scope, cursor_hash, offset, _expires_at, signature] = parts.as_slice() else {
            panic!("cursor format should have six parts");
        };
        let expired_signed_payload = format!("{prefix}:{scope}:{cursor_hash}:{offset}:1");
        let expired_signature = pagination_cursor_signature(&server, &expired_signed_payload);
        let expired_cursor = format!("{expired_signed_payload}:{expired_signature}");
        assert_eq!(
            decode_pagination_cursor(
                &server,
                PaginationCursorScope::ExecuteSql,
                &query_hash,
                &expired_cursor,
            ),
            Err(PaginationCursorDecodeError::Expired)
        );
        assert!(
            signature.len() == CURSOR_SIGNATURE_HEX_LEN,
            "cursor signature length should remain stable"
        );
    }

    #[test]
    fn runtime_error_payload_is_minimal_and_fingerprinted() {
        let payload = normalize_error_payload_for_role(
            StartupRole::Runtime,
            json!({
                "error": "failed to connect",
                "code": "DB_CONNECT_FAILED",
                "reason": "db_connect_failed",
                "sqlstate": "08006",
                "detail": "detail should be hidden in runtime role",
                "position": "internal:42",
                "hint": "retry with backoff",
            }),
        );
        let object = payload
            .as_object()
            .expect("payload should remain an object");
        assert_eq!(
            object
                .get("detail_level")
                .and_then(serde_json::Value::as_str),
            Some("minimal")
        );
        assert_eq!(object.get("detail"), None);
        assert_eq!(object.get("position"), None);
        assert_eq!(
            object.get("retryable").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            object
                .get("error_class")
                .and_then(serde_json::Value::as_str),
            Some("transient_network")
        );
        assert!(
            object
                .get("fingerprint")
                .and_then(serde_json::Value::as_str)
                .map(|value| value.starts_with("err_"))
                == Some(true),
            "runtime payload should include a stable fingerprint"
        );
    }

    #[test]
    fn migrator_error_payload_retains_detail_and_position() {
        let payload = normalize_error_payload_for_role(
            StartupRole::Migrator,
            json!({
                "error": "query execution failed",
                "code": "DB_QUERY_FAILED",
                "reason": "db_query_failed",
                "sqlstate": "42P01",
                "detail": "relation missing from schema",
                "position": "original:15",
            }),
        );
        let object = payload
            .as_object()
            .expect("payload should remain an object");
        assert_eq!(
            object
                .get("detail_level")
                .and_then(serde_json::Value::as_str),
            Some("detailed")
        );
        assert_eq!(
            object.get("detail").and_then(serde_json::Value::as_str),
            Some("relation missing from schema")
        );
        assert_eq!(
            object.get("position").and_then(serde_json::Value::as_str),
            Some("original:15")
        );
        assert_eq!(
            object.get("retryable").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            object
                .get("error_class")
                .and_then(serde_json::Value::as_str),
            Some("object_not_found")
        );
        assert!(
            object
                .get("fingerprint")
                .and_then(serde_json::Value::as_str)
                .map(|value| value.starts_with("err_"))
                == Some(true),
            "detailed payload should include a stable fingerprint"
        );
    }

    #[test]
    fn timeout_errors_are_classified_as_statement_timeout() {
        let payload = normalize_error_payload_for_role(
            StartupRole::Migrator,
            json!({
                "error": "query execution failed: canceling statement due to statement timeout",
                "code": "DB_QUERY_FAILED",
                "reason": "db_query_failed",
                "sqlstate": "57014",
                "detail": "canceling statement due to statement timeout",
            }),
        );
        let object = payload
            .as_object()
            .expect("payload should remain an object");
        assert_eq!(
            object
                .get("error_class")
                .and_then(serde_json::Value::as_str),
            Some("statement_timeout")
        );
    }

    #[test]
    fn db_connect_config_invalid_is_classified_as_configuration() {
        let payload = normalize_error_payload_for_role(
            StartupRole::Runtime,
            json!({
                "error": "invalid PostgreSQL transport configuration: sslmode=require",
                "code": "DB_CONNECT_CONFIG_INVALID",
                "reason": "db_connect_config_invalid",
            }),
        );
        let object = payload
            .as_object()
            .expect("payload should remain an object");
        assert_eq!(
            object
                .get("error_class")
                .and_then(serde_json::Value::as_str),
            Some("configuration")
        );
    }

    #[test]
    fn discovery_predicate_sources_do_not_use_raw_dynamic_like_placeholders() {
        let schema_source = include_str!("tools/schema.rs");
        let query_source = include_str!("tools/query.rs");
        let disallowed_placeholders = ["LIKE {} ESCAPE '\\\\'", "ILIKE {} ESCAPE '\\\\'"];

        for needle in disallowed_placeholders {
            assert!(
                !schema_source.contains(needle),
                "schema discovery path must use canonical predicate helpers, found `{needle}`"
            );
            assert!(
                !query_source.contains(needle),
                "query hint discovery path must use canonical predicate helpers, found `{needle}`"
            );
        }
    }

    #[test]
    fn unbounded_select_hint_emits_for_select_star_without_limit() {
        let hints = pre_execution_hints_for_sql("SELECT * FROM public.operator_review_queue");
        assert_eq!(hints.len(), 1);
        assert!(hints[0].contains("SELECT * without LIMIT"));
    }

    #[test]
    fn unbounded_select_hint_skips_bounded_queries() {
        let bounded = pre_execution_hints_for_sql(
            "SELECT * FROM public.operator_review_queue ORDER BY id DESC LIMIT 25",
        );
        assert!(bounded.is_empty());
    }

    #[test]
    fn unbounded_select_hint_skips_scalar_aggregates() {
        let aggregate =
            pre_execution_hints_for_sql("SELECT COUNT(*) FROM public.operator_review_queue");
        assert!(aggregate.is_empty());
    }

    #[test]
    fn response_meta_reports_bounded_result_fields_when_truncated() {
        let output = QueryOutput {
            rows: vec![
                serde_json::Map::from_iter([("id".to_string(), serde_json::json!(1))]),
                serde_json::Map::from_iter([("id".to_string(), serde_json::json!(2))]),
            ],
            columns: vec![QueryColumn {
                name: "id".to_string(),
                pg_type: "int4".to_string(),
                nullable: Some(false),
            }],
            rows_affected: None,
        };
        let meta = response_meta_for_rows(
            &output,
            crate::config::ResponseOutputMode::Rows,
            false,
            Some(5),
            Some("abc123"),
            Some("opaque_cursor_token".to_string()),
            Some(10),
            Some(12),
            None,
            &[],
        );
        assert_eq!(
            meta.get("returned_rows")
                .and_then(serde_json::Value::as_u64),
            Some(2)
        );
        assert_eq!(
            meta.get("truncated").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            meta.get("has_more").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            meta.get("cursor_offset")
                .and_then(serde_json::Value::as_u64),
            Some(10)
        );
        assert_eq!(
            meta.get("next_offset").and_then(serde_json::Value::as_u64),
            Some(12)
        );
        assert_eq!(
            meta.get("next_cursor").and_then(serde_json::Value::as_str),
            Some("opaque_cursor_token")
        );
        assert!(
            meta.get("query_hints")
                .is_some_and(serde_json::Value::is_array)
        );
    }

    #[test]
    fn response_meta_is_deterministic_for_non_truncated_results() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "status".to_string(),
                serde_json::json!("ok"),
            )])],
            columns: vec![QueryColumn {
                name: "status".to_string(),
                pg_type: "text".to_string(),
                nullable: Some(true),
            }],
            rows_affected: None,
        };
        let meta = response_meta_for_rows(
            &output,
            crate::config::ResponseOutputMode::DataOnly,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
        );
        assert_eq!(
            meta.get("returned_rows")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            meta.get("truncated").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            meta.get("has_more").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert!(
            meta.get("next_cursor")
                .is_some_and(serde_json::Value::is_null)
        );
        assert!(
            meta.get("next_offset")
                .is_some_and(serde_json::Value::is_null)
        );
        assert_eq!(
            meta.get("query_hints")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(0)
        );
    }

    #[test]
    fn response_meta_preserves_hints_and_clipping_telemetry() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "reason".to_string(),
                serde_json::json!("abcdefghijklmnopqrstuvwxyz"),
            )])],
            columns: vec![QueryColumn {
                name: "reason".to_string(),
                pg_type: "text".to_string(),
                nullable: Some(true),
            }],
            rows_affected: None,
        };
        let hints = vec![
            "Query appears unbounded (SELECT * without LIMIT). Consider adding LIMIT.".to_string(),
        ];
        let meta = response_meta_for_rows(
            &output,
            crate::config::ResponseOutputMode::Rows,
            false,
            Some(3),
            Some("84c44961e673fe5d"),
            Some("opaque_cursor_token_1".to_string()),
            Some(0),
            Some(1),
            Some(CellClipMeta {
                max_cell_chars: 40,
                clipped_cells: 1,
            }),
            &hints,
        );
        assert_eq!(
            meta.get("query_hints")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            meta.pointer("/cell_clipping/enabled")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            meta.pointer("/cell_clipping/applied")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            meta.pointer("/cell_clipping/clipped_cells")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
    }

    #[test]
    fn response_meta_marks_summary_only_when_requested() {
        let output = QueryOutput {
            rows: vec![
                serde_json::Map::from_iter([("id".to_string(), serde_json::json!(1))]),
                serde_json::Map::from_iter([("id".to_string(), serde_json::json!(2))]),
            ],
            columns: vec![QueryColumn {
                name: "id".to_string(),
                pg_type: "int4".to_string(),
                nullable: Some(false),
            }],
            rows_affected: None,
        };
        let meta = response_meta_for_rows(
            &output,
            crate::config::ResponseOutputMode::Rows,
            true,
            Some(2),
            Some("84c44961e673fe5d"),
            Some("opaque_cursor_token_1".to_string()),
            Some(0),
            Some(1),
            None,
            &[],
        );
        assert_eq!(
            meta.get("summary_only")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn response_meta_marks_count_exact_when_total_count_is_present() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "id".to_string(),
                serde_json::json!(1),
            )])],
            columns: vec![QueryColumn {
                name: "id".to_string(),
                pg_type: "int4".to_string(),
                nullable: Some(false),
            }],
            rows_affected: None,
        };
        let meta = response_meta_for_rows(
            &output,
            crate::config::ResponseOutputMode::Rows,
            false,
            Some(10),
            Some("84c44961e673fe5d"),
            Some("opaque_cursor_token_1".to_string()),
            Some(0),
            Some(1),
            None,
            &[],
        );
        assert_eq!(
            meta.get("row_count_mode")
                .and_then(serde_json::Value::as_str),
            Some("count_exact")
        );
    }

    #[test]
    fn response_meta_marks_page_window_when_total_count_is_skipped() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "id".to_string(),
                serde_json::json!(1),
            )])],
            columns: vec![QueryColumn {
                name: "id".to_string(),
                pg_type: "int4".to_string(),
                nullable: Some(false),
            }],
            rows_affected: None,
        };
        let meta = response_meta_for_rows(
            &output,
            crate::config::ResponseOutputMode::Rows,
            false,
            None,
            Some("84c44961e673fe5d"),
            Some("opaque_cursor_token_1".to_string()),
            Some(0),
            Some(1),
            None,
            &[],
        );
        assert_eq!(
            meta.get("row_count_mode")
                .and_then(serde_json::Value::as_str),
            Some("page_window")
        );
    }

    #[test]
    fn response_meta_treats_next_cursor_as_canonical_has_more_signal() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "id".to_string(),
                serde_json::json!(1),
            )])],
            columns: vec![QueryColumn {
                name: "id".to_string(),
                pg_type: "int4".to_string(),
                nullable: Some(false),
            }],
            rows_affected: None,
        };
        let meta = response_meta_for_rows(
            &output,
            crate::config::ResponseOutputMode::Rows,
            false,
            Some(99),
            Some("84c44961e673fe5d"),
            None,
            Some(98),
            Some(99),
            None,
            &[],
        );
        assert_eq!(
            meta.get("has_more").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            meta.get("truncated").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert!(
            meta.get("next_offset")
                .is_some_and(serde_json::Value::is_null)
        );
    }

    #[test]
    fn query_success_summary_only_omits_data_for_v2_contract() {
        let server = contract_test_server(ResponseMode::V2);
        let output = QueryOutput {
            rows: vec![
                serde_json::Map::from_iter([("id".to_string(), serde_json::json!(1))]),
                serde_json::Map::from_iter([("id".to_string(), serde_json::json!(2))]),
            ],
            columns: vec![QueryColumn {
                name: "id".to_string(),
                pg_type: "int4".to_string(),
                nullable: Some(false),
            }],
            rows_affected: None,
        };

        let result = query_success(
            &server,
            &output,
            ResponseOutputMode::Rows,
            7,
            Some(4),
            Some("84c44961e673fe5d"),
            Some("opaque_cursor_token_2".to_string()),
            Some(2),
            Some(4),
            None,
            vec![],
            true,
            false,
        );
        let payload = result
            .structured_content
            .expect("query_success should emit structured content");
        assert!(payload.get("data").is_some_and(serde_json::Value::is_null));
        assert_eq!(
            payload
                .pointer("/meta/summary_only")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            payload
                .pointer("/meta/returned_rows")
                .and_then(serde_json::Value::as_u64),
            Some(2)
        );
    }

    #[test]
    fn query_success_without_summary_only_keeps_v2_data_payload() {
        let server = contract_test_server(ResponseMode::V2);
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "id".to_string(),
                serde_json::json!(1),
            )])],
            columns: vec![QueryColumn {
                name: "id".to_string(),
                pg_type: "int4".to_string(),
                nullable: Some(false),
            }],
            rows_affected: None,
        };

        let result = query_success(
            &server,
            &output,
            ResponseOutputMode::Rows,
            5,
            None,
            None,
            None,
            None,
            None,
            None,
            vec![],
            false,
            false,
        );
        let payload = result
            .structured_content
            .expect("query_success should emit structured content");
        assert!(payload.get("data").is_some_and(serde_json::Value::is_array));
        assert_eq!(
            payload
                .pointer("/meta/summary_only")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn query_success_auto_mode_resolves_to_scalar_for_single_cell_results() {
        let server = contract_test_server_with_modes(
            ResponseMode::V2,
            ResponseOutputMode::Auto,
            ResponseAutoTabularMode::Tuples,
        );
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "count".to_string(),
                serde_json::json!(42),
            )])],
            columns: vec![QueryColumn {
                name: "count".to_string(),
                pg_type: "int8".to_string(),
                nullable: Some(false),
            }],
            rows_affected: None,
        };

        let result = query_success(
            &server,
            &output,
            ResponseOutputMode::Auto,
            5,
            Some(2),
            Some("84c44961e673fe5d"),
            None,
            None,
            None,
            None,
            vec![],
            false,
            false,
        );
        let payload = result
            .structured_content
            .expect("query_success should emit structured content");
        assert_eq!(
            payload.pointer("/meta/output_mode"),
            Some(&serde_json::json!("scalar"))
        );
        assert_eq!(payload.get("data"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn query_success_auto_mode_uses_configured_tabular_fallback() {
        let server = contract_test_server_with_modes(
            ResponseMode::V2,
            ResponseOutputMode::Auto,
            ResponseAutoTabularMode::Rows,
        );
        let output = QueryOutput {
            rows: vec![
                serde_json::Map::from_iter([("id".to_string(), serde_json::json!(1))]),
                serde_json::Map::from_iter([("id".to_string(), serde_json::json!(2))]),
            ],
            columns: vec![QueryColumn {
                name: "id".to_string(),
                pg_type: "int4".to_string(),
                nullable: Some(false),
            }],
            rows_affected: None,
        };

        let result = query_success(
            &server,
            &output,
            ResponseOutputMode::Auto,
            5,
            Some(2),
            Some("84c44961e673fe5d"),
            Some("opaque_cursor_token_1".to_string()),
            Some(0),
            Some(2),
            None,
            vec![],
            false,
            false,
        );
        let payload = result
            .structured_content
            .expect("query_success should emit structured content");
        assert_eq!(
            payload.pointer("/meta/output_mode"),
            Some(&serde_json::json!("rows"))
        );
        assert!(payload.get("data").is_some_and(serde_json::Value::is_array));
        assert_eq!(
            payload.pointer("/meta/returned_rows"),
            Some(&serde_json::json!(2))
        );
    }

    #[test]
    fn query_success_auto_mode_can_resolve_to_rows_safe_fallback() {
        let server = contract_test_server_with_modes(
            ResponseMode::V2,
            ResponseOutputMode::Auto,
            ResponseAutoTabularMode::RowsSafe,
        );
        let output = QueryOutput {
            rows: vec![
                serde_json::Map::from_iter([("id".to_string(), serde_json::json!(1))]),
                serde_json::Map::from_iter([("id".to_string(), serde_json::json!(2))]),
            ],
            columns: vec![QueryColumn {
                name: "id".to_string(),
                pg_type: "int4".to_string(),
                nullable: Some(false),
            }],
            rows_affected: None,
        };

        let payload = query_success(
            &server,
            &output,
            ResponseOutputMode::Auto,
            5,
            Some(2),
            Some("84c44961e673fe5d"),
            None,
            None,
            None,
            None,
            vec![],
            false,
            false,
        )
        .structured_content
        .expect("query_success should emit structured content");
        assert_eq!(
            payload.pointer("/meta/output_mode"),
            Some(&serde_json::json!("rows_safe"))
        );
    }

    #[test]
    fn query_success_rows_safe_mode_returns_collision_safe_objects() {
        let server = contract_test_server(ResponseMode::V2);
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([
                ("id".to_string(), serde_json::json!(1)),
                ("id__dup2".to_string(), serde_json::json!(2)),
            ])],
            columns: vec![
                QueryColumn {
                    name: "id".to_string(),
                    pg_type: "int4".to_string(),
                    nullable: Some(false),
                },
                QueryColumn {
                    name: "id__dup2".to_string(),
                    pg_type: "int4".to_string(),
                    nullable: Some(false),
                },
            ],
            rows_affected: None,
        };

        let result = query_success(
            &server,
            &output,
            ResponseOutputMode::RowsSafe,
            5,
            None,
            None,
            None,
            None,
            None,
            None,
            vec![],
            false,
            false,
        );
        let payload = result
            .structured_content
            .expect("query_success should emit structured content");
        assert_eq!(
            payload.pointer("/meta/output_mode"),
            Some(&serde_json::json!("rows_safe"))
        );
        let rows = payload
            .get("data")
            .and_then(serde_json::Value::as_array)
            .expect("rows_safe should return array payload");
        let row = rows
            .first()
            .and_then(serde_json::Value::as_object)
            .expect("rows_safe row payload should remain an object");
        assert!(row.contains_key("id"));
        assert!(row.contains_key("id__dup2"));
    }

    #[test]
    fn query_success_adds_hint_for_auto_aliased_duplicate_columns() {
        let server = contract_test_server(ResponseMode::V2);
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([
                ("id".to_string(), serde_json::json!(1)),
                ("id__dup2".to_string(), serde_json::json!(2)),
            ])],
            columns: vec![
                QueryColumn {
                    name: "id".to_string(),
                    pg_type: "int4".to_string(),
                    nullable: Some(false),
                },
                QueryColumn {
                    name: "id__dup2".to_string(),
                    pg_type: "int4".to_string(),
                    nullable: Some(false),
                },
            ],
            rows_affected: None,
        };

        let result = query_success(
            &server,
            &output,
            ResponseOutputMode::Rows,
            5,
            None,
            None,
            None,
            None,
            None,
            None,
            vec![],
            false,
            false,
        );
        let payload = result
            .structured_content
            .expect("query_success should emit structured content");
        let hints = payload
            .pointer("/meta/query_hints")
            .and_then(serde_json::Value::as_array)
            .expect("query_hints should be present");
        assert!(
            hints.iter().any(|hint| hint.as_str().is_some_and(
                |value| value.contains("duplicate output column names were auto-aliased")
            )),
            "expected duplicate alias hint in response metadata"
        );
        assert_eq!(
            payload.pointer("/meta/column_name_safety/duplicate_columns_aliased"),
            Some(&serde_json::json!(true))
        );
        assert_eq!(
            payload.pointer("/meta/column_name_safety/aliased_columns/0"),
            Some(&serde_json::json!("id__dup2"))
        );
    }

    #[test]
    fn query_success_markdown_text_replaces_default_json_text_content() {
        let server = contract_test_server(ResponseMode::V2);
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([
                ("id".to_string(), serde_json::json!(1)),
                ("name".to_string(), serde_json::json!("alice|bob")),
            ])],
            columns: vec![
                QueryColumn {
                    name: "id".to_string(),
                    pg_type: "int4".to_string(),
                    nullable: Some(false),
                },
                QueryColumn {
                    name: "name".to_string(),
                    pg_type: "text".to_string(),
                    nullable: Some(true),
                },
            ],
            rows_affected: None,
        };

        let result = query_success(
            &server,
            &output,
            ResponseOutputMode::Rows,
            5,
            None,
            None,
            None,
            None,
            None,
            None,
            vec![],
            false,
            true,
        );

        let text = result
            .content
            .first()
            .and_then(|content| content.raw.as_text())
            .map(|text| text.text.as_str())
            .expect("query_success should emit text content");
        assert!(text.contains("| id | name |"));
        assert!(text.contains("| 1 | alice\\|bob |"));

        let payload = result
            .structured_content
            .expect("structured payload should remain available");
        assert_eq!(payload.pointer("/data/0/id"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn apply_currency_display_mode_noop_without_currency_mode() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "price_cents".to_string(),
                serde_json::json!(12345),
            )])],
            columns: vec![QueryColumn {
                name: "price_cents".to_string(),
                pg_type: "int8".to_string(),
                nullable: Some(true),
            }],
            rows_affected: None,
        };

        let (formatted_output, formatted_columns) =
            apply_currency_display_mode(&output, None, &["price_cents".to_string()]);

        assert_eq!(formatted_output.rows, output.rows);
        assert_eq!(formatted_output.columns.len(), output.columns.len());
        for (actual, expected) in formatted_output.columns.iter().zip(output.columns.iter()) {
            assert_eq!(actual.name, expected.name);
            assert_eq!(actual.pg_type, expected.pg_type);
            assert_eq!(actual.nullable, expected.nullable);
        }
        assert!(formatted_columns.is_empty());
    }

    #[test]
    fn apply_currency_display_mode_formats_suffix_and_explicit_columns() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([
                ("price_cents".to_string(), serde_json::json!(12345)),
                ("total".to_string(), serde_json::json!(400)),
                ("notes".to_string(), serde_json::json!("ok")),
            ])],
            columns: vec![
                QueryColumn {
                    name: "price_cents".to_string(),
                    pg_type: "int8".to_string(),
                    nullable: Some(true),
                },
                QueryColumn {
                    name: "total".to_string(),
                    pg_type: "int4".to_string(),
                    nullable: Some(true),
                },
                QueryColumn {
                    name: "notes".to_string(),
                    pg_type: "text".to_string(),
                    nullable: Some(true),
                },
            ],
            rows_affected: None,
        };

        let (formatted_output, formatted_columns) = apply_currency_display_mode(
            &output,
            Some(ResponseFormattingMode::Currency),
            &["total".to_string()],
        );

        assert_eq!(
            formatted_columns,
            vec![
                "price_cents_formatted".to_string(),
                "total_formatted".to_string()
            ]
        );
        assert_eq!(formatted_output.columns.len(), 5);
        let first_row = formatted_output.rows.first().expect("expected one row");
        assert_eq!(
            first_row.get("price_cents"),
            Some(&serde_json::json!(12345))
        );
        assert_eq!(first_row.get("total"), Some(&serde_json::json!(400)));
        assert_eq!(
            first_row.get("price_cents_formatted"),
            Some(&serde_json::json!("123.45"))
        );
        assert_eq!(
            first_row.get("total_formatted"),
            Some(&serde_json::json!("4.00"))
        );
    }

    #[test]
    fn apply_currency_display_mode_keeps_invalid_currency_values_as_null() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "price_cents".to_string(),
                serde_json::json!("abc"),
            )])],
            columns: vec![QueryColumn {
                name: "price_cents".to_string(),
                pg_type: "int8".to_string(),
                nullable: Some(true),
            }],
            rows_affected: None,
        };

        let (formatted_output, formatted_columns) =
            apply_currency_display_mode(&output, Some(ResponseFormattingMode::Currency), &[]);

        assert_eq!(formatted_columns, vec!["price_cents_formatted".to_string()]);
        let first_row = formatted_output.rows.first().expect("expected one row");
        assert!(
            first_row
                .get("price_cents_formatted")
                .is_some_and(serde_json::Value::is_null)
        );
    }

    #[test]
    fn apply_currency_display_mode_resolves_name_collision_for_formatted_columns() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([
                ("price_cents".to_string(), serde_json::json!(500)),
                (
                    "price_cents_formatted".to_string(),
                    serde_json::json!("already_formatted"),
                ),
            ])],
            columns: vec![
                QueryColumn {
                    name: "price_cents".to_string(),
                    pg_type: "int8".to_string(),
                    nullable: Some(true),
                },
                QueryColumn {
                    name: "price_cents_formatted".to_string(),
                    pg_type: "text".to_string(),
                    nullable: Some(true),
                },
            ],
            rows_affected: None,
        };

        let (formatted_output, formatted_columns) =
            apply_currency_display_mode(&output, Some(ResponseFormattingMode::Currency), &[]);

        assert_eq!(
            formatted_columns,
            vec!["price_cents_formatted_1".to_string()]
        );
        let first_row = formatted_output.rows.first().expect("expected one row");
        assert_eq!(
            first_row.get("price_cents_formatted_1"),
            Some(&serde_json::json!("5.00"))
        );
    }

    #[test]
    fn apply_currency_display_mode_limits_formatted_columns_to_maximum() {
        let mut row = serde_json::Map::new();
        let mut columns = Vec::with_capacity(35);

        for index in 0..35 {
            let name = format!("col{index}_cents");
            row.insert(name.clone(), serde_json::json!((100 * (index + 1)) as i64));
            columns.push(QueryColumn {
                name,
                pg_type: "int8".to_string(),
                nullable: Some(true),
            });
        }

        let output = QueryOutput {
            rows: vec![row],
            columns,
            rows_affected: None,
        };

        let (formatted_output, formatted_columns) =
            apply_currency_display_mode(&output, Some(ResponseFormattingMode::Currency), &[]);

        assert_eq!(formatted_columns.len(), 32);
        assert_eq!(formatted_output.columns.len(), 67);
    }

    #[test]
    fn clip_query_output_cells_truncates_oversized_strings_only() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([
                (
                    "message".to_string(),
                    serde_json::json!("abcdefghijklmnopqrstuvwxyz"),
                ),
                ("count".to_string(), serde_json::json!(42)),
            ])],
            columns: vec![
                QueryColumn {
                    name: "message".to_string(),
                    pg_type: "text".to_string(),
                    nullable: Some(true),
                },
                QueryColumn {
                    name: "count".to_string(),
                    pg_type: "int4".to_string(),
                    nullable: Some(false),
                },
            ],
            rows_affected: None,
        };
        let (clipped, clipped_cells) = clip_query_output_cells(&output, 8);
        assert_eq!(clipped_cells, 1);
        assert_eq!(
            clipped.rows[0]
                .get("message")
                .and_then(serde_json::Value::as_str),
            Some("abcde...")
        );
        assert_eq!(
            clipped.rows[0]
                .get("count")
                .and_then(serde_json::Value::as_i64),
            Some(42)
        );
    }

    #[test]
    fn clip_query_output_cells_noops_when_values_fit_limit() {
        let output = QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "message".to_string(),
                serde_json::json!("short"),
            )])],
            columns: vec![QueryColumn {
                name: "message".to_string(),
                pg_type: "text".to_string(),
                nullable: Some(true),
            }],
            rows_affected: None,
        };
        let (clipped, clipped_cells) = clip_query_output_cells(&output, 16);
        assert_eq!(clipped_cells, 0);
        assert_eq!(clipped.rows, output.rows);
    }

    #[test]
    fn tool_schema_snapshot_contract_is_stable() {
        let server = contract_test_server(ResponseMode::V2);
        let tools = sanitize_tool_schemas_for_mcp(server.discoverable_tools());
        let snapshot_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("spec/tool_schema_snapshot.v1.json");
        assert_tool_schema_snapshot(snapshot_path, &tools);
    }

    #[test]
    fn parse_health_all() {
        let parsed = parse_health_types("all").expect("parse should pass");
        assert!(parsed.contains(&HealthType::Vacuum));
        assert!(parsed.contains(&HealthType::Constraint));
    }

    #[test]
    fn health_sql_avoids_reserved_table_aliases() {
        assert!(!VACUUM_HEALTH_SQL.contains(" AS table,"));
        assert!(!CONSTRAINT_HEALTH_SQL.contains(" AS table,"));
        assert!(VACUUM_HEALTH_SQL.contains(" AS table_name,"));
        assert!(CONSTRAINT_HEALTH_SQL.contains(" AS table_name,"));
    }

    #[test]
    fn health_sql_includes_schema_qualified_relation_columns() {
        assert!(VACUUM_HEALTH_SQL.contains("format('%I.%I', n.nspname, c.relname) AS relation"));
        assert!(
            CONSTRAINT_HEALTH_SQL.contains("format('%I.%I', nsp.nspname, rel.relname) AS relation")
        );
    }

    fn default_query_budget() -> (Option<Duration>, Option<Duration>, Option<Duration>) {
        (
            Some(Duration::from_secs(15)),
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(2)),
        )
    }

    fn contract_test_server(response_mode: ResponseMode) -> PostgresMcp {
        contract_test_server_with_modes(
            response_mode,
            ResponseOutputMode::Rows,
            ResponseAutoTabularMode::Rows,
        )
    }

    fn contract_test_server_with_modes(
        response_mode: ResponseMode,
        response_output_mode: ResponseOutputMode,
        response_output_mode_auto_tabular: ResponseAutoTabularMode,
    ) -> PostgresMcp {
        let (query_timeout, statement_timeout, lock_timeout) = default_query_budget();
        PostgresMcp::with_response_contract(
            Arc::new(DbEngine::new(
                None,
                AccessMode::Unrestricted,
                false,
                query_timeout,
                statement_timeout,
                lock_timeout,
            )),
            response_mode,
            response_output_mode,
            response_output_mode_auto_tabular,
            200,
        )
    }

    fn live_db_test_server_from_env() -> Option<PostgresMcp> {
        let database_uri = env::var("DATABASE_URI").ok()?;
        if database_uri.trim().is_empty() {
            return None;
        }
        let (query_timeout, statement_timeout, lock_timeout) = default_query_budget();
        Some(PostgresMcp::new(Arc::new(DbEngine::new(
            Some(database_uri),
            AccessMode::Unrestricted,
            false,
            query_timeout,
            statement_timeout,
            lock_timeout,
        ))))
    }

    fn value_contains_sqlstate(value: &serde_json::Value, expected: &str) -> bool {
        match value {
            serde_json::Value::Object(map) => {
                if map
                    .get("sqlstate")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| value == expected)
                {
                    return true;
                }
                map.values()
                    .any(|inner| value_contains_sqlstate(inner, expected))
            }
            serde_json::Value::Array(items) => items
                .iter()
                .any(|inner| value_contains_sqlstate(inner, expected)),
            _ => false,
        }
    }

    #[tokio::test]
    async fn health_vacuum_constraint_and_all_paths_avoid_sqlstate_42601() {
        let Some(server) = live_db_test_server_from_env() else {
            eprintln!("skipping live DB health SQL regression test (DATABASE_URI not set)");
            return;
        };

        let vacuum = run_vacuum_health(&server).await;
        assert!(
            !value_contains_sqlstate(&vacuum, "42601"),
            "vacuum health query should not return syntax errors: {vacuum:?}"
        );

        let constraint = run_constraint_health(&server).await;
        assert!(
            !value_contains_sqlstate(&constraint, "42601"),
            "constraint health query should not return syntax errors: {constraint:?}"
        );

        let requested = parse_health_types("all").expect("all should parse");
        let mut results = BTreeMap::new();
        if requested.contains(&HealthType::Vacuum) {
            results.insert("vacuum", run_vacuum_health(&server).await);
        }
        if requested.contains(&HealthType::Constraint) {
            results.insert("constraint", run_constraint_health(&server).await);
        }
        let aggregate = serde_json::json!({ "results": results });
        assert!(
            !value_contains_sqlstate(&aggregate, "42601"),
            "aggregate all path should not include syntax errors: {aggregate:?}"
        );
    }

    #[test]
    fn validate_sql_size_rejects_oversized_payload() {
        let oversized = "x".repeat(MAX_SQL_INPUT_BYTES + 1);
        let err = validate_sql_size(&oversized, "sql").expect_err("oversized sql should fail");
        assert!(err.contains("exceeds maximum size"));
    }

    #[test]
    fn extension_missing_signature_detects_pg_stat_statements_relation_errors() {
        assert!(is_extension_missing_signature(
            ExtensionCapability::PgStatStatements,
            Some("42P01"),
            "relation \"pg_stat_statements\" does not exist",
        ));
    }

    #[test]
    fn extension_missing_signature_ignores_non_missing_or_unrelated_errors() {
        assert!(!is_extension_missing_signature(
            ExtensionCapability::Hypopg,
            Some("22012"),
            "division by zero",
        ));
        assert!(!is_extension_missing_signature(
            ExtensionCapability::Hypopg,
            Some("42P01"),
            "relation \"users\" does not exist",
        ));
        assert!(!is_extension_missing_signature(
            ExtensionCapability::PgStatStatements,
            Some("42883"),
            "function pg_stat_statements() does not exist",
        ));
    }

    #[test]
    fn extension_check_in_progress_contract_is_stable() {
        let err = extension_check_in_progress_error("hypopg");
        assert_eq!(err.code, "EXTENSION_CHECK_IN_PROGRESS");
        assert_eq!(err.reason, "extension_check_in_progress");
        assert!(
            err.details
                .get("retry_after_ms")
                .and_then(serde_json::Value::as_u64)
                .is_some()
        );
    }

    #[tokio::test]
    async fn extension_transition_runtime_integration() {
        let run = env::var("POSTGRES_MCP_RUN_EXTENSION_TRANSITION_TEST").unwrap_or_default();
        if run.trim() != "1" {
            eprintln!(
                "skipping extension_transition_runtime_integration \
                 (set POSTGRES_MCP_RUN_EXTENSION_TRANSITION_TEST=1 to enable)"
            );
            return;
        }

        let database_uri = env::var("POSTGRES_MCP_EXTENSION_TRANSITION_TEST_URI")
            .or_else(|_| env::var("DATABASE_URI"))
            .expect(
                "set POSTGRES_MCP_EXTENSION_TRANSITION_TEST_URI or DATABASE_URI when \
                 POSTGRES_MCP_RUN_EXTENSION_TRANSITION_TEST=1",
            );

        let _lock = extension_transition_test_lock().lock().await;

        let server = PostgresMcp::new(Arc::new(DbEngine::new(
            Some(database_uri),
            AccessMode::Unrestricted,
            false,
            Some(Duration::from_secs(15)),
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(2)),
        )));
        let extension = ExtensionCapability::Hypopg;

        server
            .extension_guard
            .invalidate(&extension)
            .expect("invalidate extension guard before test");
        server.extension_unavailable_cache.clear(&extension);

        let refresh_state = server
            .extension_guard
            .begin_refresh(extension)
            .expect("begin refresh for in-progress simulation");
        assert_eq!(refresh_state, CapabilityRefreshState::StartRefresh);

        let in_flight =
            ensure_extension_ready(&server, extension, "hypothetical_indexing_unavailable")
                .await
                .expect_err("expected in-progress error while refresh is active");
        assert_eq!(in_flight.code, "EXTENSION_CHECK_IN_PROGRESS");
        assert_eq!(in_flight.reason, "extension_check_in_progress");

        server
            .extension_guard
            .complete_refresh(extension, false)
            .expect("complete simulated in-flight refresh");
        server
            .extension_guard
            .invalidate(&extension)
            .expect("invalidate extension guard after in-flight simulation");
        server.extension_unavailable_cache.clear(&extension);

        server
            .db
            .execute_query_unrestricted("DROP EXTENSION IF EXISTS hypopg;")
            .await
            .expect("drop hypopg extension");
        server
            .extension_guard
            .invalidate(&extension)
            .expect("invalidate extension guard after drop");
        server.extension_unavailable_cache.clear(&extension);

        let unavailable =
            ensure_extension_ready(&server, extension, "hypothetical_indexing_unavailable")
                .await
                .expect_err("expected unavailable after dropping hypopg");
        assert_eq!(unavailable.code, "EXTENSION_UNAVAILABLE");
        assert_eq!(unavailable.reason, "hypothetical_indexing_unavailable");

        server
            .db
            .execute_query_unrestricted("CREATE EXTENSION IF NOT EXISTS hypopg;")
            .await
            .expect("create hypopg extension");
        server
            .extension_guard
            .invalidate(&extension)
            .expect("invalidate extension guard after create");
        server.extension_unavailable_cache.clear(&extension);

        ensure_extension_ready(&server, extension, "hypothetical_indexing_unavailable")
            .await
            .expect("hypopg should be detected as installed");
        server
            .db
            .execute_query_unrestricted("SELECT hypopg_reset();")
            .await
            .expect("hypopg_reset should execute when extension is installed");
    }
}
