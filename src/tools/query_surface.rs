use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use super::query::{
    derive_execute_sql_hint, derive_execute_sql_schema_hints, describe_sql_with_optional_session,
    execute_sql_with_optional_session,
};
use super::*;
use crate::server::{ExportArtifactRecord, QueryJobState};

const QUERY_RENDER_DEFAULT_MAX_ROWS: usize = 25;
const QUERY_RENDER_MAX_ROWS_CAP: usize = 100;
const QUERY_RENDER_MAX_CELL_CHARS: usize = 256;
const ADMIN_RETURNING_ROWS_CAP: usize = 100;
const EXPORT_ARTIFACT_URI_PREFIX: &str = "postgres://artifacts/";
const STATEMENT_TIMEOUT_MAX_MS: u64 = 300_000;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct QuerySqlArgsVNext {
    pub sql: String,
    #[serde(default)]
    pub params: Option<Vec<Value>>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub max_rows: Option<usize>,
    #[serde(default)]
    pub max_cell_chars: Option<usize>,
    #[serde(default)]
    pub statement_timeout_ms: Option<u64>,
    #[serde(default)]
    pub preflight_check: Option<bool>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub profile: Option<ReadQueryProfile>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct QueryTuplesArgs {
    pub sql: String,
    #[serde(default)]
    pub params: Option<Vec<Value>>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub max_rows: Option<usize>,
    #[serde(default)]
    pub max_cell_chars: Option<usize>,
    #[serde(default)]
    pub statement_timeout_ms: Option<u64>,
    #[serde(default)]
    pub preflight_check: Option<bool>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub profile: Option<ReadQueryProfile>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct RenderSqlArgs {
    pub sql: String,
    #[serde(default)]
    pub params: Option<Vec<Value>>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub max_rows: Option<usize>,
    #[serde(default)]
    pub max_cell_chars: Option<usize>,
    #[serde(default)]
    pub statement_timeout_ms: Option<u64>,
    #[serde(default)]
    pub preflight_check: Option<bool>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub profile: Option<ReadQueryProfile>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct DescribeSqlArgs {
    pub sql: String,
    #[serde(default)]
    pub params: Option<Vec<Value>>,
    #[serde(default)]
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct AdminSqlArgs {
    pub sql: String,
    #[serde(default)]
    pub params: Option<Vec<Value>>,
    #[serde(default)]
    pub statement_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ExportJobStartArgs {
    pub sql: String,
    #[serde(default)]
    pub params: Option<Vec<Value>>,
    #[serde(default)]
    pub format: Option<ExecuteSqlExportFormat>,
    #[serde(default)]
    pub statement_timeout_ms: Option<u64>,
    #[serde(default)]
    pub preflight_check: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct JobStatusArgsVNext {
    pub job_id: String,
    #[serde(default)]
    pub wait_ms: Option<u64>,
    #[serde(default)]
    pub wait_until_terminal: bool,
    #[serde(default)]
    pub include_result: bool,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct JobCancelArgsVNext {
    pub job_id: String,
}

#[derive(Debug, Clone)]
struct ReadQueryResult {
    output: QueryOutput,
    elapsed_ms: u64,
    next_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadToolKind {
    Query,
    Tuple,
    Render,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResolvedReadQuerySettings {
    max_rows: Option<usize>,
    max_cell_chars: Option<usize>,
    preflight_check: bool,
}

fn resolve_read_query_settings(
    tool_kind: ReadToolKind,
    profile: Option<ReadQueryProfile>,
    max_rows: Option<usize>,
    max_cell_chars: Option<usize>,
    preflight_check: Option<bool>,
    response_page_size: usize,
) -> ResolvedReadQuerySettings {
    match tool_kind {
        ReadToolKind::Query | ReadToolKind::Tuple => match profile {
            Some(ReadQueryProfile::Compact) => ResolvedReadQuerySettings {
                max_rows: Some(max_rows.unwrap_or(response_page_size.clamp(1, 100))),
                max_cell_chars: max_cell_chars.or(Some(256)),
                preflight_check: preflight_check.unwrap_or(false),
            },
            Some(ReadQueryProfile::Inspect) => ResolvedReadQuerySettings {
                max_rows: Some(
                    max_rows.unwrap_or(response_page_size.clamp(1, QUERY_RENDER_MAX_ROWS_CAP)),
                ),
                max_cell_chars,
                preflight_check: preflight_check.unwrap_or(true),
            },
            None => ResolvedReadQuerySettings {
                max_rows,
                max_cell_chars,
                preflight_check: preflight_check.unwrap_or(false),
            },
        },
        ReadToolKind::Render => {
            let default_max_rows = match profile {
                Some(ReadQueryProfile::Inspect) => QUERY_RENDER_MAX_ROWS_CAP,
                _ => QUERY_RENDER_DEFAULT_MAX_ROWS,
            };
            let resolved_max_rows = max_rows
                .unwrap_or(default_max_rows)
                .clamp(1, QUERY_RENDER_MAX_ROWS_CAP);
            let resolved_max_cell_chars = max_cell_chars
                .unwrap_or(QUERY_RENDER_MAX_CELL_CHARS)
                .clamp(1, QUERY_RENDER_MAX_CELL_CHARS);
            ResolvedReadQuerySettings {
                max_rows: Some(resolved_max_rows),
                max_cell_chars: Some(resolved_max_cell_chars),
                preflight_check: preflight_check.unwrap_or(true),
            }
        }
    }
}

fn response_page_hash_for_session(sql: &str, params: &[Value], session_id: Option<&str>) -> String {
    let base = response_page_hash_for_params(sql, params);
    let Some(session_id) = session_id else {
        return base;
    };
    let mut hasher = Sha256::new();
    hasher.update(base.as_bytes());
    hasher.update(b"\0session\0");
    hasher.update(session_id.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    hex[..16].to_string()
}

fn response_page_hash_with_surface_salt(base: String, surface_salt: Option<&str>) -> String {
    let Some(surface_salt) = surface_salt else {
        return base;
    };
    let mut hasher = Sha256::new();
    hasher.update(base.as_bytes());
    hasher.update(b"\0surface\0");
    hasher.update(surface_salt.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    hex[..16].to_string()
}

pub(super) fn response_page_hash_for_read_context(
    sql: &str,
    params: &[Value],
    session_id: Option<&str>,
    profile: Option<ReadQueryProfile>,
) -> String {
    let base = response_page_hash_for_session(sql, params, session_id);
    let with_profile = if let Some(profile) = profile {
        let mut hasher = Sha256::new();
        hasher.update(base.as_bytes());
        hasher.update(b"\0profile\0");
        hasher.update(profile.as_str().as_bytes());
        let digest = hasher.finalize();
        let hex = format!("{digest:x}");
        hex[..16].to_string()
    } else {
        base
    };
    response_page_hash_with_surface_salt(with_profile, None)
}

fn response_page_hash_for_tuple_context(
    sql: &str,
    params: &[Value],
    session_id: Option<&str>,
    profile: Option<ReadQueryProfile>,
) -> String {
    response_page_hash_with_surface_salt(
        response_page_hash_for_read_context(sql, params, session_id, profile),
        Some("query_tuples"),
    )
}

#[derive(Debug, Clone)]
struct WrittenExportArtifact {
    handle: String,
    path: PathBuf,
    format: ExecuteSqlExportFormat,
    row_count: usize,
    bytes: u64,
}

fn statement_timeout_override(
    value: Option<u64>,
    elapsed_ms_started: u64,
    server: &PostgresMcp,
) -> Result<Option<Duration>, CallToolResult> {
    match value {
        None => Ok(None),
        Some(0) => Err(error_result(
            server,
            "statement_timeout_ms must be greater than 0",
            elapsed_ms_started,
        )),
        Some(raw) if raw > STATEMENT_TIMEOUT_MAX_MS => Err(error_result(
            server,
            &format!(
                "statement_timeout_ms must be less than or equal to {}",
                STATEMENT_TIMEOUT_MAX_MS
            ),
            elapsed_ms_started,
        )),
        Some(raw) => Ok(Some(Duration::from_millis(raw))),
    }
}

fn sparse_success_meta(
    elapsed_ms_value: u64,
    returned_rows: usize,
    next_cursor: Option<String>,
) -> Value {
    let mut meta = Map::new();
    meta.insert("elapsed_ms".to_string(), json!(elapsed_ms_value));
    meta.insert("returned_rows".to_string(), json!(returned_rows));
    if let Some(next_cursor) = next_cursor {
        meta.insert("next_cursor".to_string(), json!(next_cursor));
    }
    Value::Object(meta)
}

fn sparse_success(
    payload: Value,
    elapsed_ms_value: u64,
    next_cursor: Option<String>,
) -> CallToolResult {
    let returned_rows = payload.as_array().map(|rows| rows.len()).unwrap_or(0);
    CallToolResult::structured(json!({
        "ok": true,
        "data": payload,
        "meta": sparse_success_meta(elapsed_ms_value, returned_rows, next_cursor),
    }))
}

fn tuple_payload(output: QueryOutput) -> StructuredTableDataSchema {
    let QueryOutput { rows, columns, .. } = output;
    let columns = columns
        .into_iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    let rows = rows
        .into_iter()
        .map(|mut row| {
            columns
                .iter()
                .map(|column| row.remove(column).unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    StructuredTableDataSchema { columns, rows }
}

fn sparse_table_success(
    payload: StructuredTableDataSchema,
    elapsed_ms_value: u64,
    next_cursor: Option<String>,
) -> CallToolResult {
    let returned_rows = payload.rows.len();
    CallToolResult::structured(json!({
        "ok": true,
        "data": payload,
        "meta": sparse_success_meta(elapsed_ms_value, returned_rows, next_cursor),
    }))
}

fn sparse_object_success(payload: Value, meta: Value) -> CallToolResult {
    CallToolResult::structured(json!({
        "ok": true,
        "data": payload,
        "meta": meta,
    }))
}

fn insert_optional_error_field(map: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        map.insert(key.to_string(), json!(value));
    }
}

async fn enriched_db_error(
    server: &PostgresMcp,
    sql: &str,
    context: &str,
    err: &DbError,
    elapsed_ms_value: u64,
) -> CallToolResult {
    let root_message = if let Some(sqlstate) = err.sqlstate() {
        format!("{} [sqlstate: {sqlstate}]", err.message())
    } else {
        err.message().to_string()
    };
    let error_message = if context.is_empty() {
        root_message
    } else {
        format!("{root_message} ({context})")
    };
    let derived_hint = derive_execute_sql_hint(server, sql, context, err).await;
    let schema_hints = derive_execute_sql_schema_hints(server, sql, err).await;
    let hint = match (err.hint(), derived_hint) {
        (Some(existing), _) => Some(existing.to_string()),
        (None, Some(derived)) => Some(derived),
        (None, None) => None,
    };
    let mut payload = Map::new();
    payload.insert("error".to_string(), json!(error_message));
    payload.insert("code".to_string(), json!(err.code()));
    payload.insert("reason".to_string(), json!(err.reason()));
    insert_optional_error_field(&mut payload, "sqlstate", err.sqlstate());
    insert_optional_error_field(&mut payload, "detail", err.detail());
    if let Some(hint) = hint {
        payload.insert("hint".to_string(), json!(hint));
    }
    insert_optional_error_field(&mut payload, "position", err.position());
    if let Some(schema_hints) = schema_hints {
        payload.insert("schema_hints".to_string(), schema_hints);
    }
    let payload = normalize_error_payload_for_role(server.startup_role, Value::Object(payload));
    CallToolResult::structured(json!({
        "ok": false,
        "error": payload,
        "meta": {"elapsed_ms": elapsed_ms_value},
    }))
}

fn read_only_sql_result(
    server: &PostgresMcp,
    sql: &str,
    elapsed_ms_value: u64,
) -> Result<(), CallToolResult> {
    classify_restricted_sql(sql).map_err(|err| {
        policy_error_result(
            server,
            "SQL_POLICY_REJECTED",
            &format!("read-safe SQL required: {}", err.message),
            "restricted_sql",
            elapsed_ms_value,
        )
    })
}

fn clip_string_value(raw: &str, max_cell_chars: usize) -> String {
    if max_cell_chars == 0 {
        return raw.to_string();
    }
    let chars = raw.chars().collect::<Vec<_>>();
    if chars.len() <= max_cell_chars {
        return raw.to_string();
    }
    if max_cell_chars == 1 {
        return "…".to_string();
    }
    let clipped = chars[..max_cell_chars - 1].iter().collect::<String>();
    format!("{clipped}…")
}

fn clip_output_in_place(output: &mut QueryOutput, max_cell_chars: Option<usize>) -> bool {
    let Some(max_cell_chars) = max_cell_chars.filter(|value| *value > 0) else {
        return false;
    };
    let mut clipped = false;
    for row in &mut output.rows {
        for value in row.values_mut() {
            if let Value::String(raw) = value {
                let clipped_value = clip_string_value(raw, max_cell_chars);
                if clipped_value != *raw {
                    *raw = clipped_value;
                    clipped = true;
                }
            }
        }
    }
    clipped
}

fn pagination_error_result(
    server: &PostgresMcp,
    message: &str,
    elapsed_ms_value: u64,
) -> CallToolResult {
    CallToolResult::structured(json!({
        "ok": false,
        "error": normalize_error_payload_for_role(
            server.startup_role,
            json!({
                "error": message,
                "code": "INVALID_CURSOR",
                "reason": "invalid_cursor",
            }),
        ),
        "meta": {"elapsed_ms": elapsed_ms_value},
    }))
}

fn admin_sql_success(
    command: String,
    mut output: QueryOutput,
    elapsed_ms_value: u64,
) -> CallToolResult {
    let returning_rows_total = output.rows.len();
    let returning_rows_truncated = returning_rows_total > ADMIN_RETURNING_ROWS_CAP;
    if returning_rows_truncated {
        output.rows.truncate(ADMIN_RETURNING_ROWS_CAP);
    }
    let rows_affected = output
        .rows_affected
        .or_else(|| (!output.rows.is_empty()).then_some(returning_rows_total as u64));

    sparse_object_success(
        json!({
            "command": command,
            "rows_affected": rows_affected,
            "returning_rows": if output.rows.is_empty() {
                Value::Null
            } else {
                Value::Array(output.rows.into_iter().map(Value::Object).collect())
            },
            "returning_rows_total": if returning_rows_total == 0 {
                Value::Null
            } else {
                json!(returning_rows_total)
            },
            "returning_rows_truncated": returning_rows_truncated,
        }),
        json!({ "elapsed_ms": elapsed_ms_value }),
    )
}

fn build_paginated_sql(sql: &str, limit: usize, offset: usize) -> String {
    let canonical = canonicalize_sql(sql);
    format!("SELECT * FROM ({canonical}) AS pgmcp_query_page LIMIT {limit} OFFSET {offset}")
}

fn artifact_uri(handle: &str) -> String {
    format!("{EXPORT_ARTIFACT_URI_PREFIX}{handle}")
}

fn export_field_delimiter(format: ExecuteSqlExportFormat) -> u8 {
    match format {
        ExecuteSqlExportFormat::Csv => b',',
        ExecuteSqlExportFormat::Tsv => b'\t',
        ExecuteSqlExportFormat::Jsonl => b'\n',
    }
}

fn export_file_extension(format: ExecuteSqlExportFormat) -> &'static str {
    match format {
        ExecuteSqlExportFormat::Csv => "csv",
        ExecuteSqlExportFormat::Tsv => "tsv",
        ExecuteSqlExportFormat::Jsonl => "jsonl",
    }
}

fn export_mime_type(format: ExecuteSqlExportFormat) -> &'static str {
    match format {
        ExecuteSqlExportFormat::Csv => "text/csv",
        ExecuteSqlExportFormat::Tsv => "text/tab-separated-values",
        ExecuteSqlExportFormat::Jsonl => "application/x-ndjson",
    }
}

fn validate_artifact_file_stem(stem: &str) -> Result<(), String> {
    if stem.is_empty() || stem.len() > 80 {
        return Err("export artifact handle length is invalid".to_string());
    }
    if stem
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        Ok(())
    } else {
        Err("export artifact handle contains an invalid character".to_string())
    }
}

fn export_artifact_path(handle: &str, format: ExecuteSqlExportFormat) -> Result<PathBuf, String> {
    validate_artifact_file_stem(handle)?;
    let extension = export_file_extension(format);
    Ok(export_artifact_temp_root().join(format!("{handle}.{extension}")))
}

fn export_artifact_temp_root() -> PathBuf {
    #[cfg(unix)]
    {
        PathBuf::from("/tmp")
    }
    #[cfg(not(unix))]
    {
        std::env::temp_dir()
    }
}

fn export_artifact_handle() -> Result<String, String> {
    let mut entropy = [0u8; 6];
    getrandom::fill(&mut entropy)
        .map_err(|err| format!("failed to generate export artifact handle: {err}"))?;
    let suffix = entropy
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    Ok(format!("art_ex_{timestamp_ms:016x}_{suffix}"))
}

fn create_export_artifact_file(
    format: ExecuteSqlExportFormat,
) -> Result<(String, PathBuf, File), String> {
    let mut attempts = 0usize;
    loop {
        let handle = export_artifact_handle()?;
        let path = export_artifact_path(&handle, format)?;
        let mut file_options = OpenOptions::new();
        file_options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            file_options.mode(0o600);
        }
        match file_options.open(&path) {
            Ok(file) => return Ok((handle, path, file)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists && attempts < 32 => {
                attempts += 1;
                continue;
            }
            Err(err) => return Err(format!("failed to create export artifact: {err}")),
        }
    }
}

fn render_export_cell(value: Option<&Value>) -> String {
    match value.unwrap_or(&Value::Null) {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        other => serde_json::to_string(other).unwrap_or_else(|_| String::new()),
    }
}

fn write_delimited_row(writer: &mut File, values: &[String], delimiter: u8) -> std::io::Result<()> {
    let delimiter = delimiter as char;
    let mut first = true;
    for value in values {
        if !first {
            write!(writer, "{delimiter}")?;
        }
        first = false;
        // RFC 4180-style CSV escaping: embedded double quotes inside a field
        // must be escaped by doubling them (`"` -> `""`) before optional quoting.
        let escaped = value.replace('"', "\"\"");
        if escaped.contains(delimiter)
            || escaped.contains('\n')
            || escaped.contains('\r')
            || escaped.contains('"')
        {
            write!(writer, "\"{escaped}\"")?;
        } else {
            write!(writer, "{escaped}")?;
        }
    }
    writeln!(writer)?;
    Ok(())
}

fn write_export_artifact(
    output: &QueryOutput,
    format: ExecuteSqlExportFormat,
) -> Result<WrittenExportArtifact, String> {
    let (handle, path, mut file) = create_export_artifact_file(format)?;

    match format {
        ExecuteSqlExportFormat::Jsonl => {
            for row in &output.rows {
                let line = serde_json::to_string(row)
                    .map_err(|err| format!("failed to encode export row: {err}"))?;
                writeln!(file, "{line}")
                    .map_err(|err| format!("failed to write export artifact: {err}"))?;
            }
        }
        ExecuteSqlExportFormat::Csv | ExecuteSqlExportFormat::Tsv => {
            let delimiter = export_field_delimiter(format);
            let headers = output
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            write_delimited_row(&mut file, &headers, delimiter)
                .map_err(|err| format!("failed to write export header: {err}"))?;
            for row in &output.rows {
                let values = output
                    .columns
                    .iter()
                    .map(|column| render_export_cell(row.get(&column.name)))
                    .collect::<Vec<_>>();
                write_delimited_row(&mut file, &values, delimiter)
                    .map_err(|err| format!("failed to write export row: {err}"))?;
            }
        }
    }

    file.flush()
        .map_err(|err| format!("failed to flush export artifact: {err}"))?;
    let bytes = file
        .metadata()
        .map_err(|err| format!("failed to stat export artifact: {err}"))?
        .len();
    Ok(WrittenExportArtifact {
        handle,
        path,
        format,
        row_count: output.rows.len(),
        bytes,
    })
}

#[rmcp::tool_router(router = tool_router_postgres_query_surface, vis = "pub")]
impl PostgresMcp {
    async fn run_read_query(
        &self,
        tool_kind: ReadToolKind,
        sql: &str,
        params: &[Value],
        session_id: Option<&str>,
        profile: Option<ReadQueryProfile>,
        cursor: Option<String>,
        max_rows: Option<usize>,
        max_cell_chars: Option<usize>,
        statement_timeout_ms: Option<u64>,
        preflight_check: bool,
    ) -> Result<ReadQueryResult, CallToolResult> {
        let started = Instant::now();
        validate_sql_size(sql, "sql")
            .map_err(|err| error_result(self, &err, elapsed_ms(started)))?;
        read_only_sql_result(self, sql, elapsed_ms(started))?;
        let statement_timeout_override =
            statement_timeout_override(statement_timeout_ms, elapsed_ms(started), self)?;
        let params = params.to_vec();
        if preflight_check {
            let (preflight_result, _) = match describe_sql_with_optional_session(
                self,
                session_id,
                sql,
                &params,
                statement_timeout_override,
                elapsed_ms(started),
            )
            .await
            {
                Ok(result) => result,
                Err(result) => return Err(result),
            };
            match preflight_result {
                Ok(_) => {}
                Err(err) => {
                    return Err(enriched_db_error(
                        self,
                        sql,
                        "Error describing query",
                        &err,
                        elapsed_ms(started),
                    )
                    .await);
                }
            }
        }

        let page_size = resolve_execute_sql_page_size(self.response_page_size, max_rows);
        let query_hash = match tool_kind {
            ReadToolKind::Tuple => {
                response_page_hash_for_tuple_context(sql, &params, session_id, profile)
            }
            ReadToolKind::Query | ReadToolKind::Render => {
                response_page_hash_for_read_context(sql, &params, session_id, profile)
            }
        };
        let should_paginate = should_paginate_execute_sql(sql);
        let offset = if let Some(cursor) = cursor.as_deref() {
            if !should_paginate {
                return Err(pagination_error_result(
                    self,
                    "pagination cursor is only supported for select-like queries",
                    elapsed_ms(started),
                ));
            }
            match decode_pagination_cursor(
                self,
                PaginationCursorScope::ExecuteSql,
                &query_hash,
                cursor,
            ) {
                Ok(cursor) => cursor.offset,
                Err(PaginationCursorDecodeError::Invalid) => {
                    return Err(pagination_error_result(
                        self,
                        "invalid pagination cursor",
                        elapsed_ms(started),
                    ));
                }
                Err(PaginationCursorDecodeError::Expired) => {
                    return Err(pagination_error_result(
                        self,
                        "pagination cursor expired",
                        elapsed_ms(started),
                    ));
                }
                Err(PaginationCursorDecodeError::QueryMismatch) => {
                    return Err(pagination_error_result(
                        self,
                        "pagination cursor does not match the supplied query",
                        elapsed_ms(started),
                    ));
                }
            }
        } else {
            0
        };

        let effective_sql = if should_paginate {
            build_paginated_sql(sql, page_size + 1, offset)
        } else {
            canonicalize_sql(sql)
        };
        let (execution_result, _) = match execute_sql_with_optional_session(
            self,
            session_id,
            &effective_sql,
            &params,
            statement_timeout_override,
            elapsed_ms(started),
        )
        .await
        {
            Ok(result) => result,
            Err(result) => return Err(result),
        };
        let mut output = match execution_result {
            Ok(output) => output,
            Err(err) => {
                return Err(enriched_db_error(
                    self,
                    sql,
                    "Error executing query",
                    &err,
                    elapsed_ms(started),
                )
                .await);
            }
        };
        clip_output_in_place(&mut output, max_cell_chars);
        let next_cursor = if should_paginate && output.rows.len() > page_size {
            output.rows.truncate(page_size);
            Some(encode_pagination_cursor(
                self,
                PaginationCursorScope::ExecuteSql,
                &query_hash,
                offset + page_size,
            ))
        } else {
            None
        };
        Ok(ReadQueryResult {
            output,
            elapsed_ms: elapsed_ms(started),
            next_cursor,
        })
    }

    async fn run_export_query(
        &self,
        args: &ExportJobStartArgs,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = Instant::now();
        if let Err(err) = validate_sql_size(&args.sql, "sql") {
            return Ok(error_result(self, &err, elapsed_ms(started)));
        }
        if let Err(result) = read_only_sql_result(self, &args.sql, elapsed_ms(started)) {
            return Ok(result);
        }

        let params = args.params.clone().unwrap_or_default();
        let statement_timeout_override = match statement_timeout_override(
            args.statement_timeout_ms,
            elapsed_ms(started),
            self,
        ) {
            Ok(timeout) => timeout,
            Err(result) => return Ok(result),
        };

        if args.preflight_check.unwrap_or(true) {
            match self
                .db
                .describe_user_sql_with_params_and_statement_timeout(
                    &args.sql,
                    &params,
                    statement_timeout_override,
                )
                .await
            {
                Ok(_) => {}
                Err(err) => {
                    return Ok(enriched_db_error(
                        self,
                        &args.sql,
                        "Error describing export query",
                        &err,
                        elapsed_ms(started),
                    )
                    .await);
                }
            }
        }

        let output = match self
            .db
            .execute_user_sql_with_params_and_statement_timeout(
                &args.sql,
                &params,
                statement_timeout_override,
            )
            .await
        {
            Ok(output) => output,
            Err(err) => {
                return Ok(enriched_db_error(
                    self,
                    &args.sql,
                    "Error exporting query",
                    &err,
                    elapsed_ms(started),
                )
                .await);
            }
        };

        let artifact = match write_export_artifact(
            &output,
            args.format.unwrap_or(ExecuteSqlExportFormat::Tsv),
        ) {
            Ok(artifact) => artifact,
            Err(err) => {
                return Ok(CallToolResult::structured(json!({
                    "ok": false,
                    "error": normalize_error_payload_for_role(
                        self.startup_role,
                        json!({
                            "error": err,
                            "code": "EXPORT_FAILED",
                            "reason": "export_failed",
                        }),
                    ),
                    "meta": {"elapsed_ms": elapsed_ms(started)},
                })));
            }
        };
        let handle = artifact.handle.clone();

        self.register_export_artifact(ExportArtifactRecord {
            handle: handle.clone(),
            uri: artifact_uri(&handle),
            format: artifact.format.as_str().to_string(),
            mime_type: export_mime_type(artifact.format).to_string(),
            bytes: artifact.bytes,
            row_count: artifact.row_count,
            path: artifact.path,
        });

        Ok(sparse_object_success(
            json!({
                "artifact_handle": handle,
                "artifact_uri": artifact_uri(&handle),
                "format": artifact.format.as_str(),
                "row_count": artifact.row_count,
                "bytes": artifact.bytes,
            }),
            json!({ "elapsed_ms": elapsed_ms(started) }),
        ))
    }

    #[tool(
        name = "query_sql",
        description = "Run a structured read query and return row objects",
        execution(task_support = "optional"),
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredRowsToolResultSchema>()
    )]
    async fn query_sql(
        &self,
        Parameters(args): Parameters<QuerySqlArgsVNext>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = Instant::now();
        let session_id = args
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if args
            .session_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Ok(error_result(
                self,
                "session_id must not be empty",
                elapsed_ms(started),
            ));
        }
        let settings = resolve_read_query_settings(
            ReadToolKind::Query,
            args.profile,
            args.max_rows,
            args.max_cell_chars,
            args.preflight_check,
            self.response_page_size,
        );
        let params = args.params.unwrap_or_default();
        let result = match self
            .run_read_query(
                ReadToolKind::Query,
                &args.sql,
                &params,
                session_id,
                args.profile,
                args.cursor,
                settings.max_rows,
                settings.max_cell_chars,
                args.statement_timeout_ms,
                settings.preflight_check,
            )
            .await
        {
            Ok(result) => result,
            Err(result) => return Ok(result),
        };
        let payload = Value::Array(
            result
                .output
                .rows
                .into_iter()
                .map(Value::Object)
                .collect::<Vec<_>>(),
        );
        Ok(sparse_success(
            payload,
            result.elapsed_ms,
            result.next_cursor,
        ))
    }

    #[tool(
        name = "query_tuples",
        description = "Run a structured read query and return columns plus tuple rows",
        execution(task_support = "optional"),
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredTableToolResultSchema>()
    )]
    async fn query_tuples(
        &self,
        Parameters(args): Parameters<QueryTuplesArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = Instant::now();
        let session_id = args
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if args
            .session_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Ok(error_result(
                self,
                "session_id must not be empty",
                elapsed_ms(started),
            ));
        }
        let settings = resolve_read_query_settings(
            ReadToolKind::Tuple,
            args.profile,
            args.max_rows,
            args.max_cell_chars,
            args.preflight_check,
            self.response_page_size,
        );
        let params = args.params.unwrap_or_default();
        let result = match self
            .run_read_query(
                ReadToolKind::Tuple,
                &args.sql,
                &params,
                session_id,
                args.profile,
                args.cursor,
                settings.max_rows,
                settings.max_cell_chars,
                args.statement_timeout_ms,
                settings.preflight_check,
            )
            .await
        {
            Ok(result) => result,
            Err(result) => return Ok(result),
        };
        Ok(sparse_table_success(
            tuple_payload(result.output),
            result.elapsed_ms,
            result.next_cursor,
        ))
    }

    #[tool(
        name = "export_sql",
        description = "Run a read query, export the full result, and return an artifact handle",
        execution(task_support = "optional"),
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn export_sql(
        &self,
        Parameters(args): Parameters<ExportJobStartArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        self.run_export_query(&args).await
    }

    #[tool(
        name = "render_sql",
        description = "Run a read query and return markdown for agent/operator inspection",
        execution(task_support = "optional")
    )]
    async fn render_sql(
        &self,
        Parameters(args): Parameters<RenderSqlArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = Instant::now();
        let session_id = args
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if args
            .session_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Ok(error_result(
                self,
                "session_id must not be empty",
                elapsed_ms(started),
            ));
        }
        let settings = resolve_read_query_settings(
            ReadToolKind::Render,
            args.profile,
            args.max_rows,
            args.max_cell_chars,
            args.preflight_check,
            self.response_page_size,
        );
        let params = args.params.unwrap_or_default();
        let result = match self
            .run_read_query(
                ReadToolKind::Render,
                &args.sql,
                &params,
                session_id,
                args.profile,
                args.cursor,
                settings.max_rows,
                settings.max_cell_chars,
                args.statement_timeout_ms,
                settings.preflight_check,
            )
            .await
        {
            Ok(result) => result,
            Err(result) => return Ok(result),
        };

        let mut lines = vec![format!(
            "Returned {} row{} in {} ms{}",
            result.output.rows.len(),
            if result.output.rows.len() == 1 {
                ""
            } else {
                "s"
            },
            result.elapsed_ms,
            if result.next_cursor.is_some() {
                "; more rows available"
            } else {
                ""
            }
        )];
        if let Some(next_cursor) = &result.next_cursor {
            lines.push(format!("Next cursor: `{next_cursor}`"));
        }
        let markdown = if result.output.rows.is_empty() {
            String::new()
        } else {
            render_query_output_markdown(&result.output, ResponseOutputMode::Rows)
        };
        if !markdown.is_empty() {
            lines.push(String::new());
            lines.push(markdown);
        }
        let mut tool_result = tool_success(self, Value::Null, result.elapsed_ms);
        tool_result.structured_content = None;
        tool_result.content = vec![Content::text(lines.join("\n"))];
        Ok(tool_result)
    }

    #[tool(
        name = "describe_sql",
        description = "Describe query result columns without executing the query body",
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn describe_sql(
        &self,
        Parameters(args): Parameters<DescribeSqlArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = Instant::now();
        if let Err(err) = validate_sql_size(&args.sql, "sql") {
            return Ok(error_result(self, &err, elapsed_ms(started)));
        }
        if let Err(result) = read_only_sql_result(self, &args.sql, elapsed_ms(started)) {
            return Ok(result);
        }
        let statement_timeout_override = match statement_timeout_override(
            args.statement_timeout_ms,
            elapsed_ms(started),
            self,
        ) {
            Ok(timeout) => timeout,
            Err(result) => return Ok(result),
        };
        let params = args.params.unwrap_or_default();
        let columns = match self
            .db
            .describe_user_sql_with_params_and_statement_timeout(
                &args.sql,
                &params,
                statement_timeout_override,
            )
            .await
        {
            Ok(columns) => columns,
            Err(err) => {
                return Ok(enriched_db_error(
                    self,
                    &args.sql,
                    "Error describing query",
                    &err,
                    elapsed_ms(started),
                )
                .await);
            }
        };
        Ok(sparse_object_success(
            json!({ "columns": columns }),
            json!({ "elapsed_ms": elapsed_ms(started) }),
        ))
    }

    #[tool(
        name = "admin_sql",
        description = "Run one mutating SQL statement and return bounded returning rows",
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn admin_sql(
        &self,
        Parameters(args): Parameters<AdminSqlArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = Instant::now();
        if !self.enable_admin_sql {
            return Ok(policy_error_result(
                self,
                "ADMIN_SQL_DISABLED",
                "admin_sql is disabled; restart with --enable-admin-sql or POSTGRES_MCP_ENABLE_ADMIN_SQL=1",
                "admin_sql_disabled",
                elapsed_ms(started),
            ));
        }
        if let Err(err) = validate_sql_size(&args.sql, "sql") {
            return Ok(error_result(self, &err, elapsed_ms(started)));
        }
        let statement_timeout_override = match statement_timeout_override(
            args.statement_timeout_ms,
            elapsed_ms(started),
            self,
        ) {
            Ok(timeout) => timeout,
            Err(result) => return Ok(result),
        };
        let canonical = canonicalize_sql(&args.sql);
        if super::query::contains_top_level_statement_delimiter(&canonical) {
            return Ok(error_result(
                self,
                "admin_sql requires exactly one top-level statement",
                elapsed_ms(started),
            ));
        }
        let params = args.params.unwrap_or_default();
        let output = match self
            .db
            .execute_user_sql_with_params_and_statement_timeout(
                &canonical,
                &params,
                statement_timeout_override,
            )
            .await
        {
            Ok(output) => output,
            Err(err) => {
                return Ok(enriched_db_error(
                    self,
                    &args.sql,
                    "Error executing admin SQL",
                    &err,
                    elapsed_ms(started),
                )
                .await);
            }
        };
        let command = leading_statement_keyword(&canonical)
            .unwrap_or_else(|| "unknown".to_string())
            .to_ascii_uppercase();
        Ok(admin_sql_success(command, output, elapsed_ms(started)))
    }

    #[tool(
        name = "query_job_start",
        description = "Launch a long-running structured read query job (deprecated: use query_sql with task augmentation)",
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn query_job_start(
        &self,
        Parameters(args): Parameters<QuerySqlArgsVNext>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = Instant::now();
        if let Err(err) = validate_sql_size(&args.sql, "sql") {
            return Ok(error_result(self, &err, elapsed_ms(started)));
        }
        if let Err(result) = read_only_sql_result(self, &args.sql, elapsed_ms(started)) {
            return Ok(result);
        }
        let params = args.params.clone().unwrap_or_default();
        let query_hash = response_page_hash_for_read_context(
            &args.sql,
            &params,
            args.session_id.as_deref(),
            args.profile,
        );
        let job = match self.query_jobs.create_with_kind("query", &query_hash) {
            Ok(job) => job,
            Err(err) => {
                return Ok(CallToolResult::structured(json!({
                    "ok": false,
                    "error": normalize_error_payload_for_role(
                        self.startup_role,
                        json!({
                            "error": err.message(),
                            "code": err.code(),
                            "reason": err.reason(),
                        }),
                    ),
                    "meta": {"elapsed_ms": elapsed_ms(started)},
                })));
            }
        };
        let server = self.clone();
        let job_handle = job.clone();
        let args_clone = args.clone();
        let task = tokio::spawn(async move {
            job_handle.mark_running();
            let result = match server.query_sql(Parameters(args_clone)).await {
                Ok(result) => result,
                Err(err) => {
                    let error = err.to_string();
                    CallToolResult::structured(json!({
                        "ok": false,
                        "error": {
                            "error": error,
                            "code": "QUERY_JOB_INTERNAL",
                            "reason": "query_job_internal",
                        },
                        "meta": {"elapsed_ms": 0}
                    }))
                }
            };
            let response = result.structured_content.clone().unwrap_or_else(|| {
                json!({
                    "ok": false,
                    "error": {
                        "error": "query_sql did not return a structured payload",
                        "code": "QUERY_JOB_INTERNAL",
                        "reason": "query_job_internal",
                    },
                    "meta": {"elapsed_ms": 0}
                })
            });
            let state = if response.get("ok").and_then(Value::as_bool) == Some(true) {
                QueryJobState::Succeeded
            } else {
                QueryJobState::Failed
            };
            job_handle.complete_structured(state, response, result);
        });
        job.register_abort_handle(task.abort_handle());
        let snapshot = job.snapshot();
        Ok(sparse_object_success(
            json!({
                "job_id": snapshot.job_id,
                "kind": "query",
                "state": snapshot.state.as_str(),
                "suggested_wait_ms": 1000,
            }),
            json!({ "elapsed_ms": elapsed_ms(started) }),
        ))
    }

    #[tool(
        name = "export_job_start",
        description = "Launch a full-result export job and return an artifact handle when complete (deprecated: use export_sql with task augmentation)",
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn export_job_start(
        &self,
        Parameters(args): Parameters<ExportJobStartArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = Instant::now();
        if let Err(err) = validate_sql_size(&args.sql, "sql") {
            return Ok(error_result(self, &err, elapsed_ms(started)));
        }
        if let Err(result) = read_only_sql_result(self, &args.sql, elapsed_ms(started)) {
            return Ok(result);
        }
        let params = args.params.clone().unwrap_or_default();
        let query_hash = response_page_hash_for_read_context(&args.sql, &params, None, None);
        let job = match self.query_jobs.create_with_kind("export", &query_hash) {
            Ok(job) => job,
            Err(err) => {
                return Ok(CallToolResult::structured(json!({
                    "ok": false,
                    "error": normalize_error_payload_for_role(
                        self.startup_role,
                        json!({
                            "error": err.message(),
                            "code": err.code(),
                            "reason": err.reason(),
                        }),
                    ),
                    "meta": {"elapsed_ms": elapsed_ms(started)},
                })));
            }
        };
        let server = self.clone();
        let job_handle = job.clone();
        let args_clone = args.clone();
        let task = tokio::spawn(async move {
            job_handle.mark_running();
            let result = match server.run_export_query(&args_clone).await {
                Ok(result) => result,
                Err(err) => {
                    let error = err.to_string();
                    CallToolResult::structured(json!({
                        "ok": false,
                        "error": {
                            "error": error,
                            "code": "QUERY_JOB_INTERNAL",
                            "reason": "query_job_internal",
                        },
                        "meta": {"elapsed_ms": 0}
                    }))
                }
            };
            let fallback_response = json!({
                "ok": false,
                "error": {
                    "error": "export_sql did not return a structured payload",
                    "code": "QUERY_JOB_INTERNAL",
                    "reason": "query_job_internal"
                },
                "meta": {"elapsed_ms": 0}
            });
            let response = result
                .structured_content
                .clone()
                .unwrap_or_else(|| fallback_response.clone());
            let state = if response.get("ok").and_then(Value::as_bool) == Some(true) {
                QueryJobState::Succeeded
            } else {
                QueryJobState::Failed
            };
            job_handle.complete_structured(state, response, result);
        });
        job.register_abort_handle(task.abort_handle());
        let snapshot = job.snapshot();
        Ok(sparse_object_success(
            json!({
                "job_id": snapshot.job_id,
                "kind": "export",
                "state": snapshot.state.as_str(),
                "suggested_wait_ms": 1000,
            }),
            json!({ "elapsed_ms": elapsed_ms(started) }),
        ))
    }

    #[tool(
        name = "job_status",
        description = "Read generic job status and optionally wait or include the terminal result (deprecated: use MCP tasks/get and tasks/result)",
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn job_status(
        &self,
        Parameters(args): Parameters<JobStatusArgsVNext>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = Instant::now();
        let Some(job) = self.query_jobs.get(&args.job_id) else {
            return Ok(CallToolResult::structured(json!({
                "ok": false,
                "error": normalize_error_payload_for_role(
                    self.startup_role,
                    json!({
                        "error": "job not found",
                        "code": "QUERY_JOB_NOT_FOUND",
                        "reason": "query_job_not_found",
                    }),
                ),
                "meta": {"elapsed_ms": elapsed_ms(started)},
            })));
        };

        let wait_for = if args.wait_until_terminal {
            None
        } else {
            args.wait_ms.map(Duration::from_millis)
        };

        if args.wait_until_terminal {
            loop {
                let snapshot = job.snapshot();
                if snapshot.state.is_terminal() {
                    break;
                }
                let revision = job.revision();
                let _ = job.wait_for_update_since(revision, None).await;
            }
        } else if let Some(wait_for) = wait_for {
            let revision = job.revision();
            let _ = job.wait_for_update_since(revision, Some(wait_for)).await;
        }

        let snapshot = job.snapshot();
        let mut data = Map::new();
        data.insert("job_id".to_string(), json!(snapshot.job_id));
        data.insert("kind".to_string(), json!(snapshot.kind));
        data.insert("state".to_string(), json!(snapshot.state.as_str()));
        data.insert("terminal".to_string(), json!(snapshot.state.is_terminal()));
        data.insert(
            "created_at_unix_ms".to_string(),
            json!(snapshot.created_at_unix_ms),
        );
        if let Some(started_at) = snapshot.started_at_unix_ms {
            data.insert("started_at_unix_ms".to_string(), json!(started_at));
        }
        if let Some(finished_at) = snapshot.finished_at_unix_ms {
            data.insert("finished_at_unix_ms".to_string(), json!(finished_at));
        }
        data.insert("suggested_wait_ms".to_string(), json!(1000));
        if snapshot.state.is_terminal()
            && snapshot.kind == "export"
            && let Some(response) = snapshot.response.as_ref()
            && response.get("ok").and_then(Value::as_bool) == Some(true)
            && let Some(export_data) = response.get("data")
        {
            data.insert("artifact".to_string(), export_data.clone());
        }
        if args.include_result
            && snapshot.state.is_terminal()
            && snapshot.kind == "query"
            && let Some(response) = snapshot.response.as_ref()
        {
            data.insert("result".to_string(), response.clone());
        }
        Ok(sparse_object_success(
            Value::Object(data),
            json!({ "elapsed_ms": elapsed_ms(started) }),
        ))
    }

    #[tool(
        name = "job_cancel",
        description = "Cancel a running query or export job (deprecated: use MCP tasks/cancel)",
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn job_cancel(
        &self,
        Parameters(args): Parameters<JobCancelArgsVNext>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = Instant::now();
        let Some(job) = self.query_jobs.get(&args.job_id) else {
            return Ok(CallToolResult::structured(json!({
                "ok": false,
                "error": normalize_error_payload_for_role(
                    self.startup_role,
                    json!({
                        "error": "job not found",
                        "code": "QUERY_JOB_NOT_FOUND",
                        "reason": "query_job_not_found",
                    }),
                ),
                "meta": {"elapsed_ms": elapsed_ms(started)},
            })));
        };
        let snapshot = job.cancel(self.startup_role);
        Ok(sparse_object_success(
            json!({
                "job_id": snapshot.job_id,
                "kind": snapshot.kind,
                "state": snapshot.state.as_str(),
                "terminal": snapshot.state.is_terminal(),
                "suggested_wait_ms": 0,
            }),
            json!({ "elapsed_ms": elapsed_ms(started) }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ADMIN_RETURNING_ROWS_CAP, QueryTuplesArgs, ReadToolKind, admin_sql_success,
        build_paginated_sql, enriched_db_error, export_artifact_handle, export_artifact_path,
        export_artifact_temp_root, resolve_read_query_settings,
        response_page_hash_for_read_context, response_page_hash_for_session,
        response_page_hash_for_tuple_context, tuple_payload, validate_artifact_file_stem,
    };
    use crate::config::{
        AccessMode, AdvisorExternalConfig, ResponseAutoTabularMode, ResponseMode,
        ResponseOutputMode,
    };
    use crate::db::{DbEngine, DbError, QueryColumn, QueryOutput};
    use crate::server::PostgresMcp;
    use crate::tools::{ExecuteSqlExportFormat, ReadQueryProfile};
    use rmcp::handler::server::wrapper::Parameters;
    use serde_json::{Map, Value, json};
    use std::env;
    use std::sync::Arc;
    use std::time::Duration;

    fn test_server() -> PostgresMcp {
        PostgresMcp::with_runtime_options(
            Arc::new(DbEngine::new(
                None,
                AccessMode::Unrestricted,
                false,
                None,
                None,
                None,
            )),
            ResponseMode::V2,
            ResponseOutputMode::Rows,
            ResponseAutoTabularMode::Rows,
            100,
            AdvisorExternalConfig::disabled(),
        )
    }

    fn live_db_server_from_env() -> Option<PostgresMcp> {
        let database_uri = env::var("DATABASE_URI").ok()?;
        if database_uri.trim().is_empty() {
            return None;
        }
        let mut server = PostgresMcp::with_response_contract(
            Arc::new(DbEngine::new(
                Some(database_uri),
                AccessMode::Unrestricted,
                true,
                Some(Duration::from_secs(15)),
                Some(Duration::from_secs(10)),
                Some(Duration::from_secs(2)),
            )),
            ResponseMode::V2,
            ResponseOutputMode::Rows,
            ResponseAutoTabularMode::Rows,
            200,
        );
        server.startup_role = crate::config::StartupRole::Migrator;
        Some(server)
    }

    #[test]
    fn build_paginated_sql_strips_trailing_semicolon_before_wrapping() {
        let sql = build_paginated_sql("SELECT 1;", 26, 0);
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT 1) AS pgmcp_query_page LIMIT 26 OFFSET 0"
        );
    }

    #[test]
    fn export_artifact_handles_are_safe_file_stems() {
        let handle = export_artifact_handle().expect("handle");

        assert!(validate_artifact_file_stem(&handle).is_ok());
        assert!(handle.starts_with("art_ex_"));
        assert_eq!(handle.len(), "art_ex_".len() + 16 + 1 + 12);
        assert!(validate_artifact_file_stem("../artifact").is_err());
        assert!(validate_artifact_file_stem("artifact/name").is_err());
        assert!(validate_artifact_file_stem("").is_err());
    }

    #[test]
    fn export_artifact_paths_use_validated_temp_file_names() {
        let path = export_artifact_path(
            "art_ex_0000000000000000_abcdef123456",
            ExecuteSqlExportFormat::Csv,
        )
        .expect("path");

        assert_eq!(
            path.file_name().and_then(|name| name.to_str()),
            Some("art_ex_0000000000000000_abcdef123456.csv")
        );
        assert_eq!(path.parent(), Some(export_artifact_temp_root().as_path()));
    }

    #[test]
    fn admin_sql_success_truncates_returning_rows_without_reporting_failure() {
        let rows = (0..(ADMIN_RETURNING_ROWS_CAP + 1))
            .map(|idx| {
                let mut row = Map::new();
                row.insert("provider".to_string(), json!(format!("p{idx}")));
                row
            })
            .collect::<Vec<_>>();
        let result = admin_sql_success(
            "UPDATE".to_string(),
            QueryOutput {
                rows,
                columns: Vec::new(),
                rows_affected: None,
            },
            17,
        );
        let payload = result
            .structured_content
            .as_ref()
            .expect("admin_sql success should remain structured");
        assert_eq!(payload.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            payload
                .pointer("/data/rows_affected")
                .and_then(Value::as_u64),
            Some((ADMIN_RETURNING_ROWS_CAP + 1) as u64)
        );
        assert_eq!(
            payload
                .pointer("/data/returning_rows_total")
                .and_then(Value::as_u64),
            Some((ADMIN_RETURNING_ROWS_CAP + 1) as u64)
        );
        assert_eq!(
            payload
                .pointer("/data/returning_rows_truncated")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            payload
                .pointer("/data/returning_rows")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(ADMIN_RETURNING_ROWS_CAP)
        );
    }

    #[test]
    fn admin_sql_success_preserves_rows_affected_without_returning_rows() {
        let result = admin_sql_success(
            "UPDATE".to_string(),
            QueryOutput {
                rows: Vec::new(),
                columns: Vec::new(),
                rows_affected: Some(42),
            },
            11,
        );
        let payload = result
            .structured_content
            .as_ref()
            .expect("admin_sql success should remain structured");
        assert_eq!(
            payload
                .pointer("/data/rows_affected")
                .and_then(Value::as_u64),
            Some(42)
        );
        assert!(payload.pointer("/data/returning_rows").is_some());
        assert_eq!(
            payload
                .pointer("/data/returning_rows")
                .and_then(Value::as_array),
            None
        );
    }

    #[test]
    fn resolve_read_query_settings_query_compact_applies_agent_defaults() {
        let resolved = resolve_read_query_settings(
            ReadToolKind::Query,
            Some(ReadQueryProfile::Compact),
            None,
            None,
            None,
            250,
        );
        assert_eq!(resolved.max_rows, Some(100));
        assert_eq!(resolved.max_cell_chars, Some(256));
        assert!(!resolved.preflight_check);
    }

    #[test]
    fn resolve_read_query_settings_render_defaults_preserve_existing_behavior() {
        let resolved =
            resolve_read_query_settings(ReadToolKind::Render, None, None, None, None, 100);
        assert_eq!(resolved.max_rows, Some(25));
        assert_eq!(resolved.max_cell_chars, Some(256));
        assert!(resolved.preflight_check);
    }

    #[test]
    fn response_page_hash_for_session_changes_when_session_id_changes() {
        let params = vec![json!(7)];
        let a = response_page_hash_for_session(
            "SELECT * FROM foo WHERE id = $1",
            &params,
            Some("ps_a"),
        );
        let b = response_page_hash_for_session(
            "SELECT * FROM foo WHERE id = $1",
            &params,
            Some("ps_b"),
        );
        let base = response_page_hash_for_session("SELECT * FROM foo WHERE id = $1", &params, None);
        assert_ne!(a, b);
        assert_ne!(a, base);
        assert_ne!(b, base);
    }

    #[test]
    fn response_page_hash_for_read_context_changes_when_profile_changes() {
        let params = vec![json!(7)];
        let compact = response_page_hash_for_read_context(
            "SELECT * FROM foo WHERE id = $1",
            &params,
            Some("ps_a"),
            Some(ReadQueryProfile::Compact),
        );
        let inspect = response_page_hash_for_read_context(
            "SELECT * FROM foo WHERE id = $1",
            &params,
            Some("ps_a"),
            Some(ReadQueryProfile::Inspect),
        );
        let session_only = response_page_hash_for_read_context(
            "SELECT * FROM foo WHERE id = $1",
            &params,
            Some("ps_a"),
            None,
        );
        assert_ne!(compact, inspect);
        assert_ne!(compact, session_only);
        assert_ne!(inspect, session_only);
    }

    #[test]
    fn response_page_hash_for_tuple_context_differs_from_read_context() {
        let params = vec![json!(7)];
        let read_hash = response_page_hash_for_read_context(
            "SELECT id, email FROM foo WHERE id = $1",
            &params,
            Some("ps_a"),
            Some(ReadQueryProfile::Compact),
        );
        let tuple_hash = response_page_hash_for_tuple_context(
            "SELECT id, email FROM foo WHERE id = $1",
            &params,
            Some("ps_a"),
            Some(ReadQueryProfile::Compact),
        );
        assert_ne!(tuple_hash, read_hash);
    }

    #[test]
    fn tuple_payload_preserves_column_order() {
        let mut first_row = Map::new();
        first_row.insert("email".to_string(), json!("ada@example.com"));
        first_row.insert("id".to_string(), json!(101));
        first_row.insert("is_active".to_string(), json!(true));

        let mut second_row = Map::new();
        second_row.insert("email".to_string(), Value::Null);
        second_row.insert("id".to_string(), json!(102));
        second_row.insert("is_active".to_string(), json!(false));

        let payload = tuple_payload(QueryOutput {
            rows: vec![first_row, second_row],
            columns: vec![
                QueryColumn {
                    name: "id".to_string(),
                    pg_type: "int8".to_string(),
                    nullable: Some(false),
                },
                QueryColumn {
                    name: "email".to_string(),
                    pg_type: "text".to_string(),
                    nullable: Some(true),
                },
                QueryColumn {
                    name: "is_active".to_string(),
                    pg_type: "bool".to_string(),
                    nullable: Some(false),
                },
            ],
            rows_affected: None,
        });

        assert_eq!(payload.columns, vec!["id", "email", "is_active"]);
        assert_eq!(
            payload.rows,
            vec![
                vec![json!(101), json!("ada@example.com"), json!(true)],
                vec![json!(102), Value::Null, json!(false)],
            ]
        );
    }

    #[tokio::test]
    async fn query_tuples_paginates_in_pinned_session_across_pages() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping query_tuples_paginates_in_pinned_session_across_pages (DATABASE_URI not set)"
            );
            return;
        };

        let session_snapshot = server
            .open_pinned_session(Duration::from_secs(15))
            .await
            .expect("open_pinned_session should return a snapshot");
        let session_id = session_snapshot.session_id.clone();

        let session = server
            .pinned_session(&session_id)
            .expect("open session should be retrievable");
        session
            .execute_sql_with_statement_timeout(
                "CREATE TEMP TABLE postgres_mcp_query_tuples_repro_20260320 (id int, label text);",
                None,
            )
            .await
            .expect("temp table create should succeed");
        session
            .execute_sql_with_statement_timeout(
                "INSERT INTO postgres_mcp_query_tuples_repro_20260320 (id, label) VALUES (1, 'one'), (2, 'two'), (3, 'three');",
                None,
            )
            .await
            .expect("temp table insert should succeed");

        let first_page = server
            .query_tuples(Parameters(QueryTuplesArgs {
                sql: "SELECT id, label FROM postgres_mcp_query_tuples_repro_20260320 ORDER BY id"
                    .to_string(),
                params: None,
                cursor: None,
                max_rows: Some(2),
                max_cell_chars: None,
                statement_timeout_ms: None,
                preflight_check: Some(true),
                session_id: Some(session_id.clone()),
                profile: Some(ReadQueryProfile::Inspect),
            }))
            .await
            .expect("first page query should return a payload")
            .structured_content
            .expect("first page query should return structured content");

        assert_eq!(
            first_page.pointer("/ok").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            first_page
                .pointer("/data/columns")
                .and_then(Value::as_array)
                .cloned(),
            Some(vec![json!("id"), json!("label")])
        );
        assert_eq!(
            first_page
                .pointer("/data/rows")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2)
        );
        assert_eq!(
            first_page.pointer("/data/rows/0").cloned(),
            Some(json!([1, "one"]))
        );
        assert_eq!(
            first_page.pointer("/data/rows/1").cloned(),
            Some(json!([2, "two"]))
        );
        let next_cursor = first_page
            .pointer("/meta/next_cursor")
            .and_then(Value::as_str)
            .expect("first page should include a next cursor")
            .to_string();

        let second_page = server
            .query_tuples(Parameters(QueryTuplesArgs {
                sql: "SELECT id, label FROM postgres_mcp_query_tuples_repro_20260320 ORDER BY id"
                    .to_string(),
                params: None,
                cursor: Some(next_cursor),
                max_rows: Some(2),
                max_cell_chars: None,
                statement_timeout_ms: None,
                preflight_check: Some(true),
                session_id: Some(session_id.clone()),
                profile: Some(ReadQueryProfile::Inspect),
            }))
            .await
            .expect("second page query should return a payload")
            .structured_content
            .expect("second page query should return structured content");

        assert_eq!(
            second_page
                .pointer("/data/columns")
                .and_then(Value::as_array)
                .cloned(),
            Some(vec![json!("id"), json!("label")])
        );
        assert_eq!(
            second_page
                .pointer("/data/rows")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            second_page.pointer("/data/rows/0").cloned(),
            Some(json!([3, "three"]))
        );
        assert!(
            second_page.pointer("/meta/next_cursor").is_none(),
            "final page should not include a next cursor"
        );

        let _ = server.close_pinned_session(&session_id);
    }

    #[tokio::test]
    async fn enriched_db_error_exports_derived_hint_and_schema_hints() {
        let server = test_server();
        let err = DbError::for_test(
            "DB_QUERY_FAILED",
            "db_query_failed",
            "column \"missing_provider\" does not exist",
            Some("42703"),
            Some("Perhaps you meant to reference a valid column."),
            None,
            Some("internal:7"),
        );
        let result = enriched_db_error(
            &server,
            "select missing_provider from public.llm_usage_events",
            "Error executing query",
            &err,
            9,
        )
        .await;
        let payload = result
            .structured_content
            .as_ref()
            .expect("query errors should remain structured");
        assert!(
            payload
                .pointer("/error/hint")
                .and_then(Value::as_str)
                .is_some()
        );
        assert!(
            payload
                .pointer("/error/hint")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("list_objects")
        );
        assert_eq!(
            payload
                .pointer("/error/schema_hints/kind")
                .and_then(Value::as_str),
            Some("missing_column")
        );
        assert_eq!(
            payload
                .pointer("/error/schema_hints/missing_column")
                .and_then(Value::as_str),
            Some("missing_provider")
        );
    }

    #[tokio::test]
    async fn enriched_db_error_preserves_native_hint_over_derived_hint() {
        let server = test_server();
        let err = DbError::for_test(
            "DB_QUERY_FAILED",
            "db_query_failed",
            "column \"missing_provider\" does not exist",
            Some("42703"),
            None,
            Some("Use provider instead."),
            Some("internal:7"),
        );
        let result = enriched_db_error(
            &server,
            "select missing_provider from public.llm_usage_events",
            "Error executing query",
            &err,
            9,
        )
        .await;
        let payload = result
            .structured_content
            .as_ref()
            .expect("query errors should remain structured");
        assert_eq!(
            payload.pointer("/error/hint").and_then(Value::as_str),
            Some("Use provider instead.")
        );
        assert_eq!(
            payload
                .pointer("/error/schema_hints/kind")
                .and_then(Value::as_str),
            Some("missing_column")
        );
    }
}
