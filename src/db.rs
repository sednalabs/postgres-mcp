//! # Database Access Layer
//!
//! Lazy PostgreSQL connection handling and query execution helpers.
//!
//! ## Rationale
//! Keep startup fast by avoiding mandatory DB network I/O until explicitly
//! requested by startup mode or first tool call.
//!
//! ## Security Boundaries
//! * User SQL in restricted mode is executed in a read-only transaction.
//! * Connection URI may contain secrets; errors are sanitized before logging.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{error::Error as StdError, fmt};

use mcp_toolkit_postgres::{
    ConnectionDriverError, PgConnectionConfig, PgInsecureTlsPolicy, PgTlsMode,
    PostgresTransportError,
};
use serde_json::{Map, Number, Value};
use tokio_postgres::types::{Json, ToSql, Type};
use tokio_postgres::{Client, Column, Row, SimpleColumn, SimpleQueryMessage, SimpleQueryRow};

use crate::config::AccessMode;
use crate::sql_safety::classify_restricted_sql;

#[derive(Debug, Clone)]
pub struct DbEngine {
    database_url: Option<String>,
    access_mode: AccessMode,
    allow_insecure_tls: bool,
    query_budget: QueryBudget,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryColumn {
    pub name: String,
    pub pg_type: String,
    pub nullable: Option<bool>,
}

impl QueryColumn {
    fn from_row_columns(columns: &[SimpleColumn], pg_types: Option<&[String]>) -> Vec<Self> {
        let output_names = dedupe_output_column_names(columns);
        columns
            .iter()
            .enumerate()
            .map(|(idx, column)| Self {
                name: output_names
                    .get(idx)
                    .cloned()
                    .unwrap_or_else(|| column.name().to_string()),
                pg_type: pg_types
                    .and_then(|types| types.get(idx))
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string()),
                nullable: None,
            })
            .collect()
    }

    fn from_typed_columns(columns: &[Column]) -> Vec<Self> {
        let output_names = dedupe_output_names(columns.iter().map(|column| column.name()));
        columns
            .iter()
            .enumerate()
            .map(|(idx, column)| Self {
                name: output_names
                    .get(idx)
                    .cloned()
                    .unwrap_or_else(|| column.name().to_string()),
                pg_type: column.type_().name().to_string(),
                nullable: None,
            })
            .collect()
    }
}

fn dedupe_output_column_names(columns: &[SimpleColumn]) -> Vec<String> {
    dedupe_output_names(columns.iter().map(|column| column.name()))
}

fn dedupe_output_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut used = HashSet::new();
    let mut duplicate_counts: HashMap<String, usize> = HashMap::new();
    let mut output_names = Vec::new();

    for raw_name in names {
        let raw_name = raw_name.to_string();
        let seen_count = duplicate_counts.entry(raw_name.clone()).or_insert(0);
        *seen_count += 1;

        let mut candidate = if *seen_count == 1 {
            raw_name.clone()
        } else {
            format!("{raw_name}__dup{seen_count}")
        };

        while used.contains(&candidate) {
            *seen_count += 1;
            candidate = format!("{raw_name}__dup{seen_count}");
        }

        used.insert(candidate.clone());
        output_names.push(candidate);
    }

    output_names
}

#[derive(Debug, Clone)]
pub struct QueryOutput {
    pub rows: Vec<Map<String, Value>>,
    pub columns: Vec<QueryColumn>,
    pub rows_affected: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct QueryBudget {
    pub request_timeout: Option<Duration>,
    pub statement_timeout: Option<Duration>,
    pub lock_timeout: Option<Duration>,
}

#[derive(Clone)]
pub struct PinnedDbSession {
    inner: Arc<PinnedDbSessionInner>,
}

struct PinnedDbSessionInner {
    client: Client,
    execution_lock: tokio::sync::Mutex<()>,
    base_query_budget: QueryBudget,
    force_read_only: bool,
    driver_abort: tokio::task::AbortHandle,
    driver_failed: Arc<AtomicBool>,
    backend_pid: i32,
}

#[derive(Debug, Clone)]
enum BoundQueryParam {
    Bool(Option<bool>),
    Int8(Option<i64>),
    Float8(Option<f64>),
    Text(Option<String>),
    Jsonb(Option<Json<Value>>),
    BoolArray(Option<Vec<Option<bool>>>),
    Int8Array(Option<Vec<Option<i64>>>),
    Float8Array(Option<Vec<Option<f64>>>),
    TextArray(Option<Vec<Option<String>>>),
    JsonbArray(Option<Vec<Option<Json<Value>>>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundQueryParamKind {
    Bool,
    Int8,
    Float8,
    Text,
    Jsonb,
    BoolArray,
    Int8Array,
    Float8Array,
    TextArray,
    JsonbArray,
}

impl BoundQueryParam {
    fn postgres_type(&self) -> Type {
        match self {
            Self::Bool(_) => Type::BOOL,
            Self::Int8(_) => Type::INT8,
            Self::Float8(_) => Type::FLOAT8,
            Self::Text(_) => Type::TEXT,
            Self::Jsonb(_) => Type::JSONB,
            Self::BoolArray(_) => Type::BOOL_ARRAY,
            Self::Int8Array(_) => Type::INT8_ARRAY,
            Self::Float8Array(_) => Type::FLOAT8_ARRAY,
            Self::TextArray(_) => Type::TEXT_ARRAY,
            Self::JsonbArray(_) => Type::JSONB_ARRAY,
        }
    }

    fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Bool(value) => value,
            Self::Int8(value) => value,
            Self::Float8(value) => value,
            Self::Text(value) => value,
            Self::Jsonb(value) => value,
            Self::BoolArray(value) => value,
            Self::Int8Array(value) => value,
            Self::Float8Array(value) => value,
            Self::TextArray(value) => value,
            Self::JsonbArray(value) => value,
        }
    }
}

pub type DbResult<T> = std::result::Result<T, DbError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbError {
    code: &'static str,
    reason: &'static str,
    message: String,
    sqlstate: Option<String>,
    detail: Option<String>,
    hint: Option<String>,
    position: Option<String>,
}

impl DbError {
    fn database_uri_missing() -> Self {
        Self {
            code: "DATABASE_URI_NOT_CONFIGURED",
            reason: "database_uri_not_configured",
            message: "DATABASE_URI is not configured".to_string(),
            sqlstate: None,
            detail: None,
            hint: None,
            position: None,
        }
    }

    fn sql_input_invalid(message: impl Into<String>) -> Self {
        Self {
            code: "SQL_INPUT_INVALID",
            reason: "sql_input_invalid",
            message: message.into(),
            sqlstate: None,
            detail: None,
            hint: None,
            position: None,
        }
    }

    fn sql_policy_rejected(policy_code: &str, policy_message: &str) -> Self {
        Self {
            code: "SQL_POLICY_REJECTED",
            reason: "restricted_sql",
            message: format!("{policy_code}: {policy_message}"),
            sqlstate: None,
            detail: None,
            hint: None,
            position: None,
        }
    }

    fn startup_timeout() -> Self {
        Self {
            code: "STARTUP_DB_CONNECT_TIMEOUT",
            reason: "startup_db_connect_timeout",
            message: "startup DB connect timed out".to_string(),
            sqlstate: None,
            detail: None,
            hint: None,
            position: None,
        }
    }

    fn query_timeout(limit: Duration) -> Self {
        Self {
            code: "DB_QUERY_TIMEOUT",
            reason: "db_query_timeout",
            message: format!(
                "query timed out after {} ms (configure POSTGRES_MCP_DB_QUERY_TIMEOUT_MS to adjust)",
                limit.as_millis()
            ),
            sqlstate: None,
            detail: None,
            hint: None,
            position: None,
        }
    }

    pub(crate) fn session_closed(message: impl Into<String>) -> Self {
        Self {
            code: "DB_SESSION_CLOSED",
            reason: "db_session_closed",
            message: message.into(),
            sqlstate: None,
            detail: None,
            hint: None,
            position: None,
        }
    }

    fn connection_driver_failed(err: &ConnectionDriverError) -> Self {
        Self {
            code: "DB_CONNECTION_DRIVER_FAILED",
            reason: "db_connection_driver_failed",
            message: format!("database connection driver failed: {}", err.message()),
            sqlstate: err.sqlstate().map(str::to_string),
            detail: None,
            hint: None,
            position: None,
        }
    }

    fn connect_transport_failed(err: &PostgresTransportError) -> Self {
        match err.code() {
            "PG_SSLMODE_INVALID"
            | "PG_DSN_INVALID"
            | "PG_TLS_CONFIG_ERROR"
            | "PG_TLS_POLICY_VIOLATION" => Self {
                code: "DB_CONNECT_CONFIG_INVALID",
                reason: match err.reason() {
                    "sslmode_invalid" => "db_connect_sslmode_invalid",
                    "sslmode_ambiguous" => "db_connect_sslmode_ambiguous",
                    "dsn_invalid" => "db_connect_dsn_invalid",
                    "dsn_param_ambiguous" => "db_connect_dsn_param_ambiguous",
                    "tls_config_error" => "db_connect_tls_config_error",
                    "tls_policy_disallowed" => "db_connect_tls_policy_disallowed",
                    _ => "db_connect_config_invalid",
                },
                message: format!(
                    "invalid PostgreSQL transport configuration: {}",
                    err.message()
                ),
                sqlstate: None,
                detail: None,
                hint: None,
                position: None,
            },
            _ => Self {
                code: "DB_CONNECT_FAILED",
                reason: "db_connect_failed",
                message: format!("failed to connect to PostgreSQL: {}", err.message()),
                sqlstate: err.sqlstate().map(str::to_string),
                detail: None,
                hint: None,
                position: None,
            },
        }
    }

    fn query_failed(err: &tokio_postgres::Error) -> Self {
        Self::from_postgres_error(
            err,
            "DB_QUERY_FAILED",
            "db_query_failed",
            "query execution failed",
        )
    }

    fn from_postgres_error(
        err: &tokio_postgres::Error,
        code: &'static str,
        reason: &'static str,
        prefix: &str,
    ) -> Self {
        if let Some(db_err) = err.as_db_error() {
            return Self {
                code,
                reason,
                message: format!("{prefix}: {}", db_err.message()),
                sqlstate: Some(db_err.code().code().to_string()),
                detail: db_err.detail().map(str::to_string),
                hint: db_err.hint().map(str::to_string),
                position: db_err.position().map(|position| match position {
                    tokio_postgres::error::ErrorPosition::Original(value) => {
                        format!("original:{value}")
                    }
                    tokio_postgres::error::ErrorPosition::Internal { position, .. } => {
                        format!("internal:{position}")
                    }
                }),
            };
        }
        Self {
            code,
            reason,
            message: format!("{prefix}: {err}"),
            sqlstate: None,
            detail: None,
            hint: None,
            position: None,
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn reason(&self) -> &'static str {
        self.reason
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn sqlstate(&self) -> Option<&str> {
        self.sqlstate.as_deref()
    }

    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    pub fn position(&self) -> Option<&str> {
        self.position.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        code: &'static str,
        reason: &'static str,
        message: impl Into<String>,
        sqlstate: Option<&str>,
        detail: Option<&str>,
        hint: Option<&str>,
        position: Option<&str>,
    ) -> Self {
        Self {
            code,
            reason,
            message: message.into(),
            sqlstate: sqlstate.map(str::to_string),
            detail: detail.map(str::to_string),
            hint: hint.map(str::to_string),
            position: position.map(str::to_string),
        }
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl StdError for DbError {}

impl DbEngine {
    pub fn new(
        database_url: Option<String>,
        access_mode: AccessMode,
        allow_insecure_tls: bool,
        query_timeout: Option<Duration>,
        statement_timeout: Option<Duration>,
        lock_timeout: Option<Duration>,
    ) -> Self {
        Self {
            database_url,
            access_mode,
            allow_insecure_tls,
            query_budget: QueryBudget {
                request_timeout: query_timeout,
                statement_timeout,
                lock_timeout,
            },
        }
    }

    pub async fn startup_connect_probe(&self, timeout: Option<Duration>) -> DbResult<()> {
        let fut = async {
            let _ = self.execute_internal("SELECT 1", false).await?;
            Ok::<(), DbError>(())
        };

        if let Some(limit) = timeout {
            tokio::time::timeout(limit, fut)
                .await
                .map_err(|_| DbError::startup_timeout())??;
            return Ok(());
        }

        fut.await
    }

    pub async fn execute_user_sql(&self, sql: &str) -> DbResult<QueryOutput> {
        self.execute_user_sql_with_statement_timeout(sql, None)
            .await
    }

    pub async fn execute_user_sql_with_statement_timeout(
        &self,
        sql: &str,
        statement_timeout_override: Option<Duration>,
    ) -> DbResult<QueryOutput> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(DbError::sql_input_invalid("sql must not be empty"));
        }

        let force_read_only = self.access_mode == AccessMode::Restricted;
        if force_read_only {
            classify_restricted_sql(sql).map_err(|err| {
                DbError::sql_policy_rejected(err.code.as_str(), err.message.as_str())
            })?;
        }

        let query_budget =
            apply_statement_timeout_override(self.query_budget, statement_timeout_override);
        self.execute_internal_with_budget(sql, force_read_only, query_budget)
            .await
    }

    pub async fn execute_user_sql_with_params_and_statement_timeout(
        &self,
        sql: &str,
        params: &[Value],
        statement_timeout_override: Option<Duration>,
    ) -> DbResult<QueryOutput> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(DbError::sql_input_invalid("sql must not be empty"));
        }

        let force_read_only = self.access_mode == AccessMode::Restricted;
        if force_read_only {
            classify_restricted_sql(sql).map_err(|err| {
                DbError::sql_policy_rejected(err.code.as_str(), err.message.as_str())
            })?;
        }

        let query_budget =
            apply_statement_timeout_override(self.query_budget, statement_timeout_override);
        self.execute_internal_with_params(sql, params, force_read_only, query_budget)
            .await
    }

    pub async fn describe_user_sql_with_params_and_statement_timeout(
        &self,
        sql: &str,
        params: &[Value],
        statement_timeout_override: Option<Duration>,
    ) -> DbResult<Vec<QueryColumn>> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(DbError::sql_input_invalid("sql must not be empty"));
        }

        let force_read_only = self.access_mode == AccessMode::Restricted;
        if force_read_only {
            classify_restricted_sql(sql).map_err(|err| {
                DbError::sql_policy_rejected(err.code.as_str(), err.message.as_str())
            })?;
        }

        let query_budget =
            apply_statement_timeout_override(self.query_budget, statement_timeout_override);
        let bound_params = parse_bound_query_params(params)?;
        self.describe_internal_with_params(sql, &bound_params, force_read_only, query_budget)
            .await
    }

    pub async fn execute_query_readonly(&self, sql: &str) -> DbResult<QueryOutput> {
        self.execute_internal_with_budget(sql, true, self.query_budget)
            .await
    }

    pub async fn execute_query_unrestricted(&self, sql: &str) -> DbResult<QueryOutput> {
        self.execute_internal_with_budget(sql, false, self.query_budget)
            .await
    }

    pub async fn open_pinned_session(&self) -> DbResult<PinnedDbSession> {
        let database_url = self
            .database_url
            .as_deref()
            .ok_or_else(DbError::database_uri_missing)?;

        let connect = PgConnectionConfig::from_dsn(database_url)
            .map_err(|err| DbError::connect_transport_failed(&err))?;
        if matches!(connect.tls_mode(), PgTlsMode::InsecureRequire) && self.allow_insecure_tls {
            tracing::warn!(
                "Database sslmode=require enables encryption without certificate verification. \
                 Prefer sslmode=verify-full (or verify-ca) with trusted roots."
            );
        }
        let insecure_policy = if self.allow_insecure_tls {
            PgInsecureTlsPolicy::AllowRequireOnly
        } else {
            PgInsecureTlsPolicy::DisallowAll
        };

        let (client, connection) = connect
            .connect_with_policy(insecure_policy)
            .await
            .map_err(|err| DbError::connect_transport_failed(&err))?;
        let force_read_only = self.access_mode == AccessMode::Restricted;
        apply_pinned_session_budget(&client, force_read_only, self.query_budget).await?;
        let backend_pid = load_backend_pid(&client, self.query_budget.request_timeout).await?;
        let driver_failed = Arc::new(AtomicBool::new(false));
        let driver_failed_flag = driver_failed.clone();
        let driver_task = tokio::spawn(async move {
            let driver_result = connection.wait().await;
            if let Err(driver_err) = &driver_result {
                mcp_toolkit_observability::emit_event(
                    mcp_toolkit_observability::Level::WARN,
                    "postgres_mcp.db.connection_driver_error",
                    &mcp_toolkit_observability::EventContext::new(),
                    &[
                        mcp_toolkit_observability::safe_text("error", driver_err.message()),
                        mcp_toolkit_observability::safe_text(
                            "sqlstate",
                            driver_err.sqlstate().unwrap_or(""),
                        ),
                    ],
                );
                driver_failed_flag.store(true, Ordering::SeqCst);
            }
        });

        Ok(PinnedDbSession {
            inner: Arc::new(PinnedDbSessionInner {
                client,
                execution_lock: tokio::sync::Mutex::new(()),
                base_query_budget: self.query_budget,
                force_read_only,
                driver_abort: driver_task.abort_handle(),
                driver_failed,
                backend_pid,
            }),
        })
    }

    async fn execute_internal(&self, sql: &str, force_read_only: bool) -> DbResult<QueryOutput> {
        self.execute_internal_with_budget(sql, force_read_only, self.query_budget)
            .await
    }

    async fn execute_internal_with_budget(
        &self,
        sql: &str,
        force_read_only: bool,
        query_budget: QueryBudget,
    ) -> DbResult<QueryOutput> {
        let database_url = self
            .database_url
            .as_deref()
            .ok_or_else(DbError::database_uri_missing)?;

        let connect = PgConnectionConfig::from_dsn(database_url)
            .map_err(|err| DbError::connect_transport_failed(&err))?;
        if matches!(connect.tls_mode(), PgTlsMode::InsecureRequire) && self.allow_insecure_tls {
            tracing::warn!(
                "Database sslmode=require enables encryption without certificate verification. \
                 Prefer sslmode=verify-full (or verify-ca) with trusted roots."
            );
        }
        let insecure_policy = if self.allow_insecure_tls {
            PgInsecureTlsPolicy::AllowRequireOnly
        } else {
            PgInsecureTlsPolicy::DisallowAll
        };

        let (client, connection) = connect
            .connect_with_policy(insecure_policy)
            .await
            .map_err(|err| DbError::connect_transport_failed(&err))?;

        let sql_to_run = budgeted_sql(sql, force_read_only, query_budget);
        let query_result: DbResult<Vec<SimpleQueryMessage>> =
            if let Some(limit) = query_budget.request_timeout {
                match tokio::time::timeout(limit, client.simple_query(&sql_to_run)).await {
                    Ok(result) => result.map_err(|err| DbError::query_failed(&err)),
                    Err(_) => {
                        mcp_toolkit_observability::emit_event(
                            mcp_toolkit_observability::Level::WARN,
                            "postgres_mcp.db.query_timeout",
                            &mcp_toolkit_observability::EventContext::new(),
                            &[mcp_toolkit_observability::safe_text(
                                "query_timeout_ms",
                                limit.as_millis().to_string(),
                            )],
                        );
                        Err(DbError::query_timeout(limit))
                    }
                }
            } else {
                client
                    .simple_query(&sql_to_run)
                    .await
                    .map_err(|err| DbError::query_failed(&err))
            };

        let messages = query_result?;
        let mut output = last_statement_output(messages);
        let inferred_types = infer_result_column_types(&client, &sql_to_run, &output.columns)
            .await
            .ok()
            .flatten();
        let raw_columns = std::mem::take(&mut output.columns);
        output.columns = match inferred_types {
            Some(types) => raw_columns
                .into_iter()
                .enumerate()
                .map(|(idx, mut column)| {
                    if let Some(pg_type) = types.get(idx) {
                        column.pg_type = pg_type.clone();
                    }
                    column
                })
                .collect(),
            None => raw_columns,
        };

        // Drop client first so the background connection can shut down cleanly.
        drop(client);
        let driver_result = connection.wait().await;
        if let Err(driver_err) = &driver_result {
            mcp_toolkit_observability::emit_event(
                mcp_toolkit_observability::Level::WARN,
                "postgres_mcp.db.connection_driver_error",
                &mcp_toolkit_observability::EventContext::new(),
                &[
                    mcp_toolkit_observability::safe_text("error", driver_err.message()),
                    mcp_toolkit_observability::safe_text(
                        "sqlstate",
                        driver_err.sqlstate().unwrap_or(""),
                    ),
                ],
            );
        }

        if let Err(driver_err) = driver_result {
            return Err(DbError::connection_driver_failed(&driver_err));
        }

        Ok(output)
    }

    async fn execute_internal_with_params(
        &self,
        sql: &str,
        params: &[Value],
        force_read_only: bool,
        query_budget: QueryBudget,
    ) -> DbResult<QueryOutput> {
        let bound_params = parse_bound_query_params(params)?;
        self.execute_internal_with_bound_params(sql, &bound_params, force_read_only, query_budget)
            .await
    }

    async fn describe_internal_with_params(
        &self,
        sql: &str,
        params: &[BoundQueryParam],
        force_read_only: bool,
        query_budget: QueryBudget,
    ) -> DbResult<Vec<QueryColumn>> {
        let database_url = self
            .database_url
            .as_deref()
            .ok_or_else(DbError::database_uri_missing)?;

        let connect = PgConnectionConfig::from_dsn(database_url)
            .map_err(|err| DbError::connect_transport_failed(&err))?;
        if matches!(connect.tls_mode(), PgTlsMode::InsecureRequire) && self.allow_insecure_tls {
            tracing::warn!(
                "Database sslmode=require enables encryption without certificate verification. \
                 Prefer sslmode=verify-full (or verify-ca) with trusted roots."
            );
        }
        let insecure_policy = if self.allow_insecure_tls {
            PgInsecureTlsPolicy::AllowRequireOnly
        } else {
            PgInsecureTlsPolicy::DisallowAll
        };

        let (client, connection) = connect
            .connect_with_policy(insecure_policy)
            .await
            .map_err(|err| DbError::connect_transport_failed(&err))?;
        apply_session_budget(&client, force_read_only, query_budget).await?;
        let statement = prepare_typed_statement(&client, sql, params, query_budget).await?;
        let columns = QueryColumn::from_typed_columns(statement.columns());

        drop(client);
        let driver_result = connection.wait().await;
        if let Err(driver_err) = &driver_result {
            mcp_toolkit_observability::emit_event(
                mcp_toolkit_observability::Level::WARN,
                "postgres_mcp.db.connection_driver_error",
                &mcp_toolkit_observability::EventContext::new(),
                &[
                    mcp_toolkit_observability::safe_text("error", driver_err.message()),
                    mcp_toolkit_observability::safe_text(
                        "sqlstate",
                        driver_err.sqlstate().unwrap_or(""),
                    ),
                ],
            );
        }

        if let Err(driver_err) = driver_result {
            return Err(DbError::connection_driver_failed(&driver_err));
        }

        Ok(columns)
    }

    async fn execute_internal_with_bound_params(
        &self,
        sql: &str,
        params: &[BoundQueryParam],
        force_read_only: bool,
        query_budget: QueryBudget,
    ) -> DbResult<QueryOutput> {
        let database_url = self
            .database_url
            .as_deref()
            .ok_or_else(DbError::database_uri_missing)?;

        let connect = PgConnectionConfig::from_dsn(database_url)
            .map_err(|err| DbError::connect_transport_failed(&err))?;
        if matches!(connect.tls_mode(), PgTlsMode::InsecureRequire) && self.allow_insecure_tls {
            tracing::warn!(
                "Database sslmode=require enables encryption without certificate verification. \
                 Prefer sslmode=verify-full (or verify-ca) with trusted roots."
            );
        }
        let insecure_policy = if self.allow_insecure_tls {
            PgInsecureTlsPolicy::AllowRequireOnly
        } else {
            PgInsecureTlsPolicy::DisallowAll
        };

        let (client, connection) = connect
            .connect_with_policy(insecure_policy)
            .await
            .map_err(|err| DbError::connect_transport_failed(&err))?;
        apply_session_budget(&client, force_read_only, query_budget).await?;
        let statement = prepare_typed_statement(&client, sql, params, query_budget).await?;
        let columns = QueryColumn::from_typed_columns(statement.columns());
        let param_refs = bound_param_refs(params);
        let fetch_strategy = bound_query_row_fetch_strategy(sql);
        let result = if columns.is_empty() {
            let rows_affected = run_query_with_timeout(
                query_budget.request_timeout,
                client.execute(&statement, &param_refs),
            )
            .await?;
            QueryOutput {
                rows: Vec::new(),
                columns,
                rows_affected: Some(rows_affected),
            }
        } else if let Some(wrapper_sql) = wrap_query_for_json_rows(sql, &columns) {
            let wrapper_statement =
                prepare_typed_statement(&client, &wrapper_sql, params, query_budget).await?;
            let wrapped_rows = run_query_with_timeout(
                query_budget.request_timeout,
                client.query(&wrapper_statement, &param_refs),
            )
            .await?;
            let rows = json_rows_from_wrapped_query(wrapped_rows)?;
            let rows_affected = match fetch_strategy {
                BoundQueryRowFetchStrategy::CommonTableExpressionWrapper => Some(rows.len() as u64),
                _ => None,
            };
            QueryOutput {
                rows,
                columns,
                rows_affected,
            }
        } else {
            let typed_rows = run_query_with_timeout(
                query_budget.request_timeout,
                client.query(&statement, &param_refs),
            )
            .await?;
            let rows = json_rows_from_typed_query(typed_rows, &columns)?;
            QueryOutput {
                rows,
                columns,
                rows_affected: None,
            }
        };

        drop(client);
        let driver_result = connection.wait().await;
        if let Err(driver_err) = &driver_result {
            mcp_toolkit_observability::emit_event(
                mcp_toolkit_observability::Level::WARN,
                "postgres_mcp.db.connection_driver_error",
                &mcp_toolkit_observability::EventContext::new(),
                &[
                    mcp_toolkit_observability::safe_text("error", driver_err.message()),
                    mcp_toolkit_observability::safe_text(
                        "sqlstate",
                        driver_err.sqlstate().unwrap_or(""),
                    ),
                ],
            );
        }

        if let Err(driver_err) = driver_result {
            return Err(DbError::connection_driver_failed(&driver_err));
        }

        Ok(result)
    }
}

impl PinnedDbSession {
    pub fn backend_pid(&self) -> i32 {
        self.inner.backend_pid
    }

    pub fn close(&self) {
        self.inner.driver_abort.abort();
    }

    pub async fn execute_sql_with_statement_timeout(
        &self,
        sql: &str,
        statement_timeout_override: Option<Duration>,
    ) -> DbResult<QueryOutput> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(DbError::sql_input_invalid("sql must not be empty"));
        }
        if self.inner.force_read_only {
            classify_restricted_sql(sql).map_err(|err| {
                DbError::sql_policy_rejected(err.code.as_str(), err.message.as_str())
            })?;
        }
        let query_budget = apply_statement_timeout_override(
            self.inner.base_query_budget,
            statement_timeout_override,
        );
        let _guard = self.inner.execution_lock.lock().await;
        self.ensure_open()?;
        apply_pinned_session_budget(&self.inner.client, self.inner.force_read_only, query_budget)
            .await?;
        let messages = run_query_with_timeout(
            query_budget.request_timeout,
            self.inner.client.simple_query(sql),
        )
        .await?;
        let mut output = last_statement_output(messages);
        let inferred_types = infer_result_column_types(&self.inner.client, sql, &output.columns)
            .await
            .ok()
            .flatten();
        let raw_columns = std::mem::take(&mut output.columns);
        output.columns = match inferred_types {
            Some(types) => raw_columns
                .into_iter()
                .enumerate()
                .map(|(idx, mut column)| {
                    if let Some(pg_type) = types.get(idx) {
                        column.pg_type = pg_type.clone();
                    }
                    column
                })
                .collect(),
            None => raw_columns,
        };
        Ok(output)
    }

    pub async fn execute_sql_with_params_and_statement_timeout(
        &self,
        sql: &str,
        params: &[Value],
        statement_timeout_override: Option<Duration>,
    ) -> DbResult<QueryOutput> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(DbError::sql_input_invalid("sql must not be empty"));
        }
        if self.inner.force_read_only {
            classify_restricted_sql(sql).map_err(|err| {
                DbError::sql_policy_rejected(err.code.as_str(), err.message.as_str())
            })?;
        }
        let bound_params = parse_bound_query_params(params)?;
        let query_budget = apply_statement_timeout_override(
            self.inner.base_query_budget,
            statement_timeout_override,
        );
        let _guard = self.inner.execution_lock.lock().await;
        self.ensure_open()?;
        apply_pinned_session_budget(&self.inner.client, self.inner.force_read_only, query_budget)
            .await?;
        let statement =
            prepare_typed_statement(&self.inner.client, sql, &bound_params, query_budget).await?;
        let columns = QueryColumn::from_typed_columns(statement.columns());
        let param_refs = bound_param_refs(&bound_params);
        let fetch_strategy = bound_query_row_fetch_strategy(sql);
        if columns.is_empty() {
            let rows_affected = run_query_with_timeout(
                query_budget.request_timeout,
                self.inner.client.execute(&statement, &param_refs),
            )
            .await?;
            return Ok(QueryOutput {
                rows: Vec::new(),
                columns,
                rows_affected: Some(rows_affected),
            });
        }
        if let Some(wrapper_sql) = wrap_query_for_json_rows(sql, &columns) {
            let wrapper_statement = prepare_typed_statement(
                &self.inner.client,
                &wrapper_sql,
                &bound_params,
                query_budget,
            )
            .await?;
            let wrapped_rows = run_query_with_timeout(
                query_budget.request_timeout,
                self.inner.client.query(&wrapper_statement, &param_refs),
            )
            .await?;
            let rows = json_rows_from_wrapped_query(wrapped_rows)?;
            let rows_affected = match fetch_strategy {
                BoundQueryRowFetchStrategy::CommonTableExpressionWrapper => Some(rows.len() as u64),
                _ => None,
            };
            return Ok(QueryOutput {
                rows,
                columns,
                rows_affected,
            });
        }
        let typed_rows = run_query_with_timeout(
            query_budget.request_timeout,
            self.inner.client.query(&statement, &param_refs),
        )
        .await?;
        let rows = json_rows_from_typed_query(typed_rows, &columns)?;
        Ok(QueryOutput {
            rows,
            columns,
            rows_affected: None,
        })
    }

    pub async fn describe_sql_with_params_and_statement_timeout(
        &self,
        sql: &str,
        params: &[Value],
        statement_timeout_override: Option<Duration>,
    ) -> DbResult<Vec<QueryColumn>> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(DbError::sql_input_invalid("sql must not be empty"));
        }
        if self.inner.force_read_only {
            classify_restricted_sql(sql).map_err(|err| {
                DbError::sql_policy_rejected(err.code.as_str(), err.message.as_str())
            })?;
        }
        let bound_params = parse_bound_query_params(params)?;
        let query_budget = apply_statement_timeout_override(
            self.inner.base_query_budget,
            statement_timeout_override,
        );
        let _guard = self.inner.execution_lock.lock().await;
        self.ensure_open()?;
        apply_pinned_session_budget(&self.inner.client, self.inner.force_read_only, query_budget)
            .await?;
        let statement =
            prepare_typed_statement(&self.inner.client, sql, &bound_params, query_budget).await?;
        Ok(QueryColumn::from_typed_columns(statement.columns()))
    }

    fn ensure_open(&self) -> DbResult<()> {
        if self.inner.driver_failed.load(Ordering::SeqCst) {
            return Err(DbError::session_closed(
                "pinned session is no longer available; open a new session and retry",
            ));
        }
        Ok(())
    }
}

fn bound_param_refs(params: &[BoundQueryParam]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(BoundQueryParam::as_tosql).collect()
}

fn parse_explicit_bound_query_param_kind(raw: &str) -> Option<BoundQueryParamKind> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Some(BoundQueryParamKind::Bool),
        "int8" | "bigint" | "int" | "integer" => Some(BoundQueryParamKind::Int8),
        "float8" | "float" | "double precision" | "double" => Some(BoundQueryParamKind::Float8),
        "text" | "varchar" | "string" => Some(BoundQueryParamKind::Text),
        "json" | "jsonb" => Some(BoundQueryParamKind::Jsonb),
        "bool[]" | "boolean[]" => Some(BoundQueryParamKind::BoolArray),
        "int8[]" | "bigint[]" | "int[]" | "integer[]" => Some(BoundQueryParamKind::Int8Array),
        "float8[]" | "float[]" | "double precision[]" | "double[]" => {
            Some(BoundQueryParamKind::Float8Array)
        }
        "text[]" | "varchar[]" | "string[]" => Some(BoundQueryParamKind::TextArray),
        "json[]" | "jsonb[]" => Some(BoundQueryParamKind::JsonbArray),
        _ => None,
    }
}

fn parse_bound_query_params(values: &[Value]) -> DbResult<Vec<BoundQueryParam>> {
    values
        .iter()
        .enumerate()
        .map(|(idx, value)| parse_bound_query_param(value, idx + 1))
        .collect()
}

fn parse_bound_query_param(value: &Value, position: usize) -> DbResult<BoundQueryParam> {
    if let Value::Object(object) = value {
        if let Some((kind, explicit_value)) = explicit_bound_query_wrapper_parts(object, position)?
        {
            return parse_explicit_bound_query_param(kind, explicit_value, position);
        }
    }

    match value {
        Value::Bool(raw) => Ok(BoundQueryParam::Bool(Some(*raw))),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                return Ok(BoundQueryParam::Int8(Some(value)));
            }
            if let Some(value) = number.as_u64() {
                return i64::try_from(value)
                    .map(|cast| BoundQueryParam::Int8(Some(cast)))
                    .map_err(|_| {
                        DbError::sql_input_invalid(format!(
                            "params[{position}] integer is out of supported int8 range"
                        ))
                    });
            }
            if let Some(value) = number.as_f64() {
                if !value.is_finite() {
                    return Err(DbError::sql_input_invalid(format!(
                        "params[{position}] floating-point value must be finite"
                    )));
                }
                return Ok(BoundQueryParam::Float8(Some(value)));
            }
            Err(DbError::sql_input_invalid(format!(
                "params[{position}] uses unsupported numeric value"
            )))
        }
        Value::String(raw) => Ok(BoundQueryParam::Text(Some(raw.clone()))),
        Value::Null => Err(DbError::sql_input_invalid(format!(
            "params[{position}] null requires an explicit typed wrapper like {{\"type\":\"text\",\"value\":null}}"
        ))),
        Value::Object(_) => Ok(BoundQueryParam::Jsonb(Some(Json(value.clone())))),
        Value::Array(items) => parse_raw_bound_query_param_array(items, position),
    }
}

fn parse_explicit_bound_query_param(
    kind: BoundQueryParamKind,
    value: &Value,
    position: usize,
) -> DbResult<BoundQueryParam> {
    match kind {
        BoundQueryParamKind::Bool => match value {
            Value::Null => Ok(BoundQueryParam::Bool(None)),
            Value::Bool(raw) => Ok(BoundQueryParam::Bool(Some(*raw))),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{position}] expected boolean for explicit type bool"
            ))),
        },
        BoundQueryParamKind::Int8 => match value {
            Value::Null => Ok(BoundQueryParam::Int8(None)),
            Value::Number(number) => {
                parse_explicit_i64(number, position).map(|raw| BoundQueryParam::Int8(Some(raw)))
            }
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{position}] expected integer for explicit type int8"
            ))),
        },
        BoundQueryParamKind::Float8 => match value {
            Value::Null => Ok(BoundQueryParam::Float8(None)),
            Value::Number(number) => {
                parse_explicit_f64(number, position).map(|raw| BoundQueryParam::Float8(Some(raw)))
            }
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{position}] expected number for explicit type float8"
            ))),
        },
        BoundQueryParamKind::Text => match value {
            Value::Null => Ok(BoundQueryParam::Text(None)),
            Value::String(raw) => Ok(BoundQueryParam::Text(Some(raw.clone()))),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{position}] expected string for explicit type text"
            ))),
        },
        BoundQueryParamKind::Jsonb => match value {
            Value::Null => Ok(BoundQueryParam::Jsonb(None)),
            _ => Ok(BoundQueryParam::Jsonb(Some(Json(value.clone())))),
        },
        BoundQueryParamKind::BoolArray => match value {
            Value::Null => Ok(BoundQueryParam::BoolArray(None)),
            Value::Array(items) => parse_bool_array(items, position, true)
                .map(|raw| BoundQueryParam::BoolArray(Some(raw))),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{position}] expected array for explicit type bool[]"
            ))),
        },
        BoundQueryParamKind::Int8Array => match value {
            Value::Null => Ok(BoundQueryParam::Int8Array(None)),
            Value::Array(items) => parse_i64_array(items, position, true)
                .map(|raw| BoundQueryParam::Int8Array(Some(raw))),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{position}] expected array for explicit type int8[]"
            ))),
        },
        BoundQueryParamKind::Float8Array => match value {
            Value::Null => Ok(BoundQueryParam::Float8Array(None)),
            Value::Array(items) => parse_f64_array(items, position, true)
                .map(|raw| BoundQueryParam::Float8Array(Some(raw))),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{position}] expected array for explicit type float8[]"
            ))),
        },
        BoundQueryParamKind::TextArray => match value {
            Value::Null => Ok(BoundQueryParam::TextArray(None)),
            Value::Array(items) => parse_string_array(items, position, true)
                .map(|raw| BoundQueryParam::TextArray(Some(raw))),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{position}] expected array for explicit type text[]"
            ))),
        },
        BoundQueryParamKind::JsonbArray => match value {
            Value::Null => Ok(BoundQueryParam::JsonbArray(None)),
            Value::Array(items) => Ok(BoundQueryParam::JsonbArray(Some(
                items
                    .iter()
                    .map(|item| match item {
                        Value::Null => None,
                        other => Some(Json(other.clone())),
                    })
                    .collect(),
            ))),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{position}] expected array for explicit type jsonb[]"
            ))),
        },
    }
}

fn explicit_bound_query_wrapper_parts<'a>(
    object: &'a Map<String, Value>,
    position: usize,
) -> DbResult<Option<(BoundQueryParamKind, &'a Value)>> {
    if object.len() != 2 {
        return Ok(None);
    }

    let Some(explicit_type) = object.get("type").and_then(Value::as_str) else {
        return Ok(None);
    };
    let Some(explicit_value) = object.get("value") else {
        return Ok(None);
    };

    let Some(kind) = parse_explicit_bound_query_param_kind(explicit_type) else {
        return Ok(None);
    };

    if matches!(explicit_value, Value::Null | Value::Array(_)) {
        return Ok(Some((kind, explicit_value)));
    }

    let _ = position;
    Ok(None)
}

fn parse_explicit_i64(number: &Number, position: usize) -> DbResult<i64> {
    if let Some(value) = number.as_i64() {
        return Ok(value);
    }
    if let Some(value) = number.as_u64() {
        return i64::try_from(value).map_err(|_| {
            DbError::sql_input_invalid(format!(
                "params[{position}] integer is out of supported int8 range"
            ))
        });
    }
    Err(DbError::sql_input_invalid(format!(
        "params[{position}] expected integer-compatible numeric value"
    )))
}

fn parse_explicit_f64(number: &Number, position: usize) -> DbResult<f64> {
    let Some(value) = number.as_f64() else {
        return Err(DbError::sql_input_invalid(format!(
            "params[{position}] expected floating-point-compatible numeric value"
        )));
    };
    if !value.is_finite() {
        return Err(DbError::sql_input_invalid(format!(
            "params[{position}] floating-point value must be finite"
        )));
    }
    Ok(value)
}

fn parse_bool_array(
    items: &[Value],
    position: usize,
    allow_nulls: bool,
) -> DbResult<Vec<Option<bool>>> {
    items
        .iter()
        .enumerate()
        .map(|(idx, item)| match item {
            Value::Null if allow_nulls => Ok(None),
            Value::Bool(raw) => Ok(Some(*raw)),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{}][{}] expected boolean array element",
                position, idx
            ))),
        })
        .collect()
}

fn parse_i64_array(
    items: &[Value],
    position: usize,
    allow_nulls: bool,
) -> DbResult<Vec<Option<i64>>> {
    items
        .iter()
        .enumerate()
        .map(|(idx, item)| match item {
            Value::Null if allow_nulls => Ok(None),
            Value::Number(number) => parse_explicit_i64(number, position).map(Some).map_err(|_| {
                DbError::sql_input_invalid(format!(
                    "params[{}][{}] expected integer array element",
                    position, idx
                ))
            }),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{}][{}] expected integer array element",
                position, idx
            ))),
        })
        .collect()
}

fn parse_f64_array(
    items: &[Value],
    position: usize,
    allow_nulls: bool,
) -> DbResult<Vec<Option<f64>>> {
    items
        .iter()
        .enumerate()
        .map(|(idx, item)| match item {
            Value::Null if allow_nulls => Ok(None),
            Value::Number(number) => parse_explicit_f64(number, position).map(Some).map_err(|_| {
                DbError::sql_input_invalid(format!(
                    "params[{}][{}] expected numeric array element",
                    position, idx
                ))
            }),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{}][{}] expected numeric array element",
                position, idx
            ))),
        })
        .collect()
}

fn parse_string_array(
    items: &[Value],
    position: usize,
    allow_nulls: bool,
) -> DbResult<Vec<Option<String>>> {
    items
        .iter()
        .enumerate()
        .map(|(idx, item)| match item {
            Value::Null if allow_nulls => Ok(None),
            Value::String(raw) => Ok(Some(raw.clone())),
            _ => Err(DbError::sql_input_invalid(format!(
                "params[{}][{}] expected string array element",
                position, idx
            ))),
        })
        .collect()
}

fn parse_raw_bound_query_param_array(
    items: &[Value],
    position: usize,
) -> DbResult<BoundQueryParam> {
    if items.is_empty() {
        return Err(DbError::sql_input_invalid(format!(
            "params[{position}] empty arrays require an explicit typed wrapper like {{\"type\":\"int8[]\",\"value\":[]}}"
        )));
    }
    if items.iter().all(Value::is_boolean) {
        return parse_bool_array(items, position, false)
            .map(|raw| BoundQueryParam::BoolArray(Some(raw)));
    }
    if items.iter().all(Value::is_number) {
        if items.iter().all(|item| matches!(item, Value::Number(number) if number.as_i64().is_some() || number.as_u64().is_some())) {
            return parse_i64_array(items, position, false)
                .map(|raw| BoundQueryParam::Int8Array(Some(raw)));
        }
        return parse_f64_array(items, position, false)
            .map(|raw| BoundQueryParam::Float8Array(Some(raw)));
    }
    if items.iter().all(Value::is_string) {
        return parse_string_array(items, position, false)
            .map(|raw| BoundQueryParam::TextArray(Some(raw)));
    }
    if items.iter().all(|item| !item.is_null()) {
        return Ok(BoundQueryParam::JsonbArray(Some(
            items.iter().cloned().map(|item| Some(Json(item))).collect(),
        )));
    }
    Err(DbError::sql_input_invalid(format!(
        "params[{position}] mixed or null-containing arrays require an explicit typed wrapper"
    )))
}

async fn run_query_with_timeout<T, F>(limit: Option<Duration>, fut: F) -> DbResult<T>
where
    F: Future<Output = Result<T, tokio_postgres::Error>>,
{
    if let Some(limit) = limit {
        match tokio::time::timeout(limit, fut).await {
            Ok(result) => result.map_err(|err| DbError::query_failed(&err)),
            Err(_) => {
                mcp_toolkit_observability::emit_event(
                    mcp_toolkit_observability::Level::WARN,
                    "postgres_mcp.db.query_timeout",
                    &mcp_toolkit_observability::EventContext::new(),
                    &[mcp_toolkit_observability::safe_text(
                        "query_timeout_ms",
                        limit.as_millis().to_string(),
                    )],
                );
                Err(DbError::query_timeout(limit))
            }
        }
    } else {
        fut.await.map_err(|err| DbError::query_failed(&err))
    }
}

async fn apply_session_budget(
    client: &Client,
    force_read_only: bool,
    budget: QueryBudget,
) -> DbResult<()> {
    let mut statements = Vec::new();
    if force_read_only {
        statements.push("BEGIN TRANSACTION READ ONLY".to_string());
        if let Some(timeout) = budget.statement_timeout {
            statements.push(format!(
                "SET LOCAL statement_timeout = {}",
                timeout_ms_literal(timeout)
            ));
        }
        if let Some(timeout) = budget.lock_timeout {
            statements.push(format!(
                "SET LOCAL lock_timeout = {}",
                timeout_ms_literal(timeout)
            ));
        }
    } else {
        if let Some(timeout) = budget.statement_timeout {
            statements.push(format!(
                "SET statement_timeout = {}",
                timeout_ms_literal(timeout)
            ));
        }
        if let Some(timeout) = budget.lock_timeout {
            statements.push(format!(
                "SET lock_timeout = {}",
                timeout_ms_literal(timeout)
            ));
        }
    }

    if statements.is_empty() {
        return Ok(());
    }

    let budget_sql = statements.join("; ");
    run_query_with_timeout(budget.request_timeout, client.batch_execute(&budget_sql)).await
}

async fn apply_pinned_session_budget(
    client: &Client,
    force_read_only: bool,
    budget: QueryBudget,
) -> DbResult<()> {
    let mut statements = Vec::new();
    if force_read_only {
        statements.push("SET default_transaction_read_only = on".to_string());
    }
    match budget.statement_timeout {
        Some(timeout) => statements.push(format!(
            "SET statement_timeout = {}",
            timeout_ms_literal(timeout)
        )),
        None => statements.push("RESET statement_timeout".to_string()),
    }
    match budget.lock_timeout {
        Some(timeout) => statements.push(format!(
            "SET lock_timeout = {}",
            timeout_ms_literal(timeout)
        )),
        None => statements.push("RESET lock_timeout".to_string()),
    }
    let budget_sql = statements.join("; ");
    run_query_with_timeout(budget.request_timeout, client.batch_execute(&budget_sql)).await
}

async fn load_backend_pid(client: &Client, request_timeout: Option<Duration>) -> DbResult<i32> {
    let rows = run_query_with_timeout(
        request_timeout,
        client.simple_query("SELECT pg_backend_pid() AS backend_pid"),
    )
    .await?;
    for message in rows {
        if let SimpleQueryMessage::Row(row) = message
            && let Some(raw) = row.get("backend_pid")
            && let Ok(pid) = raw.parse::<i32>()
        {
            return Ok(pid);
        }
    }
    Err(DbError::session_closed(
        "failed to inspect backend pid for pinned session",
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundQueryRowFetchStrategy {
    DerivedTableWrapper,
    CommonTableExpressionWrapper,
    DirectDecode,
}

async fn prepare_typed_statement(
    client: &Client,
    sql: &str,
    params: &[BoundQueryParam],
    budget: QueryBudget,
) -> DbResult<tokio_postgres::Statement> {
    let param_types = params
        .iter()
        .map(BoundQueryParam::postgres_type)
        .collect::<Vec<_>>();
    run_query_with_timeout(
        budget.request_timeout,
        client.prepare_typed(sql, &param_types),
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundQueryLexState {
    Normal,
    SingleQuote,
    DoubleQuote,
    LineComment,
    BlockComment,
}

fn canonicalize_bound_query_sql(sql: &str) -> String {
    sql.trim().trim_end_matches(';').trim().to_string()
}

fn is_bound_query_identifier_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn top_level_bound_query_keyword_tokens(sql: &str) -> Vec<String> {
    let canonical = canonicalize_bound_query_sql(sql);
    let bytes = canonical.as_bytes();
    let mut tokens = Vec::new();
    let mut state = BoundQueryLexState::Normal;
    let mut paren_depth = 0usize;
    let mut i = 0usize;

    while i < bytes.len() {
        match state {
            BoundQueryLexState::Normal => {
                if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    state = BoundQueryLexState::LineComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    state = BoundQueryLexState::BlockComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'\'' {
                    state = BoundQueryLexState::SingleQuote;
                    i += 1;
                    continue;
                }
                if bytes[i] == b'"' {
                    state = BoundQueryLexState::DoubleQuote;
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
                    while i < bytes.len() && is_bound_query_identifier_token_byte(bytes[i]) {
                        i += 1;
                    }
                    tokens.push(canonical[start..i].to_ascii_lowercase());
                    continue;
                }
                i += 1;
            }
            BoundQueryLexState::SingleQuote => {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    state = BoundQueryLexState::Normal;
                }
                i += 1;
            }
            BoundQueryLexState::DoubleQuote => {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    state = BoundQueryLexState::Normal;
                }
                i += 1;
            }
            BoundQueryLexState::LineComment => {
                if bytes[i] == b'\n' {
                    state = BoundQueryLexState::Normal;
                }
                i += 1;
            }
            BoundQueryLexState::BlockComment => {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    state = BoundQueryLexState::Normal;
                    i += 2;
                    continue;
                }
                i += 1;
            }
        }
    }

    tokens
}

fn leading_bound_query_statement_keyword(sql: &str) -> Option<String> {
    let tokens = top_level_bound_query_keyword_tokens(sql);
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

fn bound_query_row_fetch_strategy(sql: &str) -> BoundQueryRowFetchStrategy {
    match leading_bound_query_statement_keyword(sql).as_deref() {
        Some("select" | "values" | "table") => BoundQueryRowFetchStrategy::DerivedTableWrapper,
        Some("insert" | "update" | "delete" | "merge") => {
            BoundQueryRowFetchStrategy::CommonTableExpressionWrapper
        }
        _ => BoundQueryRowFetchStrategy::DirectDecode,
    }
}

fn wrap_query_for_json_rows(sql: &str, columns: &[QueryColumn]) -> Option<String> {
    match bound_query_row_fetch_strategy(sql) {
        BoundQueryRowFetchStrategy::DerivedTableWrapper => {
            Some(wrap_select_like_query_for_json_rows(sql, columns))
        }
        BoundQueryRowFetchStrategy::CommonTableExpressionWrapper => {
            Some(wrap_data_modifying_query_for_json_rows(sql, columns))
        }
        BoundQueryRowFetchStrategy::DirectDecode => None,
    }
}

fn wrap_select_like_query_for_json_rows(sql: &str, columns: &[QueryColumn]) -> String {
    let aliases = columns
        .iter()
        .map(|column| sql_quote_ident(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let canonical_sql = canonicalize_bound_query_sql(sql);
    format!(
        "SELECT row_to_json(_postgres_mcp_row) AS __row_json FROM ({}) AS _postgres_mcp_row({aliases})",
        canonical_sql
    )
}

fn wrap_data_modifying_query_for_json_rows(sql: &str, columns: &[QueryColumn]) -> String {
    let aliases = columns
        .iter()
        .map(|column| sql_quote_ident(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let canonical_sql = canonicalize_bound_query_sql(sql);
    format!(
        "WITH _postgres_mcp_row({aliases}) AS ({canonical_sql}) SELECT row_to_json(_postgres_mcp_row) AS __row_json FROM _postgres_mcp_row"
    )
}

fn json_rows_from_wrapped_query(rows: Vec<Row>) -> DbResult<Vec<Map<String, Value>>> {
    rows.into_iter()
        .map(|row| {
            let value = row
                .try_get::<usize, Value>(0)
                .map_err(|err| DbError::query_failed(&err))?;
            match value {
                Value::Object(map) => Ok(map),
                _ => Err(DbError::sql_input_invalid(
                    "bound query row wrapper returned a non-object payload",
                )),
            }
        })
        .collect()
}

fn json_rows_from_typed_query(
    rows: Vec<Row>,
    mapped_columns: &[QueryColumn],
) -> DbResult<Vec<Map<String, Value>>> {
    rows.into_iter()
        .map(|row| typed_row_to_json_map(&row, mapped_columns))
        .collect()
}

fn typed_row_to_json_map(
    row: &Row,
    mapped_columns: &[QueryColumn],
) -> DbResult<Map<String, Value>> {
    let mut map = Map::with_capacity(mapped_columns.len());
    for (idx, column) in mapped_columns.iter().enumerate() {
        map.insert(column.name.clone(), typed_row_value_to_json(row, idx)?);
    }
    Ok(map)
}

fn typed_row_value_to_json(row: &Row, idx: usize) -> DbResult<Value> {
    let column_type = row.columns()[idx].type_();
    if *column_type == Type::BOOL {
        return row
            .try_get::<usize, Option<bool>>(idx)
            .map(|value| value.map(Value::Bool).unwrap_or(Value::Null))
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::INT2 {
        return row
            .try_get::<usize, Option<i16>>(idx)
            .map(|value| {
                value
                    .map(|raw| Value::Number(Number::from(raw)))
                    .unwrap_or(Value::Null)
            })
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::INT4 {
        return row
            .try_get::<usize, Option<i32>>(idx)
            .map(|value| {
                value
                    .map(|raw| Value::Number(Number::from(raw)))
                    .unwrap_or(Value::Null)
            })
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::INT8 {
        return row
            .try_get::<usize, Option<i64>>(idx)
            .map(|value| {
                value
                    .map(|raw| Value::Number(Number::from(raw)))
                    .unwrap_or(Value::Null)
            })
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::OID {
        return row
            .try_get::<usize, Option<u32>>(idx)
            .map(|value| {
                value
                    .map(|raw| Value::Number(Number::from(raw)))
                    .unwrap_or(Value::Null)
            })
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::FLOAT4 {
        return row
            .try_get::<usize, Option<f32>>(idx)
            .map_err(|err| DbError::query_failed(&err))
            .and_then(float_value_to_json);
    }
    if *column_type == Type::FLOAT8 {
        return row
            .try_get::<usize, Option<f64>>(idx)
            .map_err(|err| DbError::query_failed(&err))
            .and_then(float_value_to_json);
    }
    if matches!(
        *column_type,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN
    ) {
        return row
            .try_get::<usize, Option<String>>(idx)
            .map(|value| value.map(Value::String).unwrap_or(Value::Null))
            .map_err(|err| DbError::query_failed(&err));
    }
    if matches!(*column_type, Type::JSON | Type::JSONB) {
        return row
            .try_get::<usize, Option<Json<Value>>>(idx)
            .map(|value| value.map(|raw| raw.0).unwrap_or(Value::Null))
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::BOOL_ARRAY {
        return row
            .try_get::<usize, Option<Vec<Option<bool>>>>(idx)
            .map(|value| serde_json::to_value(value).unwrap_or(Value::Null))
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::INT2_ARRAY {
        return row
            .try_get::<usize, Option<Vec<Option<i16>>>>(idx)
            .map(|value| serde_json::to_value(value).unwrap_or(Value::Null))
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::INT4_ARRAY {
        return row
            .try_get::<usize, Option<Vec<Option<i32>>>>(idx)
            .map(|value| serde_json::to_value(value).unwrap_or(Value::Null))
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::INT8_ARRAY {
        return row
            .try_get::<usize, Option<Vec<Option<i64>>>>(idx)
            .map(|value| serde_json::to_value(value).unwrap_or(Value::Null))
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::FLOAT4_ARRAY {
        return row
            .try_get::<usize, Option<Vec<Option<f32>>>>(idx)
            .map(|value| serde_json::to_value(value).unwrap_or(Value::Null))
            .map_err(|err| DbError::query_failed(&err));
    }
    if *column_type == Type::FLOAT8_ARRAY {
        return row
            .try_get::<usize, Option<Vec<Option<f64>>>>(idx)
            .map(|value| serde_json::to_value(value).unwrap_or(Value::Null))
            .map_err(|err| DbError::query_failed(&err));
    }
    if matches!(
        *column_type,
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY | Type::BPCHAR_ARRAY | Type::NAME_ARRAY
    ) {
        return row
            .try_get::<usize, Option<Vec<Option<String>>>>(idx)
            .map(|value| serde_json::to_value(value).unwrap_or(Value::Null))
            .map_err(|err| DbError::query_failed(&err));
    }
    if matches!(*column_type, Type::JSON_ARRAY | Type::JSONB_ARRAY) {
        return row
            .try_get::<usize, Option<Vec<Option<Json<Value>>>>>(idx)
            .map(|value| {
                value
                    .map(|items| {
                        Value::Array(
                            items
                                .into_iter()
                                .map(|item| item.map(|raw| raw.0).unwrap_or(Value::Null))
                                .collect(),
                        )
                    })
                    .unwrap_or(Value::Null)
            })
            .map_err(|err| DbError::query_failed(&err));
    }

    Err(DbError::sql_input_invalid(format!(
        "parameterized non-select row decoding does not support PostgreSQL type {}",
        column_type.name()
    )))
}

fn float_value_to_json<T>(value: Option<T>) -> DbResult<Value>
where
    T: Into<f64>,
{
    match value {
        Some(raw) => Number::from_f64(raw.into())
            .map(Value::Number)
            .ok_or_else(|| {
                DbError::sql_input_invalid(
                    "parameterized non-select row decoding encountered a non-finite float",
                )
            }),
        None => Ok(Value::Null),
    }
}

fn extract_last_statement(sql: &str) -> Option<String> {
    let statements = split_sql_statements(sql);
    statements
        .into_iter()
        .rev()
        .find(|statement| !statement.trim().is_empty())
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut current_start = 0usize;
    let mut i = 0usize;
    let mut paren_depth = 0i32;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_dollar_quote: Option<String> = None;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while i < bytes.len() {
        if let Some(ref tag) = in_dollar_quote {
            if bytes[i..].starts_with(tag.as_bytes()) {
                i += tag.len();
                in_dollar_quote = None;
            } else {
                i += 1;
            }
            continue;
        }

        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        if in_block_comment {
            if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                in_block_comment = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }

        if in_single_quote {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_single_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        if in_double_quote {
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                } else {
                    in_double_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        if let Some(tag) = read_dollar_quote_start(bytes, i) {
            in_dollar_quote = Some(tag);
            i += in_dollar_quote.as_ref().map_or(0, |tag| tag.len());
            continue;
        }

        if bytes[i] == b'\'' {
            in_single_quote = true;
            i += 1;
            continue;
        }

        if bytes[i] == b'"' {
            in_double_quote = true;
            i += 1;
            continue;
        }

        if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            in_line_comment = true;
            i += 2;
            continue;
        }

        if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            in_block_comment = true;
            i += 2;
            continue;
        }

        if bytes[i] == b';' && paren_depth == 0 {
            let statement = sql[current_start..i].trim();
            if !statement.is_empty() {
                statements.push(statement.to_string());
            }
            current_start = i + 1;
            i += 1;
            continue;
        }

        if bytes[i] == b'(' {
            paren_depth += 1;
        } else if bytes[i] == b')' {
            paren_depth -= 1;
        }

        i += 1;
    }

    let tail = sql[current_start..].trim();
    if !tail.is_empty() {
        statements.push(tail.to_string());
    }

    statements
}

fn read_dollar_quote_start(bytes: &[u8], start: usize) -> Option<String> {
    if start >= bytes.len() || bytes[start] != b'$' {
        return None;
    }

    let mut end = start + 1;
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
        end += 1;
    }

    if end < bytes.len() && bytes[end] == b'$' {
        return std::str::from_utf8(&bytes[start..=end])
            .ok()
            .map(|tag| tag.to_string());
    }

    None
}

async fn infer_result_column_types(
    client: &Client,
    sql: &str,
    output_columns: &[QueryColumn],
) -> DbResult<Option<Vec<String>>> {
    if output_columns.is_empty() {
        return Ok(None);
    }

    let statement = extract_last_statement(sql)
        .unwrap_or_else(|| sql.to_string())
        .trim()
        .trim_end_matches(';')
        .to_string();

    if statement.is_empty() {
        return Ok(None);
    }

    let prepared = client
        .prepare(&statement)
        .await
        .map_err(|err: tokio_postgres::Error| DbError::query_failed(&err))?;

    let inferred_types = prepared
        .columns()
        .iter()
        .map(|column| column.type_().name().to_string())
        .collect::<Vec<_>>();

    if inferred_types.len() != output_columns.len() {
        return Ok(None);
    }

    Ok(Some(inferred_types))
}

fn timeout_ms_literal(timeout: Duration) -> u64 {
    let millis = timeout.as_millis();
    millis.min(i32::MAX as u128) as u64
}

fn apply_statement_timeout_override(
    base: QueryBudget,
    statement_timeout_override: Option<Duration>,
) -> QueryBudget {
    let Some(statement_timeout) = statement_timeout_override else {
        return base;
    };

    let request_timeout = match base.request_timeout {
        Some(existing) => {
            let floor = statement_timeout
                .checked_add(Duration::from_secs(1))
                .unwrap_or(statement_timeout);
            Some(existing.max(floor))
        }
        None => None,
    };

    QueryBudget {
        request_timeout,
        statement_timeout: Some(statement_timeout),
        lock_timeout: base.lock_timeout,
    }
}

fn budgeted_sql(sql: &str, force_read_only: bool, budget: QueryBudget) -> String {
    let mut parts = Vec::new();

    if force_read_only {
        parts.push("BEGIN TRANSACTION READ ONLY".to_string());
        if let Some(timeout) = budget.statement_timeout {
            parts.push(format!(
                "SET LOCAL statement_timeout = {}",
                timeout_ms_literal(timeout)
            ));
        }
        if let Some(timeout) = budget.lock_timeout {
            parts.push(format!(
                "SET LOCAL lock_timeout = {}",
                timeout_ms_literal(timeout)
            ));
        }
    } else {
        if let Some(timeout) = budget.statement_timeout {
            parts.push(format!(
                "SET statement_timeout = {}",
                timeout_ms_literal(timeout)
            ));
        }
        if let Some(timeout) = budget.lock_timeout {
            parts.push(format!(
                "SET lock_timeout = {}",
                timeout_ms_literal(timeout)
            ));
        }
    }

    parts.push(sql.to_string());
    parts.join("; ")
}

fn last_statement_output(messages: Vec<SimpleQueryMessage>) -> QueryOutput {
    let mut current_rows: Vec<Map<String, Value>> = Vec::new();
    let mut last_rows: Vec<Map<String, Value>> = Vec::new();
    let mut current_columns: Vec<QueryColumn> = Vec::new();
    let mut last_columns: Vec<QueryColumn> = Vec::new();
    let mut last_rows_affected: Option<u64> = None;

    for message in messages {
        match message {
            SimpleQueryMessage::Row(row) => {
                if current_columns.is_empty() {
                    current_columns = QueryColumn::from_row_columns(row.columns(), None);
                }
                current_rows.push(row_to_json_map(&row, &current_columns));
            }
            SimpleQueryMessage::CommandComplete(rows_affected) => {
                if !current_rows.is_empty() || !current_columns.is_empty() {
                    last_rows = current_rows;
                    last_columns = current_columns;
                }
                last_rows_affected = Some(rows_affected);
                current_rows = Vec::new();
                current_columns = Vec::new();
            }
            _ => {}
        }
    }

    if !current_rows.is_empty() || !current_columns.is_empty() {
        return QueryOutput {
            rows: current_rows,
            columns: current_columns,
            rows_affected: last_rows_affected,
        };
    }

    QueryOutput {
        rows: last_rows,
        columns: last_columns,
        rows_affected: last_rows_affected,
    }
}

fn row_to_json_map(row: &SimpleQueryRow, mapped_columns: &[QueryColumn]) -> Map<String, Value> {
    let mut map = Map::new();
    for (idx, column) in mapped_columns.iter().enumerate() {
        let key = column.name.to_string();
        let value = row.get(idx).map(parse_scalar).unwrap_or(Value::Null);
        map.insert(key, value);
    }
    map
}

fn parse_scalar(raw: &str) -> Value {
    let trimmed = raw.trim();

    if trimmed.eq_ignore_ascii_case("t") || trimmed.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("f") || trimmed.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }

    if let Ok(int_value) = trimmed.parse::<i64>() {
        return Value::Number(Number::from(int_value));
    }
    if let Ok(uint_value) = trimmed.parse::<u64>() {
        return Value::Number(Number::from(uint_value));
    }
    if let Ok(float_value) = trimmed.parse::<f64>()
        && let Some(number) = Number::from_f64(float_value)
    {
        return Value::Number(number);
    }

    if ((trimmed.starts_with('{') && trimmed.ends_with('}'))
        || (trimmed.starts_with('[') && trimmed.ends_with(']')))
        && let Ok(parsed) = serde_json::from_str::<Value>(trimmed)
    {
        return parsed;
    }

    Value::String(raw.to_string())
}

pub fn sql_quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn sql_quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub fn sql_quote_qualified_ident(value: &str) -> String {
    value
        .split('.')
        .filter(|part| !part.trim().is_empty())
        .map(sql_quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::{
        BoundQueryParam, BoundQueryParamKind, BoundQueryRowFetchStrategy, DbEngine, QueryBudget,
        apply_statement_timeout_override, bound_query_row_fetch_strategy, budgeted_sql,
        last_statement_output, parse_bound_query_params, parse_explicit_bound_query_param_kind,
        wrap_data_modifying_query_for_json_rows, wrap_query_for_json_rows,
        wrap_select_like_query_for_json_rows,
    };
    use crate::config::AccessMode;
    use serde_json::Value;
    use std::env;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_postgres::SimpleQueryMessage;

    fn default_query_budget() -> (Option<Duration>, Option<Duration>, Option<Duration>) {
        (
            Some(Duration::from_secs(15)),
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(2)),
        )
    }

    fn live_db_engine_from_env() -> Option<DbEngine> {
        let database_uri = env::var("DATABASE_URI").ok()?;
        if database_uri.trim().is_empty() {
            return None;
        }
        let (query_timeout, statement_timeout, lock_timeout) = default_query_budget();
        Some(DbEngine::new(
            Some(database_uri),
            AccessMode::Unrestricted,
            true,
            query_timeout,
            statement_timeout,
            lock_timeout,
        ))
    }

    #[tokio::test]
    async fn invalid_sslmode_is_rejected_before_connect_attempt() {
        let (query_timeout, statement_timeout, lock_timeout) = default_query_budget();
        let db = DbEngine::new(
            Some(
                "postgresql://user:pass@localhost:5432/db?sslmode=definitely-not-valid".to_string(),
            ),
            AccessMode::Unrestricted,
            false,
            query_timeout,
            statement_timeout,
            lock_timeout,
        );

        let err = db
            .execute_query_unrestricted("SELECT 1")
            .await
            .expect_err("invalid sslmode should fail fast");
        assert_eq!(err.code(), "DB_CONNECT_CONFIG_INVALID");
        assert_eq!(err.reason(), "db_connect_sslmode_invalid");
    }

    #[tokio::test]
    async fn duplicate_sslmode_is_rejected_before_connect_attempt() {
        let (query_timeout, statement_timeout, lock_timeout) = default_query_budget();
        let db = DbEngine::new(
            Some(
                "postgresql://user:pass@localhost:5432/db?sslmode=require&sslmode=disable"
                    .to_string(),
            ),
            AccessMode::Unrestricted,
            false,
            query_timeout,
            statement_timeout,
            lock_timeout,
        );

        let err = db
            .execute_query_unrestricted("SELECT 1")
            .await
            .expect_err("duplicate sslmode should fail fast");
        assert_eq!(err.code(), "DB_CONNECT_CONFIG_INVALID");
        assert_eq!(err.reason(), "db_connect_sslmode_ambiguous");
    }

    #[tokio::test]
    async fn insecure_sslmode_is_rejected_when_not_explicitly_allowed() {
        let (query_timeout, statement_timeout, lock_timeout) = default_query_budget();
        let db = DbEngine::new(
            Some("postgresql://user:pass@localhost:5432/db?sslmode=require".to_string()),
            AccessMode::Unrestricted,
            false,
            query_timeout,
            statement_timeout,
            lock_timeout,
        );

        let err = db
            .execute_query_unrestricted("SELECT 1")
            .await
            .expect_err("insecure TLS should be rejected by default");
        assert_eq!(err.code(), "DB_CONNECT_CONFIG_INVALID");
        assert_eq!(err.reason(), "db_connect_tls_policy_disallowed");
        assert!(err.message().contains("sslmode=require"));
    }

    #[tokio::test]
    async fn sslmode_prefer_is_rejected_even_with_insecure_override() {
        let (query_timeout, statement_timeout, lock_timeout) = default_query_budget();
        let db = DbEngine::new(
            Some("postgresql://user:pass@localhost:5432/db?sslmode=prefer".to_string()),
            AccessMode::Unrestricted,
            true,
            query_timeout,
            statement_timeout,
            lock_timeout,
        );

        let err = db
            .execute_query_unrestricted("SELECT 1")
            .await
            .expect_err("sslmode=prefer should always be rejected");
        assert_eq!(err.code(), "DB_CONNECT_CONFIG_INVALID");
        assert_eq!(err.reason(), "db_connect_tls_policy_disallowed");
        assert!(err.message().contains("sslmode=prefer"));
    }

    #[tokio::test]
    async fn keyword_dsn_prefer_is_rejected_even_with_insecure_override() {
        let (query_timeout, statement_timeout, lock_timeout) = default_query_budget();
        let db = DbEngine::new(
            Some(
                "host=localhost user=postgres dbname=app password=secret sslmode=prefer"
                    .to_string(),
            ),
            AccessMode::Unrestricted,
            true,
            query_timeout,
            statement_timeout,
            lock_timeout,
        );

        let err = db
            .execute_query_unrestricted("SELECT 1")
            .await
            .expect_err("keyword dsn sslmode=prefer should always be rejected");
        assert_eq!(err.code(), "DB_CONNECT_CONFIG_INVALID");
        assert_eq!(err.reason(), "db_connect_tls_policy_disallowed");
        assert!(err.message().contains("sslmode=prefer"));
    }

    #[tokio::test]
    async fn parameterized_insert_returning_works_on_live_db() {
        let Some(db) = live_db_engine_from_env() else {
            eprintln!(
                "skipping parameterized_insert_returning_works_on_live_db (DATABASE_URI not set)"
            );
            return;
        };
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after unix epoch")
            .as_nanos();
        let table_name = format!("postgres_mcp_param_returning_smoke_{suffix}");
        let create_sql =
            format!("CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, label TEXT NOT NULL)");
        db.execute_query_unrestricted(&create_sql)
            .await
            .expect("scratch table should be created");

        let insert_sql =
            format!("INSERT INTO {table_name} (id, label) VALUES ($1, $2) RETURNING id, label");
        let result = db
            .execute_user_sql_with_params_and_statement_timeout(
                &insert_sql,
                &[serde_json::json!(7), serde_json::json!("alpha")],
                None,
            )
            .await;

        let drop_sql = format!("DROP TABLE IF EXISTS {table_name}");
        db.execute_query_unrestricted(&drop_sql)
            .await
            .expect("scratch table should be removable");

        let output = result.expect("parameterized INSERT ... RETURNING should succeed");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].get("id"), Some(&serde_json::json!(7)));
        assert_eq!(
            output.rows[0].get("label"),
            Some(&serde_json::json!("alpha"))
        );
    }

    #[tokio::test]
    async fn parameterized_explain_format_json_works_on_live_db() {
        let Some(db) = live_db_engine_from_env() else {
            eprintln!(
                "skipping parameterized_explain_format_json_works_on_live_db (DATABASE_URI not set)"
            );
            return;
        };
        let output = db
            .execute_user_sql_with_params_and_statement_timeout(
                "EXPLAIN (FORMAT JSON) SELECT $1::BIGINT AS id",
                &[serde_json::json!(7)],
                None,
            )
            .await
            .expect("parameterized EXPLAIN (FORMAT JSON) should succeed");
        assert_eq!(output.rows.len(), 1);
        let plan = output.rows[0]
            .get("QUERY PLAN")
            .expect("EXPLAIN output should include QUERY PLAN");
        assert!(
            plan.is_array(),
            "EXPLAIN (FORMAT JSON) should decode to a JSON array"
        );
    }

    #[tokio::test]
    async fn parameterized_typed_arrays_preserve_null_elements_on_live_db() {
        let Some(db) = live_db_engine_from_env() else {
            eprintln!(
                "skipping parameterized_typed_arrays_preserve_null_elements_on_live_db (DATABASE_URI not set)"
            );
            return;
        };
        let output = db
            .execute_user_sql_with_params_and_statement_timeout(
                "SELECT $1::BOOL[] AS bools, $2::INT8[] AS ids, $3::FLOAT8[] AS scores, $4::TEXT[] AS states",
                &[
                    serde_json::json!({"type":"bool[]","value":[true, null, false]}),
                    serde_json::json!({"type":"int8[]","value":[1, null, 3]}),
                    serde_json::json!({"type":"float8[]","value":[1.5, null, 2.5]}),
                    serde_json::json!({"type":"text[]","value":["queued", null, "done"]}),
                ],
                None,
            )
            .await
            .expect("typed arrays with null elements should bind successfully");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("bools"),
            Some(&serde_json::json!([true, null, false]))
        );
        assert_eq!(
            output.rows[0].get("ids"),
            Some(&serde_json::json!([1, null, 3]))
        );
        assert_eq!(
            output.rows[0].get("scores"),
            Some(&serde_json::json!([1.5, null, 2.5]))
        );
        assert_eq!(
            output.rows[0].get("states"),
            Some(&serde_json::json!(["queued", null, "done"]))
        );
    }

    #[test]
    fn budgeted_sql_prepends_timeouts_for_read_only_queries() {
        let sql = budgeted_sql(
            "SELECT 1",
            true,
            QueryBudget {
                request_timeout: Some(Duration::from_secs(5)),
                statement_timeout: Some(Duration::from_secs(3)),
                lock_timeout: Some(Duration::from_millis(750)),
            },
        );
        assert!(sql.starts_with("BEGIN TRANSACTION READ ONLY;"));
        assert!(sql.contains("SET LOCAL statement_timeout = 3000"));
        assert!(sql.contains("SET LOCAL lock_timeout = 750"));
        assert!(sql.ends_with("SELECT 1"));
    }

    #[test]
    fn budgeted_sql_prepends_session_timeouts_for_unrestricted_queries() {
        let sql = budgeted_sql(
            "VACUUM",
            false,
            QueryBudget {
                request_timeout: Some(Duration::from_secs(5)),
                statement_timeout: Some(Duration::from_secs(3)),
                lock_timeout: None,
            },
        );
        assert!(sql.starts_with("SET statement_timeout = 3000;"));
        assert!(sql.ends_with("VACUUM"));
    }

    #[test]
    fn apply_statement_timeout_override_updates_statement_and_request_floor() {
        let base = QueryBudget {
            request_timeout: Some(Duration::from_secs(15)),
            statement_timeout: Some(Duration::from_secs(10)),
            lock_timeout: Some(Duration::from_millis(750)),
        };
        let overridden = apply_statement_timeout_override(base, Some(Duration::from_secs(30)));
        assert_eq!(overridden.statement_timeout, Some(Duration::from_secs(30)));
        assert_eq!(overridden.request_timeout, Some(Duration::from_secs(31)));
        assert_eq!(overridden.lock_timeout, Some(Duration::from_millis(750)));
    }

    #[test]
    fn apply_statement_timeout_override_keeps_unbounded_request_timeout() {
        let base = QueryBudget {
            request_timeout: None,
            statement_timeout: Some(Duration::from_secs(10)),
            lock_timeout: Some(Duration::from_millis(750)),
        };
        let overridden = apply_statement_timeout_override(base, Some(Duration::from_secs(30)));
        assert_eq!(overridden.request_timeout, None);
        assert_eq!(overridden.statement_timeout, Some(Duration::from_secs(30)));
        assert_eq!(overridden.lock_timeout, Some(Duration::from_millis(750)));
    }

    #[test]
    fn explicit_bound_param_type_parser_accepts_scalar_and_array_aliases() {
        assert_eq!(
            parse_explicit_bound_query_param_kind("boolean"),
            Some(BoundQueryParamKind::Bool)
        );
        assert_eq!(
            parse_explicit_bound_query_param_kind("int8[]"),
            Some(BoundQueryParamKind::Int8Array)
        );
        assert_eq!(parse_explicit_bound_query_param_kind("rows"), None);
    }

    #[test]
    fn parse_bound_query_params_supports_scalars_arrays_and_typed_nulls() {
        let parsed = parse_bound_query_params(&[
            serde_json::json!(true),
            serde_json::json!([1, 2, 3]),
            serde_json::json!({"type": "text", "value": null}),
        ])
        .expect("params should parse");
        assert!(matches!(parsed[0], BoundQueryParam::Bool(Some(true))));
        assert!(matches!(
            parsed[1],
            BoundQueryParam::Int8Array(Some(ref values))
                if values == &vec![Some(1), Some(2), Some(3)]
        ));
        assert!(matches!(parsed[2], BoundQueryParam::Text(None)));
    }

    #[test]
    fn parse_bound_query_param_preserves_raw_type_value_object_as_jsonb() {
        let parsed = parse_bound_query_params(&[serde_json::json!({
            "type": "invoice",
            "value": { "status": "open" },
        })])
        .expect("params should parse as jsonb");
        let json = match &parsed[0] {
            BoundQueryParam::Jsonb(Some(json)) => &json.0,
            other => panic!("expected jsonb param, got {other:?}"),
        };
        assert_eq!(
            *json,
            serde_json::json!({"type":"invoice","value":{"status":"open"}})
        );
    }

    #[test]
    fn parse_bound_query_param_preserves_supported_alias_type_value_object_as_jsonb() {
        let parsed = parse_bound_query_params(&[serde_json::json!({
            "type": "text",
            "value": { "status": "open" },
        })])
        .expect("supported-alias raw object should still parse as jsonb");
        let json = match &parsed[0] {
            BoundQueryParam::Jsonb(Some(json)) => &json.0,
            other => panic!("expected jsonb param, got {other:?}"),
        };
        assert_eq!(
            *json,
            serde_json::json!({"type":"text","value":{"status":"open"}})
        );
    }

    #[test]
    fn parse_bound_query_params_allow_null_elements_for_explicit_arrays() {
        let parsed = parse_bound_query_params(&[
            serde_json::json!({"type":"bool[]","value":[true, null, false]}),
            serde_json::json!({"type":"int8[]","value":[1, null, 3]}),
            serde_json::json!({"type":"float8[]","value":[1.5, null, 2.5]}),
            serde_json::json!({"type":"text[]","value":["queued", null, "done"]}),
            serde_json::json!({"type":"jsonb[]","value":[{"status":"queued"}, null, {"status":"done"}]}),
        ])
        .expect("explicit typed arrays should allow null elements");

        assert!(matches!(
            parsed[0],
            BoundQueryParam::BoolArray(Some(ref values))
                if values == &vec![Some(true), None, Some(false)]
        ));
        assert!(matches!(
            parsed[1],
            BoundQueryParam::Int8Array(Some(ref values))
                if values == &vec![Some(1), None, Some(3)]
        ));
        assert!(matches!(
            parsed[2],
            BoundQueryParam::Float8Array(Some(ref values))
                if values == &vec![Some(1.5), None, Some(2.5)]
        ));
        assert!(matches!(
            parsed[3],
            BoundQueryParam::TextArray(Some(ref values))
                if values
                    == &vec![
                        Some("queued".to_string()),
                        None,
                        Some("done".to_string())
                    ]
        ));
        let json_values = match &parsed[4] {
            BoundQueryParam::JsonbArray(Some(values)) => values
                .iter()
                .map(|item| item.as_ref().map(|json| json.0.clone()))
                .collect::<Vec<_>>(),
            other => panic!("expected jsonb[] param, got {other:?}"),
        };
        assert_eq!(
            json_values,
            vec![
                Some(serde_json::json!({"status":"queued"})),
                None,
                Some(serde_json::json!({"status":"done"}))
            ]
        );
    }

    #[test]
    fn parse_bound_query_params_rejects_raw_nulls_and_empty_arrays() {
        let null_err = parse_bound_query_params(&[Value::Null])
            .expect_err("raw null should require explicit type");
        assert!(null_err.message().contains("explicit typed wrapper"));

        let empty_array_err = parse_bound_query_params(&[serde_json::json!([])])
            .expect_err("empty array should require explicit type");
        assert!(empty_array_err.message().contains("empty arrays require"));
    }

    #[test]
    fn bound_query_row_fetch_strategy_uses_legal_wrapper_forms() {
        assert_eq!(
            bound_query_row_fetch_strategy("SELECT id FROM demo"),
            BoundQueryRowFetchStrategy::DerivedTableWrapper
        );
        assert_eq!(
            bound_query_row_fetch_strategy(
                "WITH picked AS (SELECT 1) UPDATE demo SET id = 2 RETURNING id"
            ),
            BoundQueryRowFetchStrategy::CommonTableExpressionWrapper
        );
        assert_eq!(
            bound_query_row_fetch_strategy("EXPLAIN SELECT * FROM demo WHERE id = $1"),
            BoundQueryRowFetchStrategy::DirectDecode
        );
    }

    #[test]
    fn last_statement_output_tracks_command_complete_rows_affected() {
        let output = last_statement_output(vec![SimpleQueryMessage::CommandComplete(7)]);
        assert_eq!(output.rows_affected, Some(7));
        assert!(output.rows.is_empty());
    }

    #[test]
    fn wrap_select_like_query_for_json_rows_uses_safe_alias_list() {
        let wrapped = wrap_select_like_query_for_json_rows(
            "SELECT 1 AS id, 2 AS id",
            &[
                crate::db::QueryColumn {
                    name: "id".to_string(),
                    pg_type: "int4".to_string(),
                    nullable: Some(false),
                },
                crate::db::QueryColumn {
                    name: "id__dup2".to_string(),
                    pg_type: "int4".to_string(),
                    nullable: Some(false),
                },
            ],
        );
        assert!(wrapped.contains("row_to_json"));
        assert!(wrapped.contains("_postgres_mcp_row(\"id\", \"id__dup2\")"));
    }

    #[test]
    fn wrap_data_modifying_query_for_json_rows_uses_cte_wrapper() {
        let wrapped = wrap_data_modifying_query_for_json_rows(
            "UPDATE demo SET state = 'done' WHERE id = $1 RETURNING id, state",
            &[
                crate::db::QueryColumn {
                    name: "id".to_string(),
                    pg_type: "int8".to_string(),
                    nullable: Some(false),
                },
                crate::db::QueryColumn {
                    name: "state".to_string(),
                    pg_type: "text".to_string(),
                    nullable: Some(false),
                },
            ],
        );
        assert!(wrapped.starts_with("WITH _postgres_mcp_row("));
        assert!(
            wrapped
                .contains("AS (UPDATE demo SET state = 'done' WHERE id = $1 RETURNING id, state)")
        );
        assert!(wrapped.contains(
            "SELECT row_to_json(_postgres_mcp_row) AS __row_json FROM _postgres_mcp_row"
        ));
    }

    #[test]
    fn wrap_query_for_json_rows_skips_non_wrappable_statements() {
        let columns = vec![crate::db::QueryColumn {
            name: "QUERY PLAN".to_string(),
            pg_type: "text".to_string(),
            nullable: Some(false),
        }];
        assert!(wrap_query_for_json_rows("EXPLAIN SELECT * FROM demo", &columns).is_none());
    }
}
