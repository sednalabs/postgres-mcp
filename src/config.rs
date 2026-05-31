//! # Runtime Configuration
//!
//! CLI and environment configuration for postgres-mcp.
//!
//! ## Rationale
//! Keep startup behavior explicit and predictable for low-latency stdio use.
//!
//! ## Security Boundaries
//! * Connection URI can contain secrets; callers must avoid printing raw values.

use std::env;
use std::path::Path;
use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Deserializer};

const DEFAULT_DB_QUERY_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_DB_STATEMENT_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_DB_LOCK_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_RESPONSE_PAGE_SIZE: usize = 200;
const DEFAULT_CURSOR_TTL_SEC: u64 = 900;
const DEFAULT_ADVISOR_EXTERNAL_TIMEOUT_MS: u64 = 8_000;
const DEFAULT_ADVISOR_EXTERNAL_MAX_ATTEMPTS: usize = 3;
const MAX_ADVISOR_EXTERNAL_MAX_ATTEMPTS: usize = 10;

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum AccessMode {
    Unrestricted,
    Restricted,
}

impl AccessMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unrestricted => "unrestricted",
            Self::Restricted => "restricted",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum StartupRole {
    Runtime,
    Migrator,
}

impl StartupRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Migrator => "migrator",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum MetadataPolicyMode {
    Full,
    Limited,
    Denied,
}

impl MetadataPolicyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Limited => "limited",
            Self::Denied => "denied",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum StartupDbConnectMode {
    Warn,
    FailFast,
    Background,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ResponseMode {
    /// Return canonical `v2` payload envelopes with metadata and diagnostics.
    V2,
}

impl ResponseMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V2 => "v2",
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
    ValueEnum,
    PartialEq,
    Eq,
)]
#[serde(rename_all = "lowercase")]
pub enum ResponseOutputMode {
    #[default]
    Auto,
    #[serde(alias = "table")]
    #[value(alias = "table")]
    Rows,
    #[serde(rename = "rows_safe")]
    #[value(name = "rows_safe", alias = "rows-safe")]
    RowsSafe,
    Tuples,
    Scalar,
    #[serde(rename = "data_only")]
    #[value(name = "data_only", alias = "data-only")]
    DataOnly,
}

impl ResponseOutputMode {
    const CANONICAL_MODE_LIST: &str = "auto, rows, rows_safe, tuples, scalar, data_only";

    fn legacy_compact_hint(raw: &str) -> &'static str {
        if raw.trim().eq_ignore_ascii_case("compact") {
            "; legacy compact is no longer accepted, use data_only"
        } else {
            ""
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Rows => "rows",
            Self::RowsSafe => "rows_safe",
            Self::Tuples => "tuples",
            Self::Scalar => "scalar",
            Self::DataOnly => "data_only",
        }
    }

    pub fn parse_with_alias(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "rows" | "table" | "json" => Some(Self::Rows),
            "rows_safe" | "rows-safe" => Some(Self::RowsSafe),
            "tuples" => Some(Self::Tuples),
            "scalar" => Some(Self::Scalar),
            "data_only" | "data-only" => Some(Self::DataOnly),
            _ => None,
        }
    }

    pub fn execute_sql_validation_message(raw: &str) -> String {
        format!(
            "output_mode must be one of [{}] (aliases: table -> rows, json -> rows){}; got {:?}. Example: {{\"output_mode\":\"auto\"}}",
            Self::CANONICAL_MODE_LIST,
            Self::legacy_compact_hint(raw),
            raw,
        )
    }

    pub fn env_validation_message(var_name: &str, raw: &str) -> String {
        format!(
            "invalid {var_name} value {:?} (expected auto|rows|rows_safe|tuples|scalar|data_only; aliases table->rows, json->rows{})",
            raw,
            Self::legacy_compact_hint(raw),
        )
    }
}

pub fn deserialize_optional_response_output_mode<'de, D>(
    deserializer: D,
) -> Result<Option<ResponseOutputMode>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    raw.map(|value| {
        ResponseOutputMode::parse_with_alias(&value).ok_or_else(|| {
            serde::de::Error::custom(ResponseOutputMode::execute_sql_validation_message(&value))
        })
    })
    .transpose()
}

#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
pub enum ResponseAutoTabularMode {
    #[value(alias = "table")]
    Rows,
    #[value(name = "rows_safe", alias = "rows-safe")]
    RowsSafe,
    #[default]
    Tuples,
}

impl ResponseAutoTabularMode {
    pub fn parse_with_alias(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "rows" | "table" => Some(Self::Rows),
            "rows_safe" | "rows-safe" => Some(Self::RowsSafe),
            "tuples" => Some(Self::Tuples),
            _ => None,
        }
    }

    pub fn as_output_mode(self) -> ResponseOutputMode {
        match self {
            Self::Rows => ResponseOutputMode::Rows,
            Self::RowsSafe => ResponseOutputMode::RowsSafe,
            Self::Tuples => ResponseOutputMode::Tuples,
        }
    }

    pub fn env_validation_message(var_name: &str, raw: &str) -> String {
        format!(
            "invalid {var_name} value {:?} (expected rows|rows_safe|tuples; alias table->rows)",
            raw
        )
    }
}

#[derive(Debug, Clone)]
pub struct AdvisorExternalConfig {
    pub enabled: bool,
    pub command: Option<String>,
    pub args: Vec<String>,
    pub timeout: Duration,
    pub max_attempts: usize,
    pub fallback_to_dta: bool,
}

impl AdvisorExternalConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            command: None,
            args: Vec::new(),
            timeout: Duration::from_millis(DEFAULT_ADVISOR_EXTERNAL_TIMEOUT_MS),
            max_attempts: DEFAULT_ADVISOR_EXTERNAL_MAX_ATTEMPTS,
            fallback_to_dta: true,
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "postgres-mcp")]
#[command(about = "Rust stdio MCP server for PostgreSQL")]
pub struct Cli {
    /// Database connection URL. Optional if DATABASE_URI is set.
    pub database_url: Option<String>,

    #[arg(long, value_enum, default_value = "restricted")]
    pub access_mode: AccessMode,

    /// Metadata discovery policy (`full|limited|denied`).
    #[arg(long, value_enum, default_value = "full")]
    pub metadata_policy_mode: MetadataPolicyMode,

    /// Startup role (`runtime|migrator`) for least-privilege execution.
    #[arg(long, value_enum, default_value = "runtime")]
    pub startup_role: StartupRole,

    /// Repeatable schema include entries for metadata discovery policy.
    #[arg(long = "metadata-schema-allow")]
    pub metadata_schema_allow: Vec<String>,

    /// Repeatable schema exclude entries for metadata discovery policy.
    #[arg(long = "metadata-schema-deny")]
    pub metadata_schema_deny: Vec<String>,

    #[arg(long, value_enum)]
    pub startup_db_connect: Option<StartupDbConnectMode>,

    /// Optional startup DB connection timeout in seconds (0 = no timeout).
    #[arg(long)]
    pub startup_db_connect_timeout_sec: Option<f64>,

    /// Print tool names as JSON and exit.
    #[arg(long, default_value_t = false)]
    pub print_tools: bool,

    /// Print the registered tool schema snapshot and exit.
    #[arg(long, default_value_t = false)]
    pub print_tool_schema: bool,

    /// Execute a read-only probe SQL statement and exit (benchmark helper).
    #[arg(long, hide = true)]
    pub probe_sql: Option<String>,

    /// Number of times to execute `--probe-sql` before exit.
    #[arg(long, default_value_t = 1, hide = true)]
    pub probe_repeat: u32,

    /// Maximum wall-clock time for one DB request in milliseconds (0 disables).
    #[arg(long)]
    pub db_query_timeout_ms: Option<u64>,

    /// PostgreSQL statement_timeout in milliseconds (0 disables).
    #[arg(long)]
    pub db_statement_timeout_ms: Option<u64>,

    /// PostgreSQL lock_timeout in milliseconds (0 disables).
    #[arg(long)]
    pub db_lock_timeout_ms: Option<u64>,

    /// Allow insecure TLS mode (`sslmode=require`) for DB connections.
    /// `sslmode=prefer` remains disallowed.
    /// Disabled by default; prefer `sslmode=verify-full` or `sslmode=verify-ca`.
    #[arg(long, default_value_t = false)]
    pub allow_insecure_tls: bool,

    /// Enable the mutating `admin_sql` tool in tool discovery and execution.
    #[arg(long, default_value_t = false)]
    pub enable_admin_sql: bool,

    /// Expose `execute_sql` in tool discovery. Hidden by default.
    #[arg(long, default_value_t = false)]
    pub expose_execute_sql: bool,

    /// Response payload contract version.
    #[arg(long, value_enum, default_value = "v2")]
    pub response_mode: ResponseMode,

    /// Default output representation for tabular tool payloads.
    #[arg(long, value_enum, default_value = "data_only")]
    pub response_output_mode: ResponseOutputMode,

    /// When `response_output_mode=auto`, choose this mode for non-scalar tabular output.
    #[arg(long, value_enum, default_value = "rows")]
    pub response_output_mode_auto_tabular: ResponseAutoTabularMode,

    /// Default max rows in v2 contract payloads before truncation.
    #[arg(long, default_value_t = DEFAULT_RESPONSE_PAGE_SIZE)]
    pub response_page_size: usize,

    /// Pagination cursor TTL in seconds.
    #[arg(long)]
    pub cursor_ttl_sec: Option<u64>,

    /// Optional pagination cursor signing key. Prefer env var for non-interactive usage.
    #[arg(long)]
    pub cursor_signing_key: Option<String>,

    /// Enable provider-neutral external advisor mode (disabled by default).
    #[arg(long, default_value_t = false)]
    pub advisor_external_enabled: bool,

    /// Executable used when `method=external` requests advisor recommendations.
    #[arg(long)]
    pub advisor_external_command: Option<String>,

    /// Repeatable arguments passed to the external advisor command.
    #[arg(long = "advisor-external-arg")]
    pub advisor_external_args: Vec<String>,

    /// Allow PATH-relative external advisor commands (disabled by default).
    #[arg(long, default_value_t = false)]
    pub advisor_external_allow_relative_command: bool,

    /// External advisor execution timeout in milliseconds.
    #[arg(long)]
    pub advisor_external_timeout_ms: Option<u64>,

    /// Max bounded external advisor iterations (1-10).
    #[arg(long)]
    pub advisor_external_max_attempts: Option<usize>,

    /// When true, external advisor failures fall back to deterministic `dta`.
    #[arg(long)]
    pub advisor_external_fallback_dta: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub database_url: Option<String>,
    pub access_mode: AccessMode,
    pub metadata_policy_mode: MetadataPolicyMode,
    pub metadata_schema_allow: Vec<String>,
    pub metadata_schema_deny: Vec<String>,
    pub startup_role: StartupRole,
    pub startup_db_connect: StartupDbConnectMode,
    pub startup_db_connect_timeout: Option<Duration>,
    pub allow_insecure_tls: bool,
    pub enable_admin_sql: bool,
    pub expose_execute_sql: bool,
    pub print_tools: bool,
    pub print_tool_schema: bool,
    pub probe_sql: Option<String>,
    pub probe_repeat: u32,
    pub db_query_timeout: Option<Duration>,
    pub db_statement_timeout: Option<Duration>,
    pub db_lock_timeout: Option<Duration>,
    pub response_mode: ResponseMode,
    pub response_output_mode: ResponseOutputMode,
    pub response_output_mode_auto_tabular: ResponseAutoTabularMode,
    pub response_page_size: usize,
    pub cursor_ttl: Duration,
    pub cursor_signing_key: Option<String>,
    pub advisor_external: AdvisorExternalConfig,
}

impl Settings {
    pub fn from_cli(cli: Cli) -> Result<Self> {
        let startup_db_connect = if let Some(mode) = cli.startup_db_connect {
            mode
        } else if let Ok(value) = env::var("POSTGRES_MCP_STARTUP_DB_CONNECT") {
            StartupDbConnectMode::from_str(&value, true).map_err(|err| {
                anyhow!(
                    "invalid POSTGRES_MCP_STARTUP_DB_CONNECT (expected warn|fail-fast|background): {err}"
                )
            })?
        } else {
            // Deliberately default to background to avoid mandatory DB network I/O on startup.
            StartupDbConnectMode::Background
        };

        let timeout_raw = if let Some(v) = cli.startup_db_connect_timeout_sec {
            Some(v)
        } else {
            env::var("POSTGRES_MCP_STARTUP_DB_CONNECT_TIMEOUT_SEC")
                .ok()
                .map(|v| v.parse::<f64>())
                .transpose()
                .map_err(|_| {
                    anyhow!(
                        "invalid POSTGRES_MCP_STARTUP_DB_CONNECT_TIMEOUT_SEC (expected float seconds)"
                    )
                })?
        };

        let startup_db_connect_timeout = match timeout_raw {
            Some(v) if v < 0.0 => {
                return Err(anyhow!("startup DB connect timeout must be >= 0 seconds"));
            }
            Some(0.0) => None,
            Some(v) => Some(Duration::from_secs_f64(v)),
            None => None,
        };

        let database_url = env::var("DATABASE_URI").ok().or(cli.database_url);
        let metadata_policy_mode = if let Ok(raw) =
            std::env::var("POSTGRES_MCP_METADATA_POLICY_MODE")
        {
            MetadataPolicyMode::from_str(&raw, true).map_err(|err| {
                    anyhow!("invalid POSTGRES_MCP_METADATA_POLICY_MODE (expected full|limited|denied): {err}",)
                })?
        } else {
            cli.metadata_policy_mode
        };
        let metadata_schema_allow = if !cli.metadata_schema_allow.is_empty() {
            normalize_schema_list(cli.metadata_schema_allow)
        } else {
            normalize_schema_list(parse_env_csv_string_array(
                "POSTGRES_MCP_METADATA_SCHEMA_ALLOW",
            )?)
        };
        let metadata_schema_deny = if !cli.metadata_schema_deny.is_empty() {
            normalize_schema_list(cli.metadata_schema_deny)
        } else {
            normalize_schema_list(parse_env_csv_string_array(
                "POSTGRES_MCP_METADATA_SCHEMA_DENY",
            )?)
        };
        let startup_role = if let Ok(raw) = std::env::var("POSTGRES_MCP_STARTUP_ROLE") {
            StartupRole::from_str(&raw, true).map_err(|err| {
                anyhow!("invalid POSTGRES_MCP_STARTUP_ROLE (expected runtime|migrator): {err}")
            })?
        } else {
            cli.startup_role
        };
        let response_mode = if let Ok(raw) = std::env::var("POSTGRES_MCP_RESPONSE_MODE") {
            ResponseMode::from_str(&raw, true).map_err(|err| {
                anyhow!("invalid POSTGRES_MCP_RESPONSE_MODE (expected v2): {err}",)
            })?
        } else {
            cli.response_mode
        };

        let response_output_mode =
            if let Ok(raw) = std::env::var("POSTGRES_MCP_RESPONSE_OUTPUT_MODE") {
                ResponseOutputMode::parse_with_alias(&raw).ok_or_else(|| {
                    anyhow!(ResponseOutputMode::env_validation_message(
                        "POSTGRES_MCP_RESPONSE_OUTPUT_MODE",
                        &raw,
                    ))
                })?
            } else {
                cli.response_output_mode
            };

        let response_output_mode_auto_tabular =
            if let Ok(raw) = std::env::var("POSTGRES_MCP_RESPONSE_OUTPUT_MODE_AUTO_TABULAR") {
                ResponseAutoTabularMode::parse_with_alias(&raw).ok_or_else(|| {
                    anyhow!(ResponseAutoTabularMode::env_validation_message(
                        "POSTGRES_MCP_RESPONSE_OUTPUT_MODE_AUTO_TABULAR",
                        &raw,
                    ))
                })?
            } else {
                cli.response_output_mode_auto_tabular
            };

        let response_page_size = if let Ok(raw) = std::env::var("POSTGRES_MCP_RESPONSE_PAGE_SIZE") {
            raw.parse::<usize>().map_err(|_| {
                anyhow!(
                    "invalid POSTGRES_MCP_RESPONSE_PAGE_SIZE value {:?} (expected positive integer)",
                    raw,
                )
            })?
        } else {
            cli.response_page_size
        };
        let response_page_size = response_page_size.max(1);
        let cursor_ttl_sec = resolve_positive_u64(
            cli.cursor_ttl_sec,
            "POSTGRES_MCP_CURSOR_TTL_SEC",
            DEFAULT_CURSOR_TTL_SEC,
            "cursor TTL",
        )?;
        let cursor_signing_key = cli
            .cursor_signing_key
            .or_else(|| env::var("POSTGRES_MCP_CURSOR_SIGNING_KEY").ok())
            .map(|raw| raw.trim().to_string())
            .filter(|raw| !raw.is_empty());

        let probe_repeat = cli.probe_repeat.max(1);
        let allow_insecure_tls =
            cli.allow_insecure_tls || parse_env_bool("POSTGRES_MCP_ALLOW_INSECURE_TLS", false)?;
        let enable_admin_sql =
            cli.enable_admin_sql || parse_env_bool("POSTGRES_MCP_ENABLE_ADMIN_SQL", false)?;
        let expose_execute_sql =
            cli.expose_execute_sql || parse_env_bool("POSTGRES_MCP_EXPOSE_EXECUTE_SQL", false)?;
        let db_query_timeout = resolve_timeout_ms(
            cli.db_query_timeout_ms,
            "POSTGRES_MCP_DB_QUERY_TIMEOUT_MS",
            DEFAULT_DB_QUERY_TIMEOUT_MS,
        )?;
        let db_statement_timeout = resolve_timeout_ms(
            cli.db_statement_timeout_ms,
            "POSTGRES_MCP_DB_STATEMENT_TIMEOUT_MS",
            DEFAULT_DB_STATEMENT_TIMEOUT_MS,
        )?;
        let db_lock_timeout = resolve_timeout_ms(
            cli.db_lock_timeout_ms,
            "POSTGRES_MCP_DB_LOCK_TIMEOUT_MS",
            DEFAULT_DB_LOCK_TIMEOUT_MS,
        )?;
        let advisor_external_enabled = cli.advisor_external_enabled
            || parse_env_bool("POSTGRES_MCP_ADVISOR_EXTERNAL_ENABLED", false)?;
        let advisor_external_command = cli
            .advisor_external_command
            .or_else(|| env::var("POSTGRES_MCP_ADVISOR_EXTERNAL_COMMAND").ok())
            .map(|raw| raw.trim().to_string())
            .filter(|raw| !raw.is_empty());
        let advisor_external_args = if !cli.advisor_external_args.is_empty() {
            cli.advisor_external_args
        } else {
            parse_env_json_string_array("POSTGRES_MCP_ADVISOR_EXTERNAL_ARGS_JSON")?
                .unwrap_or_default()
        };
        let advisor_external_allow_relative_command = cli.advisor_external_allow_relative_command
            || parse_env_bool(
                "POSTGRES_MCP_ADVISOR_EXTERNAL_ALLOW_RELATIVE_COMMAND",
                false,
            )?;
        let advisor_external_timeout_ms = resolve_positive_u64(
            cli.advisor_external_timeout_ms,
            "POSTGRES_MCP_ADVISOR_EXTERNAL_TIMEOUT_MS",
            DEFAULT_ADVISOR_EXTERNAL_TIMEOUT_MS,
            "external advisor timeout",
        )?;
        let advisor_external_max_attempts = resolve_bounded_usize(
            cli.advisor_external_max_attempts,
            "POSTGRES_MCP_ADVISOR_EXTERNAL_MAX_ATTEMPTS",
            DEFAULT_ADVISOR_EXTERNAL_MAX_ATTEMPTS,
            1,
            MAX_ADVISOR_EXTERNAL_MAX_ATTEMPTS,
            "external advisor max attempts",
        )?;
        let advisor_external_fallback_dta = if let Some(value) = cli.advisor_external_fallback_dta {
            value
        } else {
            parse_env_bool("POSTGRES_MCP_ADVISOR_EXTERNAL_FALLBACK_DTA", true)?
        };
        if advisor_external_enabled && advisor_external_command.is_none() {
            return Err(anyhow!(
                "advisor external mode is enabled but no command is configured (set --advisor-external-command or POSTGRES_MCP_ADVISOR_EXTERNAL_COMMAND)"
            ));
        }
        if advisor_external_enabled
            && !advisor_external_allow_relative_command
            && advisor_external_command
                .as_deref()
                .is_some_and(|cmd| !Path::new(cmd).is_absolute())
        {
            return Err(anyhow!(
                "advisor external command must be an absolute path unless relative commands are explicitly allowed (--advisor-external-allow-relative-command or POSTGRES_MCP_ADVISOR_EXTERNAL_ALLOW_RELATIVE_COMMAND=1)"
            ));
        }
        let advisor_external = AdvisorExternalConfig {
            enabled: advisor_external_enabled,
            command: advisor_external_command,
            args: advisor_external_args,
            timeout: Duration::from_millis(advisor_external_timeout_ms),
            max_attempts: advisor_external_max_attempts,
            fallback_to_dta: advisor_external_fallback_dta,
        };

        Ok(Self {
            database_url,
            access_mode: cli.access_mode,
            metadata_policy_mode,
            metadata_schema_allow,
            metadata_schema_deny,
            startup_role,
            startup_db_connect,
            startup_db_connect_timeout,
            allow_insecure_tls,
            enable_admin_sql,
            expose_execute_sql,
            print_tools: cli.print_tools,
            print_tool_schema: cli.print_tool_schema,
            probe_sql: cli.probe_sql,
            probe_repeat,
            db_query_timeout,
            db_statement_timeout,
            db_lock_timeout,
            response_mode,
            response_output_mode,
            response_output_mode_auto_tabular,
            response_page_size,
            cursor_ttl: Duration::from_secs(cursor_ttl_sec),
            cursor_signing_key,
            advisor_external,
        })
    }
}

fn resolve_timeout_ms(
    cli_value: Option<u64>,
    env_name: &str,
    default_ms: u64,
) -> Result<Option<Duration>> {
    let value = if let Some(value) = cli_value {
        value
    } else if let Ok(raw) = env::var(env_name) {
        raw.parse::<u64>().map_err(|_| {
            anyhow!(
                "invalid {env_name} value {:?} (expected non-negative integer milliseconds)",
                raw
            )
        })?
    } else {
        default_ms
    };

    if value == 0 {
        return Ok(None);
    }
    Ok(Some(Duration::from_millis(value)))
}

fn parse_env_bool(name: &str, default: bool) -> Result<bool> {
    let raw = match env::var(name) {
        Ok(value) => value,
        Err(_) => return Ok(default),
    };

    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(anyhow!(
            "invalid {name} value {:?} (expected 1|0|true|false|yes|no|on|off)",
            raw
        )),
    }
}

fn parse_env_json_string_array(name: &str) -> Result<Option<Vec<String>>> {
    let raw = match env::var(name) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let parsed: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| anyhow!("invalid {name} (expected JSON array of strings): {err}"))?;
    let items = parsed
        .as_array()
        .ok_or_else(|| anyhow!("invalid {name} (expected JSON array of strings)"))?;
    let mut values = Vec::with_capacity(items.len());
    for (idx, item) in items.iter().enumerate() {
        let value = item
            .as_str()
            .ok_or_else(|| anyhow!("invalid {name}[{}] (expected string)", idx))?;
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            values.push(trimmed.to_string());
        }
    }
    Ok(Some(values))
}

fn parse_env_csv_string_array(name: &str) -> Result<Vec<String>> {
    let raw = match env::var(name) {
        Ok(value) => value,
        Err(_) => return Ok(Vec::new()),
    };
    Ok(raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect())
}

fn normalize_schema_list(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect()
}

fn resolve_positive_u64(
    cli_value: Option<u64>,
    env_name: &str,
    default: u64,
    label: &str,
) -> Result<u64> {
    let value = if let Some(value) = cli_value {
        value
    } else if let Ok(raw) = env::var(env_name) {
        raw.parse::<u64>().map_err(|_| {
            anyhow!(
                "invalid {env_name} value {:?} (expected positive integer milliseconds)",
                raw
            )
        })?
    } else {
        default
    };
    if value == 0 {
        return Err(anyhow!("{label} must be > 0"));
    }
    Ok(value)
}

fn resolve_bounded_usize(
    cli_value: Option<usize>,
    env_name: &str,
    default: usize,
    min: usize,
    max: usize,
    label: &str,
) -> Result<usize> {
    let value = if let Some(value) = cli_value {
        value
    } else if let Ok(raw) = env::var(env_name) {
        raw.parse::<usize>().map_err(|_| {
            anyhow!(
                "invalid {env_name} value {:?} (expected integer in range {}..={})",
                raw,
                min,
                max
            )
        })?
    } else {
        default
    };
    if value < min || value > max {
        return Err(anyhow!("{label} must be in range {min}..={max}"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{
        AccessMode, Cli, ResponseAutoTabularMode, ResponseMode, ResponseOutputMode, Settings,
        parse_env_bool, parse_env_json_string_array,
    };
    use clap::Parser;
    use std::env;
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    #[cfg(test)]
    fn set_env_var<K: AsRef<std::ffi::OsStr>, V: AsRef<std::ffi::OsStr>>(key: K, value: V) {
        unsafe {
            env::set_var(key, value);
        }
    }

    #[cfg(test)]
    fn remove_env_var<K: AsRef<std::ffi::OsStr>>(key: K) {
        unsafe {
            env::remove_var(key);
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn parse_env_bool_defaults_when_absent() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_PARSE_BOOL_TEST_DEFAULT";
        remove_env_var(key);
        let parsed = parse_env_bool(key, true).expect("default parse should succeed");
        assert!(parsed);
    }

    #[test]
    fn parse_env_bool_accepts_truthy_and_falsy_values() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_PARSE_BOOL_TEST_VALUES";

        set_env_var(key, "yes");
        assert!(parse_env_bool(key, false).expect("yes should parse true"));

        set_env_var(key, "0");
        assert!(!parse_env_bool(key, true).expect("0 should parse false"));

        remove_env_var(key);
    }

    #[test]
    fn parse_env_bool_rejects_invalid_value() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_PARSE_BOOL_TEST_INVALID";

        set_env_var(key, "maybe");
        let err = parse_env_bool(key, false).expect_err("invalid value should fail");
        assert!(
            err.to_string()
                .contains("invalid POSTGRES_MCP_PARSE_BOOL_TEST_INVALID value")
        );

        remove_env_var(key);
    }

    #[test]
    fn cli_defaults_response_page_size_to_200() {
        let cli = Cli::parse_from(["postgres-mcp"]);
        assert_eq!(cli.response_page_size, 200);
    }

    #[test]
    fn cli_defaults_access_mode_to_restricted() {
        let cli = Cli::parse_from(["postgres-mcp"]);
        assert_eq!(cli.access_mode, AccessMode::Restricted);
    }

    #[test]
    fn cli_defaults_admin_sql_to_disabled() {
        let cli = Cli::parse_from(["postgres-mcp"]);
        assert!(!cli.enable_admin_sql);
    }

    #[test]
    fn cli_defaults_execute_sql_exposure_to_disabled() {
        let cli = Cli::parse_from(["postgres-mcp"]);
        assert!(!cli.expose_execute_sql);
    }

    #[test]
    fn settings_defaults_db_query_timeout_to_300000_ms() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_DB_QUERY_TIMEOUT_MS";
        let original = env::var_os(key);
        remove_env_var(key);

        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("settings defaults should parse");
        assert_eq!(
            settings.db_query_timeout,
            Some(Duration::from_millis(300_000))
        );

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_accepts_db_query_timeout_from_env() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_DB_QUERY_TIMEOUT_MS";
        let original = env::var_os(key);
        set_env_var(key, "42000");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("db query timeout env should parse");
        assert_eq!(
            settings.db_query_timeout,
            Some(Duration::from_millis(42_000))
        );

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_accepts_enable_admin_sql_from_env() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_ENABLE_ADMIN_SQL";
        let original = env::var_os(key);
        set_env_var(key, "1");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("admin_sql env should parse");
        assert!(settings.enable_admin_sql);

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_accepts_expose_execute_sql_from_env() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_EXPOSE_EXECUTE_SQL";
        let original = env::var_os(key);
        set_env_var(key, "1");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("execute_sql exposure env should parse");
        assert!(settings.expose_execute_sql);

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_defaults_cursor_ttl_to_900_seconds() {
        let _guard = env_lock().lock().expect("env lock");
        remove_env_var("POSTGRES_MCP_CURSOR_TTL_SEC");
        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("defaults should parse");
        assert_eq!(settings.cursor_ttl, Duration::from_secs(900));
    }

    #[test]
    fn settings_accepts_cursor_ttl_from_env() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_CURSOR_TTL_SEC";
        let original = env::var_os(key);
        set_env_var(key, "120");
        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("cursor ttl env should parse");
        assert_eq!(settings.cursor_ttl, Duration::from_secs(120));
        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_rejects_zero_cursor_ttl() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_CURSOR_TTL_SEC";
        let original = env::var_os(key);
        set_env_var(key, "0");
        let cli = Cli::parse_from(["postgres-mcp"]);
        let err = Settings::from_cli(cli).expect_err("cursor ttl must be positive");
        assert!(err.to_string().contains("cursor TTL must be > 0"));
        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn cli_defaults_response_mode_to_v2() {
        let cli = Cli::parse_from(["postgres-mcp"]);
        assert_eq!(cli.response_mode, ResponseMode::V2);
    }

    #[test]
    fn cli_defaults_response_output_mode_to_data_only() {
        let cli = Cli::parse_from(["postgres-mcp"]);
        assert_eq!(cli.response_output_mode, ResponseOutputMode::DataOnly);
    }

    #[test]
    fn cli_defaults_response_output_mode_auto_tabular_to_rows() {
        let cli = Cli::parse_from(["postgres-mcp"]);
        assert_eq!(
            cli.response_output_mode_auto_tabular,
            ResponseAutoTabularMode::Rows
        );
    }

    #[test]
    fn response_output_mode_parse_accepts_table_alias() {
        assert_eq!(
            ResponseOutputMode::parse_with_alias("table"),
            Some(ResponseOutputMode::Rows)
        );
    }

    #[test]
    fn response_output_mode_parse_accepts_auto() {
        assert_eq!(
            ResponseOutputMode::parse_with_alias("auto"),
            Some(ResponseOutputMode::Auto)
        );
    }

    #[test]
    fn response_output_mode_parse_accepts_rows_safe() {
        assert_eq!(
            ResponseOutputMode::parse_with_alias("rows_safe"),
            Some(ResponseOutputMode::RowsSafe)
        );
        assert_eq!(
            ResponseOutputMode::parse_with_alias("rows-safe"),
            Some(ResponseOutputMode::RowsSafe)
        );
    }

    #[test]
    fn response_auto_tabular_parse_accepts_table_alias() {
        assert_eq!(
            ResponseAutoTabularMode::parse_with_alias("table"),
            Some(ResponseAutoTabularMode::Rows)
        );
    }

    #[test]
    fn response_auto_tabular_parse_accepts_rows_safe() {
        assert_eq!(
            ResponseAutoTabularMode::parse_with_alias("rows_safe"),
            Some(ResponseAutoTabularMode::RowsSafe)
        );
        assert_eq!(
            ResponseAutoTabularMode::parse_with_alias("rows-safe"),
            Some(ResponseAutoTabularMode::RowsSafe)
        );
    }

    #[test]
    fn cli_accepts_table_alias_for_response_output_mode() {
        let cli = Cli::parse_from(["postgres-mcp", "--response-output-mode", "table"]);
        assert_eq!(cli.response_output_mode, ResponseOutputMode::Rows);
    }

    #[test]
    fn settings_accepts_env_table_alias_for_response_output_mode() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_RESPONSE_OUTPUT_MODE";
        let original = env::var_os(key);
        set_env_var(key, "table");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("settings should parse table alias");
        assert_eq!(settings.response_output_mode, ResponseOutputMode::Rows);

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_accepts_env_auto_for_response_output_mode() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_RESPONSE_OUTPUT_MODE";
        let original = env::var_os(key);
        set_env_var(key, "auto");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("settings should parse auto output mode");
        assert_eq!(settings.response_output_mode, ResponseOutputMode::Auto);

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_accepts_env_table_alias_for_auto_tabular_mode() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_RESPONSE_OUTPUT_MODE_AUTO_TABULAR";
        let original = env::var_os(key);
        set_env_var(key, "table");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("settings should parse table alias");
        assert_eq!(
            settings.response_output_mode_auto_tabular,
            ResponseAutoTabularMode::Rows
        );

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_accepts_env_rows_safe_for_auto_tabular_mode() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_RESPONSE_OUTPUT_MODE_AUTO_TABULAR";
        let original = env::var_os(key);
        set_env_var(key, "rows_safe");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("settings should parse rows_safe");
        assert_eq!(
            settings.response_output_mode_auto_tabular,
            ResponseAutoTabularMode::RowsSafe
        );

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_rejects_invalid_env_response_output_mode_with_alias_hint() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_RESPONSE_OUTPUT_MODE";
        let original = env::var_os(key);
        set_env_var(key, "grid");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let err = Settings::from_cli(cli).expect_err("invalid output mode should fail");
        assert!(err.to_string().contains(
            "expected auto|rows|rows_safe|tuples|scalar|data_only; aliases table->rows, json->rows"
        ));

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_rejects_legacy_compact_env_response_output_mode_with_data_only_hint() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_RESPONSE_OUTPUT_MODE";
        let original = env::var_os(key);
        set_env_var(key, "compact");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let err = Settings::from_cli(cli).expect_err("legacy compact mode should fail");
        assert!(err.to_string().contains("use data_only"));

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn settings_rejects_invalid_env_auto_tabular_mode_with_alias_hint() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_RESPONSE_OUTPUT_MODE_AUTO_TABULAR";
        let original = env::var_os(key);
        set_env_var(key, "compact");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let err = Settings::from_cli(cli).expect_err("invalid auto tabular mode should fail");
        assert!(
            err.to_string()
                .contains("expected rows|rows_safe|tuples; alias table->rows")
        );

        if let Some(value) = original {
            set_env_var(key, value);
        } else {
            remove_env_var(key);
        }
    }

    #[test]
    fn advisor_external_defaults_are_safe_and_disabled() {
        let _guard = env_lock().lock().expect("env lock");
        remove_env_var("POSTGRES_MCP_ADVISOR_EXTERNAL_ENABLED");
        remove_env_var("POSTGRES_MCP_ADVISOR_EXTERNAL_COMMAND");
        remove_env_var("POSTGRES_MCP_ADVISOR_EXTERNAL_ARGS_JSON");
        remove_env_var("POSTGRES_MCP_ADVISOR_EXTERNAL_TIMEOUT_MS");
        remove_env_var("POSTGRES_MCP_ADVISOR_EXTERNAL_MAX_ATTEMPTS");
        remove_env_var("POSTGRES_MCP_ADVISOR_EXTERNAL_FALLBACK_DTA");
        remove_env_var("POSTGRES_MCP_ADVISOR_EXTERNAL_ALLOW_RELATIVE_COMMAND");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("defaults should parse");
        assert!(!settings.advisor_external.enabled);
        assert!(settings.advisor_external.command.is_none());
        assert!(settings.advisor_external.args.is_empty());
        assert!(settings.advisor_external.fallback_to_dta);
        assert_eq!(settings.advisor_external.max_attempts, 3);
    }

    #[test]
    fn advisor_external_enabled_requires_command() {
        let _guard = env_lock().lock().expect("env lock");
        remove_env_var("POSTGRES_MCP_ADVISOR_EXTERNAL_COMMAND");
        let cli = Cli::parse_from(["postgres-mcp", "--advisor-external-enabled"]);
        let err = Settings::from_cli(cli).expect_err("enabled without command should fail");
        assert!(err.to_string().contains("no command is configured"));
    }

    #[test]
    fn advisor_external_accepts_env_json_args() {
        let _guard = env_lock().lock().expect("env lock");
        let command_key = "POSTGRES_MCP_ADVISOR_EXTERNAL_COMMAND";
        let args_key = "POSTGRES_MCP_ADVISOR_EXTERNAL_ARGS_JSON";
        let enabled_key = "POSTGRES_MCP_ADVISOR_EXTERNAL_ENABLED";
        set_env_var(command_key, "/usr/bin/fake-ext");
        set_env_var(args_key, "[\"--mode\",\"safe\",\" \",\"--limit\",\"5\"]");
        set_env_var(enabled_key, "1");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let settings = Settings::from_cli(cli).expect("env JSON args should parse");
        assert_eq!(
            settings.advisor_external.command.as_deref(),
            Some("/usr/bin/fake-ext")
        );
        assert_eq!(
            settings.advisor_external.args,
            vec!["--mode", "safe", "--limit", "5"]
        );

        remove_env_var(command_key);
        remove_env_var(args_key);
        remove_env_var(enabled_key);
    }

    #[test]
    fn advisor_external_rejects_invalid_env_json_args() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_ADVISOR_EXTERNAL_ARGS_JSON";
        set_env_var(key, "{\"not\":\"an-array\"}");
        let err = parse_env_json_string_array(key).expect_err("invalid json array should fail");
        assert!(err.to_string().contains("expected JSON array of strings"));
        remove_env_var(key);
    }

    #[test]
    fn advisor_external_rejects_out_of_range_attempts() {
        let _guard = env_lock().lock().expect("env lock");
        let attempts_key = "POSTGRES_MCP_ADVISOR_EXTERNAL_MAX_ATTEMPTS";
        let command_key = "POSTGRES_MCP_ADVISOR_EXTERNAL_COMMAND";
        let enabled_key = "POSTGRES_MCP_ADVISOR_EXTERNAL_ENABLED";
        set_env_var(command_key, "/usr/bin/fake-ext");
        set_env_var(enabled_key, "1");
        set_env_var(attempts_key, "99");

        let cli = Cli::parse_from(["postgres-mcp"]);
        let err = Settings::from_cli(cli).expect_err("invalid attempts should fail");
        assert!(err.to_string().contains("external advisor max attempts"));

        remove_env_var(attempts_key);
        remove_env_var(command_key);
        remove_env_var(enabled_key);
    }

    #[test]
    fn advisor_external_rejects_relative_command_by_default() {
        let _guard = env_lock().lock().expect("env lock");
        let cli = Cli::parse_from([
            "postgres-mcp",
            "--advisor-external-enabled",
            "--advisor-external-command",
            "fake-advisor",
        ]);
        let err = Settings::from_cli(cli).expect_err("relative command should fail by default");
        assert!(err.to_string().contains("must be an absolute path"));
    }

    #[test]
    fn advisor_external_allows_relative_command_when_explicitly_enabled() {
        let _guard = env_lock().lock().expect("env lock");
        let cli = Cli::parse_from([
            "postgres-mcp",
            "--advisor-external-enabled",
            "--advisor-external-command",
            "fake-advisor",
            "--advisor-external-allow-relative-command",
        ]);
        let settings =
            Settings::from_cli(cli).expect("explicit relative command allowance should pass");
        assert_eq!(
            settings.advisor_external.command.as_deref(),
            Some("fake-advisor")
        );
    }

    #[test]
    fn advisor_external_accepts_absolute_command_without_override() {
        let _guard = env_lock().lock().expect("env lock");
        let cli = Cli::parse_from([
            "postgres-mcp",
            "--advisor-external-enabled",
            "--advisor-external-command",
            "/usr/bin/fake-advisor",
        ]);
        let settings =
            Settings::from_cli(cli).expect("absolute command should be accepted by default");
        assert_eq!(
            settings.advisor_external.command.as_deref(),
            Some("/usr/bin/fake-advisor")
        );
    }

    #[test]
    fn advisor_external_cli_command_overrides_env_command() {
        let _guard = env_lock().lock().expect("env lock");
        let key = "POSTGRES_MCP_ADVISOR_EXTERNAL_COMMAND";
        set_env_var(key, "/usr/bin/from-env");
        let cli = Cli::parse_from([
            "postgres-mcp",
            "--advisor-external-enabled",
            "--advisor-external-command",
            "/usr/bin/from-cli",
        ]);
        let settings = Settings::from_cli(cli).expect("cli command should override env command");
        assert_eq!(
            settings.advisor_external.command.as_deref(),
            Some("/usr/bin/from-cli")
        );
        remove_env_var(key);
    }
}
