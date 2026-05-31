use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use mcp_toolkit_policy_runtime::CapabilityRefreshState;
use mcp_toolkit_postgres::{PgConnectionConfig, PgInsecureTlsPolicy};
use postgres_mcp::config::AccessMode;
use postgres_mcp::db::{DbEngine, QueryOutput};
use postgres_mcp::server::{ExtensionCapability, ExtensionUnavailableCache, PostgresMcp};
use serde::Serialize;
use serde_json::{Value, json};

const ONLINE_CHECK_NAMES: &[&str] = &[
    "read_only_transaction_and_local_timeouts",
    "request_timeout_enforced",
    "statement_timeout_enforced",
    "lock_timeout_enforced",
];
const ADVISORY_LOCK_KEY: i64 = 874_221_119;

#[derive(Parser, Debug)]
#[command(name = "runtime-safety-probe")]
#[command(about = "Verify runtime safety invariants for postgres-mcp")]
struct Args {
    /// Optional database URI for online runtime checks.
    #[arg(long)]
    database_uri: Option<String>,

    /// Require online runtime checks (fail if DATABASE_URI is not provided).
    #[arg(long, default_value_t = false)]
    require_db_runtime: bool,

    /// Path to write deterministic runtime safety report JSON.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct CheckResult {
    name: String,
    status: String,
    details: Value,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeSafetyReport {
    database_uri_configured: bool,
    require_db_runtime: bool,
    checks: Vec<CheckResult>,
    failed_checks: usize,
    pass: bool,
}

fn pass(name: &str, details: Value) -> CheckResult {
    CheckResult {
        name: name.to_string(),
        status: "pass".to_string(),
        details,
    }
}

fn fail(name: &str, details: Value) -> CheckResult {
    CheckResult {
        name: name.to_string(),
        status: "fail".to_string(),
        details,
    }
}

fn skip(name: &str, details: Value) -> CheckResult {
    CheckResult {
        name: name.to_string(),
        status: "skip".to_string(),
        details,
    }
}

fn refresh_state_name(state: CapabilityRefreshState) -> &'static str {
    match state {
        CapabilityRefreshState::FreshSuccess => "fresh_success",
        CapabilityRefreshState::StartRefresh => "start_refresh",
        CapabilityRefreshState::RefreshInFlight => "refresh_in_flight",
    }
}

fn first_row_field_as_string(output: &QueryOutput, key: &str) -> Option<String> {
    let row = output.rows.first()?;
    let value = row.get(key)?;
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(if *b { "true" } else { "false" }.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn offline_db_engine() -> DbEngine {
    DbEngine::new(
        None,
        AccessMode::Restricted,
        false,
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_millis(500)),
    )
}

async fn check_restricted_write_rejected_without_database_uri() -> CheckResult {
    let db = offline_db_engine();

    match db.execute_user_sql("INSERT INTO t VALUES (1)").await {
        Err(err)
            if err.code() == "SQL_POLICY_REJECTED"
                && err.reason() == "restricted_sql"
                && err.message().contains("NOT_READ_ONLY_PREFIX") =>
        {
            pass(
                "restricted_write_rejected_without_database_uri",
                json!({
                    "code": err.code(),
                    "reason": err.reason(),
                }),
            )
        }
        Err(err) => fail(
            "restricted_write_rejected_without_database_uri",
            json!({
                "unexpected_code": err.code(),
                "unexpected_reason": err.reason(),
                "message": err.message(),
            }),
        ),
        Ok(_) => fail(
            "restricted_write_rejected_without_database_uri",
            json!({ "error": "write SQL unexpectedly allowed" }),
        ),
    }
}

async fn check_restricted_read_reaches_database_uri_requirement() -> CheckResult {
    let db = offline_db_engine();

    match db.execute_user_sql("SELECT 1").await {
        Err(err)
            if err.code() == "DATABASE_URI_NOT_CONFIGURED"
                && err.reason() == "database_uri_not_configured" =>
        {
            pass(
                "restricted_read_reaches_database_uri_requirement",
                json!({
                    "code": err.code(),
                    "reason": err.reason(),
                }),
            )
        }
        Err(err) => fail(
            "restricted_read_reaches_database_uri_requirement",
            json!({
                "unexpected_code": err.code(),
                "unexpected_reason": err.reason(),
                "message": err.message(),
            }),
        ),
        Ok(_) => fail(
            "restricted_read_reaches_database_uri_requirement",
            json!({ "error": "expected missing DATABASE_URI error" }),
        ),
    }
}

fn check_extension_capability_names_stable() -> CheckResult {
    let hypopg = ExtensionCapability::Hypopg.extension_name();
    let pg_stat = ExtensionCapability::PgStatStatements.extension_name();
    if hypopg == "hypopg" && pg_stat == "pg_stat_statements" {
        return pass(
            "extension_capability_names_stable",
            json!({
                "hypopg": hypopg,
                "pg_stat_statements": pg_stat,
            }),
        );
    }
    fail(
        "extension_capability_names_stable",
        json!({
            "observed": {
                "hypopg": hypopg,
                "pg_stat_statements": pg_stat,
            },
            "expected": {
                "hypopg": "hypopg",
                "pg_stat_statements": "pg_stat_statements",
            }
        }),
    )
}

async fn check_extension_guard_singleflight_contract() -> CheckResult {
    let server = PostgresMcp::new(Arc::new(offline_db_engine()));
    let capability = ExtensionCapability::Hypopg;

    let first = server.extension_guard.begin_refresh(capability);
    let second = server.extension_guard.begin_refresh(capability);
    let complete = server.extension_guard.complete_refresh(capability, true);
    let cached_after_complete = server.extension_guard.has_fresh_success(&capability);
    let invalidate = server.extension_guard.invalidate(&capability);
    let cached_after_invalidate = server.extension_guard.has_fresh_success(&capability);

    let ok = matches!(first.as_ref(), Ok(CapabilityRefreshState::StartRefresh))
        && matches!(second.as_ref(), Ok(CapabilityRefreshState::RefreshInFlight))
        && complete.is_ok()
        && matches!(cached_after_complete.as_ref(), Ok(true))
        && invalidate.is_ok()
        && matches!(cached_after_invalidate.as_ref(), Ok(false));

    let details = json!({
        "first_begin_refresh": match first.as_ref() {
            Ok(state) => refresh_state_name(*state).to_string(),
            Err(err) => format!("error:{}:{}", err.code, err.reason),
        },
        "second_begin_refresh": match second.as_ref() {
            Ok(state) => refresh_state_name(*state).to_string(),
            Err(err) => format!("error:{}:{}", err.code, err.reason),
        },
        "complete_refresh": complete.is_ok(),
        "cached_after_complete": cached_after_complete.as_ref().ok().copied(),
        "invalidate": invalidate.is_ok(),
        "cached_after_invalidate": cached_after_invalidate.as_ref().ok().copied(),
    });

    if ok {
        pass("extension_guard_singleflight_contract", details)
    } else {
        fail("extension_guard_singleflight_contract", details)
    }
}

async fn check_extension_unavailable_cache_contract() -> CheckResult {
    let cache = ExtensionUnavailableCache::new(Duration::from_millis(25), 4);
    let capability = ExtensionCapability::PgStatStatements;

    let initial = cache.get_fresh(&capability);
    cache.record(
        capability,
        "extension_not_installed",
        "extension unavailable for runtime check".to_string(),
    );
    let after_record = cache.get_fresh(&capability);
    tokio::time::sleep(Duration::from_millis(40)).await;
    let after_ttl = cache.get_fresh(&capability);
    cache.record(
        capability,
        "extension_not_installed",
        "extension unavailable for runtime check".to_string(),
    );
    cache.clear(&capability);
    let after_clear = cache.get_fresh(&capability);

    let record_reason_ok = after_record
        .as_ref()
        .map(|entry| entry.guard_reason == "extension_not_installed")
        .unwrap_or(false);
    let record_message_ok = after_record
        .as_ref()
        .map(|entry| entry.message == "extension unavailable for runtime check")
        .unwrap_or(false);

    let ok = initial.is_none()
        && record_reason_ok
        && record_message_ok
        && after_ttl.is_none()
        && after_clear.is_none();

    let details = json!({
        "initial": initial.as_ref().map(|entry| {
            json!({
                "guard_reason": entry.guard_reason,
                "message": entry.message,
            })
        }),
        "after_record": after_record.as_ref().map(|entry| {
            json!({
                "guard_reason": entry.guard_reason,
                "message": entry.message,
            })
        }),
        "after_ttl_expiry": after_ttl.as_ref().map(|entry| {
            json!({
                "guard_reason": entry.guard_reason,
                "message": entry.message,
            })
        }),
        "after_clear": after_clear.as_ref().map(|entry| {
            json!({
                "guard_reason": entry.guard_reason,
                "message": entry.message,
            })
        }),
    });

    if ok {
        pass("extension_unavailable_cache_contract", details)
    } else {
        fail("extension_unavailable_cache_contract", details)
    }
}

async fn check_read_only_transaction_and_local_timeouts(database_uri: &str) -> CheckResult {
    let db = DbEngine::new(
        Some(database_uri.to_string()),
        AccessMode::Restricted,
        false,
        Some(Duration::from_secs(2)),
        Some(Duration::from_millis(50)),
        Some(Duration::from_millis(25)),
    );

    let sql = "SELECT current_setting('transaction_read_only') AS read_only_flag, current_setting('statement_timeout') AS statement_timeout_setting, current_setting('lock_timeout') AS lock_timeout_setting";

    match db.execute_query_readonly(sql).await {
        Ok(output) => {
            let read_only = first_row_field_as_string(&output, "read_only_flag");
            let statement_timeout = first_row_field_as_string(&output, "statement_timeout_setting");
            let lock_timeout = first_row_field_as_string(&output, "lock_timeout_setting");

            let ok = read_only.as_deref() == Some("on")
                && statement_timeout.as_deref() == Some("50ms")
                && lock_timeout.as_deref() == Some("25ms");

            if ok {
                pass(
                    "read_only_transaction_and_local_timeouts",
                    json!({
                        "transaction_read_only": read_only,
                        "statement_timeout": statement_timeout,
                        "lock_timeout": lock_timeout,
                    }),
                )
            } else {
                fail(
                    "read_only_transaction_and_local_timeouts",
                    json!({
                        "transaction_read_only": read_only,
                        "statement_timeout": statement_timeout,
                        "lock_timeout": lock_timeout,
                        "expected": {
                            "transaction_read_only": "on",
                            "statement_timeout": "50ms",
                            "lock_timeout": "25ms"
                        }
                    }),
                )
            }
        }
        Err(err) => fail(
            "read_only_transaction_and_local_timeouts",
            json!({
                "code": err.code(),
                "reason": err.reason(),
                "sqlstate": err.sqlstate(),
                "message": err.message(),
            }),
        ),
    }
}

async fn check_request_timeout_enforced(database_uri: &str) -> CheckResult {
    let db = DbEngine::new(
        Some(database_uri.to_string()),
        AccessMode::Unrestricted,
        false,
        Some(Duration::from_millis(75)),
        None,
        None,
    );

    match db.execute_query_readonly("SELECT pg_sleep(0.3)").await {
        Err(err) if err.code() == "DB_QUERY_TIMEOUT" && err.reason() == "db_query_timeout" => pass(
            "request_timeout_enforced",
            json!({
                "code": err.code(),
                "reason": err.reason(),
            }),
        ),
        Err(err) => fail(
            "request_timeout_enforced",
            json!({
                "unexpected_code": err.code(),
                "unexpected_reason": err.reason(),
                "sqlstate": err.sqlstate(),
                "message": err.message(),
            }),
        ),
        Ok(_) => fail(
            "request_timeout_enforced",
            json!({ "error": "expected timeout error for pg_sleep scenario" }),
        ),
    }
}

async fn check_statement_timeout_enforced(database_uri: &str) -> CheckResult {
    let db = DbEngine::new(
        Some(database_uri.to_string()),
        AccessMode::Unrestricted,
        false,
        Some(Duration::from_secs(2)),
        Some(Duration::from_millis(50)),
        None,
    );

    match db.execute_query_readonly("SELECT pg_sleep(0.3)").await {
        Err(err)
            if err.code() == "DB_QUERY_FAILED"
                && err.reason() == "db_query_failed"
                && err.sqlstate() == Some("57014") =>
        {
            pass(
                "statement_timeout_enforced",
                json!({
                    "code": err.code(),
                    "reason": err.reason(),
                    "sqlstate": err.sqlstate(),
                }),
            )
        }
        Err(err) => fail(
            "statement_timeout_enforced",
            json!({
                "unexpected_code": err.code(),
                "unexpected_reason": err.reason(),
                "sqlstate": err.sqlstate(),
                "message": err.message(),
            }),
        ),
        Ok(_) => fail(
            "statement_timeout_enforced",
            json!({ "error": "expected statement timeout error for pg_sleep scenario" }),
        ),
    }
}

async fn check_lock_timeout_enforced(database_uri: &str) -> CheckResult {
    let connect = match PgConnectionConfig::from_dsn(database_uri) {
        Ok(connect) => connect,
        Err(err) => {
            return fail(
                "lock_timeout_enforced",
                json!({
                    "code": err.code(),
                    "reason": err.reason(),
                    "message": err.message(),
                    "phase": "parse_dsn"
                }),
            );
        }
    };

    let (holder_client, holder_driver) = match connect
        .connect_with_policy(PgInsecureTlsPolicy::AllowRequireOnly)
        .await
    {
        Ok(pair) => pair,
        Err(err) => {
            return fail(
                "lock_timeout_enforced",
                json!({
                    "code": err.code(),
                    "reason": err.reason(),
                    "message": err.message(),
                    "phase": "connect_holder"
                }),
            );
        }
    };

    let lock_sql = format!("SELECT pg_advisory_lock({ADVISORY_LOCK_KEY})");
    if let Err(err) = holder_client.simple_query(&lock_sql).await {
        let wait_result = holder_driver.wait().await.err().map(|driver_err| {
            json!({
                "message": driver_err.message(),
                "sqlstate": driver_err.sqlstate(),
            })
        });
        return fail(
            "lock_timeout_enforced",
            json!({
                "phase": "acquire_holder_lock",
                "message": err.to_string(),
                "driver_wait_error": wait_result,
            }),
        );
    }

    let db = DbEngine::new(
        Some(database_uri.to_string()),
        AccessMode::Unrestricted,
        false,
        Some(Duration::from_secs(2)),
        None,
        Some(Duration::from_millis(50)),
    );

    let contender_result = db.execute_query_unrestricted(&lock_sql).await;

    let unlock_sql = format!("SELECT pg_advisory_unlock({ADVISORY_LOCK_KEY})");
    let unlock_result = holder_client.simple_query(&unlock_sql).await;
    drop(holder_client);
    let driver_result = holder_driver.wait().await;

    let unlock_error = unlock_result.err().map(|err| err.to_string());
    let driver_error = driver_result.err().map(|err| {
        json!({
            "message": err.message(),
            "sqlstate": err.sqlstate(),
        })
    });

    match contender_result {
        Err(err)
            if err.code() == "DB_QUERY_FAILED"
                && err.reason() == "db_query_failed"
                && err.sqlstate() == Some("55P03")
                && unlock_error.is_none()
                && driver_error.is_none() =>
        {
            pass(
                "lock_timeout_enforced",
                json!({
                    "code": err.code(),
                    "reason": err.reason(),
                    "sqlstate": err.sqlstate(),
                }),
            )
        }
        Err(err) => fail(
            "lock_timeout_enforced",
            json!({
                "unexpected_code": err.code(),
                "unexpected_reason": err.reason(),
                "sqlstate": err.sqlstate(),
                "message": err.message(),
                "unlock_error": unlock_error,
                "driver_error": driver_error,
            }),
        ),
        Ok(_) => fail(
            "lock_timeout_enforced",
            json!({
                "error": "expected lock timeout error for advisory lock contention scenario",
                "unlock_error": unlock_error,
                "driver_error": driver_error,
            }),
        ),
    }
}

fn write_report(path: &Path, report: &RuntimeSafetyReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create report directory {}", parent.display()))?;
    }
    let body = serde_json::to_string_pretty(report)
        .context("failed to serialize runtime safety report")?;
    fs::write(path, format!("{body}\n"))
        .with_context(|| format!("failed to write runtime safety report {}", path.display()))?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let database_uri = args
        .database_uri
        .or_else(|| std::env::var("DATABASE_URI").ok());

    let mut checks = Vec::new();
    checks.push(check_restricted_write_rejected_without_database_uri().await);
    checks.push(check_restricted_read_reaches_database_uri_requirement().await);
    checks.push(check_extension_capability_names_stable());
    checks.push(check_extension_guard_singleflight_contract().await);
    checks.push(check_extension_unavailable_cache_contract().await);

    match database_uri.as_deref() {
        Some(uri) => {
            checks.push(check_read_only_transaction_and_local_timeouts(uri).await);
            checks.push(check_request_timeout_enforced(uri).await);
            checks.push(check_statement_timeout_enforced(uri).await);
            checks.push(check_lock_timeout_enforced(uri).await);
        }
        None => {
            for name in ONLINE_CHECK_NAMES {
                checks.push(skip(
                    name,
                    json!({ "reason": "DATABASE_URI not configured" }),
                ));
            }
            if args.require_db_runtime {
                checks.push(fail(
                    "database_uri_required_for_online_runtime_checks",
                    json!({
                        "reason": "DATABASE_URI not configured and --require-db-runtime enabled",
                    }),
                ));
            }
        }
    }

    let failed_checks = checks.iter().filter(|check| check.status == "fail").count();
    let pass = failed_checks == 0;

    let report = RuntimeSafetyReport {
        database_uri_configured: database_uri.is_some(),
        require_db_runtime: args.require_db_runtime,
        checks,
        failed_checks,
        pass,
    };

    write_report(&args.output, &report)?;

    if !report.pass {
        return Err(anyhow!(
            "runtime safety probe failed (failed_checks={}, require_db_runtime={}, output={})",
            report.failed_checks,
            report.require_db_runtime,
            args.output.display(),
        ));
    }

    println!(
        "runtime safety probe ok (failed_checks={}, output={})",
        report.failed_checks,
        args.output.display()
    );

    Ok(())
}
