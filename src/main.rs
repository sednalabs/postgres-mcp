//! # postgres-mcp Main
//!
//! Entrypoint for the Rust stdio PostgreSQL MCP server.
//!
//! ## Rationale
//! Provide low-latency stdio startup while preserving practical parity with
//! the Python postgres-mcp tool surface.
//!
//! ## Security Boundaries
//! * Connection URI is consumed from env/CLI and never emitted in clear text.
//! * Read-oriented SQL tools enforce read-safe checks before execution.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::Parser;
use rmcp::serve_server;
use rmcp::transport::stdio;
use serde_json::json;
use tracing_subscriber::EnvFilter;

use postgres_mcp::config::{Cli, Settings, StartupDbConnectMode, StartupRole};
use postgres_mcp::db::DbEngine;
use postgres_mcp::server::PostgresMcp;
use postgres_mcp::startup_coordination::StartupCoordinationConfig;
use postgres_mcp::startup_coordination::StartupCoordinator;
use postgres_mcp::startup_dependencies::{
    StartupDependencyConfig, StartupDependencyOutcome, validate_startup_dependencies,
};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("postgres-mcp failed to start: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let settings = Settings::from_cli(cli)?;
    let startup_coordination = StartupCoordinationConfig::from_env(settings.startup_role)?;
    let startup_dependencies = StartupDependencyConfig::from_env(settings.startup_role)?;

    let db = Arc::new(DbEngine::new(
        settings.database_url.clone(),
        settings.access_mode,
        settings.allow_insecure_tls,
        settings.db_query_timeout,
        settings.db_statement_timeout,
        settings.db_lock_timeout,
    ));
    let mut server = PostgresMcp::with_runtime_options(
        db.clone(),
        settings.response_mode,
        settings.response_output_mode,
        settings.response_output_mode_auto_tabular,
        settings.response_page_size,
        settings.advisor_external.clone(),
    );
    server.metadata_policy_mode = settings.metadata_policy_mode;
    server.metadata_schema_allow = Arc::new(settings.metadata_schema_allow.clone());
    server.metadata_schema_deny = Arc::new(settings.metadata_schema_deny.clone());
    server.startup_role = settings.startup_role;
    server.enable_admin_sql = settings.enable_admin_sql;
    server.expose_execute_sql = settings.expose_execute_sql;
    server.configure_pagination_cursor_security(
        settings.cursor_ttl,
        settings.cursor_signing_key.as_deref(),
    );

    if settings.print_tools {
        let names = server.tool_names();
        println!("{}", serde_json::to_string_pretty(&names)?);
        return Ok(());
    }

    if settings.print_tool_schema {
        println!(
            "{}",
            serde_json::to_string_pretty(&server.tool_schema_snapshot()?)?
        );
        return Ok(());
    }

    if let Some(sql) = settings.probe_sql.as_deref() {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("--probe-sql must not be empty"));
        }

        for _ in 0..settings.probe_repeat {
            let _ = db
                .execute_query_readonly(trimmed)
                .await
                .map_err(anyhow::Error::new)?;
        }

        println!(
            "{}",
            serde_json::to_string(&json!({
                "status": "ok",
                "probe_type": "sql_readonly",
                "probe_repeat": settings.probe_repeat,
            }))?
        );
        return Ok(());
    }

    mcp_toolkit_observability::emit_event(
        mcp_toolkit_observability::Level::INFO,
        "postgres_mcp.startup",
        &mcp_toolkit_observability::EventContext::new(),
        &[
            mcp_toolkit_observability::safe_text("transport", "stdio"),
            mcp_toolkit_observability::safe_text("access_mode", settings.access_mode.as_str()),
            mcp_toolkit_observability::safe_text("startup_role", settings.startup_role.as_str()),
            mcp_toolkit_observability::safe_text(
                "metadata_policy_mode",
                settings.metadata_policy_mode.as_str(),
            ),
            mcp_toolkit_observability::safe_text(
                "startup_db_connect",
                match settings.startup_db_connect {
                    StartupDbConnectMode::Warn => "warn",
                    StartupDbConnectMode::FailFast => "fail-fast",
                    StartupDbConnectMode::Background => "background",
                },
            ),
            mcp_toolkit_observability::safe_text(
                "allow_insecure_tls",
                if settings.allow_insecure_tls {
                    "true"
                } else {
                    "false"
                },
            ),
            mcp_toolkit_observability::safe_text(
                "enable_admin_sql",
                if settings.enable_admin_sql {
                    "true"
                } else {
                    "false"
                },
            ),
            mcp_toolkit_observability::safe_text(
                "expose_execute_sql",
                if settings.expose_execute_sql {
                    "true"
                } else {
                    "false"
                },
            ),
            mcp_toolkit_observability::safe_text(
                "startup_coordination_mode",
                startup_coordination.mode.as_str(),
            ),
            mcp_toolkit_observability::safe_text(
                "startup_dependency_mode",
                startup_dependencies.mode.as_str(),
            ),
            mcp_toolkit_observability::safe_text(
                "cursor_ttl_sec",
                settings.cursor_ttl.as_secs().to_string(),
            ),
            mcp_toolkit_observability::safe_text(
                "advisor_external_enabled",
                if settings.advisor_external.enabled {
                    "true"
                } else {
                    "false"
                },
            ),
        ],
    );

    let dependency_outcome = if startup_coordination.enabled() {
        let coordinator = StartupCoordinator::new(db.clone(), startup_coordination.clone());
        let lease = coordinator.acquire().await?;
        let coordinated_mode = startup_probe_mode_for_coordination(settings.startup_db_connect);
        if settings.startup_db_connect == StartupDbConnectMode::Background {
            tracing::info!(
                "startup coordination mode=lease enabled; startup_db_connect=background is executed as warn during coordinated startup phase"
            );
        }
        lease
            .run_phase("startup_db_connect_probe", false, || {
                let db = db.clone();
                async move {
                    run_startup_db_connect(
                        db,
                        coordinated_mode,
                        settings.startup_db_connect_timeout,
                    )
                    .await
                }
            })
            .await?;
        let dependency_outcome = run_startup_dependency_validation(
            db.clone(),
            startup_dependencies.clone(),
            Some(&lease),
        )
        .await?;
        lease.release().await?;
        dependency_outcome
    } else {
        run_startup_db_connect(
            db.clone(),
            settings.startup_db_connect,
            settings.startup_db_connect_timeout,
        )
        .await?;
        run_startup_dependency_validation(db.clone(), startup_dependencies.clone(), None).await?
    };

    if dependency_outcome.degraded_read_only {
        tracing::warn!(
            reason = ?dependency_outcome.reason,
            missing_dependencies = ?dependency_outcome.missing_relations,
            "startup dependency validation entered degraded read-only mode"
        );
        server.set_startup_degraded_read_only(
            dependency_outcome.reason.clone(),
            dependency_outcome.missing_relations.clone(),
        );
    }

    if settings.startup_role == StartupRole::Migrator {
        tracing::info!(
            "startup_role=migrator selected; server accepts privileged schema-changing operations for migration workflows"
        );
    }

    let transport = stdio();
    let service = serve_server(server, transport).await?;
    service.waiting().await?;
    Ok(())
}

fn startup_probe_mode_for_coordination(mode: StartupDbConnectMode) -> StartupDbConnectMode {
    match mode {
        StartupDbConnectMode::Background => StartupDbConnectMode::Warn,
        StartupDbConnectMode::Warn => StartupDbConnectMode::Warn,
        StartupDbConnectMode::FailFast => StartupDbConnectMode::FailFast,
    }
}

async fn run_startup_dependency_validation(
    db: Arc<DbEngine>,
    config: StartupDependencyConfig,
    lease: Option<&postgres_mcp::startup_coordination::StartupLeaseGuard>,
) -> Result<StartupDependencyOutcome> {
    if let Some(lease) = lease {
        let outcome_cell =
            std::sync::Arc::new(std::sync::Mutex::new(None::<StartupDependencyOutcome>));
        lease
            .run_phase("startup_dependency_validation", false, || {
                let db = db.clone();
                let config = config.clone();
                let outcome_cell = outcome_cell.clone();
                async move {
                    let validated = validate_startup_dependencies(db.as_ref(), &config).await?;
                    if let Ok(mut guard) = outcome_cell.lock() {
                        *guard = Some(validated);
                    }
                    Ok(())
                }
            })
            .await?;
        if let Ok(mut guard) = outcome_cell.lock()
            && let Some(outcome) = guard.take()
        {
            return Ok(outcome);
        }
        return Ok(StartupDependencyOutcome::healthy());
    }
    validate_startup_dependencies(db.as_ref(), &config).await
}

async fn run_startup_db_connect(
    db: Arc<DbEngine>,
    mode: StartupDbConnectMode,
    timeout: Option<Duration>,
) -> Result<()> {
    match mode {
        StartupDbConnectMode::Warn => {
            if let Err(err) = db.startup_connect_probe(timeout).await {
                tracing::warn!(error = %err, "startup DB probe failed; server will continue");
            }
            Ok(())
        }
        StartupDbConnectMode::FailFast => {
            db.startup_connect_probe(timeout)
                .await
                .map_err(anyhow::Error::new)?;
            Ok(())
        }
        StartupDbConnectMode::Background => {
            tokio::spawn(async move {
                if let Err(err) = db.startup_connect_probe(timeout).await {
                    tracing::warn!(error = %err.to_string(), "background startup DB probe failed");
                }
            });
            Ok(())
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_writer(std::io::stderr)
        .try_init();
}
