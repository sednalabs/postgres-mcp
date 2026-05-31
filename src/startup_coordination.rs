//! # Startup Coordination
//!
//! Lease + fencing + phase journal coordination for crash-safe startup
//! transitions when multiple processes share one PostgreSQL database.
//!
//! ## Rationale
//! Avoid split ownership and ambiguous recovery when startup/migration work is
//! interrupted or executed concurrently.
//!
//! ## Security Boundaries
//! * SQL literals are escaped for all dynamic values.
//! * No secrets are emitted in logs.
//! * Coordination is disabled by default for `startup_role=runtime`.

use std::env;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::config::StartupRole;
use crate::db::{DbEngine, QueryOutput};

const ENV_MODE: &str = "POSTGRES_MCP_STARTUP_COORDINATION_MODE";
const ENV_LEASE_KEY: &str = "POSTGRES_MCP_STARTUP_LEASE_KEY";
const ENV_LEASE_TTL_SEC: &str = "POSTGRES_MCP_STARTUP_LEASE_TTL_SEC";
const ENV_HEARTBEAT_SEC: &str = "POSTGRES_MCP_STARTUP_HEARTBEAT_SEC";
const ENV_ACQUIRE_TIMEOUT_SEC: &str = "POSTGRES_MCP_STARTUP_LEASE_ACQUIRE_TIMEOUT_SEC";

const DEFAULT_LEASE_KEY: &str = "postgres-mcp/startup";
const DEFAULT_LEASE_TTL_SEC: u64 = 30;
const DEFAULT_HEARTBEAT_SEC: u64 = 5;
const DEFAULT_ACQUIRE_TIMEOUT_SEC: u64 = 60;
const LEASE_RETRY_SLEEP_MS: u64 = 300;
const MAX_RECORDED_ERROR_LEN: usize = 320;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupCoordinationMode {
    Off,
    Lease,
}

impl StartupCoordinationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Lease => "lease",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "lease" => Ok(Self::Lease),
            _ => Err(anyhow!(
                "invalid {ENV_MODE} value {:?} (expected off|lease)",
                raw
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StartupCoordinationConfig {
    pub mode: StartupCoordinationMode,
    pub lease_key: String,
    pub lease_ttl: Duration,
    pub heartbeat_interval: Duration,
    pub acquire_timeout: Duration,
}

impl StartupCoordinationConfig {
    pub fn from_env(startup_role: StartupRole) -> Result<Self> {
        let mode = if let Ok(raw) = env::var(ENV_MODE) {
            Some(StartupCoordinationMode::parse(&raw)?)
        } else {
            None
        };
        let lease_key = env::var(ENV_LEASE_KEY).ok();
        let lease_ttl_sec = parse_u64_env(ENV_LEASE_TTL_SEC)?;
        let heartbeat_sec = parse_u64_env(ENV_HEARTBEAT_SEC)?;
        let acquire_timeout_sec = parse_u64_env(ENV_ACQUIRE_TIMEOUT_SEC)?;
        let options = ParsedEnvOptions {
            mode,
            lease_key,
            lease_ttl_sec,
            heartbeat_sec,
            acquire_timeout_sec,
        };
        Self::from_parsed_options(options, startup_role)
    }

    pub fn enabled(&self) -> bool {
        self.mode == StartupCoordinationMode::Lease
    }

    fn from_parsed_options(options: ParsedEnvOptions, startup_role: StartupRole) -> Result<Self> {
        let mode = options.mode.unwrap_or(match startup_role {
            StartupRole::Runtime => StartupCoordinationMode::Off,
            StartupRole::Migrator => StartupCoordinationMode::Lease,
        });
        let lease_key = options
            .lease_key
            .unwrap_or_else(|| DEFAULT_LEASE_KEY.to_string())
            .trim()
            .to_string();
        if lease_key.is_empty() {
            return Err(anyhow!("{ENV_LEASE_KEY} must not be empty"));
        }

        let lease_ttl_sec = options.lease_ttl_sec.unwrap_or(DEFAULT_LEASE_TTL_SEC);
        let heartbeat_sec = options.heartbeat_sec.unwrap_or(DEFAULT_HEARTBEAT_SEC);
        let acquire_timeout_sec = options
            .acquire_timeout_sec
            .unwrap_or(DEFAULT_ACQUIRE_TIMEOUT_SEC);

        if lease_ttl_sec == 0 {
            return Err(anyhow!("{ENV_LEASE_TTL_SEC} must be >= 1"));
        }
        if heartbeat_sec == 0 {
            return Err(anyhow!("{ENV_HEARTBEAT_SEC} must be >= 1"));
        }
        if heartbeat_sec >= lease_ttl_sec {
            return Err(anyhow!(
                "{ENV_HEARTBEAT_SEC} must be less than {ENV_LEASE_TTL_SEC}"
            ));
        }
        if acquire_timeout_sec == 0 {
            return Err(anyhow!("{ENV_ACQUIRE_TIMEOUT_SEC} must be >= 1"));
        }

        Ok(Self {
            mode,
            lease_key,
            lease_ttl: Duration::from_secs(lease_ttl_sec),
            heartbeat_interval: Duration::from_secs(heartbeat_sec),
            acquire_timeout: Duration::from_secs(acquire_timeout_sec),
        })
    }
}

#[derive(Debug, Clone, Default)]
struct ParsedEnvOptions {
    mode: Option<StartupCoordinationMode>,
    lease_key: Option<String>,
    lease_ttl_sec: Option<u64>,
    heartbeat_sec: Option<u64>,
    acquire_timeout_sec: Option<u64>,
}

#[derive(Debug)]
pub struct StartupCoordinator {
    db: Arc<DbEngine>,
    config: StartupCoordinationConfig,
}

impl StartupCoordinator {
    pub fn new(db: Arc<DbEngine>, config: StartupCoordinationConfig) -> Self {
        Self { db, config }
    }

    pub async fn acquire(&self) -> Result<StartupLeaseGuard> {
        if self.config.mode != StartupCoordinationMode::Lease {
            return Err(anyhow!("startup coordination acquire called when mode=off"));
        }

        self.ensure_tables().await?;
        let owner_id = generated_id("owner");
        let run_id = generated_id("run");
        let deadline = Instant::now() + self.config.acquire_timeout;

        loop {
            if let Some(fence_token) = self.try_acquire_lease(&owner_id).await? {
                let (heartbeat_stop_tx, heartbeat_stop_rx) = oneshot::channel::<()>();
                let fence_lost = Arc::new(AtomicBool::new(false));
                let heartbeat_task = spawn_heartbeat_task(
                    self.db.clone(),
                    self.config.clone(),
                    self.config.lease_key.clone(),
                    owner_id.clone(),
                    fence_token,
                    heartbeat_stop_rx,
                    fence_lost.clone(),
                );
                let guard = StartupLeaseGuard {
                    db: self.db.clone(),
                    config: self.config.clone(),
                    owner_id,
                    run_id,
                    fence_token,
                    heartbeat_stop_tx: Some(heartbeat_stop_tx),
                    heartbeat_task: Some(heartbeat_task),
                    fence_lost,
                };
                guard
                    .record_phase_event("startup_lease_acquired", "started", None, None)
                    .await?;
                guard
                    .record_phase_event("startup_lease_acquired", "completed", None, None)
                    .await?;
                return Ok(guard);
            }

            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "startup coordination lease busy for key {:?} after {}s",
                    self.config.lease_key,
                    self.config.acquire_timeout.as_secs()
                ));
            }

            tokio::time::sleep(Duration::from_millis(LEASE_RETRY_SLEEP_MS)).await;
        }
    }

    async fn ensure_tables(&self) -> Result<()> {
        let sql = r#"
CREATE TABLE IF NOT EXISTS mcp_startup_leases (
    lease_key TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    fence_token BIGINT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mcp_startup_phase_events (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    lease_key TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    fence_token BIGINT NOT NULL,
    phase TEXT NOT NULL,
    event TEXT NOT NULL CHECK (event IN ('started', 'completed', 'failed', 'skipped')),
    error_code TEXT,
    error_message TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mcp_startup_phase_events_lookup
ON mcp_startup_phase_events (lease_key, phase, id DESC);
        "#;
        execute_sql(&self.db, sql).await?;
        Ok(())
    }

    async fn try_acquire_lease(&self, owner_id: &str) -> Result<Option<i64>> {
        let lease_key = sql_literal(&self.config.lease_key);
        let owner_id = sql_literal(owner_id);
        let ttl_sec = self.config.lease_ttl.as_secs().max(1);
        let sql = format!(
            r#"
INSERT INTO mcp_startup_leases (
    lease_key,
    owner_id,
    fence_token,
    lease_expires_at,
    heartbeat_at,
    updated_at
)
VALUES (
    {lease_key},
    {owner_id},
    1,
    now() + make_interval(secs => {ttl_sec}),
    now(),
    now()
)
ON CONFLICT (lease_key) DO UPDATE
SET
    owner_id = EXCLUDED.owner_id,
    fence_token = mcp_startup_leases.fence_token + 1,
    lease_expires_at = EXCLUDED.lease_expires_at,
    heartbeat_at = EXCLUDED.heartbeat_at,
    updated_at = now()
WHERE
    mcp_startup_leases.lease_expires_at <= now()
    OR mcp_startup_leases.owner_id = EXCLUDED.owner_id
RETURNING fence_token::BIGINT AS fence_token
            "#
        );
        let output = execute_sql(&self.db, &sql).await?;
        if output.rows.is_empty() {
            return Ok(None);
        }
        let fence = first_i64(&output, "fence_token")?;
        Ok(Some(fence))
    }
}

#[derive(Debug)]
pub struct StartupLeaseGuard {
    db: Arc<DbEngine>,
    config: StartupCoordinationConfig,
    owner_id: String,
    run_id: String,
    fence_token: i64,
    heartbeat_stop_tx: Option<oneshot::Sender<()>>,
    heartbeat_task: Option<JoinHandle<()>>,
    fence_lost: Arc<AtomicBool>,
}

impl StartupLeaseGuard {
    pub fn fence_token(&self) -> i64 {
        self.fence_token
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub async fn run_phase<F, Fut>(
        &self,
        phase: &str,
        skip_if_completed: bool,
        work: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        validate_phase_name(phase)?;

        if skip_if_completed {
            let latest_event = self.latest_phase_event(phase).await?;
            if latest_event.as_deref() == Some("completed") {
                self.record_phase_event(phase, "skipped", None, None)
                    .await?;
                tracing::info!(
                    lease_key = %self.config.lease_key,
                    phase = %phase,
                    run_id = %self.run_id,
                    "startup phase already completed; skipping"
                );
                return Ok(());
            }
        }

        self.record_phase_event(phase, "started", None, None)
            .await?;
        match work().await {
            Ok(()) => {
                self.record_phase_event(phase, "completed", None, None)
                    .await?;
                Ok(())
            }
            Err(err) => {
                let message = clipped_error_text(&err.to_string());
                if let Err(record_err) = self
                    .record_phase_event(phase, "failed", Some("phase_failed"), Some(&message))
                    .await
                {
                    return Err(anyhow!(
                        "phase {phase:?} failed: {message}; failed to record journal error: {record_err}"
                    ));
                }
                Err(err)
            }
        }
    }

    pub async fn release(mut self) -> Result<()> {
        self.stop_heartbeat().await;
        let lease_key = sql_literal(&self.config.lease_key);
        let owner_id = sql_literal(&self.owner_id);
        let fence = self.fence_token;
        let sql = format!(
            r#"
DELETE FROM mcp_startup_leases
WHERE lease_key = {lease_key}
  AND owner_id = {owner_id}
  AND fence_token = {fence}
            "#
        );
        execute_sql(&self.db, &sql).await?;
        Ok(())
    }

    async fn latest_phase_event(&self, phase: &str) -> Result<Option<String>> {
        let phase = sql_literal(phase);
        let lease_key = sql_literal(&self.config.lease_key);
        let sql = format!(
            r#"
SELECT event
FROM mcp_startup_phase_events
WHERE lease_key = {lease_key}
  AND phase = {phase}
ORDER BY id DESC
LIMIT 1
            "#
        );
        let output = execute_sql(&self.db, &sql).await?;
        if output.rows.is_empty() {
            return Ok(None);
        }
        Ok(first_text(&output, "event"))
    }

    async fn assert_fence(&self) -> Result<()> {
        if self.fence_lost.load(Ordering::Relaxed) {
            return Err(anyhow!(
                "startup coordination lease lost before phase journal write (lease_key={:?}, owner_id={:?})",
                self.config.lease_key,
                self.owner_id
            ));
        }

        let lease_key = sql_literal(&self.config.lease_key);
        let owner_id = sql_literal(&self.owner_id);
        let fence = self.fence_token;
        let sql = format!(
            r#"
SELECT 1 AS held
FROM mcp_startup_leases
WHERE lease_key = {lease_key}
  AND owner_id = {owner_id}
  AND fence_token = {fence}
  AND lease_expires_at > now()
LIMIT 1
            "#
        );
        let output = execute_sql(&self.db, &sql).await?;
        if output.rows.is_empty() {
            self.fence_lost.store(true, Ordering::Relaxed);
            return Err(anyhow!(
                "startup coordination lease no longer held (lease_key={:?}, owner_id={:?}, fence_token={})",
                self.config.lease_key,
                self.owner_id,
                self.fence_token
            ));
        }
        Ok(())
    }

    async fn record_phase_event(
        &self,
        phase: &str,
        event: &str,
        error_code: Option<&str>,
        error_message: Option<&str>,
    ) -> Result<()> {
        validate_phase_name(phase)?;
        self.assert_fence().await?;
        let lease_key = sql_literal(&self.config.lease_key);
        let owner_id = sql_literal(&self.owner_id);
        let run_id = sql_literal(&self.run_id);
        let phase = sql_literal(phase);
        let event = sql_literal(event);
        let error_code = sql_literal_nullable(error_code);
        let error_message = sql_literal_nullable(error_message);
        let fence = self.fence_token;
        let sql = format!(
            r#"
INSERT INTO mcp_startup_phase_events (
    run_id,
    lease_key,
    owner_id,
    fence_token,
    phase,
    event,
    error_code,
    error_message,
    occurred_at
)
VALUES (
    {run_id},
    {lease_key},
    {owner_id},
    {fence},
    {phase},
    {event},
    {error_code},
    {error_message},
    now()
)
            "#
        );
        execute_sql(&self.db, &sql).await?;
        Ok(())
    }

    async fn stop_heartbeat(&mut self) {
        if let Some(stop_tx) = self.heartbeat_stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(task) = self.heartbeat_task.take() {
            if let Err(err) = task.await {
                tracing::warn!(error = %err, "startup coordination heartbeat task join failed");
            }
        }
    }
}

impl Drop for StartupLeaseGuard {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.heartbeat_stop_tx.take() {
            let _ = stop_tx.send(());
        }
    }
}

fn spawn_heartbeat_task(
    db: Arc<DbEngine>,
    config: StartupCoordinationConfig,
    lease_key: String,
    owner_id: String,
    fence_token: i64,
    mut stop_rx: oneshot::Receiver<()>,
    fence_lost: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    break;
                }
                _ = tokio::time::sleep(config.heartbeat_interval) => {
                    match heartbeat_once(&db, &config, &lease_key, &owner_id, fence_token).await {
                        Ok(true) => {}
                        Ok(false) => {
                            fence_lost.store(true, Ordering::Relaxed);
                            tracing::error!(
                                lease_key = %lease_key,
                                owner_id = %owner_id,
                                fence_token = fence_token,
                                "startup coordination lease heartbeat lost fence ownership"
                            );
                            break;
                        }
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                lease_key = %lease_key,
                                owner_id = %owner_id,
                                "startup coordination heartbeat failed; will retry"
                            );
                        }
                    }
                }
            }
        }
    })
}

async fn heartbeat_once(
    db: &DbEngine,
    config: &StartupCoordinationConfig,
    lease_key: &str,
    owner_id: &str,
    fence_token: i64,
) -> Result<bool> {
    let ttl_sec = config.lease_ttl.as_secs().max(1);
    let lease_key = sql_literal(lease_key);
    let owner_id = sql_literal(owner_id);
    let sql = format!(
        r#"
UPDATE mcp_startup_leases
SET lease_expires_at = now() + make_interval(secs => {ttl_sec}),
    heartbeat_at = now(),
    updated_at = now()
WHERE lease_key = {lease_key}
  AND owner_id = {owner_id}
  AND fence_token = {fence_token}
RETURNING fence_token::BIGINT AS fence_token
        "#
    );
    let output = execute_sql(db, &sql).await?;
    Ok(!output.rows.is_empty())
}

async fn execute_sql(db: &DbEngine, sql: &str) -> Result<QueryOutput> {
    db.execute_query_unrestricted(sql)
        .await
        .map_err(anyhow::Error::new)
}

fn parse_u64_env(var_name: &str) -> Result<Option<u64>> {
    match env::var(var_name) {
        Ok(raw) => {
            let value = raw
                .trim()
                .parse::<u64>()
                .map_err(|_| anyhow!("invalid {var_name} value {:?} (expected integer)", raw))?;
            Ok(Some(value))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(anyhow!("failed reading {var_name}: {err}")),
    }
}

fn generated_id(prefix: &str) -> String {
    let mut bytes = [0_u8; 12];
    if getrandom::fill(&mut bytes).is_err() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        return format!("{prefix}-pid{}-{nanos:x}", std::process::id());
    }

    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let hi = byte >> 4;
        let lo = byte & 0x0f;
        hex.push(nibble_to_hex(hi));
        hex.push(nibble_to_hex(lo));
    }
    format!("{prefix}-pid{}-{hex}", std::process::id())
}

fn nibble_to_hex(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + (nibble - 10)) as char,
        _ => '0',
    }
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_literal_nullable(value: Option<&str>) -> String {
    match value {
        Some(v) => sql_literal(v),
        None => "NULL".to_string(),
    }
}

fn validate_phase_name(phase: &str) -> Result<()> {
    let phase = phase.trim();
    if phase.is_empty() {
        return Err(anyhow!("phase name must not be empty"));
    }
    if phase.len() > 80 {
        return Err(anyhow!("phase name too long (max 80 chars)"));
    }
    for ch in phase.chars() {
        let valid = ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':');
        if !valid {
            return Err(anyhow!(
                "invalid phase name {:?}; allowed chars are [a-zA-Z0-9_.:-]",
                phase
            ));
        }
    }
    Ok(())
}

fn first_i64(output: &QueryOutput, key: &str) -> Result<i64> {
    let row = output
        .rows
        .first()
        .ok_or_else(|| anyhow!("expected at least one row for key {:?}", key))?;
    let value = row
        .get(key)
        .ok_or_else(|| anyhow!("missing key {:?} in SQL row", key))?;
    match value {
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| anyhow!("value for {:?} is not an i64", key)),
        Value::String(text) => text
            .parse::<i64>()
            .map_err(|_| anyhow!("value for {:?} is not a parseable i64", key)),
        _ => Err(anyhow!("value for {:?} is not numeric", key)),
    }
}

fn first_text(output: &QueryOutput, key: &str) -> Option<String> {
    let row = output.rows.first()?;
    let value = row.get(key)?;
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn clipped_error_text(message: &str) -> String {
    let trimmed = message.trim();
    if trimmed.chars().count() <= MAX_RECORDED_ERROR_LEN {
        return trimmed.to_string();
    }
    trimmed.chars().take(MAX_RECORDED_ERROR_LEN).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_defaults_to_coordination_off() {
        let cfg = StartupCoordinationConfig::from_parsed_options(
            ParsedEnvOptions::default(),
            StartupRole::Runtime,
        )
        .expect("runtime defaults should parse");
        assert_eq!(cfg.mode, StartupCoordinationMode::Off);
    }

    #[test]
    fn migrator_defaults_to_coordination_lease() {
        let cfg = StartupCoordinationConfig::from_parsed_options(
            ParsedEnvOptions::default(),
            StartupRole::Migrator,
        )
        .expect("migrator defaults should parse");
        assert_eq!(cfg.mode, StartupCoordinationMode::Lease);
    }

    #[test]
    fn validates_heartbeat_lt_ttl() {
        let options = ParsedEnvOptions {
            mode: Some(StartupCoordinationMode::Lease),
            lease_key: Some(DEFAULT_LEASE_KEY.to_string()),
            lease_ttl_sec: Some(10),
            heartbeat_sec: Some(10),
            acquire_timeout_sec: Some(60),
        };
        let err = StartupCoordinationConfig::from_parsed_options(options, StartupRole::Migrator)
            .expect_err("equal heartbeat/ttl should fail");
        assert!(err.to_string().contains(ENV_HEARTBEAT_SEC));
    }

    #[test]
    fn parses_mode_with_validation() {
        assert_eq!(
            StartupCoordinationMode::parse("off").expect("off should parse"),
            StartupCoordinationMode::Off
        );
        assert_eq!(
            StartupCoordinationMode::parse("lease").expect("lease should parse"),
            StartupCoordinationMode::Lease
        );
        assert!(StartupCoordinationMode::parse("invalid").is_err());
    }

    #[test]
    fn sql_literal_escapes_single_quotes() {
        assert_eq!(sql_literal("a'b"), "'a''b'");
    }

    #[test]
    fn phase_name_validation_rejects_bad_chars() {
        assert!(validate_phase_name("startup.probe").is_ok());
        assert!(validate_phase_name("bad phase").is_err());
    }

    #[test]
    fn clipped_error_message_bounds_length() {
        let source = "x".repeat(MAX_RECORDED_ERROR_LEN + 25);
        let clipped = clipped_error_text(&source);
        assert_eq!(clipped.chars().count(), MAX_RECORDED_ERROR_LEN);
    }
}
