//! # MCP Server Handler
//!
//! Registers tool routers and serves the MCP protocol over stdio.
//!
//! ## Rationale
//! Keep the server surface focused on tools used by agent workflows.
//!
//! ## Security Boundaries
//! * Tool execution delegates DB safety checks to `db` and `sql_safety` modules.

use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use std::collections::{HashMap, VecDeque};

use mcp_toolkit_core::rmcp_models;
use mcp_toolkit_core::tool_schema::tool_schema_snapshot_value;
use mcp_toolkit_policy_runtime::CapabilityGuard;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::tool::ToolCallContext;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, CancelTaskParams, CancelTaskResult, CreateTaskResult,
    GetTaskInfoParams, GetTaskPayloadResult, GetTaskResult, GetTaskResultParams, Implementation,
    ListResourcesResult, ListTasksResult, ListToolsResult, PaginatedRequestParams,
    ProgressNotificationParam, ProtocolVersion, RawResource, ReadResourceRequestParams,
    ReadResourceResult, Resource, ResourceContents, ServerCapabilities, ServerInfo, Task,
    TaskStatus, Tool,
};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler};
use serde_json::Value;
use serde_json::json;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tokio::sync::Notify;
use tokio::task::AbortHandle;

use crate::advisor_extension::{AdvisorExternalRunner, default_advisor_external_runner};
use crate::config::{
    AdvisorExternalConfig, MetadataPolicyMode, ResponseAutoTabularMode, ResponseMode,
    ResponseOutputMode, StartupRole,
};
use crate::db::{DbEngine, PinnedDbSession};

const EXTENSION_GUARD_TTL: Duration = Duration::from_secs(300);
const EXTENSION_GUARD_MAX_ENTRIES: usize = 16;
const EXTENSION_UNAVAILABLE_TTL: Duration = Duration::from_secs(30);
const EXTENSION_UNAVAILABLE_MAX_ENTRIES: usize = 16;
const DEFAULT_PAGINATION_CURSOR_TTL: Duration = Duration::from_secs(900);
const QUERY_JOB_MAX_ENTRIES_ENV: &str = "POSTGRES_MCP_QUERY_JOB_MAX_ENTRIES";
const QUERY_JOB_TERMINAL_TTL_SEC_ENV: &str = "POSTGRES_MCP_QUERY_JOB_TERMINAL_TTL_SEC";
const DEFAULT_QUERY_JOB_MAX_ENTRIES: usize = 1024;
const DEFAULT_QUERY_JOB_TERMINAL_TTL_SEC: u64 = 3600;
const DEFAULT_EXPORT_ARTIFACT_MAX_ENTRIES: usize = 128;
const DEFAULT_PINNED_SESSION_IDLE_TTL: Duration = Duration::from_secs(900);
const TELEMETRY_DEBUG_ENV: &str = "POSTGRES_MCP_TELEMETRY_DEBUG";
const TELEMETRY_DEBUG_MAX_CHARS: usize = 256;
const CIRCUIT_BREAKER_ENABLED_ENV: &str = "POSTGRES_MCP_CIRCUIT_BREAKER_ENABLED";
const CIRCUIT_BREAKER_FAILURE_THRESHOLD_ENV: &str =
    "POSTGRES_MCP_CIRCUIT_BREAKER_FAILURE_THRESHOLD";
const CIRCUIT_BREAKER_COOLDOWN_SEC_ENV: &str = "POSTGRES_MCP_CIRCUIT_BREAKER_COOLDOWN_SEC";
const DEFAULT_CIRCUIT_BREAKER_FAILURE_THRESHOLD: u32 = 3;
const DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SEC: u64 = 30;

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn iso8601_from_unix_ms(value: u64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos((value as i128) * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn serialize_call_tool_result(result: &CallToolResult) -> Value {
    serde_json::to_value(result).unwrap_or_else(|_| {
        json!({
            "structuredContent": {
                "ok": false,
                "error": {
                    "error": "failed to serialize tool result",
                    "code": "QUERY_JOB_INTERNAL",
                    "reason": "query_job_internal",
                },
                "meta": {
                    "elapsed_ms": 0
                }
            }
        })
    })
}

#[derive(Debug, Clone)]
struct ToolCircuitBreakerConfig {
    enabled: bool,
    failure_threshold: u32,
    cooldown: Duration,
}

impl ToolCircuitBreakerConfig {
    fn from_env() -> Self {
        let enabled = parse_bool_env(CIRCUIT_BREAKER_ENABLED_ENV).unwrap_or(true);
        let failure_threshold = parse_u32_env(CIRCUIT_BREAKER_FAILURE_THRESHOLD_ENV)
            .unwrap_or(DEFAULT_CIRCUIT_BREAKER_FAILURE_THRESHOLD)
            .max(1);
        let cooldown_sec = parse_u64_env(CIRCUIT_BREAKER_COOLDOWN_SEC_ENV)
            .unwrap_or(DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SEC)
            .max(1);
        Self {
            enabled,
            failure_threshold,
            cooldown: Duration::from_secs(cooldown_sec),
        }
    }
}

#[derive(Debug, Clone)]
struct ToolCircuitState {
    consecutive_retryable_failures: u32,
    open_until: Option<Instant>,
}

impl ToolCircuitState {
    fn closed() -> Self {
        Self {
            consecutive_retryable_failures: 0,
            open_until: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolCallOutcome {
    Success,
    RetryableFailure,
    NonRetryableFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryJobState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl QueryJobState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }
}

#[derive(Debug, Clone)]
pub struct QueryJobSnapshot {
    pub job_id: String,
    pub kind: String,
    pub query_hash: String,
    pub task_managed: bool,
    pub state: QueryJobState,
    pub created_at_unix_ms: u64,
    pub started_at_unix_ms: Option<u64>,
    pub finished_at_unix_ms: Option<u64>,
    pub cancel_requested: bool,
    pub response: Option<Value>,
    pub tool_result: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryJobRegistryError {
    CapacityReached,
    Unavailable,
}

impl QueryJobRegistryError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::CapacityReached => "QUERY_JOB_CAPACITY_REACHED",
            Self::Unavailable => "QUERY_JOB_INTERNAL",
        }
    }

    pub const fn reason(self) -> &'static str {
        match self {
            Self::CapacityReached => "query_job_capacity_reached",
            Self::Unavailable => "query_job_internal",
        }
    }

    pub const fn message(self) -> &'static str {
        match self {
            Self::CapacityReached => {
                "query job capacity reached; retry later or poll existing jobs to completion"
            }
            Self::Unavailable => "query job registry unavailable",
        }
    }
}

#[derive(Debug)]
pub struct QueryJobHandle {
    snapshot: Mutex<QueryJobSnapshot>,
    notify: Notify,
    update_revision: AtomicU64,
    abort_handle: Mutex<Option<AbortHandle>>,
}

impl QueryJobHandle {
    fn new(job_id: String, kind: String, query_hash: String, task_managed: bool) -> Self {
        Self {
            snapshot: Mutex::new(QueryJobSnapshot {
                job_id,
                kind,
                query_hash,
                task_managed,
                state: QueryJobState::Pending,
                created_at_unix_ms: unix_time_ms(),
                started_at_unix_ms: None,
                finished_at_unix_ms: None,
                cancel_requested: false,
                response: None,
                tool_result: None,
            }),
            notify: Notify::new(),
            update_revision: AtomicU64::new(0),
            abort_handle: Mutex::new(None),
        }
    }

    pub fn snapshot(&self) -> QueryJobSnapshot {
        self.snapshot
            .lock()
            .map(|snapshot| snapshot.clone())
            .unwrap_or_else(|_| QueryJobSnapshot {
                job_id: "unknown".to_string(),
                kind: "unknown".to_string(),
                query_hash: String::new(),
                task_managed: false,
                state: QueryJobState::Failed,
                created_at_unix_ms: unix_time_ms(),
                started_at_unix_ms: None,
                finished_at_unix_ms: Some(unix_time_ms()),
                cancel_requested: false,
                response: Some(json!({
                    "ok": false,
                    "error": {
                        "error": "query job lock poisoned",
                        "code": "QUERY_JOB_INTERNAL",
                        "reason": "query_job_lock_poisoned",
                    }
                })),
                tool_result: Some(json!({
                    "structuredContent": {
                        "ok": false,
                        "error": {
                            "error": "query job lock poisoned",
                            "code": "QUERY_JOB_INTERNAL",
                            "reason": "query_job_lock_poisoned"
                        }
                    }
                })),
            })
    }

    pub fn mark_running(&self) -> QueryJobSnapshot {
        let mut changed = false;
        let snapshot = if let Ok(mut snapshot) = self.snapshot.lock() {
            if snapshot.state == QueryJobState::Pending {
                snapshot.state = QueryJobState::Running;
                snapshot.started_at_unix_ms = Some(unix_time_ms());
                changed = true;
            }
            snapshot.clone()
        } else {
            self.snapshot()
        };
        if changed {
            self.update_revision.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
        snapshot
    }

    pub fn complete(&self, state: QueryJobState, response: Value) -> QueryJobSnapshot {
        let tool_result = serialize_call_tool_result(&CallToolResult::structured(response.clone()));
        self.complete_payloads(state, response, tool_result)
    }

    pub fn complete_structured(
        &self,
        state: QueryJobState,
        response: Value,
        tool_result: CallToolResult,
    ) -> QueryJobSnapshot {
        self.complete_payloads(state, response, serialize_call_tool_result(&tool_result))
    }

    fn complete_payloads(
        &self,
        state: QueryJobState,
        response: Value,
        tool_result: Value,
    ) -> QueryJobSnapshot {
        let mut changed = false;
        let snapshot = if let Ok(mut snapshot) = self.snapshot.lock() {
            if !snapshot.state.is_terminal() {
                snapshot.state = state;
                snapshot.finished_at_unix_ms = Some(unix_time_ms());
                snapshot.response = Some(response);
                snapshot.tool_result = Some(tool_result);
                changed = true;
            }
            snapshot.clone()
        } else {
            self.snapshot()
        };
        if changed {
            if let Ok(mut abort_handle) = self.abort_handle.lock() {
                abort_handle.take();
            }
            self.update_revision.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
        snapshot
    }

    pub fn cancel(&self, startup_role: StartupRole) -> QueryJobSnapshot {
        let mut changed = false;
        let snapshot = if let Ok(mut snapshot) = self.snapshot.lock() {
            if !snapshot.state.is_terminal() {
                snapshot.cancel_requested = true;
                snapshot.state = QueryJobState::Canceled;
                snapshot.finished_at_unix_ms = Some(unix_time_ms());
                let normalized_error = crate::tools::normalize_error_payload_for_role(
                    startup_role,
                    json!({
                        "error": "query canceled by request",
                        "code": "QUERY_CANCELED",
                        "reason": "query_canceled",
                        "sqlstate": "57014",
                    }),
                );
                let response = json!({
                    "ok": false,
                    "error": normalized_error,
                    "meta": {
                        "elapsed_ms": 0
                    },
                });
                snapshot.tool_result = Some(serialize_call_tool_result(
                    &CallToolResult::structured(response.clone()),
                ));
                snapshot.response = Some(response);
                changed = true;
            }
            snapshot.clone()
        } else {
            self.snapshot()
        };
        if changed {
            if let Ok(mut abort_handle) = self.abort_handle.lock()
                && let Some(abort_handle) = abort_handle.take()
            {
                abort_handle.abort();
            }
            self.update_revision.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
        snapshot
    }

    pub fn revision(&self) -> u64 {
        self.update_revision.load(Ordering::SeqCst)
    }

    pub fn snapshot_with_revision(&self) -> (QueryJobSnapshot, u64) {
        loop {
            let revision_before = self.revision();
            let snapshot = if let Ok(snapshot) = self.snapshot.lock() {
                snapshot.clone()
            } else {
                self.snapshot()
            };
            let revision_after = self.revision();
            if revision_before == revision_after {
                return (snapshot, revision_after);
            }
        }
    }

    pub fn register_abort_handle(&self, abort_handle: AbortHandle) {
        let mut should_abort_after_registration = self
            .snapshot
            .lock()
            .map(|snapshot| snapshot.cancel_requested || snapshot.state == QueryJobState::Canceled)
            .unwrap_or(true);

        let mut handle_registered = false;
        if let Ok(mut slot) = self.abort_handle.lock() {
            *slot = Some(abort_handle.clone());
            handle_registered = true;
        } else {
            should_abort_after_registration = true;
        }

        // Re-check after installing the handle to avoid a cancellation race
        // where the request is canceled before the handle becomes visible.
        // Keep lock ordering consistent with cancel/complete by never taking
        // snapshot while holding abort_handle.
        if handle_registered && !should_abort_after_registration {
            should_abort_after_registration = self
                .snapshot
                .lock()
                .map(|snapshot| {
                    snapshot.cancel_requested || snapshot.state == QueryJobState::Canceled
                })
                .unwrap_or(true);
        }

        if should_abort_after_registration {
            if let Ok(mut slot) = self.abort_handle.lock()
                && let Some(registered_abort_handle) = slot.take()
            {
                registered_abort_handle.abort();
            } else {
                abort_handle.abort();
            }
        }
    }

    pub async fn wait_for_update_since(
        &self,
        observed_revision: u64,
        wait_for: Option<Duration>,
    ) -> bool {
        if self.revision() != observed_revision {
            return true;
        }
        let notified = self.notify.notified();
        if self.revision() != observed_revision {
            return true;
        }
        if let Some(wait_for) = wait_for {
            if wait_for.is_zero() {
                return false;
            }
            tokio::time::timeout(wait_for, notified).await.is_ok()
        } else {
            notified.await;
            true
        }
    }
}

#[derive(Debug)]
pub struct QueryJobRegistry {
    jobs: Mutex<HashMap<String, Arc<QueryJobHandle>>>,
    next_id: AtomicU64,
    terminal_ttl: Duration,
    max_entries: usize,
}

impl QueryJobRegistry {
    pub fn with_limits(max_entries: usize, terminal_ttl: Duration) -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            terminal_ttl: terminal_ttl.max(Duration::from_secs(1)),
            max_entries: max_entries.max(1),
        }
    }

    pub fn new() -> Self {
        let max_entries = parse_u64_env(QUERY_JOB_MAX_ENTRIES_ENV)
            .unwrap_or(DEFAULT_QUERY_JOB_MAX_ENTRIES as u64)
            .max(1) as usize;
        let ttl_sec = parse_u64_env(QUERY_JOB_TERMINAL_TTL_SEC_ENV)
            .unwrap_or(DEFAULT_QUERY_JOB_TERMINAL_TTL_SEC)
            .max(1);
        Self::with_limits(max_entries, Duration::from_secs(ttl_sec))
    }

    fn prune_terminal_jobs_locked(&self, jobs: &mut HashMap<String, Arc<QueryJobHandle>>) {
        let now_unix_ms = unix_time_ms();
        let ttl_ms = self.terminal_ttl.as_millis() as u64;
        jobs.retain(|_, handle| {
            let snapshot = handle.snapshot();
            if !snapshot.state.is_terminal() {
                return true;
            }
            let Some(finished_at_unix_ms) = snapshot.finished_at_unix_ms else {
                return true;
            };
            now_unix_ms.saturating_sub(finished_at_unix_ms) <= ttl_ms
        });
    }

    fn evict_oldest_terminal_job_locked(
        &self,
        jobs: &mut HashMap<String, Arc<QueryJobHandle>>,
    ) -> bool {
        let oldest_terminal = jobs
            .iter()
            .filter_map(|(job_id, handle)| {
                let snapshot = handle.snapshot();
                if !snapshot.state.is_terminal() {
                    return None;
                }
                let finished_at = snapshot
                    .finished_at_unix_ms
                    .unwrap_or(snapshot.created_at_unix_ms);
                Some((job_id.clone(), finished_at))
            })
            .min_by_key(|(_, finished_at)| *finished_at);
        if let Some((job_id, _)) = oldest_terminal {
            jobs.remove(&job_id);
            return true;
        }
        false
    }

    pub fn create(&self, query_hash: &str) -> Result<Arc<QueryJobHandle>, QueryJobRegistryError> {
        self.create_with_kind("query", query_hash)
    }

    pub fn create_with_kind(
        &self,
        kind: &str,
        query_hash: &str,
    ) -> Result<Arc<QueryJobHandle>, QueryJobRegistryError> {
        self.create_internal(kind, query_hash, false)
    }

    pub fn create_task(
        &self,
        kind: &str,
        query_hash: &str,
    ) -> Result<Arc<QueryJobHandle>, QueryJobRegistryError> {
        self.create_internal(kind, query_hash, true)
    }

    fn create_internal(
        &self,
        kind: &str,
        query_hash: &str,
        task_managed: bool,
    ) -> Result<Arc<QueryJobHandle>, QueryJobRegistryError> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| QueryJobRegistryError::Unavailable)?;
        self.prune_terminal_jobs_locked(&mut jobs);
        if jobs.len() >= self.max_entries && !self.evict_oldest_terminal_job_locked(&mut jobs) {
            return Err(QueryJobRegistryError::CapacityReached);
        }
        let raw_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let prefix = match kind {
            "query" => "qj",
            "export" => "xj",
            _ => "jb",
        };
        let job_id = format!("{prefix}_{raw_id:016x}");
        let handle = Arc::new(QueryJobHandle::new(
            job_id.clone(),
            kind.to_string(),
            query_hash.to_string(),
            task_managed,
        ));
        jobs.insert(job_id, handle.clone());
        Ok(handle)
    }

    pub fn get(&self, job_id: &str) -> Option<Arc<QueryJobHandle>> {
        let mut jobs = self.jobs.lock().ok()?;
        self.prune_terminal_jobs_locked(&mut jobs);
        jobs.get(job_id).cloned()
    }

    pub fn list_tasks(&self) -> Vec<QueryJobSnapshot> {
        let Ok(mut jobs) = self.jobs.lock() else {
            return Vec::new();
        };
        self.prune_terminal_jobs_locked(&mut jobs);
        let mut snapshots = jobs
            .values()
            .map(|handle| handle.snapshot())
            .filter(|snapshot| snapshot.task_managed)
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|snapshot| std::cmp::Reverse(snapshot.created_at_unix_ms));
        snapshots
    }
}

#[derive(Debug, Clone)]
pub struct ExportArtifactRecord {
    pub handle: String,
    pub uri: String,
    pub format: String,
    pub mime_type: String,
    pub bytes: u64,
    pub row_count: usize,
    pub path: PathBuf,
}

#[derive(Debug, Default)]
struct ExportArtifactRegistryState {
    artifacts: HashMap<String, ExportArtifactRecord>,
    order: VecDeque<String>,
}

#[derive(Debug)]
pub struct ExportArtifactRegistry {
    state: Mutex<ExportArtifactRegistryState>,
    max_entries: usize,
}

impl ExportArtifactRegistry {
    pub fn with_limit(max_entries: usize) -> Self {
        Self {
            state: Mutex::new(ExportArtifactRegistryState::default()),
            max_entries: max_entries.max(1),
        }
    }

    pub fn new() -> Self {
        Self::with_limit(DEFAULT_EXPORT_ARTIFACT_MAX_ENTRIES)
    }

    pub fn register(&self, record: ExportArtifactRecord) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some(existing) = state.artifacts.remove(&record.handle) {
            let _ = std::fs::remove_file(existing.path);
            state.order.retain(|handle| handle != &record.handle);
        }
        state.order.push_back(record.handle.clone());
        state.artifacts.insert(record.handle.clone(), record);
        while state.artifacts.len() > self.max_entries {
            let Some(oldest_handle) = state.order.pop_front() else {
                break;
            };
            if let Some(oldest) = state.artifacts.remove(&oldest_handle) {
                let _ = std::fs::remove_file(oldest.path);
            }
        }
    }

    pub fn list(&self) -> Vec<ExportArtifactRecord> {
        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        state
            .order
            .iter()
            .rev()
            .filter_map(|handle| state.artifacts.get(handle).cloned())
            .collect()
    }

    pub fn get_by_uri(&self, uri: &str) -> Option<ExportArtifactRecord> {
        let handle = uri.rsplit('/').next()?;
        let Ok(state) = self.state.lock() else {
            return None;
        };
        state.artifacts.get(handle).cloned()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PinnedSessionSnapshot {
    pub session_id: String,
    pub backend_pid: i32,
    pub created_at_unix_ms: u64,
    pub last_used_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub idle_ttl_ms: u64,
    pub transaction_open: bool,
    pub last_statement_keyword: Option<String>,
}

struct PinnedSessionEntry {
    session_id: String,
    session: PinnedDbSession,
    backend_pid: i32,
    created_at_unix_ms: u64,
    idle_ttl: Duration,
    last_used_unix_ms: AtomicU64,
    expires_at_unix_ms: AtomicU64,
    generation: AtomicU64,
    transaction_open: AtomicBool,
    last_statement_keyword: Mutex<Option<String>>,
}

impl PinnedSessionEntry {
    fn snapshot(&self) -> PinnedSessionSnapshot {
        let last_statement_keyword = self
            .last_statement_keyword
            .lock()
            .ok()
            .and_then(|value| value.clone());
        PinnedSessionSnapshot {
            session_id: self.session_id.clone(),
            backend_pid: self.backend_pid,
            created_at_unix_ms: self.created_at_unix_ms,
            last_used_unix_ms: self.last_used_unix_ms.load(Ordering::SeqCst),
            expires_at_unix_ms: self.expires_at_unix_ms.load(Ordering::SeqCst),
            idle_ttl_ms: self.idle_ttl.as_millis().min(u64::MAX as u128) as u64,
            transaction_open: self.transaction_open.load(Ordering::SeqCst),
            last_statement_keyword,
        }
    }

    fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    fn touch(&self, statement_keyword: Option<&str>) -> PinnedSessionSnapshot {
        let now = unix_time_ms();
        self.last_used_unix_ms.store(now, Ordering::SeqCst);
        let expires_at = now.saturating_add(self.idle_ttl.as_millis() as u64);
        self.expires_at_unix_ms.store(expires_at, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
        if let Some(keyword) = statement_keyword {
            if matches!(keyword, "begin" | "start") {
                self.transaction_open.store(true, Ordering::SeqCst);
            } else if matches!(keyword, "commit" | "rollback") {
                self.transaction_open.store(false, Ordering::SeqCst);
            }
            if let Ok(mut last) = self.last_statement_keyword.lock() {
                *last = Some(keyword.to_ascii_uppercase());
            }
        }
        self.snapshot()
    }
}

pub struct PinnedSessionRegistry {
    entries: Mutex<HashMap<String, Arc<PinnedSessionEntry>>>,
    next_id: AtomicU64,
}

impl PinnedSessionRegistry {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn insert(&self, session: PinnedDbSession, idle_ttl: Duration) -> PinnedSessionSnapshot {
        let session_id = format!("ps_{:016x}", self.next_id.fetch_add(1, Ordering::SeqCst));
        let now = unix_time_ms();
        let entry = Arc::new(PinnedSessionEntry {
            session_id: session_id.clone(),
            backend_pid: session.backend_pid(),
            session,
            created_at_unix_ms: now,
            idle_ttl,
            last_used_unix_ms: AtomicU64::new(now),
            expires_at_unix_ms: AtomicU64::new(now.saturating_add(idle_ttl.as_millis() as u64)),
            generation: AtomicU64::new(0),
            transaction_open: AtomicBool::new(false),
            last_statement_keyword: Mutex::new(None),
        });
        let snapshot = entry.snapshot();
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(session_id, entry);
        }
        snapshot
    }

    fn get(&self, session_id: &str) -> Option<Arc<PinnedSessionEntry>> {
        self.entries.lock().ok()?.get(session_id).cloned()
    }

    fn remove(&self, session_id: &str) -> Option<Arc<PinnedSessionEntry>> {
        self.entries.lock().ok()?.remove(session_id)
    }

    pub fn close(&self, session_id: &str) -> Option<PinnedSessionSnapshot> {
        let entry = self.remove(session_id)?;
        entry.session.close();
        Some(entry.snapshot())
    }

    pub fn expire_if_generation_matches(
        &self,
        session_id: &str,
        generation: u64,
    ) -> Option<PinnedSessionSnapshot> {
        let entry = self.get(session_id)?;
        if entry.current_generation() != generation {
            return None;
        }
        let now = unix_time_ms();
        if now < entry.expires_at_unix_ms.load(Ordering::SeqCst) {
            return None;
        }
        let snapshot = entry.snapshot();
        self.remove(session_id)?;
        entry.session.close();
        Some(snapshot)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExtensionCapability {
    Hypopg,
    PgStatStatements,
}

impl ExtensionCapability {
    pub const fn extension_name(self) -> &'static str {
        match self {
            Self::Hypopg => "hypopg",
            Self::PgStatStatements => "pg_stat_statements",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExtensionUnavailableStatus {
    pub guard_reason: &'static str,
    pub message: String,
}

#[derive(Debug, Clone)]
struct TimedUnavailableStatus {
    guard_reason: &'static str,
    message: String,
    recorded_at: Instant,
}

#[derive(Debug)]
pub struct ExtensionUnavailableCache {
    ttl: Duration,
    max_entries: usize,
    entries: Mutex<HashMap<ExtensionCapability, TimedUnavailableStatus>>,
}

impl ExtensionUnavailableCache {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            ttl,
            max_entries: max_entries.max(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_fresh(&self, extension: &ExtensionCapability) -> Option<ExtensionUnavailableStatus> {
        let mut entries = self.entries.lock().ok()?;
        prune_unavailable_entries(&mut entries, self.ttl);
        entries
            .get(extension)
            .map(|cached| ExtensionUnavailableStatus {
                guard_reason: cached.guard_reason,
                message: cached.message.clone(),
            })
    }

    pub fn record(
        &self,
        extension: ExtensionCapability,
        guard_reason: &'static str,
        message: String,
    ) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        prune_unavailable_entries(&mut entries, self.ttl);
        if !entries.contains_key(&extension) && entries.len() >= self.max_entries {
            return;
        }
        entries.insert(
            extension,
            TimedUnavailableStatus {
                guard_reason,
                message,
                recorded_at: Instant::now(),
            },
        );
    }

    pub fn clear(&self, extension: &ExtensionCapability) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        entries.remove(extension);
    }
}

fn prune_unavailable_entries(
    entries: &mut HashMap<ExtensionCapability, TimedUnavailableStatus>,
    ttl: Duration,
) {
    if ttl.is_zero() {
        entries.clear();
        return;
    }
    let now = Instant::now();
    entries.retain(|_, status| now.duration_since(status.recorded_at) <= ttl);
}

#[derive(Clone)]
pub struct PostgresMcp {
    pub db: Arc<DbEngine>,
    pub extension_guard: Arc<CapabilityGuard<ExtensionCapability>>,
    pub extension_unavailable_cache: Arc<ExtensionUnavailableCache>,
    pub metadata_policy_mode: MetadataPolicyMode,
    pub metadata_schema_allow: Arc<Vec<String>>,
    pub metadata_schema_deny: Arc<Vec<String>>,
    pub startup_role: StartupRole,
    pub enable_admin_sql: bool,
    pub expose_execute_sql: bool,
    pub startup_degraded_read_only: bool,
    pub startup_degraded_reason: Option<String>,
    pub startup_missing_dependencies: Arc<Vec<String>>,
    circuit_breaker_config: ToolCircuitBreakerConfig,
    circuit_state: Arc<Mutex<HashMap<String, ToolCircuitState>>>,
    pub response_mode: ResponseMode,
    pub response_output_mode: ResponseOutputMode,
    pub response_output_mode_auto_tabular: ResponseAutoTabularMode,
    pub response_page_size: usize,
    pub query_jobs: Arc<QueryJobRegistry>,
    pub export_artifacts: Arc<ExportArtifactRegistry>,
    pub pinned_sessions: Arc<PinnedSessionRegistry>,
    pub pagination_cursor_ttl: Duration,
    pub pagination_cursor_signing_key: Arc<[u8; 32]>,
    pub advisor_external: AdvisorExternalConfig,
    pub advisor_external_runner: Arc<dyn AdvisorExternalRunner>,
    tool_router: ToolRouter<PostgresMcp>,
}

impl PostgresMcp {
    pub fn new(db: Arc<DbEngine>) -> Self {
        Self::with_runtime_options(
            db,
            ResponseMode::V2,
            ResponseOutputMode::DataOnly,
            ResponseAutoTabularMode::Rows,
            100,
            AdvisorExternalConfig::disabled(),
        )
    }

    pub fn with_response_contract(
        db: Arc<DbEngine>,
        response_mode: ResponseMode,
        response_output_mode: ResponseOutputMode,
        response_output_mode_auto_tabular: ResponseAutoTabularMode,
        response_page_size: usize,
    ) -> Self {
        Self::with_runtime_options(
            db,
            response_mode,
            response_output_mode,
            response_output_mode_auto_tabular,
            response_page_size,
            AdvisorExternalConfig::disabled(),
        )
    }

    pub fn with_runtime_options(
        db: Arc<DbEngine>,
        response_mode: ResponseMode,
        response_output_mode: ResponseOutputMode,
        response_output_mode_auto_tabular: ResponseAutoTabularMode,
        response_page_size: usize,
        advisor_external: AdvisorExternalConfig,
    ) -> Self {
        let tool_router = Self::tool_router_postgres();
        Self {
            db,
            extension_guard: Arc::new(CapabilityGuard::new(
                EXTENSION_GUARD_TTL,
                EXTENSION_GUARD_MAX_ENTRIES,
            )),
            extension_unavailable_cache: Arc::new(ExtensionUnavailableCache::new(
                EXTENSION_UNAVAILABLE_TTL,
                EXTENSION_UNAVAILABLE_MAX_ENTRIES,
            )),
            metadata_policy_mode: MetadataPolicyMode::Full,
            metadata_schema_allow: Arc::new(Vec::new()),
            metadata_schema_deny: Arc::new(Vec::new()),
            startup_role: StartupRole::Runtime,
            enable_admin_sql: false,
            expose_execute_sql: false,
            startup_degraded_read_only: false,
            startup_degraded_reason: None,
            startup_missing_dependencies: Arc::new(Vec::new()),
            circuit_breaker_config: ToolCircuitBreakerConfig::from_env(),
            circuit_state: Arc::new(Mutex::new(HashMap::new())),
            response_mode,
            response_output_mode,
            response_output_mode_auto_tabular,
            response_page_size: response_page_size.max(1),
            query_jobs: Arc::new(QueryJobRegistry::new()),
            export_artifacts: Arc::new(ExportArtifactRegistry::new()),
            pinned_sessions: Arc::new(PinnedSessionRegistry::new()),
            pagination_cursor_ttl: DEFAULT_PAGINATION_CURSOR_TTL,
            pagination_cursor_signing_key: Arc::new(generate_ephemeral_cursor_signing_key()),
            advisor_external,
            advisor_external_runner: default_advisor_external_runner(),
            tool_router,
        }
    }

    pub fn tool_names(&self) -> Vec<String> {
        self.discoverable_tools()
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect()
    }

    pub fn tool_schema_snapshot(&self) -> Result<Value, serde_json::Error> {
        tool_schema_snapshot_value(&self.discoverable_tools())
    }

    pub(crate) fn discoverable_tools(&self) -> Vec<Tool> {
        self.tool_router
            .list_all()
            .into_iter()
            .filter(|tool| self.enable_admin_sql || tool.name != "admin_sql")
            .filter(|tool| self.expose_execute_sql || tool.name != "execute_sql")
            .collect()
    }

    fn task_enabled_tool(tool_name: &str) -> bool {
        matches!(
            tool_name,
            "query_sql"
                | "query_tuples"
                | "render_sql"
                | "analyze_db_health"
                | "analyze_query_indexes"
                | "analyze_workload_indexes"
                | "export_sql"
        )
    }

    fn task_for_snapshot(&self, snapshot: &QueryJobSnapshot) -> Task {
        let status = match snapshot.state {
            QueryJobState::Pending | QueryJobState::Running => TaskStatus::Working,
            QueryJobState::Succeeded => TaskStatus::Completed,
            QueryJobState::Failed => TaskStatus::Failed,
            QueryJobState::Canceled => TaskStatus::Cancelled,
        };
        let last_updated_unix_ms = snapshot
            .finished_at_unix_ms
            .or(snapshot.started_at_unix_ms)
            .unwrap_or(snapshot.created_at_unix_ms);
        let status_message = match snapshot.state {
            QueryJobState::Pending => Some(format!("queued {}", snapshot.kind)),
            QueryJobState::Running => Some(format!("running {}", snapshot.kind)),
            QueryJobState::Succeeded => Some(format!("completed {}", snapshot.kind)),
            QueryJobState::Failed => Some(format!("failed {}", snapshot.kind)),
            QueryJobState::Canceled => Some(format!("cancelled {}", snapshot.kind)),
        };
        Task::new(
            snapshot.job_id.clone(),
            status,
            iso8601_from_unix_ms(snapshot.created_at_unix_ms),
            iso8601_from_unix_ms(last_updated_unix_ms),
        )
        .with_poll_interval(1000)
        .with_ttl(self.query_jobs.terminal_ttl.as_millis() as u64)
        .with_status_message(status_message.unwrap_or_default())
    }

    async fn emit_progress(
        context: &RequestContext<RoleServer>,
        progress_token: Option<rmcp::model::ProgressToken>,
        progress: f64,
        message: &str,
    ) {
        let Some(progress_token) = progress_token else {
            return;
        };
        let notification = ProgressNotificationParam::new(progress_token, progress)
            .with_total(100.0)
            .with_message(message.to_string());
        let _ = context.peer.notify_progress(notification).await;
    }

    pub fn register_export_artifact(&self, record: ExportArtifactRecord) {
        self.export_artifacts.register(record);
    }

    pub fn default_pinned_session_idle_ttl(&self) -> Duration {
        DEFAULT_PINNED_SESSION_IDLE_TTL
    }

    pub fn schedule_pinned_session_expiry(
        &self,
        session_id: String,
        generation: u64,
        ttl: Duration,
    ) {
        let registry = self.pinned_sessions.clone();
        tokio::spawn(async move {
            tokio::time::sleep(ttl).await;
            let _ = registry.expire_if_generation_matches(&session_id, generation);
        });
    }

    pub async fn open_pinned_session(
        &self,
        idle_ttl: Duration,
    ) -> Result<PinnedSessionSnapshot, crate::db::DbError> {
        let session = self.db.open_pinned_session().await?;
        let snapshot = self.pinned_sessions.insert(session, idle_ttl);
        self.schedule_pinned_session_expiry(snapshot.session_id.clone(), 0, idle_ttl);
        Ok(snapshot)
    }

    pub fn pinned_session(&self, session_id: &str) -> Option<PinnedDbSession> {
        self.pinned_sessions
            .get(session_id)
            .map(|entry| entry.session.clone())
    }

    pub fn pinned_session_snapshot(&self, session_id: &str) -> Option<PinnedSessionSnapshot> {
        self.pinned_sessions
            .get(session_id)
            .map(|entry| entry.snapshot())
    }

    pub fn touch_pinned_session(
        &self,
        session_id: &str,
        statement_keyword: Option<&str>,
    ) -> Option<PinnedSessionSnapshot> {
        let entry = self.pinned_sessions.get(session_id)?;
        let snapshot = entry.touch(statement_keyword);
        self.schedule_pinned_session_expiry(
            snapshot.session_id.clone(),
            entry.current_generation(),
            entry.idle_ttl,
        );
        Some(snapshot)
    }

    pub fn close_pinned_session(&self, session_id: &str) -> Option<PinnedSessionSnapshot> {
        self.pinned_sessions.close(session_id)
    }

    pub(crate) fn export_resources(&self) -> Vec<Resource> {
        self.export_artifacts
            .list()
            .into_iter()
            .map(|artifact| {
                Resource::new(
                    RawResource::new(&artifact.uri, format!("export {}", artifact.handle))
                        .with_title(format!("Export artifact {}", artifact.handle))
                        .with_description(format!(
                            "Query export artifact in {} format ({} rows)",
                            artifact.format, artifact.row_count
                        ))
                        .with_mime_type(&artifact.mime_type)
                        .with_size(artifact.bytes.min(u32::MAX as u64) as u32),
                    None,
                )
            })
            .collect()
    }

    pub(crate) fn read_export_resource_uri(
        &self,
        uri: &str,
    ) -> Result<ReadResourceResult, rmcp::ErrorData> {
        let Some(artifact) = self.export_artifacts.get_by_uri(uri) else {
            return Err(crate::McpError::resource_not_found(
                format!("resource not found: {uri}"),
                None,
            ));
        };
        let text = std::fs::read_to_string(&artifact.path).map_err(|err| {
            crate::McpError::resource_not_found(format!("resource unavailable: {uri}: {err}"), None)
        })?;
        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(text, uri).with_mime_type(artifact.mime_type),
        ]))
    }

    pub fn metadata_access_denied(&self) -> bool {
        self.metadata_policy_mode == MetadataPolicyMode::Denied
    }

    pub fn configure_pagination_cursor_security(
        &mut self,
        ttl: Duration,
        signing_key_material: Option<&str>,
    ) {
        self.pagination_cursor_ttl = ttl.max(Duration::from_secs(1));
        if let Some(raw) = signing_key_material
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
        {
            self.pagination_cursor_signing_key = Arc::new(derive_cursor_signing_key(raw));
        }
    }

    pub fn metadata_schema_visible(&self, schema_name: &str) -> bool {
        if self.metadata_access_denied() {
            return false;
        }
        let normalized = schema_name.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return false;
        }
        if self
            .metadata_schema_deny
            .iter()
            .any(|schema| schema == &normalized)
        {
            return false;
        }
        match self.metadata_policy_mode {
            MetadataPolicyMode::Full => true,
            MetadataPolicyMode::Limited => self
                .metadata_schema_allow
                .iter()
                .any(|schema| schema == &normalized),
            MetadataPolicyMode::Denied => false,
        }
    }

    pub fn set_startup_degraded_read_only(
        &mut self,
        reason: Option<String>,
        missing_dependencies: Vec<String>,
    ) {
        self.startup_degraded_read_only = true;
        self.startup_degraded_reason = reason;
        self.startup_missing_dependencies = Arc::new(missing_dependencies);
    }

    pub fn startup_capabilities_meta(&self) -> Value {
        json!({
            "startup_state": if self.startup_degraded_read_only { "degraded_read_only" } else { "healthy" },
            "degraded_read_only": self.startup_degraded_read_only,
            "read_only_sql": true,
            "read_write_sql": !self.startup_degraded_read_only,
            "metadata_discovery": !self.metadata_access_denied(),
            "reason": self.startup_degraded_reason,
            "missing_dependencies": self.startup_missing_dependencies.as_ref(),
        })
    }

    fn circuit_group_key<'a>(&self, tool_name: &'a str) -> &'a str {
        match tool_name {
            "query_sql" | "query_tuples" | "render_sql" | "describe_sql" | "admin_sql"
            | "query_job_start" | "export_job_start" => "sql_surface",
            _ => tool_name,
        }
    }

    fn circuit_open_retry_after_ms(&self, tool_name: &str) -> Option<u64> {
        if !self.circuit_breaker_config.enabled {
            return None;
        }
        let Ok(mut states) = self.circuit_state.lock() else {
            return None;
        };
        let circuit_key = self.circuit_group_key(tool_name);
        let now = Instant::now();
        if let Some(state) = states.get_mut(circuit_key)
            && let Some(open_until) = state.open_until
        {
            if open_until > now {
                return Some(open_until.duration_since(now).as_millis() as u64);
            }
            state.open_until = None;
            state.consecutive_retryable_failures = 0;
        }
        None
    }

    fn record_tool_outcome(&self, tool_name: &str, outcome: ToolCallOutcome) {
        if !self.circuit_breaker_config.enabled {
            return;
        }
        let Ok(mut states) = self.circuit_state.lock() else {
            return;
        };
        let now = Instant::now();
        let circuit_key = self.circuit_group_key(tool_name);
        let state = states
            .entry(circuit_key.to_string())
            .or_insert_with(ToolCircuitState::closed);
        if let Some(open_until) = state.open_until
            && open_until <= now
        {
            state.open_until = None;
            state.consecutive_retryable_failures = 0;
        }
        match outcome {
            ToolCallOutcome::Success | ToolCallOutcome::NonRetryableFailure => {
                state.consecutive_retryable_failures = 0;
                if state.open_until.is_none() {
                    return;
                }
            }
            ToolCallOutcome::RetryableFailure => {
                state.consecutive_retryable_failures =
                    state.consecutive_retryable_failures.saturating_add(1);
                if state.consecutive_retryable_failures
                    >= self.circuit_breaker_config.failure_threshold
                {
                    state.open_until = Some(now + self.circuit_breaker_config.cooldown);
                    state.consecutive_retryable_failures = 0;
                }
            }
        }
    }

    fn classify_outcome(
        tool_name: &str,
        result: &Result<CallToolResult, rmcp::ErrorData>,
    ) -> ToolCallOutcome {
        match result {
            Err(err) => {
                let err_text = err.to_string();
                if extract_execute_sql_validation_summary(
                    tool_name,
                    "router_deserialize",
                    &err_text,
                )
                .is_some()
                {
                    return ToolCallOutcome::NonRetryableFailure;
                }
                ToolCallOutcome::RetryableFailure
            }
            Ok(tool_result) => {
                let Some(summary) = extract_contract_error_summary(tool_result) else {
                    return ToolCallOutcome::Success;
                };
                if summary.retryable.unwrap_or(false) {
                    return ToolCallOutcome::RetryableFailure;
                }
                ToolCallOutcome::NonRetryableFailure
            }
        }
    }

    fn circuit_open_result(&self, tool_name: &str, retry_after_ms: u64) -> CallToolResult {
        let detail_level = match self.startup_role {
            StartupRole::Runtime => "minimal",
            StartupRole::Migrator => "detailed",
        };
        let mut hasher = Sha256::new();
        hasher.update(b"TOOL_CIRCUIT_OPEN");
        hasher.update([0u8]);
        hasher.update(b"circuit_breaker_open");
        hasher.update([0u8]);
        hasher.update(detail_level.as_bytes());
        let digest = hasher.finalize();
        let fingerprint = format!("err_{}", &format!("{digest:x}")[..12]);
        let error_message =
            format!("Tool circuit breaker is open for {tool_name}; retry after {retry_after_ms}ms");
        CallToolResult::structured(json!({
            "ok": false,
            "error": {
                "error": error_message,
                "code": "TOOL_CIRCUIT_OPEN",
                "reason": "circuit_breaker_open",
                "detail_level": detail_level,
                "retryable": true,
                "fingerprint": fingerprint,
                "retry_after_ms": retry_after_ms
            },
            "meta": {
                "elapsed_ms": 0,
                "capabilities": self.startup_capabilities_meta()
            }
        }))
    }
}

fn derive_cursor_signing_key(raw: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    key
}

fn generate_ephemeral_cursor_signing_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    if getrandom::fill(&mut key).is_ok() {
        return key;
    }
    let epoch_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let fallback_seed = format!("postgres-mcp:{}:{}", std::process::id(), epoch_nanos);
    derive_cursor_signing_key(&fallback_seed)
}

fn schema_allows_null(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            if matches!(object.get("const"), Some(Value::Null)) {
                return true;
            }
            match object.get("type") {
                Some(Value::String(raw)) => raw == "null",
                Some(Value::Array(items)) => items.iter().any(|item| item.as_str() == Some("null")),
                _ => false,
            }
        }
        _ => false,
    }
}

fn make_type_nullable(type_value: &mut Value) {
    match type_value {
        Value::String(raw_type) => {
            if raw_type != "null" {
                let existing = raw_type.clone();
                *type_value = Value::Array(vec![
                    Value::String(existing),
                    Value::String("null".to_string()),
                ]);
            }
        }
        Value::Array(types) => {
            if !types.iter().any(|item| item.as_str() == Some("null")) {
                types.push(Value::String("null".to_string()));
            }
        }
        _ => {}
    }
}

fn normalize_nullable_marker(schema_object: &mut serde_json::Map<String, Value>) {
    let is_nullable = schema_object
        .get("nullable")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !is_nullable {
        return;
    }

    if matches!(schema_object.get("const"), Some(Value::Null)) {
        schema_object.remove("nullable");
        return;
    }

    if let Some(type_value) = schema_object.get_mut("type") {
        make_type_nullable(type_value);
        schema_object.remove("nullable");
        return;
    }

    if let Some(Value::Array(any_of)) = schema_object.get_mut("anyOf") {
        if !any_of.iter().any(schema_allows_null) {
            any_of.push(serde_json::json!({ "type": "null" }));
        }
        schema_object.remove("nullable");
        return;
    }

    if let Some(Value::Array(one_of)) = schema_object.get_mut("oneOf") {
        if !one_of.iter().any(schema_allows_null) {
            one_of.push(serde_json::json!({ "type": "null" }));
        }
        schema_object.remove("nullable");
        return;
    }

    schema_object.remove("nullable");
}

fn sanitize_schema_value(schema_value: &mut Value) {
    match schema_value {
        Value::Object(object) => {
            for child in object.values_mut() {
                sanitize_schema_value(child);
            }
            if object
                .get("format")
                .and_then(Value::as_str)
                .map(|raw| matches!(raw, "uint" | "uint32" | "uint64"))
                .unwrap_or(false)
            {
                object.remove("format");
            }
            normalize_nullable_marker(object);
        }
        Value::Array(values) => {
            for value in values.iter_mut() {
                sanitize_schema_value(value);
            }
        }
        _ => {}
    }
}

pub(crate) fn sanitize_tool_schemas_for_mcp(tools: Vec<Tool>) -> Vec<Tool> {
    tools
        .into_iter()
        .map(|mut tool| {
            let mut input_schema = Value::Object((*tool.input_schema).clone());
            sanitize_schema_value(&mut input_schema);
            if let Value::Object(input_schema) = input_schema {
                tool.input_schema = Arc::new(input_schema);
            }

            if let Some(output_schema) = tool.output_schema.clone() {
                let mut output_schema_value = Value::Object((*output_schema).clone());
                sanitize_schema_value(&mut output_schema_value);
                if let Value::Object(output_schema_object) = output_schema_value {
                    tool.output_schema = Some(Arc::new(output_schema_object));
                }
            }
            tool
        })
        .collect()
}

impl ServerHandler for PostgresMcp {
    fn get_info(&self) -> ServerInfo {
        rmcp_models::server_info(
            ProtocolVersion::V_2024_11_05,
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_tasks()
                .build(),
            Implementation::from_build_env(),
            Some(
                "PostgreSQL MCP Rust stdio server. Use tools for schema, query analysis, and health checks."
                    .to_string(),
            ),
        )
    }

    fn enqueue_task(
        &self,
        mut request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CreateTaskResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            let tool_name = request.name.to_string();
            if !Self::task_enabled_tool(&tool_name) {
                return Err(crate::McpError::invalid_params(
                    format!("tool `{tool_name}` does not support task augmentation"),
                    None,
                ));
            }

            let query_hash = telemetry_fingerprint(&format!(
                "{}|{}",
                tool_name,
                serde_json::to_string(&request.arguments).unwrap_or_default()
            ));
            let job = self
                .query_jobs
                .create_task(&tool_name, &query_hash)
                .map_err(|err| crate::McpError::internal_error(err.message().to_string(), None))?;

            let progress_token = request
                .meta
                .as_ref()
                .and_then(|meta| meta.get_progress_token());
            let task = self.task_for_snapshot(&job.snapshot());

            let server = self.clone();
            let job_handle = job.clone();
            let mut task_context = RequestContext::new(context.id.clone(), context.peer.clone());
            task_context.meta = context.meta.clone();
            task_context.extensions = context.extensions.clone();
            let mut task_progress_context =
                RequestContext::new(context.id.clone(), context.peer.clone());
            task_progress_context.meta = context.meta.clone();
            task_progress_context.extensions = context.extensions.clone();
            request.task = None;

            tokio::spawn(async move {
                PostgresMcp::emit_progress(
                    &task_progress_context,
                    progress_token.clone(),
                    5.0,
                    "queued",
                )
                .await;
                job_handle.mark_running();
                PostgresMcp::emit_progress(
                    &task_progress_context,
                    progress_token.clone(),
                    35.0,
                    "executing",
                )
                .await;
                let result = match server
                    .tool_router
                    .call(ToolCallContext::new(&server, request, task_context))
                    .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        let error = err.to_string();
                        CallToolResult::structured(json!({
                            "ok": false,
                            "error": {
                                "error": error,
                                "code": "TASK_EXECUTION_FAILED",
                                "reason": "task_execution_failed",
                            },
                            "meta": {"elapsed_ms": 0}
                        }))
                    }
                };
                let succeeded = extract_contract_error_summary(&result).is_none()
                    && result.is_error != Some(true);
                let structured = result.structured_content.clone().unwrap_or_else(|| {
                    json!({
                        "ok": succeeded,
                        "meta": {
                            "elapsed_ms": 0
                        }
                    })
                });
                PostgresMcp::emit_progress(
                    &task_progress_context,
                    progress_token.clone(),
                    85.0,
                    "finalizing",
                )
                .await;
                let state = if succeeded {
                    QueryJobState::Succeeded
                } else {
                    QueryJobState::Failed
                };
                job_handle.complete_structured(state, structured, result);
                PostgresMcp::emit_progress(
                    &task_progress_context,
                    progress_token,
                    100.0,
                    if succeeded { "completed" } else { "failed" },
                )
                .await;
            });

            Ok(CreateTaskResult::new(task))
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_ {
        let tools = sanitize_tool_schemas_for_mcp(self.discoverable_tools());
        std::future::ready(Ok(ListToolsResult {
            meta: None,
            tools,
            next_cursor: None,
        }))
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListResourcesResult, rmcp::ErrorData>> + Send + '_ {
        let resources = self.export_resources();
        std::future::ready(Ok(ListResourcesResult {
            meta: None,
            resources,
            next_cursor: None,
        }))
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ReadResourceResult, rmcp::ErrorData>> + Send + '_ {
        std::future::ready(self.read_export_resource_uri(&request.uri))
    }

    fn list_tasks(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListTasksResult, rmcp::ErrorData>> + Send + '_ {
        std::future::ready(Ok(ListTasksResult::new(
            self.query_jobs
                .list_tasks()
                .into_iter()
                .map(|snapshot| self.task_for_snapshot(&snapshot))
                .collect(),
        )))
    }

    fn get_task_info(
        &self,
        request: GetTaskInfoParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<GetTaskResult, rmcp::ErrorData>> + Send + '_ {
        std::future::ready(match self.query_jobs.get(&request.task_id) {
            Some(job) => {
                let snapshot = job.snapshot();
                if !snapshot.task_managed {
                    Err(crate::McpError::invalid_params("task not found", None))
                } else {
                    Ok(GetTaskResult {
                        meta: None,
                        task: self.task_for_snapshot(&snapshot),
                    })
                }
            }
            None => Err(crate::McpError::invalid_params("task not found", None)),
        })
    }

    fn get_task_result(
        &self,
        request: GetTaskResultParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<GetTaskPayloadResult, rmcp::ErrorData>> + Send + '_ {
        std::future::ready(match self.query_jobs.get(&request.task_id) {
            Some(job) => {
                let snapshot = job.snapshot();
                if !snapshot.task_managed {
                    Err(crate::McpError::invalid_params("task not found", None))
                } else if !snapshot.state.is_terminal() {
                    Err(crate::McpError::invalid_params(
                        "task result is not ready; call tasks/get until the task reaches a terminal state",
                        None,
                    ))
                } else {
                    Ok(GetTaskPayloadResult::new(
                        snapshot.tool_result.unwrap_or_else(|| {
                            serialize_call_tool_result(&CallToolResult::structured(
                                snapshot.response.unwrap_or_else(|| {
                                    json!({
                                        "ok": false,
                                        "error": {
                                            "error": "task result unavailable",
                                            "code": "TASK_RESULT_UNAVAILABLE",
                                            "reason": "task_result_unavailable",
                                        },
                                        "meta": {
                                            "elapsed_ms": 0
                                        }
                                    })
                                }),
                            ))
                        }),
                    ))
                }
            }
            None => Err(crate::McpError::invalid_params("task not found", None)),
        })
    }

    fn cancel_task(
        &self,
        request: CancelTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CancelTaskResult, rmcp::ErrorData>> + Send + '_ {
        std::future::ready(match self.query_jobs.get(&request.task_id) {
            Some(job) => {
                let snapshot = job.snapshot();
                if !snapshot.task_managed {
                    Err(crate::McpError::invalid_params("task not found", None))
                } else {
                    let snapshot = job.cancel(self.startup_role);
                    Ok(CancelTaskResult {
                        meta: None,
                        task: self.task_for_snapshot(&snapshot),
                    })
                }
            }
            None => Err(crate::McpError::invalid_params("task not found", None)),
        })
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_ {
        let tool_name = request.name.clone();
        let progress_token = request
            .meta
            .as_ref()
            .and_then(|meta| meta.get_progress_token());
        let mut progress_context = RequestContext::new(context.id.clone(), context.peer.clone());
        progress_context.meta = context.meta.clone();
        progress_context.extensions = context.extensions.clone();
        let tool_context = ToolCallContext::new(self, request, context);
        async move {
            Self::emit_progress(&progress_context, progress_token.clone(), 5.0, "queued").await;
            mcp_toolkit_observability::emit_event(
                mcp_toolkit_observability::Level::INFO,
                "postgres_mcp.tool.start",
                &mcp_toolkit_observability::EventContext::new().with_tool_name(&tool_name),
                &[mcp_toolkit_observability::safe_text("tool", &tool_name)],
            );

            if tool_name == "admin_sql" && !self.enable_admin_sql {
                return Err(crate::McpError::invalid_params("tool not found", None));
            }

            if let Some(retry_after_ms) = self.circuit_open_retry_after_ms(&tool_name) {
                mcp_toolkit_observability::emit_event(
                    mcp_toolkit_observability::Level::WARN,
                    "postgres_mcp.tool.circuit_open",
                    &mcp_toolkit_observability::EventContext::new().with_tool_name(&tool_name),
                    &[
                        mcp_toolkit_observability::safe_text("tool", &tool_name),
                        mcp_toolkit_observability::safe_text(
                            "retry_after_ms",
                            retry_after_ms.to_string(),
                        ),
                    ],
                );
                return Ok(self.circuit_open_result(&tool_name, retry_after_ms));
            }

            Self::emit_progress(
                tool_context.request_context(),
                progress_token.clone(),
                35.0,
                "executing",
            )
            .await;
            let result = self.tool_router.call(tool_context).await;
            let outcome = Self::classify_outcome(&tool_name, &result);
            self.record_tool_outcome(&tool_name, outcome);
            Self::emit_progress(
                &progress_context,
                progress_token.clone(),
                85.0,
                "finalizing",
            )
            .await;

            match &result {
                Err(err) => {
                    let err_text = err.to_string();
                    let err_fingerprint = telemetry_fingerprint(&err_text);
                    let validation = extract_execute_sql_validation_summary(
                        &tool_name,
                        "router_deserialize",
                        &err_text,
                    );
                    let mut fields = vec![
                        mcp_toolkit_observability::safe_text("tool", &tool_name),
                        mcp_toolkit_observability::safe_text("kind", "router_error"),
                        mcp_toolkit_observability::safe_text("fingerprint", &err_fingerprint),
                    ];
                    if let Some(validation) = validation {
                        fields.push(mcp_toolkit_observability::safe_text(
                            "code",
                            "INVALID_REQUEST",
                        ));
                        fields.push(mcp_toolkit_observability::safe_text(
                            "reason",
                            "invalid_request",
                        ));
                        fields.push(mcp_toolkit_observability::safe_text("retryable", "false"));
                        fields.push(mcp_toolkit_observability::safe_text(
                            "validation_surface",
                            validation.validation_surface,
                        ));
                        fields.push(mcp_toolkit_observability::safe_text(
                            "validation_param",
                            validation.validation_param,
                        ));
                        fields.push(mcp_toolkit_observability::safe_text(
                            "validation_kind",
                            validation.validation_kind,
                        ));
                        fields.push(mcp_toolkit_observability::safe_text(
                            "hint_family",
                            validation.hint_family,
                        ));
                        fields.push(mcp_toolkit_observability::safe_text(
                            "rejected_value",
                            validation.rejected_value,
                        ));
                    }
                    mcp_toolkit_observability::emit_event(
                        mcp_toolkit_observability::Level::WARN,
                        "postgres_mcp.tool.error",
                        &mcp_toolkit_observability::EventContext::new().with_tool_name(&tool_name),
                        &fields,
                    );
                    if telemetry_debug_enabled() {
                        mcp_toolkit_observability::emit_event(
                            mcp_toolkit_observability::Level::DEBUG,
                            "postgres_mcp.tool.error.debug",
                            &mcp_toolkit_observability::EventContext::new()
                                .with_tool_name(&tool_name),
                            &[
                                mcp_toolkit_observability::safe_text("tool", &tool_name),
                                mcp_toolkit_observability::safe_text("kind", "router_error"),
                                mcp_toolkit_observability::safe_text(
                                    "message",
                                    clip_for_telemetry(&err_text),
                                ),
                            ],
                        );
                    }
                }
                Ok(tool_result) => {
                    if let Some(summary) = extract_contract_error_summary(tool_result) {
                        let retryable = summary
                            .retryable
                            .map(|value| if value { "true" } else { "false" })
                            .unwrap_or("unknown");
                        let validation = summary.message.as_deref().and_then(|message| {
                            extract_execute_sql_validation_summary(
                                &tool_name,
                                "tool_validation",
                                message,
                            )
                        });
                        let mut fields = vec![
                            mcp_toolkit_observability::safe_text("tool", &tool_name),
                            mcp_toolkit_observability::safe_text("kind", "contract_error"),
                            mcp_toolkit_observability::safe_text("code", &summary.code),
                            mcp_toolkit_observability::safe_text("reason", &summary.reason),
                            mcp_toolkit_observability::safe_text(
                                "fingerprint",
                                &summary.fingerprint,
                            ),
                            mcp_toolkit_observability::safe_text("retryable", retryable),
                            mcp_toolkit_observability::safe_text(
                                "detail_level",
                                &summary.detail_level,
                            ),
                        ];
                        if let Some(validation) = validation {
                            fields.push(mcp_toolkit_observability::safe_text(
                                "validation_surface",
                                validation.validation_surface,
                            ));
                            fields.push(mcp_toolkit_observability::safe_text(
                                "validation_param",
                                validation.validation_param,
                            ));
                            fields.push(mcp_toolkit_observability::safe_text(
                                "validation_kind",
                                validation.validation_kind,
                            ));
                            fields.push(mcp_toolkit_observability::safe_text(
                                "hint_family",
                                validation.hint_family,
                            ));
                            fields.push(mcp_toolkit_observability::safe_text(
                                "rejected_value",
                                validation.rejected_value,
                            ));
                        }
                        mcp_toolkit_observability::emit_event(
                            mcp_toolkit_observability::Level::WARN,
                            "postgres_mcp.tool.error",
                            &mcp_toolkit_observability::EventContext::new()
                                .with_tool_name(&tool_name),
                            &fields,
                        );
                        if telemetry_debug_enabled()
                            && let Some(message) = summary.message.as_deref()
                        {
                            mcp_toolkit_observability::emit_event(
                                mcp_toolkit_observability::Level::DEBUG,
                                "postgres_mcp.tool.error.debug",
                                &mcp_toolkit_observability::EventContext::new()
                                    .with_tool_name(&tool_name),
                                &[
                                    mcp_toolkit_observability::safe_text("tool", &tool_name),
                                    mcp_toolkit_observability::safe_text("kind", "contract_error"),
                                    mcp_toolkit_observability::safe_text(
                                        "message",
                                        clip_for_telemetry(message),
                                    ),
                                ],
                            );
                        }
                    } else {
                        mcp_toolkit_observability::emit_event(
                            mcp_toolkit_observability::Level::INFO,
                            "postgres_mcp.tool.finish",
                            &mcp_toolkit_observability::EventContext::new()
                                .with_tool_name(&tool_name),
                            &[mcp_toolkit_observability::safe_text("tool", &tool_name)],
                        );
                    }
                }
            }

            Self::emit_progress(
                &progress_context,
                progress_token,
                100.0,
                if matches!(outcome, ToolCallOutcome::Success) {
                    "completed"
                } else {
                    "failed"
                },
            )
            .await;

            result
        }
    }
}

#[derive(Debug, Clone)]
struct ContractErrorSummary {
    code: String,
    reason: String,
    fingerprint: String,
    detail_level: String,
    retryable: Option<bool>,
    message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExecuteSqlValidationSummary {
    validation_surface: &'static str,
    validation_param: &'static str,
    validation_kind: &'static str,
    hint_family: &'static str,
    rejected_value: &'static str,
}

fn telemetry_debug_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        let raw = std::env::var(TELEMETRY_DEBUG_ENV).unwrap_or_default();
        matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn clip_for_telemetry(message: &str) -> String {
    if message.chars().count() <= TELEMETRY_DEBUG_MAX_CHARS {
        return message.to_string();
    }
    message
        .chars()
        .take(TELEMETRY_DEBUG_MAX_CHARS)
        .collect::<String>()
}

fn telemetry_fingerprint(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    format!("evt_{}", &hex[..12])
}

fn parse_bool_env(var_name: &str) -> Option<bool> {
    let raw = std::env::var(var_name).ok()?;
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_u32_env(var_name: &str) -> Option<u32> {
    let raw = std::env::var(var_name).ok()?;
    raw.trim().parse::<u32>().ok()
}

fn parse_u64_env(var_name: &str) -> Option<u64> {
    let raw = std::env::var(var_name).ok()?;
    raw.trim().parse::<u64>().ok()
}

fn classify_execute_sql_rejected_value(message: &str, safelist: &[&'static str]) -> &'static str {
    let normalized = message.to_ascii_lowercase();
    for value in safelist {
        if normalized.contains(&format!("got \"{value}\""))
            || normalized.contains(&format!("got `{value}`"))
            || normalized.contains(&format!("\"{value}\""))
            || normalized.contains(&format!("`{value}`"))
        {
            return value;
        }
    }
    "other"
}

fn extract_execute_sql_validation_summary(
    tool_name: &str,
    validation_surface: &'static str,
    message: &str,
) -> Option<ExecuteSqlValidationSummary> {
    if !matches!(
        tool_name,
        "execute_sql"
            | "query_sql"
            | "query_tuples"
            | "render_sql"
            | "describe_sql"
            | "admin_sql"
            | "export_sql"
            | "query_job_start"
            | "export_job_start"
    ) {
        return None;
    }

    let normalized = message.to_ascii_lowercase();
    if normalized.contains("missing field")
        || normalized.contains("unknown field")
        || normalized.contains("unknown variant")
        || normalized.contains("invalid type")
    {
        return Some(ExecuteSqlValidationSummary {
            validation_surface,
            validation_param: "request",
            validation_kind: "request_rejected",
            hint_family: "fix_request_shape",
            rejected_value: "request_shape",
        });
    }
    if normalized.contains("metadata_verbosity must be one of") {
        return Some(ExecuteSqlValidationSummary {
            validation_surface,
            validation_param: "metadata_verbosity",
            validation_kind: "enum_rejected",
            hint_family: "compact_metadata",
            rejected_value: classify_execute_sql_rejected_value(
                message,
                &["verbose", "compact", "markdown"],
            ),
        });
    }
    if normalized.contains("response_formatting_mode only supports") {
        let hint_family = if normalized.contains("output_mode=table")
            || normalized.contains("output_mode=rows_safe")
        {
            "readable_table"
        } else if normalized.contains("profile=fast_agent") {
            "fast_agent"
        } else {
            "none"
        };
        return Some(ExecuteSqlValidationSummary {
            validation_surface,
            validation_param: "response_formatting_mode",
            validation_kind: "enum_rejected",
            hint_family,
            rejected_value: classify_execute_sql_rejected_value(
                message,
                &["markdown", "compact", "currency"],
            ),
        });
    }
    if normalized.contains("params currently require exactly one sql statement") {
        return Some(ExecuteSqlValidationSummary {
            validation_surface,
            validation_param: "params",
            validation_kind: "params_rejected",
            hint_family: "remove_top_level_semicolons",
            rejected_value: "multi_statement",
        });
    }
    if normalized.contains("syntax error at or near \";\"") {
        return Some(ExecuteSqlValidationSummary {
            validation_surface,
            validation_param: "sql",
            validation_kind: "multi_statement_rejected",
            hint_family: "remove_top_level_semicolons",
            rejected_value: "semicolon_split",
        });
    }

    None
}

fn extract_contract_error_summary(result: &CallToolResult) -> Option<ContractErrorSummary> {
    let payload = result.structured_content.as_ref()?;
    if payload.get("ok").and_then(Value::as_bool) != Some(false) {
        return None;
    }
    let error = payload.get("error")?.as_object()?;
    Some(ContractErrorSummary {
        code: error
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN_ERROR")
            .to_string(),
        reason: error
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("unknown_error")
            .to_string(),
        fingerprint: error
            .get("fingerprint")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| {
                telemetry_fingerprint(
                    error
                        .get("code")
                        .and_then(Value::as_str)
                        .unwrap_or("UNKNOWN_ERROR"),
                )
            }),
        detail_level: error
            .get("detail_level")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        retryable: error.get("retryable").and_then(Value::as_bool),
        message: error
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ExportArtifactRecord, ExportArtifactRegistry, PostgresMcp, QueryJobHandle,
        QueryJobRegistry, QueryJobRegistryError, QueryJobState, TELEMETRY_DEBUG_MAX_CHARS,
        ToolCallOutcome, clip_for_telemetry, extract_contract_error_summary,
        extract_execute_sql_validation_summary, sanitize_schema_value,
        sanitize_tool_schemas_for_mcp, telemetry_fingerprint, unix_time_ms,
    };
    use crate::config::{
        AccessMode, AdvisorExternalConfig, ResponseAutoTabularMode, ResponseMode,
        ResponseOutputMode, StartupRole,
    };
    use crate::db::DbEngine;
    use rmcp::model::{CallToolResult, ResourceContents, Tool};
    use serde_json::{Value, json};
    use std::sync::Arc;
    use std::time::Duration;

    fn contains_nullable_key(value: &Value) -> bool {
        match value {
            Value::Object(object) => {
                if object.contains_key("nullable") {
                    return true;
                }
                object.values().any(contains_nullable_key)
            }
            Value::Array(values) => values.iter().any(contains_nullable_key),
            _ => false,
        }
    }

    #[test]
    fn sanitize_schema_value_removes_nonstandard_nullable_and_uint_formats() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "max_rows": {
                    "type": "integer",
                    "format": "uint",
                    "minimum": 0,
                    "nullable": true
                },
                "output_mode": {
                    "anyOf": [
                        {"type": "string"},
                        {"const": null, "nullable": true}
                    ],
                    "default": null
                },
                "health_type": {
                    "type": "string",
                    "nullable": true
                }
            }
        });

        sanitize_schema_value(&mut schema);
        assert!(!contains_nullable_key(&schema));
        let max_rows = &schema["properties"]["max_rows"];
        assert!(max_rows.get("format").is_none());
        assert_eq!(max_rows["type"], json!(["integer", "null"]));

        let output_mode_any_of = schema["properties"]["output_mode"]["anyOf"]
            .as_array()
            .expect("output_mode.anyOf should be array");
        assert!(
            output_mode_any_of
                .iter()
                .any(|entry| entry.get("const") == Some(&Value::Null))
        );
    }

    #[test]
    fn sanitize_tool_schemas_for_mcp_applies_schema_normalization() {
        let input_schema = serde_json::Map::from_iter([(
            "properties".to_string(),
            json!({
                "max_cell_chars": {
                    "type": "integer",
                    "format": "uint",
                    "nullable": true
                }
            }),
        )]);

        let tool = Tool::new("execute_sql", "Execute SQL", input_schema);
        let sanitized = sanitize_tool_schemas_for_mcp(vec![tool]);
        assert_eq!(sanitized.len(), 1);

        let first = &sanitized[0];
        let max_cell_chars = first
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| props.get("max_cell_chars"))
            .expect("sanitized schema should keep max_cell_chars");
        assert!(max_cell_chars.get("format").is_none());
        assert_eq!(
            max_cell_chars.get("type"),
            Some(&json!(["integer", "null"]))
        );
    }

    #[test]
    fn extract_contract_error_summary_reads_low_cardinality_fields() {
        let result = CallToolResult::structured(json!({
            "ok": false,
            "error": {
                "error": "relation missing",
                "code": "DB_QUERY_FAILED",
                "reason": "db_query_failed",
                "fingerprint": "err_abcd1234abcd",
                "detail_level": "minimal",
                "retryable": false
            },
            "meta": {"elapsed_ms": 4}
        }));
        let summary = extract_contract_error_summary(&result)
            .expect("contract error summary should extract from v2 error envelope");
        assert_eq!(summary.code, "DB_QUERY_FAILED");
        assert_eq!(summary.reason, "db_query_failed");
        assert_eq!(summary.fingerprint, "err_abcd1234abcd");
        assert_eq!(summary.detail_level, "minimal");
        assert_eq!(summary.retryable, Some(false));
        assert_eq!(summary.message.as_deref(), Some("relation missing"));
    }

    #[test]
    fn extract_contract_error_summary_ignores_success_payloads() {
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [],
            "meta": {"elapsed_ms": 2}
        }));
        assert!(extract_contract_error_summary(&result).is_none());
    }

    #[test]
    fn extract_execute_sql_validation_summary_detects_router_enum_rejects() {
        let summary = extract_execute_sql_validation_summary(
            "execute_sql",
            "router_deserialize",
            "metadata_verbosity must be one of [compact, standard, full] (alias: low -> compact); got \"verbose\". Example: {\"metadata_verbosity\":\"compact\"}",
        )
        .expect("metadata_verbosity reject should be summarized");
        assert_eq!(summary.validation_surface, "router_deserialize");
        assert_eq!(summary.validation_param, "metadata_verbosity");
        assert_eq!(summary.validation_kind, "enum_rejected");
        assert_eq!(summary.hint_family, "compact_metadata");
        assert_eq!(summary.rejected_value, "verbose");

        let response_summary = extract_execute_sql_validation_summary(
            "execute_sql",
            "router_deserialize",
            "response_formatting_mode only supports `currency`. For readable verification loops, use output_mode=table or output_mode=rows_safe. For compact verification loops, use profile=fast_agent. Got \"markdown\". Example: {\"output_mode\":\"table\"}",
        )
        .expect("response_formatting_mode reject should be summarized");
        assert_eq!(
            response_summary.validation_param,
            "response_formatting_mode"
        );
        assert_eq!(response_summary.validation_kind, "enum_rejected");
        assert_eq!(response_summary.hint_family, "readable_table");
        assert_eq!(response_summary.rejected_value, "markdown");
    }

    #[test]
    fn extract_execute_sql_validation_summary_detects_tool_validation_params_rejects() {
        let summary = extract_execute_sql_validation_summary(
            "execute_sql",
            "tool_validation",
            "params currently require exactly one SQL statement; remove top-level semicolons before retrying",
        )
        .expect("params reject should be summarized");
        assert_eq!(summary.validation_surface, "tool_validation");
        assert_eq!(summary.validation_param, "params");
        assert_eq!(summary.validation_kind, "params_rejected");
        assert_eq!(summary.hint_family, "remove_top_level_semicolons");
        assert_eq!(summary.rejected_value, "multi_statement");

        let syntax_summary = extract_execute_sql_validation_summary(
            "execute_sql",
            "tool_validation",
            "query execution failed: syntax error at or near \";\" [sqlstate: 42601] (Error executing paginated query)",
        )
        .expect("semicolon syntax reject should be summarized");
        assert_eq!(syntax_summary.validation_param, "sql");
        assert_eq!(syntax_summary.validation_kind, "multi_statement_rejected");
        assert_eq!(syntax_summary.hint_family, "remove_top_level_semicolons");
        assert_eq!(syntax_summary.rejected_value, "semicolon_split");
    }

    #[test]
    fn classify_outcome_treats_execute_sql_validation_router_errors_as_non_retryable() {
        let result: Result<CallToolResult, crate::McpError> = Err(crate::McpError::invalid_params(
            "response_formatting_mode only supports `currency`. For readable verification loops, use output_mode=table or output_mode=rows_safe. For compact verification loops, use profile=fast_agent. Got \"markdown\". Example: {\"output_mode\":\"table\"}",
            None,
        ));
        assert_eq!(
            PostgresMcp::classify_outcome("execute_sql", &result),
            ToolCallOutcome::NonRetryableFailure
        );
    }

    #[test]
    fn classify_outcome_treats_execute_sql_validation_contract_errors_as_non_retryable() {
        let result: Result<CallToolResult, crate::McpError> = Ok(CallToolResult::structured(
            json!({
                "ok": false,
                "error": {
                    "error": "params currently require exactly one SQL statement; remove top-level semicolons before retrying",
                    "code": "INVALID_REQUEST",
                    "reason": "invalid_request",
                    "detail_level": "minimal",
                    "retryable": false,
                    "fingerprint": "err_deadbeef0001"
                },
                "meta": {"elapsed_ms": 1}
            }),
        ));
        assert_eq!(
            PostgresMcp::classify_outcome("execute_sql", &result),
            ToolCallOutcome::NonRetryableFailure
        );
    }

    #[test]
    fn telemetry_fingerprint_and_clipping_are_deterministic() {
        let fingerprint_a = telemetry_fingerprint("same-input");
        let fingerprint_b = telemetry_fingerprint("same-input");
        assert_eq!(fingerprint_a, fingerprint_b);
        assert!(fingerprint_a.starts_with("evt_"));
        assert_eq!(fingerprint_a.len(), 16);

        let long = "x".repeat(TELEMETRY_DEBUG_MAX_CHARS + 32);
        let clipped = clip_for_telemetry(&long);
        assert_eq!(clipped.chars().count(), TELEMETRY_DEBUG_MAX_CHARS);
    }

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

    #[test]
    fn circuit_breaker_opens_after_retryable_failures_threshold() {
        let mut server = test_server();
        server.circuit_breaker_config.enabled = true;
        server.circuit_breaker_config.failure_threshold = 2;
        server.circuit_breaker_config.cooldown = Duration::from_secs(60);

        server.record_tool_outcome("execute_sql", ToolCallOutcome::RetryableFailure);
        assert!(server.circuit_open_retry_after_ms("execute_sql").is_none());

        server.record_tool_outcome("execute_sql", ToolCallOutcome::RetryableFailure);
        assert!(
            server
                .circuit_open_retry_after_ms("execute_sql")
                .unwrap_or_default()
                > 0
        );
    }

    #[test]
    fn circuit_breaker_resets_on_success() {
        let mut server = test_server();
        server.circuit_breaker_config.enabled = true;
        server.circuit_breaker_config.failure_threshold = 3;

        server.record_tool_outcome("list_objects", ToolCallOutcome::RetryableFailure);
        server.record_tool_outcome("list_objects", ToolCallOutcome::Success);
        assert!(server.circuit_open_retry_after_ms("list_objects").is_none());
        let states = server
            .circuit_state
            .lock()
            .expect("state lock should succeed");
        let state = states
            .get(server.circuit_group_key("list_objects"))
            .expect("list_objects state should exist");
        assert_eq!(state.consecutive_retryable_failures, 0);
    }

    #[test]
    fn circuit_breaker_resets_on_non_retryable_failure() {
        let mut server = test_server();
        server.circuit_breaker_config.enabled = true;
        server.circuit_breaker_config.failure_threshold = 2;

        server.record_tool_outcome("execute_sql", ToolCallOutcome::RetryableFailure);
        server.record_tool_outcome("execute_sql", ToolCallOutcome::NonRetryableFailure);
        assert!(server.circuit_open_retry_after_ms("execute_sql").is_none());
        let states = server
            .circuit_state
            .lock()
            .expect("state lock should succeed");
        let state = states
            .get(server.circuit_group_key("execute_sql"))
            .expect("execute_sql state should exist");
        assert_eq!(state.consecutive_retryable_failures, 0);
        drop(states);

        server.record_tool_outcome("execute_sql", ToolCallOutcome::RetryableFailure);
        let states = server
            .circuit_state
            .lock()
            .expect("state lock should succeed");
        let state = states
            .get(server.circuit_group_key("execute_sql"))
            .expect("execute_sql state should exist");
        assert_eq!(state.consecutive_retryable_failures, 1);
    }

    #[test]
    fn retry_storm_smoke_opens_circuit() {
        let mut server = test_server();
        server.circuit_breaker_config.enabled = true;
        server.circuit_breaker_config.failure_threshold = 3;
        server.circuit_breaker_config.cooldown = Duration::from_secs(60);

        for _ in 0..3 {
            server.record_tool_outcome("execute_sql", ToolCallOutcome::RetryableFailure);
        }
        let retry_after_ms = server
            .circuit_open_retry_after_ms("execute_sql")
            .unwrap_or_default();
        assert!(retry_after_ms > 0);

        let blocked = server.circuit_open_result("execute_sql", retry_after_ms);
        let payload = blocked
            .structured_content
            .expect("circuit-open result should have structured payload");
        let code = payload["error"]["code"].as_str().unwrap_or_default();
        assert_eq!(code, "TOOL_CIRCUIT_OPEN");
    }

    #[test]
    fn success_payload_smoke_does_not_attach_retry_metadata() {
        let mut server = test_server();
        server.circuit_breaker_config.enabled = true;
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [],
            "meta": {"elapsed_ms": 1}
        }));
        let payload = result
            .structured_content
            .expect("result should remain structured after construction");
        assert!(payload["meta"].get("retry_after_ms").is_none());
    }

    #[test]
    fn query_job_registry_rejects_creation_when_capacity_is_full_with_running_jobs() {
        let registry = QueryJobRegistry::with_limits(1, Duration::from_secs(3600));
        let first = registry
            .create("deadbeefdeadbeef")
            .expect("first job should be created");
        let _ = first.mark_running();

        let err = registry
            .create("feedfacefeedface")
            .expect_err("capacity should reject additional running jobs");
        assert_eq!(err, QueryJobRegistryError::CapacityReached);
    }

    #[test]
    fn query_job_registry_evicts_terminal_jobs_when_capacity_is_reached() {
        let registry = QueryJobRegistry::with_limits(1, Duration::from_secs(3600));
        let first = registry
            .create("deadbeefdeadbeef")
            .expect("first job should be created");
        let first_id = first.snapshot().job_id;
        let _ = first.complete(
            QueryJobState::Succeeded,
            json!({
                "ok": true,
                "data": {
                    "rows": 1
                }
            }),
        );

        let second = registry
            .create("feedfacefeedface")
            .expect("terminal job should be evicted to make room");
        let second_id = second.snapshot().job_id;
        assert_ne!(first_id, second_id);
        assert!(
            registry.get(&first_id).is_none(),
            "evicted terminal job should be absent from registry"
        );
    }

    #[tokio::test]
    async fn query_job_register_abort_handle_rechecks_after_cancellation() {
        let handle = QueryJobHandle::new(
            "qj_0000000000000001".to_string(),
            "query".to_string(),
            "deadbeefdeadbeef".to_string(),
            false,
        );

        let task: tokio::task::JoinHandle<()> = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let abort_handle = task.abort_handle();

        let _ = handle.cancel(StartupRole::Runtime);
        handle.register_abort_handle(abort_handle);

        let result = tokio::time::timeout(Duration::from_millis(200), async { task.await }).await;
        assert!(
            result.is_ok(),
            "canceled job should abort previously running task handle"
        );
        let join_result = result.expect("status check should finish before timeout");
        if let Err(err) = join_result {
            assert!(
                err.is_cancelled(),
                "aborted task should report cancellation"
            );
        } else {
            panic!("task should have been canceled after abort_handle registration");
        }
    }

    #[tokio::test]
    async fn query_job_cancel_stores_v2_error_payload() {
        let handle = QueryJobHandle::new(
            "qj_0000000000000001".to_string(),
            "query".to_string(),
            "deadbeefdeadbeef".to_string(),
            false,
        );
        let snapshot = handle.cancel(StartupRole::Runtime);
        let response = snapshot
            .response
            .expect("cancel should attach response payload");
        assert_eq!(response.get("ok").and_then(Value::as_bool), Some(false));
        let error = response
            .get("error")
            .and_then(Value::as_object)
            .expect("canceled payload should include error object");
        assert_eq!(
            error.get("code").and_then(Value::as_str),
            Some("QUERY_CANCELED")
        );
        assert_eq!(
            error.get("detail_level").and_then(Value::as_str),
            Some("minimal")
        );
        assert_eq!(error.get("retryable").and_then(Value::as_bool), Some(false));
        assert_eq!(
            error.get("error_class").and_then(Value::as_str),
            Some("client_cancelled")
        );
        assert!(
            error
                .get("fingerprint")
                .and_then(Value::as_str)
                .is_some_and(|value| value.starts_with("err_")),
            "canceled payload should include normalized error fingerprint"
        );
        assert_eq!(
            response
                .get("meta")
                .and_then(Value::as_object)
                .and_then(|meta| meta.get("elapsed_ms"))
                .and_then(Value::as_u64),
            Some(0)
        );
    }

    #[tokio::test]
    async fn query_job_wait_for_update_since_detects_state_change_without_delay() {
        let handle = QueryJobHandle::new(
            "qj_0000000000000001".to_string(),
            "query".to_string(),
            "deadbeefdeadbeef".to_string(),
            false,
        );
        let revision = handle.revision();
        let _ = handle.mark_running();

        let updated = handle
            .wait_for_update_since(revision, Some(Duration::from_millis(10)))
            .await;
        assert!(
            updated,
            "revision-aware wait should detect state transition"
        );
    }

    #[test]
    fn query_job_registry_create_with_kind_uses_kind_specific_ids_and_snapshot_kind() {
        let registry = QueryJobRegistry::with_limits(2, Duration::from_secs(60));
        let export_job = registry
            .create_with_kind("export", "deadbeefdeadbeef")
            .expect("export job should be created");
        let snapshot = export_job.snapshot();
        assert!(snapshot.job_id.starts_with("xj_"));
        assert_eq!(snapshot.kind, "export");
    }

    #[test]
    fn sql_surface_circuit_breaker_state_is_shared_between_query_tuple_and_render_tools() {
        let mut server = test_server();
        server.circuit_breaker_config.enabled = true;
        server.circuit_breaker_config.failure_threshold = 2;
        server.circuit_breaker_config.cooldown = Duration::from_secs(60);

        server.record_tool_outcome("query_sql", ToolCallOutcome::RetryableFailure);
        server.record_tool_outcome("query_tuples", ToolCallOutcome::RetryableFailure);

        assert!(
            server
                .circuit_open_retry_after_ms("query_sql")
                .unwrap_or_default()
                > 0
        );
        assert!(
            server
                .circuit_open_retry_after_ms("query_tuples")
                .unwrap_or_default()
                > 0
        );
        assert!(
            server
                .circuit_open_retry_after_ms("render_sql")
                .unwrap_or_default()
                > 0
        );
    }

    #[test]
    fn extract_execute_sql_validation_summary_detects_query_sql_request_shape_errors() {
        let summary = extract_execute_sql_validation_summary(
            "query_sql",
            "router_deserialize",
            "missing field `sql`",
        )
        .expect("query_sql request shape errors should be classified");
        assert_eq!(summary.validation_param, "request");
        assert_eq!(summary.validation_kind, "request_rejected");
        assert_eq!(summary.hint_family, "fix_request_shape");
    }

    #[test]
    fn extract_execute_sql_validation_summary_detects_query_tuples_request_shape_errors() {
        let summary = extract_execute_sql_validation_summary(
            "query_tuples",
            "router_deserialize",
            "missing field `sql`",
        )
        .expect("query_tuples request shape errors should be classified");
        assert_eq!(summary.validation_param, "request");
        assert_eq!(summary.validation_kind, "request_rejected");
        assert_eq!(summary.hint_family, "fix_request_shape");
    }

    #[test]
    fn admin_sql_is_hidden_from_discovery_by_default() {
        let server = test_server();
        assert!(!server.tool_names().iter().any(|name| name == "admin_sql"));
    }

    #[test]
    fn execute_sql_is_hidden_from_discovery_by_default() {
        let server = test_server();
        assert!(!server.tool_names().iter().any(|name| name == "execute_sql"));
    }

    #[test]
    fn admin_sql_is_listed_when_enabled() {
        let mut server = test_server();
        server.enable_admin_sql = true;
        assert!(server.tool_names().iter().any(|name| name == "admin_sql"));
    }

    #[test]
    fn execute_sql_is_listed_when_exposed() {
        let mut server = test_server();
        server.expose_execute_sql = true;
        assert!(server.tool_names().iter().any(|name| name == "execute_sql"));
    }

    #[test]
    fn export_artifact_registry_round_trips_through_resource_read() {
        let registry = ExportArtifactRegistry::with_limit(2);
        let path = std::env::temp_dir().join(format!("postgres-mcp-test-{}.csv", unix_time_ms()));
        std::fs::write(&path, "id,name\n1,alice\n").expect("artifact file should be written");
        registry.register(ExportArtifactRecord {
            handle: "art_test".to_string(),
            uri: "postgres://artifacts/art_test".to_string(),
            format: "csv".to_string(),
            mime_type: "text/csv".to_string(),
            bytes: 16,
            row_count: 1,
            path: path.clone(),
        });

        let mut server = test_server();
        server.export_artifacts = Arc::new(registry);

        let resources = server.export_resources();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].uri, "postgres://artifacts/art_test");

        let result = server
            .read_export_resource_uri("postgres://artifacts/art_test")
            .expect("resource read should succeed");
        assert_eq!(result.contents.len(), 1);
        match &result.contents[0] {
            ResourceContents::TextResourceContents { uri, text, .. } => {
                assert_eq!(uri, "postgres://artifacts/art_test");
                assert!(text.contains("alice"));
            }
            other => panic!("expected text resource contents, got {other:?}"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    #[ignore = "extended concurrency stress"]
    fn extended_concurrency_retry_storm() {
        let mut server = test_server();
        server.circuit_breaker_config.enabled = true;
        server.circuit_breaker_config.failure_threshold = 10;
        server.circuit_breaker_config.cooldown = Duration::from_secs(60);

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..20 {
                        server
                            .record_tool_outcome("execute_sql", ToolCallOutcome::RetryableFailure);
                    }
                });
            }
        });

        assert!(
            server
                .circuit_open_retry_after_ms("execute_sql")
                .unwrap_or_default()
                > 0
        );
    }
}
