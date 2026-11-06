//! Authenticated Streamable HTTP adapter over the fixed read-only database.
//!
//! RMCP owns protocol dispatch. Toolkit owns OAuth verification and transport
//! guards. This module binds a consumed request witness to query authority,
//! bounds admission through response-body lifetime, and isolates legacy sessions.
#![deny(clippy::all)]

mod auth;
pub mod config;

use crate::{
    config::{AccessMode, MetadataPolicyMode, Settings, StartupRole},
    db::DbEngine,
    remote::{RemotePostgresMcp, RemotePrincipal},
};
use ::http::{
    Method, StatusCode,
    header::{AUTHORIZATION, CACHE_CONTROL, HeaderValue, ORIGIN},
};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
};
use config::{HttpConfig, READ_SCOPE};
use mcp_toolkit_auth::{
    AuthContext, Authenticator, VerifiedAuthContext,
    consume_verified_auth_surface_request_from_request,
};
use mcp_toolkit_http::{host::validate_request_authority, session::BoundedSessionManager};
use mcp_toolkit_server::http::{
    HttpBindSafety, LocalMcpHttpRouterBuilder, LocalMcpHttpRuntimeBuilder,
};
use rmcp::transport::streamable_http_server::session::SessionManager;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone, PartialEq, Eq)]
struct Identity {
    issuer: String,
    subject: String,
    client: String,
}
struct LegacyCustody {
    owners: Mutex<HashMap<String, Identity>>,
    manager: Arc<BoundedSessionManager>,
    maximum: usize,
}
impl LegacyCustody {
    async fn sweep(&self) {
        let ids: Vec<_> = self.owners.lock().await.keys().cloned().collect();
        for id in ids {
            if !self
                .manager
                .has_session(&id.as_str().into())
                .await
                .unwrap_or(false)
            {
                self.owners.lock().await.remove(&id);
            }
        }
    }
    async fn check(&self, id: &str, principal: &Identity) -> bool {
        if self.owners.lock().await.get(id) != Some(principal) {
            return false;
        }
        self.manager.has_session(&id.into()).await.unwrap_or(false)
    }
    async fn bind(&self, id: &str, principal: Identity) -> bool {
        if !self.manager.has_session(&id.into()).await.unwrap_or(false) {
            return false;
        }
        self.sweep().await;
        let mut owners = self.owners.lock().await;
        if let Some(owner) = owners.get(id) {
            return owner == &principal;
        }
        if owners.len() >= self.maximum {
            return false;
        }
        owners.insert(id.into(), principal);
        true
    }
}
#[derive(Clone)]
struct GuardState {
    config: HttpConfig,
    authenticator: Arc<Authenticator>,
    issuer: String,
    custody: Arc<LegacyCustody>,
}
#[derive(Clone)]
struct Admission {
    config: HttpConfig,
    permits: Arc<Semaphore>,
}

/// Starts an authenticated loopback backend for a trusted HTTPS reverse proxy.
/// No credentials or grants are created and no database is selected by requests.
pub async fn serve(settings: Settings) -> Result<(), String> {
    let path = settings
        .http_config
        .as_deref()
        .ok_or("--transport http requires --http-config")?;
    let config = HttpConfig::load(path)?;
    if settings.access_mode != AccessMode::Restricted
        || settings.startup_role != StartupRole::Runtime
        || settings.enable_admin_sql
        || settings.expose_execute_sql
        || settings.advisor_external.enabled
        || settings.allow_insecure_tls
        || settings.probe_sql.is_some()
        || settings.print_tools
        || settings.print_tool_schema
    {
        return Err("HTTP requires restricted runtime mode without admin/compatibility tools, advisors, insecure DB TLS, or stdio probes".into());
    }
    if settings
        .database_url
        .as_ref()
        .is_none_or(|v| v.trim().is_empty())
    {
        return Err("HTTP requires one operator-configured DATABASE_URI".into());
    }
    if settings.metadata_policy_mode != MetadataPolicyMode::Full
        || !settings.metadata_schema_allow.is_empty()
        || !settings.metadata_schema_deny.is_empty()
    {
        return Err("HTTP schema access is enforced by the fixed database role; stdio metadata-policy flags are not supported".into());
    }
    let db_config = mcp_toolkit_postgres::PgConnectionConfig::from_dsn(
        settings
            .database_url
            .as_deref()
            .ok_or("DATABASE_URI required")?,
    )
    .map_err(|_| "Invalid HTTP database connection configuration")?;
    db_config
        .validate_tls_policy(mcp_toolkit_postgres::PgInsecureTlsPolicy::DisallowAll)
        .map_err(|_| "HTTP database connections require verified TLS; use sslmode=verify-full")?;
    HttpBindSafety::new(false, true)
        .validate(config.bind)
        .map_err(|_| "HTTP backend must bind loopback")?;
    // Remote budgets never inherit permissive stdio timeout overrides.
    let db = DbEngine::new(
        settings.database_url,
        AccessMode::Restricted,
        false,
        Some(Duration::from_secs(8)),
        Some(Duration::from_secs(5)),
        Some(Duration::from_secs(1)),
    );
    let authentication = auth::build(&config).await?;
    let server = RemotePostgresMcp::new(
        db,
        READ_SCOPE.into(),
        config.expected_database_role.clone(),
        format!(
            "{}/.well-known/oauth-protected-resource/mcp",
            config.public_base_url()?
        ),
    );
    let stop = CancellationToken::new();
    let (router, custody) = build_router(config.clone(), server, authentication, stop.clone());
    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .map_err(|_| "Cannot bind HTTP backend")?;
    let signal = stop.clone();
    let shutdown = tokio::spawn(async move {
        #[cfg(unix)]
        {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut terminate) => {
                    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
                }
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        signal.cancel();
    });
    let sweep_stop = stop.clone();
    let sweep = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! { _ = sweep_stop.cancelled() => break, _ = interval.tick() => custody.sweep().await }
        }
    });
    tracing::info!(
        "Authenticated Postgres HTTP backend started; public TLS must terminate at the trusted local proxy"
    );
    let outcome = axum::serve(listener, router)
        .with_graceful_shutdown(stop.clone().cancelled_owned())
        .await
        .map_err(|_| "HTTP server failed".to_owned());
    stop.cancel();
    shutdown.abort();
    let _ = sweep.await;
    outcome
}

fn build_router(
    config: HttpConfig,
    server: RemotePostgresMcp,
    authentication: auth::Authentication,
    stop: CancellationToken,
) -> (Router, Arc<LegacyCustody>) {
    let runtime = LocalMcpHttpRuntimeBuilder::new()
        .allowed_hosts(config.allowed_hosts.clone())
        .allowed_origins(config.allowed_origins.clone())
        .max_sessions(config.limits.max_legacy_sessions)
        .max_request_body_bytes(config.limits.max_request_bytes)
        .allow_resume(false)
        .stateless_fallback(false)
        .cancellation_token(stop.clone())
        .build(move || Ok::<_, std::io::Error>(server.clone()));
    let custody = Arc::new(LegacyCustody {
        owners: Mutex::new(HashMap::new()),
        manager: runtime.session_manager.clone(),
        maximum: config.limits.max_legacy_sessions,
    });
    let guards = GuardState {
        config: config.clone(),
        authenticator: authentication.authenticator,
        issuer: authentication.issuer,
        custody: custody.clone(),
    };
    let readiness = stop.clone();
    let router = LocalMcpHttpRouterBuilder::new(runtime.into_state(true))
        .include_health(false)
        .build()
        .route(
            "/healthz",
            axum::routing::get(|| async { StatusCode::NO_CONTENT }),
        )
        .route(
            "/readyz",
            axum::routing::get(move || {
                let ready = !readiness.is_cancelled();
                async move {
                    if ready {
                        StatusCode::NO_CONTENT
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        )
        .layer(middleware::from_fn_with_state(guards, bind_principal))
        .layer(authentication.layer)
        .layer(middleware::from_fn_with_state(
            Admission {
                permits: Arc::new(Semaphore::new(config.limits.max_requests)),
                config,
            },
            admit,
        ));
    (router, custody)
}

fn rejection(status: StatusCode, message: &'static str) -> Response {
    (status, axum::Json(json!({"error":message}))).into_response()
}
use axum::response::IntoResponse;

fn session_id(headers: &::http::HeaderMap) -> Result<Option<String>, ()> {
    let mut values = headers.get_all("mcp-session-id").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| ())?;
    if values.next().is_some()
        || value.is_empty()
        || value.len() > 256
        || !value.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(());
    }
    Ok(Some(value.into()))
}
fn current_request(headers: &::http::HeaderMap, payload: Option<&Value>) -> bool {
    if payload.is_some_and(|p| p.get("method").and_then(Value::as_str) == Some("initialize")) {
        return false;
    }
    headers
        .get("mcp-protocol-version")
        .and_then(|v| v.to_str().ok())
        == Some("2026-07-28")
        || payload
            .and_then(|p| p.pointer("/params/_meta/io.modelcontextprotocol~1protocolVersion"))
            .and_then(Value::as_str)
            == Some("2026-07-28")
}

async fn bind_principal(State(state): State<GuardState>, request: Request, next: Next) -> Response {
    if matches!(request.uri().path(), "/healthz" | "/readyz") {
        return next.run(request).await;
    }
    let Some(witness) =
        consume_verified_auth_surface_request_from_request(&request, &state.authenticator)
    else {
        return rejection(
            StatusCode::UNAUTHORIZED,
            "Verified request authentication required",
        );
    };
    let Some(auth) = witness.auth() else {
        return rejection(StatusCode::UNAUTHORIZED, "Authentication required");
    };
    let Some(subject) = auth
        .subject
        .as_ref()
        .filter(|s| state.config.allowed_subjects.contains(*s))
    else {
        return rejection(StatusCode::FORBIDDEN, "Principal is not authorized");
    };
    let Some(client) = auth
        .azp
        .as_ref()
        .filter(|s| state.config.allowed_client_ids.contains(*s))
    else {
        return rejection(StatusCode::FORBIDDEN, "Client is not authorized");
    };
    if !auth.scopes.iter().any(|scope| scope == READ_SCOPE) {
        return rejection(StatusCode::FORBIDDEN, "Read scope required");
    }
    let identity = Identity {
        issuer: state.issuer.clone(),
        subject: subject.clone(),
        client: client.clone(),
    };
    let principal = RemotePrincipal::authorized(
        identity.issuer.clone(),
        subject.clone(),
        client.clone(),
        vec![READ_SCOPE.into()],
    );
    drop(witness);
    let (mut parts, body) = request.into_parts();
    let (body, current) = if parts.method == Method::POST {
        let bytes = match tokio::time::timeout(
            Duration::from_secs(5),
            to_bytes(body, state.config.limits.max_request_bytes),
        )
        .await
        {
            Ok(Ok(bytes)) => bytes,
            _ => {
                return rejection(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "Request body unavailable or above configured limit",
                );
            }
        };
        let payload = serde_json::from_slice::<Value>(&bytes).ok();
        let current = current_request(&parts.headers, payload.as_ref());
        (Body::from(bytes), current)
    } else {
        (body, current_request(&parts.headers, None))
    };
    if !current {
        match session_id(&parts.headers) {
            Ok(Some(id)) if state.custody.check(&id, &identity).await => {}
            Ok(None) => {}
            _ => return rejection(StatusCode::NOT_FOUND, "Legacy session unavailable"),
        }
    }
    parts.headers.remove(AUTHORIZATION);
    parts.headers.remove("proxy-authorization");
    parts.extensions.remove::<AuthContext>();
    parts.extensions.remove::<VerifiedAuthContext>();
    parts.extensions.insert(principal);
    let response = next.run(Request::from_parts(parts, body)).await;
    if !current {
        match session_id(response.headers()) {
            Ok(Some(id)) if !state.custody.bind(&id, identity).await => {
                return rejection(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Cannot bind legacy session",
                );
            }
            Err(()) => {
                return rejection(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Invalid legacy session response",
                );
            }
            _ => {}
        }
    }
    response
}

// Use exact normalized origins, not Toolkit's optional any-port matching. An
// empty list means browser Origins are denied, not that every origin is trusted.
fn origin_allowed(headers: &::http::HeaderMap, allowed: &[String]) -> bool {
    let mut values = headers.get_all(ORIGIN).iter();
    let Some(value) = values.next() else {
        return true;
    };
    values.next().is_none() && value.to_str().is_ok_and(|s| allowed.iter().any(|v| v == s))
}
async fn admit(State(state): State<Admission>, request: Request, next: Next) -> Response {
    if validate_request_authority(
        Some(request.uri()),
        request.headers(),
        &state.config.allowed_hosts,
    )
    .is_err()
        || !origin_allowed(request.headers(), &state.config.allowed_origins)
    {
        return rejection(StatusCode::FORBIDDEN, "Host or Origin is not allowed");
    }
    let permit = match state.permits.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return rejection(StatusCode::TOO_MANY_REQUESTS, "HTTP capacity reached"),
    };
    let response = match tokio::time::timeout(Duration::from_secs(15), next.run(request)).await {
        Ok(response) => response,
        Err(_) => return rejection(StatusCode::REQUEST_TIMEOUT, "Request deadline exceeded"),
    };
    let (mut parts, body) = response.into_parts();
    parts
        .headers
        .insert(CACHE_CONTROL, HeaderValue::from_static("private, no-store"));
    parts.headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    Response::from_parts(
        parts,
        Body::new(AdmissionBody {
            inner: body,
            permit: Some(permit),
            deadline: Box::pin(tokio::time::sleep(Duration::from_secs(15))),
        }),
    )
}
struct AdmissionBody {
    inner: Body,
    permit: Option<OwnedSemaphorePermit>,
    deadline: Pin<Box<tokio::time::Sleep>>,
}
impl http_body::Body for AdmissionBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use std::future::Future;
        let this = self.get_mut();
        if this.deadline.as_mut().poll(cx).is_ready() {
            this.permit.take();
            return Poll::Ready(None);
        }
        let frame = Pin::new(&mut this.inner).poll_frame(cx);
        if matches!(&frame, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            this.permit.take();
        }
        frame
    }
    fn is_end_stream(&self) -> bool {
        http_body::Body::is_end_stream(&self.inner)
    }
    fn size_hint(&self) -> http_body::SizeHint {
        http_body::Body::size_hint(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn origins_are_exact_and_empty_denies_browser_origin() {
        let mut headers = ::http::HeaderMap::new();
        assert!(origin_allowed(&headers, &[]));
        headers.insert(ORIGIN, HeaderValue::from_static("https://chatgpt.com"));
        assert!(!origin_allowed(&headers, &[]));
        assert!(origin_allowed(&headers, &["https://chatgpt.com".into()]));
        assert!(!origin_allowed(
            &headers,
            &["https://chatgpt.com:8443".into()]
        ));
        headers.append(ORIGIN, HeaderValue::from_static("https://chatgpt.com"));
        assert!(!origin_allowed(&headers, &["https://chatgpt.com".into()]));
    }
    #[test]
    fn legacy_custody_cannot_be_bypassed_by_initialize_metadata() {
        let mut headers = ::http::HeaderMap::new();
        headers.insert(
            "mcp-protocol-version",
            HeaderValue::from_static("2026-07-28"),
        );
        assert!(!current_request(
            &headers,
            Some(&json!({"method":"initialize"}))
        ));
        assert!(current_request(
            &headers,
            Some(&json!({"method":"tools/list"}))
        ));
    }
    #[test]
    fn session_identifiers_are_single_and_bounded() {
        let mut headers = ::http::HeaderMap::new();
        headers.append("mcp-session-id", HeaderValue::from_static("one"));
        headers.append("mcp-session-id", HeaderValue::from_static("two"));
        assert!(session_id(&headers).is_err());
    }
    #[tokio::test]
    async fn response_body_holds_admission_until_drop() {
        let permits = Arc::new(Semaphore::new(1));
        let body = AdmissionBody {
            inner: Body::from("data: result\n\n"),
            permit: Some(permits.clone().try_acquire_owned().unwrap()),
            deadline: Box::pin(tokio::time::sleep(Duration::from_secs(15))),
        };
        assert_eq!(permits.available_permits(), 0);
        drop(body);
        assert_eq!(permits.available_permits(), 1);
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod integration_tests;
