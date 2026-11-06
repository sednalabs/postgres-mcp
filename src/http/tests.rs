//! Synthetic OAuth + protocol contracts. Keys are generated per fixture and
//! never leave the test process. No live identity provider or database is used.
use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use std::sync::atomic::{AtomicU64, Ordering};
use tower::ServiceExt;

static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    router: Router,
    issuer: String,
    key: EncodingKey,
    stop: CancellationToken,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
impl Fixture {
    async fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "postgres-mcp-auth-test-{}-{}",
            std::process::id(),
            FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        let key_path = dir.join("key.pem");
        assert!(
            std::process::Command::new("openssl")
                .args([
                    "genpkey",
                    "-algorithm",
                    "EC",
                    "-pkeyopt",
                    "ec_paramgen_curve:P-256",
                    "-out"
                ])
                .arg(&key_path)
                .status()
                .unwrap()
                .success()
        );
        let public = std::process::Command::new("openssl")
            .arg("pkey")
            .arg("-in")
            .arg(&key_path)
            .args(["-pubout", "-outform", "DER"])
            .output()
            .unwrap();
        assert!(public.status.success());
        let bytes = &public.stdout[public.stdout.len() - 65..];
        assert_eq!(bytes[0], 4);
        let jwks = json!({"keys":[{"kty":"EC","crv":"P-256","x":URL_SAFE_NO_PAD.encode(&bytes[1..33]),"y":URL_SAFE_NO_PAD.encode(&bytes[33..65]),"kid":"fixture","alg":"ES256","use":"sig"}]});
        let key = EncodingKey::from_ec_pem(&std::fs::read(&key_path).unwrap()).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let metadata = json!({"issuer":issuer,"jwks_uri":format!("{issuer}/jwks"),
            "authorization_endpoint":format!("{issuer}/authorize"),"token_endpoint":format!("{issuer}/token"),
            "response_types_supported":["code"],"grant_types_supported":["authorization_code"],"code_challenge_methods_supported":["S256"]});
        let app = Router::new()
            .route(
                "/.well-known/openid-configuration",
                axum::routing::get(move || {
                    let value = metadata.clone();
                    async move { axum::Json(value) }
                }),
            )
            .route(
                "/jwks",
                axum::routing::get(move || {
                    let value = jwks.clone();
                    async move { axum::Json(value) }
                }),
            );
        let stop = CancellationToken::new();
        let end = stop.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(end.cancelled_owned())
                .await;
        });
        let mut config: HttpConfig = serde_json::from_value(json!({"bind":"127.0.0.1:8080","resource_url":"http://127.0.0.1:8080/mcp",
            "issuer":issuer,"allowed_hosts":["127.0.0.1:8080"],"allowed_origins":["https://chatgpt.com"],
            "allowed_client_ids":["chatgpt-fixture"],"allowed_subjects":["alice","bob"],"expected_database_role":"mcp_reader","allow_loopback_http":true})).unwrap();
        if let Ok(role) = std::env::var("POSTGRES_MCP_TEST_DATABASE_ROLE") {
            config.expected_database_role = role;
        }
        config.validate().unwrap();
        let auth = auth::build(&config).await.unwrap();
        let db = DbEngine::new(
            std::env::var("POSTGRES_MCP_TEST_DATABASE_URI").ok(),
            AccessMode::Restricted,
            false,
            None,
            None,
            None,
        );
        let server = RemotePostgresMcp::new(
            db,
            READ_SCOPE.into(),
            config.expected_database_role.clone(),
            "http://127.0.0.1:8080/.well-known/oauth-protected-resource/mcp".into(),
        );
        let (router, _) = build_router(config, server, auth, stop.clone());
        Self {
            router,
            issuer,
            key,
            stop,
        }
    }
    fn claims(&self, subject: &str) -> Value {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        json!({"iss":self.issuer,"aud":"http://127.0.0.1:8080/mcp","sub":subject,"azp":"chatgpt-fixture","scope":READ_SCOPE,"iat":now,"exp":now+300})
    }
    fn token(&self, claims: &Value) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some("fixture".into());
        encode(&header, claims, &self.key).unwrap()
    }
    fn request(&self, method: &str, mut params: Value, token: Option<&str>) -> Request {
        params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"postgres-contract","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}});
        let mut req = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("host", "127.0.0.1:8080")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", method);
        if let Some(name) = params.get("name").and_then(Value::as_str) {
            req = req.header("mcp-name", name);
        }
        if let Some(token) = token {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        req.body(Body::from(
            json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string(),
        ))
        .unwrap()
    }
    async fn send(&self, req: Request) -> Response {
        self.router.clone().oneshot(req).await.unwrap()
    }
}
async fn payload(response: Response) -> Value {
    let bytes = to_bytes(response.into_body(), 256 * 1024).await.unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    if text.starts_with('{') {
        serde_json::from_str(text).unwrap()
    } else {
        text.lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .filter_map(|data| serde_json::from_str(data.trim()).ok())
            .next()
            .expect("SSE response contains a JSON-RPC message")
    }
}

#[tokio::test]
async fn oauth_metadata_and_missing_or_malformed_bearer_fail_closed() {
    let f = Fixture::new().await;
    let response = f
        .send(
            Request::builder()
                .uri("/.well-known/oauth-protected-resource/mcp")
                .header("host", "127.0.0.1:8080")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let metadata = payload(response).await;
    assert_eq!(metadata["resource"], "http://127.0.0.1:8080/mcp");
    assert_eq!(metadata["authorization_servers"][0], f.issuer);
    for token in [None, Some("not-a-jwt")] {
        let response = f.send(f.request("tools/list", json!({}), token)).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(
            response
                .headers()
                .get("www-authenticate")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("resource_metadata")
        );
    }
}

#[tokio::test]
async fn jwt_identity_audience_expiry_scope_and_client_are_enforced() {
    let f = Fixture::new().await;
    for (key, value) in [
        ("iss", json!("https://wrong.invalid")),
        ("aud", json!("wrong")),
        ("exp", json!(1)),
        ("scope", json!("other")),
        ("azp", json!("other")),
        ("sub", json!("mallory")),
    ] {
        let mut claims = f.claims("alice");
        claims[key] = value;
        let token = f.token(&claims);
        let response = f
            .send(f.request("tools/list", json!({}), Some(&token)))
            .await;
        assert!(
            matches!(
                response.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            ),
            "claim {key}"
        );
    }
    let valid = f.token(&f.claims("alice"));
    let mut bytes = valid.into_bytes();
    let last = bytes.len() - 8;
    bytes[last] = if bytes[last] == b'A' { b'B' } else { b'A' };
    let invalid = String::from_utf8(bytes).unwrap();
    assert_eq!(
        f.send(f.request("tools/list", json!({}), Some(&invalid)))
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn authenticated_current_catalog_and_call_allowlist_match() {
    let f = Fixture::new().await;
    let token = f.token(&f.claims("alice"));
    let response = f
        .send(f.request("tools/list", json!({}), Some(&token)))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("mcp-session-id").is_none());
    let data = payload(response).await;
    let tools = data["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 2);
    for tool in tools {
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
        assert_eq!(tool["_meta"]["securitySchemes"][0]["type"], "oauth2");
    }
    for name in [
        "admin_sql",
        "export_sql",
        "query_job_start",
        "query_session_open",
        "execute_sql",
    ] {
        let response = f
            .send(f.request(
                "tools/call",
                json!({"name":name,"arguments":{}}),
                Some(&token),
            ))
            .await;
        let data = payload(response).await;
        assert!(
            data.get("error").is_some() || data["result"]["isError"] == true,
            "{name} must be denied"
        );
    }
    let response = f
        .send(f.request(
            "tools/call",
            json!({"name":"query","arguments":{"sql":"DELETE FROM secret","params":[]}}),
            Some(&token),
        ))
        .await;
    assert_eq!(payload(response).await["result"]["isError"], true);
}

#[tokio::test]
async fn host_origin_body_and_protocol_mismatch_are_rejected() {
    let f = Fixture::new().await;
    let token = f.token(&f.claims("alice"));
    for (header, value) in [
        ("host", "attacker.invalid"),
        ("origin", "https://attacker.invalid"),
        ("origin", "null"),
    ] {
        let mut req = f.request("tools/list", json!({}), Some(&token));
        req.headers_mut().insert(
            ::http::header::HeaderName::from_static(header),
            HeaderValue::from_static(value),
        );
        assert_eq!(f.send(req).await.status(), StatusCode::FORBIDDEN);
    }
    let mut req = f.request("tools/list", json!({}), Some(&token));
    req.headers_mut()
        .insert("mcp-method", HeaderValue::from_static("tools/call"));
    assert_eq!(f.send(req).await.status(), StatusCode::BAD_REQUEST);
    let response = f
        .send(f.request(
            "tools/list",
            json!({"unused":"x".repeat(65537)}),
            Some(&token),
        ))
        .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn legacy_sessions_are_principal_bound_and_current_requests_ignore_stale_ids() {
    let f = Fixture::new().await;
    let alice = f.token(&f.claims("alice"));
    let bob = f.token(&f.claims("bob"));
    let initialize = Request::builder().method("POST").uri("/mcp").header("host","127.0.0.1:8080")
        .header("authorization",format!("Bearer {alice}")).header("content-type","application/json").header("accept","application/json, text/event-stream")
        .body(Body::from(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"legacy","version":"1"}}}).to_string())).unwrap();
    let response = f.send(initialize).await;
    assert_eq!(response.status(), StatusCode::OK);
    let session = response.headers().get("mcp-session-id").unwrap().clone();
    let _ = payload(response).await;
    let legacy = |method: &str, token: &str, body: Value| {
        Request::builder()
            .method(method)
            .uri("/mcp")
            .header("host", "127.0.0.1:8080")
            .header("accept", "application/json, text/event-stream")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .header("mcp-session-id", session.clone())
            .header("mcp-protocol-version", "2025-11-25")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let response = f
        .send(legacy(
            "POST",
            &alice,
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        ))
        .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let response = f
        .send(legacy(
            "POST",
            &alice,
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
        ))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        payload(response).await["result"]["tools"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let response = f.send(legacy("POST", &alice, json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query","arguments":{"sql":"DELETE FROM secret","params":[]}}}))).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(payload(response).await["result"]["isError"], true);
    for method in ["GET", "POST", "DELETE"] {
        let response = f
            .send(legacy(
                method,
                &bob,
                json!({"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}),
            ))
            .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    let mut req = f.request("tools/list", json!({}), Some(&bob));
    req.headers_mut()
        .insert("mcp-session-id", HeaderValue::from_static("stale"));
    let response = f.send(req).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        payload(response).await["result"]["tools"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn isolated_tls_authenticated_http_query_end_to_end() {
    if std::env::var("POSTGRES_MCP_TEST_DATABASE_URI").is_err() {
        assert_ne!(
            std::env::var("POSTGRES_MCP_REQUIRE_DB_TESTS").as_deref(),
            Ok("1"),
            "TLS DB fixture required"
        );
        eprintln!("SKIP: synthetic TLS HTTP query fixture is not configured");
        return;
    }
    let f = Fixture::new().await;
    for subject in ["alice", "bob"] {
        let token = f.token(&f.claims(subject));
        let response = f.send(f.request("tools/call", json!({"name":"query","arguments":{"sql":"SELECT $1::text AS value, NULL::text AS absent","params":[subject]}}), Some(&token))).await;
        assert_eq!(response.status(), StatusCode::OK);
        let result = payload(response).await;
        assert_ne!(
            result["result"]["isError"], true,
            "synthetic HTTP query failed: {:?}",
            result["result"]["structuredContent"]
        );
        assert_eq!(
            result["result"]["structuredContent"]["rows"][0]["value"],
            subject
        );
        assert_eq!(
            result["result"]["structuredContent"]["rows"][0]["absent"],
            Value::Null
        );
    }
}
