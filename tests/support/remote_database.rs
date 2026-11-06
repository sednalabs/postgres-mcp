//! Opt-in tests against an operator-provisioned, isolated TLS fixture only.
//! No fixture writes are performed here. CI sets REQUIRE_DB_TESTS=1, turning
//! missing configuration into a hard failure rather than silently skipping.
use super::*;

fn fixture() -> Option<RemotePostgresMcp> {
    let uri = std::env::var("POSTGRES_MCP_TEST_DATABASE_URI").ok();
    let role = std::env::var("POSTGRES_MCP_TEST_DATABASE_ROLE").ok();
    match (uri, role) {
        (Some(uri), Some(role)) if !uri.is_empty() && !role.is_empty() => {
            Some(RemotePostgresMcp::new(
                DbEngine::new(
                    Some(uri),
                    crate::config::AccessMode::Unrestricted,
                    false,
                    None,
                    None,
                    None,
                ),
                "postgres:read".into(),
                role,
            ))
        }
        _ if std::env::var("POSTGRES_MCP_REQUIRE_DB_TESTS").as_deref() == Ok("1") => {
            panic!("isolated TLS database fixture configuration is required")
        }
        _ => {
            eprintln!("SKIP: isolated TLS remote database fixture is not configured");
            None
        }
    }
}

fn payload(result: CallToolResult) -> Value {
    assert_ne!(
        result.is_error,
        Some(true),
        "fixture query unexpectedly failed (details redacted)"
    );
    result.structured_content.expect("structured response")
}

#[tokio::test]
async fn isolated_tls_parameterized_queries_nulls_and_schema() {
    let Some(s) = fixture() else { return };
    let result = payload(s.dispatch("query", json!({"sql":"SELECT $1::text AS value, NULL::text AS absent", "params":["x' OR true --"]})).await);
    assert_eq!(result["rows"][0]["value"], "x' OR true --");
    assert_eq!(result["rows"][0]["absent"], Value::Null);
    assert_eq!(result["truncated"], false);
    let schema = payload(
        s.dispatch(
            "schema",
            json!({"schema":"public", "table":"remote_fixture"}),
        )
        .await,
    );
    assert!(
        !schema["rows"].as_array().unwrap().is_empty(),
        "fixture table is required"
    );
}

#[tokio::test]
async fn isolated_tls_large_wire_values_rows_and_nested_cells_are_bounded() {
    let Some(s) = fixture() else { return };
    // Data is prepared by the fixture, not generated through unapproved SQL functions.
    let result = payload(
        s.dispatch(
            "query",
            json!({"sql":"SELECT id FROM public.remote_fixture ORDER BY id", "params":[]}),
        )
        .await,
    );
    assert_eq!(result["rows"].as_array().unwrap().len(), MAX_ROWS);
    assert_eq!(result["truncated"], true);
    for column in ["large_text", "nested_json", "large_array"] {
        let result = payload(s.dispatch("query", json!({"sql":format!("SELECT {column} FROM public.remote_fixture WHERE id = 1"), "params":[]})).await);
        assert_eq!(result["truncated"], true);
        assert!(serde_json::to_vec(&result).unwrap().len() <= MAX_OUTPUT_BYTES);
    }
}

#[tokio::test]
async fn isolated_tls_connection_is_fresh_and_read_only_even_with_unrestricted_engine() {
    let Some(s) = fixture() else { return };
    let query = "SELECT pg_catalog.pg_backend_pid() AS pid, pg_catalog.current_setting('transaction_read_only') AS read_only";
    let a =
        s.db.execute_remote_read(query, &[], &s.expected_database_role)
            .await
            .unwrap_or_else(|_| panic!("fixture connection failed (details redacted)"));
    let b =
        s.db.execute_remote_read(query, &[], &s.expected_database_role)
            .await
            .unwrap_or_else(|_| panic!("fixture connection failed (details redacted)"));
    assert_eq!(a["rows"][0]["read_only"], "on");
    assert_eq!(b["rows"][0]["read_only"], "on");
    assert_ne!(a["rows"][0]["pid"], b["rows"][0]["pid"]);
    let result =
        s.db.execute_remote_read(
            "DELETE FROM public.remote_fixture RETURNING id",
            &[],
            &s.expected_database_role,
        )
        .await;
    assert!(result.is_err(), "database must independently reject writes");
    let result =
        s.db.execute_remote_read("SELECT 1", &[], "not_the_configured_role")
            .await;
    assert!(
        result.is_err(),
        "unexpected database identity must be rejected"
    );
}

#[tokio::test]
async fn isolated_tls_backend_errors_are_sanitized_and_server_deadline_is_finite() {
    let Some(s) = fixture() else { return };
    let result = s
        .dispatch(
            "query",
            json!({"sql":"SELECT * FROM private_missing_secret_relation", "params":[]}),
        )
        .await;
    assert_eq!(result.is_error, Some(true));
    let wire = serde_json::to_string(&result).unwrap();
    assert!(!wire.contains("private_missing_secret_relation"));
    assert!(!wire.contains("does not exist"));
    // Internal primitive deliberately bypasses lexical pg_sleep rejection to
    // verify its independent server-side statement timeout in an isolated DB.
    let outcome = tokio::time::timeout(
        Duration::from_secs(7),
        s.db.execute_remote_read(
            "SELECT pg_catalog.pg_sleep(20)",
            &[],
            &s.expected_database_role,
        ),
    )
    .await;
    assert!(
        matches!(outcome, Ok(Err(_))),
        "server statement timeout must terminate work"
    );
}

#[tokio::test]
async fn isolated_tls_all_elevated_roles_and_memberships_are_rejected() {
    let Some(_) = fixture() else { return };
    let Some(config) = std::env::var("POSTGRES_MCP_TEST_REJECTED_ROLES").ok() else {
        assert_ne!(
            std::env::var("POSTGRES_MCP_REQUIRE_DB_TESTS").as_deref(),
            Ok("1"),
            "elevated-role fixture configuration is required"
        );
        eprintln!("SKIP: elevated-role fixture configuration is not provided");
        return;
    };
    let fixtures: Vec<Value> = serde_json::from_str(&config).expect("invalid role fixture JSON");
    for role in [
        "remote_fixture_super",
        "remote_fixture_bypass",
        "remote_fixture_createdb",
        "remote_fixture_createrole",
        "remote_fixture_replication",
        "remote_fixture_member",
    ] {
        let record = fixtures
            .iter()
            .find(|v| v["role"] == role)
            .expect("required elevated-role fixture is missing");
        let uri = record["uri"].as_str().expect("missing fixture URI");
        let db = DbEngine::new(
            Some(uri.into()),
            crate::config::AccessMode::Unrestricted,
            false,
            None,
            None,
            None,
        );
        let rejected = db.execute_remote_read("SELECT 1", &[], role).await;
        assert!(
            matches!(rejected, Err(ref err) if err.to_string() == "remote database role rejected"),
            "expected an authenticated elevated-role rejection, not a connection failure"
        );
    }
}

#[tokio::test]
async fn isolated_tls_cancellation_bounds_backend_activity() {
    let Some(s) = fixture() else { return };
    let db = s.db.clone();
    let role = s.expected_database_role.clone();
    let worker = tokio::spawn(async move {
        db.execute_remote_read(
            "SELECT pg_catalog.pg_sleep(20) /* remote_fixture_cancelled_sleep */",
            &[],
            &role,
        )
        .await
    });
    let activity_sql = "SELECT pg_catalog.count(*) AS active FROM pg_catalog.pg_stat_activity WHERE usename = current_user AND state = 'active' AND query LIKE $1";
    let params = [json!("%remote_fixture_cancelled_sleep%")];
    let mut observed = false;
    for _ in 0..20 {
        let status =
            s.db.execute_remote_read(activity_sql, &params, &s.expected_database_role)
                .await
                .unwrap_or_else(|_| panic!("fixture activity inspection failed"));
        if status["rows"][0]["active"].as_u64().unwrap_or(0) > 0 {
            observed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        observed,
        "must observe a live backend query before testing cancellation"
    );
    worker.abort();
    let _ = worker.await;
    // Dropping the client does not immediately interrupt PostgreSQL. Its fixed
    // five-second statement timeout bounds remaining active backend execution.
    let stopped = tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            let status =
                s.db.execute_remote_read(activity_sql, &params, &s.expected_database_role)
                    .await
                    .unwrap_or_else(|_| panic!("fixture activity inspection failed"));
            if status["rows"][0]["active"].as_u64() == Some(0) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        stopped.is_ok(),
        "cancelled query exceeded its backend statement budget"
    );
}

#[tokio::test]
async fn isolated_tls_untrusted_certificate_is_rejected() {
    let Some(s) = fixture() else { return };
    let Some(uri) = std::env::var("POSTGRES_MCP_TEST_UNTRUSTED_DATABASE_URI").ok() else {
        assert_ne!(
            std::env::var("POSTGRES_MCP_REQUIRE_DB_TESTS").as_deref(),
            Ok("1"),
            "untrusted-certificate fixture configuration is required"
        );
        eprintln!("SKIP: untrusted-certificate fixture URI is not provided");
        return;
    };
    let db = DbEngine::new(
        Some(uri),
        crate::config::AccessMode::Restricted,
        false,
        None,
        None,
        None,
    );
    let result = tokio::time::timeout(
        TOOL_TIMEOUT,
        db.execute_remote_read("SELECT 1", &[], &s.expected_database_role),
    )
    .await;
    assert!(
        matches!(result, Ok(Err(_))),
        "an untrusted certificate must not establish a usable connection"
    );
}
