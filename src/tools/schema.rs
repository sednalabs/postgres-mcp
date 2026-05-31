use super::*;

const DISCOVERY_DEFAULT_PAGE_SIZE: usize = 100;
const DISCOVERY_MAX_PAGE_SIZE: usize = 200;
const DISCOVERY_MAX_COLUMNS_PER_OBJECT: usize = 128;

fn discovery_now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn discovery_schema_version_token(scope: &str, schema_name: &str, payload: &Value) -> String {
    let serialized =
        serde_json::to_string(payload).unwrap_or_else(|_| "serialization_error".to_string());
    response_page_hash(&format!("{scope}|{schema_name}|{serialized}"))
}

fn discovery_freshness_meta(scope: &str, schema_name: &str, payload: &Value) -> Value {
    json!({
        "metadata_freshness": {
            "cache_mode": "direct",
            "cache_status": "bypass",
            "invalidation_policy": "schema_version",
            "staleness_bound_ms": 0,
            "as_of_unix_ms": discovery_now_epoch_ms(),
            "schema_name": schema_name,
            "schema_version_token": discovery_schema_version_token(scope, schema_name, payload),
        }
    })
}

fn optional_name_filter_clause(
    column_expr: &str,
    compiled_filter: Option<&CompiledDiscoveryNameFilter>,
) -> String {
    compiled_filter
        .map(|filter| {
            let predicate = ilike_literal_predicate(column_expr, &filter.pattern);
            let mut clause = String::from(" AND ");
            clause.push_str(&predicate);
            clause
        })
        .unwrap_or_default()
}

fn list_objects_sql(
    schema_name: &str,
    object_type: &str,
    compiled_filter: Option<&CompiledDiscoveryNameFilter>,
    include_columns: bool,
    fetch_rows: usize,
    offset: usize,
) -> Result<String, String> {
    match object_type {
        "table" | "view" => {
            let table_type = if object_type == "table" {
                "BASE TABLE"
            } else {
                "VIEW"
            };
            let name_filter = optional_name_filter_clause("t.table_name", compiled_filter);
            if include_columns {
                return Ok(format!(
                    "SELECT t.table_schema, t.table_name, t.table_type, COALESCE(cols.columns, ARRAY[]::text[]) AS columns FROM information_schema.tables t LEFT JOIN LATERAL (SELECT array_agg(c.column_name ORDER BY c.ordinal_position) AS columns FROM (SELECT c.column_name, c.ordinal_position FROM information_schema.columns c WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name ORDER BY c.ordinal_position LIMIT {DISCOVERY_MAX_COLUMNS_PER_OBJECT}) c) cols ON true WHERE t.table_schema = {} AND t.table_type = {}{} ORDER BY t.table_name LIMIT {} OFFSET {}",
                    sql_quote_literal(schema_name),
                    sql_quote_literal(table_type),
                    name_filter,
                    fetch_rows,
                    offset
                ));
            }
            Ok(format!(
                "SELECT table_schema, table_name, table_type FROM information_schema.tables t WHERE t.table_schema = {} AND t.table_type = {}{} ORDER BY t.table_name LIMIT {} OFFSET {}",
                sql_quote_literal(schema_name),
                sql_quote_literal(table_type),
                name_filter,
                fetch_rows,
                offset
            ))
        }
        "sequence" => {
            if include_columns {
                return Err(
                    "include_columns is only supported for object_type table|view".to_string(),
                );
            }
            Ok(format!(
                "SELECT sequence_schema, sequence_name, data_type FROM information_schema.sequences WHERE sequence_schema = {}{} ORDER BY sequence_name LIMIT {} OFFSET {}",
                sql_quote_literal(schema_name),
                optional_name_filter_clause("sequence_name", compiled_filter),
                fetch_rows,
                offset
            ))
        }
        "extension" => {
            if include_columns {
                return Err(
                    "include_columns is only supported for object_type table|view".to_string(),
                );
            }
            Ok(format!(
                "SELECT extname, extversion, extrelocatable FROM pg_extension WHERE TRUE{} ORDER BY extname LIMIT {} OFFSET {}",
                optional_name_filter_clause("extname", compiled_filter),
                fetch_rows,
                offset
            ))
        }
        _ => Err("Unsupported object type: expected table|view|sequence|extension".to_string()),
    }
}

#[rmcp::tool_router(router = tool_router_postgres_schema, vis = "pub")]
impl PostgresMcp {
    #[tool(
        name = "list_schemas",
        description = "List all schemas in the database",
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredRowsToolResultSchema>()
    )]
    async fn list_schemas(
        &self,
        Parameters(_args): Parameters<ListSchemasArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        if self.metadata_access_denied() {
            return Ok(policy_error_result(
                self,
                "METADATA_ACCESS_DENIED",
                "Metadata discovery is denied by policy",
                "metadata_access_denied",
                elapsed_ms(started),
            ));
        }
        let sql = r#"
            SELECT
                schema_name,
                schema_owner,
                CASE
                    WHEN schema_name LIKE 'pg_%' THEN 'System Schema'
                    WHEN schema_name = 'information_schema' THEN 'System Information Schema'
                    ELSE 'User Schema'
                END as schema_type
            FROM information_schema.schemata
            ORDER BY schema_type, schema_name
        "#;

        match self.db.execute_query_readonly(sql).await {
            Ok(output) => {
                let visible_rows = output
                    .rows
                    .into_iter()
                    .filter(|row| {
                        row.get("schema_name")
                            .and_then(Value::as_str)
                            .is_some_and(|schema| self.metadata_schema_visible(schema))
                    })
                    .collect::<Vec<_>>();
                let payload = json!(visible_rows);
                Ok(contract_success(
                    self,
                    payload.clone(),
                    elapsed_ms(started),
                    merge_payload(
                        json!({
                            "returned_rows": payload.as_array().map(Vec::len).unwrap_or(0),
                        }),
                        &discovery_freshness_meta("list_schemas", "*", &payload),
                    ),
                ))
            }
            Err(err) => Ok(db_error_result(
                self,
                "Error listing schemas",
                &err,
                elapsed_ms(started),
            )),
        }
    }

    #[tool(
        name = "list_objects",
        description = "List objects in a schema",
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredRowsToolResultSchema>()
    )]
    async fn list_objects(
        &self,
        Parameters(args): Parameters<ListObjectsArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let schema_name = args.schema_name.trim();
        if schema_name.is_empty() {
            return Ok(error_result(
                self,
                "schema_name must not be empty",
                elapsed_ms(started),
            ));
        }
        if !self.metadata_schema_visible(schema_name) {
            return Ok(policy_error_result(
                self,
                "METADATA_ACCESS_DENIED",
                "Schema is not visible under the configured metadata policy",
                "metadata_access_denied",
                elapsed_ms(started),
            ));
        }

        let object_type = args
            .object_type
            .as_deref()
            .unwrap_or("table")
            .trim()
            .to_ascii_lowercase();
        let compiled_filter = match compile_discovery_name_filter(
            args.name_like.as_deref(),
            args.name_prefix.as_deref(),
            args.name_contains.as_deref(),
            args.name_exact.as_deref(),
            args.name_pattern.as_deref(),
        ) {
            Ok(value) => value,
            Err(message) => {
                return Ok(error_result(self, message.as_str(), elapsed_ms(started)));
            }
        };
        let effective_limit = args
            .limit
            .unwrap_or(DISCOVERY_DEFAULT_PAGE_SIZE)
            .max(1)
            .min(DISCOVERY_MAX_PAGE_SIZE);
        let scope_material = format!(
            "{}|{}|{}|{}|{}",
            schema_name,
            object_type,
            compiled_filter
                .as_ref()
                .map(|filter| filter.mode.as_str())
                .unwrap_or("none"),
            compiled_filter
                .as_ref()
                .map(|filter| filter.pattern.as_str())
                .unwrap_or(""),
            args.include_columns
        );
        let query_hash = response_page_hash(&scope_material);
        let offset = if let Some(raw_cursor) = args.cursor.as_deref() {
            match decode_pagination_cursor(
                self,
                PaginationCursorScope::ListObjects,
                &query_hash,
                raw_cursor,
            ) {
                Ok(cursor) => cursor.offset,
                Err(PaginationCursorDecodeError::QueryMismatch) => {
                    return Ok(policy_error_result(
                        self,
                        "CURSOR_QUERY_MISMATCH",
                        "Invalid pagination cursor",
                        "invalid_cursor",
                        elapsed_ms(started),
                    ));
                }
                Err(PaginationCursorDecodeError::Expired) => {
                    return Ok(policy_error_result(
                        self,
                        "CURSOR_EXPIRED",
                        "Pagination cursor expired",
                        "invalid_cursor",
                        elapsed_ms(started),
                    ));
                }
                Err(PaginationCursorDecodeError::Invalid) => {
                    return Ok(policy_error_result(
                        self,
                        "INVALID_CURSOR",
                        "Invalid pagination cursor",
                        "invalid_cursor",
                        elapsed_ms(started),
                    ));
                }
            }
        } else {
            0
        };
        let fetch_rows = effective_limit.saturating_add(1);
        let sql = match list_objects_sql(
            schema_name,
            object_type.as_str(),
            compiled_filter.as_ref(),
            args.include_columns,
            fetch_rows,
            offset,
        ) {
            Ok(sql) => sql,
            Err(message) => {
                return Ok(error_result(self, message.as_str(), elapsed_ms(started)));
            }
        };

        match self.db.execute_query_readonly(&sql).await {
            Ok(output) => {
                let mut rows = output.rows;
                let truncated = rows.len() > effective_limit;
                if truncated {
                    rows.truncate(effective_limit);
                }
                let payload = json!(rows);
                let next_cursor = if truncated {
                    Some(encode_pagination_cursor(
                        self,
                        PaginationCursorScope::ListObjects,
                        &query_hash,
                        offset.saturating_add(effective_limit),
                    ))
                } else {
                    None
                };
                let next_offset = if truncated {
                    Some(offset.saturating_add(effective_limit))
                } else {
                    None
                };

                Ok(contract_success(
                    self,
                    payload.clone(),
                    elapsed_ms(started),
                    merge_payload(
                        json!({
                            "returned_rows": payload.as_array().map(Vec::len).unwrap_or(0),
                            "has_more": truncated,
                            "truncated": truncated,
                            "next_cursor": next_cursor,
                            "next_offset": next_offset,
                            "query_hash": query_hash,
                            "limit_requested": args.limit,
                            "limit_effective": effective_limit,
                            "limit_hard_cap": DISCOVERY_MAX_PAGE_SIZE,
                            "offset": offset,
                            "column_budget_per_object": if args.include_columns {
                                Some(DISCOVERY_MAX_COLUMNS_PER_OBJECT)
                            } else {
                                None::<usize>
                            },
                        }),
                        &discovery_freshness_meta("list_objects", schema_name, &payload),
                    ),
                ))
            }
            Err(err) => Ok(db_error_result(
                self,
                "Error listing objects",
                &err,
                elapsed_ms(started),
            )),
        }
    }

    #[tool(
        name = "get_object_details",
        description = "Show detailed information about a database object",
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn get_object_details(
        &self,
        Parameters(args): Parameters<GetObjectDetailsArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let schema_name = args.schema_name.trim();
        let object_name = args.object_name.trim();
        if schema_name.is_empty() {
            return Ok(error_result(
                self,
                "schema_name must not be empty",
                elapsed_ms(started),
            ));
        }
        if !self.metadata_schema_visible(schema_name) {
            return Ok(policy_error_result(
                self,
                "METADATA_ACCESS_DENIED",
                "Schema is not visible under the configured metadata policy",
                "metadata_access_denied",
                elapsed_ms(started),
            ));
        }
        if object_name.is_empty() {
            return Ok(error_result(
                self,
                "object_name must not be empty",
                elapsed_ms(started),
            ));
        }

        let object_type = args
            .object_type
            .as_deref()
            .unwrap_or("table")
            .trim()
            .to_ascii_lowercase();

        let result = match object_type.as_str() {
            "table" | "view" => {
                let col_sql = format!(
                    "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
                    sql_quote_literal(schema_name),
                    sql_quote_literal(object_name)
                );
                let con_sql = format!(
                    "SELECT tc.constraint_name, tc.constraint_type, kcu.column_name FROM information_schema.table_constraints tc LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema WHERE tc.table_schema = {} AND tc.table_name = {} ORDER BY tc.constraint_name, kcu.ordinal_position",
                    sql_quote_literal(schema_name),
                    sql_quote_literal(object_name)
                );
                let idx_sql = format!(
                    "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = {} AND tablename = {} ORDER BY indexname",
                    sql_quote_literal(schema_name),
                    sql_quote_literal(object_name)
                );

                let columns = match self.db.execute_query_readonly(&col_sql).await {
                    Ok(output) => output,
                    Err(err) => {
                        return Ok(db_error_result(
                            self,
                            "Error reading table columns",
                            &err,
                            elapsed_ms(started),
                        ));
                    }
                };
                let constraints = match self.db.execute_query_readonly(&con_sql).await {
                    Ok(output) => output,
                    Err(err) => {
                        return Ok(db_error_result(
                            self,
                            "Error reading table constraints",
                            &err,
                            elapsed_ms(started),
                        ));
                    }
                };
                let indexes = match self.db.execute_query_readonly(&idx_sql).await {
                    Ok(output) => output,
                    Err(err) => {
                        return Ok(db_error_result(
                            self,
                            "Error reading table indexes",
                            &err,
                            elapsed_ms(started),
                        ));
                    }
                };

                json!({
                    "basic": {
                        "schema": schema_name,
                        "name": object_name,
                        "type": object_type,
                    },
                    "columns": columns.rows,
                    "constraints": constraints.rows,
                    "indexes": indexes.rows,
                })
            }
            "sequence" => {
                let sql = format!(
                    "SELECT sequence_schema, sequence_name, data_type, start_value, increment FROM information_schema.sequences WHERE sequence_schema = {} AND sequence_name = {}",
                    sql_quote_literal(schema_name),
                    sql_quote_literal(object_name)
                );
                match self.db.execute_query_readonly(&sql).await {
                    Ok(output) => json!(output.rows),
                    Err(err) => {
                        return Ok(db_error_result(
                            self,
                            "Error reading sequence details",
                            &err,
                            elapsed_ms(started),
                        ));
                    }
                }
            }
            "extension" => {
                let sql = format!(
                    "SELECT extname, extversion, extrelocatable FROM pg_extension WHERE extname = {}",
                    sql_quote_literal(object_name)
                );
                match self.db.execute_query_readonly(&sql).await {
                    Ok(output) => json!(output.rows),
                    Err(err) => {
                        return Ok(db_error_result(
                            self,
                            "Error reading extension details",
                            &err,
                            elapsed_ms(started),
                        ));
                    }
                }
            }
            _ => {
                return Ok(error_result(
                    self,
                    "Unsupported object type: expected table|view|sequence|extension",
                    elapsed_ms(started),
                ));
            }
        };

        Ok(contract_success(
            self,
            result.clone(),
            elapsed_ms(started),
            merge_payload(
                json!({}),
                &discovery_freshness_meta("get_object_details", schema_name, &result),
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compile_filter(
        name_like: Option<&str>,
        name_prefix: Option<&str>,
        name_contains: Option<&str>,
        name_exact: Option<&str>,
        name_pattern: Option<&str>,
    ) -> Option<CompiledDiscoveryNameFilter> {
        compile_discovery_name_filter(
            name_like,
            name_prefix,
            name_contains,
            name_exact,
            name_pattern,
        )
        .expect("filter should compile")
    }

    #[test]
    fn list_objects_sql_keeps_default_table_projection() {
        let sql = list_objects_sql("public", "table", None, false, 101, 0)
            .expect("default table list SQL should build");
        assert!(sql.contains("SELECT table_schema, table_name, table_type"));
        assert!(sql.contains("t.table_type = 'BASE TABLE'"));
        assert!(!sql.contains("array_agg"));
    }

    #[test]
    fn list_objects_sql_include_columns_adds_lateral_column_probe() {
        let sql = list_objects_sql("public", "view", None, true, 101, 0)
            .expect("include_columns query should build for views");
        assert!(sql.contains("LEFT JOIN LATERAL"));
        assert!(sql.contains("array_agg(c.column_name ORDER BY c.ordinal_position)"));
        assert!(sql.contains("COALESCE(cols.columns, ARRAY[]::text[]) AS columns"));
        assert!(sql.contains("LIMIT 128"));
    }

    #[test]
    fn list_objects_sql_name_like_without_wildcards_uses_substring_mode() {
        let filter = compile_filter(Some("coverage"), None, None, None, None);
        let sql = list_objects_sql("public", "table", filter.as_ref(), false, 101, 0)
            .expect("name_like query should build");
        assert!(sql.contains("ILIKE E'%coverage%' ESCAPE E'\\\\'"));
    }

    #[test]
    fn list_objects_sql_name_like_with_wildcards_preserves_pattern_mode() {
        let filter = compile_filter(Some("coverage_%"), None, None, None, None);
        let sql = list_objects_sql("public", "table", filter.as_ref(), false, 101, 0)
            .expect("name_like wildcard query should build");
        assert!(sql.contains("ILIKE E'coverage_%' ESCAPE E'\\\\'"));
    }

    #[test]
    fn list_objects_sql_name_like_supports_literal_wildcard_escape() {
        let filter = compile_filter(Some(r"coverage\_%"), None, None, None, None);
        let sql = list_objects_sql("public", "table", filter.as_ref(), false, 101, 0)
            .expect("name_like escaped wildcard query should build");
        assert!(sql.contains(r"ILIKE E'coverage\\_%' ESCAPE E'\\'"));
    }

    #[test]
    fn list_objects_sql_name_like_rejects_dangling_escape() {
        let err = compile_discovery_name_filter(Some("coverage\\"), None, None, None, None)
            .expect_err("dangling escape should be rejected");
        assert_eq!(
            err,
            "name_like must not end with an unfinished escape ('\\\\')"
        );
    }

    #[test]
    fn list_objects_sql_name_prefix_builds_prefix_pattern() {
        let filter = compile_filter(None, Some("v_mobile_"), None, None, None);
        let sql = list_objects_sql("public", "table", filter.as_ref(), false, 101, 0)
            .expect("name_prefix query should build");
        assert!(sql.contains(r"ILIKE E'v\\_mobile\\_%' ESCAPE E'\\'"));
    }

    #[test]
    fn list_objects_sql_name_contains_builds_contains_pattern() {
        let filter = compile_filter(None, None, Some("coverage"), None, None);
        let sql = list_objects_sql("public", "table", filter.as_ref(), false, 101, 0)
            .expect("name_contains query should build");
        assert!(sql.contains("ILIKE E'%coverage%' ESCAPE E'\\\\'"));
    }

    #[test]
    fn list_objects_sql_name_exact_builds_literal_pattern() {
        let filter = compile_filter(None, None, None, Some("coverage_exact"), None);
        let sql = list_objects_sql("public", "table", filter.as_ref(), false, 101, 0)
            .expect("name_exact query should build");
        assert!(sql.contains(r"ILIKE E'coverage\\_exact' ESCAPE E'\\'"));
        assert!(!sql.contains("%coverage"));
    }

    #[test]
    fn list_objects_sql_name_pattern_preserves_pattern_mode() {
        let filter = compile_filter(None, None, None, None, Some("coverage_%"));
        let sql = list_objects_sql("public", "table", filter.as_ref(), false, 101, 0)
            .expect("name_pattern query should build");
        assert!(sql.contains("ILIKE E'coverage_%' ESCAPE E'\\\\'"));
    }

    #[test]
    fn list_objects_sql_sequence_with_name_prefix_keeps_stable_ordering() {
        let filter = compile_filter(None, Some("recrawl_"), None, None, None);
        let sql = list_objects_sql("public", "sequence", filter.as_ref(), false, 101, 0)
            .expect("sequence query should build");
        assert!(sql.contains(r"sequence_name ILIKE E'recrawl\\_%' ESCAPE E'\\'"));
        assert!(sql.contains("ORDER BY sequence_name LIMIT 101 OFFSET 0"));
    }

    #[test]
    fn list_objects_sql_extension_with_name_like_keeps_stable_ordering() {
        let filter = compile_filter(Some("post"), None, None, None, None);
        let sql = list_objects_sql("public", "extension", filter.as_ref(), false, 101, 0)
            .expect("extension query should build");
        assert!(sql.contains(r"extname ILIKE E'%post%' ESCAPE E'\\'"));
        assert!(sql.contains("ORDER BY extname LIMIT 101 OFFSET 0"));
    }

    #[test]
    fn list_objects_sql_rejects_multiple_filter_inputs_together() {
        let err =
            compile_discovery_name_filter(Some("coverage"), Some("coverage_"), None, None, None)
                .expect_err("conflicting filters should be rejected");
        assert_eq!(
            err,
            "only one name filter may be provided: name_like, name_prefix, name_contains, name_exact, name_pattern"
        );
    }

    #[test]
    fn list_objects_sql_rejects_include_columns_for_sequences() {
        let err = list_objects_sql("public", "sequence", None, true, 101, 0)
            .expect_err("include_columns should be rejected for sequence");
        assert_eq!(
            err,
            "include_columns is only supported for object_type table|view"
        );
    }

    #[test]
    fn list_objects_sql_rejects_include_columns_for_extensions() {
        let err = list_objects_sql("public", "extension", None, true, 101, 0)
            .expect_err("include_columns should be rejected for extension");
        assert_eq!(
            err,
            "include_columns is only supported for object_type table|view"
        );
    }

    #[test]
    fn discovery_schema_version_token_changes_with_payload_shape() {
        let base = json!([{ "table_name": "alpha" }]);
        let changed = json!([{ "table_name": "beta" }]);
        let token_a = discovery_schema_version_token("list_objects", "public", &base);
        let token_b = discovery_schema_version_token("list_objects", "public", &changed);
        assert_ne!(token_a, token_b);
    }

    #[test]
    fn discovery_freshness_meta_exposes_contract_fields() {
        let payload = json!([{ "schema_name": "public" }]);
        let meta = discovery_freshness_meta("list_schemas", "*", &payload);
        assert_eq!(
            meta.pointer("/metadata_freshness/cache_mode")
                .and_then(Value::as_str),
            Some("direct")
        );
        assert_eq!(
            meta.pointer("/metadata_freshness/invalidation_policy")
                .and_then(Value::as_str),
            Some("schema_version")
        );
        assert_eq!(
            meta.pointer("/metadata_freshness/staleness_bound_ms")
                .and_then(Value::as_u64),
            Some(0)
        );
        assert!(
            meta.pointer("/metadata_freshness/schema_version_token")
                .and_then(Value::as_str)
                .is_some_and(|token| token.len() == 16)
        );
    }
}
