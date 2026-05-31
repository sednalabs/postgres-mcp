use super::*;

#[rmcp::tool_router(router = tool_router_postgres_health, vis = "pub")]
impl PostgresMcp {
    #[tool(
        name = "analyze_db_health",
        description = "Analyzes database health. Here are the available health checks: index, connection, vacuum, sequence, replication, buffer, constraint, all.",
        execution(task_support = "optional"),
        output_schema = rmcp::handler::server::tool::schema_for_type::<StructuredObjectToolResultSchema>()
    )]
    async fn analyze_db_health(
        &self,
        Parameters(args): Parameters<AnalyzeDbHealthArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let health_type_input = args.health_type.unwrap_or_else(|| "all".to_string());
        let requested = parse_health_types(&health_type_input)?;

        let mut results = BTreeMap::new();

        if requested.contains(&HealthType::Index) {
            results.insert("index", run_index_health(self).await);
        }
        if requested.contains(&HealthType::Connection) {
            results.insert("connection", run_connection_health(self).await);
        }
        if requested.contains(&HealthType::Vacuum) {
            results.insert("vacuum", run_vacuum_health(self).await);
        }
        if requested.contains(&HealthType::Sequence) {
            results.insert("sequence", run_sequence_health(self).await);
        }
        if requested.contains(&HealthType::Replication) {
            results.insert("replication", run_replication_health(self).await);
        }
        if requested.contains(&HealthType::Buffer) {
            results.insert("buffer", run_buffer_health(self).await);
        }
        if requested.contains(&HealthType::Constraint) {
            results.insert("constraint", run_constraint_health(self).await);
        }

        Ok(tool_success(
            self,
            json!({
                "health_type": health_type_input,
                "results": results,
            }),
            elapsed_ms(started),
        ))
    }
}
