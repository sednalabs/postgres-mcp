//! # Startup Dependency Validation
//!
//! Startup-time dependency closure checks with explicit fail/degrade policies.
//!
//! ## Rationale
//! Catch partial initialization and missing relation prerequisites before the
//! server advertises full capability.
//!
//! ## Security Boundaries
//! * Relation names are sanitized and SQL-escaped.
//! * Validation is read-only and startup-scoped.

use std::env;

use anyhow::{Result, anyhow};

use crate::config::StartupRole;
use crate::db::DbEngine;

const ENV_MODE: &str = "POSTGRES_MCP_STARTUP_DEPENDENCY_MODE";
const ENV_REQUIRED_RELATIONS: &str = "POSTGRES_MCP_STARTUP_REQUIRED_RELATIONS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupDependencyMode {
    Off,
    Fail,
    DegradeReadOnly,
}

impl StartupDependencyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Fail => "fail",
            Self::DegradeReadOnly => "degrade-read-only",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "fail" => Ok(Self::Fail),
            "degrade-read-only" | "degrade_read_only" | "degrade" => Ok(Self::DegradeReadOnly),
            _ => Err(anyhow!(
                "invalid {ENV_MODE} value {:?} (expected off|fail|degrade-read-only)",
                raw
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StartupDependencyConfig {
    pub mode: StartupDependencyMode,
    pub required_relations: Vec<String>,
}

impl StartupDependencyConfig {
    pub fn from_env(startup_role: StartupRole) -> Result<Self> {
        let mode = if let Ok(raw) = env::var(ENV_MODE) {
            StartupDependencyMode::parse(&raw)?
        } else {
            match startup_role {
                StartupRole::Runtime => StartupDependencyMode::Off,
                StartupRole::Migrator => StartupDependencyMode::Fail,
            }
        };
        let required_relations = env::var(ENV_REQUIRED_RELATIONS)
            .ok()
            .map(|raw| parse_required_relations(&raw))
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            mode,
            required_relations,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StartupDependencyOutcome {
    pub degraded_read_only: bool,
    pub reason: Option<String>,
    pub missing_relations: Vec<String>,
}

impl StartupDependencyOutcome {
    pub fn healthy() -> Self {
        Self {
            degraded_read_only: false,
            reason: None,
            missing_relations: Vec::new(),
        }
    }
}

pub async fn validate_startup_dependencies(
    db: &DbEngine,
    config: &StartupDependencyConfig,
) -> Result<StartupDependencyOutcome> {
    if config.mode == StartupDependencyMode::Off || config.required_relations.is_empty() {
        return Ok(StartupDependencyOutcome::healthy());
    }

    let mut missing_relations = Vec::new();
    for relation in &config.required_relations {
        if !relation_exists(db, relation).await? {
            missing_relations.push(relation.clone());
        }
    }

    if missing_relations.is_empty() {
        return Ok(StartupDependencyOutcome::healthy());
    }

    match config.mode {
        StartupDependencyMode::Off => Ok(StartupDependencyOutcome::healthy()),
        StartupDependencyMode::Fail => Err(anyhow!(
            "startup dependency validation failed; missing relations: {}",
            missing_relations.join(", ")
        )),
        StartupDependencyMode::DegradeReadOnly => Ok(StartupDependencyOutcome {
            degraded_read_only: true,
            reason: Some("missing_startup_dependencies".to_string()),
            missing_relations,
        }),
    }
}

async fn relation_exists(db: &DbEngine, relation_name: &str) -> Result<bool> {
    let escaped = sql_literal(relation_name);
    let query = format!("SELECT to_regclass({escaped})::text AS relation_name");
    let output = db
        .execute_query_readonly(&query)
        .await
        .map_err(anyhow::Error::new)?;
    let Some(row) = output.rows.first() else {
        return Ok(false);
    };
    let relation_name = row
        .get("relation_name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    Ok(!relation_name.is_empty())
}

fn parse_required_relations(raw: &str) -> Result<Vec<String>> {
    let mut relations = Vec::new();
    for token in raw.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        validate_relation_name(trimmed)?;
        relations.push(trimmed.to_string());
    }
    Ok(relations)
}

fn validate_relation_name(value: &str) -> Result<()> {
    if value.len() > 256 {
        return Err(anyhow!(
            "relation name too long in {ENV_REQUIRED_RELATIONS}: {:?}",
            value
        ));
    }
    for ch in value.chars() {
        let ok = ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.');
        if !ok {
            return Err(anyhow!(
                "invalid relation name {:?} in {ENV_REQUIRED_RELATIONS}; allowed chars [a-zA-Z0-9_.]",
                value
            ));
        }
    }
    if !value.contains('.') {
        return Err(anyhow!(
            "relation name {:?} in {ENV_REQUIRED_RELATIONS} must be schema-qualified (schema.relation)",
            value
        ));
    }
    Ok(())
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::{
        StartupDependencyMode, parse_required_relations, sql_literal, validate_relation_name,
    };

    #[test]
    fn startup_dependency_mode_parse() {
        assert_eq!(
            StartupDependencyMode::parse("off").expect("off"),
            StartupDependencyMode::Off
        );
        assert_eq!(
            StartupDependencyMode::parse("fail").expect("fail"),
            StartupDependencyMode::Fail
        );
        assert_eq!(
            StartupDependencyMode::parse("degrade-read-only").expect("degrade"),
            StartupDependencyMode::DegradeReadOnly
        );
        assert!(StartupDependencyMode::parse("unknown").is_err());
    }

    #[test]
    fn required_relations_must_be_schema_qualified() {
        assert!(validate_relation_name("public.some_view").is_ok());
        assert!(validate_relation_name("some_view").is_err());
        assert!(validate_relation_name("public.bad-name").is_err());
    }

    #[test]
    fn parse_required_relations_trims_and_validates() {
        let relations =
            parse_required_relations(" public.v_a , analytics.v_b ").expect("valid list");
        assert_eq!(relations, vec!["public.v_a", "analytics.v_b"]);
    }

    #[test]
    fn sql_literal_escapes_single_quotes() {
        assert_eq!(sql_literal("a'b"), "'a''b'");
    }

    #[test]
    fn fuzz_smoke_rejects_invalid_relation_tokens() {
        let invalid_tokens = [
            "public.bad-name",
            "public.bad name",
            "public.bad;drop",
            "public.bad/*x*/",
            "public.bad$",
            "public.bad\nline",
            "public.bad\tcol",
            "'public.bad'",
            "\"public.bad\"",
            "public.bad\\",
            "public.bad:",
            "public.bad@",
            "public.bad#",
        ];
        for token in invalid_tokens {
            assert!(
                validate_relation_name(token).is_err(),
                "expected token {:?} to be rejected",
                token
            );
        }
    }

    #[test]
    fn property_valid_tokens_are_accepted() {
        let schemas = ["public", "analytics_2026", "mcp_internal"];
        let names = ["v_mobile", "recrawl_state", "table_001"];
        for schema in schemas {
            for name in names {
                let relation = format!("{schema}.{name}");
                assert!(
                    validate_relation_name(&relation).is_ok(),
                    "expected relation {:?} to be accepted",
                    relation
                );
            }
        }
    }
}
