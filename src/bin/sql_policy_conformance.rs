use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use postgres_mcp::sql_safety::classify_restricted_sql;
use serde::{Deserialize, Serialize};

const SQL_POLICY_CONTRACT_VERSION: &str = "sql-restricted/v1";
const SQL_POLICY_REASON: &str = "restricted_sql";

#[derive(Parser, Debug)]
#[command(name = "sql-policy-conformance")]
#[command(about = "Diff postgres sql policy behavior against kernel vectors")]
struct Args {
    /// Path to kernel SQL vector corpus.
    #[arg(long)]
    vectors: PathBuf,
    /// Path to write deterministic report JSON.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct VectorCase {
    op: String,
    case: String,
    input: SqlVectorInput,
    expect: DecisionModel,
}

#[derive(Debug, Clone, Deserialize)]
struct SqlVectorInput {
    policy_contract_version: String,
    sql: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct DecisionModel {
    allow: bool,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CaseMismatch {
    case: String,
    expected: DecisionModel,
    actual: DecisionModel,
    mismatch_fields: Vec<String>,
    actual_classifier_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ConformanceReport {
    vectors_path: String,
    policy_contract_version: String,
    total_cases: usize,
    matched_cases: usize,
    mismatch_count: usize,
    mismatches: Vec<CaseMismatch>,
}

fn evaluate_runtime(input: &SqlVectorInput) -> (DecisionModel, Option<String>) {
    if input.policy_contract_version != SQL_POLICY_CONTRACT_VERSION {
        return (
            DecisionModel {
                allow: false,
                code: Some("CLASSIFIER_UNAVAILABLE".to_string()),
                reason: Some(SQL_POLICY_REASON.to_string()),
            },
            Some(format!(
                "unsupported policy_contract_version: {}",
                input.policy_contract_version
            )),
        );
    }

    match classify_restricted_sql(&input.sql) {
        Ok(()) => (
            DecisionModel {
                allow: true,
                code: None,
                reason: None,
            },
            None,
        ),
        Err(err) => (
            DecisionModel {
                allow: false,
                code: Some(err.code.as_str().to_string()),
                reason: Some(SQL_POLICY_REASON.to_string()),
            },
            Some(err.message),
        ),
    }
}

fn mismatch_fields(expected: &DecisionModel, actual: &DecisionModel) -> Vec<String> {
    let mut fields = Vec::new();
    if expected.allow != actual.allow {
        fields.push("allow".to_string());
    }
    if expected.code != actual.code {
        fields.push("code".to_string());
    }
    if expected.reason != actual.reason {
        fields.push("reason".to_string());
    }
    fields
}

fn write_report(path: &Path, report: &ConformanceReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create output directory {}",
                parent.to_string_lossy()
            )
        })?;
    }
    let body = serde_json::to_string_pretty(report)
        .context("failed to serialize sql policy conformance report")?;
    fs::write(path, format!("{body}\n"))
        .with_context(|| format!("failed to write report {}", path.to_string_lossy()))?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    let raw = fs::read_to_string(&args.vectors).with_context(|| {
        format!(
            "failed to read vectors file {}",
            args.vectors.to_string_lossy()
        )
    })?;
    let cases: Vec<VectorCase> =
        serde_json::from_str(&raw).context("failed to parse vectors JSON")?;

    let mut mismatches = Vec::new();

    for case in &cases {
        if case.op != "sql_restricted_policy_decision" {
            return Err(anyhow::anyhow!(
                "unexpected op '{}' in case '{}'",
                case.op,
                case.case
            ));
        }

        let (actual, message) = evaluate_runtime(&case.input);
        let mismatched_fields = mismatch_fields(&case.expect, &actual);

        if !mismatched_fields.is_empty() {
            mismatches.push(CaseMismatch {
                case: case.case.clone(),
                expected: case.expect.clone(),
                actual,
                mismatch_fields: mismatched_fields,
                actual_classifier_message: message,
            });
        }
    }

    let report = ConformanceReport {
        vectors_path: args.vectors.to_string_lossy().into_owned(),
        policy_contract_version: SQL_POLICY_CONTRACT_VERSION.to_string(),
        total_cases: cases.len(),
        matched_cases: cases.len().saturating_sub(mismatches.len()),
        mismatch_count: mismatches.len(),
        mismatches,
    };

    write_report(&args.output, &report)?;

    if report.mismatch_count > 0 {
        eprintln!(
            "sql policy conformance mismatches: {} (report: {})",
            report.mismatch_count,
            args.output.to_string_lossy()
        );
        std::process::exit(1);
    }

    println!(
        "sql policy conformance ok ({} cases, report: {})",
        report.total_cases,
        args.output.to_string_lossy()
    );
    Ok(())
}
