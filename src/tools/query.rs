use super::*;

const LATEST_SNAPSHOT_HELPER: &str = "latest_snapshot";
const MAX_LATEST_SNAPSHOT_PARTITION_COLUMNS: usize = 16;
const EXECUTE_SQL_STATEMENT_TIMEOUT_OVERRIDE_MAX_MS: u64 = 300_000;
const QUERY_STATUS_WAIT_MS_MAX: u64 = 3_600_000;
const PROFILE_FAST_AGENT_PAGE_SIZE_CAP: usize = 100;
const PROFILE_FAST_AGENT_MAX_CELL_CHARS: usize = 256;
const PROFILE_HEAVY_VIEW_STATEMENT_TIMEOUT_MS: u64 = 300_000;
const PINNED_SESSION_IDLE_TTL_MAX_MS: u64 = 3_600_000;

#[derive(Debug, Deserialize, JsonSchema)]
struct SessionOpenArgs {
    #[serde(default)]
    #[schemars(range(min = 1, max = 3600000))]
    idle_ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SessionIdArgs {
    session_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryStatusWaitMode {
    Immediate,
    Deadline { wait_ms: u64 },
    UntilTerminal,
}

impl QueryStatusWaitMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::Deadline { .. } => "deadline",
            Self::UntilTerminal => "until_terminal",
        }
    }
}

#[derive(Debug)]
struct LatestSnapshotRewrite {
    sql: String,
    helper_count: usize,
}

#[derive(Debug, Default)]
struct LatestSnapshotArgs {
    source: Option<String>,
    ts_column: Option<String>,
    partition_by: Vec<String>,
    tie_breakers: Vec<String>,
    include_null_timestamps: bool,
    nulls_first: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelperScanState {
    Normal,
    SingleQuote,
    DoubleQuote,
    LineComment,
    BlockComment,
    DollarQuote(usize),
}

fn rewrite_latest_snapshot_helpers(sql: &str) -> Result<LatestSnapshotRewrite, String> {
    let mut output = String::with_capacity(sql.len());
    let mut helper_count = 0usize;
    let mut state = HelperScanState::Normal;
    let mut pending_dollar_quote_end = 0usize;
    let mut last_emit = 0usize;

    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        match state {
            HelperScanState::Normal => {
                if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    state = HelperScanState::LineComment;
                    i += 2;
                    continue;
                }

                if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    state = HelperScanState::BlockComment;
                    i += 2;
                    continue;
                }

                if bytes[i] == b'\'' {
                    state = HelperScanState::SingleQuote;
                    i += 1;
                    continue;
                }

                if bytes[i] == b'"' {
                    state = HelperScanState::DoubleQuote;
                    i += 1;
                    continue;
                }

                if let Some((delimiter_len, start)) = parse_dollar_quote_open(sql, i)? {
                    state = HelperScanState::DollarQuote(delimiter_len);
                    pending_dollar_quote_end = start;
                    i += delimiter_len;
                    continue;
                }

                if let Some((replacement, next_i)) = expand_latest_snapshot_invocation(sql, i)? {
                    if next_i > i {
                        output.push_str(&sql[last_emit..i]);
                        output.push_str(&replacement);
                        last_emit = next_i;
                        helper_count += 1;
                    }
                    i = next_i;
                    continue;
                }

                i += 1;
            }
            HelperScanState::SingleQuote => {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::DoubleQuote => {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::LineComment => {
                if bytes[i] == b'\n' {
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::BlockComment => {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    state = HelperScanState::Normal;
                    i += 2;
                    continue;
                }
                i += 1;
            }
            HelperScanState::DollarQuote(end_len) => {
                if i + end_len <= bytes.len()
                    && is_dollar_quote_end(sql, i, end_len, pending_dollar_quote_end)
                {
                    state = HelperScanState::Normal;
                    i += end_len;
                    continue;
                }
                i += 1;
            }
        }
    }

    output.push_str(&sql[last_emit..]);

    Ok(LatestSnapshotRewrite {
        sql: output,
        helper_count,
    })
}

fn pinned_session_not_found_result(
    server: &PostgresMcp,
    session_id: &str,
    elapsed_ms: u64,
) -> CallToolResult {
    contract_error(
        server,
        json!({
            "error": format!("pinned session {session_id} was not found or has expired"),
            "code": "PINNED_SESSION_NOT_FOUND",
            "reason": "pinned_session_not_found",
        }),
        elapsed_ms,
        json!({}),
    )
}

fn apply_pinned_session_meta(
    mut result: CallToolResult,
    snapshot: Option<&crate::server::PinnedSessionSnapshot>,
) -> CallToolResult {
    let Some(snapshot) = snapshot else {
        return result;
    };
    let Some(structured) = result.structured_content.as_mut() else {
        return result;
    };
    let Some(meta) = structured.get_mut("meta").and_then(Value::as_object_mut) else {
        return result;
    };
    meta.insert(
        "pinned_session".to_string(),
        serde_json::to_value(snapshot).unwrap_or(Value::Null),
    );
    result
}

pub(super) async fn execute_sql_with_optional_session(
    server: &PostgresMcp,
    session_id: Option<&str>,
    sql: &str,
    params: &[Value],
    statement_timeout_override: Option<Duration>,
    elapsed_ms: u64,
) -> Result<
    (
        crate::db::DbResult<QueryOutput>,
        Option<crate::server::PinnedSessionSnapshot>,
    ),
    CallToolResult,
> {
    let Some(session_id) = session_id else {
        let result = if params.is_empty() {
            server
                .db
                .execute_user_sql_with_statement_timeout(sql, statement_timeout_override)
                .await
        } else {
            server
                .db
                .execute_user_sql_with_params_and_statement_timeout(
                    sql,
                    params,
                    statement_timeout_override,
                )
                .await
        };
        return Ok((result, None));
    };

    let Some(session) = server.pinned_session(session_id) else {
        return Err(pinned_session_not_found_result(
            server, session_id, elapsed_ms,
        ));
    };
    let result = if params.is_empty() {
        session
            .execute_sql_with_statement_timeout(sql, statement_timeout_override)
            .await
    } else {
        session
            .execute_sql_with_params_and_statement_timeout(sql, params, statement_timeout_override)
            .await
    };
    let session_snapshot = match &result {
        Ok(_) => server.touch_pinned_session(session_id, leading_statement_keyword(sql).as_deref()),
        Err(err) if err.code() == "DB_SESSION_CLOSED" => server.close_pinned_session(session_id),
        Err(_) => {
            server.touch_pinned_session(session_id, leading_statement_keyword(sql).as_deref())
        }
    };
    Ok((result, session_snapshot))
}

pub(super) async fn describe_sql_with_optional_session(
    server: &PostgresMcp,
    session_id: Option<&str>,
    sql: &str,
    params: &[Value],
    statement_timeout_override: Option<Duration>,
    elapsed_ms: u64,
) -> Result<
    (
        crate::db::DbResult<Vec<QueryColumn>>,
        Option<crate::server::PinnedSessionSnapshot>,
    ),
    CallToolResult,
> {
    let Some(session_id) = session_id else {
        return Ok((
            server
                .db
                .describe_user_sql_with_params_and_statement_timeout(
                    sql,
                    params,
                    statement_timeout_override,
                )
                .await,
            None,
        ));
    };
    let Some(session) = server.pinned_session(session_id) else {
        return Err(pinned_session_not_found_result(
            server, session_id, elapsed_ms,
        ));
    };
    let result = session
        .describe_sql_with_params_and_statement_timeout(sql, params, statement_timeout_override)
        .await;
    let session_snapshot = match &result {
        Ok(_) => server.touch_pinned_session(session_id, Some("describe")),
        Err(err) if err.code() == "DB_SESSION_CLOSED" => server.close_pinned_session(session_id),
        Err(_) => server.touch_pinned_session(session_id, Some("describe")),
    };
    Ok((result, session_snapshot))
}

fn parse_dollar_quote_open(sql: &str, start: usize) -> Result<Option<(usize, usize)>, String> {
    if sql.as_bytes()[start] != b'$' {
        return Ok(None);
    }

    let mut end = start + 1;
    let bytes = sql.as_bytes();
    while end < bytes.len() {
        let byte = bytes[end];
        if byte == b'$' {
            let tag_len = end + 1 - start;
            return Ok(Some((tag_len, end + 1)));
        }
        if parse_dollar_tag_char(byte) {
            end += 1;
            continue;
        }
        break;
    }
    Ok(None)
}

fn is_dollar_quote_end(
    sql: &str,
    position: usize,
    delimiter_len: usize,
    delimiter_end: usize,
) -> bool {
    if delimiter_len == 0 {
        return false;
    }
    if position + delimiter_len > sql.len() {
        return false;
    }
    let bytes = sql.as_bytes();
    if bytes[position..position + delimiter_len]
        != bytes[(delimiter_end - delimiter_len)..delimiter_end]
    {
        return false;
    }
    true
}

fn parse_dollar_tag_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn expand_latest_snapshot_invocation(
    sql: &str,
    start: usize,
) -> Result<Option<(String, usize)>, String> {
    if start >= sql.len() {
        return Ok(None);
    }

    if !is_name_match(sql, start, LATEST_SNAPSHOT_HELPER) {
        return Ok(None);
    }
    if !is_relation_helper_position(sql, start) {
        return Ok(None);
    }

    let bytes = sql.as_bytes();
    let helper_len = LATEST_SNAPSHOT_HELPER.len();
    let mut i = start + helper_len;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b'(' {
        return Ok(None);
    }
    let open_paren = i;
    let close_paren = find_matching_paren(sql, open_paren)
        .ok_or_else(|| "unclosed latest_snapshot() helper invocation".to_string())?;
    let args_text = &sql[open_paren + 1..close_paren];

    let args = parse_latest_snapshot_arguments(args_text)?;
    let replacement = latest_snapshot_rewrite_sql(&args)?;
    Ok(Some((replacement, close_paren + 1)))
}

fn is_relation_helper_position(sql: &str, helper_start: usize) -> bool {
    if helper_start == 0 {
        return false;
    }
    let normalized_prefix = match strip_sql_comments(&sql[..helper_start]) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let bytes = normalized_prefix.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let mut i = helper_start.min(bytes.len());
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    if i == 0 {
        return false;
    }
    if bytes[i - 1] == b',' {
        return true;
    }
    if !is_identifier_char(bytes[i - 1]) {
        return false;
    }
    let token_end = i;
    while i > 0 && is_identifier_char(bytes[i - 1]) {
        i -= 1;
    }
    let token = &normalized_prefix[i..token_end];
    token.eq_ignore_ascii_case("from")
        || token.eq_ignore_ascii_case("join")
        || token.eq_ignore_ascii_case("lateral")
}

fn is_name_match(sql: &str, start: usize, expected: &str) -> bool {
    let bytes = sql.as_bytes();
    let end = start + expected.len();
    if end > bytes.len() {
        return false;
    }

    if !sql[start..end].eq_ignore_ascii_case(expected) {
        return false;
    }

    let prev_ok = start == 0 || !is_identifier_char(bytes[start - 1]);
    let next_ok = end == bytes.len() || !is_identifier_char(bytes[end]);
    prev_ok && next_ok
}

fn is_identifier_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.'
}

fn find_matching_paren(sql: &str, open_paren: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if open_paren >= bytes.len() || bytes[open_paren] != b'(' {
        return None;
    }

    let mut depth = 1usize;
    let mut i = open_paren + 1;
    let mut state = HelperScanState::Normal;
    let mut pending_dollar_quote_end = 0usize;

    while i < bytes.len() {
        match state {
            HelperScanState::Normal => {
                if bytes[i] == b'\'' {
                    state = HelperScanState::SingleQuote;
                    i += 1;
                    continue;
                }
                if bytes[i] == b'"' {
                    state = HelperScanState::DoubleQuote;
                    i += 1;
                    continue;
                }
                if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    state = HelperScanState::LineComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    state = HelperScanState::BlockComment;
                    i += 2;
                    continue;
                }
                if let Ok(Some((delimiter_len, end))) = parse_dollar_quote_open(sql, i)
                    && delimiter_len > 0
                {
                    state = HelperScanState::DollarQuote(delimiter_len);
                    pending_dollar_quote_end = end;
                    i += delimiter_len;
                    continue;
                }
                if bytes[i] == b'(' {
                    depth += 1;
                } else if bytes[i] == b')' {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Some(i);
                    }
                }
                i += 1;
            }
            HelperScanState::SingleQuote => {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::DoubleQuote => {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::LineComment => {
                if bytes[i] == b'\n' {
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::BlockComment => {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    state = HelperScanState::Normal;
                    i += 2;
                    continue;
                }
                i += 1;
            }
            HelperScanState::DollarQuote(end_len) => {
                if i + end_len <= bytes.len()
                    && is_dollar_quote_end(sql, i, end_len, pending_dollar_quote_end)
                {
                    state = HelperScanState::Normal;
                    i += end_len;
                    continue;
                }
                i += 1;
            }
        }
    }

    None
}

fn parse_latest_snapshot_arguments(args_text: &str) -> Result<LatestSnapshotArgs, String> {
    let mut parsed = LatestSnapshotArgs::default();
    let args = split_top_level_csv(args_text)?;
    for arg in args {
        let normalized_arg = strip_sql_comments(&arg)?;
        if normalized_arg.trim().is_empty() {
            continue;
        }
        let (raw_name, raw_value) = parse_named_argument(&normalized_arg)
            .ok_or_else(|| "latest_snapshot() requires key => value arguments".to_string())?;

        match raw_name.as_str() {
            "source" => parsed.source = Some(parse_source_identifier(&raw_value)?),
            "ts_column" => parsed.ts_column = Some(parse_identifier_value(&raw_value, false)?),
            "partition_by" => parsed.partition_by = parse_identifier_array(&raw_value)?,
            "tie_breakers" => parsed.tie_breakers = parse_identifier_array(&raw_value)?,
            "include_null_timestamps" => {
                parsed.include_null_timestamps = parse_bool_arg(&raw_value)?
            }
            "nulls_first" => parsed.nulls_first = parse_bool_arg(&raw_value)?,
            _ => {
                return Err(format!("unknown latest_snapshot argument '{raw_name}'"));
            }
        }
    }

    parsed
        .source
        .as_deref()
        .ok_or_else(|| "latest_snapshot() requires argument 'source'".to_string())?;
    parsed
        .ts_column
        .as_deref()
        .ok_or_else(|| "latest_snapshot() requires argument 'ts_column'".to_string())?;

    if parsed.partition_by.len() > MAX_LATEST_SNAPSHOT_PARTITION_COLUMNS {
        return Err(format!(
            "latest_snapshot() partition_by exceeds maximum of {} columns",
            MAX_LATEST_SNAPSHOT_PARTITION_COLUMNS
        ));
    }

    if parsed.tie_breakers.len() > MAX_HYPOTHETICAL_INDEX_COLUMNS {
        return Err(format!(
            "latest_snapshot() tie_breakers exceeds maximum of {} columns",
            MAX_HYPOTHETICAL_INDEX_COLUMNS
        ));
    }

    Ok(parsed)
}

fn parse_named_argument(arg: &str) -> Option<(String, String)> {
    let bytes = arg.as_bytes();
    let mut i = 0usize;
    let mut state = HelperScanState::Normal;
    let mut start = 0usize;
    let mut end = 0usize;

    while i < bytes.len() {
        match state {
            HelperScanState::Normal => {
                if bytes[i] == b'\'' {
                    state = HelperScanState::SingleQuote;
                    i += 1;
                    continue;
                }
                if bytes[i] == b'"' {
                    state = HelperScanState::DoubleQuote;
                    i += 1;
                    continue;
                }
                if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    break;
                }
                if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    state = HelperScanState::BlockComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'=' && i + 1 < bytes.len() && bytes[i + 1] == b'>' {
                    end = i;
                    start = i + 2;
                    break;
                }
                i += 1;
            }
            HelperScanState::SingleQuote => {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::DoubleQuote => {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::LineComment => {
                if bytes[i] == b'\n' {
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::BlockComment => {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    state = HelperScanState::Normal;
                    i += 2;
                    continue;
                }
                i += 1;
            }
            HelperScanState::DollarQuote(_) => {
                i += 1;
            }
        }
    }

    if start == 0 {
        return None;
    }
    let name = arg[..end].trim().to_lowercase();
    let value = arg[start..].trim().trim_end_matches(',').to_string();
    if name.is_empty() || value.is_empty() {
        return None;
    }
    Some((name, value))
}

fn parse_bool_arg(raw: &str) -> Result<bool, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("invalid boolean value '{raw}'")),
    }
}

fn parse_source_identifier(raw: &str) -> Result<String, String> {
    let value = parse_identifier_value(raw, true)?;
    if value.len() > MAX_QUALIFIED_IDENTIFIER_BYTES {
        return Err("source identifier exceeds maximum length".to_string());
    }
    Ok(value)
}

fn parse_identifier_value(raw: &str, allow_qualified: bool) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("identifier cannot be empty".to_string());
    }

    if trimmed.len() > MAX_IDENTIFIER_BYTES {
        return Err(format!(
            "identifier exceeds maximum length of {MAX_IDENTIFIER_BYTES} bytes"
        ));
    }

    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() > 1 {
        return parse_single_quoted_identifier(trimmed);
    }

    if !allow_qualified && trimmed.contains('.') {
        return Err("identifier must not be qualified".to_string());
    }
    if !is_safe_identifier(trimmed, allow_qualified) {
        return Err(format!("invalid identifier '{trimmed}'"));
    }

    Ok(trimmed.to_string())
}

fn parse_identifier_array(raw: &str) -> Result<Vec<String>, String> {
    let trimmed = raw.trim();
    if !trimmed.to_ascii_lowercase().starts_with("array") {
        return Err("array arguments must use ARRAY[...]".to_string());
    }
    let array_open = trimmed
        .find('[')
        .ok_or_else(|| "array arguments must use ARRAY[...]".to_string())?;
    let array_close = trimmed
        .rfind(']')
        .ok_or_else(|| "array arguments must use ARRAY[...]".to_string())?;
    if array_open + 1 > array_close {
        return Err("array argument is empty".to_string());
    }
    let content = &trimmed[array_open + 1..array_close];
    let values = split_top_level_csv(content)?;
    let mut parsed = Vec::new();
    for value in values {
        let normalized_value = strip_sql_comments(&value)?;
        let trimmed_value = normalized_value.trim();
        if trimmed_value.is_empty() {
            continue;
        }
        parsed.push(parse_identifier_value(trimmed_value, false)?);
    }
    Ok(parsed)
}

fn parse_single_quoted_identifier(raw: &str) -> Result<String, String> {
    let mut escaped = String::new();
    let inner = raw
        .strip_prefix('\'')
        .and_then(|rest| rest.strip_suffix('\''))
        .ok_or_else(|| "expected single-quoted identifier".to_string())?;

    let mut chars = inner.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if chars.peek() == Some(&'\'') {
                escaped.push('\'');
                chars.next();
                continue;
            }
            return Err("unterminated single-quoted identifier".to_string());
        }
        escaped.push(ch);
    }

    Ok(escaped)
}

fn is_safe_identifier(value: &str, allow_qualified: bool) -> bool {
    let valid_segment = |segment: &str| {
        segment
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
    };
    if allow_qualified {
        value
            .split('.')
            .all(|segment| !segment.is_empty() && valid_segment(segment))
    } else {
        valid_segment(value)
    }
}

fn split_top_level_csv(input: &str) -> Result<Vec<String>, String> {
    let mut args = Vec::new();
    let bytes = input.as_bytes();
    let mut state = HelperScanState::Normal;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match state {
            HelperScanState::Normal => {
                if bytes[i] == b'\'' {
                    state = HelperScanState::SingleQuote;
                    i += 1;
                    continue;
                }
                if bytes[i] == b'"' {
                    state = HelperScanState::DoubleQuote;
                    i += 1;
                    continue;
                }
                if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    state = HelperScanState::LineComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    state = HelperScanState::BlockComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'(' {
                    state = HelperScanState::Normal;
                    let close = find_matching_paren(input, i);
                    if let Some(end_paren) = close {
                        i = end_paren + 1;
                    } else {
                        return Err("unterminated parenthesis in argument list".to_string());
                    }
                    continue;
                }
                if bytes[i] == b'[' {
                    let close = find_matching_bracket(input, i)
                        .ok_or_else(|| "unterminated array bracket in argument list".to_string())?;
                    i = close + 1;
                    continue;
                }
                if bytes[i] == b',' {
                    args.push(input[start..i].to_string());
                    i += 1;
                    start = i;
                    continue;
                }
                i += 1;
            }
            HelperScanState::SingleQuote => {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::DoubleQuote => {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::LineComment => {
                if bytes[i] == b'\n' {
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::BlockComment => {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    state = HelperScanState::Normal;
                    i += 2;
                    continue;
                }
                i += 1;
            }
            HelperScanState::DollarQuote(_) => {
                i += 1;
            }
        }
    }
    if state == HelperScanState::LineComment {
        state = HelperScanState::Normal;
    }
    if state != HelperScanState::Normal {
        return Err("unterminated quoted expression in argument list".to_string());
    }
    args.push(input[start..].to_string());
    Ok(args)
}

fn strip_sql_comments(input: &str) -> Result<String, String> {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut state = HelperScanState::Normal;
    let mut pending_dollar_quote_end = 0usize;
    let mut block_comment_depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match state {
            HelperScanState::Normal => {
                if bytes[i] == b'\'' {
                    state = HelperScanState::SingleQuote;
                    output.push('\'');
                    i += 1;
                    continue;
                }
                if bytes[i] == b'"' {
                    state = HelperScanState::DoubleQuote;
                    output.push('"');
                    i += 1;
                    continue;
                }
                if let Some((delimiter_len, start)) = parse_dollar_quote_open(input, i)? {
                    state = HelperScanState::DollarQuote(delimiter_len);
                    pending_dollar_quote_end = start;
                    output.push_str(&input[i..i + delimiter_len]);
                    i += delimiter_len;
                    continue;
                }
                if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    state = HelperScanState::LineComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    state = HelperScanState::BlockComment;
                    block_comment_depth = 1;
                    i += 2;
                    continue;
                }
                output.push(bytes[i] as char);
                i += 1;
            }
            HelperScanState::SingleQuote => {
                output.push(bytes[i] as char);
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        output.push('\'');
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::DoubleQuote => {
                output.push(bytes[i] as char);
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        output.push('"');
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::LineComment => {
                if bytes[i] == b'\n' {
                    output.push('\n');
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::BlockComment => {
                if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    block_comment_depth = block_comment_depth.saturating_add(1);
                    i += 2;
                    continue;
                }
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    block_comment_depth = block_comment_depth.saturating_sub(1);
                    if block_comment_depth == 0 {
                        let prev_byte = output.as_bytes().last().copied();
                        let next_byte = bytes.get(i + 2).copied();
                        if prev_byte.map(is_identifier_char).unwrap_or(false)
                            && next_byte.map(is_identifier_char).unwrap_or(false)
                        {
                            output.push(' ');
                        }
                        state = HelperScanState::Normal;
                    }
                    i += 2;
                    continue;
                }
                i += 1;
            }
            HelperScanState::DollarQuote(_) => {
                if let HelperScanState::DollarQuote(end_len) = state {
                    if i + end_len <= bytes.len()
                        && is_dollar_quote_end(input, i, end_len, pending_dollar_quote_end)
                    {
                        state = HelperScanState::Normal;
                        output.push_str(&input[i..i + end_len]);
                        i += end_len;
                        continue;
                    }
                }
                output.push(bytes[i] as char);
                i += 1;
            }
        }
    }
    if state == HelperScanState::LineComment {
        state = HelperScanState::Normal;
    }
    if state != HelperScanState::Normal {
        return Err("unterminated quoted expression in argument list".to_string());
    }
    Ok(output)
}

fn find_matching_bracket(input: &str, open_bracket: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    if open_bracket >= bytes.len() || bytes[open_bracket] != b'[' {
        return None;
    }
    let mut depth = 1usize;
    let mut i = open_bracket + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = skip_single_quoted(input, i).unwrap_or(bytes.len());
            }
            b'"' => {
                i = skip_double_quoted(input, i).unwrap_or(bytes.len());
            }
            b'[' => {
                depth += 1;
                i += 1;
            }
            b']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

fn skip_single_quoted(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

fn skip_double_quoted(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

fn latest_snapshot_rewrite_sql(args: &LatestSnapshotArgs) -> Result<String, String> {
    let source = sql_quote_qualified_ident(
        args.source
            .as_ref()
            .ok_or_else(|| "latest_snapshot() missing source".to_string())?,
    );
    if source.is_empty() {
        return Err("latest_snapshot() invalid source".to_string());
    }

    let ts = sql_quote_ident(
        args.ts_column
            .as_ref()
            .ok_or_else(|| "latest_snapshot() missing ts_column".to_string())?,
    );
    if ts.is_empty() {
        return Err("latest_snapshot() invalid ts_column".to_string());
    }

    let alias = sql_quote_ident("_ls_source");
    let mut partition_exprs = Vec::new();
    for part in &args.partition_by {
        partition_exprs.push(format!("{alias}.{}", sql_quote_ident(part)));
    }

    let mut order_exprs = Vec::new();
    order_exprs.extend(partition_exprs.clone());
    let nulls_position = if args.include_null_timestamps && args.nulls_first {
        "NULLS FIRST"
    } else {
        "NULLS LAST"
    };
    order_exprs.push(format!("{alias}.{ts} DESC {nulls_position}"));
    order_exprs.extend(
        args.tie_breakers
            .iter()
            .map(|tb| format!("{alias}.{}", sql_quote_ident(tb))),
    );
    order_exprs.push(format!("to_jsonb({alias})::text"));

    let mut query = String::new();
    query.push('(');
    if partition_exprs.is_empty() {
        query.push_str(&format!("SELECT {alias}.* FROM {source} AS {alias}"));
    } else {
        query.push_str(&format!(
            "SELECT DISTINCT ON ({}) {}.* FROM {} AS {}",
            partition_exprs.join(", "),
            alias,
            source,
            alias
        ));
    }

    if !args.include_null_timestamps {
        query.push_str(&format!(" WHERE {alias}.{ts} IS NOT NULL"));
    }

    query.push_str(&format!(" ORDER BY {}", order_exprs.join(", ")));
    if partition_exprs.is_empty() {
        query.push_str(" LIMIT 1");
    }
    query.push(')');
    Ok(query)
}

fn missing_relation_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?i)relation "([^"]+)" does not exist"#)
            .expect("missing-relation regex should compile")
    })
}

fn missing_from_clause_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?i)missing FROM-clause entry for table "([^"]+)""#)
            .expect("missing-from-clause regex should compile")
    })
}

fn missing_column_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?i)column "([^"]+)" does not exist"#)
            .expect("missing-column regex should compile")
    })
}

fn relation_source_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?is)\b(?:from|join)\s+((?:"[^"]+"|[a-z_][a-z0-9_]*)(?:\s*\.\s*(?:"[^"]+"|[a-z_][a-z0-9_]*))?)"#,
        )
        .expect("relation extraction regex should compile")
    })
}

fn error_message_capture(re: &Regex, text: &str) -> Option<String> {
    re.captures(text)
        .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
}

fn parse_missing_relation_kind(err_message: &str) -> Option<MissingRelationKind> {
    if let Some(alias) = error_message_capture(missing_from_clause_re(), err_message) {
        return Some(MissingRelationKind::MissingFromAlias(alias));
    }
    error_message_capture(missing_relation_re(), err_message)
        .map(MissingRelationKind::MissingRelation)
}

fn parse_missing_column_name(err_message: &str) -> Option<String> {
    error_message_capture(missing_column_re(), err_message)
}

fn parse_identifier_token(token: &str) -> Option<String> {
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    if token.starts_with('"') && token.ends_with('"') && token.len() >= 2 {
        let inner = &token[1..token.len() - 1];
        return Some(inner.replace("\"\"", "\""));
    }
    if token
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && token
            .chars()
            .next()
            .map(|ch| ch.is_ascii_alphabetic() || ch == '_')
            .unwrap_or(false)
    {
        return Some(token.to_ascii_lowercase());
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RelationRef {
    schema: Option<String>,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MissingRelationKind {
    MissingRelation(String),
    MissingFromAlias(String),
}

fn parse_relation_ref(raw: &str) -> Option<RelationRef> {
    let mut parts = raw.split('.');
    let first = parse_identifier_token(parts.next()?.trim())?;
    let second = parts.next().map(str::trim);
    if parts.next().is_some() {
        return None;
    }
    match second {
        Some(name) => Some(RelationRef {
            schema: Some(first),
            name: parse_identifier_token(name)?,
        }),
        None => Some(RelationRef {
            schema: None,
            name: first,
        }),
    }
}

fn extract_relation_refs(sql: &str) -> Vec<RelationRef> {
    let mut refs = Vec::new();
    let mut seen = HashSet::new();
    for caps in relation_source_re().captures_iter(sql) {
        let Some(raw_relation) = caps.get(1).map(|m| m.as_str()) else {
            continue;
        };
        let compact = raw_relation
            .split_whitespace()
            .collect::<Vec<_>>()
            .join("")
            .trim()
            .to_string();
        let Some(relation_ref) = parse_relation_ref(&compact) else {
            continue;
        };
        if seen.insert(relation_ref.clone()) {
            refs.push(relation_ref);
        }
        if refs.len() >= 4 {
            break;
        }
    }
    refs
}

fn row_str<'a>(row: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    row.get(key).and_then(Value::as_str)
}

const METADATA_HINT_QUERY_LIMIT: usize = 32;
const METADATA_HINT_RESULT_LIMIT: usize = 8;

fn metadata_discovery_allowed_for_schema(server: &PostgresMcp, schema: Option<&str>) -> bool {
    if server.metadata_access_denied() {
        return false;
    }
    match schema {
        Some(schema_name) => server.metadata_schema_visible(schema_name),
        None => true,
    }
}

fn metadata_schema_visibility_sql_predicate(
    server: &PostgresMcp,
    schema_column_sql: &str,
) -> Option<String> {
    let normalized_schema_sql = format!("lower({schema_column_sql})");
    let deny_list = server
        .metadata_schema_deny
        .iter()
        .map(|schema| sql_quote_literal(schema))
        .collect::<Vec<_>>();
    let allow_list = server
        .metadata_schema_allow
        .iter()
        .map(|schema| sql_quote_literal(schema))
        .collect::<Vec<_>>();

    match server.metadata_policy_mode {
        crate::config::MetadataPolicyMode::Denied => Some("FALSE".to_string()),
        crate::config::MetadataPolicyMode::Full => {
            if deny_list.is_empty() {
                None
            } else {
                Some(format!(
                    "{normalized_schema_sql} NOT IN ({})",
                    deny_list.join(", ")
                ))
            }
        }
        crate::config::MetadataPolicyMode::Limited => {
            if allow_list.is_empty() {
                return Some("FALSE".to_string());
            }
            let mut predicates = vec![format!(
                "{normalized_schema_sql} IN ({})",
                allow_list.join(", ")
            )];
            if !deny_list.is_empty() {
                predicates.push(format!(
                    "{normalized_schema_sql} NOT IN ({})",
                    deny_list.join(", ")
                ));
            }
            Some(predicates.join(" AND "))
        }
    }
}

async fn resolve_relation_ref(server: &PostgresMcp, relation: &RelationRef) -> Option<RelationRef> {
    if let Some(schema_name) = relation.schema.as_deref() {
        if !metadata_discovery_allowed_for_schema(server, Some(schema_name)) {
            return None;
        }
        return Some(relation.clone());
    }
    if server.metadata_access_denied() {
        return None;
    }
    let sql = format!(
        "SELECT n.nspname AS schema_name, c.relname AS relation_name FROM pg_class c INNER JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','v','m','f','p') AND c.relname = {} ORDER BY CASE WHEN n.nspname = 'public' THEN 0 WHEN n.nspname LIKE 'pg_temp_%' THEN 1 ELSE 2 END, n.nspname LIMIT {METADATA_HINT_QUERY_LIMIT}",
        sql_quote_literal(&relation.name)
    );
    let output = server.db.execute_query_readonly(&sql).await.ok()?;
    let row = output.rows.iter().find(|row| {
        row_str(row, "schema_name").is_some_and(|schema_name| {
            metadata_discovery_allowed_for_schema(server, Some(schema_name))
        })
    })?;
    let schema_name = row_str(row, "schema_name")?.to_string();
    let relation_name = row_str(row, "relation_name")?.to_string();
    Some(RelationRef {
        schema: Some(schema_name),
        name: relation_name,
    })
}

async fn relation_column_preview(server: &PostgresMcp, relation: &RelationRef) -> Option<String> {
    let resolved = resolve_relation_ref(server, relation).await?;
    let schema = resolved.schema.as_ref()?;
    if !metadata_discovery_allowed_for_schema(server, Some(schema)) {
        return None;
    }
    let sql = format!(
        "SELECT a.attname AS column_name FROM pg_attribute a INNER JOIN pg_class c ON c.oid = a.attrelid INNER JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = {} AND c.relname = {} AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum LIMIT 12",
        sql_quote_literal(schema),
        sql_quote_literal(&resolved.name)
    );
    let output = server.db.execute_query_readonly(&sql).await.ok()?;
    let columns = output
        .rows
        .iter()
        .filter_map(|row| row_str(row, "column_name"))
        .map(str::to_string)
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return None;
    }
    Some(format!(
        "{}.{} [{}]",
        schema,
        resolved.name,
        columns.join(", ")
    ))
}

async fn relation_name_suggestions(server: &PostgresMcp, relation_name: &str) -> Vec<String> {
    if server.metadata_access_denied() {
        return Vec::new();
    }
    let base_name = relation_name
        .rsplit('.')
        .next()
        .unwrap_or(relation_name)
        .trim_matches('"')
        .to_ascii_lowercase();
    if base_name.is_empty() {
        return Vec::new();
    }
    let escaped = escape_like_pattern(&base_name);
    let prefix = format!("{escaped}%");
    let contains = format!("%{escaped}%");
    let prefix_predicate = ilike_literal_predicate("c.relname", &prefix);
    let contains_predicate = ilike_literal_predicate("c.relname", &contains);
    let visibility_clause = metadata_schema_visibility_sql_predicate(server, "n.nspname")
        .map(|predicate| format!(" AND ({predicate})"))
        .unwrap_or_default();
    let sql = format!(
        "SELECT n.nspname AS schema_name, format('%I.%I', n.nspname, c.relname) AS relation FROM pg_class c INNER JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','v','m','f','p') AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND ({prefix_predicate} OR {contains_predicate}){visibility_clause} ORDER BY CASE WHEN {prefix_predicate} THEN 0 ELSE 1 END, abs(length(c.relname) - length({})), n.nspname, c.relname LIMIT {METADATA_HINT_QUERY_LIMIT}",
        sql_quote_literal(&base_name),
    );
    let Ok(output) = server.db.execute_query_readonly(&sql).await else {
        return Vec::new();
    };
    output
        .rows
        .into_iter()
        .filter_map(|row| {
            let schema_name = row.get("schema_name").and_then(Value::as_str)?;
            if !metadata_discovery_allowed_for_schema(server, Some(schema_name)) {
                return None;
            }
            row.get("relation")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .take(METADATA_HINT_RESULT_LIMIT)
        .collect()
}

async fn column_name_suggestions(server: &PostgresMcp, column_name: &str) -> Vec<String> {
    if server.metadata_access_denied() {
        return Vec::new();
    }
    let base_name = column_name.trim_matches('"').to_ascii_lowercase();
    if base_name.is_empty() {
        return Vec::new();
    }
    let escaped = escape_like_pattern(&base_name);
    let prefix = format!("{escaped}%");
    let contains = format!("%{escaped}%");
    let prefix_predicate = ilike_literal_predicate("a.attname", &prefix);
    let contains_predicate = ilike_literal_predicate("a.attname", &contains);
    let visibility_clause = metadata_schema_visibility_sql_predicate(server, "n.nspname")
        .map(|predicate| format!(" AND ({predicate})"))
        .unwrap_or_default();
    let sql = format!(
        "SELECT n.nspname AS schema_name, format('%I.%I.%I', n.nspname, c.relname, a.attname) AS column_ref FROM pg_attribute a INNER JOIN pg_class c ON c.oid = a.attrelid INNER JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','v','m','f','p') AND a.attnum > 0 AND NOT a.attisdropped AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND ({prefix_predicate} OR {contains_predicate}){visibility_clause} ORDER BY CASE WHEN {prefix_predicate} THEN 0 ELSE 1 END, abs(length(a.attname) - length({})), n.nspname, c.relname LIMIT {METADATA_HINT_QUERY_LIMIT}",
        sql_quote_literal(&base_name),
    );
    let Ok(output) = server.db.execute_query_readonly(&sql).await else {
        return Vec::new();
    };
    output
        .rows
        .into_iter()
        .filter_map(|row| {
            let schema_name = row.get("schema_name").and_then(Value::as_str)?;
            if !metadata_discovery_allowed_for_schema(server, Some(schema_name)) {
                return None;
            }
            row.get("column_ref")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .take(METADATA_HINT_RESULT_LIMIT)
        .collect()
}

fn join_limited(items: &[String], limit: usize) -> String {
    items
        .iter()
        .take(limit)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ")
}

fn discovery_schema_for_sql(sql: &str, missing_relation_name: &str) -> String {
    if let Some(missing_relation_ref) = parse_relation_ref(missing_relation_name)
        && let Some(schema) = missing_relation_ref.schema
    {
        return schema;
    }
    for relation_ref in extract_relation_refs(sql) {
        if let Some(schema) = relation_ref.schema {
            return schema;
        }
    }
    "public".to_string()
}

fn discovery_name_like_for_relation(missing_relation_name: &str) -> String {
    parse_relation_ref(missing_relation_name)
        .map(|relation| relation.name)
        .unwrap_or_else(|| {
            missing_relation_name
                .trim_matches('"')
                .rsplit('.')
                .next()
                .unwrap_or(missing_relation_name)
                .to_string()
        })
}

async fn column_name_suggestions_for_relations(
    server: &PostgresMcp,
    column_name: &str,
    relations: &[RelationRef],
) -> Vec<String> {
    if server.metadata_access_denied() {
        return Vec::new();
    }
    if relations.is_empty() {
        return Vec::new();
    }
    let base_name = column_name.trim_matches('"').to_ascii_lowercase();
    if base_name.is_empty() {
        return Vec::new();
    }

    let mut resolved_relations = Vec::new();
    let mut seen = HashSet::new();
    for relation in relations.iter().take(4) {
        let Some(resolved) = resolve_relation_ref(server, relation).await else {
            continue;
        };
        if resolved.schema.is_none() || !seen.insert(resolved.clone()) {
            continue;
        }
        resolved_relations.push(resolved);
    }
    if resolved_relations.is_empty() {
        return Vec::new();
    }

    let relation_scope = resolved_relations
        .iter()
        .filter_map(|relation| {
            relation.schema.as_ref().map(|schema| {
                format!(
                    "(n.nspname = {} AND c.relname = {})",
                    sql_quote_literal(schema),
                    sql_quote_literal(&relation.name)
                )
            })
        })
        .collect::<Vec<_>>();
    if relation_scope.is_empty() {
        return Vec::new();
    }

    let escaped = escape_like_pattern(&base_name);
    let prefix = format!("{escaped}%");
    let contains = format!("%{escaped}%");
    let prefix_predicate = ilike_literal_predicate("a.attname", &prefix);
    let contains_predicate = ilike_literal_predicate("a.attname", &contains);
    let sql = format!(
        "SELECT n.nspname AS schema_name, format('%I.%I.%I', n.nspname, c.relname, a.attname) AS column_ref FROM pg_attribute a INNER JOIN pg_class c ON c.oid = a.attrelid INNER JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','v','m','f','p') AND a.attnum > 0 AND NOT a.attisdropped AND ({}) AND ({prefix_predicate} OR {contains_predicate}) ORDER BY CASE WHEN {prefix_predicate} THEN 0 ELSE 1 END, abs(length(a.attname) - length({})), n.nspname, c.relname LIMIT {METADATA_HINT_QUERY_LIMIT}",
        relation_scope.join(" OR "),
        sql_quote_literal(&base_name),
    );
    let Ok(output) = server.db.execute_query_readonly(&sql).await else {
        return Vec::new();
    };
    output
        .rows
        .into_iter()
        .filter_map(|row| {
            let schema_name = row.get("schema_name").and_then(Value::as_str)?;
            if !metadata_discovery_allowed_for_schema(server, Some(schema_name)) {
                return None;
            }
            row.get("column_ref")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .take(METADATA_HINT_RESULT_LIMIT)
        .collect()
}

pub(crate) async fn derive_execute_sql_hint(
    server: &PostgresMcp,
    sql: &str,
    context: &str,
    err: &crate::db::DbError,
) -> Option<String> {
    if err.code() == "DB_QUERY_TIMEOUT" {
        return Some(statement_timeout_guidance_for_context(context));
    }
    match err.sqlstate() {
        Some("57014") => {
            let message = err.message().to_ascii_lowercase();
            if message.contains("statement timeout") {
                return Some(statement_timeout_guidance_for_context(context));
            }
            None
        }
        Some("42P01") => match parse_missing_relation_kind(err.message())? {
            MissingRelationKind::MissingRelation(missing_relation) => {
                let mut parts = Vec::new();
                if !server.metadata_access_denied() {
                    let relation_suggestions =
                        relation_name_suggestions(server, &missing_relation).await;
                    if !relation_suggestions.is_empty() {
                        parts.push(format!(
                            "Similar relations: {}.",
                            join_limited(&relation_suggestions, 6)
                        ));
                    }
                    let discovery_schema = discovery_schema_for_sql(sql, &missing_relation);
                    if metadata_discovery_allowed_for_schema(server, Some(&discovery_schema)) {
                        let discovery_payload = json!({
                            "schema_name": discovery_schema,
                            "object_type": "table",
                            "name_like": discovery_name_like_for_relation(&missing_relation),
                            "include_columns": true,
                        });
                        parts.push(format!(
                                "Quick discovery: call list_objects with {} (switch object_type to \"view\" when needed).",
                                discovery_payload
                            ));
                    }
                }
                if parts.is_empty() {
                    parts.push(
                        "Relation could not be resolved. Verify schema qualification and relation spelling."
                            .to_string(),
                    );
                }
                Some(parts.join(" "))
            }
            MissingRelationKind::MissingFromAlias(alias) => {
                let mut parts = vec![format!(
                    "Alias \"{}\" is referenced but not present in FROM/JOIN scope. Check JOIN predicates for alias typos or missing alias declarations.",
                    alias
                )];
                let relation_refs = extract_relation_refs(sql);
                if !relation_refs.is_empty() {
                    let relation_list = relation_refs
                        .iter()
                        .map(|relation| match &relation.schema {
                            Some(schema) => format!("{schema}.{}", relation.name),
                            None => relation.name.clone(),
                        })
                        .collect::<Vec<_>>();
                    parts.push(format!(
                        "Observed FROM/JOIN relations: {}.",
                        join_limited(&relation_list, 6)
                    ));
                }
                if !server.metadata_access_denied() {
                    let discovery_schema = discovery_schema_for_sql(sql, "");
                    if metadata_discovery_allowed_for_schema(server, Some(&discovery_schema)) {
                        let discovery_payload = json!({
                            "schema_name": discovery_schema,
                            "object_type": "table",
                            "include_columns": true,
                        });
                        parts.push(format!(
                                "Quick discovery: call list_objects with {} and confirm each alias maps to an introduced relation.",
                                discovery_payload
                            ));
                    }
                }
                Some(parts.join(" "))
            }
        },
        Some("42703") => {
            let missing_column = parse_missing_column_name(err.message())?;
            let mut parts = Vec::new();
            let relation_refs = extract_relation_refs(sql);
            if !server.metadata_access_denied() {
                let mut relation_previews = Vec::new();
                for relation in &relation_refs {
                    if let Some(preview) = relation_column_preview(server, relation).await {
                        relation_previews.push(preview);
                    }
                }
                if !relation_previews.is_empty() {
                    parts.push(format!(
                        "Referenced relation columns: {}.",
                        join_limited(&relation_previews, 3)
                    ));
                }
                let mut column_suggestions =
                    column_name_suggestions_for_relations(server, &missing_column, &relation_refs)
                        .await;
                if column_suggestions.is_empty() {
                    column_suggestions = column_name_suggestions(server, &missing_column).await;
                }
                if !column_suggestions.is_empty() {
                    parts.push(format!(
                        "Similar columns: {}.",
                        join_limited(&column_suggestions, 8)
                    ));
                }
            }
            if parts.is_empty() {
                if server.metadata_access_denied() {
                    return Some(
                        "Review projected columns and alias names before retrying (metadata discovery hints are disabled by policy)."
                            .to_string(),
                    );
                }
                return Some(
                    "Try list_objects with include_columns=true to inspect columns before re-running the query."
                        .to_string(),
                );
            }
            parts.push(
                "Tip: list_objects include_columns=true gives a one-call relation+column preview."
                    .to_string(),
            );
            Some(parts.join(" "))
        }
        _ => None,
    }
}

pub(crate) async fn derive_execute_sql_schema_hints(
    server: &PostgresMcp,
    sql: &str,
    err: &crate::db::DbError,
) -> Option<Value> {
    match err.sqlstate() {
        Some("42P01") => match parse_missing_relation_kind(err.message())? {
            MissingRelationKind::MissingRelation(missing_relation) => {
                let relation_suggestions = if server.metadata_access_denied() {
                    Vec::new()
                } else {
                    relation_name_suggestions(server, &missing_relation).await
                };
                let discovery_schema = discovery_schema_for_sql(sql, &missing_relation);
                let discovery =
                    if metadata_discovery_allowed_for_schema(server, Some(&discovery_schema)) {
                        Some(json!({
                            "tool": "list_objects",
                            "arguments": {
                                "schema_name": discovery_schema,
                                "object_type": "table",
                                "name_like": discovery_name_like_for_relation(&missing_relation),
                                "include_columns": true,
                            }
                        }))
                    } else {
                        None
                    };
                Some(json!({
                    "kind": "missing_relation",
                    "missing_relation": missing_relation,
                    "similar_relations": relation_suggestions,
                    "discovery": discovery,
                }))
            }
            MissingRelationKind::MissingFromAlias(alias) => {
                let referenced_relations = extract_relation_refs(sql)
                    .into_iter()
                    .map(|relation| match relation.schema {
                        Some(schema) => format!("{schema}.{}", relation.name),
                        None => relation.name,
                    })
                    .collect::<Vec<_>>();
                Some(json!({
                    "kind": "missing_from_alias",
                    "missing_alias": alias,
                    "referenced_relations": referenced_relations,
                }))
            }
        },
        Some("42703") => {
            let missing_column = parse_missing_column_name(err.message())?;
            let relation_refs = extract_relation_refs(sql);
            let relation_columns = if server.metadata_access_denied() {
                Vec::new()
            } else {
                let mut previews = Vec::new();
                for relation in &relation_refs {
                    if let Some(preview) = relation_column_preview(server, relation).await {
                        previews.push(preview);
                    }
                }
                previews
            };
            let similar_columns = if server.metadata_access_denied() {
                Vec::new()
            } else {
                let mut suggestions =
                    column_name_suggestions_for_relations(server, &missing_column, &relation_refs)
                        .await;
                if suggestions.is_empty() {
                    suggestions = column_name_suggestions(server, &missing_column).await;
                }
                suggestions
            };
            Some(json!({
                "kind": "missing_column",
                "missing_column": missing_column,
                "similar_columns": similar_columns,
                "relation_columns": relation_columns,
                "metadata_policy": if server.metadata_access_denied() { "denied" } else { "available" },
            }))
        }
        _ => None,
    }
}

fn resolve_execute_sql_statement_timeout_override(
    raw_timeout_ms: Option<u64>,
) -> Result<Option<std::time::Duration>, String> {
    let Some(timeout_ms) = raw_timeout_ms else {
        return Ok(None);
    };

    if timeout_ms == 0 {
        return Err("statement_timeout_ms must be greater than 0".to_string());
    }

    if timeout_ms > EXECUTE_SQL_STATEMENT_TIMEOUT_OVERRIDE_MAX_MS {
        return Err(format!(
            "statement_timeout_ms exceeds maximum allowed value ({}ms)",
            EXECUTE_SQL_STATEMENT_TIMEOUT_OVERRIDE_MAX_MS
        ));
    }

    Ok(Some(std::time::Duration::from_millis(timeout_ms)))
}

fn normalize_query_status_wait_ms(wait_ms: Option<u64>) -> Result<Option<u64>, String> {
    let Some(wait_ms) = wait_ms else {
        return Ok(None);
    };
    if wait_ms == 0 {
        return Err(
            "wait_ms must be >= 1; omit wait_ms to use the default wait behavior".to_string(),
        );
    }
    if wait_ms > QUERY_STATUS_WAIT_MS_MAX {
        return Err(format!("wait_ms must be <= {QUERY_STATUS_WAIT_MS_MAX}"));
    }
    Ok(Some(wait_ms))
}

fn parse_query_status_wait_mode(
    wait_ms: Option<u64>,
    wait_until_terminal: bool,
) -> Result<QueryStatusWaitMode, String> {
    let normalized_wait_ms = normalize_query_status_wait_ms(wait_ms)?;
    if wait_until_terminal {
        if normalized_wait_ms.is_some() {
            return Err("wait_ms cannot be combined with wait_until_terminal=true".to_string());
        }
        return Ok(QueryStatusWaitMode::UntilTerminal);
    }
    Ok(match normalized_wait_ms {
        Some(wait_ms) => QueryStatusWaitMode::Deadline { wait_ms },
        None => QueryStatusWaitMode::Immediate,
    })
}

fn current_unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn validate_query_export_hash(query_hash: &str) -> Result<(), String> {
    if query_hash.len() == 16 && query_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err("query hash is not a valid export path component".to_string())
    }
}

fn query_export_temp_root() -> std::path::PathBuf {
    #[cfg(unix)]
    {
        std::path::PathBuf::from("/tmp")
    }
    #[cfg(not(unix))]
    {
        std::env::temp_dir()
    }
}

fn query_job_suggested_wait_ms(snapshot: &crate::server::QueryJobSnapshot) -> Option<u64> {
    if snapshot.state.is_terminal() {
        return None;
    }
    Some(match snapshot.state {
        crate::server::QueryJobState::Pending => 250,
        crate::server::QueryJobState::Running => 1_000,
        crate::server::QueryJobState::Succeeded
        | crate::server::QueryJobState::Failed
        | crate::server::QueryJobState::Canceled => return None,
    })
}

fn query_job_payload(
    snapshot: &crate::server::QueryJobSnapshot,
    wait_mode: QueryStatusWaitMode,
    wait_trigger: &str,
    wait_elapsed_ms: u64,
    include_response: bool,
) -> Value {
    let now_unix_ms = current_unix_time_ms();
    let age_ms = now_unix_ms.saturating_sub(snapshot.created_at_unix_ms);
    let queue_ms = snapshot
        .started_at_unix_ms
        .unwrap_or(now_unix_ms)
        .saturating_sub(snapshot.created_at_unix_ms);
    let run_ms = snapshot
        .started_at_unix_ms
        .map(|started_at| {
            snapshot
                .finished_at_unix_ms
                .unwrap_or(now_unix_ms)
                .saturating_sub(started_at)
        })
        .unwrap_or(0);
    let suggested_wait_ms = query_job_suggested_wait_ms(snapshot);
    let mut payload = json!({
        "job_id": snapshot.job_id,
        "query_hash": snapshot.query_hash,
        "state": snapshot.state.as_str(),
        "terminal": snapshot.state.is_terminal(),
        "cancel_requested": snapshot.cancel_requested,
        "created_at_unix_ms": snapshot.created_at_unix_ms,
        "started_at_unix_ms": snapshot.started_at_unix_ms,
        "finished_at_unix_ms": snapshot.finished_at_unix_ms,
        "wait": {
            "mode": wait_mode.as_str(),
            "trigger": wait_trigger,
            "elapsed_ms": wait_elapsed_ms,
            "suggested_wait_ms": suggested_wait_ms,
        },
        "progress": {
            "kind": "lifecycle",
            "phase": snapshot.state.as_str(),
            "age_ms": age_ms,
            "queue_ms": queue_ms,
            "run_ms": run_ms,
            "suggested_wait_ms": suggested_wait_ms,
        },
        "follow_up": {
            "tool": if snapshot.state.is_terminal() { Value::Null } else { json!("query_status") },
            "suggested_wait_ms": suggested_wait_ms,
        },
    });
    if include_response && let Some(payload_obj) = payload.as_object_mut() {
        payload_obj.insert(
            "response".to_string(),
            snapshot.response.clone().unwrap_or(Value::Null),
        );
    }
    payload
}

fn query_job_payload_legacy_error_code(code: &str) -> bool {
    let code = code.trim();
    if code.is_empty() {
        return false;
    }
    let mut has_underscore = false;
    for c in code.chars() {
        if c == '_' {
            has_underscore = true;
            continue;
        }
        if c.is_ascii_uppercase() || c.is_ascii_digit() {
            continue;
        }
        return false;
    }
    has_underscore
}

fn query_job_payload_legacy_error_code_matches_sqlstate(payload: &Value, code: &str) -> bool {
    if code.len() != 5 {
        return false;
    }
    if !code
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
    {
        return false;
    }
    payload
        .get("sqlstate")
        .and_then(Value::as_str)
        .is_some_and(|sqlstate| sqlstate.eq_ignore_ascii_case(code))
}

fn query_job_payload_legacy_error_reason(reason: &str) -> bool {
    let reason = reason.trim();
    if reason.is_empty() {
        return false;
    }
    let mut has_underscore = false;
    for c in reason.chars() {
        if c == '_' {
            has_underscore = true;
            continue;
        }
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            continue;
        }
        return false;
    }
    has_underscore
}

fn query_job_payload_legacy_error_signature(payload: &Value) -> bool {
    let Some(code) = payload.get("code").and_then(Value::as_str) else {
        return false;
    };
    let Some(reason) = payload.get("reason").and_then(Value::as_str) else {
        return false;
    };
    let code_matches = query_job_payload_legacy_error_code(code)
        || query_job_payload_legacy_error_code_matches_sqlstate(payload, code);
    if !code_matches || !query_job_payload_legacy_error_reason(reason) {
        return false;
    }

    let Some(error) = payload.get("error") else {
        return false;
    };

    if let Some(error_message) = error.as_str() {
        return !error_message.trim().is_empty();
    }

    error
        .get("message")
        .and_then(Value::as_str)
        .is_some_and(|message| !message.trim().is_empty())
}

fn query_job_payload_failed(payload: &Value) -> bool {
    if payload.get("ok").and_then(Value::as_bool) == Some(false) {
        return true;
    }
    if payload.get("ok").is_some() {
        return false;
    }

    if query_job_payload_legacy_error_signature(payload) {
        return true;
    }

    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecuteSqlResolvedCountMode {
    None,
    Exact,
    Estimated,
    Async,
}

impl ExecuteSqlResolvedCountMode {
    const fn from_requested(requested: ExecuteSqlCountMode) -> Self {
        match requested {
            ExecuteSqlCountMode::None => Self::None,
            ExecuteSqlCountMode::Exact => Self::Exact,
            ExecuteSqlCountMode::Estimated => Self::Estimated,
            ExecuteSqlCountMode::Async => Self::Async,
        }
    }

    const fn row_count_mode(self) -> &'static str {
        match self {
            Self::None => "page_window",
            Self::Exact => "count_exact",
            Self::Estimated => "count_estimated",
            Self::Async => "count_async",
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Exact => "exact",
            Self::Estimated => "estimated",
            Self::Async => "async",
        }
    }

    const fn uses_exact_count_query(self) -> bool {
        matches!(self, Self::Exact)
    }
}

fn resolve_execute_sql_count_mode(
    count_mode: Option<ExecuteSqlCountMode>,
    include_total_row_count: Option<bool>,
) -> (ExecuteSqlResolvedCountMode, Vec<String>) {
    let mut query_hints = Vec::new();

    if let Some(requested_mode) = count_mode {
        let resolved = ExecuteSqlResolvedCountMode::from_requested(requested_mode);
        if let Some(legacy_flag) = include_total_row_count {
            let legacy_resolved = if legacy_flag {
                ExecuteSqlResolvedCountMode::Exact
            } else {
                ExecuteSqlResolvedCountMode::None
            };
            if legacy_resolved != resolved {
                query_hints.push(format!(
                    "count_mode={} overrides include_total_row_count={legacy_flag} for this call",
                    requested_mode.as_str()
                ));
            }
        }
        return (resolved, query_hints);
    }

    if include_total_row_count == Some(true) {
        query_hints
            .push("legacy include_total_row_count=true mapped to count_mode=exact".to_string());
        return (ExecuteSqlResolvedCountMode::Exact, query_hints);
    }

    (ExecuteSqlResolvedCountMode::None, query_hints)
}

#[derive(Debug, Clone)]
struct ExecuteSqlProfileResolution {
    effective_profile: Option<ExecuteSqlProfile>,
    output_mode: ResponseOutputMode,
    page_size: usize,
    max_cell_chars: Option<usize>,
    statement_timeout_ms: Option<u64>,
    preflight_check: bool,
    requested_count_mode: Option<ExecuteSqlCountMode>,
    metadata_verbosity: ExecuteSqlMetadataVerbosity,
    profile_hints: Vec<String>,
}

fn normalize_execute_sql_output_preferences(
    requested_output_mode: Option<ResponseOutputMode>,
    response_formatting_mode: Option<ResponseFormattingMode>,
) -> (
    Option<ResponseOutputMode>,
    Option<ResponseFormattingMode>,
    Option<&'static str>,
    bool,
) {
    if requested_output_mode.is_none()
        && response_formatting_mode == Some(ResponseFormattingMode::Markdown)
    {
        (
            Some(ResponseOutputMode::Rows),
            None,
            Some("response_formatting_mode=markdown normalized to output_mode=table"),
            true,
        )
    } else {
        (requested_output_mode, response_formatting_mode, None, false)
    }
}

fn resolve_execute_sql_profile(
    requested_profile: Option<ExecuteSqlProfile>,
    requested_output_mode: Option<ResponseOutputMode>,
    requested_max_rows: Option<usize>,
    requested_max_cell_chars: Option<usize>,
    requested_count_mode: Option<ExecuteSqlCountMode>,
    requested_metadata_verbosity: Option<ExecuteSqlMetadataVerbosity>,
    requested_statement_timeout_ms: Option<u64>,
    requested_preflight_check: Option<bool>,
    requested_include_total_row_count: Option<bool>,
    default_output_mode: ResponseOutputMode,
    default_page_size: usize,
) -> ExecuteSqlProfileResolution {
    let mut output_mode = requested_output_mode.unwrap_or(default_output_mode);
    let mut page_size = resolve_execute_sql_page_size(default_page_size, requested_max_rows);
    let mut max_cell_chars = requested_max_cell_chars.filter(|value| *value > 0);
    let mut requested_count_mode = requested_count_mode;
    let mut metadata_verbosity =
        resolve_execute_sql_metadata_verbosity(requested_metadata_verbosity);
    let mut statement_timeout_ms = requested_statement_timeout_ms;
    let mut preflight_check = requested_preflight_check.unwrap_or(false);
    let mut profile_hints = Vec::new();

    if let Some(profile) = requested_profile {
        match profile {
            ExecuteSqlProfile::FastAgent => {
                if requested_output_mode.is_none() {
                    output_mode = ResponseOutputMode::DataOnly;
                    profile_hints.push(
                        "profile=fast_agent applied default output_mode=data_only".to_string(),
                    );
                }
                if requested_max_rows.is_none() {
                    let bounded = page_size.min(PROFILE_FAST_AGENT_PAGE_SIZE_CAP);
                    if bounded != page_size {
                        page_size = bounded;
                    }
                    profile_hints.push(format!(
                        "profile=fast_agent applied default max_rows={page_size}"
                    ));
                }
                if requested_max_cell_chars.is_none() {
                    max_cell_chars = Some(PROFILE_FAST_AGENT_MAX_CELL_CHARS);
                    profile_hints.push(format!(
                        "profile=fast_agent applied default max_cell_chars={PROFILE_FAST_AGENT_MAX_CELL_CHARS}"
                    ));
                }
            }
            ExecuteSqlProfile::HumanDebug => {
                if requested_output_mode.is_none() {
                    output_mode = ResponseOutputMode::RowsSafe;
                    profile_hints.push(
                        "profile=human_debug applied default output_mode=rows_safe".to_string(),
                    );
                }
                if requested_metadata_verbosity.is_none() {
                    metadata_verbosity = ExecuteSqlMetadataVerbosity::Full;
                    profile_hints.push(
                        "profile=human_debug applied default metadata_verbosity=full".to_string(),
                    );
                }
                if requested_count_mode.is_none() && requested_include_total_row_count.is_none() {
                    requested_count_mode = Some(ExecuteSqlCountMode::Estimated);
                    profile_hints.push(
                        "profile=human_debug applied default count_mode=estimated".to_string(),
                    );
                }
                if requested_preflight_check.is_none() {
                    preflight_check = true;
                    profile_hints.push(
                        "profile=human_debug applied default preflight_check=true".to_string(),
                    );
                }
            }
            ExecuteSqlProfile::HeavyView => {
                if requested_output_mode.is_none() {
                    output_mode = ResponseOutputMode::Tuples;
                    profile_hints
                        .push("profile=heavy_view applied default output_mode=tuples".to_string());
                }
                if requested_metadata_verbosity.is_none() {
                    metadata_verbosity = ExecuteSqlMetadataVerbosity::Standard;
                    profile_hints.push(
                        "profile=heavy_view applied default metadata_verbosity=standard"
                            .to_string(),
                    );
                }
                if requested_statement_timeout_ms.is_none() {
                    statement_timeout_ms = Some(PROFILE_HEAVY_VIEW_STATEMENT_TIMEOUT_MS);
                    profile_hints.push(format!(
                        "profile=heavy_view applied default statement_timeout_ms={PROFILE_HEAVY_VIEW_STATEMENT_TIMEOUT_MS}"
                    ));
                }
                if requested_preflight_check.is_none() {
                    preflight_check = true;
                    profile_hints.push(
                        "profile=heavy_view applied default preflight_check=true".to_string(),
                    );
                }
            }
        }
    }

    ExecuteSqlProfileResolution {
        effective_profile: requested_profile,
        output_mode,
        page_size,
        max_cell_chars,
        statement_timeout_ms,
        preflight_check,
        requested_count_mode,
        metadata_verbosity,
        profile_hints,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecuteSqlRequestScope {
    ExecuteSql,
    QueryStart,
}

impl ExecuteSqlRequestScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExecuteSql => "execute_sql",
            Self::QueryStart => "query_start",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedExecuteSqlSql {
    raw: String,
    rewritten: String,
    helper_count: usize,
}

impl NormalizedExecuteSqlSql {
    fn statement_kind(&self) -> String {
        leading_statement_keyword(&self.rewritten).unwrap_or_else(|| "unknown".to_string())
    }

    fn rewritten_from_input(&self) -> bool {
        self.raw != self.rewritten
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedExecuteSqlCore {
    sql: NormalizedExecuteSqlSql,
    params: Vec<Value>,
    cursor: Option<String>,
    describe_only: bool,
    export_to_file: bool,
    statement_timeout_override: Option<std::time::Duration>,
    query_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EmptyExecuteSqlCore {
    param_count: usize,
    statement_timeout_override: Option<std::time::Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExecuteSqlNormalizationStage {
    Empty(EmptyExecuteSqlCore),
    Ready(NormalizedExecuteSqlCore),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecuteSqlPaginationStrategy {
    None,
    Offset,
}

impl ExecuteSqlPaginationStrategy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Offset => "offset",
        }
    }

    const fn supports_cursor(self) -> bool {
        matches!(self, Self::Offset)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NormalizedExecuteSqlPagination {
    strategy: ExecuteSqlPaginationStrategy,
    offset: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecuteSqlCountExecution {
    None,
    PageWindow,
    InlineExact,
    EstimatedPlan,
    BackgroundQuery,
}

impl ExecuteSqlCountExecution {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::PageWindow => "page_window",
            Self::InlineExact => "inline_exact",
            Self::EstimatedPlan => "estimated_plan",
            Self::BackgroundQuery => "background_query",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExecuteSqlExecutionFacts {
    scope: ExecuteSqlRequestScope,
    statement_kind: String,
    sql_rewritten: bool,
    helper_expansions: usize,
    bound_param_count: usize,
    statement_timeout_override_applied: bool,
    pagination_strategy: ExecuteSqlPaginationStrategy,
    count_execution: Option<ExecuteSqlCountExecution>,
}

impl ExecuteSqlExecutionFacts {
    fn from_core(
        scope: ExecuteSqlRequestScope,
        core: &NormalizedExecuteSqlCore,
        pagination: NormalizedExecuteSqlPagination,
        count_execution: Option<ExecuteSqlCountExecution>,
    ) -> Self {
        Self {
            scope,
            statement_kind: core.sql.statement_kind(),
            sql_rewritten: core.sql.rewritten_from_input(),
            helper_expansions: core.sql.helper_count,
            bound_param_count: core.params.len(),
            statement_timeout_override_applied: core.statement_timeout_override.is_some(),
            pagination_strategy: pagination.strategy,
            count_execution,
        }
    }

    fn empty(
        scope: ExecuteSqlRequestScope,
        param_count: usize,
        statement_timeout_override: Option<std::time::Duration>,
    ) -> Self {
        Self {
            scope,
            statement_kind: "empty".to_string(),
            sql_rewritten: false,
            helper_expansions: 0,
            bound_param_count: param_count,
            statement_timeout_override_applied: statement_timeout_override.is_some(),
            pagination_strategy: ExecuteSqlPaginationStrategy::None,
            count_execution: Some(ExecuteSqlCountExecution::None),
        }
    }

    fn as_meta_value(&self) -> Value {
        let cursor_binding = if self.pagination_strategy.supports_cursor() {
            Some("sql_plus_params")
        } else {
            None
        };
        json!({
            "contract_version": "execution/v1",
            "scope": self.scope.as_str(),
            "sql": {
                "statement_kind": self.statement_kind,
                "rewritten": self.sql_rewritten,
                "helper_expansions": self.helper_expansions,
            },
            "params": {
                "bound_count": self.bound_param_count,
            },
            "timeout": {
                "override_applied": self.statement_timeout_override_applied,
            },
            "pagination": {
                "supported": self.pagination_strategy.supports_cursor(),
                "strategy": self.pagination_strategy.as_str(),
                "cursor_binding": cursor_binding,
            },
            "count": self.count_execution.map(|execution| {
                json!({
                    "execution": execution.as_str(),
                })
            }),
        })
    }
}

fn normalize_execute_sql_core(
    sql: Option<String>,
    params: Option<Vec<Value>>,
    cursor: Option<String>,
    describe_only: bool,
    export_to_file: bool,
    statement_timeout_override: Option<std::time::Duration>,
) -> Result<ExecuteSqlNormalizationStage, String> {
    let params = params.unwrap_or_default();
    let raw_sql = sql.unwrap_or_default().trim().to_string();
    if raw_sql.is_empty() {
        return Ok(ExecuteSqlNormalizationStage::Empty(EmptyExecuteSqlCore {
            param_count: params.len(),
            statement_timeout_override,
        }));
    }
    validate_sql_size(raw_sql.as_str(), "sql")?;
    let latest_snapshot_rewrite = rewrite_latest_snapshot_helpers(&raw_sql)
        .map_err(|err| format!("invalid latest_snapshot helper: {err}"))?;
    validate_execute_sql_bound_params(&latest_snapshot_rewrite.sql, &params)?;
    validate_execute_sql_describe_export_flags(describe_only, export_to_file)?;
    let query_hash = response_page_hash_for_params(&latest_snapshot_rewrite.sql, &params);

    Ok(ExecuteSqlNormalizationStage::Ready(
        NormalizedExecuteSqlCore {
            sql: NormalizedExecuteSqlSql {
                raw: raw_sql,
                rewritten: latest_snapshot_rewrite.sql,
                helper_count: latest_snapshot_rewrite.helper_count,
            },
            params,
            cursor,
            describe_only,
            export_to_file,
            statement_timeout_override,
            query_hash,
        },
    ))
}

fn normalize_execute_sql_pagination(
    server: &PostgresMcp,
    core: &NormalizedExecuteSqlCore,
    elapsed_ms: u64,
) -> Result<NormalizedExecuteSqlPagination, CallToolResult> {
    let strategy = if should_paginate_execute_sql(&core.sql.rewritten) {
        ExecuteSqlPaginationStrategy::Offset
    } else {
        ExecuteSqlPaginationStrategy::None
    };

    let offset = if let Some(raw_cursor) = core.cursor.as_deref() {
        if !strategy.supports_cursor() {
            return Err(query_response_error(
                "Cursor pagination is only supported for SELECT/VALUES/TABLE query shapes that satisfy active SQL safety policy",
                "INVALID_CURSOR",
                "invalid_cursor",
                elapsed_ms,
                server,
            ));
        }
        match decode_pagination_cursor(
            server,
            PaginationCursorScope::ExecuteSql,
            &core.query_hash,
            raw_cursor,
        ) {
            Ok(decoded) => decoded.offset,
            Err(PaginationCursorDecodeError::QueryMismatch) => {
                return Err(query_response_error(
                    "Cursor does not match query hash",
                    "CURSOR_QUERY_MISMATCH",
                    "invalid_cursor",
                    elapsed_ms,
                    server,
                ));
            }
            Err(PaginationCursorDecodeError::Expired) => {
                return Err(query_response_error(
                    "Pagination cursor expired",
                    "CURSOR_EXPIRED",
                    "invalid_cursor",
                    elapsed_ms,
                    server,
                ));
            }
            Err(PaginationCursorDecodeError::Invalid) => {
                return Err(query_response_error(
                    "Invalid pagination cursor",
                    "INVALID_CURSOR",
                    "invalid_cursor",
                    elapsed_ms,
                    server,
                ));
            }
        }
    } else {
        0
    };

    Ok(NormalizedExecuteSqlPagination { strategy, offset })
}

fn resolve_execute_sql_metadata_verbosity(
    metadata_verbosity: Option<ExecuteSqlMetadataVerbosity>,
) -> ExecuteSqlMetadataVerbosity {
    metadata_verbosity.unwrap_or(ExecuteSqlMetadataVerbosity::Compact)
}

fn apply_execute_sql_metadata_verbosity(
    mut result: CallToolResult,
    verbosity: ExecuteSqlMetadataVerbosity,
) -> CallToolResult {
    if let Some(payload) = result.structured_content.as_mut()
        && let Some(meta) = payload
            .get_mut("meta")
            .and_then(serde_json::Value::as_object_mut)
    {
        meta.insert("metadata_verbosity".to_string(), json!(verbosity.as_str()));
        match verbosity {
            ExecuteSqlMetadataVerbosity::Compact => {
                meta.remove("columns");
                meta.remove("query_hints");
            }
            ExecuteSqlMetadataVerbosity::Standard => {
                meta.remove("columns");
            }
            ExecuteSqlMetadataVerbosity::Full => {}
        }
    }
    result
}

fn apply_execute_sql_count_metadata(
    mut result: CallToolResult,
    row_count_mode: &str,
    row_count_job_id: Option<&str>,
) -> CallToolResult {
    if let Some(payload) = result.structured_content.as_mut()
        && let Some(meta) = payload
            .get_mut("meta")
            .and_then(serde_json::Value::as_object_mut)
    {
        meta.insert("row_count_mode".to_string(), json!(row_count_mode));
        match row_count_job_id {
            Some(job_id) => {
                meta.insert("row_count_job_id".to_string(), json!(job_id));
            }
            None => {
                meta.remove("row_count_job_id");
            }
        }
    }
    result
}

fn apply_execute_sql_export_metadata(
    mut result: CallToolResult,
    export_meta: Option<Value>,
) -> CallToolResult {
    let Some(export_meta) = export_meta else {
        return result;
    };
    if let Some(payload) = result.structured_content.as_mut()
        && let Some(meta) = payload
            .get_mut("meta")
            .and_then(serde_json::Value::as_object_mut)
    {
        meta.insert("export".to_string(), export_meta);
    }
    result
}

fn apply_execute_sql_effective_metadata(
    mut result: CallToolResult,
    effective_profile: Option<ExecuteSqlProfile>,
    effective_count_mode: ExecuteSqlResolvedCountMode,
    metadata_verbosity: ExecuteSqlMetadataVerbosity,
    requested_output_mode: ResponseOutputMode,
    auto_tabular_mode: ResponseOutputMode,
) -> CallToolResult {
    if let Some(payload) = result.structured_content.as_mut()
        && let Some(meta) = payload
            .get_mut("meta")
            .and_then(serde_json::Value::as_object_mut)
    {
        meta.insert(
            "effective_profile".to_string(),
            effective_profile
                .map(|profile| json!(profile.as_str()))
                .unwrap_or(Value::Null),
        );
        meta.insert(
            "effective_count_mode".to_string(),
            json!(effective_count_mode.as_str()),
        );
        meta.insert(
            "effective_metadata_verbosity".to_string(),
            json!(metadata_verbosity.as_str()),
        );
        meta.insert(
            "requested_output_mode".to_string(),
            json!(requested_output_mode.as_str()),
        );
        let output_mode = meta
            .get("output_mode")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(output_mode) = output_mode {
            meta.insert("effective_output_mode".to_string(), json!(output_mode));
            if requested_output_mode == ResponseOutputMode::Auto {
                let reason = if output_mode == ResponseOutputMode::Scalar.as_str() {
                    "single_cell_result"
                } else {
                    "configured_auto_tabular_default"
                };
                meta.insert(
                    "auto_output_resolution".to_string(),
                    json!({
                        "requested": requested_output_mode.as_str(),
                        "resolved": output_mode,
                        "reason": reason,
                        "tabular_default": auto_tabular_mode.as_str(),
                    }),
                );
            }
        }
    }
    result
}

fn apply_execute_sql_execution_metadata(
    mut result: CallToolResult,
    execution_facts: &ExecuteSqlExecutionFacts,
) -> CallToolResult {
    if let Some(payload) = result.structured_content.as_mut()
        && let Some(meta) = payload
            .get_mut("meta")
            .and_then(serde_json::Value::as_object_mut)
    {
        meta.insert("execution".to_string(), execution_facts.as_meta_value());
    }
    result
}

fn apply_execute_sql_query_telemetry(
    mut result: CallToolResult,
    query_hash: &str,
    query_fingerprint: &str,
    metadata_verbosity: ExecuteSqlMetadataVerbosity,
) -> CallToolResult {
    if let Some(payload) = result.structured_content.as_mut()
        && let Some(meta) = payload
            .get_mut("meta")
            .and_then(serde_json::Value::as_object_mut)
    {
        let canonical_query_hash = meta
            .get("query_hash")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or(query_hash);
        let mut telemetry = serde_json::Map::new();
        telemetry.insert("query_hash".to_string(), json!(canonical_query_hash));
        telemetry.insert("query_fingerprint".to_string(), json!(query_fingerprint));

        if let Some(value) = meta.get("elapsed_ms").cloned() {
            telemetry.insert("elapsed_ms".to_string(), value);
        }
        if let Some(value) = meta.get("returned_rows").cloned() {
            telemetry.insert("returned_rows".to_string(), value);
        }
        if metadata_verbosity != ExecuteSqlMetadataVerbosity::Compact {
            if let Some(value) = meta.get("row_count_mode").cloned() {
                telemetry.insert("row_count_mode".to_string(), value);
            }
            if let Some(value) = meta.get("row_count_total").cloned() {
                telemetry.insert("row_count_total".to_string(), value);
            }
            if let Some(value) = meta.get("has_more").cloned() {
                telemetry.insert("has_more".to_string(), value);
            }
            if let Some(value) = meta.get("cursor_offset").cloned() {
                telemetry.insert("cursor_offset".to_string(), value);
            }
            if let Some(value) = meta.get("next_offset").cloned() {
                telemetry.insert("next_offset".to_string(), value);
            }
        }
        meta.insert("query_telemetry".to_string(), Value::Object(telemetry));
    }
    result
}

fn should_preserve_data_only_capabilities(capabilities: &Value) -> bool {
    let Some(object) = capabilities.as_object() else {
        return false;
    };
    let startup_state = object.get("startup_state").and_then(Value::as_str);
    let degraded_read_only = object
        .get("degraded_read_only")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let read_write_sql = object
        .get("read_write_sql")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let metadata_discovery = object
        .get("metadata_discovery")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let reason_present = object.get("reason").is_some_and(|value| !value.is_null());
    let missing_dependencies = object
        .get("missing_dependencies")
        .and_then(Value::as_array)
        .is_some_and(|values| !values.is_empty());

    startup_state.is_some_and(|state| state != "healthy")
        || degraded_read_only
        || !read_write_sql
        || !metadata_discovery
        || reason_present
        || missing_dependencies
}

fn should_preserve_compact_cell_clipping(cell_clipping: &Value) -> bool {
    let Some(object) = cell_clipping.as_object() else {
        return false;
    };
    object
        .get("applied")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn should_preserve_compact_column_name_safety(column_name_safety: &Value) -> bool {
    let Some(object) = column_name_safety.as_object() else {
        return false;
    };
    object
        .get("duplicate_columns_aliased")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn apply_execute_sql_compact_meta_cleanup(
    mut result: CallToolResult,
    metadata_verbosity: ExecuteSqlMetadataVerbosity,
) -> CallToolResult {
    if metadata_verbosity != ExecuteSqlMetadataVerbosity::Compact {
        return result;
    }
    if let Some(payload) = result.structured_content.as_mut()
        && let Some(meta) = payload
            .get_mut("meta")
            .and_then(serde_json::Value::as_object_mut)
    {
        let output_mode = meta.get("output_mode").cloned().unwrap_or(Value::Null);
        let metadata_verbosity_value = meta
            .get("metadata_verbosity")
            .cloned()
            .unwrap_or_else(|| json!(metadata_verbosity.as_str()));
        let query_hash = meta.get("query_hash").cloned().unwrap_or(Value::Null);
        let elapsed_ms = meta.get("elapsed_ms").cloned().unwrap_or(Value::Null);
        let truncated = meta.get("truncated").cloned().unwrap_or(Value::Bool(false));
        let returned_rows = meta.get("returned_rows").cloned().unwrap_or(Value::Null);
        let has_more = meta.get("has_more").cloned().unwrap_or(Value::Bool(false));
        let next_cursor = meta.get("next_cursor").cloned().unwrap_or(Value::Null);
        let summary_only = meta.get("summary_only").cloned();
        let row_count_mode = meta.get("row_count_mode").cloned();
        let row_count_total = meta.get("row_count_total").cloned();
        let row_count_job_id = meta.get("row_count_job_id").cloned();
        let export = meta.get("export").cloned();
        let capabilities = meta.get("capabilities").cloned();
        let cell_clipping = meta.get("cell_clipping").cloned();
        let column_name_safety = meta.get("column_name_safety").cloned();
        let row_count_mode_name = meta
            .get("row_count_mode")
            .and_then(Value::as_str)
            .map(str::to_string);

        meta.clear();
        meta.insert("output_mode".to_string(), output_mode);
        meta.insert("metadata_verbosity".to_string(), metadata_verbosity_value);
        if summary_only
            .as_ref()
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            meta.insert("summary_only".to_string(), json!(true));
        }
        meta.insert("query_hash".to_string(), query_hash);
        meta.insert("elapsed_ms".to_string(), elapsed_ms);
        meta.insert("truncated".to_string(), truncated);
        meta.insert("returned_rows".to_string(), returned_rows);
        meta.insert("has_more".to_string(), has_more);
        meta.insert("next_cursor".to_string(), next_cursor);
        if let Some(value) = row_count_mode {
            meta.insert("row_count_mode".to_string(), value);
        }
        if let Some(value) = row_count_total
            && row_count_mode_name.as_deref() != Some("page_window")
        {
            meta.insert("row_count_total".to_string(), value);
        }
        if let Some(value) = row_count_job_id {
            meta.insert("row_count_job_id".to_string(), value);
        }
        if let Some(value) = export {
            meta.insert("export".to_string(), value);
        }
        if let Some(value) = capabilities
            && should_preserve_data_only_capabilities(&value)
        {
            meta.insert("capabilities".to_string(), value);
        }
        if let Some(value) = cell_clipping
            && should_preserve_compact_cell_clipping(&value)
        {
            meta.insert("cell_clipping".to_string(), value);
        }
        if let Some(value) = column_name_safety
            && should_preserve_compact_column_name_safety(&value)
        {
            meta.insert("column_name_safety".to_string(), value);
        }
    }
    result
}

fn apply_execute_sql_compaction(
    result: CallToolResult,
    metadata_verbosity: ExecuteSqlMetadataVerbosity,
    output_mode: ResponseOutputMode,
) -> CallToolResult {
    let result = apply_execute_sql_compact_meta_cleanup(result, metadata_verbosity);
    apply_execute_sql_data_only_compaction(result, output_mode)
}
fn apply_execute_sql_data_only_compaction(
    mut result: CallToolResult,
    output_mode: ResponseOutputMode,
) -> CallToolResult {
    if output_mode != ResponseOutputMode::DataOnly {
        return result;
    }
    if let Some(payload) = result.structured_content.as_mut()
        && let Some(meta) = payload
            .get_mut("meta")
            .and_then(serde_json::Value::as_object_mut)
    {
        let query_hash = meta.get("query_hash").cloned().unwrap_or(Value::Null);
        let elapsed_ms = meta.get("elapsed_ms").cloned().unwrap_or(Value::Null);
        let truncated = meta.get("truncated").cloned().unwrap_or(Value::Bool(false));
        let returned_rows = meta.get("returned_rows").cloned().unwrap_or(Value::Null);
        let has_more = meta.get("has_more").cloned().unwrap_or(Value::Bool(false));
        let next_cursor = meta.get("next_cursor").cloned().unwrap_or(Value::Null);
        let capabilities = meta.get("capabilities").cloned();
        let row_count_mode = meta.get("row_count_mode").cloned();
        let row_count_total = meta.get("row_count_total").cloned();
        let row_count_job_id = meta.get("row_count_job_id").cloned();
        let export = meta.get("export").cloned();
        meta.clear();
        meta.insert("output_mode".to_string(), json!("data_only"));
        meta.insert("query_hash".to_string(), query_hash);
        meta.insert("elapsed_ms".to_string(), elapsed_ms);
        meta.insert("truncated".to_string(), truncated);
        meta.insert("returned_rows".to_string(), returned_rows);
        meta.insert("has_more".to_string(), has_more);
        meta.insert("next_cursor".to_string(), next_cursor);
        if let Some(value) = row_count_mode {
            meta.insert("row_count_mode".to_string(), value);
        }
        if let Some(value) = row_count_total
            && meta
                .get("row_count_mode")
                .and_then(Value::as_str)
                .is_some_and(|mode| mode != "page_window")
        {
            meta.insert("row_count_total".to_string(), value);
        }
        if let Some(value) = row_count_job_id {
            meta.insert("row_count_job_id".to_string(), value);
        }
        if let Some(value) = export {
            meta.insert("export".to_string(), value);
        }
        if let Some(value) = capabilities
            && should_preserve_data_only_capabilities(&value)
        {
            meta.insert("capabilities".to_string(), value);
        }
    }
    result
}

fn statement_timeout_guidance_for_context(context: &str) -> String {
    let mut guidance = Vec::new();

    if context == "Error counting query rows" {
        guidance.push(
            "Set count_mode=none (or include_total_row_count=false) to skip COUNT(*) when heavy views time out."
                .to_string(),
        );
    }

    guidance.push(format!(
        "Retry with statement_timeout_ms (up to {}ms) for this call when heavier plans are expected.",
        EXECUTE_SQL_STATEMENT_TIMEOUT_OVERRIDE_MAX_MS
    ));
    guidance.push(
        "Use explain_query with analyze=false first to inspect plan cost before increasing timeout further."
            .to_string(),
    );

    guidance.join(" ")
}

fn is_statement_timeout_error(err: &crate::db::DbError) -> bool {
    if err.code() == "DB_QUERY_TIMEOUT" {
        return true;
    }
    if err.sqlstate() == Some("57014") {
        let error = err.message().to_ascii_lowercase();
        let detail = err.detail().unwrap_or_default().to_ascii_lowercase();
        let hint = err.hint().unwrap_or_default().to_ascii_lowercase();
        if error.contains("statement timeout")
            || detail.contains("statement timeout")
            || hint.contains("statement timeout")
        {
            return true;
        }
    }
    false
}

fn timeout_diagnostics_payload(
    query_hash: &str,
    context: &str,
    statement_timeout_override: Option<Duration>,
    count_mode: Option<ExecuteSqlResolvedCountMode>,
) -> Value {
    let statement_timeout_ms = statement_timeout_override.map(|value| value.as_millis() as u64);
    let count_mode = count_mode.map(|mode| mode.row_count_mode());
    let mut recommended_actions = vec![format!(
        "retry with statement_timeout_ms <= {EXECUTE_SQL_STATEMENT_TIMEOUT_OVERRIDE_MAX_MS}"
    )];
    if context.contains("count") {
        recommended_actions.push("retry with count_mode=none or count_mode=estimated".to_string());
    } else {
        recommended_actions.push(
            "for long-running reads, use query_start_and_wait (or query_start + query_status)"
                .to_string(),
        );
    }
    if count_mode == Some("count_async") {
        recommended_actions.push(
            "inspect async count progress with query_status using row_count_job_id".to_string(),
        );
    }

    json!({
        "kind": "statement_timeout",
        "context": context,
        "query_hash": query_hash,
        "statement_timeout_ms": statement_timeout_ms,
        "count_mode": count_mode,
        "recommended_actions": recommended_actions,
    })
}

fn validate_execute_sql_describe_export_flags(
    describe_only: bool,
    export_to_file: bool,
) -> Result<(), &'static str> {
    if describe_only && export_to_file {
        return Err("describe_only cannot be combined with export_to_file");
    }
    Ok(())
}

fn validate_execute_sql_bound_params(sql: &str, params: &[Value]) -> Result<(), String> {
    if params.is_empty() {
        return Ok(());
    }
    if contains_top_level_statement_delimiter(sql) {
        return Err(
            "params currently require exactly one SQL statement; remove top-level semicolons before retrying"
                .to_string(),
        );
    }
    Ok(())
}

fn execute_sql_describe_success(
    server: &PostgresMcp,
    columns: &[crate::db::QueryColumn],
    query_hash: &str,
    elapsed_ms: u64,
    query_hints: &[String],
) -> CallToolResult {
    contract_success(
        server,
        json!({
            "columns": columns,
        }),
        elapsed_ms,
        json!({
            "describe_only": true,
            "query_hash": query_hash,
            "column_count": columns.len(),
            "columns": columns,
            "query_hints": query_hints,
            "column_name_safety": column_name_safety_meta(columns),
        }),
    )
}

fn export_file_suffix(format: ExecuteSqlExportFormat) -> &'static str {
    match format {
        ExecuteSqlExportFormat::Csv => "csv",
        ExecuteSqlExportFormat::Tsv => "tsv",
        ExecuteSqlExportFormat::Jsonl => "jsonl",
    }
}

fn export_delimiter(format: ExecuteSqlExportFormat) -> Option<char> {
    match format {
        ExecuteSqlExportFormat::Csv => Some(','),
        ExecuteSqlExportFormat::Tsv => Some('\t'),
        ExecuteSqlExportFormat::Jsonl => None,
    }
}

fn export_cell_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(raw) => raw.to_string(),
        Value::Number(raw) => raw.to_string(),
        Value::String(raw) => raw.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn export_escape_field(raw: &str, delimiter: char) -> String {
    if raw.contains(delimiter) || raw.contains('"') || raw.contains('\n') || raw.contains('\r') {
        format!("\"{}\"", raw.replace('"', "\"\""))
    } else {
        raw.to_string()
    }
}

fn write_query_output_export(
    output: &crate::db::QueryOutput,
    query_hash: &str,
    format: ExecuteSqlExportFormat,
) -> Result<Value, String> {
    validate_query_export_hash(query_hash)?;
    let now_ms = current_unix_time_ms();
    let temp_dir = query_export_temp_root();
    let content = match export_delimiter(format) {
        Some(delimiter) => {
            let mut lines = Vec::new();
            if !output.columns.is_empty() {
                let header = output
                    .columns
                    .iter()
                    .map(|column| export_escape_field(&column.name, delimiter))
                    .collect::<Vec<_>>()
                    .join(&delimiter.to_string());
                lines.push(header);
            }
            for row in &output.rows {
                let line = output
                    .columns
                    .iter()
                    .map(|column| {
                        let cell = row
                            .get(&column.name)
                            .map(export_cell_string)
                            .unwrap_or_default();
                        export_escape_field(&cell, delimiter)
                    })
                    .collect::<Vec<_>>()
                    .join(&delimiter.to_string());
                lines.push(line);
            }
            format!("{}\n", lines.join("\n"))
        }
        None => {
            let mut lines = output
                .rows
                .iter()
                .map(|row| serde_json::to_string(row).unwrap_or_else(|_| "{}".to_string()))
                .collect::<Vec<_>>();
            if !lines.is_empty() {
                lines.push(String::new());
            }
            lines.join("\n")
        }
    };
    let mut attempts = 0usize;
    loop {
        let mut entropy = [0u8; 6];
        if let Err(err) = getrandom::fill(&mut entropy) {
            return Err(format!("failed to generate export path entropy: {err}"));
        }
        let suffix = entropy
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let path = temp_dir.join(format!(
            "postgres-mcp-{query_hash}-{now_ms}-{suffix}.{}",
            export_file_suffix(format)
        ));
        let mut file_options = std::fs::OpenOptions::new();
        file_options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            file_options.mode(0o600);
        }
        let mut file = match file_options.open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists && attempts < 32 => {
                attempts += 1;
                continue;
            }
            Err(err) => {
                return Err(format!("failed to write export file: {err}"));
            }
        };

        use std::io::Write;
        file.write_all(content.as_bytes())
            .map_err(|err| format!("failed to write export file: {err}"))?;
        return Ok(json!({
            "enabled": true,
            "format": format.as_str(),
            "path": path.display().to_string(),
            "row_count": output.rows.len(),
            "column_count": output.columns.len(),
            "bytes": content.len(),
        }));
    }
}

async fn execute_sql_db_error_result(
    server: &PostgresMcp,
    query_hash: &str,
    sql: &str,
    context: &str,
    err: &crate::db::DbError,
    elapsed_ms: u64,
    diagnose_on_timeout: bool,
    statement_timeout_override: Option<Duration>,
    count_mode: Option<ExecuteSqlResolvedCountMode>,
) -> CallToolResult {
    let root_message = if let Some(sqlstate) = err.sqlstate() {
        format!("{} [sqlstate: {sqlstate}]", err.message())
    } else {
        err.message().to_string()
    };
    let error_message = if context.is_empty() {
        root_message
    } else {
        format!("{root_message} ({context})")
    };
    let derived_hint = derive_execute_sql_hint(server, sql, context, err).await;
    let schema_hints = derive_execute_sql_schema_hints(server, sql, err).await;
    let hint = match (err.hint(), derived_hint) {
        (Some(existing), _) => Some(existing.to_string()),
        (None, Some(derived)) => Some(derived),
        (None, None) => None,
    };
    let mut payload = json!({
        "error": error_message,
        "code": err.code(),
        "reason": err.reason(),
        "sqlstate": err.sqlstate(),
        "detail": err.detail(),
        "hint": hint,
        "position": err.position(),
        "schema_hints": schema_hints,
    });
    if diagnose_on_timeout
        && is_statement_timeout_error(err)
        && let Some(map) = payload.as_object_mut()
    {
        map.insert(
            "diagnostics".to_string(),
            timeout_diagnostics_payload(
                query_hash,
                context,
                statement_timeout_override,
                count_mode,
            ),
        );
    }
    contract_error(server, payload, elapsed_ms, json!({}))
}

async fn execute_sql_preflight_schema_error_result(
    server: &PostgresMcp,
    query_hash: &str,
    sql: &str,
    err: &crate::db::DbError,
    elapsed_ms: u64,
) -> CallToolResult {
    let failure_kind = match err.sqlstate() {
        Some("42P01") => "missing_relation",
        Some("42703") => "missing_column",
        _ => "schema_validation_failed",
    };
    let (code, reason) = match failure_kind {
        "missing_relation" => (
            "SQL_PREFLIGHT_MISSING_RELATION",
            "sql_preflight_missing_relation",
        ),
        "missing_column" => (
            "SQL_PREFLIGHT_MISSING_COLUMN",
            "sql_preflight_missing_column",
        ),
        _ => ("SQL_PREFLIGHT_FAILED", "sql_preflight_failed"),
    };
    let derived_hint =
        derive_execute_sql_hint(server, sql, "Preflight schema validation failed", err).await;
    let schema_hints = derive_execute_sql_schema_hints(server, sql, err).await;
    let hint = match (err.hint(), derived_hint) {
        (Some(existing), _) => Some(existing.to_string()),
        (None, Some(derived)) => Some(derived),
        (None, None) => None,
    };
    let mut recommended_actions = vec![
        "run list_objects with include_columns=true to verify relation and column names"
            .to_string(),
        "retry after correcting relation aliases and column identifiers".to_string(),
    ];
    if let Some(hint_text) = hint.as_ref() {
        recommended_actions.push(hint_text.clone());
    }

    contract_error(
        server,
        json!({
            "error": format!("Preflight rejected query before execution: {}", err.message()),
            "code": code,
            "reason": reason,
            "sqlstate": err.sqlstate(),
            "detail": err.detail(),
            "hint": hint,
            "position": err.position(),
            "schema_hints": schema_hints,
            "preflight": {
                "enabled": true,
                "phase": "schema_validation",
                "failure_kind": failure_kind,
                "query_hash": query_hash,
                "recommended_actions": recommended_actions,
            }
        }),
        elapsed_ms,
        json!({}),
    )
}

pub(crate) fn contains_top_level_statement_delimiter(sql: &str) -> bool {
    let canonical = canonicalize_sql(sql);
    let bytes = canonical.as_bytes();
    let mut state = HelperScanState::Normal;
    let mut block_comment_depth = 0usize;
    let mut single_quote_backslash_escape = false;
    let mut pending_dollar_quote_end = 0usize;
    let mut paren_depth = 0usize;
    let mut i = 0usize;

    while i < bytes.len() {
        match state {
            HelperScanState::Normal => {
                if bytes[i] == b'\'' {
                    state = HelperScanState::SingleQuote;
                    single_quote_backslash_escape =
                        single_quote_uses_backslash_escape(&canonical, i);
                    i += 1;
                    continue;
                }
                if bytes[i] == b'"' {
                    state = HelperScanState::DoubleQuote;
                    i += 1;
                    continue;
                }
                if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    state = HelperScanState::LineComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    state = HelperScanState::BlockComment;
                    block_comment_depth = 1;
                    i += 2;
                    continue;
                }
                if let Ok(Some((delimiter_len, end))) = parse_dollar_quote_open(&canonical, i)
                    && delimiter_len > 0
                {
                    state = HelperScanState::DollarQuote(delimiter_len);
                    pending_dollar_quote_end = end;
                    i += delimiter_len;
                    continue;
                }
                if bytes[i] == b'(' {
                    paren_depth = paren_depth.saturating_add(1);
                    i += 1;
                    continue;
                }
                if bytes[i] == b')' {
                    paren_depth = paren_depth.saturating_sub(1);
                    i += 1;
                    continue;
                }
                if bytes[i] == b';' && paren_depth == 0 {
                    return has_top_level_sql_after_delimiter(&canonical, i + 1);
                }
                i += 1;
            }
            HelperScanState::SingleQuote => {
                if single_quote_backslash_escape && bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::DoubleQuote => {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::LineComment => {
                if bytes[i] == b'\n' {
                    state = HelperScanState::Normal;
                }
                i += 1;
            }
            HelperScanState::BlockComment => {
                if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    block_comment_depth = block_comment_depth.saturating_add(1);
                    i += 2;
                    continue;
                }
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    block_comment_depth = block_comment_depth.saturating_sub(1);
                    if block_comment_depth == 0 {
                        state = HelperScanState::Normal;
                    }
                    i += 2;
                    continue;
                }
                i += 1;
            }
            HelperScanState::DollarQuote(end_len) => {
                if i + end_len <= bytes.len()
                    && is_dollar_quote_end(&canonical, i, end_len, pending_dollar_quote_end)
                {
                    state = HelperScanState::Normal;
                    i += end_len;
                    continue;
                }
                i += 1;
            }
        }
    }

    false
}

fn single_quote_uses_backslash_escape(sql: &str, quote_idx: usize) -> bool {
    if quote_idx == 0 {
        return false;
    }
    let bytes = sql.as_bytes();
    let prefix = bytes[quote_idx - 1];
    if prefix != b'e' && prefix != b'E' {
        return false;
    }
    if quote_idx >= 2 {
        let prior = bytes[quote_idx - 2];
        if prior.is_ascii_alphanumeric() || prior == b'_' || prior == b'$' {
            return false;
        }
    }
    true
}

fn has_top_level_sql_after_delimiter(sql: &str, start: usize) -> bool {
    let bytes = sql.as_bytes();
    let mut i = start;

    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() || bytes[i] == b';' {
            i += 1;
            continue;
        }
        if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            let mut depth = 1usize;
            while i < bytes.len() && depth > 0 {
                if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                    depth = depth.saturating_add(1);
                    i += 2;
                    continue;
                }
                if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    depth = depth.saturating_sub(1);
                    i += 2;
                    continue;
                }
                i += 1;
            }
            continue;
        }
        return true;
    }

    false
}

fn execute_sql_preflight_multi_statement_error_result(
    server: &PostgresMcp,
    query_hash: &str,
    elapsed_ms: u64,
) -> CallToolResult {
    contract_error(
        server,
        json!({
            "error": "Preflight requires exactly one SQL statement; remove top-level semicolons or disable preflight_check.",
            "code": "SQL_PREFLIGHT_MULTI_STATEMENT",
            "reason": "sql_preflight_multi_statement",
            "preflight": {
                "enabled": true,
                "phase": "schema_validation",
                "failure_kind": "multiple_statements",
                "query_hash": query_hash,
                "recommended_actions": [
                    "submit a single SQL statement when preflight_check=true",
                    "remove top-level semicolons that separate statements",
                    "set preflight_check=false only when multi-statement behavior is required"
                ],
            }
        }),
        elapsed_ms,
        json!({}),
    )
}

async fn execute_sql_schema_preflight(
    server: &PostgresMcp,
    session_id: Option<&str>,
    query_hash: &str,
    sql: &str,
    params: Option<&[Value]>,
    statement_timeout_override: Option<Duration>,
    elapsed_ms: u64,
) -> Result<Option<String>, CallToolResult> {
    if contains_top_level_statement_delimiter(sql) {
        return Err(execute_sql_preflight_multi_statement_error_result(
            server, query_hash, elapsed_ms,
        ));
    }
    let preflight_sql = format!("EXPLAIN (FORMAT JSON) {}", canonicalize_sql(sql));
    let preflight_params = params.unwrap_or(&[]);
    let (preflight_result, _) = execute_sql_with_optional_session(
        server,
        session_id,
        &preflight_sql,
        preflight_params,
        statement_timeout_override,
        elapsed_ms,
    )
    .await?;
    match preflight_result {
        Ok(_) => Ok(Some(
            "preflight_check validated relation and column references before execution".to_string(),
        )),
        Err(err) if matches!(err.sqlstate(), Some("42P01" | "42703")) => Err(
            execute_sql_preflight_schema_error_result(server, query_hash, sql, &err, elapsed_ms)
                .await,
        ),
        Err(err) => Ok(Some(format!(
            "preflight_check skipped due to planner error {}",
            err.code()
        ))),
    }
}

fn extract_plan_row_estimate(plan: &Value) -> Option<usize> {
    match plan {
        Value::Object(object) => {
            if let Some(estimate) = object.get("Plan Rows").and_then(parse_usize_from_json) {
                return Some(estimate);
            }
            if let Some(estimate) = object.get("Plan").and_then(extract_plan_row_estimate) {
                return Some(estimate);
            }
            if let Some(plans) = object.get("Plans").and_then(Value::as_array) {
                for entry in plans {
                    if let Some(estimate) = extract_plan_row_estimate(entry) {
                        return Some(estimate);
                    }
                }
            }
            for value in object.values() {
                if let Some(estimate) = extract_plan_row_estimate(value) {
                    return Some(estimate);
                }
            }
            None
        }
        Value::Array(items) => {
            for item in items {
                if let Some(estimate) = extract_plan_row_estimate(item) {
                    return Some(estimate);
                }
            }
            None
        }
        _ => None,
    }
}

fn wrap_for_row_count_estimate(sql: &str) -> String {
    format!(
        "EXPLAIN (FORMAT JSON) SELECT * FROM ({}) AS _postgres_mcp_results",
        canonicalize_sql(sql)
    )
}

fn next_offset_for_exact_count(
    current_offset: usize,
    rows_returned: usize,
    row_count_total: usize,
) -> Option<usize> {
    if rows_returned == 0 {
        return None;
    }
    let next_offset = current_offset.saturating_add(rows_returned);
    if next_offset <= current_offset {
        return None;
    }
    if next_offset < row_count_total {
        Some(next_offset)
    } else {
        None
    }
}

fn next_offset_for_page_window(
    current_offset: usize,
    rows_fetched: usize,
    page_size: usize,
) -> Option<usize> {
    if rows_fetched <= page_size {
        return None;
    }
    let next_offset = current_offset.saturating_add(page_size);
    if next_offset <= current_offset {
        return None;
    }
    Some(next_offset)
}

async fn estimate_row_count_total(
    server: &PostgresMcp,
    session_id: Option<&str>,
    sql: &str,
    params: Option<&[Value]>,
    statement_timeout_override: Option<Duration>,
) -> Result<Option<usize>, crate::db::DbError> {
    let explain_sql = wrap_for_row_count_estimate(sql);
    let explain_params = params.unwrap_or(&[]);
    let (explain_result, _) = execute_sql_with_optional_session(
        server,
        session_id,
        &explain_sql,
        explain_params,
        statement_timeout_override,
        0,
    )
    .await
    .map_err(|_| crate::db::DbError::session_closed("pinned session not found for explain"))?;
    let explain_output = explain_result?;
    Ok(extract_query_plan_value(&explain_output.rows)
        .as_ref()
        .and_then(extract_plan_row_estimate))
}

fn spawn_async_row_count_job(
    server: PostgresMcp,
    sql: String,
    params: Vec<Value>,
    statement_timeout_override: Option<Duration>,
) -> Option<String> {
    let count_query = wrap_for_row_count(&sql);
    let query_hash = response_page_hash_for_params(&count_query, &params);
    let job = match server.query_jobs.create(&query_hash) {
        Ok(job) => job,
        Err(_) => return None,
    };
    let job_id = job.snapshot().job_id;
    let job_handle = job.clone();

    let task = tokio::spawn(async move {
        let running_snapshot = job_handle.mark_running();
        if running_snapshot.state == crate::server::QueryJobState::Canceled {
            return;
        }

        let payload = match server
            .db
            .execute_user_sql_with_params_and_statement_timeout(
                &count_query,
                &params,
                statement_timeout_override,
            )
            .await
        {
            Ok(output) => {
                let row_count_total = extract_row_count(&output).unwrap_or(0);
                json!({
                    "ok": true,
                    "data": {
                        "row_count_total": row_count_total,
                    },
                    "meta": {
                        "row_count_mode": "count_exact",
                    },
                })
            }
            Err(err) => {
                let error_payload = execute_sql_db_error_result(
                    &server,
                    &query_hash,
                    &sql,
                    "Error counting query rows (async)",
                    &err,
                    0,
                    false,
                    statement_timeout_override,
                    Some(ExecuteSqlResolvedCountMode::Exact),
                )
                .await
                .structured_content
                .unwrap_or_else(|| {
                    json!({
                        "ok": false,
                        "error": {
                            "error": err.message(),
                            "code": err.code(),
                            "reason": err.reason(),
                            "sqlstate": err.sqlstate(),
                        },
                        "meta": {},
                    })
                });
                error_payload
            }
        };

        let terminal_state = if query_job_payload_failed(&payload) {
            crate::server::QueryJobState::Failed
        } else {
            crate::server::QueryJobState::Succeeded
        };
        let _ = job_handle.complete(terminal_state, payload);
    });
    job.register_abort_handle(task.abort_handle());

    Some(job_id)
}

fn is_runtime_blocked_ddl(sql: &str) -> bool {
    let canonical = canonicalize_sql(sql).to_ascii_lowercase();
    starts_with_any_keyword(
        &canonical,
        &[
            "create ",
            "alter ",
            "drop ",
            "truncate ",
            "comment ",
            "grant ",
            "revoke ",
            "reindex ",
            "refresh materialized view ",
        ],
    )
}

#[rmcp::tool_router(router = tool_router_postgres_query, vis = "pub")]
impl PostgresMcp {
    #[tool(
        name = "session_open",
        description = "Open a pinned PostgreSQL session for temp-table and transaction workflows"
    )]
    async fn session_open(
        &self,
        Parameters(args): Parameters<SessionOpenArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let idle_ttl = args
            .idle_ttl_ms
            .map(Duration::from_millis)
            .unwrap_or_else(|| self.default_pinned_session_idle_ttl())
            .min(Duration::from_millis(PINNED_SESSION_IDLE_TTL_MAX_MS));
        match self.open_pinned_session(idle_ttl).await {
            Ok(snapshot) => Ok(contract_success(
                self,
                serde_json::to_value(&snapshot).unwrap_or(Value::Null),
                elapsed_ms(started),
                json!({ "session_id": snapshot.session_id, "backend_pid": snapshot.backend_pid }),
            )),
            Err(err) => Ok(contract_error(
                self,
                json!({
                    "error": err.message(),
                    "code": err.code(),
                    "reason": err.reason(),
                }),
                elapsed_ms(started),
                json!({}),
            )),
        }
    }

    #[tool(
        name = "session_status",
        description = "Inspect pinned-session state including expiry and transaction hints"
    )]
    async fn session_status(
        &self,
        Parameters(args): Parameters<SessionIdArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let session_id = args.session_id.trim();
        if session_id.is_empty() {
            return Ok(error_result(
                self,
                "session_id must not be empty",
                elapsed_ms(started),
            ));
        }
        let Some(snapshot) = self.pinned_session_snapshot(session_id) else {
            return Ok(pinned_session_not_found_result(
                self,
                session_id,
                elapsed_ms(started),
            ));
        };
        Ok(contract_success(
            self,
            serde_json::to_value(&snapshot).unwrap_or(Value::Null),
            elapsed_ms(started),
            json!({ "session_id": snapshot.session_id, "backend_pid": snapshot.backend_pid }),
        ))
    }

    #[tool(
        name = "session_close",
        description = "Close a pinned PostgreSQL session and discard its temp state"
    )]
    async fn session_close(
        &self,
        Parameters(args): Parameters<SessionIdArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let session_id = args.session_id.trim();
        if session_id.is_empty() {
            return Ok(error_result(
                self,
                "session_id must not be empty",
                elapsed_ms(started),
            ));
        }
        let Some(snapshot) = self.close_pinned_session(session_id) else {
            return Ok(pinned_session_not_found_result(
                self,
                session_id,
                elapsed_ms(started),
            ));
        };
        Ok(contract_success(
            self,
            serde_json::to_value(&snapshot).unwrap_or(Value::Null),
            elapsed_ms(started),
            json!({ "session_id": snapshot.session_id, "backend_pid": snapshot.backend_pid }),
        ))
    }

    #[tool(
        name = "query_start",
        description = "Start an asynchronous SQL query and return a job_id (compatibility surface; prefer query_sql or query_tuples with task augmentation)"
    )]
    async fn query_start(
        &self,
        Parameters(args): Parameters<QueryStartArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let mut execute_sql_args = args.into_execute_sql_args();
        if execute_sql_args.session_id.is_some() {
            return Ok(error_result(
                self,
                "query_start does not support session_id; use query_sql or query_tuples with task augmentation for async reads or execute_sql for pinned-session workflows",
                elapsed_ms(started),
            ));
        }
        let statement_timeout_override = match resolve_execute_sql_statement_timeout_override(
            execute_sql_args.statement_timeout_ms,
        ) {
            Ok(timeout) => timeout,
            Err(err) => return Ok(error_result(self, &err, elapsed_ms(started))),
        };
        let normalized = match normalize_execute_sql_core(
            execute_sql_args.sql.clone(),
            execute_sql_args.params.clone(),
            execute_sql_args.cursor.clone(),
            execute_sql_args.describe_only,
            execute_sql_args.export_to_file,
            statement_timeout_override,
        ) {
            Ok(ExecuteSqlNormalizationStage::Ready(normalized)) => normalized,
            Ok(ExecuteSqlNormalizationStage::Empty(_)) => {
                return Ok(error_result(
                    self,
                    "sql must not be empty",
                    elapsed_ms(started),
                ));
            }
            Err(err) => return Ok(error_result(self, &err, elapsed_ms(started))),
        };
        if let Err(err) = classify_restricted_sql(&normalized.sql.rewritten) {
            return Ok(policy_error_result(
                self,
                err.code.as_str(),
                &format!("query_start only supports read-safe SQL: {}", err.message),
                "restricted_sql",
                elapsed_ms(started),
            ));
        }
        let pagination =
            match normalize_execute_sql_pagination(self, &normalized, elapsed_ms(started)) {
                Ok(pagination) => pagination,
                Err(result) => return Ok(result),
            };
        let execution_facts = ExecuteSqlExecutionFacts::from_core(
            ExecuteSqlRequestScope::QueryStart,
            &normalized,
            pagination,
            None,
        );
        execute_sql_args.sql = Some(normalized.sql.rewritten.clone());
        let job = match self.query_jobs.create(&normalized.query_hash) {
            Ok(job) => job,
            Err(err) => {
                return Ok(query_response_error(
                    err.message(),
                    err.code(),
                    err.reason(),
                    elapsed_ms(started),
                    self,
                ));
            }
        };
        let snapshot = job.snapshot();

        let job_handle = job.clone();
        let server = self.clone();
        let task = tokio::spawn(async move {
            let running_snapshot = job_handle.mark_running();
            if running_snapshot.state == crate::server::QueryJobState::Canceled {
                return;
            }

            let execution_result = server.execute_sql(Parameters(execute_sql_args)).await;
            let payload = match execution_result {
                Ok(result) => result.structured_content.unwrap_or_else(|| {
                    json!({
                        "ok": true,
                        "data": null,
                        "meta": {},
                    })
                }),
                Err(err) => {
                    let error = err.to_string();
                    json!({
                        "ok": false,
                        "error": {
                            "error": error,
                            "code": "QUERY_JOB_INTERNAL",
                            "reason": "query_job_internal",
                        },
                        "meta": {},
                    })
                }
            };

            let terminal_state = if query_job_payload_failed(&payload) {
                crate::server::QueryJobState::Failed
            } else {
                crate::server::QueryJobState::Succeeded
            };
            let _ = job_handle.complete(terminal_state, payload);
        });
        job.register_abort_handle(task.abort_handle());

        let launch_result = contract_success(
            self,
            query_job_payload(
                &snapshot,
                QueryStatusWaitMode::Immediate,
                "queued",
                0,
                false,
            ),
            elapsed_ms(started),
            json!({
                "job_id": snapshot.job_id,
                "query_hash": snapshot.query_hash,
            }),
        );
        Ok(apply_execute_sql_execution_metadata(
            launch_result,
            &execution_facts,
        ))
    }

    #[tool(
        name = "query_start_and_wait",
        description = "Start a query and block until terminal status (or until wait_ms deadline) (compatibility surface; prefer query_sql or query_tuples with task augmentation)"
    )]
    async fn query_start_and_wait(
        &self,
        Parameters(args): Parameters<QueryStartAndWaitArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let QueryStartAndWaitArgs {
            query_start,
            wait_ms,
        } = args;
        if let Err(err) = parse_query_status_wait_mode(wait_ms, wait_ms.is_none()) {
            return Ok(error_result(self, &err, elapsed_ms(started)));
        }
        let launch = self.query_start(Parameters(query_start)).await?;
        let Some(launch_payload) = launch.structured_content.as_ref() else {
            return Ok(error_result(
                self,
                "query_start_and_wait failed to capture launch payload",
                elapsed_ms(started),
            ));
        };
        if launch_payload
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .is_some_and(|ok| !ok)
        {
            return Ok(launch);
        }
        let job_id = launch_payload
            .pointer("/data/job_id")
            .and_then(Value::as_str)
            .or_else(|| launch_payload.get("job_id").and_then(Value::as_str))
            .unwrap_or_default()
            .to_string();
        if job_id.is_empty() {
            return Ok(error_result(
                self,
                "query_start_and_wait launch response missing job_id",
                elapsed_ms(started),
            ));
        }
        if let Some(wait_ms) = wait_ms {
            let Some(job) = self.query_jobs.get(&job_id) else {
                return Ok(query_response_error(
                    "Query job not found",
                    "QUERY_JOB_NOT_FOUND",
                    "query_job_not_found",
                    elapsed_ms(started),
                    self,
                ));
            };
            let wait_started = std::time::Instant::now();
            let deadline_at = wait_started + Duration::from_millis(wait_ms);
            let mut wait_trigger = "immediate";
            let (mut snapshot, mut revision) = job.snapshot_with_revision();
            while !snapshot.state.is_terminal() {
                let now = std::time::Instant::now();
                if now >= deadline_at {
                    wait_trigger = "deadline_elapsed";
                    break;
                }
                let remaining = deadline_at.saturating_duration_since(now);
                let updated = job.wait_for_update_since(revision, Some(remaining)).await;
                if !updated {
                    wait_trigger = "deadline_elapsed";
                    break;
                }
                let (new_snapshot, new_revision) = job.snapshot_with_revision();
                snapshot = new_snapshot;
                revision = new_revision;
            }
            // Re-read once after the wait loop so near-deadline terminal transitions
            // are reflected in returned state/response payload fields.
            snapshot = job.snapshot();
            if snapshot.state.is_terminal() {
                wait_trigger = "job_terminal";
            }
            let wait_elapsed_ms = wait_started.elapsed().as_millis() as u64;
            let include_response = snapshot.state.is_terminal();
            return Ok(contract_success(
                self,
                query_job_payload(
                    &snapshot,
                    QueryStatusWaitMode::Deadline { wait_ms },
                    wait_trigger,
                    wait_elapsed_ms,
                    include_response,
                ),
                elapsed_ms(started),
                json!({
                    "job_id": snapshot.job_id,
                    "query_hash": snapshot.query_hash,
                    "state": snapshot.state.as_str(),
                    "terminal": snapshot.state.is_terminal(),
                }),
            ));
        }
        self.query_status(Parameters(QueryStatusArgs {
            job_id,
            wait_ms: None,
            wait_until_terminal: true,
        }))
        .await
    }

    #[tool(
        name = "query_status",
        description = "Get asynchronous query status and optionally wait for updates (compatibility surface)"
    )]
    async fn query_status(
        &self,
        Parameters(args): Parameters<QueryStatusArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let job_id = args.job_id.trim();
        if job_id.is_empty() {
            return Ok(error_result(
                self,
                "job_id must not be empty",
                elapsed_ms(started),
            ));
        }
        let wait_mode = match parse_query_status_wait_mode(args.wait_ms, args.wait_until_terminal) {
            Ok(mode) => mode,
            Err(err) => return Ok(error_result(self, err.as_str(), elapsed_ms(started))),
        };
        let Some(job) = self.query_jobs.get(job_id) else {
            return Ok(query_response_error(
                "Query job not found",
                "QUERY_JOB_NOT_FOUND",
                "query_job_not_found",
                elapsed_ms(started),
                self,
            ));
        };

        let wait_started = std::time::Instant::now();
        let mut wait_trigger = "immediate";
        let (mut snapshot, mut revision) = job.snapshot_with_revision();
        if !snapshot.state.is_terminal() {
            match wait_mode {
                QueryStatusWaitMode::Immediate => {}
                QueryStatusWaitMode::Deadline { wait_ms } => {
                    let updated = job
                        .wait_for_update_since(revision, Some(Duration::from_millis(wait_ms)))
                        .await;
                    wait_trigger = if updated {
                        "job_updated"
                    } else {
                        "deadline_elapsed"
                    };
                    snapshot = job.snapshot();
                }
                QueryStatusWaitMode::UntilTerminal => loop {
                    if snapshot.state.is_terminal() {
                        wait_trigger = "job_terminal";
                        break;
                    }
                    let updated = job
                        .wait_for_update_since(revision, Some(Duration::from_secs(60)))
                        .await;
                    if updated {
                        let (new_snapshot, new_revision) = job.snapshot_with_revision();
                        snapshot = new_snapshot;
                        revision = new_revision;
                        if snapshot.state.is_terminal() {
                            wait_trigger = "job_terminal";
                            break;
                        }
                    }
                },
            }
        }

        let wait_elapsed_ms = wait_started.elapsed().as_millis() as u64;
        let include_response = snapshot.state.is_terminal();
        Ok(contract_success(
            self,
            query_job_payload(
                &snapshot,
                wait_mode,
                wait_trigger,
                wait_elapsed_ms,
                include_response,
            ),
            elapsed_ms(started),
            json!({
                "job_id": snapshot.job_id,
                "query_hash": snapshot.query_hash,
                "state": snapshot.state.as_str(),
                "terminal": snapshot.state.is_terminal(),
            }),
        ))
    }

    #[tool(
        name = "query_cancel",
        description = "Cancel an asynchronous SQL query job (compatibility surface)"
    )]
    async fn query_cancel(
        &self,
        Parameters(args): Parameters<QueryCancelArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let job_id = args.job_id.trim();
        if job_id.is_empty() {
            return Ok(error_result(
                self,
                "job_id must not be empty",
                elapsed_ms(started),
            ));
        }
        let Some(job) = self.query_jobs.get(job_id) else {
            return Ok(query_response_error(
                "Query job not found",
                "QUERY_JOB_NOT_FOUND",
                "query_job_not_found",
                elapsed_ms(started),
                self,
            ));
        };

        let before = job.snapshot();
        let after = job.cancel(self.startup_role);
        let canceled =
            !before.state.is_terminal() && after.state == crate::server::QueryJobState::Canceled;
        Ok(contract_success(
            self,
            json!({
                "job_id": after.job_id,
                "state": after.state.as_str(),
                "terminal": after.state.is_terminal(),
                "canceled": canceled,
                "was_terminal": before.state.is_terminal(),
                "finished_at_unix_ms": after.finished_at_unix_ms,
            }),
            elapsed_ms(started),
            json!({
                "job_id": after.job_id,
                "query_hash": after.query_hash,
            }),
        ))
    }

    #[tool(
        name = "explain_query",
        description = "Explains the execution plan for a SQL query, showing how the database will execute it and provides detailed cost estimates."
    )]
    async fn explain_query(
        &self,
        Parameters(args): Parameters<ExplainQueryArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let sql = args.sql.trim();
        if sql.is_empty() {
            return Ok(error_result(
                self,
                "sql must not be empty",
                elapsed_ms(started),
            ));
        }
        if let Err(err) = validate_sql_size(sql, "sql") {
            return Ok(error_result(self, &err, elapsed_ms(started)));
        }
        if args.hypothetical_indexes.len() > MAX_HYPOTHETICAL_INDEXES {
            return Ok(error_result(
                self,
                &format!(
                    "hypothetical_indexes exceeds maximum of {} entries",
                    MAX_HYPOTHETICAL_INDEXES
                ),
                elapsed_ms(started),
            ));
        }

        if let Err(err) = classify_restricted_sql(sql) {
            return Ok(policy_error_result(
                self,
                err.code.as_str(),
                &format!("unsafe SQL for explain_query: {}", err.message),
                "restricted_sql",
                elapsed_ms(started),
            ));
        }

        if args.analyze && !args.hypothetical_indexes.is_empty() {
            return Ok(error_result(
                self,
                "Cannot use analyze=true together with hypothetical_indexes",
                elapsed_ms(started),
            ));
        }

        let mut prefix = String::new();
        if !args.hypothetical_indexes.is_empty() {
            if let Err(err) = ensure_extension_ready(
                self,
                ExtensionCapability::Hypopg,
                "hypothetical_indexing_unavailable",
            )
            .await
            {
                if err.code != "EXTENSION_UNAVAILABLE" {
                    return Ok(extension_check_error_result(
                        self,
                        &err,
                        elapsed_ms(started),
                    ));
                }
                return Ok(extension_unavailable_result(
                    self,
                    ExtensionCapability::Hypopg.extension_name(),
                    &err.reason,
                    &err.message,
                    merge_payload(json!({ "hypopg_installed": false }), &err.details),
                    elapsed_ms(started),
                ));
            }

            prefix.push_str("SELECT hypopg_reset();");
            for idx in &args.hypothetical_indexes {
                if idx.table.trim().is_empty() || idx.columns.is_empty() {
                    return Ok(error_result(
                        self,
                        "Each hypothetical index requires table and non-empty columns",
                        elapsed_ms(started),
                    ));
                }
                if idx.table.len() > MAX_QUALIFIED_IDENTIFIER_BYTES {
                    return Ok(error_result(
                        self,
                        "hypothetical index table identifier exceeds maximum length",
                        elapsed_ms(started),
                    ));
                }
                if idx.columns.len() > MAX_HYPOTHETICAL_INDEX_COLUMNS {
                    return Ok(error_result(
                        self,
                        &format!(
                            "hypothetical index column list exceeds maximum of {}",
                            MAX_HYPOTHETICAL_INDEX_COLUMNS
                        ),
                        elapsed_ms(started),
                    ));
                }
                let using = idx
                    .using
                    .as_deref()
                    .unwrap_or("btree")
                    .trim()
                    .to_ascii_lowercase();
                if !index_method_re().is_match(&using) {
                    return Ok(error_result(
                        self,
                        "Invalid index method in hypothetical_indexes.using",
                        elapsed_ms(started),
                    ));
                }
                if using.len() > MAX_IDENTIFIER_BYTES {
                    return Ok(error_result(
                        self,
                        "Invalid index method length",
                        elapsed_ms(started),
                    ));
                }
                if idx
                    .columns
                    .iter()
                    .any(|column| column.trim().is_empty() || column.len() > MAX_IDENTIFIER_BYTES)
                {
                    return Ok(error_result(
                        self,
                        "Invalid hypothetical index column identifier length",
                        elapsed_ms(started),
                    ));
                }

                let table_ident = sql_quote_qualified_ident(idx.table.trim());
                if table_ident.is_empty() {
                    return Ok(error_result(
                        self,
                        "Invalid hypothetical index table identifier",
                        elapsed_ms(started),
                    ));
                }
                let columns = idx
                    .columns
                    .iter()
                    .map(|c| sql_quote_ident(c.trim()))
                    .collect::<Vec<_>>()
                    .join(", ");
                let ddl = format!("CREATE INDEX ON {table_ident} USING {using} ({columns})");
                prefix.push_str(&format!(
                    "SELECT hypopg_create_index({});",
                    sql_quote_literal(&ddl)
                ));
            }
        }

        let mut options = vec!["FORMAT JSON".to_string()];
        if args.analyze {
            options.push("ANALYZE".to_string());
        }
        if !args.hypothetical_indexes.is_empty() {
            options.push("COSTS TRUE".to_string());
        }

        let explain_sql = format!("{prefix}EXPLAIN ({}) {sql}", options.join(", "));

        match self.db.execute_query_unrestricted(&explain_sql).await {
            Ok(output) => {
                let plan = extract_query_plan_value(&output.rows)
                    .unwrap_or_else(|| json!({ "error": "No QUERY PLAN returned" }));
                Ok(contract_success(
                    self,
                    json!({
                    "analyze": args.analyze,
                    "hypothetical_indexes": args.hypothetical_indexes,
                    "plan": plan,
                    }),
                    elapsed_ms(started),
                    json!({ "row_count_returned": 1 }),
                ))
            }
            Err(err) => Ok(extension_runtime_error_result(
                self,
                ExtensionCapability::Hypopg,
                "hypothetical_indexing_unavailable",
                &err,
                json!({ "hypopg_installed": false }),
                "Error explaining query",
                elapsed_ms(started),
            )
            .await),
        }
    }

    #[tool(
        name = "execute_sql",
        description = "Execute any SQL query (compatibility surface; prefer query_sql/query_tuples/render_sql for agent-facing reads)"
    )]
    async fn execute_sql(
        &self,
        Parameters(args): Parameters<ExecuteSqlArgs>,
    ) -> Result<CallToolResult, crate::McpError> {
        let started = std::time::Instant::now();
        let (
            normalized_output_mode,
            response_formatting_mode,
            readable_output_hint,
            emit_markdown_text,
        ) = normalize_execute_sql_output_preferences(
            args.output_mode,
            args.response_formatting_mode,
        );
        let profile_resolution = resolve_execute_sql_profile(
            args.profile,
            normalized_output_mode,
            args.max_rows,
            args.max_cell_chars,
            args.count_mode,
            args.metadata_verbosity,
            args.statement_timeout_ms,
            args.preflight_check,
            args.include_total_row_count,
            self.response_output_mode,
            self.response_page_size,
        );
        let effective_profile = profile_resolution.effective_profile;
        let requested_output_mode = profile_resolution.output_mode;
        let page_size = profile_resolution.page_size;
        let max_cell_chars = profile_resolution.max_cell_chars;
        let preflight_check = profile_resolution.preflight_check;
        let statement_timeout_override = match resolve_execute_sql_statement_timeout_override(
            profile_resolution.statement_timeout_ms,
        ) {
            Ok(timeout) => timeout,
            Err(err) => return Ok(error_result(self, &err, elapsed_ms(started))),
        };
        let currency_columns = args.currency_columns.unwrap_or_default();
        let diagnose_on_timeout = args.diagnose_on_timeout.unwrap_or(false);
        let session_id = args.session_id.as_deref();
        let mut profile_hints = profile_resolution.profile_hints;
        let (resolved_count_mode, mut count_mode_hints) = resolve_execute_sql_count_mode(
            profile_resolution.requested_count_mode,
            args.include_total_row_count,
        );
        let metadata_verbosity = profile_resolution.metadata_verbosity;
        let export_requested = args.export_to_file;
        let export_format = args.export_format.unwrap_or(ExecuteSqlExportFormat::Tsv);
        let normalized = match normalize_execute_sql_core(
            args.sql.clone(),
            args.params.clone(),
            args.cursor.clone(),
            args.describe_only,
            export_requested,
            statement_timeout_override,
        ) {
            Ok(ExecuteSqlNormalizationStage::Empty(empty)) => {
                let empty_output = crate::db::QueryOutput {
                    rows: Vec::new(),
                    columns: Vec::new(),
                    rows_affected: None,
                };
                let execution_facts = ExecuteSqlExecutionFacts::empty(
                    ExecuteSqlRequestScope::ExecuteSql,
                    empty.param_count,
                    empty.statement_timeout_override,
                );
                let result = query_success(
                    self,
                    &empty_output,
                    requested_output_mode,
                    elapsed_ms(started),
                    None,
                    None,
                    None,
                    None,
                    None,
                    max_cell_chars,
                    Vec::new(),
                    args.summary_only,
                    emit_markdown_text,
                );
                let result = apply_execute_sql_count_metadata(
                    result,
                    ExecuteSqlResolvedCountMode::None.row_count_mode(),
                    None,
                );
                let result = apply_execute_sql_metadata_verbosity(result, metadata_verbosity);
                let result = apply_execute_sql_execution_metadata(result, &execution_facts);
                let result = apply_execute_sql_effective_metadata(
                    result,
                    effective_profile,
                    ExecuteSqlResolvedCountMode::None,
                    metadata_verbosity,
                    requested_output_mode,
                    self.response_output_mode_auto_tabular.as_output_mode(),
                );
                return Ok(apply_execute_sql_compaction(
                    result,
                    metadata_verbosity,
                    requested_output_mode,
                ));
            }
            Ok(ExecuteSqlNormalizationStage::Ready(normalized)) => normalized,
            Err(err) => return Ok(error_result(self, &err, elapsed_ms(started))),
        };
        let mut query_hints = pre_execution_hints_for_sql(&normalized.sql.rewritten);
        query_hints.append(&mut profile_hints);
        if let Some(hint) = readable_output_hint {
            query_hints.push(hint.to_string());
        }
        query_hints.append(&mut count_mode_hints);
        if normalized.sql.helper_count > 0 {
            query_hints.push(format!(
                "expanded latest_snapshot helper: {}",
                normalized.sql.helper_count
            ));
        }
        if let Some(timeout) = normalized.statement_timeout_override {
            query_hints.push(format!(
                "statement timeout override applied for this call: {}ms",
                timeout.as_millis()
            ));
        }
        if !normalized.params.is_empty() {
            query_hints.push(format!("bound params applied: {}", normalized.params.len()));
        }
        let sql = normalized.sql.rewritten.clone();
        let params = normalized.params.clone();
        let query_hash = normalized.query_hash.clone();
        if normalized.describe_only {
            let pagination =
                match normalize_execute_sql_pagination(self, &normalized, elapsed_ms(started)) {
                    Ok(pagination) => pagination,
                    Err(result) => return Ok(result),
                };
            let describe_hints = {
                let mut hints = query_hints.clone();
                hints.push(
                    "describe_only prepared the SQL statement to return result columns without executing the query body".to_string(),
                );
                hints
            };
            let execution_facts = ExecuteSqlExecutionFacts::from_core(
                ExecuteSqlRequestScope::ExecuteSql,
                &normalized,
                pagination,
                Some(ExecuteSqlCountExecution::None),
            );
            let (describe_result, session_snapshot) = match describe_sql_with_optional_session(
                self,
                session_id,
                &sql,
                &params,
                normalized.statement_timeout_override,
                elapsed_ms(started),
            )
            .await
            {
                Ok(result) => result,
                Err(result) => return Ok(result),
            };
            return match describe_result {
                Ok(columns) => Ok(apply_pinned_session_meta(
                    apply_execute_sql_execution_metadata(
                        execute_sql_describe_success(
                            self,
                            &columns,
                            &query_hash,
                            elapsed_ms(started),
                            &describe_hints,
                        ),
                        &execution_facts,
                    ),
                    session_snapshot.as_ref(),
                )),
                Err(err) => Ok(apply_pinned_session_meta(
                    execute_sql_db_error_result(
                        self,
                        &query_hash,
                        &sql,
                        "Error describing query result schema",
                        &err,
                        elapsed_ms(started),
                        diagnose_on_timeout,
                        normalized.statement_timeout_override,
                        Some(resolved_count_mode),
                    )
                    .await,
                    session_snapshot.as_ref(),
                )),
            };
        }
        if self.startup_role == StartupRole::Runtime && is_runtime_blocked_ddl(&sql) {
            return Ok(policy_error_result(
                self,
                "RUNTIME_ROLE_DDL_BLOCKED",
                "DDL statements are blocked in startup_role=runtime; use startup_role=migrator for schema transitions",
                "startup_role_runtime",
                elapsed_ms(started),
            ));
        }
        if self.startup_degraded_read_only
            && let Err(err) = classify_restricted_sql(&sql)
        {
            let reason_suffix = self
                .startup_degraded_reason
                .as_deref()
                .map(|value| format!(" ({value})"))
                .unwrap_or_default();
            return Ok(policy_error_result(
                self,
                "STARTUP_DEGRADED_READ_ONLY",
                &format!(
                    "Server is running in degraded read-only mode{reason_suffix}; only read-safe SQL is allowed: {}",
                    err.message
                ),
                "startup_degraded_read_only",
                elapsed_ms(started),
            ));
        }
        let pagination =
            match normalize_execute_sql_pagination(self, &normalized, elapsed_ms(started)) {
                Ok(pagination) => pagination,
                Err(result) => return Ok(result),
            };
        let should_paginate = pagination.strategy.supports_cursor();
        let offset = pagination.offset;
        if preflight_check {
            match execute_sql_schema_preflight(
                self,
                session_id,
                &query_hash,
                &sql,
                Some(&params),
                normalized.statement_timeout_override,
                elapsed_ms(started),
            )
            .await
            {
                Ok(Some(hint)) => query_hints.push(hint),
                Ok(None) => {}
                Err(result) => return Ok(result),
            }
        }
        let query_fingerprint = query_fingerprint(&sql);
        if !should_paginate {
            let (execution_result, session_snapshot) = match execute_sql_with_optional_session(
                self,
                session_id,
                &sql,
                &params,
                normalized.statement_timeout_override,
                elapsed_ms(started),
            )
            .await
            {
                Ok(result) => result,
                Err(result) => return Ok(result),
            };
            return match execution_result {
                Ok(output) => {
                    let (output, formatted_columns) = apply_currency_display_mode(
                        &output,
                        response_formatting_mode,
                        &currency_columns,
                    );
                    if !formatted_columns.is_empty() {
                        query_hints.push(format!(
                            "response formatting mode currency applied to: {}",
                            formatted_columns.join(", ")
                        ));
                    }
                    let export_meta = if export_requested {
                        match write_query_output_export(&output, &query_hash, export_format) {
                            Ok(meta) => {
                                query_hints.push(format!(
                                    "export_to_file wrote {} rows to {}",
                                    output.rows.len(),
                                    meta.get("path").and_then(Value::as_str).unwrap_or_default()
                                ));
                                Some(meta)
                            }
                            Err(err) => return Ok(error_result(self, &err, elapsed_ms(started))),
                        }
                    } else {
                        None
                    };
                    let result = query_success(
                        self,
                        &output,
                        requested_output_mode,
                        elapsed_ms(started),
                        None,
                        None,
                        None,
                        None,
                        None,
                        max_cell_chars,
                        query_hints.clone(),
                        args.summary_only,
                        emit_markdown_text,
                    );
                    let result = apply_execute_sql_count_metadata(
                        result,
                        ExecuteSqlResolvedCountMode::None.row_count_mode(),
                        None,
                    );
                    let result = apply_execute_sql_metadata_verbosity(result, metadata_verbosity);
                    let result = apply_execute_sql_query_telemetry(
                        result,
                        &query_hash,
                        &query_fingerprint,
                        metadata_verbosity,
                    );
                    let execution_facts = ExecuteSqlExecutionFacts::from_core(
                        ExecuteSqlRequestScope::ExecuteSql,
                        &normalized,
                        pagination,
                        Some(ExecuteSqlCountExecution::None),
                    );
                    let result = apply_execute_sql_execution_metadata(result, &execution_facts);
                    let result = apply_execute_sql_export_metadata(result, export_meta);
                    let result = apply_execute_sql_effective_metadata(
                        result,
                        effective_profile,
                        ExecuteSqlResolvedCountMode::None,
                        metadata_verbosity,
                        requested_output_mode,
                        self.response_output_mode_auto_tabular.as_output_mode(),
                    );
                    Ok(apply_pinned_session_meta(
                        apply_execute_sql_compaction(
                            result,
                            metadata_verbosity,
                            requested_output_mode,
                        ),
                        session_snapshot.as_ref(),
                    ))
                }
                Err(err) => Ok(apply_pinned_session_meta(
                    execute_sql_db_error_result(
                        self,
                        &query_hash,
                        &sql,
                        "Error executing query",
                        &err,
                        elapsed_ms(started),
                        diagnose_on_timeout,
                        normalized.statement_timeout_override,
                        Some(resolved_count_mode),
                    )
                    .await,
                    session_snapshot.as_ref(),
                )),
            };
        }
        let mut effective_count_mode = resolved_count_mode;
        let mut row_count_mode = resolved_count_mode.row_count_mode().to_string();
        let mut row_count_job_id: Option<String> = None;
        let mut should_spawn_async_row_count = false;
        let row_count_total = match resolved_count_mode {
            ExecuteSqlResolvedCountMode::Exact => {
                let count_query = wrap_for_row_count(&sql);
                let (count_result, count_session_snapshot) =
                    match execute_sql_with_optional_session(
                        self,
                        session_id,
                        &count_query,
                        &params,
                        normalized.statement_timeout_override,
                        elapsed_ms(started),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(result) => return Ok(result),
                    };
                let count_output = match count_result {
                    Ok(output) => output,
                    Err(err) => {
                        return Ok(apply_pinned_session_meta(
                            execute_sql_db_error_result(
                                self,
                                &query_hash,
                                &sql,
                                "Error counting query rows",
                                &err,
                                elapsed_ms(started),
                                diagnose_on_timeout,
                                normalized.statement_timeout_override,
                                Some(resolved_count_mode),
                            )
                            .await,
                            count_session_snapshot.as_ref(),
                        ));
                    }
                };
                Some(extract_row_count(&count_output).unwrap_or(0))
            }
            ExecuteSqlResolvedCountMode::Estimated => {
                let estimated = match estimate_row_count_total(
                    self,
                    session_id,
                    &sql,
                    Some(&params),
                    normalized.statement_timeout_override,
                )
                .await
                {
                    Ok(estimate) => estimate,
                    Err(err) => {
                        query_hints.push(format!(
                            "count_mode=estimated fallback to page_window: {}",
                            err.message()
                        ));
                        None
                    }
                };
                if estimated.is_some() {
                    query_hints.push(
                        "count_mode=estimated uses EXPLAIN plan rows for approximate totals"
                            .to_string(),
                    );
                } else {
                    effective_count_mode = ExecuteSqlResolvedCountMode::None;
                    row_count_mode = effective_count_mode.row_count_mode().to_string();
                }
                estimated
            }
            ExecuteSqlResolvedCountMode::Async => {
                if offset == 0 {
                    should_spawn_async_row_count = true;
                } else {
                    query_hints.push(
                        "count_mode=async skipped on cursor pages to avoid duplicate count jobs"
                            .to_string(),
                    );
                    effective_count_mode = ExecuteSqlResolvedCountMode::None;
                    row_count_mode = effective_count_mode.row_count_mode().to_string();
                }
                None
            }
            ExecuteSqlResolvedCountMode::None => {
                query_hints.push(
                    "count_mode=none uses one-row look-ahead pagination without COUNT(*)"
                        .to_string(),
                );
                None
            }
        };

        let page_fetch_size = if resolved_count_mode.uses_exact_count_query() {
            page_size
        } else {
            page_size.saturating_add(1)
        };
        let page_query = wrap_for_page(&sql, offset, page_fetch_size);
        let (page_result, page_session_snapshot) = match execute_sql_with_optional_session(
            self,
            session_id,
            &page_query,
            &params,
            normalized.statement_timeout_override,
            elapsed_ms(started),
        )
        .await
        {
            Ok(result) => result,
            Err(result) => return Ok(result),
        };
        let mut output = match page_result {
            Ok(output) => output,
            Err(err) => {
                return Ok(apply_pinned_session_meta(
                    execute_sql_db_error_result(
                        self,
                        &query_hash,
                        &sql,
                        "Error executing paginated query",
                        &err,
                        elapsed_ms(started),
                        diagnose_on_timeout,
                        normalized.statement_timeout_override,
                        Some(resolved_count_mode),
                    )
                    .await,
                    page_session_snapshot.as_ref(),
                ));
            }
        };
        let next_offset = if resolved_count_mode.uses_exact_count_query() {
            let row_count_total = row_count_total.unwrap_or(0);
            next_offset_for_exact_count(offset, output.rows.len(), row_count_total)
        } else {
            let next_offset = next_offset_for_page_window(offset, output.rows.len(), page_size);
            if next_offset.is_some() {
                output.rows.truncate(page_size);
            }
            next_offset
        };
        if should_spawn_async_row_count {
            let job_id = spawn_async_row_count_job(
                self.clone(),
                sql.clone(),
                params.clone(),
                normalized.statement_timeout_override,
            );
            if let Some(job_id) = job_id {
                query_hints.push(format!(
                    "count_mode=async started background count job {job_id}; use query_status to await totals"
                ));
                row_count_job_id = Some(job_id);
            } else {
                query_hints.push(
                    "count_mode=async fallback to page_window: query job capacity reached"
                        .to_string(),
                );
                effective_count_mode = ExecuteSqlResolvedCountMode::None;
                row_count_mode = effective_count_mode.row_count_mode().to_string();
            }
        }
        let next_cursor = next_offset.map(|next_offset| {
            encode_pagination_cursor(
                self,
                PaginationCursorScope::ExecuteSql,
                &query_hash,
                next_offset,
            )
        });
        let (output, formatted_columns) =
            apply_currency_display_mode(&output, response_formatting_mode, &currency_columns);
        if !formatted_columns.is_empty() {
            query_hints.push(format!(
                "response formatting mode currency applied to: {}",
                formatted_columns.join(", ")
            ));
        }
        let export_meta = if export_requested {
            match write_query_output_export(&output, &query_hash, export_format) {
                Ok(meta) => {
                    query_hints.push(format!(
                        "export_to_file wrote {} rows to {}",
                        output.rows.len(),
                        meta.get("path").and_then(Value::as_str).unwrap_or_default()
                    ));
                    if next_offset.is_some() {
                        query_hints.push(
                            "export_to_file wrote the current page window only; follow next_cursor to export additional rows".to_string(),
                        );
                    }
                    Some(meta)
                }
                Err(err) => return Ok(error_result(self, &err, elapsed_ms(started))),
            }
        } else {
            None
        };

        let result = query_success(
            self,
            &output,
            requested_output_mode,
            elapsed_ms(started),
            row_count_total,
            Some(query_hash.as_str()),
            next_cursor,
            Some(offset),
            next_offset,
            max_cell_chars,
            query_hints,
            args.summary_only,
            emit_markdown_text,
        );
        let result =
            apply_execute_sql_count_metadata(result, &row_count_mode, row_count_job_id.as_deref());
        let result = apply_execute_sql_metadata_verbosity(result, metadata_verbosity);
        let result = apply_execute_sql_query_telemetry(
            result,
            &query_hash,
            &query_fingerprint,
            metadata_verbosity,
        );
        let count_execution = match effective_count_mode {
            ExecuteSqlResolvedCountMode::Exact => ExecuteSqlCountExecution::InlineExact,
            ExecuteSqlResolvedCountMode::Estimated => {
                if row_count_total.is_some() {
                    ExecuteSqlCountExecution::EstimatedPlan
                } else {
                    ExecuteSqlCountExecution::PageWindow
                }
            }
            ExecuteSqlResolvedCountMode::Async => {
                if row_count_job_id.is_some() {
                    ExecuteSqlCountExecution::BackgroundQuery
                } else {
                    ExecuteSqlCountExecution::PageWindow
                }
            }
            ExecuteSqlResolvedCountMode::None => ExecuteSqlCountExecution::PageWindow,
        };
        let execution_facts = ExecuteSqlExecutionFacts::from_core(
            ExecuteSqlRequestScope::ExecuteSql,
            &normalized,
            pagination,
            Some(count_execution),
        );
        let result = apply_execute_sql_execution_metadata(result, &execution_facts);
        let result = apply_execute_sql_export_metadata(result, export_meta);
        let result = apply_execute_sql_effective_metadata(
            result,
            effective_profile,
            effective_count_mode,
            metadata_verbosity,
            requested_output_mode,
            self.response_output_mode_auto_tabular.as_output_mode(),
        );
        Ok(apply_pinned_session_meta(
            apply_execute_sql_compaction(result, metadata_verbosity, requested_output_mode),
            page_session_snapshot.as_ref(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::config::{
        AccessMode, MetadataPolicyMode, ResponseAutoTabularMode, ResponseMode, ResponseOutputMode,
        StartupRole,
    };
    use crate::db::DbEngine;
    use rmcp::handler::server::wrapper::Parameters;

    fn live_db_server_from_env() -> Option<PostgresMcp> {
        let database_uri = env::var("DATABASE_URI").ok()?;
        if database_uri.trim().is_empty() {
            return None;
        }
        let mut server = PostgresMcp::with_response_contract(
            Arc::new(DbEngine::new(
                Some(database_uri),
                AccessMode::Unrestricted,
                true,
                Some(Duration::from_secs(15)),
                Some(Duration::from_secs(10)),
                Some(Duration::from_secs(2)),
            )),
            ResponseMode::V2,
            ResponseOutputMode::Auto,
            ResponseAutoTabularMode::Rows,
            200,
        );
        server.startup_role = StartupRole::Migrator;
        Some(server)
    }

    async fn execute_sql_for_test(server: &PostgresMcp, sql: &str) -> serde_json::Value {
        let result = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some(sql.to_string()),
                session_id: None,
                params: None,
                cursor: None,
                max_rows: None,
                max_cell_chars: None,
                output_mode: Some(ResponseOutputMode::Rows),
                response_formatting_mode: None,
                currency_columns: None,
                summary_only: false,
                include_total_row_count: None,
                count_mode: None,
                profile: None,
                metadata_verbosity: Some(ExecuteSqlMetadataVerbosity::Full),
                describe_only: false,
                export_to_file: false,
                export_format: None,
                statement_timeout_ms: None,
                diagnose_on_timeout: None,
                preflight_check: None,
            }))
            .await
            .expect("execute_sql should return tool result");
        result
            .structured_content
            .expect("failed execute_sql should still emit structured content")
    }

    async fn execute_sql_for_test_with_formatting(
        server: &PostgresMcp,
        sql: &str,
        response_formatting_mode: Option<ResponseFormattingMode>,
        currency_columns: Option<Vec<String>>,
    ) -> serde_json::Value {
        let result = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some(sql.to_string()),
                session_id: None,
                params: None,
                cursor: None,
                max_rows: None,
                max_cell_chars: None,
                output_mode: Some(ResponseOutputMode::Rows),
                response_formatting_mode,
                currency_columns,
                summary_only: false,
                include_total_row_count: None,
                count_mode: None,
                profile: None,
                metadata_verbosity: Some(ExecuteSqlMetadataVerbosity::Full),
                describe_only: false,
                export_to_file: false,
                export_format: None,
                statement_timeout_ms: None,
                diagnose_on_timeout: None,
                preflight_check: None,
            }))
            .await
            .expect("execute_sql should return tool result");
        result
            .structured_content
            .expect("failed execute_sql should still emit structured content")
    }

    #[test]
    fn tool_router_hides_execute_sql_by_default_and_keeps_pinned_session_tools() {
        let server = test_server_without_db();
        let tool_names = server.tool_names();
        assert!(!tool_names.iter().any(|name| name == "execute_sql"));
        assert!(tool_names.iter().any(|name| name == "query_start"));
        assert!(tool_names.iter().any(|name| name == "session_open"));
        assert!(tool_names.iter().any(|name| name == "session_status"));
        assert!(tool_names.iter().any(|name| name == "session_close"));
    }

    #[test]
    fn tool_router_exposes_execute_sql_when_enabled() {
        let mut server = test_server_without_db();
        server.expose_execute_sql = true;
        let tool_names = server.tool_names();
        assert!(tool_names.iter().any(|name| name == "execute_sql"));
    }

    #[tokio::test]
    async fn query_start_rejects_session_id() {
        let server = test_server_without_db();
        let payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some("SELECT 1".to_string()),
                    session_id: Some("ps_deadbeef".to_string()),
                    ..ExecuteSqlArgs::default()
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should return a payload")
            .structured_content
            .expect("query_start should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_request")
        );
        assert!(
            error
                .get("error")
                .and_then(Value::as_str)
                .is_some_and(|message| message.contains("does not support session_id"))
        );
    }

    #[tokio::test]
    async fn pinned_session_preserves_temp_tables_across_execute_sql_calls() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping pinned_session_preserves_temp_tables_across_execute_sql_calls (DATABASE_URI not set)"
            );
            return;
        };
        let session_payload = server
            .session_open(Parameters(SessionOpenArgs { idle_ttl_ms: None }))
            .await
            .expect("session_open should return a payload")
            .structured_content
            .expect("session_open should return structured content");
        let session_id = tool_success_payload(&session_payload)
            .get("session_id")
            .and_then(Value::as_str)
            .expect("session_open should return session_id")
            .to_string();

        let create_payload = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some(
                    "CREATE TEMP TABLE postgres_mcp_session_repro_2724 (id int);".to_string(),
                ),
                session_id: Some(session_id.clone()),
                ..ExecuteSqlArgs::default()
            }))
            .await
            .expect("temp-table create should return a payload")
            .structured_content
            .expect("temp-table create should return structured content");
        assert_eq!(
            create_payload
                .pointer("/meta/pinned_session/session_id")
                .and_then(Value::as_str),
            Some(session_id.as_str())
        );

        let _ = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some(
                    "INSERT INTO postgres_mcp_session_repro_2724 (id) VALUES (1), (2);".to_string(),
                ),
                session_id: Some(session_id.clone()),
                ..ExecuteSqlArgs::default()
            }))
            .await
            .expect("temp-table insert should return a payload");

        let count_payload = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some(
                    "SELECT COUNT(*) AS row_count FROM postgres_mcp_session_repro_2724".to_string(),
                ),
                session_id: Some(session_id.clone()),
                output_mode: Some(ResponseOutputMode::Rows),
                ..ExecuteSqlArgs::default()
            }))
            .await
            .expect("temp-table count should return a payload")
            .structured_content
            .expect("temp-table count should return structured content");
        assert_eq!(
            count_payload
                .pointer("/meta/pinned_session/session_id")
                .and_then(Value::as_str),
            Some(session_id.as_str())
        );
        assert_eq!(
            tool_success_payload(&count_payload)
                .pointer("/0/row_count")
                .and_then(Value::as_i64),
            Some(2)
        );

        let _ = server
            .session_close(Parameters(SessionIdArgs {
                session_id: session_id.clone(),
            }))
            .await
            .expect("session_close should return a payload");

        let missing_payload = server
            .session_status(Parameters(SessionIdArgs { session_id }))
            .await
            .expect("session_status should return a payload")
            .structured_content
            .expect("session_status should return structured content");
        assert_eq!(
            tool_error_payload(&missing_payload)
                .get("code")
                .and_then(Value::as_str),
            Some("PINNED_SESSION_NOT_FOUND")
        );
    }

    #[tokio::test]
    async fn execute_sql_preflight_missing_relation_returns_structured_error() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping execute_sql_preflight_missing_relation_returns_structured_error (DATABASE_URI not set)"
            );
            return;
        };
        let payload = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some("SELECT * FROM postgres_mcp_preflight_missing_relation_2518".to_string()),
                session_id: None,
                params: None,
                cursor: None,
                max_rows: None,
                max_cell_chars: None,
                output_mode: None,
                response_formatting_mode: None,
                currency_columns: None,
                summary_only: false,
                include_total_row_count: None,
                count_mode: None,
                profile: None,
                metadata_verbosity: Some(ExecuteSqlMetadataVerbosity::Full),
                describe_only: false,
                export_to_file: false,
                export_format: None,
                statement_timeout_ms: None,
                diagnose_on_timeout: None,
                preflight_check: Some(true),
            }))
            .await
            .expect("execute_sql should return tool result")
            .structured_content
            .expect("execute_sql should return structured payload");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("code").and_then(Value::as_str),
            Some("SQL_PREFLIGHT_MISSING_RELATION")
        );
        assert_eq!(
            error
                .pointer("/preflight/failure_kind")
                .and_then(Value::as_str),
            Some("missing_relation")
        );
        assert_eq!(
            error.pointer("/preflight/enabled").and_then(Value::as_bool),
            Some(true)
        );
    }

    #[tokio::test]
    async fn execute_sql_preflight_with_params_reports_param_aware_query_hash() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping execute_sql_preflight_with_params_reports_param_aware_query_hash (DATABASE_URI not set)"
            );
            return;
        };
        let sql = "SELECT * FROM postgres_mcp_preflight_missing_relation_2518 WHERE id = $1";
        let params = vec![serde_json::json!(7)];
        let payload = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some(sql.to_string()),
                session_id: None,
                params: Some(params.clone()),
                cursor: None,
                max_rows: None,
                max_cell_chars: None,
                output_mode: None,
                response_formatting_mode: None,
                currency_columns: None,
                summary_only: false,
                include_total_row_count: None,
                count_mode: None,
                profile: None,
                metadata_verbosity: Some(ExecuteSqlMetadataVerbosity::Full),
                describe_only: false,
                export_to_file: false,
                export_format: None,
                statement_timeout_ms: None,
                diagnose_on_timeout: None,
                preflight_check: Some(true),
            }))
            .await
            .expect("execute_sql should return tool result")
            .structured_content
            .expect("execute_sql should return structured payload");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error
                .pointer("/preflight/query_hash")
                .and_then(Value::as_str),
            Some(response_page_hash_for_params(sql, &params).as_str())
        );
    }

    #[tokio::test]
    async fn execute_sql_currency_formatting_mode_formats_suffix_and_explicit_columns() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping execute_sql_currency_formatting_mode_formats_suffix_and_explicit_columns (DATABASE_URI not set)"
            );
            return;
        };

        let payload = execute_sql_for_test_with_formatting(
            &server,
            "SELECT 12345 AS price_cents, 800 AS fee",
            Some(ResponseFormattingMode::Currency),
            Some(vec!["fee".to_string()]),
        )
        .await;

        let data = tool_success_payload(&payload);
        let rows = data
            .as_array()
            .expect("successful query should include row array");
        assert_eq!(rows.len(), 1);
        let row = rows.first().expect("expected one formatted row");
        assert_eq!(row.get("price_cents"), Some(&serde_json::json!(12345)));
        assert_eq!(row.get("fee"), Some(&serde_json::json!(800)));
        assert_eq!(
            row.get("price_cents_formatted"),
            Some(&serde_json::json!("123.45"))
        );
        assert_eq!(row.get("fee_formatted"), Some(&serde_json::json!("8.00")));

        let hints = payload
            .pointer("/meta/query_hints")
            .and_then(serde_json::Value::as_array)
            .expect("meta should include query_hints");
        assert!(
            hints.iter().any(|hint| {
                hint.as_str()
                    .is_some_and(|value| value.contains("response formatting mode currency"))
            }),
            "expected currency formatting hint in response meta"
        );
    }

    #[tokio::test]
    async fn execute_sql_currency_formatting_mode_noop_without_mode() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping execute_sql_currency_formatting_mode_noop_without_mode (DATABASE_URI not set)"
            );
            return;
        };

        let payload = execute_sql_for_test_with_formatting(
            &server,
            "SELECT 12345 AS price_cents",
            None,
            Some(vec!["price_cents".to_string()]),
        )
        .await;

        let data = tool_success_payload(&payload);
        let row = data
            .as_array()
            .and_then(|rows| rows.first())
            .expect("successful query should include row array");
        assert!(row.get("price_cents_formatted").is_none());
    }

    fn tool_error_payload(payload: &serde_json::Value) -> serde_json::Value {
        if payload
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .is_some_and(|ok| !ok)
        {
            return payload
                .get("error")
                .cloned()
                .unwrap_or_else(|| payload.clone());
        }
        payload.clone()
    }

    fn tool_success_payload(payload: &serde_json::Value) -> serde_json::Value {
        if payload
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .is_some_and(|ok| !ok)
        {
            panic!("expected successful payload but got error: {payload:?}");
        }
        if payload
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .is_some_and(|ok| ok)
        {
            return payload
                .get("data")
                .cloned()
                .unwrap_or_else(|| payload.clone());
        }
        payload.clone()
    }

    #[test]
    fn execute_sql_output_mode_accepts_canonical_modes() {
        let cases = [
            ("auto", ResponseOutputMode::Auto),
            ("rows", ResponseOutputMode::Rows),
            ("rows_safe", ResponseOutputMode::RowsSafe),
            ("tuples", ResponseOutputMode::Tuples),
            ("scalar", ResponseOutputMode::Scalar),
            ("data_only", ResponseOutputMode::DataOnly),
        ];

        for (raw_mode, expected) in cases {
            let parsed = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
                "sql": "SELECT 1",
                "output_mode": raw_mode
            }))
            .expect("canonical output_mode value should deserialize");
            assert_eq!(parsed.output_mode, Some(expected));
        }
    }

    #[test]
    fn execute_sql_output_mode_accepts_table_alias() {
        let parsed = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT 1",
            "output_mode": "table"
        }))
        .expect("table alias should deserialize");
        assert_eq!(parsed.output_mode, Some(ResponseOutputMode::Rows));
    }

    #[test]
    fn execute_sql_output_mode_accepts_json_alias() {
        let parsed = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT 1",
            "output_mode": "json"
        }))
        .expect("json alias should deserialize");
        assert_eq!(parsed.output_mode, Some(ResponseOutputMode::Rows));
    }

    #[test]
    fn execute_sql_args_accept_minimal_sql_only_payload() {
        let parsed = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT 1"
        }))
        .expect("sql-only payload should deserialize");
        assert_eq!(parsed.sql.as_deref(), Some("SELECT 1"));
        assert_eq!(parsed.max_rows, None);
        assert_eq!(parsed.output_mode, None);
        assert_eq!(parsed.profile, None);
        assert_eq!(parsed.metadata_verbosity, None);
    }

    #[test]
    fn execute_sql_output_mode_rejects_unknown_values_with_actionable_error() {
        let err = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT 1",
            "output_mode": "grid"
        }))
        .expect_err("unexpected output_mode must fail deserialization");
        let message = err.to_string();
        assert!(message.contains(
            "output_mode must be one of [auto, rows, rows_safe, tuples, scalar, data_only]"
        ));
        assert!(message.contains("aliases: table -> rows, json -> rows"));
        assert!(message.contains("{\"output_mode\":\"auto\"}"));
    }

    #[test]
    fn execute_sql_output_mode_rejects_legacy_compact_with_data_only_hint() {
        let err = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT 1",
            "output_mode": "compact"
        }))
        .expect_err("legacy compact output_mode must fail deserialization");
        let message = err.to_string();
        assert!(message.contains("use data_only"));
        assert!(message.contains("data_only"));
    }

    #[test]
    fn execute_sql_metadata_verbosity_accepts_low_alias() {
        let parsed = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT 1",
            "metadata_verbosity": "low"
        }))
        .expect("low metadata_verbosity alias should deserialize");
        assert_eq!(
            parsed.metadata_verbosity,
            Some(ExecuteSqlMetadataVerbosity::Compact)
        );
    }

    #[test]
    fn execute_sql_metadata_verbosity_rejects_unknown_values_with_actionable_error() {
        let err = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT 1",
            "metadata_verbosity": "verbose"
        }))
        .expect_err("unexpected metadata_verbosity must fail deserialization");
        let message = err.to_string();
        assert!(message.contains("metadata_verbosity must be one of [compact, standard, full]"));
        assert!(message.contains("alias: low -> compact"));
        assert!(message.contains("{\"metadata_verbosity\":\"compact\"}"));
    }

    #[test]
    fn execute_sql_response_formatting_mode_accepts_currency() {
        let parsed = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT 1",
            "response_formatting_mode": "currency"
        }))
        .expect("currency response_formatting_mode should deserialize");
        assert_eq!(
            parsed.response_formatting_mode,
            Some(ResponseFormattingMode::Currency)
        );
    }

    #[test]
    fn execute_sql_response_formatting_mode_rejects_compact_with_guidance() {
        let err = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT 1",
            "response_formatting_mode": "compact"
        }))
        .expect_err("compact response_formatting_mode must fail deserialization");
        let message = err.to_string();
        assert!(message.contains(
            "response_formatting_mode supports `currency` and compatibility alias `markdown`"
        ));
        assert!(message.contains("metadata_verbosity=compact"));
        assert!(message.contains("output_mode=data_only"));
        assert!(message.contains("profile=fast_agent"));
    }

    #[test]
    fn execute_sql_response_formatting_mode_accepts_markdown_alias() {
        let parsed = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT 1",
            "response_formatting_mode": "markdown"
        }))
        .expect("markdown response_formatting_mode should deserialize as compatibility alias");
        assert_eq!(
            parsed.response_formatting_mode,
            Some(ResponseFormattingMode::Markdown)
        );
    }

    #[test]
    fn normalize_execute_sql_output_preferences_maps_markdown_to_rows() {
        let (output_mode, formatting_mode, hint, emit_markdown_text) =
            normalize_execute_sql_output_preferences(None, Some(ResponseFormattingMode::Markdown));
        assert_eq!(output_mode, Some(ResponseOutputMode::Rows));
        assert_eq!(formatting_mode, None);
        assert_eq!(
            hint,
            Some("response_formatting_mode=markdown normalized to output_mode=table")
        );
        assert!(emit_markdown_text);
    }

    #[test]
    fn execute_sql_args_accept_params_export_and_describe_flags() {
        let parsed = serde_json::from_value::<ExecuteSqlArgs>(serde_json::json!({
            "sql": "SELECT * FROM providers WHERE id = ANY($1)",
            "params": [[1, 2, 3]],
            "describe_only": true,
            "export_to_file": true,
            "export_format": "csv"
        }))
        .expect("execute_sql args should deserialize extended ergonomics fields");
        assert_eq!(parsed.params, Some(vec![serde_json::json!([1, 2, 3])]));
        assert!(parsed.describe_only);
        assert!(parsed.export_to_file);
        assert_eq!(parsed.export_format, Some(ExecuteSqlExportFormat::Csv));
    }

    #[test]
    fn rewrite_latest_snapshot_global_without_partitions() {
        let query = "SELECT * FROM latest_snapshot(source => 'public.events', ts_column => 'snapshot_ts') AS latest";
        let rewrite =
            rewrite_latest_snapshot_helpers(query).expect("helper rewrite should succeed");
        assert_eq!(rewrite.helper_count, 1);
        assert!(rewrite.sql.contains(
            "FROM (SELECT \"_ls_source\".* FROM \"public\".\"events\" AS \"_ls_source\""
        ));
        assert!(
            rewrite
                .sql
                .contains("WHERE \"_ls_source\".\"snapshot_ts\" IS NOT NULL")
        );
        assert!(rewrite.sql.contains(
            "ORDER BY \"_ls_source\".\"snapshot_ts\" DESC NULLS LAST, to_jsonb(\"_ls_source\")::text"
        ));
        assert!(rewrite.sql.contains("LIMIT 1"));
        assert!(!rewrite.sql.contains("DISTINCT ON"));
    }

    #[test]
    fn rewrite_latest_snapshot_partitioned_and_tie_breakers() {
        let query = "SELECT * FROM latest_snapshot(
            source => 'public.events',
            ts_column => 'snapshot_ts',
            partition_by => ARRAY['tenant_id'],
            tie_breakers => ARRAY['event_id'],
            include_null_timestamps => false,
            nulls_first => false
        ) AS latest";
        let rewrite =
            rewrite_latest_snapshot_helpers(query).expect("helper rewrite should succeed");
        assert_eq!(rewrite.helper_count, 1);
        assert!(
            rewrite
                .sql
                .contains("DISTINCT ON (\"_ls_source\".\"tenant_id\")")
        );
        assert!(rewrite
            .sql
            .contains("ORDER BY \"_ls_source\".\"tenant_id\", \"_ls_source\".\"snapshot_ts\" DESC NULLS LAST, \"_ls_source\".\"event_id\", to_jsonb(\"_ls_source\")::text"));
        assert!(
            rewrite
                .sql
                .contains("WHERE \"_ls_source\".\"snapshot_ts\" IS NOT NULL")
        );
        assert!(!rewrite.sql.contains("LIMIT 1"));
    }

    #[test]
    fn rewrite_latest_snapshot_parser_ignores_strings_and_comments() {
        let query = "SELECT 'latest_snapshot(source => ''public.events'', ts_column => ''snapshot_ts'')' AS label -- latest_snapshot() in comment";
        let rewrite =
            rewrite_latest_snapshot_helpers(query).expect("helper rewrite should succeed");
        assert_eq!(rewrite.helper_count, 0);
        assert_eq!(rewrite.sql, query);
    }

    #[test]
    fn parse_latest_snapshot_missing_source_errors() {
        let query = "SELECT * FROM latest_snapshot(ts_column => 'snapshot_ts') AS latest";
        let err = rewrite_latest_snapshot_helpers(query).expect_err("missing source should error");
        assert!(err.contains("requires argument 'source'"));
    }

    #[test]
    fn parse_latest_snapshot_invalid_bool_argument_errors() {
        let query = "SELECT * FROM latest_snapshot(
            source => 'public.events',
            ts_column => 'snapshot_ts',
            include_null_timestamps => 'maybe'
        ) AS latest";
        let err = rewrite_latest_snapshot_helpers(query).expect_err("invalid boolean should fail");
        assert!(err.contains("invalid boolean value"));
    }

    #[test]
    fn rewrite_latest_snapshot_leaves_scalar_function_usage_untouched() {
        let query = "SELECT latest_snapshot(source => 'public.events', ts_column => 'snapshot_ts') AS helper_value";
        let rewrite =
            rewrite_latest_snapshot_helpers(query).expect("scalar helper usage should be ignored");
        assert_eq!(rewrite.helper_count, 0);
        assert_eq!(rewrite.sql, query);
    }

    #[test]
    fn rewrite_latest_snapshot_allows_line_comments_between_arguments() {
        let query = "SELECT * FROM latest_snapshot(
            source => 'public.events', -- keep latest rows
            ts_column => 'snapshot_ts'
        ) AS latest";
        let rewrite =
            rewrite_latest_snapshot_helpers(query).expect("line comments in args should parse");
        assert_eq!(rewrite.helper_count, 1);
        assert!(rewrite.sql.contains("\"_ls_source\".\"snapshot_ts\""));
    }

    #[test]
    fn rewrite_latest_snapshot_allows_block_comment_between_from_and_helper() {
        let query = "SELECT * FROM /* latest rows */ latest_snapshot(source => 'public.events', ts_column => 'snapshot_ts') AS latest";
        let rewrite = rewrite_latest_snapshot_helpers(query)
            .expect("block comments before helper should still allow rewrite");
        assert_eq!(rewrite.helper_count, 1);
        assert!(rewrite.sql.contains("\"_ls_source\".\"snapshot_ts\""));
    }

    #[test]
    fn rewrite_latest_snapshot_allows_touching_block_comment_between_from_and_helper() {
        let query = "SELECT * FROM/* latest rows */latest_snapshot(source => 'public.events', ts_column => 'snapshot_ts') AS latest";
        let rewrite = rewrite_latest_snapshot_helpers(query)
            .expect("touching block comments should preserve relation token boundaries");
        assert_eq!(rewrite.helper_count, 1);
        assert!(rewrite.sql.contains("\"_ls_source\".\"snapshot_ts\""));
    }

    #[test]
    fn rewrite_latest_snapshot_allows_nested_block_comments_between_from_and_helper() {
        let query = "SELECT * FROM /* outer /* inner */ still outer */ latest_snapshot(source => 'public.events', ts_column => 'snapshot_ts') AS latest";
        let rewrite = rewrite_latest_snapshot_helpers(query)
            .expect("nested block comments before helper should still allow rewrite");
        assert_eq!(rewrite.helper_count, 1);
        assert!(rewrite.sql.contains("\"_ls_source\".\"snapshot_ts\""));
    }

    #[test]
    fn rewrite_latest_snapshot_allows_line_comment_between_from_and_helper() {
        let query = "SELECT * FROM -- latest rows\nlatest_snapshot(source => 'public.events', ts_column => 'snapshot_ts') AS latest";
        let rewrite = rewrite_latest_snapshot_helpers(query)
            .expect("line comments before helper should still allow rewrite");
        assert_eq!(rewrite.helper_count, 1);
        assert!(rewrite.sql.contains("\"_ls_source\".\"snapshot_ts\""));
    }

    #[test]
    fn rewrite_latest_snapshot_ignores_dollar_quote_comments() {
        let query = "SELECT * FROM $$-- latest_snapshot(source => 'public.events', ts_column => 'snapshot_ts')$$, latest_snapshot(source => 'public.events', ts_column => 'snapshot_ts') AS latest";
        let rewrite = rewrite_latest_snapshot_helpers(query)
            .expect("dollar-quoted literals should not disrupt comment stripping");
        assert_eq!(rewrite.helper_count, 1);
        assert!(rewrite.sql.contains("\"_ls_source\".\"snapshot_ts\""));
    }

    #[test]
    fn rewrite_latest_snapshot_allows_comma_joined_relation_position() {
        let query = "SELECT * FROM public.base_table AS b, latest_snapshot(source => 'public.events', ts_column => 'snapshot_ts') AS latest";
        let rewrite = rewrite_latest_snapshot_helpers(query)
            .expect("comma-joined relation context should still allow rewrite");
        assert_eq!(rewrite.helper_count, 1);
        assert!(rewrite.sql.contains(", (SELECT \"_ls_source\".* FROM"));
    }

    #[test]
    fn find_matching_bracket_ignores_brackets_inside_single_quoted_array_items() {
        let input = "ARRAY['tenant]id', 'event_id']";
        let open = input.find('[').expect("array must contain opening bracket");
        let close = find_matching_bracket(input, open)
            .expect("matcher should skip ] inside single-quoted array items");
        assert_eq!(
            close,
            input
                .rfind(']')
                .expect("array must contain closing bracket")
        );
    }

    #[test]
    fn parse_missing_identifiers_from_error_messages() {
        assert_eq!(
            parse_missing_relation_kind(
                "query execution failed: relation \"mobile_coverage_latest\" does not exist"
            ),
            Some(MissingRelationKind::MissingRelation(
                "mobile_coverage_latest".to_string()
            ))
        );
        assert_eq!(
            parse_missing_relation_kind(
                "query execution failed: missing FROM-clause entry for table \"missing_alias\""
            ),
            Some(MissingRelationKind::MissingFromAlias(
                "missing_alias".to_string()
            ))
        );
        assert_eq!(
            parse_missing_column_name(
                "query execution failed: column \"provider_slug\" does not exist"
            ),
            Some("provider_slug".to_string())
        );
    }

    #[test]
    fn runtime_role_ddl_guard_detects_schema_changing_statements() {
        assert!(is_runtime_blocked_ddl("CREATE TABLE demo(id int)"));
        assert!(is_runtime_blocked_ddl("alter table demo add column n int"));
        assert!(is_runtime_blocked_ddl("DROP VIEW IF EXISTS demo"));
        assert!(!is_runtime_blocked_ddl("select * from demo"));
        assert!(!is_runtime_blocked_ddl("insert into demo(id) values (1)"));
    }

    #[test]
    fn extract_relation_refs_scans_from_and_join_relations() {
        let refs = extract_relation_refs(
            "SELECT * FROM public.mobile_coverage m JOIN reporting.latest_extract AS le ON m.id = le.id",
        );
        assert!(
            refs.contains(&RelationRef {
                schema: Some("public".to_string()),
                name: "mobile_coverage".to_string(),
            }),
            "expected mobile_coverage relation to be extracted"
        );
        assert!(
            refs.contains(&RelationRef {
                schema: Some("reporting".to_string()),
                name: "latest_extract".to_string(),
            }),
            "expected latest_extract relation to be extracted"
        );
    }

    #[test]
    fn discovery_schema_prefers_missing_relation_schema() {
        let schema = discovery_schema_for_sql(
            "SELECT * FROM public.mobile_coverage m",
            "reporting.coverage_latest",
        );
        assert_eq!(schema, "reporting");
    }

    #[test]
    fn discovery_schema_falls_back_to_first_scoped_relation() {
        let schema =
            discovery_schema_for_sql("SELECT * FROM reporting.coverage_latest c", "coverage_now");
        assert_eq!(schema, "reporting");
    }

    #[test]
    fn discovery_name_like_uses_base_relation_name() {
        assert_eq!(
            discovery_name_like_for_relation("reporting.coverage_latest"),
            "coverage_latest"
        );
    }

    #[test]
    fn resolve_execute_sql_statement_timeout_override_accepts_valid_value() {
        let timeout = resolve_execute_sql_statement_timeout_override(Some(25_000))
            .expect("valid statement timeout should parse");
        assert_eq!(timeout.map(|value| value.as_millis()), Some(25_000));
    }

    #[test]
    fn normalize_execute_sql_core_preserves_raw_and_rewritten_sql_forms() {
        let raw_sql =
            "SELECT * FROM latest_snapshot(source => 'public.events', ts_column => 'snapshot_ts')";
        let normalized = normalize_execute_sql_core(
            Some(raw_sql.to_string()),
            Some(vec![json!(1)]),
            None,
            false,
            false,
            Some(std::time::Duration::from_secs(1)),
        )
        .expect("normalization should succeed");

        let ExecuteSqlNormalizationStage::Ready(core) = normalized else {
            panic!("expected ready normalization stage");
        };
        assert_eq!(core.sql.raw, raw_sql);
        assert_ne!(core.sql.rewritten, core.sql.raw);
        assert!(core.sql.rewritten_from_input());
        assert_eq!(core.sql.helper_count, 1);
        assert_eq!(core.params, vec![json!(1)]);
        assert_eq!(
            core.query_hash,
            response_page_hash_for_params(&core.sql.rewritten, &core.params)
        );
    }

    #[test]
    fn resolve_execute_sql_count_mode_defaults_to_none() {
        let (mode, hints) = resolve_execute_sql_count_mode(None, None);
        assert_eq!(mode, ExecuteSqlResolvedCountMode::None);
        assert!(hints.is_empty());
    }

    #[test]
    fn resolve_execute_sql_count_mode_maps_legacy_include_total_row_count_true() {
        let (mode, hints) = resolve_execute_sql_count_mode(None, Some(true));
        assert_eq!(mode, ExecuteSqlResolvedCountMode::Exact);
        assert!(
            hints
                .iter()
                .any(|hint| hint.contains("mapped to count_mode=exact"))
        );
    }

    #[test]
    fn resolve_execute_sql_count_mode_prefers_explicit_count_mode_over_legacy_flag() {
        let (mode, hints) =
            resolve_execute_sql_count_mode(Some(ExecuteSqlCountMode::Estimated), Some(false));
        assert_eq!(mode, ExecuteSqlResolvedCountMode::Estimated);
        assert!(
            hints
                .iter()
                .any(|hint| hint.contains("overrides include_total_row_count=false"))
        );
    }

    #[test]
    fn resolve_execute_sql_profile_fast_agent_applies_low_overhead_defaults() {
        let resolution = resolve_execute_sql_profile(
            Some(ExecuteSqlProfile::FastAgent),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            ResponseOutputMode::Auto,
            200,
        );
        assert_eq!(
            resolution.effective_profile,
            Some(ExecuteSqlProfile::FastAgent)
        );
        assert_eq!(resolution.output_mode, ResponseOutputMode::DataOnly);
        assert_eq!(resolution.page_size, PROFILE_FAST_AGENT_PAGE_SIZE_CAP);
        assert_eq!(
            resolution.max_cell_chars,
            Some(PROFILE_FAST_AGENT_MAX_CELL_CHARS)
        );
        assert_eq!(resolution.requested_count_mode, None);
        assert!(!resolution.preflight_check);
        assert!(
            resolution
                .profile_hints
                .iter()
                .any(|hint| hint.contains("profile=fast_agent")),
            "profile defaults should emit operator-visible hints"
        );
    }

    #[test]
    fn resolve_execute_sql_profile_human_debug_defaults_count_mode_and_preflight() {
        let resolution = resolve_execute_sql_profile(
            Some(ExecuteSqlProfile::HumanDebug),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            ResponseOutputMode::Auto,
            200,
        );
        assert_eq!(resolution.output_mode, ResponseOutputMode::RowsSafe);
        assert_eq!(
            resolution.requested_count_mode,
            Some(ExecuteSqlCountMode::Estimated)
        );
        assert_eq!(
            resolution.metadata_verbosity,
            ExecuteSqlMetadataVerbosity::Full
        );
        assert!(resolution.preflight_check);
        assert!(
            resolution
                .profile_hints
                .iter()
                .any(|hint| hint.contains("profile=human_debug")),
            "human_debug defaults should emit operator-visible hints"
        );
    }

    #[test]
    fn resolve_execute_sql_profile_human_debug_legacy_include_total_row_count_overrides_default() {
        let true_resolution = resolve_execute_sql_profile(
            Some(ExecuteSqlProfile::HumanDebug),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(true),
            ResponseOutputMode::Auto,
            200,
        );
        assert_eq!(true_resolution.requested_count_mode, None);
        let (resolved_count_mode, count_mode_hints) =
            resolve_execute_sql_count_mode(true_resolution.requested_count_mode, Some(true));
        assert_eq!(resolved_count_mode, ExecuteSqlResolvedCountMode::Exact);
        assert!(count_mode_hints.iter().any(|hint| {
            hint.contains("legacy include_total_row_count=true mapped to count_mode=exact")
        }));

        let false_resolution = resolve_execute_sql_profile(
            Some(ExecuteSqlProfile::HumanDebug),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(false),
            ResponseOutputMode::Auto,
            200,
        );
        assert_eq!(false_resolution.requested_count_mode, None);
        let (resolved_count_mode, count_mode_hints) =
            resolve_execute_sql_count_mode(false_resolution.requested_count_mode, Some(false));
        assert_eq!(resolved_count_mode, ExecuteSqlResolvedCountMode::None);
        assert!(count_mode_hints.is_empty());
    }

    #[test]
    fn resolve_execute_sql_profile_heavy_view_respects_explicit_overrides() {
        let resolution = resolve_execute_sql_profile(
            Some(ExecuteSqlProfile::HeavyView),
            Some(ResponseOutputMode::Rows),
            Some(175),
            Some(64),
            Some(ExecuteSqlCountMode::Exact),
            Some(ExecuteSqlMetadataVerbosity::Full),
            Some(90_000),
            Some(false),
            None,
            ResponseOutputMode::Auto,
            200,
        );
        assert_eq!(resolution.output_mode, ResponseOutputMode::Rows);
        assert_eq!(resolution.page_size, 175);
        assert_eq!(resolution.max_cell_chars, Some(64));
        assert_eq!(
            resolution.requested_count_mode,
            Some(ExecuteSqlCountMode::Exact)
        );
        assert_eq!(
            resolution.metadata_verbosity,
            ExecuteSqlMetadataVerbosity::Full
        );
        assert_eq!(resolution.statement_timeout_ms, Some(90_000));
        assert!(!resolution.preflight_check);
    }

    #[test]
    fn next_offset_for_exact_count_returns_none_for_empty_tail_page() {
        assert_eq!(next_offset_for_exact_count(200, 0, 250), None);
    }

    #[test]
    fn next_offset_for_exact_count_advances_until_terminal_page() {
        assert_eq!(next_offset_for_exact_count(0, 25, 100), Some(25));
        assert_eq!(next_offset_for_exact_count(75, 25, 100), None);
    }

    #[test]
    fn next_offset_for_page_window_uses_page_size_progression() {
        assert_eq!(next_offset_for_page_window(50, 26, 25), Some(75));
        assert_eq!(next_offset_for_page_window(50, 25, 25), None);
    }

    #[test]
    fn apply_execute_sql_count_metadata_sets_mode_and_async_job_id() {
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [],
            "meta": {
                "row_count_mode": "page_window"
            }
        }));
        let payload =
            apply_execute_sql_count_metadata(result, "count_async", Some("qj_0000000000000001"))
                .structured_content
                .expect("structured payload should remain available");
        assert_eq!(
            payload
                .pointer("/meta/row_count_mode")
                .and_then(serde_json::Value::as_str),
            Some("count_async")
        );
        assert_eq!(
            payload
                .pointer("/meta/row_count_job_id")
                .and_then(serde_json::Value::as_str),
            Some("qj_0000000000000001")
        );
    }

    #[test]
    fn apply_execute_sql_effective_metadata_sets_canonical_effective_fields() {
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [],
            "meta": {
                "output_mode": "tuples",
                "metadata_verbosity": "compact"
            }
        }));
        let payload = apply_execute_sql_effective_metadata(
            result,
            Some(ExecuteSqlProfile::FastAgent),
            ExecuteSqlResolvedCountMode::Estimated,
            ExecuteSqlMetadataVerbosity::Compact,
            ResponseOutputMode::Auto,
            ResponseOutputMode::RowsSafe,
        )
        .structured_content
        .expect("structured payload should remain available");
        assert_eq!(
            payload
                .pointer("/meta/effective_profile")
                .and_then(serde_json::Value::as_str),
            Some("fast_agent")
        );
        assert_eq!(
            payload
                .pointer("/meta/effective_count_mode")
                .and_then(serde_json::Value::as_str),
            Some("estimated")
        );
        assert_eq!(
            payload
                .pointer("/meta/effective_output_mode")
                .and_then(serde_json::Value::as_str),
            Some("tuples")
        );
        assert_eq!(
            payload
                .pointer("/meta/effective_metadata_verbosity")
                .and_then(serde_json::Value::as_str),
            Some("compact")
        );
        assert_eq!(
            payload
                .pointer("/meta/requested_output_mode")
                .and_then(serde_json::Value::as_str),
            Some("auto")
        );
        assert_eq!(
            payload
                .pointer("/meta/auto_output_resolution/tabular_default")
                .and_then(serde_json::Value::as_str),
            Some("rows_safe")
        );
    }

    #[test]
    fn apply_execute_sql_execution_metadata_inserts_nested_fact_block() {
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [],
            "meta": {
                "query_hash": "84c44961e673fe5d"
            }
        }));
        let execution_facts = ExecuteSqlExecutionFacts {
            scope: ExecuteSqlRequestScope::ExecuteSql,
            statement_kind: "select".to_string(),
            sql_rewritten: true,
            helper_expansions: 1,
            bound_param_count: 2,
            statement_timeout_override_applied: true,
            pagination_strategy: ExecuteSqlPaginationStrategy::Offset,
            count_execution: Some(ExecuteSqlCountExecution::PageWindow),
        };

        let payload = apply_execute_sql_execution_metadata(result, &execution_facts)
            .structured_content
            .expect("structured payload should remain available");

        assert_eq!(
            payload
                .pointer("/meta/execution/contract_version")
                .and_then(Value::as_str),
            Some("execution/v1")
        );
        assert_eq!(
            payload
                .pointer("/meta/execution/sql/statement_kind")
                .and_then(Value::as_str),
            Some("select")
        );
        assert_eq!(
            payload
                .pointer("/meta/execution/pagination/strategy")
                .and_then(Value::as_str),
            Some("offset")
        );
        assert_eq!(
            payload
                .pointer("/meta/execution/count/execution")
                .and_then(Value::as_str),
            Some("page_window")
        );
    }

    #[test]
    fn apply_execute_sql_data_only_compaction_reduces_meta_surface() {
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [[1, "x"]],
            "meta": {
                "output_mode": "data_only",
                "query_hash": "84c44961e673fe5d",
                "elapsed_ms": 12,
                "truncated": false,
                "returned_rows": 1,
                "has_more": false,
                "next_cursor": null,
                "row_count_mode": "page_window",
                "row_count_total": 1,
                "capabilities": {"startup_degraded_read_only": false},
                "execution": {"contract_version": "execution/v1"},
                "columns": [{"name": "id", "pg_type": "int4"}],
                "query_hints": ["hint"],
                "effective_profile": "fast_agent"
            }
        }));

        let payload = apply_execute_sql_data_only_compaction(result, ResponseOutputMode::DataOnly)
            .structured_content
            .expect("structured payload should remain available");
        let meta = payload
            .get("meta")
            .and_then(Value::as_object)
            .expect("meta object must remain present");
        assert_eq!(
            meta.get("output_mode").and_then(Value::as_str),
            Some("data_only")
        );
        assert!(meta.get("query_hash").is_some());
        assert!(meta.get("elapsed_ms").is_some());
        assert!(meta.get("truncated").is_some());
        assert!(meta.get("returned_rows").is_some());
        assert!(meta.get("has_more").is_some());
        assert!(meta.get("next_cursor").is_some());
        assert_eq!(
            meta.get("row_count_mode").and_then(Value::as_str),
            Some("page_window")
        );
        assert!(
            meta.get("row_count_total").is_none(),
            "page-window totals are redundant in compact mode"
        );
        assert!(
            meta.get("capabilities").is_none(),
            "healthy startup capabilities should be omitted from compact data_only responses"
        );
        assert!(
            meta.get("execution").is_none(),
            "execution facts should be omitted from compact data_only responses"
        );
        assert_eq!(meta.len(), 8, "data_only meta should stay tightly bounded");
    }

    #[test]
    fn apply_execute_sql_data_only_compaction_preserves_abnormal_capabilities() {
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [],
            "meta": {
                "output_mode": "data_only",
                "query_hash": "84c44961e673fe5d",
                "elapsed_ms": 12,
                "truncated": false,
                "returned_rows": 0,
                "has_more": false,
                "next_cursor": null,
                "row_count_mode": "page_window",
                "capabilities": {
                    "startup_state": "degraded_read_only",
                    "degraded_read_only": true,
                    "read_only_sql": true,
                    "read_write_sql": false,
                    "metadata_discovery": true,
                    "reason": "missing relation",
                    "missing_dependencies": ["public.offer_history"]
                }
            }
        }));

        let payload = apply_execute_sql_data_only_compaction(result, ResponseOutputMode::DataOnly)
            .structured_content
            .expect("structured payload should remain available");
        assert_eq!(
            payload
                .pointer("/meta/capabilities/startup_state")
                .and_then(Value::as_str),
            Some("degraded_read_only")
        );
    }

    #[test]
    fn apply_execute_sql_data_only_compaction_preserves_count_metadata() {
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [[1, "x"]],
            "meta": {
                "output_mode": "data_only",
                "query_hash": "84c44961e673fe5d",
                "elapsed_ms": 12,
                "truncated": false,
                "returned_rows": 1,
                "has_more": false,
                "next_cursor": null,
                "capabilities": {"startup_degraded_read_only": false},
                "row_count_mode": "count_async",
                "row_count_total": 100,
                "row_count_job_id": "qj_0000000000000042"
            }
        }));

        let payload = apply_execute_sql_data_only_compaction(result, ResponseOutputMode::DataOnly)
            .structured_content
            .expect("structured payload should remain available");
        let meta = payload
            .get("meta")
            .and_then(Value::as_object)
            .expect("meta object must remain present");
        assert_eq!(
            meta.get("row_count_mode").and_then(Value::as_str),
            Some("count_async")
        );
        assert_eq!(
            meta.get("row_count_total").and_then(Value::as_u64),
            Some(100)
        );
        assert_eq!(
            meta.get("row_count_job_id").and_then(Value::as_str),
            Some("qj_0000000000000042")
        );
    }

    #[test]
    fn apply_execute_sql_data_only_compaction_preserves_export_metadata() {
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [[1, "x"]],
            "meta": {
                "output_mode": "data_only",
                "query_hash": "84c44961e673fe5d",
                "elapsed_ms": 12,
                "truncated": false,
                "returned_rows": 1,
                "has_more": false,
                "next_cursor": null,
                "capabilities": {"startup_degraded_read_only": false},
                "export": {
                    "enabled": true,
                    "format": "tsv",
                    "path": "/tmp/postgres-mcp-test.tsv"
                }
            }
        }));

        let payload = apply_execute_sql_data_only_compaction(result, ResponseOutputMode::DataOnly)
            .structured_content
            .expect("structured payload should remain available");
        assert_eq!(
            payload.pointer("/meta/export/path").and_then(Value::as_str),
            Some("/tmp/postgres-mcp-test.tsv")
        );
    }

    #[test]
    fn validate_execute_sql_bound_params_rejects_multi_statement_sql() {
        let err = validate_execute_sql_bound_params("SELECT 1; SELECT 2", &[serde_json::json!(1)])
            .expect_err("multi-statement bound params should be rejected");
        assert!(err.contains("exactly one SQL statement"));
    }

    #[test]
    fn write_query_output_export_emits_temp_file_metadata() {
        let output = crate::db::QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "provider".to_string(),
                serde_json::json!("aldi"),
            )])],
            columns: vec![crate::db::QueryColumn {
                name: "provider".to_string(),
                pg_type: "text".to_string(),
                nullable: Some(true),
            }],
            rows_affected: None,
        };

        let export =
            write_query_output_export(&output, "84c44961e673fe5d", ExecuteSqlExportFormat::Csv)
                .expect("export should succeed");
        let path = export
            .get("path")
            .and_then(Value::as_str)
            .expect("export path should be present")
            .to_string();
        let written = std::fs::read_to_string(&path).expect("export file should exist");
        assert!(written.contains("provider"));
        assert!(written.contains("aldi"));
        std::fs::remove_file(path).expect("export file should be removable");
    }

    #[test]
    fn write_query_output_export_rejects_invalid_hash_path_components() {
        let output = crate::db::QueryOutput {
            rows: Vec::new(),
            columns: Vec::new(),
            rows_affected: None,
        };

        let err = write_query_output_export(&output, "../bad", ExecuteSqlExportFormat::Csv)
            .expect_err("invalid hash should be rejected");
        assert!(err.contains("query hash"));
    }

    #[test]
    fn write_query_output_export_paths_are_unique() {
        let output = crate::db::QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "provider".to_string(),
                serde_json::json!("aldi"),
            )])],
            columns: vec![crate::db::QueryColumn {
                name: "provider".to_string(),
                pg_type: "text".to_string(),
                nullable: Some(true),
            }],
            rows_affected: None,
        };

        let mut paths = Vec::new();
        for _ in 0..25 {
            let export =
                write_query_output_export(&output, "84c44961e673fe5d", ExecuteSqlExportFormat::Csv)
                    .expect("export should succeed");
            let path = export
                .get("path")
                .and_then(Value::as_str)
                .expect("export path should be present")
                .to_string();
            assert!(
                !paths.contains(&path),
                "export path unexpectedly duplicated: {path}"
            );
            paths.push(path);
        }

        for path in paths {
            std::fs::remove_file(path).expect("export file should be removable");
        }
    }

    #[cfg(unix)]
    #[test]
    fn write_query_output_export_uses_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let output = crate::db::QueryOutput {
            rows: vec![serde_json::Map::from_iter([(
                "provider".to_string(),
                serde_json::json!("aldi"),
            )])],
            columns: vec![crate::db::QueryColumn {
                name: "provider".to_string(),
                pg_type: "text".to_string(),
                nullable: Some(true),
            }],
            rows_affected: None,
        };

        let export =
            write_query_output_export(&output, "84c44961e673fe5d", ExecuteSqlExportFormat::Csv)
                .expect("export should succeed");
        let path = export
            .get("path")
            .and_then(Value::as_str)
            .expect("export path should be present")
            .to_string();
        let metadata = std::fs::metadata(&path).expect("export file should exist");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        std::fs::remove_file(path).expect("export file should be removable");
    }

    #[test]
    fn apply_execute_sql_query_telemetry_compact_is_bounded() {
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [],
            "meta": {
                "elapsed_ms": 12,
                "returned_rows": 3,
                "row_count_mode": "page_window",
                "row_count_total": 3,
                "has_more": false,
                "cursor_offset": 0,
                "next_offset": null
            }
        }));
        let payload = apply_execute_sql_query_telemetry(
            result,
            "84c44961e673fe5d",
            "qf_1234567890abcdef",
            ExecuteSqlMetadataVerbosity::Compact,
        )
        .structured_content
        .expect("structured payload should remain available");

        assert_eq!(
            payload
                .pointer("/meta/query_telemetry/query_hash")
                .and_then(serde_json::Value::as_str),
            Some("84c44961e673fe5d")
        );
        assert_eq!(
            payload
                .pointer("/meta/query_telemetry/query_fingerprint")
                .and_then(serde_json::Value::as_str),
            Some("qf_1234567890abcdef")
        );
        assert_eq!(
            payload
                .pointer("/meta/query_telemetry/returned_rows")
                .and_then(serde_json::Value::as_u64),
            Some(3)
        );
        assert!(
            payload
                .pointer("/meta/query_telemetry/row_count_total")
                .is_none(),
            "compact telemetry should stay bounded"
        );
    }

    #[test]
    fn apply_execute_sql_query_telemetry_full_includes_cursor_context() {
        let result = CallToolResult::structured(json!({
            "ok": true,
            "data": [],
            "meta": {
                "elapsed_ms": 55,
                "returned_rows": 25,
                "row_count_mode": "count_exact",
                "row_count_total": 100,
                "has_more": true,
                "cursor_offset": 50,
                "next_offset": 75
            }
        }));
        let payload = apply_execute_sql_query_telemetry(
            result,
            "84c44961e673fe5d",
            "qf_1234567890abcdef",
            ExecuteSqlMetadataVerbosity::Full,
        )
        .structured_content
        .expect("structured payload should remain available");

        assert_eq!(
            payload
                .pointer("/meta/query_telemetry/row_count_total")
                .and_then(serde_json::Value::as_u64),
            Some(100)
        );
        assert_eq!(
            payload
                .pointer("/meta/query_telemetry/cursor_offset")
                .and_then(serde_json::Value::as_u64),
            Some(50)
        );
        assert_eq!(
            payload
                .pointer("/meta/query_telemetry/next_offset")
                .and_then(serde_json::Value::as_u64),
            Some(75)
        );
    }

    #[test]
    fn extract_plan_row_estimate_reads_nested_explain_json() {
        let plan = json!([
            {
                "Plan": {
                    "Node Type": "Aggregate",
                    "Plans": [
                        {
                            "Node Type": "Seq Scan",
                            "Plan Rows": 321
                        }
                    ]
                }
            }
        ]);
        assert_eq!(extract_plan_row_estimate(&plan), Some(321));
    }

    #[test]
    fn resolve_execute_sql_metadata_verbosity_defaults_to_compact() {
        assert_eq!(
            resolve_execute_sql_metadata_verbosity(None),
            ExecuteSqlMetadataVerbosity::Compact
        );
    }

    #[test]
    fn apply_execute_sql_metadata_verbosity_compact_removes_verbose_keys() {
        let result = CallToolResult::structured(json!({
            "meta": {
                "columns": [{"name":"id","pg_type":"int4","nullable":false}],
                "query_hints": ["hint"]
            }
        }));
        let payload =
            apply_execute_sql_metadata_verbosity(result, ExecuteSqlMetadataVerbosity::Compact)
                .structured_content
                .expect("structured content should be preserved");
        let meta = payload
            .get("meta")
            .and_then(serde_json::Value::as_object)
            .expect("meta object expected");
        assert!(!meta.contains_key("columns"));
        assert!(!meta.contains_key("query_hints"));
        assert_eq!(
            meta.get("metadata_verbosity")
                .and_then(serde_json::Value::as_str),
            Some("compact")
        );
    }

    #[test]
    fn apply_execute_sql_compact_meta_cleanup_reduces_meta_surface() {
        let result = CallToolResult::structured(json!({
            "meta": {
                "output_mode": "rows",
                "metadata_verbosity": "compact",
                "query_hash": "84c44961e673fe5d",
                "elapsed_ms": 12,
                "truncated": false,
                "returned_rows": 1,
                "has_more": false,
                "next_cursor": null,
                "row_count_mode": "page_window",
                "row_count_total": 10,
                "row_count_returned": 1,
                "cursor_offset": 0,
                "next_offset": null,
                "query_telemetry": {"query_hash": "84c44961e673fe5d"},
                "capabilities": {
                    "startup_state": "healthy",
                    "degraded_read_only": false,
                    "read_write_sql": true,
                    "metadata_discovery": true,
                    "missing_dependencies": [],
                    "reason": null
                },
                "cell_clipping": {"applied": false, "clipped_cells": 0},
                "column_name_safety": {"duplicate_columns_aliased": false, "aliased_columns": []},
                "execution": {"contract_version": "execution/v1"},
                "effective_profile": null,
                "effective_count_mode": "none",
                "effective_metadata_verbosity": "compact",
                "requested_output_mode": "rows",
                "effective_output_mode": "rows",
                "auto_output_resolution": {"reason": "configured_auto_tabular_default"}
            }
        }));

        let payload =
            apply_execute_sql_compact_meta_cleanup(result, ExecuteSqlMetadataVerbosity::Compact)
                .structured_content
                .expect("structured content should be preserved");
        let meta = payload
            .get("meta")
            .and_then(serde_json::Value::as_object)
            .expect("meta object expected");

        assert_eq!(
            meta.get("output_mode").and_then(Value::as_str),
            Some("rows")
        );
        assert_eq!(
            meta.get("metadata_verbosity").and_then(Value::as_str),
            Some("compact")
        );
        assert_eq!(
            meta.get("query_hash").and_then(Value::as_str),
            Some("84c44961e673fe5d")
        );
        assert_eq!(meta.get("elapsed_ms").and_then(Value::as_u64), Some(12));
        assert_eq!(meta.get("returned_rows").and_then(Value::as_u64), Some(1));
        assert_eq!(
            meta.get("row_count_mode").and_then(Value::as_str),
            Some("page_window")
        );
        assert!(meta.get("row_count_total").is_none());
        assert!(meta.get("query_telemetry").is_none());
        assert!(meta.get("capabilities").is_none());
        assert!(meta.get("cell_clipping").is_none());
        assert!(meta.get("column_name_safety").is_none());
        assert!(meta.get("execution").is_none());
        assert!(meta.get("effective_profile").is_none());
        assert!(meta.get("effective_count_mode").is_none());
        assert!(meta.get("effective_metadata_verbosity").is_none());
        assert!(meta.get("requested_output_mode").is_none());
        assert!(meta.get("effective_output_mode").is_none());
        assert!(meta.get("auto_output_resolution").is_none());
        assert_eq!(
            meta.len(),
            9,
            "compact rows meta should stay tightly bounded"
        );
    }

    #[test]
    fn apply_execute_sql_compact_meta_cleanup_preserves_diagnostic_signals() {
        let result = CallToolResult::structured(json!({
            "meta": {
                "output_mode": "rows",
                "metadata_verbosity": "compact",
                "query_hash": "84c44961e673fe5d",
                "elapsed_ms": 12,
                "truncated": false,
                "returned_rows": 1,
                "has_more": false,
                "next_cursor": null,
                "row_count_mode": "exact",
                "row_count_total": 42,
                "capabilities": {
                    "startup_state": "degraded",
                    "degraded_read_only": true,
                    "read_write_sql": false,
                    "metadata_discovery": false,
                    "missing_dependencies": ["openssl"],
                    "reason": "dependency missing"
                },
                "cell_clipping": {"applied": true, "clipped_cells": 2},
                "column_name_safety": {"duplicate_columns_aliased": true, "aliased_columns": ["id__dup2"]},
                "query_telemetry": {"query_hash": "84c44961e673fe5d"}
            }
        }));

        let payload =
            apply_execute_sql_compact_meta_cleanup(result, ExecuteSqlMetadataVerbosity::Compact)
                .structured_content
                .expect("structured content should be preserved");
        assert_eq!(
            payload.pointer("/meta/output_mode").and_then(Value::as_str),
            Some("rows")
        );
        assert_eq!(
            payload
                .pointer("/meta/capabilities/startup_state")
                .and_then(Value::as_str),
            Some("degraded")
        );
        assert_eq!(
            payload
                .pointer("/meta/cell_clipping/applied")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            payload
                .pointer("/meta/column_name_safety/duplicate_columns_aliased")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            payload
                .pointer("/meta/row_count_total")
                .and_then(Value::as_u64),
            Some(42)
        );
        assert!(payload.pointer("/meta/query_telemetry").is_none());
    }

    #[test]
    fn resolve_execute_sql_statement_timeout_override_rejects_zero() {
        let err = resolve_execute_sql_statement_timeout_override(Some(0))
            .expect_err("zero timeout should be rejected");
        assert!(
            err.contains("greater than 0"),
            "unexpected validation message: {err}"
        );
    }

    #[test]
    fn resolve_execute_sql_statement_timeout_override_rejects_values_above_cap() {
        let err = resolve_execute_sql_statement_timeout_override(Some(
            EXECUTE_SQL_STATEMENT_TIMEOUT_OVERRIDE_MAX_MS + 1,
        ))
        .expect_err("values above cap should be rejected");
        assert!(
            err.contains("maximum allowed value"),
            "unexpected validation message: {err}"
        );
    }

    #[test]
    fn statement_timeout_guidance_for_count_context_mentions_count_skip() {
        let hint = statement_timeout_guidance_for_context("Error counting query rows");
        assert!(
            hint.contains("count_mode=none"),
            "count-path guidance should mention count_mode: {hint}"
        );
        assert!(
            hint.contains("include_total_row_count=false"),
            "count-path guidance should mention count skip: {hint}"
        );
    }

    #[test]
    fn timeout_diagnostics_payload_is_bounded_and_actionable() {
        let query_hash =
            response_page_hash_for_params("SELECT * FROM public.events WHERE id = $1", &[json!(7)]);
        let payload = timeout_diagnostics_payload(
            &query_hash,
            "Error executing paginated query",
            Some(Duration::from_millis(7_500)),
            Some(ExecuteSqlResolvedCountMode::Async),
        );
        assert_eq!(
            payload.get("kind").and_then(serde_json::Value::as_str),
            Some("statement_timeout")
        );
        assert_eq!(
            payload
                .get("statement_timeout_ms")
                .and_then(serde_json::Value::as_u64),
            Some(7_500)
        );
        assert_eq!(
            payload
                .get("count_mode")
                .and_then(serde_json::Value::as_str),
            Some("count_async")
        );
        assert_eq!(
            payload.get("query_hash").and_then(Value::as_str),
            Some(query_hash.as_str())
        );
        let actions = payload
            .get("recommended_actions")
            .and_then(serde_json::Value::as_array)
            .expect("timeout diagnostics should include actions");
        assert!(actions.len() <= 3, "diagnostic actions must remain bounded");
        assert!(
            actions.iter().any(|action| {
                action
                    .as_str()
                    .is_some_and(|value| value.contains("query_status"))
            }),
            "expected async follow-up action in diagnostics"
        );
    }

    #[test]
    fn parse_query_status_wait_mode_accepts_immediate_deadline_and_terminal_modes() {
        assert_eq!(
            parse_query_status_wait_mode(None, false).expect("immediate"),
            QueryStatusWaitMode::Immediate
        );
        assert_eq!(
            parse_query_status_wait_mode(Some(250), false).expect("deadline"),
            QueryStatusWaitMode::Deadline { wait_ms: 250 }
        );
        assert_eq!(
            parse_query_status_wait_mode(None, true).expect("until terminal"),
            QueryStatusWaitMode::UntilTerminal
        );
    }

    #[test]
    fn query_job_payload_includes_progress_and_follow_up_guidance() {
        let snapshot = crate::server::QueryJobSnapshot {
            job_id: "qj_0000000000000001".to_string(),
            kind: "query".to_string(),
            query_hash: "84c44961e673fe5d".to_string(),
            task_managed: false,
            state: crate::server::QueryJobState::Running,
            created_at_unix_ms: current_unix_time_ms().saturating_sub(2_000),
            started_at_unix_ms: Some(current_unix_time_ms().saturating_sub(1_500)),
            finished_at_unix_ms: None,
            cancel_requested: false,
            response: None,
            tool_result: None,
        };

        let payload = query_job_payload(
            &snapshot,
            QueryStatusWaitMode::Immediate,
            "immediate",
            0,
            false,
        );
        assert_eq!(
            payload.pointer("/progress/phase").and_then(Value::as_str),
            Some("running")
        );
        assert_eq!(
            payload.pointer("/follow_up/tool").and_then(Value::as_str),
            Some("query_status")
        );
        assert!(
            payload
                .pointer("/wait/suggested_wait_ms")
                .and_then(Value::as_u64)
                .is_some()
        );
    }

    #[test]
    fn normalize_query_status_wait_ms_rejects_zero() {
        let err =
            normalize_query_status_wait_ms(Some(0)).expect_err("zero wait_ms should be rejected");
        assert!(
            err.contains("omit wait_ms"),
            "unexpected validation message: {err}"
        );
    }

    #[test]
    fn contains_top_level_statement_delimiter_detects_multi_statement_sql() {
        assert!(contains_top_level_statement_delimiter("SELECT 1; SELECT 2"));
        assert!(contains_top_level_statement_delimiter(
            "SELECT 1; -- next statement\nSELECT 2"
        ));
        assert!(contains_top_level_statement_delimiter(
            "SELECT 1; 'literal second statement'"
        ));
        assert!(contains_top_level_statement_delimiter(
            "SELECT 1; $$literal second statement$$"
        ));
    }

    #[test]
    fn contains_top_level_statement_delimiter_ignores_literals_comments_and_trailing_semicolon() {
        assert!(!contains_top_level_statement_delimiter(
            "SELECT ';' AS marker"
        ));
        assert!(!contains_top_level_statement_delimiter(
            "SELECT E'it\\'s; ok'"
        ));
        assert!(!contains_top_level_statement_delimiter(
            "SELECT $$body;still_body$$::text"
        ));
        assert!(!contains_top_level_statement_delimiter(
            "SELECT 1 /* ; comment */"
        ));
        assert!(!contains_top_level_statement_delimiter("SELECT 1;"));
        assert!(!contains_top_level_statement_delimiter(
            "SELECT 1; -- trailing note"
        ));
        assert!(!contains_top_level_statement_delimiter(
            "SELECT 1; /* trailing note */"
        ));
        assert!(!contains_top_level_statement_delimiter(
            "SELECT 1; /* outer /* inner */ */"
        ));
        assert!(contains_top_level_statement_delimiter(
            "SELECT E'it\\'s; ok'; SELECT 2"
        ));
        assert!(contains_top_level_statement_delimiter(
            "SELECT 1; /* outer /* inner */ */ SELECT 2"
        ));
    }

    #[test]
    fn metadata_discovery_allowed_for_schema_respects_policy_modes() {
        let mut server = test_server_without_db();

        server.metadata_policy_mode = MetadataPolicyMode::Denied;
        assert!(!metadata_discovery_allowed_for_schema(&server, None));
        assert!(!metadata_discovery_allowed_for_schema(
            &server,
            Some("public")
        ));

        server.metadata_policy_mode = MetadataPolicyMode::Limited;
        server.metadata_schema_allow = Arc::new(vec!["public".to_string()]);
        server.metadata_schema_deny = Arc::new(vec!["private".to_string()]);
        assert!(metadata_discovery_allowed_for_schema(&server, None));
        assert!(metadata_discovery_allowed_for_schema(
            &server,
            Some("public")
        ));
        assert!(!metadata_discovery_allowed_for_schema(
            &server,
            Some("private")
        ));
        assert!(!metadata_discovery_allowed_for_schema(
            &server,
            Some("internal")
        ));
    }

    #[test]
    fn metadata_schema_visibility_sql_predicate_matches_policy_modes() {
        let mut server = test_server_without_db();

        server.metadata_policy_mode = MetadataPolicyMode::Denied;
        assert_eq!(
            metadata_schema_visibility_sql_predicate(&server, "n.nspname").as_deref(),
            Some("FALSE")
        );

        server.metadata_policy_mode = MetadataPolicyMode::Full;
        server.metadata_schema_deny = Arc::new(vec!["private".to_string()]);
        let full_predicate = metadata_schema_visibility_sql_predicate(&server, "n.nspname")
            .expect("full mode with deny list should build predicate");
        assert!(full_predicate.contains("lower(n.nspname) NOT IN ('private')"));

        server.metadata_policy_mode = MetadataPolicyMode::Limited;
        server.metadata_schema_allow =
            Arc::new(vec!["public".to_string(), "analytics".to_string()]);
        let limited_predicate = metadata_schema_visibility_sql_predicate(&server, "n.nspname")
            .expect("limited mode should build predicate");
        assert!(limited_predicate.contains("lower(n.nspname) IN ('public', 'analytics')"));
        assert!(limited_predicate.contains("lower(n.nspname) NOT IN ('private')"));
    }

    #[test]
    fn query_start_args_accept_top_level_execute_sql_shape() {
        let args = serde_json::from_value::<QueryStartArgs>(serde_json::json!({
            "sql": "SELECT 1",
            "max_rows": 25
        }))
        .expect("top-level query_start args should deserialize");
        let execute_sql_args = args.into_execute_sql_args();
        assert_eq!(execute_sql_args.sql.as_deref(), Some("SELECT 1"));
        assert_eq!(execute_sql_args.max_rows, Some(25));
    }

    #[test]
    fn query_start_args_accept_nested_execute_sql_shape_for_compatibility() {
        let args = serde_json::from_value::<QueryStartArgs>(serde_json::json!({
            "execute_sql": {
                "sql": "SELECT 1",
                "max_rows": 10,
                "count_mode": "none"
            }
        }))
        .expect("nested query_start args should deserialize");
        let execute_sql_args = args.into_execute_sql_args();
        assert_eq!(execute_sql_args.sql.as_deref(), Some("SELECT 1"));
        assert_eq!(execute_sql_args.max_rows, Some(10));
        assert_eq!(execute_sql_args.count_mode, Some(ExecuteSqlCountMode::None));
    }

    #[test]
    fn query_start_args_prefer_top_level_fields_when_both_shapes_are_present() {
        let args = serde_json::from_value::<QueryStartArgs>(serde_json::json!({
            "sql": "SELECT 2",
            "max_rows": 5,
            "execute_sql": {
                "sql": "SELECT 1",
                "max_rows": 10,
                "count_mode": "none"
            }
        }))
        .expect("mixed query_start args should deserialize");
        let execute_sql_args = args.into_execute_sql_args();
        assert_eq!(execute_sql_args.sql.as_deref(), Some("SELECT 2"));
        assert_eq!(execute_sql_args.max_rows, Some(5));
        assert_eq!(execute_sql_args.count_mode, Some(ExecuteSqlCountMode::None));
    }

    #[test]
    fn query_start_args_use_summary_only_from_top_level_when_merge_needed() {
        let args = serde_json::from_value::<QueryStartArgs>(serde_json::json!({
            "sql": "SELECT 2",
            "summary_only": true,
            "execute_sql": {
                "sql": "SELECT 1",
                "summary_only": false
            }
        }))
        .expect("mixed query_start args should deserialize");
        let execute_sql_args = args.into_execute_sql_args();
        assert_eq!(execute_sql_args.sql.as_deref(), Some("SELECT 2"));
        assert!(execute_sql_args.summary_only);
    }

    #[test]
    fn query_start_args_prefers_top_level_summary_only_default_when_merge_needed() {
        let args = serde_json::from_value::<QueryStartArgs>(serde_json::json!({
            "sql": "SELECT 2",
            "execute_sql": {
                "summary_only": true
            }
        }))
        .expect("mixed query_start args should deserialize");
        let execute_sql_args = args.into_execute_sql_args();
        assert_eq!(execute_sql_args.sql.as_deref(), Some("SELECT 2"));
        assert!(!execute_sql_args.summary_only);
    }

    #[test]
    fn query_start_args_allows_top_level_summary_only_false_to_override_nested_true() {
        let args = serde_json::from_value::<QueryStartArgs>(serde_json::json!({
            "sql": "SELECT 2",
            "summary_only": false,
            "execute_sql": {
                "summary_only": true
            }
        }))
        .expect("mixed query_start args should deserialize");
        let execute_sql_args = args.into_execute_sql_args();
        assert_eq!(execute_sql_args.sql.as_deref(), Some("SELECT 2"));
        assert!(!execute_sql_args.summary_only);
    }

    #[test]
    fn query_start_args_honors_explicit_top_level_summary_only_false_without_other_top_level_overrides()
     {
        let args = serde_json::from_value::<QueryStartArgs>(serde_json::json!({
            "summary_only": false,
            "execute_sql": {
                "sql": "SELECT 1",
                "summary_only": true,
                "count_mode": "none"
            }
        }))
        .expect("mixed query_start args should deserialize");
        let execute_sql_args = args.into_execute_sql_args();
        assert_eq!(execute_sql_args.sql.as_deref(), Some("SELECT 1"));
        assert!(!execute_sql_args.summary_only);
        assert_eq!(execute_sql_args.count_mode, Some(ExecuteSqlCountMode::None));
    }

    #[test]
    fn query_start_args_allows_top_level_describe_only_false_to_override_nested_true() {
        let args = serde_json::from_value::<QueryStartArgs>(serde_json::json!({
            "describe_only": false,
            "execute_sql": {
                "sql": "SELECT 1",
                "describe_only": true,
                "count_mode": "none"
            }
        }))
        .expect("mixed query_start args should deserialize");
        let execute_sql_args = args.into_execute_sql_args();
        assert_eq!(execute_sql_args.sql.as_deref(), Some("SELECT 1"));
        assert!(!execute_sql_args.describe_only);
        assert_eq!(execute_sql_args.count_mode, Some(ExecuteSqlCountMode::None));
    }

    #[test]
    fn query_start_args_allows_top_level_export_to_file_false_to_override_nested_true() {
        let args = serde_json::from_value::<QueryStartArgs>(serde_json::json!({
            "export_to_file": false,
            "execute_sql": {
                "sql": "SELECT 1",
                "export_to_file": true,
                "count_mode": "none"
            }
        }))
        .expect("mixed query_start args should deserialize");
        let execute_sql_args = args.into_execute_sql_args();
        assert_eq!(execute_sql_args.sql.as_deref(), Some("SELECT 1"));
        assert!(!execute_sql_args.export_to_file);
        assert_eq!(execute_sql_args.count_mode, Some(ExecuteSqlCountMode::None));
    }

    #[test]
    fn query_start_args_use_nested_summary_only_when_top_level_is_just_compatibility_fallback() {
        let args = serde_json::from_value::<QueryStartArgs>(serde_json::json!({
            "execute_sql": {
                "sql": "SELECT 1",
                "summary_only": true,
                "count_mode": "none"
            }
        }))
        .expect("nested query_start args should deserialize");
        let execute_sql_args = args.into_execute_sql_args();
        assert_eq!(execute_sql_args.sql.as_deref(), Some("SELECT 1"));
        assert!(execute_sql_args.summary_only);
        assert_eq!(execute_sql_args.count_mode, Some(ExecuteSqlCountMode::None));
    }

    #[test]
    fn parse_query_status_wait_mode_rejects_conflicting_wait_controls() {
        let err = parse_query_status_wait_mode(Some(1), true)
            .expect_err("wait_ms + wait_until_terminal should fail");
        assert!(
            err.contains("cannot be combined"),
            "unexpected validation message: {err}"
        );
    }

    #[test]
    fn query_status_args_accept_zero_wait_for_tool_level_validation() {
        let args = serde_json::from_value::<QueryStatusArgs>(json!({
            "job_id": "qj_0000000000000001",
            "wait_ms": 0
        }))
        .expect("zero wait_ms should deserialize so tool validation can shape the error");
        assert_eq!(args.wait_ms, Some(0));
    }

    #[test]
    fn query_start_and_wait_args_accept_zero_wait_for_tool_level_validation() {
        let args = serde_json::from_value::<QueryStartAndWaitArgs>(json!({
            "sql": "SELECT 1",
            "wait_ms": 0
        }))
        .expect("zero wait_ms should deserialize so tool validation can shape the error");
        assert_eq!(args.wait_ms, Some(0));
    }

    #[test]
    fn query_job_payload_failed_reports_v2_errors() {
        let failed = json!({
            "ok": false,
            "error": {
                "error": "query execution failed",
                "code": "DB_QUERY_TIMEOUT",
                "reason": "statement_timeout",
            }
        });
        assert!(query_job_payload_failed(&failed));
    }

    #[test]
    fn query_job_payload_failed_reports_legacy_error_payload_without_ok() {
        let legacy_error_payload = json!({
            "error": "division by zero",
            "code": "22012",
            "reason": "arithmetic_error",
            "sqlstate": "22012"
        });
        assert!(query_job_payload_failed(&legacy_error_payload));
    }

    #[test]
    fn query_job_payload_failed_does_not_treat_plain_legacy_error_string_as_failure_without_signature()
     {
        let plain_error_payload = json!({
            "error": "success response",
            "data": {
                "rows": [1, 2, 3],
            }
        });
        assert!(!query_job_payload_failed(&plain_error_payload));
    }

    #[test]
    fn query_job_payload_failed_reports_legacy_error_payload() {
        let failed_legacy_payload = json!({
            "error": "division by zero",
            "code": "22012",
            "reason": "arithmetic_error",
            "sqlstate": "22012"
        });
        assert!(query_job_payload_failed(&failed_legacy_payload));
    }

    #[test]
    fn query_job_payload_failed_does_not_treat_arbitrary_objects_as_errors() {
        let arbitrary_payload = json!({
            "error": "not a database failure",
            "code": "abcde",
            "reason": "all good",
            "rows": [1, 2, 3]
        });
        assert!(!query_job_payload_failed(&arbitrary_payload));
    }

    #[test]
    fn query_job_payload_failed_does_not_treat_success_payload_with_error_like_keys_as_failure() {
        let successful_payload = json!({
            "error": "note",
            "code": "ABC",
            "reason": "ok",
            "rows": [1]
        });
        assert!(!query_job_payload_failed(&successful_payload));
    }

    fn test_server_without_db() -> PostgresMcp {
        PostgresMcp::with_response_contract(
            Arc::new(DbEngine::new(
                None,
                AccessMode::Unrestricted,
                false,
                Some(Duration::from_secs(15)),
                Some(Duration::from_secs(10)),
                Some(Duration::from_secs(2)),
            )),
            ResponseMode::V2,
            ResponseOutputMode::Auto,
            ResponseAutoTabularMode::Rows,
            200,
        )
    }

    #[tokio::test]
    async fn query_start_and_wait_until_terminal_returns_job_response() {
        let server = test_server_without_db();
        let status_payload = server
            .query_start_and_wait(Parameters(QueryStartAndWaitArgs {
                query_start: QueryStartArgs {
                    execute_sql: ExecuteSqlArgs {
                        sql: Some("SELECT 1".to_string()),
                        session_id: None,
                        params: None,
                        cursor: None,
                        max_rows: None,
                        max_cell_chars: None,
                        output_mode: None,
                        response_formatting_mode: None,
                        currency_columns: None,
                        summary_only: false,
                        include_total_row_count: None,
                        count_mode: None,
                        profile: None,
                        metadata_verbosity: None,
                        describe_only: false,
                        export_to_file: false,
                        export_format: None,
                        statement_timeout_ms: None,
                        diagnose_on_timeout: None,
                        preflight_check: None,
                    },
                    execute_sql_nested: None,
                    top_level_summary_only_present: false,
                    top_level_describe_only_present: false,
                    top_level_export_to_file_present: false,
                },
                wait_ms: None,
            }))
            .await
            .expect("query_start_and_wait should succeed")
            .structured_content
            .expect("query_start_and_wait should return structured payload");
        let status_data = tool_success_payload(&status_payload);
        assert_eq!(
            status_data.get("terminal").and_then(Value::as_bool),
            Some(true)
        );
        assert!(status_data.get("response").is_some());
    }

    #[tokio::test]
    async fn query_start_and_wait_rejects_out_of_range_wait_without_launching_job() {
        let server = test_server_without_db();
        let wait_ms = QUERY_STATUS_WAIT_MS_MAX + 1;

        let payload = server
            .query_start_and_wait(Parameters(QueryStartAndWaitArgs {
                query_start: QueryStartArgs {
                    execute_sql: ExecuteSqlArgs {
                        sql: Some("SELECT 1".to_string()),
                        session_id: None,
                        params: None,
                        cursor: None,
                        max_rows: None,
                        max_cell_chars: None,
                        output_mode: None,
                        response_formatting_mode: None,
                        currency_columns: None,
                        summary_only: false,
                        include_total_row_count: None,
                        count_mode: None,
                        profile: None,
                        metadata_verbosity: None,
                        describe_only: false,
                        export_to_file: false,
                        export_format: None,
                        statement_timeout_ms: None,
                        diagnose_on_timeout: None,
                        preflight_check: None,
                    },
                    execute_sql_nested: None,
                    top_level_summary_only_present: false,
                    top_level_describe_only_present: false,
                    top_level_export_to_file_present: false,
                },
                wait_ms: Some(wait_ms),
            }))
            .await
            .expect("query_start_and_wait should return an error payload")
            .structured_content
            .expect("query_start_and_wait should return structured content");

        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_request")
        );
        assert!(
            error
                .get("error")
                .and_then(Value::as_str)
                .is_some_and(|message| message.contains("wait_ms must be <=")),
            "unexpected error payload: {error:?}"
        );

        let start_payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some("SELECT 1".to_string()),
                    session_id: None,
                    params: None,
                    cursor: None,
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: None,
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should succeed")
            .structured_content
            .expect("query_start should return structured payload");
        let start_data = tool_success_payload(&start_payload);
        assert_eq!(
            start_data.get("job_id").and_then(Value::as_str),
            Some("qj_0000000000000001"),
            "invalid wait_ms should not create an orphaned background job"
        );
    }

    #[tokio::test]
    async fn query_status_rejects_zero_wait_with_structured_error() {
        let server = test_server_without_db();
        let payload = server
            .query_status(Parameters(QueryStatusArgs {
                job_id: "qj_0000000000000001".to_string(),
                wait_ms: Some(0),
                wait_until_terminal: false,
            }))
            .await
            .expect("query_status should return an error payload")
            .structured_content
            .expect("query_status should return structured content");

        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_request")
        );
        assert!(
            error
                .get("error")
                .and_then(Value::as_str)
                .is_some_and(|message| {
                    message.contains("wait_ms must be >= 1") && message.contains("omit wait_ms")
                }),
            "unexpected error payload: {error:?}"
        );
    }

    #[tokio::test]
    async fn query_start_and_wait_rejects_zero_wait_without_launching_job() {
        let server = test_server_without_db();

        let payload = server
            .query_start_and_wait(Parameters(QueryStartAndWaitArgs {
                query_start: QueryStartArgs {
                    execute_sql: ExecuteSqlArgs {
                        sql: Some("SELECT 1".to_string()),
                        session_id: None,
                        params: None,
                        cursor: None,
                        max_rows: None,
                        max_cell_chars: None,
                        output_mode: None,
                        response_formatting_mode: None,
                        currency_columns: None,
                        summary_only: false,
                        include_total_row_count: None,
                        count_mode: None,
                        profile: None,
                        metadata_verbosity: None,
                        describe_only: false,
                        export_to_file: false,
                        export_format: None,
                        statement_timeout_ms: None,
                        diagnose_on_timeout: None,
                        preflight_check: None,
                    },
                    execute_sql_nested: None,
                    top_level_summary_only_present: false,
                    top_level_describe_only_present: false,
                    top_level_export_to_file_present: false,
                },
                wait_ms: Some(0),
            }))
            .await
            .expect("query_start_and_wait should return an error payload")
            .structured_content
            .expect("query_start_and_wait should return structured content");

        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_request")
        );
        assert!(
            error
                .get("error")
                .and_then(Value::as_str)
                .is_some_and(|message| {
                    message.contains("wait_ms must be >= 1") && message.contains("omit wait_ms")
                }),
            "unexpected error payload: {error:?}"
        );

        let start_payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some("SELECT 1".to_string()),
                    session_id: None,
                    params: None,
                    cursor: None,
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: None,
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should succeed")
            .structured_content
            .expect("query_start should return structured payload");
        let start_data = tool_success_payload(&start_payload);
        assert_eq!(
            start_data.get("job_id").and_then(Value::as_str),
            Some("qj_0000000000000001"),
            "zero wait_ms should not create an orphaned background job"
        );
    }

    #[tokio::test]
    async fn query_start_and_wait_wait_ms_path_returns_terminal_when_job_finishes_before_deadline()
    {
        let server = test_server_without_db();
        let payload = server
            .query_start_and_wait(Parameters(QueryStartAndWaitArgs {
                query_start: QueryStartArgs {
                    execute_sql: ExecuteSqlArgs {
                        sql: Some("SELECT 1".to_string()),
                        session_id: None,
                        params: None,
                        cursor: None,
                        max_rows: None,
                        max_cell_chars: None,
                        output_mode: None,
                        response_formatting_mode: None,
                        currency_columns: None,
                        summary_only: false,
                        include_total_row_count: None,
                        count_mode: None,
                        profile: None,
                        metadata_verbosity: None,
                        describe_only: false,
                        export_to_file: false,
                        export_format: None,
                        statement_timeout_ms: None,
                        diagnose_on_timeout: None,
                        preflight_check: None,
                    },
                    execute_sql_nested: None,
                    top_level_summary_only_present: false,
                    top_level_describe_only_present: false,
                    top_level_export_to_file_present: false,
                },
                wait_ms: Some(200),
            }))
            .await
            .expect("query_start_and_wait should succeed")
            .structured_content
            .expect("query_start_and_wait should return structured content");
        let data = tool_success_payload(&payload);
        assert_eq!(data.get("terminal").and_then(Value::as_bool), Some(true));
        assert_eq!(
            data.pointer("/wait/mode").and_then(Value::as_str),
            Some("deadline")
        );
    }

    #[tokio::test]
    async fn query_status_until_terminal_wait_continues_on_non_terminal_updates() {
        let server = test_server_without_db();
        let job = server
            .query_jobs
            .create("query-status-progress")
            .expect("query job should be createable");

        let job_id = job.snapshot().job_id;
        let status_task = tokio::spawn({
            let server = server.clone();
            let job_id = job_id.clone();
            async move {
                server
                    .query_status(Parameters(QueryStatusArgs {
                        job_id,
                        wait_ms: None,
                        wait_until_terminal: true,
                    }))
                    .await
            }
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        job.mark_running();

        tokio::time::sleep(Duration::from_millis(25)).await;
        let _ = job.complete(
            crate::server::QueryJobState::Succeeded,
            json!({
                "ok": true,
                "data": [{"id": 1}],
                "meta": {}
            }),
        );

        let status_payload = tokio::time::timeout(Duration::from_secs(1), async {
            status_task.await.expect("status task must complete")
        })
        .await
        .expect("status task should complete before timeout")
        .expect("query_status should succeed")
        .structured_content
        .expect("query_status should return structured payload");
        let status_data = tool_success_payload(&status_payload);
        assert_eq!(
            status_data.get("state").and_then(Value::as_str),
            Some("succeeded")
        );
        assert_eq!(
            status_data.get("terminal").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            status_data
                .get("wait")
                .and_then(|wait| wait.get("trigger"))
                .and_then(Value::as_str),
            Some("job_terminal")
        );
        assert!(status_data.get("response").is_some());
    }

    #[tokio::test]
    async fn execute_sql_async_count_page_failures_do_not_spawn_background_jobs() {
        let server = test_server_without_db();

        let failed_payload = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some("SELECT 1".to_string()),
                session_id: None,
                params: None,
                cursor: None,
                max_rows: None,
                max_cell_chars: None,
                output_mode: None,
                response_formatting_mode: None,
                currency_columns: None,
                summary_only: false,
                include_total_row_count: None,
                count_mode: Some(ExecuteSqlCountMode::Async),
                profile: None,
                metadata_verbosity: Some(ExecuteSqlMetadataVerbosity::Full),
                describe_only: false,
                export_to_file: false,
                export_format: None,
                statement_timeout_ms: None,
                diagnose_on_timeout: None,
                preflight_check: None,
            }))
            .await
            .expect("execute_sql should return tool result")
            .structured_content
            .expect("failed execute_sql should still emit structured content");
        let error = tool_error_payload(&failed_payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("database_uri_not_configured")
        );

        let start_payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some("SELECT 1".to_string()),
                    session_id: None,
                    params: None,
                    cursor: None,
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: None,
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should succeed")
            .structured_content
            .expect("query_start should return structured payload");
        let start_data = tool_success_payload(&start_payload);
        assert_eq!(
            start_data.get("job_id").and_then(Value::as_str),
            Some("qj_0000000000000001")
        );
    }

    #[tokio::test]
    async fn query_start_rejects_blank_sql() {
        let server = test_server_without_db();
        let payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some("   ".to_string()),
                    session_id: None,
                    params: None,
                    cursor: None,
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: None,
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should return a payload")
            .structured_content
            .expect("query_start should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_request")
        );
        assert_eq!(
            error.get("error").and_then(Value::as_str),
            Some("sql must not be empty")
        );
    }

    #[tokio::test]
    async fn query_start_rejects_non_read_safe_sql() {
        let server = test_server_without_db();
        let payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some("UPDATE public.jobs SET state = 'done'".to_string()),
                    session_id: None,
                    params: None,
                    cursor: None,
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: None,
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should return a payload")
            .structured_content
            .expect("query_start should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("restricted_sql")
        );
    }

    #[tokio::test]
    async fn query_start_hash_uses_canonical_rewritten_sql() {
        let server = test_server_without_db();
        let raw_sql = "SELECT * FROM latest_snapshot(source => 'public.events', ts_column => 'snapshot_ts') AS latest";
        let expected_hash = response_page_hash(
            &rewrite_latest_snapshot_helpers(raw_sql)
                .expect("helper rewrite should succeed")
                .sql,
        );
        let payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some(raw_sql.to_string()),
                    session_id: None,
                    params: None,
                    cursor: None,
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: None,
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should succeed")
            .structured_content
            .expect("query_start should return structured payload");
        let data = tool_success_payload(&payload);
        assert_eq!(
            data.get("query_hash").and_then(Value::as_str),
            Some(expected_hash.as_str())
        );
    }

    #[tokio::test]
    async fn query_start_success_payload_includes_execution_metadata() {
        let server = test_server_without_db();
        let payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some("SELECT 1".to_string()),
                    session_id: None,
                    params: Some(vec![json!(1)]),
                    cursor: None,
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: Some(5_000),
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should succeed")
            .structured_content
            .expect("query_start should return structured payload");

        assert_eq!(
            payload.pointer("/meta/job_id").and_then(Value::as_str),
            Some("qj_0000000000000001")
        );
        assert!(
            payload
                .pointer("/meta/query_hash")
                .and_then(Value::as_str)
                .is_some(),
            "legacy query_hash should remain present"
        );
        assert_eq!(
            payload
                .pointer("/meta/execution/contract_version")
                .and_then(Value::as_str),
            Some("execution/v1")
        );
        assert_eq!(
            payload
                .pointer("/meta/execution/scope")
                .and_then(Value::as_str),
            Some("query_start")
        );
        assert_eq!(
            payload
                .pointer("/meta/execution/params/bound_count")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            payload
                .pointer("/meta/execution/timeout/override_applied")
                .and_then(Value::as_bool),
            Some(true)
        );
    }

    #[tokio::test]
    async fn query_start_rejects_cursor_for_non_paginated_shape() {
        let server = test_server_without_db();
        let sql = "SHOW search_path";
        let query_hash = response_page_hash(sql);
        let cursor =
            encode_pagination_cursor(&server, PaginationCursorScope::ExecuteSql, &query_hash, 25);
        let payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some(sql.to_string()),
                    session_id: None,
                    params: None,
                    cursor: Some(cursor),
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: None,
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should return a payload")
            .structured_content
            .expect("query_start should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_cursor")
        );
    }

    #[tokio::test]
    async fn query_start_rejects_invalid_statement_timeout_ms() {
        let server = test_server_without_db();
        let payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some("SELECT 1".to_string()),
                    session_id: None,
                    params: None,
                    cursor: None,
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: Some(0),
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should return a payload")
            .structured_content
            .expect("query_start should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_request")
        );
        assert_eq!(
            error.get("error").and_then(Value::as_str),
            Some("statement_timeout_ms must be greater than 0")
        );
    }

    #[tokio::test]
    async fn query_start_rejects_describe_only_with_export_to_file() {
        let server = test_server_without_db();
        let payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some("SELECT 1".to_string()),
                    session_id: None,
                    params: None,
                    cursor: None,
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: true,
                    export_to_file: true,
                    export_format: None,
                    statement_timeout_ms: None,
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should return a payload")
            .structured_content
            .expect("query_start should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_request")
        );
        assert_eq!(
            error.get("error").and_then(Value::as_str),
            Some("describe_only cannot be combined with export_to_file")
        );
    }

    #[tokio::test]
    async fn query_start_rejects_invalid_cursor() {
        let server = test_server_without_db();
        let payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some("SELECT 1".to_string()),
                    session_id: None,
                    params: None,
                    cursor: Some("not-a-pagination-cursor".to_string()),
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: None,
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should return a payload")
            .structured_content
            .expect("query_start should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_cursor")
        );
        assert_eq!(
            error.get("code").and_then(Value::as_str),
            Some("INVALID_CURSOR")
        );
    }

    #[tokio::test]
    async fn query_start_rejects_cursor_when_bound_params_change() {
        let server = test_server_without_db();
        let sql = "SELECT id FROM public.operator_review_queue WHERE id = $1 ORDER BY id";
        let query_hash = response_page_hash_for_params(sql, &[serde_json::json!(1)]);
        let cursor =
            encode_pagination_cursor(&server, PaginationCursorScope::ExecuteSql, &query_hash, 10);
        let payload = server
            .query_start(Parameters(QueryStartArgs {
                execute_sql: ExecuteSqlArgs {
                    sql: Some(sql.to_string()),
                    session_id: None,
                    params: Some(vec![serde_json::json!(2)]),
                    cursor: Some(cursor),
                    max_rows: None,
                    max_cell_chars: None,
                    output_mode: None,
                    response_formatting_mode: None,
                    currency_columns: None,
                    summary_only: false,
                    include_total_row_count: None,
                    count_mode: None,
                    profile: None,
                    metadata_verbosity: None,
                    describe_only: false,
                    export_to_file: false,
                    export_format: None,
                    statement_timeout_ms: None,
                    diagnose_on_timeout: None,
                    preflight_check: None,
                },
                execute_sql_nested: None,
                top_level_summary_only_present: false,
                top_level_describe_only_present: false,
                top_level_export_to_file_present: false,
            }))
            .await
            .expect("query_start should return a payload")
            .structured_content
            .expect("query_start should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_cursor")
        );
        assert_eq!(
            error.get("code").and_then(Value::as_str),
            Some("CURSOR_QUERY_MISMATCH")
        );
    }

    #[tokio::test]
    async fn execute_sql_rejects_cursor_for_non_select_like_query() {
        let server = test_server_without_db();
        let payload = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some("UPDATE public.operator_review_queue SET reason = 'x'".to_string()),
                session_id: None,
                params: None,
                cursor: Some("opaque-cursor".to_string()),
                max_rows: None,
                max_cell_chars: None,
                output_mode: None,
                response_formatting_mode: None,
                currency_columns: None,
                summary_only: false,
                include_total_row_count: None,
                count_mode: None,
                profile: None,
                metadata_verbosity: None,
                describe_only: false,
                export_to_file: false,
                export_format: None,
                statement_timeout_ms: None,
                diagnose_on_timeout: None,
                preflight_check: None,
            }))
            .await
            .expect("execute_sql should return payload")
            .structured_content
            .expect("execute_sql should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_cursor")
        );
        assert_eq!(
            error.get("code").and_then(Value::as_str),
            Some("INVALID_CURSOR")
        );
    }

    #[tokio::test]
    async fn execute_sql_describe_only_rejects_invalid_cursor() {
        let server = test_server_without_db();
        let payload = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some("SELECT 1".to_string()),
                session_id: None,
                params: None,
                cursor: Some("not-a-pagination-cursor".to_string()),
                max_rows: None,
                max_cell_chars: None,
                output_mode: None,
                response_formatting_mode: None,
                currency_columns: None,
                summary_only: false,
                include_total_row_count: None,
                count_mode: None,
                profile: None,
                metadata_verbosity: None,
                describe_only: true,
                export_to_file: false,
                export_format: None,
                statement_timeout_ms: None,
                diagnose_on_timeout: None,
                preflight_check: None,
            }))
            .await
            .expect("execute_sql should return payload")
            .structured_content
            .expect("execute_sql should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_cursor")
        );
        assert_eq!(
            error.get("code").and_then(Value::as_str),
            Some("INVALID_CURSOR")
        );
    }

    #[tokio::test]
    async fn execute_sql_rejects_invalid_cursor_before_preflight_validation() {
        let server = test_server_without_db();
        let payload = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some("SELECT 1; SELECT 2".to_string()),
                session_id: None,
                params: None,
                cursor: Some("not-a-pagination-cursor".to_string()),
                max_rows: None,
                max_cell_chars: None,
                output_mode: None,
                response_formatting_mode: None,
                currency_columns: None,
                summary_only: false,
                include_total_row_count: None,
                count_mode: None,
                profile: None,
                metadata_verbosity: None,
                describe_only: false,
                export_to_file: false,
                export_format: None,
                statement_timeout_ms: None,
                diagnose_on_timeout: None,
                preflight_check: Some(true),
            }))
            .await
            .expect("execute_sql should return payload")
            .structured_content
            .expect("execute_sql should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_cursor")
        );
        assert_eq!(
            error.get("code").and_then(Value::as_str),
            Some("INVALID_CURSOR")
        );
    }

    #[tokio::test]
    async fn execute_sql_rejects_cursor_when_bound_params_change() {
        let server = test_server_without_db();
        let sql = "SELECT id FROM public.operator_review_queue WHERE id = $1 ORDER BY id";
        let query_hash = response_page_hash_for_params(sql, &[serde_json::json!(1)]);
        let cursor =
            encode_pagination_cursor(&server, PaginationCursorScope::ExecuteSql, &query_hash, 10);
        let payload = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some(sql.to_string()),
                session_id: None,
                params: Some(vec![serde_json::json!(2)]),
                cursor: Some(cursor),
                max_rows: None,
                max_cell_chars: None,
                output_mode: None,
                response_formatting_mode: None,
                currency_columns: None,
                summary_only: false,
                include_total_row_count: None,
                count_mode: None,
                profile: None,
                metadata_verbosity: None,
                describe_only: false,
                export_to_file: false,
                export_format: None,
                statement_timeout_ms: None,
                diagnose_on_timeout: None,
                preflight_check: None,
            }))
            .await
            .expect("execute_sql should return payload")
            .structured_content
            .expect("execute_sql should return structured content");
        let error = tool_error_payload(&payload);
        assert_eq!(
            error.get("reason").and_then(Value::as_str),
            Some("invalid_cursor")
        );
        assert_eq!(
            error.get("code").and_then(Value::as_str),
            Some("CURSOR_QUERY_MISMATCH")
        );
    }

    #[tokio::test]
    async fn query_cancel_transitions_pending_job_to_canceled() {
        let server = test_server_without_db();
        let job = server
            .query_jobs
            .create("deadbeefdeadbeef")
            .expect("query job creation should succeed");
        let job_id = job.snapshot().job_id;
        let payload = server
            .query_cancel(Parameters(QueryCancelArgs { job_id }))
            .await
            .expect("query_cancel should succeed")
            .structured_content
            .expect("query_cancel should return structured payload");
        let data = tool_success_payload(&payload);
        assert_eq!(data.get("state").and_then(Value::as_str), Some("canceled"));
        assert_eq!(data.get("canceled").and_then(Value::as_bool), Some(true));
        assert_eq!(data.get("terminal").and_then(Value::as_bool), Some(true));
    }

    async fn assert_execute_sql_error(
        server: &PostgresMcp,
        sql: &str,
        expected_sqlstate: &str,
        expected_error_fragments: &[&str],
        expected_hint_fragments: &[&str],
    ) -> serde_json::Value {
        let payload = execute_sql_for_test(server, sql).await;
        let error_payload = tool_error_payload(&payload);
        assert_eq!(
            error_payload
                .get("sqlstate")
                .and_then(serde_json::Value::as_str),
            Some(expected_sqlstate),
            "expected sqlstate {expected_sqlstate} but got {payload:?}"
        );

        let error = error_payload
            .get("error")
            .and_then(serde_json::Value::as_str)
            .expect("error payload should contain a string message");
        for fragment in expected_error_fragments {
            assert!(
                error.contains(fragment),
                "expected error message to contain `{fragment}` but got `{error}`"
            );
        }

        let hint = error_payload
            .get("hint")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        for fragment in expected_hint_fragments {
            assert!(
                hint.contains(fragment),
                "expected hint to contain `{fragment}` but got `{hint}`"
            );
        }

        payload
    }

    #[tokio::test]
    async fn execute_sql_error_shows_sql_error_first_for_counting_path() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping execute_sql_error_shows_sql_error_first_for_counting_path (DATABASE_URI not set)"
            );
            return;
        };
        let payload = server
            .execute_sql(Parameters(ExecuteSqlArgs {
                sql: Some(
                    "SELECT * FROM postgres_mcp_execute_sql_repro_missing_relation_1733"
                        .to_string(),
                ),
                session_id: None,
                params: None,
                cursor: None,
                max_rows: None,
                max_cell_chars: None,
                output_mode: Some(ResponseOutputMode::Rows),
                response_formatting_mode: None,
                currency_columns: None,
                summary_only: false,
                include_total_row_count: None,
                count_mode: Some(ExecuteSqlCountMode::Exact),
                profile: None,
                metadata_verbosity: Some(ExecuteSqlMetadataVerbosity::Full),
                describe_only: false,
                export_to_file: false,
                export_format: None,
                statement_timeout_ms: None,
                diagnose_on_timeout: None,
                preflight_check: None,
            }))
            .await
            .expect("execute_sql should return tool result")
            .structured_content
            .expect("execute_sql should return structured content");
        let error_payload = tool_error_payload(&payload);
        assert_eq!(
            error_payload.get("sqlstate").and_then(Value::as_str),
            Some("42P01")
        );
        let error = error_payload
            .get("error")
            .and_then(serde_json::Value::as_str)
            .expect("error payload should contain error message");
        assert!(
            error.starts_with("query execution failed"),
            "root DB error should be first in message: {error}"
        );
        assert!(
            error.contains("sqlstate: 42P01"),
            "error should expose the SQLSTATE explicitly: {error}"
        );
        assert!(
            error.ends_with("(Error counting query rows)"),
            "context should be secondary in message: {error}"
        );
    }

    #[tokio::test]
    async fn execute_sql_latest_snapshot_helper_rewrites_partitioned_latest_row() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping execute_sql_latest_snapshot_helper_rewrites_partitioned_latest_row (DATABASE_URI not set)"
            );
            return;
        };

        let payload = tool_success_payload(
            &execute_sql_for_test(
                &server,
                "
            CREATE TEMP TABLE postgres_mcp_execute_sql_repro_latest_snapshot_1737 (
                tenant_id integer,
                snapshot_id integer,
                snapshot_ts timestamptz
            );
            INSERT INTO postgres_mcp_execute_sql_repro_latest_snapshot_1737 (tenant_id, snapshot_id, snapshot_ts) VALUES
                (1, 10, '2026-01-01T10:00:00Z'),
                (1, 11, '2026-01-02T10:00:00Z'),
                (2, 20, '2026-01-01T12:00:00Z'),
                (2, 21, NULL);
                SELECT tenant_id, snapshot_id
                FROM latest_snapshot(
                    source => 'postgres_mcp_execute_sql_repro_latest_snapshot_1737',
                    ts_column => 'snapshot_ts',
                    partition_by => ARRAY['tenant_id']
                ) AS latest
                ORDER BY tenant_id, snapshot_id;
                ",
            )
            .await,
        );

        let rows = payload
            .as_array()
            .expect("successful helper query should include row array");
        let mut got = rows
            .iter()
            .map(|row| {
                let tenant = row
                    .get("tenant_id")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap();
                let snapshot_id = row
                    .get("snapshot_id")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap();
                (tenant, snapshot_id)
            })
            .collect::<Vec<_>>();
        got.sort_unstable();
        assert_eq!(
            got,
            vec![(1, 11), (2, 20)],
            "should return latest snapshot id per tenant"
        );
    }

    #[tokio::test]
    async fn execute_sql_error_shows_sql_error_first_for_non_paginated_path() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping execute_sql_error_shows_sql_error_first_for_non_paginated_path (DATABASE_URI not set)"
            );
            return;
        };
        let payload = assert_execute_sql_error(
            &server,
            "INSERT INTO postgres_mcp_execute_sql_repro_missing_relation_1733 (id) VALUES (1)",
            "42P01",
            &[
                "query execution failed",
                "relation \"postgres_mcp_execute_sql_repro_missing_relation_1733\" does not exist",
            ],
            &[],
        )
        .await;
        let error_payload = tool_error_payload(&payload);
        let error = error_payload
            .get("error")
            .and_then(serde_json::Value::as_str)
            .expect("error payload should contain error message");
        assert!(
            error.starts_with("query execution failed"),
            "root DB error should be first in message: {error}"
        );
        assert!(
            error.contains("sqlstate: 42P01"),
            "error should expose the SQLSTATE explicitly: {error}"
        );
        assert!(
            error.ends_with("(Error executing query)"),
            "context should be secondary in message: {error}"
        );
    }

    #[tokio::test]
    async fn execute_sql_repro_pack_filter_syntax_42601() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!("skipping execute_sql_repro_pack_filter_syntax_42601 (DATABASE_URI not set)");
            return;
        };

        assert_execute_sql_error(
            &server,
            "
            CREATE TEMP TABLE postgres_mcp_repro_1734_filter (id int);
            INSERT INTO postgres_mcp_repro_1734_filter (id) VALUES (1), (2);
            SELECT count(*) FILTER (id > 0) FROM postgres_mcp_repro_1734_filter;
            ",
            "42601",
            &["query execution failed", "syntax error"],
            &[],
        )
        .await;

        let rewritten_payload = tool_success_payload(
            &execute_sql_for_test(
                &server,
                "
            CREATE TEMP TABLE postgres_mcp_repro_1734_filter (id int);
            INSERT INTO postgres_mcp_repro_1734_filter (id) VALUES (1), (2);
            SELECT count(*) FILTER (WHERE id > 0) FROM postgres_mcp_repro_1734_filter;
            ",
            )
            .await,
        );
        let rows = rewritten_payload
            .as_array()
            .expect("successful query should include row array");
        assert_eq!(rows.len(), 1, "filter query rewrite should return one row");
    }

    #[tokio::test]
    async fn execute_sql_repro_pack_alias_errors_42703_42_p01() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping execute_sql_repro_pack_alias_errors_42703_42_p01 (DATABASE_URI not set)"
            );
            return;
        };

        assert_execute_sql_error(
            &server,
            "
            CREATE TEMP TABLE postgres_mcp_repro_1734_aliases (id int, offer_id int);
            INSERT INTO postgres_mcp_repro_1734_aliases (id, offer_id) VALUES (1, 10), (2, 20);
            SELECT a.missing_column FROM postgres_mcp_repro_1734_aliases AS a;
            ",
            "42703",
            &[
                "query execution failed",
                "missing_column",
                "does not exist",
                "42703",
            ],
            &[],
        )
        .await;

        assert_execute_sql_error(
            &server,
            "
            CREATE TEMP TABLE postgres_mcp_repro_1734_aliases (id int, offer_id int);
            INSERT INTO postgres_mcp_repro_1734_aliases (id, offer_id) VALUES (1, 10);
            SELECT *
            FROM postgres_mcp_repro_1734_aliases AS a
            JOIN postgres_mcp_repro_1734_aliases AS b
                ON a.offer_id = missing.offer_id;
            ",
            "42P01",
            &[
                "query execution failed",
                "missing FROM-clause entry for table \"missing\"",
            ],
            &[],
        )
        .await;

        let rewritten_payload = tool_success_payload(
            &execute_sql_for_test(
                &server,
                "
            CREATE TEMP TABLE postgres_mcp_repro_1734_aliases (id int, offer_id int);
            INSERT INTO postgres_mcp_repro_1734_aliases (id, offer_id) VALUES (1, 10), (2, 20);
            SELECT a.id, b.offer_id
            FROM postgres_mcp_repro_1734_aliases AS a
            JOIN postgres_mcp_repro_1734_aliases AS b
                ON a.offer_id = b.offer_id;
            ",
            )
            .await,
        );
        let rows = rewritten_payload
            .as_array()
            .expect("successful query should include row array");
        assert_eq!(
            rows.len(),
            2,
            "alias-corrected join should return matched rows"
        );
    }

    #[tokio::test]
    async fn execute_sql_missing_from_clause_error_includes_alias_scope_hint_when_no_pg_hint() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping execute_sql_missing_from_clause_error_includes_alias_scope_hint_when_no_pg_hint (DATABASE_URI not set)"
            );
            return;
        };

        let payload = assert_execute_sql_error(
            &server,
            "
            CREATE TEMP TABLE postgres_mcp_repro_2169_alias (id int);
            INSERT INTO postgres_mcp_repro_2169_alias (id) VALUES (1);
            SELECT *
            FROM postgres_mcp_repro_2169_alias AS a
            JOIN postgres_mcp_repro_2169_alias AS b
              ON a.id = missing.id;
            ",
            "42P01",
            &["missing FROM-clause entry for table \"missing\""],
            &[
                "Alias \"missing\" is referenced but not present in FROM/JOIN scope.",
                "Quick discovery: call list_objects",
            ],
        )
        .await;

        let error_payload = tool_error_payload(&payload);
        let hint = error_payload
            .get("hint")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            hint.contains("Observed FROM/JOIN relations:"),
            "hint should include relation-scope guidance: {hint}"
        );
    }

    #[tokio::test]
    async fn execute_sql_repro_pack_round_cast_42883() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!("skipping execute_sql_repro_pack_round_cast_42883 (DATABASE_URI not set)");
            return;
        };

        assert_execute_sql_error(
            &server,
            "SELECT round(1.2345::double precision, 2)",
            "42883",
            &[
                "query execution failed",
                "function round(double precision, integer) does not exist",
            ],
            &["No function matches the given name and argument types."],
        )
        .await;

        let rewritten_payload = tool_success_payload(
            &execute_sql_for_test(&server, "SELECT round(1.2345::numeric, 2) AS rounded").await,
        );
        let rows = rewritten_payload
            .as_array()
            .expect("successful query should include row array");
        let row = rows
            .first()
            .and_then(|value| value.as_object())
            .expect("successful query should include one row");
        let rounded = row
            .get("rounded")
            .and_then(serde_json::Value::as_f64)
            .unwrap();
        assert_eq!(
            rounded, 1.23,
            "rounded numeric rewrite should return expected result"
        );
    }

    #[tokio::test]
    async fn execute_sql_reports_concrete_projection_types() {
        let Some(server) = live_db_server_from_env() else {
            eprintln!(
                "skipping execute_sql_reports_concrete_projection_types (DATABASE_URI not set)"
            );
            return;
        };

        let payload = execute_sql_for_test(
            &server,
            "SELECT 42::numeric AS amount, 'ok'::text AS status, CURRENT_DATE AS run_date",
        )
        .await;
        let columns = payload
            .pointer("/meta/columns")
            .and_then(serde_json::Value::as_array)
            .expect("successful query should include columns in meta");

        let amount = columns
            .first()
            .and_then(|value| value.get("pg_type"))
            .and_then(serde_json::Value::as_str)
            .expect("numeric column should include pg_type");
        let status = columns
            .get(1)
            .and_then(|value| value.get("pg_type"))
            .and_then(serde_json::Value::as_str)
            .expect("text column should include pg_type");
        let run_date = columns
            .get(2)
            .and_then(|value| value.get("pg_type"))
            .and_then(serde_json::Value::as_str)
            .expect("date column should include pg_type");

        assert_eq!(amount, "numeric");
        assert_eq!(status, "text");
        assert_eq!(run_date, "date");
    }
}
