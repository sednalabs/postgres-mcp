//! Operator-owned HTTP configuration. No tool call can change endpoint authority.
//!
//! The backend listener is loopback-only. A trusted local HTTPS proxy provides
//! public TLS; forwarded headers never select issuer, database, or resource.
use serde::Deserialize;
use std::{collections::HashSet, net::SocketAddr, path::Path};
use url::Url;

pub const READ_SCOPE: &str = "postgres:read";

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    pub bind: SocketAddr,
    pub resource_url: String,
    pub issuer: String,
    pub allowed_hosts: Vec<String>,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    pub allowed_client_ids: HashSet<String>,
    pub allowed_subjects: HashSet<String>,
    pub expected_database_role: String,
    /// Only permits loopback issuer/resource URLs for synthetic development.
    #[serde(default)]
    pub allow_loopback_http: bool,
    #[serde(default)]
    pub limits: HttpLimits,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpLimits {
    pub max_requests: usize,
    pub max_legacy_sessions: usize,
    pub max_request_bytes: usize,
}
impl Default for HttpLimits {
    fn default() -> Self {
        Self {
            max_requests: 16,
            max_legacy_sessions: 64,
            max_request_bytes: 32 * 1024,
        }
    }
}

pub fn endpoint(raw: &str, allow_local: bool) -> Result<Url, String> {
    let url = Url::parse(raw).map_err(|_| "Invalid endpoint URL")?;
    let local = url.host_str().is_some_and(|h| {
        h == "localhost"
            || h.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
        || url.host_str().is_none()
        || !(url.scheme() == "https" || (allow_local && local && url.scheme() == "http"))
    {
        return Err("Endpoint requires HTTPS without credentials, query or fragment; only explicit loopback development HTTP is allowed.".into());
    }
    Ok(url)
}

impl HttpConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        use std::io::Read;
        let file = std::fs::File::open(path).map_err(|_| "Cannot open HTTP configuration")?;
        let metadata = file
            .metadata()
            .map_err(|_| "Cannot inspect HTTP configuration")?;
        if !metadata.is_file() || metadata.len() > 65536 {
            return Err("HTTP configuration must be a regular file of at most 64 KiB".into());
        }
        let mut bytes = Vec::new();
        file.take(65537)
            .read_to_end(&mut bytes)
            .map_err(|_| "Cannot read HTTP configuration")?;
        if bytes.len() > 65536 {
            return Err("HTTP configuration is too large".into());
        }
        let config: Self =
            serde_json::from_slice(&bytes).map_err(|_| "Invalid HTTP configuration JSON")?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), String> {
        if !self.bind.ip().is_loopback() {
            return Err(
                "HTTP backend must bind loopback behind a trusted local HTTPS proxy".into(),
            );
        }
        let resource = endpoint(&self.resource_url, self.allow_loopback_http)?;
        endpoint(&self.issuer, self.allow_loopback_http)?;
        if resource.path() != "/mcp" {
            return Err("resource_url must identify the exact /mcp endpoint".into());
        }
        if self.allowed_hosts.is_empty()
            || self.allowed_hosts.len() > 32
            || self.allowed_hosts.iter().any(|h| {
                h.is_empty()
                    || h.len() > 255
                    || h.contains(['*', '/', '@', '?', '#'])
                    || h.chars().any(char::is_whitespace)
            })
        {
            return Err("Set 1..32 explicit allowed_hosts without wildcards".into());
        }
        if self.allowed_origins.len() > 32 {
            return Err("Too many allowed_origins".into());
        }
        for raw in &self.allowed_origins {
            let origin = endpoint(raw, self.allow_loopback_http)?;
            if origin.origin().ascii_serialization() != *raw {
                return Err("allowed_origins must be exact origins without paths".into());
            }
        }
        for (values, name) in [
            (&self.allowed_client_ids, "allowed_client_ids"),
            (&self.allowed_subjects, "allowed_subjects"),
        ] {
            if values.is_empty()
                || values.len() > 256
                || values
                    .iter()
                    .any(|s| s.is_empty() || s.len() > 512 || s.chars().any(char::is_control))
            {
                return Err(format!(
                    "{name} must contain 1..256 bounded nonempty identities"
                ));
            }
        }
        if self.expected_database_role.is_empty()
            || self.expected_database_role.len() > 63
            || self.expected_database_role.chars().any(char::is_control)
        {
            return Err("expected_database_role must be a bounded PostgreSQL role name".into());
        }
        if !(1..=64).contains(&self.limits.max_requests)
            || !(1..=256).contains(&self.limits.max_legacy_sessions)
            || !(1024..=65536).contains(&self.limits.max_request_bytes)
        {
            return Err("HTTP limits must be finite: requests 1..64, sessions 1..256, body 1024..65536 bytes".into());
        }
        Ok(())
    }
    pub fn public_base_url(&self) -> Result<String, String> {
        let mut url = endpoint(&self.resource_url, self.allow_loopback_http)?;
        url.set_path("");
        Ok(url.as_str().trim_end_matches('/').to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn sample() -> HttpConfig {
        serde_json::from_value(serde_json::json!({
            "bind":"127.0.0.1:8080", "resource_url":"https://postgres.example/mcp",
            "issuer":"https://id.example/realms/example", "allowed_hosts":["postgres.example"],
            "allowed_client_ids":["chatgpt"], "allowed_subjects":["operator"], "expected_database_role":"mcp_reader"
        })).unwrap()
    }
    #[test]
    fn fail_closed_configuration() {
        assert!(sample().validate().is_ok());
        let mut c = sample();
        c.bind = "0.0.0.0:8080".parse().unwrap();
        assert!(c.validate().is_err());
        let mut c = sample();
        c.allowed_subjects.clear();
        assert!(c.validate().is_err());
        let mut c = sample();
        c.allowed_client_ids.clear();
        assert!(c.validate().is_err());
        let mut c = sample();
        c.limits.max_requests = 0;
        assert!(c.validate().is_err());
        let mut c = sample();
        c.allowed_hosts = vec!["*".into()];
        assert!(c.validate().is_err());
        let mut c = sample();
        c.allowed_origins = vec!["https://chatgpt.com/path".into()];
        assert!(c.validate().is_err());
    }
    #[test]
    fn endpoint_authority_is_operator_owned_and_secure() {
        assert!(endpoint("http://127.0.0.1:8080/mcp", true).is_ok());
        for raw in [
            "http://127.0.0.1:8080",
            "https://user:pass@example.com",
            "https://example.com?aud=x",
            "https://example.com#x",
        ] {
            assert!(endpoint(raw, false).is_err());
        }
        assert!(endpoint("http://example.com", true).is_err());
    }
}
