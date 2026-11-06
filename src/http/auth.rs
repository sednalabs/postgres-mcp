//! OAuth resource-server composition; credentials are verified by Toolkit.
//! Only a consumed live request witness may become a remote query principal.
use super::config::{HttpConfig, READ_SCOPE, endpoint};
use mcp_toolkit_auth::{
    AuthConfig, AuthMode, AuthSecurityProfile, Authenticator, discover_oidc_metadata,
    surface::{AuthSurfaceLayer, AuthorizationServerMetadataSource, IssuerEntry},
};
use mcp_toolkit_server::auth::AuthSurfaceBuilder;
use std::sync::Arc;

pub(super) struct Authentication {
    pub authenticator: Arc<Authenticator>,
    pub layer: AuthSurfaceLayer,
    pub issuer: String,
}

pub(super) async fn build(config: &HttpConfig) -> Result<Authentication, String> {
    let issuer = config.issuer.trim_end_matches('/').to_owned();
    let metadata = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        discover_oidc_metadata(&issuer, None),
    )
    .await
    .map_err(|_| "Authorization server discovery timed out")?
    .map_err(|_| "Authorization server discovery failed; refusing unauthenticated startup")?;
    endpoint(&metadata.jwks_uri, config.allow_loopback_http)?;
    for value in [
        &metadata.authorization_endpoint,
        &metadata.token_endpoint,
        &metadata.registration_endpoint,
        &metadata.introspection_endpoint,
        &metadata.device_authorization_endpoint,
    ]
    .into_iter()
    .flatten()
    {
        endpoint(value, config.allow_loopback_http)?;
    }
    if metadata.authorization_endpoint.is_none()
        || metadata.token_endpoint.is_none()
        || !metadata
            .code_challenge_methods_supported
            .as_ref()
            .is_some_and(|m| m.iter().any(|v| v == "S256"))
    {
        return Err(
            "Authorization server must advertise authorization/token endpoints and PKCE S256"
                .into(),
        );
    }
    let mut settings = AuthConfig::with_profile(AuthSecurityProfile::L2Strong);
    settings.mode = AuthMode::Jwks;
    settings.issuer = Some(issuer.clone());
    settings.audience = Some(config.resource_url.clone());
    settings.jwks_url = Some(metadata.jwks_uri.clone());
    settings.required_scopes = vec![READ_SCOPE.into()];
    settings.actor_claim = "sub".into();
    settings.clock_skew_s = 0.0;
    let authenticator = Arc::new(
        Authenticator::new(settings).map_err(|_| "Invalid OAuth validation configuration")?,
    );
    let entry = IssuerEntry::from_metadata_source(
        "/mcp",
        AuthorizationServerMetadataSource::OidcDiscovery(metadata),
        "postgres-mcp",
        vec![READ_SCOPE.into()],
        config.allowed_client_ids.clone(),
        authenticator.clone(),
        Some(config.resource_url.clone()),
    )
    .map_err(|_| "Invalid OAuth resource metadata")?;
    let layer = AuthSurfaceBuilder::single_issuer(config.public_base_url()?, entry)
        .allow_insecure_http(config.allow_loopback_http)
        .public_path("/healthz")
        .public_path("/readyz")
        .build()
        .map_err(|_| "Invalid OAuth authentication surface")?;
    Ok(Authentication {
        authenticator,
        layer,
        issuer,
    })
}
