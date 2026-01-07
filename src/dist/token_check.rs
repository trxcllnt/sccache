use crate::{
    config::{
        INSECURE_DIST_CLIENT_TOKEN,
        scheduler::{ClientAuth, ProxyTokenDecodeConfig},
    },
    dist::http::{ClientAuthCheck, ClientClaims},
    errors::*,
    util::new_reqwest_client,
};
use anyhow::{Context, bail};
use async_trait::async_trait;
use futures::{TryFutureExt, lock::Mutex};
use jwt::jwk::JwkSet;

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};

// Check a token is equal to a fixed string
pub struct EqCheck {
    s: String,
}

#[async_trait]
impl ClientAuthCheck for EqCheck {
    async fn check(&self, _: &SocketAddr, token: &str) -> Result<ClientClaims> {
        if self.s == token {
            Ok(HashMap::with_capacity(0))
        } else {
            Err(anyhow!("Fixed token mismatch"))
        }
    }
}

impl EqCheck {
    pub fn new(s: String) -> Self {
        Self { s }
    }
}

enum ProxyTokenDecoder {
    None,
    Jwt(ValidJWTCheck),
}

impl ProxyTokenDecoder {
    async fn decode(&self, response: &str) -> Result<ClientClaims> {
        match self {
            Self::Jwt(jwt) => jwt
                .check_jwt_validity(response)
                .await
                .inspect_err(|e| tracing::warn!("JWT validation failed: {}", e)),
            _ => Ok(HashMap::with_capacity(0)),
        }
    }
}

struct ProxyTokenErrorRateLimiter {
    errors: Mutex<HashMap<IpAddr, Vec<Instant>>>,
    rate_limit_on_error_count: usize,
    rate_limit_on_error_window_size_secs: Duration,
}

impl ProxyTokenErrorRateLimiter {
    pub fn new(
        rate_limit_on_error_count: usize,
        rate_limit_on_error_window_size_secs: Duration,
    ) -> Self {
        Self {
            errors: Default::default(),
            rate_limit_on_error_window_size_secs,
            rate_limit_on_error_count,
        }
    }

    pub async fn check(&self, remote_addr: &IpAddr) -> Result<()> {
        let now = Instant::now();
        let mut errors = self.errors.lock().await;
        if let Some(window) = errors.get_mut(remote_addr) {
            self.truncate_window(&now, window);
            if window.is_empty() {
                errors.remove(remote_addr);
            } else if window.len() >= self.rate_limit_on_error_count {
                bail!("Too many unauthenticated requests within time window")
            }
        }
        Ok(())
    }

    pub async fn record(&self, remote_addr: IpAddr) {
        let now = Instant::now();
        let mut errors = self.errors.lock().await;
        let window = errors.entry(remote_addr).or_default();
        self.truncate_window(&now, window);
        window.push(now);
    }

    fn truncate_window(&self, now: &Instant, window: &mut Vec<Instant>) {
        // Drop all error timestamps older than `rate_limit_on_error_window_size_secs`
        window.retain(|then| now.duration_since(*then) < self.rate_limit_on_error_window_size_secs);
        // Drop all but the last `rate_limit_on_error_count` number of error timestamps
        window.drain(0..window.len().saturating_sub(self.rate_limit_on_error_count));
    }
}

// Don't check a token is valid (it may not even be a JWT) just forward it to
// an API and check for success
pub struct ProxyTokenCheck {
    cache: Mutex<HashMap<String, (Instant, ClientClaims)>>,
    client: reqwest::Client,
    decoder: ProxyTokenDecoder,
    rate_limiter: ProxyTokenErrorRateLimiter,
    token_ttl: Option<Duration>,
    url: String,
}

#[async_trait]
impl ClientAuthCheck for ProxyTokenCheck {
    async fn check(&self, remote_addr: &SocketAddr, token: &str) -> Result<ClientClaims> {
        let remote_addr = remote_addr.ip();
        match self
            .rate_limiter
            .check(&remote_addr)
            .and_then(|_| self.check_token_with_forwarding(token))
            .await
        {
            Ok(claims) => Ok(claims),
            Err(err) => {
                tracing::warn!("Proxying token validation failed: {err}");
                self.rate_limiter.record(remote_addr).await;
                Err(err)
            }
        }
    }
}

impl ProxyTokenCheck {
    pub async fn new(
        url: String,
        token_ttl: Option<u64>,
        rate_limit_on_error_count: Option<usize>,
        rate_limit_on_error_window_size_secs: Option<u64>,
        decode: Option<ProxyTokenDecodeConfig>,
    ) -> Result<Self> {
        let decoder = match decode {
            Some(ProxyTokenDecodeConfig::JwtDecoder {
                audience,
                issuer,
                jwks_url,
                claims,
            }) => ProxyTokenDecoder::Jwt(
                ValidJWTCheck::new(audience, issuer, &jwks_url, claims)
                    .await
                    .context("Failed to create a checker for valid JWTs")?,
            ),
            _ => ProxyTokenDecoder::None,
        };

        Ok(Self {
            cache: Mutex::new(HashMap::new()),
            client: new_reqwest_client(None),
            decoder,
            rate_limiter: ProxyTokenErrorRateLimiter::new(
                rate_limit_on_error_count.unwrap_or(5),
                Duration::from_secs(rate_limit_on_error_window_size_secs.unwrap_or(60)),
            ),
            token_ttl: token_ttl.map(Duration::from_secs),
            url,
        })
    }

    async fn check_token_with_forwarding(&self, token: &str) -> Result<ClientClaims> {
        // If the token is cached and not cache has not expired, return it
        if let Some(token_ttl) = self.token_ttl {
            let mut cache = self.cache.lock().await;
            let entry = cache
                .get(token)
                .filter(|(cached_at, _)| cached_at.elapsed() < token_ttl)
                .map(|(_, claims)| claims);
            if let Some(claims) = entry {
                return Ok(claims.clone());
            } else {
                cache.remove(token);
            }
        }

        tracing::trace!("Validating token by forwarding to {}", self.url);

        // Make a request to another API, which as a side effect should actually check the token
        let res = self
            .client
            .get(&self.url)
            .bearer_auth(token)
            .send()
            .await
            .context("Failed to make request to proxying url")?;

        if !res.status().is_success() {
            bail!("Token forwarded to {} returned {}", self.url, res.status());
        }

        let mime = res
            .headers()
            .get(http::header::CONTENT_TYPE)
            .map(|t| t.to_str().unwrap_or("application/text"));

        let auth = match mime.unwrap_or("application/text") {
            "application/json" => {
                let json = res.json::<serde_json::Value>().await?;
                json.as_object()
                    .and_then(|o| o.get("token"))
                    .and_then(|s| s.as_str())
                    .map(|s| s.to_owned())
                    .unwrap_or_default()
            }
            _ => res.text().await?,
        };

        let claims = self.decoder.decode(&auth).await.or_else(|err| {
            tracing::warn!("Failed to decode token: {err}");
            Result::Ok(HashMap::with_capacity(0))
        })?;

        // Cache the token
        if self.token_ttl.is_some() {
            self.cache
                .lock()
                .await
                .insert(token.to_owned(), (Instant::now(), claims.clone()));
        }

        Ok(claims)
    }
}

// Check a JWT is valid
pub struct ValidJWTCheck {
    audience: String,
    issuer: String,
    keys: HashMap<String, jwt::DecodingKey>,
    claims: Vec<String>,
}

#[async_trait]
impl ClientAuthCheck for ValidJWTCheck {
    async fn check(&self, _: &SocketAddr, token: &str) -> Result<ClientClaims> {
        match self.check_jwt_validity(token).await {
            Ok(claims) => Ok(claims),
            Err(e) => {
                tracing::warn!("JWT validation failed: {}", e);
                Err(e)
            }
        }
    }
}

impl ValidJWTCheck {
    pub async fn new(
        audience: String,
        issuer: String,
        jwks_url: &str,
        claims: Option<Vec<String>>,
    ) -> Result<Self> {
        let res = reqwest::get(jwks_url)
            .await
            .context("Failed to make request to JWKs url")?;
        if !res.status().is_success() {
            bail!("Could not retrieve JWKs, HTTP error: {}", res.status())
        }

        let keys = res
            .json::<JwkSet>()
            .await
            .context("Failed to parse JWKs json")?
            .keys
            .iter()
            .filter_map(|jwk| {
                jwk.common.key_id.as_ref().and_then(|kid| {
                    jwt::DecodingKey::from_jwk(jwk)
                        .ok()
                        .map(|key| (kid.clone(), key))
                })
            })
            .collect();

        Ok(Self {
            audience,
            issuer,
            keys,
            claims: claims.unwrap_or_default(),
        })
    }

    async fn check_jwt_validity(&self, token: &str) -> Result<ClientClaims> {
        tracing::trace!("Validating JWT");

        let header = jwt::decode_header(token).context("Could not decode jwt header")?;

        // Prepare validation
        let mut validation = jwt::Validation::new(header.alg);
        validation.set_audience(&[&self.audience]);
        validation.set_issuer(&[&self.issuer]);

        let decode = |key: &jwt::DecodingKey| {
            let jwt::TokenData { claims, .. } =
                jwt::decode::<serde_json::Value>(token, key, &validation)?;
            Ok(claims
                .as_object()
                .map(|obj| {
                    self.claims
                        .iter()
                        .filter_map(|key| {
                            obj.get(key).map(|val| {
                                (
                                    key.clone(),
                                    val.as_str()
                                        .map(|val| val.to_string())
                                        .unwrap_or_else(|| val.to_string()),
                                )
                            })
                        })
                        .collect()
                })
                .unwrap_or_default())
        };

        // Decode the JWT and return the claims

        if let Some(key) = header.kid.as_ref().and_then(|kid| self.keys.get(kid)) {
            decode(key)
        } else {
            self.keys
                .values()
                .try_for_each(|key| {
                    if let Ok(claims) = decode(key) {
                        std::ops::ControlFlow::Break(claims)
                    } else {
                        std::ops::ControlFlow::Continue(())
                    }
                })
                .map_break(Ok)
                .break_value()
                .unwrap_or_else(|| bail!("Unable to validate and decode jwt"))
        }
    }
}

pub async fn new_client_auth_check(client_auth: ClientAuth) -> Result<Box<dyn ClientAuthCheck>> {
    Ok(match client_auth {
        ClientAuth::Insecure => Box::new(EqCheck::new(INSECURE_DIST_CLIENT_TOKEN.to_owned())),
        ClientAuth::Token { token } => Box::new(EqCheck::new(token)),
        ClientAuth::JwtValidate {
            audience,
            issuer,
            jwks_url,
            claims,
        } => Box::new(
            ValidJWTCheck::new(audience, issuer, &jwks_url, claims)
                .await
                .context("Failed to create a checker for valid JWTs")?,
        ),
        ClientAuth::ProxyToken {
            url,
            cache_secs,
            decode,
            rate_limit_on_error_count,
            rate_limit_on_error_window_size_secs,
        } => Box::new(
            ProxyTokenCheck::new(
                url,
                cache_secs,
                rate_limit_on_error_count,
                rate_limit_on_error_window_size_secs,
                decode,
            )
            .await?,
        ),
    })
}
