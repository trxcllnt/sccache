use crate::{
    config::{scheduler::ClientAuth, INSECURE_DIST_CLIENT_TOKEN},
    dist::http::{ClientAuthCheck, ClientVisibleMsg},
    util::{new_reqwest_client, BASE64_URL_SAFE_ENGINE},
};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use base64::Engine;
use futures::lock::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::result::Result as StdResult;
use std::time::{Duration, Instant};

// https://auth0.com/docs/jwks
#[derive(Debug, Serialize, Deserialize)]
pub struct Jwks {
    pub keys: Vec<Jwk>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Jwk {
    pub kid: String,
    kty: String,
    n: String,
    e: String,
}

impl Jwk {
    // https://github.com/lawliet89/biscuit/issues/96#issuecomment-399149872
    pub fn to_der_pkcs1(&self) -> Result<Vec<u8>> {
        if self.kty != "RSA" {
            bail!("Cannot handle non-RSA JWK")
        }

        // JWK is big-endian, openssl bignum from_slice is big-endian
        let n = BASE64_URL_SAFE_ENGINE
            .decode(&self.n)
            .context("Failed to base64 decode n")?;
        let e = BASE64_URL_SAFE_ENGINE
            .decode(&self.e)
            .context("Failed to base64 decode e")?;
        let n_bn = openssl::bn::BigNum::from_slice(&n)
            .context("Failed to create openssl bignum from n")?;
        let e_bn = openssl::bn::BigNum::from_slice(&e)
            .context("Failed to create openssl bignum from e")?;
        let pubkey = openssl::rsa::Rsa::from_public_components(n_bn, e_bn)
            .context("Failed to create pubkey from n and e")?;
        let der: Vec<u8> = pubkey
            .public_key_to_der_pkcs1()
            .context("Failed to convert public key to der pkcs1")?;
        Ok(der)
    }
}

// Check a token is equal to a fixed string
pub struct EqCheck {
    s: String,
}

#[async_trait]
impl ClientAuthCheck for EqCheck {
    async fn check(&self, token: &str) -> StdResult<(), ClientVisibleMsg> {
        if self.s == token {
            Ok(())
        } else {
            Err(ClientVisibleMsg::from_nonsensitive(
                "Fixed token mismatch".to_owned(),
            ))
        }
    }
}

impl EqCheck {
    pub fn new(s: String) -> Self {
        Self { s }
    }
}

// Don't check a token is valid (it may not even be a JWT) just forward it to
// an API and check for success
pub struct ProxyTokenCheck {
    client: reqwest::Client,
    maybe_auth_cache: Option<Mutex<(HashMap<String, Instant>, Duration)>>,
    url: String,
}

#[async_trait]
impl ClientAuthCheck for ProxyTokenCheck {
    async fn check(&self, token: &str) -> StdResult<(), ClientVisibleMsg> {
        match self.check_token_with_forwarding(token).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!("Proxying token validation failed: {}", e);
                Err(ClientVisibleMsg::from_nonsensitive(
                    "Validation with token forwarding failed".to_owned(),
                ))
            }
        }
    }
}

impl ProxyTokenCheck {
    pub fn new(url: String, cache_secs: Option<u64>) -> Self {
        let maybe_auth_cache: Option<Mutex<(HashMap<String, Instant>, Duration)>> =
            cache_secs.map(|secs| Mutex::new((HashMap::new(), Duration::from_secs(secs))));
        Self {
            client: new_reqwest_client(None),
            maybe_auth_cache,
            url,
        }
    }

    async fn check_token_with_forwarding(&self, token: &str) -> Result<()> {
        // If the token is cached and not cache has not expired, return it
        if let Some(ref auth_cache) = self.maybe_auth_cache {
            let mut auth_cache = auth_cache.lock().await;
            let (ref mut auth_cache, cache_duration) = *auth_cache;
            if let Some(cached_at) = auth_cache.get(token) {
                if cached_at.elapsed() < cache_duration {
                    return Ok(());
                }
            }
            auth_cache.remove(token);
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
        // Cache the token
        if let Some(ref auth_cache) = self.maybe_auth_cache {
            let mut auth_cache = auth_cache.lock().await;
            let (ref mut auth_cache, _) = *auth_cache;
            auth_cache.insert(token.to_owned(), Instant::now());
        }
        Ok(())
    }
}

// Check a JWT is valid
pub struct ValidJWTCheck {
    audience: String,
    issuer: String,
    kid_to_pkcs1: HashMap<String, Vec<u8>>,
}

#[async_trait]
impl ClientAuthCheck for ValidJWTCheck {
    async fn check(&self, token: &str) -> StdResult<(), ClientVisibleMsg> {
        match self.check_jwt_validity(token).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!("JWT validation failed: {}", e);
                Err(ClientVisibleMsg::from_nonsensitive(
                    "JWT could not be validated".to_owned(),
                ))
            }
        }
    }
}

impl ValidJWTCheck {
    pub async fn new(audience: String, issuer: String, jwks_url: &str) -> Result<Self> {
        let res = reqwest::get(jwks_url)
            .await
            .context("Failed to make request to JWKs url")?;
        if !res.status().is_success() {
            bail!("Could not retrieve JWKs, HTTP error: {}", res.status())
        }
        let jwks: Jwks = res.json().await.context("Failed to parse JWKs json")?;
        let kid_to_pkcs1 = jwks
            .keys
            .into_iter()
            .map(|k| k.to_der_pkcs1().map(|pkcs1| (k.kid, pkcs1)))
            .collect::<Result<_>>()
            .context("Failed to convert JWKs into pkcs1")?;
        Ok(Self {
            audience,
            issuer,
            kid_to_pkcs1,
        })
    }

    async fn check_jwt_validity(&self, token: &str) -> Result<()> {
        let header = jwt::decode_header(token).context("Could not decode jwt header")?;
        tracing::trace!("Validating JWT in scheduler");
        // Prepare validation
        let kid = header.kid.context("No kid found")?;
        let pkcs1 = jwt::DecodingKey::from_rsa_der(
            self.kid_to_pkcs1
                .get(&kid)
                .context("kid not found in jwks")?,
        );
        let mut validation = jwt::Validation::new(header.alg);
        validation.set_audience(&[&self.audience]);
        validation.set_issuer(&[&self.issuer]);
        #[derive(Deserialize)]
        struct Claims {}
        // Decode the JWT, discarding any claims - we just care about validity
        let _tokendata = jwt::decode::<Claims>(token, &pkcs1, &validation)
            .context("Unable to validate and decode jwt")?;
        Ok(())
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
        } => Box::new(
            ValidJWTCheck::new(audience, issuer, &jwks_url)
                .await
                .context("Failed to create a checker for valid JWTs")?,
        ),
        ClientAuth::ProxyToken { url, cache_secs } => {
            Box::new(ProxyTokenCheck::new(url, cache_secs))
        }
    })
}
