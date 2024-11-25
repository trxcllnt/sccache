// Copyright 2016 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(feature = "dist-client")]
pub use self::client::Client;
#[cfg(feature = "dist-server")]
pub use self::server::{
    ClientAuthCheck, ClientVisibleMsg, JWTJobAuthorizer, ServerAuthCheck, HEARTBEAT_ERROR_INTERVAL,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT,
};
#[cfg(feature = "dist-server")]
pub use self::server::{Scheduler, Server};

use std::env;
use std::time::Duration;

/// Default timeout for connections to an sccache-dist server
const DEFAULT_DIST_CONNECT_TIMEOUT: u64 = 5;

/// Timeout for connections to an sccache-dist server
pub fn get_dist_connect_timeout() -> Duration {
    Duration::new(
        env::var("SCCACHE_DIST_CONNECT_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_DIST_CONNECT_TIMEOUT),
        0,
    )
}

/// Default timeout for compile requests to an sccache-dist server
const DEFAULT_DIST_REQUEST_TIMEOUT: u64 = 600;

/// Timeout for compile requests to an sccache-dist server
pub fn get_dist_request_timeout() -> Duration {
    Duration::new(
        env::var("SCCACHE_DIST_REQUEST_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_DIST_REQUEST_TIMEOUT),
        0,
    )
}

mod common {
    use reqwest::header;
    use serde::{Deserialize, Serialize};
    #[cfg(feature = "dist-server")]
    use std::collections::HashMap;
    use std::fmt;

    use crate::dist;

    use crate::errors::*;

    // Note that content-length is necessary due to https://github.com/tiny-http/tiny-http/issues/147
    pub trait ReqwestRequestBuilderExt: Sized {
        fn bincode<T: serde::Serialize + ?Sized>(self, bincode: &T) -> Result<Self>;
        fn bytes(self, bytes: Vec<u8>) -> Self;
    }
    impl ReqwestRequestBuilderExt for reqwest::blocking::RequestBuilder {
        fn bincode<T: serde::Serialize + ?Sized>(self, bincode: &T) -> Result<Self> {
            let bytes =
                bincode::serialize(bincode).context("Failed to serialize body to bincode")?;
            Ok(self.bytes(bytes))
        }
        fn bytes(self, bytes: Vec<u8>) -> Self {
            self.header(
                header::CONTENT_TYPE,
                mime::APPLICATION_OCTET_STREAM.to_string(),
            )
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(bytes)
        }
    }
    impl ReqwestRequestBuilderExt for reqwest::RequestBuilder {
        fn bincode<T: serde::Serialize + ?Sized>(self, bincode: &T) -> Result<Self> {
            let bytes =
                bincode::serialize(bincode).context("Failed to serialize body to bincode")?;
            Ok(self.bytes(bytes))
        }
        fn bytes(self, bytes: Vec<u8>) -> Self {
            self.header(
                header::CONTENT_TYPE,
                mime::APPLICATION_OCTET_STREAM.to_string(),
            )
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(bytes)
        }
    }

    #[cfg(any(feature = "dist-client", feature = "dist-server"))]
    pub async fn bincode_req_fut<T: serde::de::DeserializeOwned + 'static>(
        req: reqwest::RequestBuilder,
    ) -> Result<T> {
        // Work around tiny_http issue #151 by disabling HTTP pipeline with
        // `Connection: close`.
        let res = match req.header(header::CONNECTION, "close").send().await {
            Ok(res) => res,
            Err(err) => {
                error!("Response error: err={err}");
                return Err(err.into());
            }
        };

        let url = res.url().clone();
        let status = res.status();
        let bytes = match res.bytes().await {
            Ok(b) => b,
            Err(err) => {
                error!("Body error: url={url}, status={status}, err={err}");
                return Err(err.into());
            }
        };
        trace!("Response: url={url}, status={status}, body={}", bytes.len());
        if !status.is_success() {
            let errmsg = format!(
                "Error {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            );
            if status.is_client_error() {
                anyhow::bail!(HttpClientError(errmsg));
            } else {
                anyhow::bail!(errmsg);
            }
        } else {
            Ok(bincode::deserialize(&bytes)?)
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct JobJwt {
        pub exp: u64,
        pub job_id: dist::JobId,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum AllocJobHttpResponse {
        Success {
            job_alloc: dist::JobAlloc,
            need_toolchain: bool,
            cert_digest: Vec<u8>,
        },
        Fail {
            msg: String,
        },
    }
    impl AllocJobHttpResponse {
        #[cfg(feature = "dist-server")]
        pub fn from_alloc_job_result(
            res: dist::AllocJobResult,
            certs: &HashMap<dist::ServerId, (Vec<u8>, Vec<u8>)>,
        ) -> Self {
            match res {
                dist::AllocJobResult::Success {
                    job_alloc,
                    need_toolchain,
                } => {
                    if let Some((digest, _)) = certs.get(&job_alloc.server_id) {
                        AllocJobHttpResponse::Success {
                            job_alloc,
                            need_toolchain,
                            cert_digest: digest.to_owned(),
                        }
                    } else {
                        AllocJobHttpResponse::Fail {
                            msg: format!(
                                "missing certificates for server {}",
                                job_alloc.server_id.addr()
                            ),
                        }
                    }
                }
                dist::AllocJobResult::Fail { msg } => AllocJobHttpResponse::Fail { msg },
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ServerCertificateHttpResponse {
        pub cert_digest: Vec<u8>,
        pub cert_pem: Vec<u8>,
    }

    #[derive(Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct HeartbeatServerHttpRequest {
        pub jwt_key: Vec<u8>,
        pub num_cpus: usize,
        pub max_per_core_load: f64,
        pub server_nonce: dist::ServerNonce,
        pub cert_digest: Vec<u8>,
        pub cert_pem: Vec<u8>,
        pub num_assigned_jobs: usize,
        pub num_active_jobs: usize,
    }
    // cert_pem is quite long so elide it (you can retrieve it by hitting the server url anyway)
    impl fmt::Debug for HeartbeatServerHttpRequest {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let HeartbeatServerHttpRequest {
                jwt_key,
                num_cpus,
                max_per_core_load,
                server_nonce,
                cert_digest,
                cert_pem,
                num_assigned_jobs: assigned,
                num_active_jobs: active,
            } = self;
            write!(f,
                "HeartbeatServerHttpRequest {{ jwt_key: {:?}, num_cpus: {:?}, max_per_core_load: {:?}, server_nonce: {:?}, cert_digest: {:?}, cert_pem: [...{} bytes...], jobs: {{ assigned: {:?}, active: {:?} }} }}",
                                               jwt_key,       num_cpus,       max_per_core_load,       server_nonce,       cert_digest,       cert_pem.len(),                      assigned,       active)
        }
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct UpdateJobStateHttpRequest {
        pub num_assigned_jobs: usize,
        pub num_active_jobs: usize,
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RunJobHttpRequest {
        pub command: dist::CompileCommand,
        pub outputs: Vec<String>,
    }
}

pub mod urls {
    use crate::dist::{JobId, ServerId};

    pub fn scheduler_alloc_job(scheduler_url: &reqwest::Url) -> reqwest::Url {
        scheduler_url
            .join("/api/v1/scheduler/alloc_job")
            .expect("failed to create alloc job url")
    }
    pub fn scheduler_server_certificate(
        scheduler_url: &reqwest::Url,
        server_id: ServerId,
    ) -> reqwest::Url {
        scheduler_url
            .join(&format!(
                "/api/v1/scheduler/server_certificate/{}",
                server_id.addr()
            ))
            .expect("failed to create server certificate url")
    }
    pub fn scheduler_heartbeat_server(scheduler_url: &reqwest::Url) -> reqwest::Url {
        scheduler_url
            .join("/api/v1/scheduler/heartbeat_server")
            .expect("failed to create heartbeat url")
    }
    pub fn scheduler_job_state(scheduler_url: &reqwest::Url) -> reqwest::Url {
        scheduler_url
            .join("/api/v1/scheduler/job_state")
            .expect("failed to create job state url")
    }
    pub fn scheduler_status(scheduler_url: &reqwest::Url) -> reqwest::Url {
        scheduler_url
            .join("/api/v1/scheduler/status")
            .expect("failed to create alloc job url")
    }

    pub fn server_reserve_job(server_id: ServerId) -> reqwest::Url {
        let url = format!("https://{}/api/v1/distserver/reserve_job", server_id.addr());
        reqwest::Url::parse(&url).expect("failed to create reserve job url")
    }
    pub fn server_assign_job(server_id: ServerId) -> reqwest::Url {
        let url = format!("https://{}/api/v1/distserver/assign_job", server_id.addr(),);
        reqwest::Url::parse(&url).expect("failed to create assign job url")
    }
    pub fn server_submit_toolchain(server_id: ServerId, job_id: JobId) -> reqwest::Url {
        let url = format!(
            "https://{}/api/v1/distserver/submit_toolchain/{}",
            server_id.addr(),
            job_id
        );
        reqwest::Url::parse(&url).expect("failed to create submit toolchain url")
    }
    pub fn server_run_job(server_id: ServerId, job_id: JobId) -> reqwest::Url {
        let url = format!(
            "https://{}/api/v1/distserver/run_job/{}",
            server_id.addr(),
            job_id
        );
        reqwest::Url::parse(&url).expect("failed to create run job url")
    }
}

#[cfg(feature = "dist-server")]
mod server {

    use async_trait::async_trait;
    use axum::{
        body::Bytes,
        extract::{
            ConnectInfo, DefaultBodyLimit, Extension, FromRequest, FromRequestParts, Path, Request,
        },
        http::{request::Parts, HeaderMap, Method, StatusCode, Uri},
        response::{IntoResponse, Response},
        routing, RequestPartsExt, Router,
    };

    use axum_extra::{
        headers::{authorization::Bearer, Authorization},
        TypedHeader,
    };
    use bytes::Buf;

    use async_compression::tokio::bufread::ZlibDecoder as ZlibReadDecoder;
    use futures::{lock::Mutex, TryStreamExt};

    use hyper_util::rt::{TokioExecutor, TokioIo};

    use once_cell::sync::Lazy;
    use openssl::ssl::{Ssl, SslAcceptor, SslMethod};

    use rand::{rngs::OsRng, RngCore};

    use serde_json::json;

    use std::{
        borrow::{Borrow, BorrowMut},
        collections::HashMap,
        io,
        net::SocketAddr,
        result::Result as StdResult,
        sync::Arc,
        time::Duration,
    };

    use tokio::{io::AsyncReadExt, net::TcpListener};
    use tokio_openssl::SslStream;
    use tokio_util::io::StreamReader;
    use tower::{Service, ServiceBuilder, ServiceExt};
    use tower_http::{
        request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
        sensitive_headers::{SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer},
        trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
    };

    use super::common::{
        bincode_req_fut, AllocJobHttpResponse, HeartbeatServerHttpRequest, JobJwt,
        ReqwestRequestBuilderExt, RunJobHttpRequest, ServerCertificateHttpResponse,
        UpdateJobStateHttpRequest,
    };
    use crate::dist::{
        self,
        http::{get_dist_connect_timeout, get_dist_request_timeout, urls},
        AssignJobResult, HeartbeatServerResult, JobAuthorizer, JobId, SchedulerIncoming,
        SchedulerOutgoing, ServerId, ServerIncoming, ServerNonce, ServerOutgoing, Toolchain,
        UpdateJobStateResult,
    };
    use crate::util::new_reqwest_client;

    use crate::errors::*;

    pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
    pub const HEARTBEAT_ERROR_INTERVAL: Duration = Duration::from_secs(3);
    pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

    fn create_https_cert_and_privkey(addr: SocketAddr) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let rsa_key = openssl::rsa::Rsa::<openssl::pkey::Private>::generate(2048)
            .context("failed to generate rsa privkey")?;
        let privkey_pem = rsa_key
            .private_key_to_pem()
            .context("failed to create pem from rsa privkey")?;
        let privkey: openssl::pkey::PKey<openssl::pkey::Private> =
            openssl::pkey::PKey::from_rsa(rsa_key)
                .context("failed to create openssl pkey from rsa privkey")?;
        let mut builder =
            openssl::x509::X509::builder().context("failed to create x509 builder")?;

        // Populate the certificate with the necessary parts, mostly from mkcert in openssl
        builder
            .set_version(2)
            .context("failed to set x509 version")?;
        let serial_number = openssl::bn::BigNum::from_u32(0)
            .and_then(|bn| bn.to_asn1_integer())
            .context("failed to create openssl asn1 0")?;
        builder
            .set_serial_number(serial_number.as_ref())
            .context("failed to set x509 serial number")?;
        let not_before = openssl::asn1::Asn1Time::days_from_now(0)
            .context("failed to create openssl not before asn1")?;
        builder
            .set_not_before(not_before.as_ref())
            .context("failed to set not before on x509")?;
        let not_after = openssl::asn1::Asn1Time::days_from_now(365)
            .context("failed to create openssl not after asn1")?;
        builder
            .set_not_after(not_after.as_ref())
            .context("failed to set not after on x509")?;
        builder
            .set_pubkey(privkey.as_ref())
            .context("failed to set pubkey for x509")?;

        let mut name = openssl::x509::X509Name::builder()?;
        name.append_entry_by_nid(openssl::nid::Nid::COMMONNAME, &addr.to_string())?;
        let name = name.build();

        builder
            .set_subject_name(&name)
            .context("failed to set subject name")?;
        builder
            .set_issuer_name(&name)
            .context("failed to set issuer name")?;

        // Add the SubjectAlternativeName
        let extension = openssl::x509::extension::SubjectAlternativeName::new()
            .ip(&addr.ip().to_string())
            .build(&builder.x509v3_context(None, None))
            .context("failed to build SAN extension for x509")?;
        builder
            .append_extension(extension)
            .context("failed to append SAN extension for x509")?;

        // Add ExtendedKeyUsage
        let ext_key_usage = openssl::x509::extension::ExtendedKeyUsage::new()
            .server_auth()
            .build()
            .context("failed to build EKU extension for x509")?;
        builder
            .append_extension(ext_key_usage)
            .context("fails to append EKU extension for x509")?;

        // Finish the certificate
        builder
            .sign(&privkey, openssl::hash::MessageDigest::sha1())
            .context("failed to sign x509 with sha1")?;
        let cert: openssl::x509::X509 = builder.build();
        let cert_pem = cert.to_pem().context("failed to create pem from x509")?;
        let cert_digest = cert
            .digest(openssl::hash::MessageDigest::sha256())
            .context("failed to create digest of x509 certificate")?
            .as_ref()
            .to_owned();

        Ok((cert_digest, cert_pem, privkey_pem))
    }

    // Messages that are non-sensitive and can be sent to the client
    #[derive(Debug)]
    pub struct ClientVisibleMsg(String);
    impl ClientVisibleMsg {
        pub fn from_nonsensitive(s: String) -> Self {
            ClientVisibleMsg(s)
        }
    }

    pub trait ClientAuthCheck: Send + Sync {
        fn check(&self, token: &str) -> StdResult<(), ClientVisibleMsg>;
    }
    pub type ServerAuthCheck = Box<dyn Fn(&str) -> Option<ServerId> + Send + Sync>;

    const JWT_KEY_LENGTH: usize = 256 / 8;
    static JWT_HEADER: Lazy<jwt::Header> = Lazy::new(|| jwt::Header::new(jwt::Algorithm::HS256));
    static JWT_VALIDATION: Lazy<jwt::Validation> = Lazy::new(|| {
        let mut validation = jwt::Validation::new(jwt::Algorithm::HS256);
        validation.leeway = 0;
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation
    });

    fn with_request_tracing(app: Router) -> Router {
        // Mark these headers as sensitive so they don't show in logs
        let headers_to_redact: Arc<[_]> = Arc::new([
            http::header::AUTHORIZATION,
            http::header::PROXY_AUTHORIZATION,
            http::header::COOKIE,
            http::header::SET_COOKIE,
        ]);
        app.layer(
            ServiceBuilder::new()
                .layer(SetSensitiveRequestHeadersLayer::from_shared(Arc::clone(
                    &headers_to_redact,
                )))
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(DefaultMakeSpan::new().include_headers(true))
                        .on_response(DefaultOnResponse::new().include_headers(true)),
                )
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(SetSensitiveResponseHeadersLayer::from_shared(
                    headers_to_redact,
                )),
        )
    }

    fn get_header_value<'a>(headers: &'a HeaderMap, name: &'a str) -> Option<&'a str> {
        if let Some(header) = headers.get(name) {
            if let Ok(header) = header.to_str() {
                return Some(header);
            }
        }
        None
    }

    /// Return `content` as either a bincode or json encoded `Response`
    /// depending on the Accept header in `request`.
    pub fn accepts_response<T>(headers: &HeaderMap, content: &T) -> (StatusCode, Vec<u8>)
    where
        T: serde::Serialize,
    {
        if let Some(header) = headers.get("Accept") {
            // This is the only function we use from rouille.
            // Maybe we can find a replacement?
            match rouille::input::priority_header_preferred(
                header.to_str().unwrap_or("*/*"),
                ["application/octet-stream", "application/json"]
                    .iter()
                    .cloned(),
            ) {
                // application/octet-stream
                Some(0) => match bincode::serialize(content) {
                    Ok(body) => (StatusCode::OK, body),
                    Err(err) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to serialize response body: {err}").into_bytes(),
                    ),
                },
                // application/json
                Some(1) => (StatusCode::OK, json!(content).as_str().unwrap().into()),
                _ => (
                    StatusCode::BAD_REQUEST,
                    "Request must accept application/json or application/octet-stream".into(),
                ),
            }
        } else {
            (
                StatusCode::BAD_REQUEST,
                "Request must accept application/json or application/octet-stream".into(),
            )
        }
    }

    fn anyhow_response(
        method: Method,
        uri: Uri,
        // the last argument must be the error itself
        err: anyhow::Error,
    ) -> (StatusCode, Vec<u8>) {
        let msg = format!("sccache: `{method} {uri}` failed with {err}");
        tracing::error!("{}", msg);
        (StatusCode::INTERNAL_SERVER_ERROR, msg.into_bytes())
    }

    fn unwrap_infallible<T>(result: StdResult<T, std::convert::Infallible>) -> T {
        match result {
            Ok(value) => value,
            Err(err) => match err {},
        }
    }

    struct Bincode<T>(T);

    #[async_trait]
    impl<S, T> FromRequest<S> for Bincode<T>
    where
        Bytes: FromRequest<S>,
        S: Send + Sync,
        T: serde::de::DeserializeOwned,
    {
        type Rejection = Response;

        async fn from_request(req: Request, state: &S) -> StdResult<Self, Self::Rejection> {
            let data = match get_header_value(req.headers(), "Content-Type") {
                Some("application/octet-stream") => Bytes::from_request(req, state)
                    .await
                    .map_err(IntoResponse::into_response)?
                    .to_vec(),
                _ => return Err((StatusCode::BAD_REQUEST, "Wrong content type").into_response()),
            };

            let data = bincode::deserialize_from::<_, T>(data.reader())
                .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()).into_response())?;

            Ok(Self(data))
        }
    }

    // Generation and verification of job auth
    pub struct JWTJobAuthorizer {
        server_key: Vec<u8>,
    }

    impl JWTJobAuthorizer {
        pub fn new(server_key: Vec<u8>) -> Box<Self> {
            Box::new(Self { server_key })
        }
    }

    impl dist::JobAuthorizer for JWTJobAuthorizer {
        fn generate_token(&self, job_id: JobId) -> Result<String> {
            let claims = JobJwt { exp: 0, job_id };
            let key = jwt::EncodingKey::from_secret(&self.server_key);
            jwt::encode(&JWT_HEADER, &claims, &key)
                .map_err(|e| anyhow!("Failed to create JWT for job: {}", e))
        }
        fn verify_token(&self, job_id: JobId, token: &str) -> Result<()> {
            let valid_claims = JobJwt { exp: 0, job_id };
            let key = jwt::DecodingKey::from_secret(&self.server_key);
            jwt::decode(token, &key, &JWT_VALIDATION)
                .map_err(|e| anyhow!("JWT decode failed: {}", e))
                .and_then(|res| {
                    fn identical_t<T>(_: &T, _: &T) {}
                    identical_t(&res.claims, &valid_claims);
                    if res.claims == valid_claims {
                        Ok(())
                    } else {
                        Err(anyhow!("mismatched claims"))
                    }
                })
        }
    }

    #[test]
    fn test_job_token_verification() {
        let ja = JWTJobAuthorizer::new(vec![1, 2, 2]);

        let job_id = JobId(55);
        let token = ja.generate_token(job_id).unwrap();

        let job_id2 = JobId(56);
        let token2 = ja.generate_token(job_id2).unwrap();

        let ja2 = JWTJobAuthorizer::new(vec![1, 2, 3]);

        // Check tokens are deterministic
        assert_eq!(token, ja.generate_token(job_id).unwrap());
        // Check token verification works
        assert!(ja.verify_token(job_id, &token).is_ok());
        assert!(ja.verify_token(job_id, &token2).is_err());
        assert!(ja.verify_token(job_id2, &token).is_err());
        assert!(ja.verify_token(job_id2, &token2).is_ok());
        // Check token verification with a different key fails
        assert!(ja2.verify_token(job_id, &token).is_err());
        assert!(ja2.verify_token(job_id2, &token2).is_err());
    }

    pub struct Scheduler<S> {
        public_addr: SocketAddr,
        handler: S,
        // Is this client permitted to use the scheduler?
        check_client_auth: Box<dyn ClientAuthCheck>,
        // Do we believe the server is who they appear to be?
        check_server_auth: ServerAuthCheck,
    }

    impl<S: SchedulerIncoming + 'static> Scheduler<S> {
        pub fn new(
            public_addr: SocketAddr,
            handler: S,
            check_client_auth: Box<dyn ClientAuthCheck>,
            check_server_auth: ServerAuthCheck,
        ) -> Self {
            Self {
                public_addr,
                handler,
                check_client_auth,
                check_server_auth,
            }
        }

        pub async fn start(self) -> Result<std::convert::Infallible> {
            pub struct SchedulerRequester {
                client: Mutex<reqwest::Client>,
            }

            #[async_trait]
            impl SchedulerOutgoing for SchedulerRequester {
                async fn do_assign_job(
                    &self,
                    server_id: ServerId,
                    tc: Toolchain,
                    auth: String,
                ) -> Result<AssignJobResult> {
                    let url = urls::server_assign_job(server_id);
                    let req = self.client.lock().await.post(url);
                    bincode_req_fut(req.bearer_auth(auth).bincode(&tc)?)
                        .await
                        .context("POST to server assign_job failed")
                }
            }

            struct SchedulerState {
                client_auth: Box<dyn ClientAuthCheck>,
                server_auth: ServerAuthCheck,
                server_certs: Mutex<HashMap<ServerId, (Vec<u8>, Vec<u8>)>>,
                requester: SchedulerRequester,
            }

            // Verify authenticated sccache clients
            struct AuthenticatedClient;

            #[async_trait]
            impl<S> FromRequestParts<S> for AuthenticatedClient
            where
                S: Send + Sync,
            {
                type Rejection = StatusCode;

                async fn from_request_parts(
                    parts: &mut Parts,
                    _state: &S,
                ) -> StdResult<Self, Self::Rejection> {
                    let TypedHeader(Authorization(bearer)) = parts
                        .extract::<TypedHeader<Authorization<Bearer>>>()
                        .await
                        .map_err(|_| StatusCode::UNAUTHORIZED)?;

                    let Extension(this) = parts
                        .extract::<Extension<Arc<SchedulerState>>>()
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    this.client_auth
                        .check(bearer.token())
                        .map(|_| AuthenticatedClient)
                        .map_err(|err| {
                            tracing::warn!(
                                "[AuthenticatedClient()]: invalid client auth: {}",
                                err.0
                            );
                            StatusCode::UNAUTHORIZED
                        })
                }
            }

            // Verify authenticated sccache servers
            struct AuthenticatedServerId(ServerId);

            #[async_trait]
            impl<S> FromRequestParts<S> for AuthenticatedServerId
            where
                S: Send + Sync,
            {
                type Rejection = StatusCode;

                async fn from_request_parts(
                    parts: &mut Parts,
                    _state: &S,
                ) -> StdResult<Self, Self::Rejection> {
                    let Extension(this) = parts
                        .extract::<Extension<Arc<SchedulerState>>>()
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    let ConnectInfo(remote_addr) = parts
                        .extract::<ConnectInfo<SocketAddr>>()
                        .await
                        .map_err(|_| StatusCode::BAD_REQUEST)?;

                    let TypedHeader(Authorization(bearer)) = parts
                        .extract::<TypedHeader<Authorization<Bearer>>>()
                        .await
                        .map_err(|_| StatusCode::UNAUTHORIZED)?;

                    let server_id = (this.server_auth)(bearer.token()).ok_or_else(|| {
                        tracing::warn!(
                            "[AuthenticatedServerId({remote_addr})]: invalid server auth token"
                        );
                        StatusCode::UNAUTHORIZED
                    })?;

                    let origin_ip =
                        if let Some(header) = get_header_value(&parts.headers, "X-Real-IP") {
                            tracing::trace!("X-Real-IP: {:?}", header);
                            match header.parse() {
                                Ok(ip) => ip,
                                Err(err) => {
                                    tracing::warn!(
                                        "X-Real-IP value {:?} could not be parsed: {:?}",
                                        header,
                                        err
                                    );
                                    return Err(StatusCode::UNAUTHORIZED);
                                }
                            }
                        } else {
                            remote_addr.ip()
                        };

                    if server_id.addr().ip() != origin_ip {
                        tracing::trace!("server ip: {:?}", server_id.addr().ip());
                        tracing::trace!("request ip: {:?}", remote_addr.ip());
                        Err(StatusCode::UNAUTHORIZED)
                    } else {
                        Ok(AuthenticatedServerId(server_id))
                    }
                }
            }

            fn maybe_update_certs(
                client: &mut reqwest::Client,
                certs: &mut HashMap<ServerId, (Vec<u8>, Vec<u8>)>,
                server_id: ServerId,
                cert_digest: Vec<u8>,
                cert_pem: Vec<u8>,
            ) -> Result<()> {
                if let Some((saved_cert_digest, _)) = certs.get(&server_id) {
                    if saved_cert_digest == &cert_digest {
                        return Ok(());
                    }
                }
                tracing::info!(
                    "Adding new certificate for {} to scheduler",
                    server_id.addr()
                );
                let mut client_builder = reqwest::ClientBuilder::new();
                // Add all the certificates we know about
                client_builder = client_builder.add_root_certificate(
                    reqwest::Certificate::from_pem(&cert_pem)
                        .context("failed to interpret pem as certificate")?,
                );
                // Remove the old entry first so it isn't added to the client in the following loop
                certs.remove(&server_id);
                for (_, cert_pem) in certs.values() {
                    client_builder = client_builder.add_root_certificate(
                        reqwest::Certificate::from_pem(cert_pem).expect("previously valid cert"),
                    );
                }
                // Finish the client
                let new_client = client_builder
                    .timeout(get_dist_request_timeout())
                    .connect_timeout(get_dist_connect_timeout())
                    // Disable connection pool to avoid broken connection
                    // between runtime
                    .pool_max_idle_per_host(0)
                    .build()
                    .context("failed to create a HTTP client")?;
                // Use the updated certificates
                *client = new_client;
                certs.insert(server_id, (cert_digest, cert_pem));
                Ok(())
            }

            let Self {
                check_client_auth,
                check_server_auth,
                handler,
                ..
            } = self;

            // Doesn't seem like we can easily add the handler to SchedulerState,
            // so create a clone and move it into each route handling closure.
            let handler = Arc::new(handler);

            let app = Router::new()
                .route(
                    "/api/v1/scheduler/alloc_job",
                    routing::post({
                        let handler = Arc::clone(&handler);
                        move |method: Method,
                              uri: Uri,
                              headers: HeaderMap,
                              Extension(state): Extension<Arc<SchedulerState>>,
                              Bincode(toolchain): Bincode<Toolchain>| async move {
                            let res =
                                match handler.handle_alloc_job(&state.requester, toolchain).await {
                                    Ok(res) => res,
                                    Err(err) => return anyhow_response(method, uri, err),
                                };

                            accepts_response(
                                &headers,
                                &AllocJobHttpResponse::from_alloc_job_result(
                                    res,
                                    state.server_certs.lock().await.borrow(),
                                ),
                            )
                        }
                    }),
                )
                .route(
                    "/api/v1/scheduler/server_certificate/:server_id",
                    routing::get({
                        move |method: Method,
                              uri: Uri,
                              headers: HeaderMap,
                              Extension(state): Extension<Arc<SchedulerState>>,
                              Path(server_id): Path<ServerId>| async move {
                            match state
                                .server_certs
                                .lock()
                                .await
                                .get(&server_id)
                                .map(|v| v.to_owned())
                                .context("server cert not available")
                                .map(|(cert_digest, cert_pem)| ServerCertificateHttpResponse {
                                    cert_digest,
                                    cert_pem,
                                }) {
                                Ok(res) => accepts_response(&headers, &res),
                                Err(err) => anyhow_response(method, uri, err),
                            }
                        }
                    }),
                )
                .route(
                    "/api/v1/scheduler/heartbeat_server",
                    routing::post({
                        let handler = Arc::clone(&handler);
                        move |
                            method: Method,
                            uri: Uri,
                            headers: HeaderMap,
                            Extension(state): Extension<Arc<SchedulerState>>,
                            AuthenticatedServerId(server_id): AuthenticatedServerId,
                            Bincode(heartbeat) : Bincode<HeartbeatServerHttpRequest>
                        | async move {
                            if let Err(err) = maybe_update_certs(
                                state.requester.client.lock().await.borrow_mut(),
                                state.server_certs.lock().await.borrow_mut(),
                                server_id,
                                heartbeat.cert_digest,
                                heartbeat.cert_pem
                            ) {
                                return anyhow_response(method, uri, err);
                            }
                            match handler.handle_heartbeat_server(
                                server_id,
                                heartbeat.server_nonce,
                                heartbeat.num_cpus,
                                heartbeat.max_per_core_load,
                                JWTJobAuthorizer::new(heartbeat.jwt_key),
                                heartbeat.num_assigned_jobs,
                                heartbeat.num_active_jobs,
                            ).await {
                                Ok(res) => accepts_response(&headers, &res),
                                Err(err) => anyhow_response(method, uri, err)
                            }
                        }
                    }),
                )
                .route(
                    "/api/v1/scheduler/job_state",
                    routing::post({
                        let handler = Arc::clone(&handler);
                        move |
                            method: Method,
                            uri: Uri,
                            headers: HeaderMap,
                            AuthenticatedServerId(server_id): AuthenticatedServerId,
                            Bincode(update) : Bincode<UpdateJobStateHttpRequest>
                        | async move {
                            match handler.handle_update_job_state(
                                server_id,
                                update.num_assigned_jobs,
                                update.num_active_jobs,
                            ).await {
                                Ok(res) => accepts_response(&headers, &res),
                                Err(err) => anyhow_response(method, uri, err)
                            }
                        }
                    }),
                )
                .route(
                    "/api/v1/scheduler/status",
                    routing::get({
                        let handler = Arc::clone(&handler);
                        move |method: Method,
                              uri: Uri,
                              headers: HeaderMap,
                              _: AuthenticatedClient| async move {
                            match handler.handle_status().await {
                                Ok(res) => accepts_response(&headers, &res),
                                Err(err) => anyhow_response(method, uri, err),
                            }
                        }
                    }),
                )
                .fallback(|| async move { (StatusCode::NOT_FOUND, "404") })
                .layer(Extension(Arc::new(SchedulerState {
                    client_auth: check_client_auth,
                    server_auth: check_server_auth,
                    server_certs: Default::default(),
                    requester: SchedulerRequester {
                        client: Mutex::new(new_reqwest_client(None)),
                    },
                })));

            let app = with_request_tracing(app);

            let mut make_service = app.into_make_service_with_connect_info::<SocketAddr>();

            let listener = TcpListener::bind(self.public_addr).await.unwrap();

            tracing::info!("Scheduler listening for clients on {}", self.public_addr);

            loop {
                let (tcp_stream, remote_addr) = listener.accept().await.unwrap();
                let tower_service = unwrap_infallible(make_service.call(remote_addr).await);

                tokio::spawn(async move {
                    // Hyper has its own `AsyncRead` and `AsyncWrite` traits and doesn't use tokio.
                    // `TokioIo` converts between them.
                    let tok_stream = TokioIo::new(tcp_stream);

                    let hyper_service = hyper::service::service_fn(
                        move |request: Request<hyper::body::Incoming>| {
                            // Clone `tower_service` because hyper's `Service` uses `&self` whereas
                            // tower's `Service` requires `&mut self`.
                            tower_service.clone().oneshot(request)
                        },
                    );

                    if let Err(err) =
                        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .serve_connection(tok_stream, hyper_service)
                            .await
                    {
                        tracing::debug!("sccache: failed to serve connection: {err:#}");
                    }
                });
            }
        }
    }

    pub struct Server<S> {
        public_addr: SocketAddr,
        bind_addr: SocketAddr,
        scheduler_url: reqwest::Url,
        scheduler_auth: String,
        // HTTPS pieces all the builders will use for connection encryption
        cert_digest: Vec<u8>,
        cert_pem: Vec<u8>,
        privkey_pem: Vec<u8>,
        // Key used to sign any requests relating to jobs
        jwt_key: Vec<u8>,
        // Randomly generated nonce to allow the scheduler to detect server restarts
        server_nonce: ServerNonce,
        max_per_core_load: f64,
        num_cpus: usize,
        handler: S,
    }

    impl<S: ServerIncoming + 'static> Server<S> {
        #[allow(clippy::too_many_arguments)]
        pub fn new(
            public_addr: SocketAddr,
            bind_addr: SocketAddr,
            scheduler_url: reqwest::Url,
            scheduler_auth: String,
            max_per_core_load: f64,
            num_cpus: usize,
            handler: S,
        ) -> Result<Self> {
            let (cert_digest, cert_pem, privkey_pem) =
                create_https_cert_and_privkey(public_addr)
                    .context("failed to create HTTPS certificate for server")?;
            let mut jwt_key = vec![0; JWT_KEY_LENGTH];
            OsRng.fill_bytes(&mut jwt_key);
            let server_nonce = ServerNonce::new();

            Ok(Self {
                public_addr,
                bind_addr,
                scheduler_url,
                scheduler_auth,
                cert_digest,
                cert_pem,
                privkey_pem,
                jwt_key,
                server_nonce,
                max_per_core_load: max_per_core_load.max(1f64),
                num_cpus: num_cpus.max(1),
                handler,
            })
        }

        pub async fn start(self) -> Result<std::convert::Infallible> {
            #[derive(Clone)]
            pub struct ServerRequester {
                client: reqwest::Client,
                heartbeat_url: reqwest::Url,
                heartbeat_req: HeartbeatServerHttpRequest,
                scheduler_url: reqwest::Url,
                scheduler_auth: String,
            }

            #[async_trait]
            impl ServerOutgoing for ServerRequester {
                async fn do_heartbeat(
                    &self,
                    num_assigned_jobs: usize,
                    num_active_jobs: usize,
                ) -> Result<HeartbeatServerResult> {
                    let mut heartbeat_req = self.heartbeat_req.clone();
                    heartbeat_req.num_assigned_jobs = num_assigned_jobs;
                    heartbeat_req.num_active_jobs = num_active_jobs;
                    bincode_req_fut(
                        self.client
                            .post(self.heartbeat_url.clone())
                            .bearer_auth(self.scheduler_auth.clone())
                            .bincode(&heartbeat_req)
                            .expect("failed to serialize heartbeat"),
                    )
                    .await
                }
                async fn do_update_job_state(
                    &self,
                    num_assigned_jobs: usize,
                    num_active_jobs: usize,
                ) -> Result<UpdateJobStateResult> {
                    let url = urls::scheduler_job_state(&self.scheduler_url);
                    let req = UpdateJobStateHttpRequest {
                        num_assigned_jobs,
                        num_active_jobs,
                    };
                    bincode_req_fut(
                        self.client
                            .post(url)
                            .bearer_auth(self.scheduler_auth.clone())
                            .bincode(&req)?,
                    )
                    .await
                    .context("POST to scheduler job_state failed")
                }
            }

            struct ServerState {
                auth: Box<dyn JobAuthorizer>,
                requester: Arc<ServerRequester>,
                server_nonce: ServerNonce,
            }

            // Verification of job auth in a request
            struct AuthenticatedJob(JobId);

            #[async_trait]
            impl<S> FromRequestParts<S> for AuthenticatedJob
            where
                S: Send + Sync,
            {
                type Rejection = StatusCode;

                async fn from_request_parts(
                    parts: &mut Parts,
                    _state: &S,
                ) -> StdResult<Self, Self::Rejection> {
                    let Extension(this) = parts
                        .extract::<Extension<Arc<ServerState>>>()
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    let Path(job_id) = parts
                        .extract::<Path<JobId>>()
                        .await
                        .map_err(|_| StatusCode::BAD_REQUEST)?;

                    let TypedHeader(Authorization(bearer)) = parts
                        .extract::<TypedHeader<Authorization<Bearer>>>()
                        .await
                        .map_err(|_| StatusCode::UNAUTHORIZED)?;

                    this.auth
                        .verify_token(job_id, bearer.token())
                        .map(|_| AuthenticatedJob(job_id))
                        .map_err(|_| StatusCode::UNAUTHORIZED)
                }
            }

            // Verify assign_job is from a scheduler with which this server has registered
            struct AuthenticatedScheduler;

            #[async_trait]
            impl<S> FromRequestParts<S> for AuthenticatedScheduler
            where
                S: Send + Sync,
            {
                type Rejection = StatusCode;

                async fn from_request_parts(
                    parts: &mut Parts,
                    _state: &S,
                ) -> StdResult<Self, Self::Rejection> {
                    let Extension(this) = parts
                        .extract::<Extension<Arc<ServerState>>>()
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    let TypedHeader(Authorization(bearer)) = parts
                        .extract::<TypedHeader<Authorization<Bearer>>>()
                        .await
                        .map_err(|_| StatusCode::UNAUTHORIZED)?;

                    this.auth
                        .verify_token(JobId(this.server_nonce.as_u64()), bearer.token())
                        .map(|_| AuthenticatedScheduler)
                        .map_err(|_| StatusCode::UNAUTHORIZED)
                }
            }

            let Self {
                public_addr,
                bind_addr,
                scheduler_url,
                scheduler_auth,
                cert_digest,
                cert_pem,
                privkey_pem,
                jwt_key,
                server_nonce,
                max_per_core_load,
                num_cpus,
                handler,
            } = self;

            let requester = Arc::new(ServerRequester {
                client: new_reqwest_client(Some(public_addr)),
                scheduler_url: scheduler_url.clone(),
                scheduler_auth: scheduler_auth.clone(),
                heartbeat_url: urls::scheduler_heartbeat_server(&scheduler_url),
                heartbeat_req: HeartbeatServerHttpRequest {
                    num_cpus,
                    max_per_core_load,
                    jwt_key: jwt_key.clone(),
                    server_nonce: server_nonce.clone(),
                    cert_digest: cert_digest.clone(),
                    cert_pem: cert_pem.clone(),
                    num_assigned_jobs: 0,
                    num_active_jobs: 0,
                },
            });

            // Doesn't seem like we can easily add the handler to ServerState,
            // so create a clone and move it into each route handling closure.
            let handler = Arc::new(handler);

            handler.start_heartbeat(requester.clone());

            let app = Router::new()
                .route(
                    "/api/v1/distserver/assign_job",
                    routing::post({
                        let handler = Arc::clone(&handler);
                        move |method: Method,
                              uri: Uri,
                              headers: HeaderMap,
                              _: AuthenticatedScheduler,
                              Bincode(toolchain): Bincode<Toolchain>| async move {
                            match handler.handle_assign_job(toolchain).await {
                                Ok(res) => accepts_response(&headers, &res),
                                Err(err) => anyhow_response(method, uri, err),
                            }
                        }
                    }),
                )
                .route(
                    "/api/v1/distserver/submit_toolchain/:job_id",
                    routing::post({
                        let handler = Arc::clone(&handler);
                        move |method: Method,
                              uri: Uri,
                              headers: HeaderMap,
                              AuthenticatedJob(job_id): AuthenticatedJob,
                              request: Request| async move {
                            // Convert the request body stream into an `AsyncRead`
                            let toolchain_reader = StreamReader::new(
                                request
                                    .into_body()
                                    .into_data_stream()
                                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err)),
                            );

                            futures::pin_mut!(toolchain_reader);

                            match handler
                                .handle_submit_toolchain(job_id, toolchain_reader)
                                .await
                            {
                                Ok(res) => accepts_response(&headers, &res),
                                Err(err) => anyhow_response(method, uri, err),
                            }
                        }
                    }),
                )
                .route(
                    "/api/v1/distserver/run_job/:job_id",
                    routing::post({
                        let handler = Arc::clone(&handler);
                        move |method: Method,
                              uri: Uri,
                              headers: HeaderMap,
                              AuthenticatedJob(job_id): AuthenticatedJob,
                              Extension(state): Extension<Arc<ServerState>>,
                              request: Request| async move {
                            // Convert the request body stream into an `AsyncRead`
                            let body_reader = StreamReader::new(
                                request
                                    .into_body()
                                    .into_data_stream()
                                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err)),
                            );

                            futures::pin_mut!(body_reader);

                            // Read the RunJob message, then take the rest of the body as the job inputs
                            let (run_job, inputs_reader) = {
                                let run_job_length =
                                    body_reader.read_u32().await.map_err(|err| {
                                        (
                                            StatusCode::BAD_REQUEST,
                                            format!("Invalid bincode length: {err}"),
                                        )
                                            .into_response()
                                    })? as usize;

                                let mut run_job_reader = body_reader.take(run_job_length as u64);
                                let mut run_job_buf = vec![];
                                run_job_reader.read_to_end(&mut run_job_buf).await.map_err(
                                    |err| {
                                        (StatusCode::BAD_REQUEST, err.to_string()).into_response()
                                    },
                                )?;

                                let run_job = bincode::deserialize_from::<_, RunJobHttpRequest>(
                                    run_job_buf.reader(),
                                )
                                .map_err(|err| {
                                    (StatusCode::BAD_REQUEST, err.to_string()).into_response()
                                })?;

                                (run_job, ZlibReadDecoder::new(run_job_reader.into_inner()))
                            };

                            futures::pin_mut!(inputs_reader);

                            tracing::trace!("[run_job({})]: {:?}", job_id, run_job);
                            let RunJobHttpRequest { command, outputs } = run_job;

                            match handler
                                .handle_run_job(
                                    state.requester.as_ref(),
                                    job_id,
                                    command,
                                    outputs,
                                    inputs_reader,
                                )
                                .await
                            {
                                Ok(res) => Ok(accepts_response(&headers, &res).into_response()),
                                Err(err) => Err(anyhow_response(method, uri, err).into_response()),
                            }
                        }
                    }),
                )
                .fallback(|| async move { (StatusCode::NOT_FOUND, "404") })
                // 1GiB should be enough for toolchains and compile inputs, right?
                .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
                // .layer(
                //     TraceLayer::new_for_http()
                //         // Create our own span for the request and include the matched path. The matched
                //         // path is useful for figuring out which handler the request was routed to.
                //         .make_span_with(|req: &Request| {
                //             let method = req.method();
                //             let uri = req.uri();
                //             // axum automatically adds this extension.
                //             let matched_path = req
                //                 .extensions()
                //                 .get::<MatchedPath>()
                //                 .map(|matched_path| matched_path.as_str());
                //             tracing::debug_span!("request", %method, %uri, matched_path)
                //         }),
                // )
                .layer(Extension(Arc::new(ServerState {
                    auth: JWTJobAuthorizer::new(jwt_key.clone()),
                    server_nonce: server_nonce.clone(),
                    requester,
                })));

            let app = with_request_tracing(app);

            let tls_acceptor = {
                let cert = openssl::x509::X509::from_pem(&cert_pem).unwrap();
                let key = openssl::pkey::PKey::private_key_from_pem(&privkey_pem).unwrap();
                let mut tls_builder =
                    SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
                tls_builder.set_certificate(&cert).unwrap();
                tls_builder.set_private_key(&key).unwrap();
                tls_builder.check_private_key().unwrap();
                tls_builder.build()
            };

            let listener = TcpListener::bind(bind_addr).await.unwrap();

            tracing::info!(
                "Server listening for clients on {}, public_addr is: {}",
                bind_addr,
                public_addr
            );

            loop {
                let tls_acceptor = tls_acceptor.clone();
                let (tcp_stream, remote_addr) = listener.accept().await.unwrap();
                let tower_service = app.clone();

                tokio::spawn(async move {
                    // Wait for tls handshake to happen
                    let ssl = Ssl::new(tls_acceptor.context()).unwrap();
                    let mut tls_stream = SslStream::new(ssl, tcp_stream).unwrap();
                    if let Err(err) = SslStream::accept(std::pin::Pin::new(&mut tls_stream)).await {
                        tracing::debug!(
                            "error during tls handshake connection from {}: {}",
                            remote_addr,
                            err
                        );
                        return;
                    }

                    // Hyper has its own `AsyncRead` and `AsyncWrite` traits and doesn't use tokio.
                    // `TokioIo` converts between them.
                    let tok_stream = TokioIo::new(tls_stream);

                    let hyper_service = hyper::service::service_fn(
                        move |request: Request<hyper::body::Incoming>| {
                            // Clone `tower_service` because hyper's `Service` uses `&self` whereas
                            // tower's `Service` requires `&mut self`.
                            tower_service.clone().oneshot(request)
                        },
                    );

                    if let Err(err) =
                        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .serve_connection(tok_stream, hyper_service)
                            .await
                    {
                        tracing::debug!("sccache: failed to serve connection: {err:#}");
                    }
                });
            }
        }
    }
}

#[cfg(feature = "dist-client")]
mod client {
    use super::super::cache;
    use crate::config;
    use crate::dist::pkg::{InputsPackager, ToolchainPackager};
    use crate::dist::{
        self, AllocJobResult, CompileCommand, JobAlloc, PathTransformer, RunJobResult,
        SchedulerStatusResult, SubmitToolchainResult, Toolchain,
    };

    use async_trait::async_trait;
    use byteorder::{BigEndian, WriteBytesExt};
    use flate2::write::ZlibEncoder as ZlibWriteEncoder;
    use flate2::Compression;
    use futures::TryFutureExt;
    use reqwest::Body;
    use std::collections::HashMap;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use super::common::{
        bincode_req_fut, AllocJobHttpResponse, ReqwestRequestBuilderExt, RunJobHttpRequest,
        ServerCertificateHttpResponse,
    };
    use super::{get_dist_connect_timeout, get_dist_request_timeout, urls};
    use crate::errors::*;

    pub struct Client {
        auth_token: String,
        scheduler_url: reqwest::Url,
        // cert_digest -> cert_pem
        server_certs: Arc<Mutex<HashMap<dist::ServerId, (Vec<u8>, Vec<u8>)>>>,
        client: Arc<Mutex<reqwest::Client>>,
        pool: tokio::runtime::Handle,
        tc_cache: Arc<cache::ClientToolchains>,
        rewrite_includes_only: bool,
    }

    impl Client {
        pub fn new(
            pool: &tokio::runtime::Handle,
            scheduler_url: reqwest::Url,
            cache_dir: &Path,
            cache_size: u64,
            toolchain_configs: &[config::DistToolchainConfig],
            auth_token: String,
            rewrite_includes_only: bool,
        ) -> Result<Self> {
            let client = reqwest::ClientBuilder::new()
                .timeout(get_dist_request_timeout())
                .connect_timeout(get_dist_connect_timeout())
                // Disable connection pool to avoid broken connection
                // between runtime
                .pool_max_idle_per_host(0)
                .build()
                .context("failed to create an async HTTP client")?;
            let client_toolchains =
                cache::ClientToolchains::new(cache_dir, cache_size, toolchain_configs)
                    .context("failed to initialise client toolchains")?;
            Ok(Self {
                auth_token,
                scheduler_url,
                server_certs: Default::default(),
                client: Arc::new(Mutex::new(client)),
                pool: pool.clone(),
                tc_cache: Arc::new(client_toolchains),
                rewrite_includes_only,
            })
        }

        fn update_certs(
            client: &mut reqwest::Client,
            certs: &mut HashMap<dist::ServerId, (Vec<u8>, Vec<u8>)>,
            server_id: dist::ServerId,
            cert_digest: Vec<u8>,
            cert_pem: Vec<u8>,
        ) -> Result<()> {
            let mut client_async_builder = reqwest::ClientBuilder::new();
            // Add all the certificates we know about
            client_async_builder = client_async_builder.add_root_certificate(
                reqwest::Certificate::from_pem(&cert_pem)
                    .context("failed to interpret pem as certificate")?,
            );
            // Remove the old entry first so it isn't added to the client in the following loop
            certs.remove(&server_id);
            for (_, cert_pem) in certs.values() {
                client_async_builder = client_async_builder.add_root_certificate(
                    reqwest::Certificate::from_pem(cert_pem).expect("previously valid cert"),
                );
            }
            // Finish the client
            let new_client_async = client_async_builder
                .timeout(get_dist_request_timeout())
                .connect_timeout(get_dist_connect_timeout())
                // Disable keep-alive
                .pool_max_idle_per_host(0)
                .build()
                .context("failed to create an async HTTP client")?;
            // Use the updated certificates
            *client = new_client_async;
            certs.insert(server_id, (cert_digest, cert_pem));
            Ok(())
        }
    }

    #[async_trait]
    impl dist::Client for Client {
        async fn do_alloc_job(&self, tc: Toolchain) -> Result<AllocJobResult> {
            let scheduler_url = self.scheduler_url.clone();
            let url = urls::scheduler_alloc_job(&scheduler_url);
            let mut req = self.client.lock().unwrap().post(url);
            req = req.bearer_auth(self.auth_token.clone()).bincode(&tc)?;

            let client = self.client.clone();
            let server_certs = self.server_certs.clone();

            match bincode_req_fut(req).await? {
                AllocJobHttpResponse::Success {
                    job_alloc,
                    need_toolchain,
                    cert_digest,
                } => {
                    let server_id = job_alloc.server_id;
                    let alloc_job_res = Ok(AllocJobResult::Success {
                        job_alloc,
                        need_toolchain,
                    });
                    if let Some((digest, _)) = server_certs.lock().unwrap().get(&server_id) {
                        if cert_digest == *digest {
                            return alloc_job_res;
                        }
                    }
                    info!(
                        "Need to request new certificate for server {}",
                        server_id.addr()
                    );
                    let url = urls::scheduler_server_certificate(&scheduler_url, server_id);
                    let req = client.lock().unwrap().get(url);
                    let res: ServerCertificateHttpResponse = bincode_req_fut(req)
                        .await
                        .context("GET to scheduler server_certificate failed")?;

                    // TODO: Move to asynchronous reqwest client only.
                    // This function internally builds a blocking reqwest client;
                    // However, it does so by utilizing a runtime which it drops,
                    // triggering (rightfully) a sanity check that prevents from
                    // dropping a runtime in asynchronous context.
                    // For the time being, we work around this by off-loading it
                    // to a dedicated blocking-friendly thread pool.
                    let _ = self
                        .pool
                        .spawn_blocking(move || {
                            Self::update_certs(
                                &mut client.lock().unwrap(),
                                &mut server_certs.lock().unwrap(),
                                server_id,
                                res.cert_digest,
                                res.cert_pem,
                            )
                            .context("Failed to update certificate")
                            .unwrap_or_else(|e| warn!("Failed to update certificate: {:?}", e));
                        })
                        .await;

                    alloc_job_res
                }
                AllocJobHttpResponse::Fail { msg } => Ok(AllocJobResult::Fail { msg }),
            }
        }

        async fn do_get_status(&self) -> Result<SchedulerStatusResult> {
            let scheduler_url = self.scheduler_url.clone();
            let url = urls::scheduler_status(&scheduler_url);
            let mut req = self.client.lock().unwrap().get(url);
            req = req.bearer_auth(self.auth_token.clone());
            bincode_req_fut(req).await
        }

        async fn do_submit_toolchain(
            &self,
            job_alloc: JobAlloc,
            tc: Toolchain,
        ) -> Result<SubmitToolchainResult> {
            match self.tc_cache.get_toolchain(&tc) {
                Ok(Some(toolchain_file)) => {
                    let url = urls::server_submit_toolchain(job_alloc.server_id, job_alloc.job_id);
                    let req = self.client.lock().unwrap().post(url);
                    let toolchain_file = tokio::fs::File::from_std(toolchain_file.into());
                    let toolchain_file_stream = tokio_util::io::ReaderStream::new(toolchain_file);
                    let body = Body::wrap_stream(toolchain_file_stream);
                    let req = req.bearer_auth(job_alloc.auth).body(body);
                    bincode_req_fut(req).await
                }
                Ok(None) => Err(anyhow!("couldn't find toolchain locally")),
                Err(e) => Err(e),
            }
        }

        async fn do_run_job(
            &self,
            job_alloc: JobAlloc,
            command: CompileCommand,
            outputs: Vec<String>,
            inputs_packager: Box<dyn InputsPackager>,
        ) -> Result<(RunJobResult, PathTransformer)> {
            let url = urls::server_run_job(job_alloc.server_id, job_alloc.job_id);

            let (body, path_transformer) = self
                .pool
                .spawn_blocking(move || -> Result<_> {
                    let bincode = bincode::serialize(&RunJobHttpRequest { command, outputs })
                        .context("failed to serialize run job request")?;
                    let bincode_length = bincode.len();

                    let mut body = vec![];
                    body.write_u32::<BigEndian>(bincode_length as u32)
                        .expect("Infallible write of bincode length to vec failed");
                    body.write_all(&bincode)
                        .expect("Infallible write of bincode body to vec failed");
                    let path_transformer;
                    {
                        let mut compressor = ZlibWriteEncoder::new(&mut body, Compression::fast());
                        path_transformer = inputs_packager
                            .write_inputs(&mut compressor)
                            .context("Could not write inputs for compilation")?;
                        compressor.flush().context("failed to flush compressor")?;
                        trace!(
                            "Compressed inputs from {} -> {}",
                            compressor.total_in(),
                            compressor.total_out()
                        );
                        compressor.finish().context("failed to finish compressor")?;
                    }

                    Ok((body, path_transformer))
                })
                .await??;
            let mut req = self.client.lock().unwrap().post(url);
            req = req.bearer_auth(job_alloc.auth.clone()).bytes(body);
            bincode_req_fut(req)
                .map_ok(|res| (res, path_transformer))
                .await
        }

        async fn put_toolchain(
            &self,
            compiler_path: PathBuf,
            weak_key: String,
            toolchain_packager: Box<dyn ToolchainPackager>,
        ) -> Result<(Toolchain, Option<(String, PathBuf)>)> {
            let compiler_path = compiler_path.to_owned();
            let weak_key = weak_key.to_owned();
            let tc_cache = self.tc_cache.clone();

            self.pool
                .spawn_blocking(move || {
                    tc_cache.put_toolchain(&compiler_path, &weak_key, toolchain_packager)
                })
                .await?
        }

        fn rewrite_includes_only(&self) -> bool {
            self.rewrite_includes_only
        }
        fn get_custom_toolchain(&self, exe: &Path) -> Option<PathBuf> {
            match self.tc_cache.get_custom_toolchain(exe) {
                Some(Ok((_, _, path))) => Some(path),
                _ => None,
            }
        }
    }
}
