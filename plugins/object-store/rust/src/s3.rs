//! Amazon S3 and S3-compatible stores (Cloudflare R2, MinIO, ...).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use object_store::aws::{AmazonS3, AmazonS3Builder, AwsCredential, S3ConditionalPut};
use object_store::path::Path;
use object_store::signer::{Method, Signer};
use url::Url;

use crate::core::{Backend, Core};
use crate::credentials::VendedProvider;
use crate::delegate::delegate_object_store;
use crate::{CredentialScope, CredentialVendor, Error, Result, VendedCredential};

/// How an [`S3Store`] authenticates.
#[derive(Clone)]
pub enum S3Credentials {
    /// The standard AWS chain: environment, web identity (IRSA), ECS task
    /// role, then EC2 instance metadata.
    Ambient,
    /// Scoped STS credentials from a vendor, refreshed before expiry.
    Vended(Arc<dyn CredentialVendor>),
    /// Explicit keys supplied at runtime (customer config, local emulators).
    /// Held in memory only.
    Static {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
}

impl fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ambient => "Ambient",
            Self::Vended(_) => "Vended(..)",
            Self::Static { .. } => "Static(<redacted>)",
        })
    }
}

/// Connection settings for [`S3Store`]. Contains no secrets other than an
/// optional in-memory [`S3Credentials::Static`].
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    /// Override for S3-compatible stores, e.g.
    /// `https://<account>.r2.cloudflarestorage.com` or `http://localhost:9000`.
    pub endpoint: Option<String>,
    /// Permit `http://` endpoints (local emulators only).
    pub allow_http: bool,
    /// Virtual-hosted-style (`bucket.host`) instead of path-style requests.
    pub virtual_hosted_style: bool,
    /// Key prefix the credentials are scoped to; passed to the vendor.
    pub scope_prefix: Option<String>,
    pub credentials: S3Credentials,
}

impl S3Config {
    pub fn new(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            allow_http: false,
            virtual_hosted_style: false,
            scope_prefix: None,
            credentials: S3Credentials::Ambient,
        }
    }
}

#[derive(Debug)]
pub(crate) struct S3Backend(AmazonS3);

#[async_trait]
impl Backend for S3Backend {
    const NAME: &'static str = "s3";

    fn store(&self) -> &dyn object_store::ObjectStore {
        &self.0
    }

    async fn presign(&self, path: &Path, ttl: Duration) -> Result<Url> {
        self.0
            .signed_url(Method::GET, path, ttl)
            .await
            .map_err(|e| Error::from_store(Self::NAME, path.as_ref(), e))
    }
}

/// [`crate::ObjectStore`] over Amazon S3 or an S3-compatible endpoint.
#[derive(Debug)]
pub struct S3Store {
    core: Core<S3Backend>,
}

delegate_object_store!(S3Store);

impl S3Store {
    pub fn new(config: S3Config) -> Result<Self> {
        let mut builder = match &config.credentials {
            S3Credentials::Ambient => AmazonS3Builder::from_env(),
            _ => AmazonS3Builder::new(),
        }
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_allow_http(config.allow_http)
        .with_virtual_hosted_style_request(config.virtual_hosted_style)
        // `If-Match` on PUT, supported by S3 (2024-11), R2 and MinIO.
        .with_conditional_put(S3ConditionalPut::ETagMatch);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        match config.credentials {
            S3Credentials::Ambient => {}
            S3Credentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                builder = builder
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key);
                if let Some(token) = session_token {
                    builder = builder.with_token(token);
                }
            }
            S3Credentials::Vended(vendor) => {
                let scope = CredentialScope {
                    backend: S3Backend::NAME,
                    bucket: config.bucket.clone(),
                    prefix: config.scope_prefix.clone(),
                };
                builder = builder.with_credentials(Arc::new(VendedProvider::new(
                    vendor,
                    scope,
                    aws_credential,
                )));
            }
        }
        let store = builder
            .build()
            .map_err(|e| Error::from_store(S3Backend::NAME, "", e))?;
        Ok(Self {
            core: Core::new(S3Backend(store)),
        })
    }

    /// Smaller list pages, for exercising cursors in tests.
    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.core.set_page_size(page_size);
        self
    }
}

fn aws_credential(vended: VendedCredential) -> Option<AwsCredential> {
    match vended {
        VendedCredential::Aws {
            access_key_id,
            secret_access_key,
            session_token,
            ..
        } => Some(AwsCredential {
            key_id: access_key_id,
            secret_key: secret_access_key,
            token: session_token,
        }),
        _ => None,
    }
}
