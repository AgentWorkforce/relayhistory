//! Google Cloud Storage.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use object_store::gcp::{GcpCredential, GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::signer::{Method, Signer};
use url::Url;

use crate::core::{Backend, Core};
use crate::credentials::VendedProvider;
use crate::delegate::delegate_object_store;
use crate::{CredentialScope, CredentialVendor, Error, Result, VendedCredential};

/// How a [`GcsStore`] authenticates.
#[derive(Clone)]
pub enum GcsCredentials {
    /// Application Default Credentials: `GOOGLE_APPLICATION_CREDENTIALS`,
    /// workload identity, then the GCE metadata server. Presigning uses the
    /// IAM `signBlob` API.
    Ambient,
    /// A scoped OAuth2 access token from a vendor (e.g. a Credential Access
    /// Boundary token). Presigning is unavailable with a bearer token alone.
    Vended(Arc<dyn CredentialVendor>),
    /// A service-account key JSON supplied at runtime. Held in memory only.
    ServiceAccountKey(String),
}

impl fmt::Debug for GcsCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ambient => "Ambient",
            Self::Vended(_) => "Vended(..)",
            Self::ServiceAccountKey(_) => "ServiceAccountKey(<redacted>)",
        })
    }
}

#[derive(Debug, Clone)]
pub struct GcsConfig {
    pub bucket: String,
    /// Override the API base URL (e.g. `http://localhost:4443` for
    /// fake-gcs-server).
    pub base_url: Option<String>,
    /// Key prefix the credentials are scoped to; passed to the vendor.
    pub scope_prefix: Option<String>,
    pub credentials: GcsCredentials,
}

impl GcsConfig {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            base_url: None,
            scope_prefix: None,
            credentials: GcsCredentials::Ambient,
        }
    }
}

#[derive(Debug)]
pub(crate) struct GcsBackend {
    store: GoogleCloudStorage,
    can_sign: bool,
}

#[async_trait]
impl Backend for GcsBackend {
    const NAME: &'static str = "gcs";

    fn store(&self) -> &dyn object_store::ObjectStore {
        &self.store
    }

    async fn presign(&self, path: &Path, ttl: Duration) -> Result<Url> {
        if !self.can_sign {
            return Err(Error::Unsupported {
                backend: Self::NAME,
                operation: "presign_get with a vended bearer token (configure a service account)"
                    .into(),
            });
        }
        self.store
            .signed_url(Method::GET, path, ttl)
            .await
            .map_err(|e| Error::from_store(Self::NAME, path.as_ref(), e))
    }
}

/// [`crate::ObjectStore`] over Google Cloud Storage.
#[derive(Debug)]
pub struct GcsStore {
    core: Core<GcsBackend>,
}

delegate_object_store!(GcsStore);

impl GcsStore {
    pub fn new(config: GcsConfig) -> Result<Self> {
        let mut builder = match &config.credentials {
            GcsCredentials::Ambient => GoogleCloudStorageBuilder::from_env(),
            _ => GoogleCloudStorageBuilder::new(),
        }
        .with_bucket_name(&config.bucket);
        if let Some(base_url) = &config.base_url {
            builder = builder.with_base_url(base_url);
        }
        let can_sign = !matches!(config.credentials, GcsCredentials::Vended(_));
        match config.credentials {
            GcsCredentials::Ambient => {}
            GcsCredentials::ServiceAccountKey(json) => {
                builder = builder.with_service_account_key(json);
            }
            GcsCredentials::Vended(vendor) => {
                let scope = CredentialScope {
                    backend: GcsBackend::NAME,
                    bucket: config.bucket.clone(),
                    prefix: config.scope_prefix.clone(),
                };
                builder = builder.with_credentials(Arc::new(VendedProvider::new(
                    vendor,
                    scope,
                    gcp_credential,
                )));
            }
        }
        let store = builder
            .build()
            .map_err(|e| Error::from_store(GcsBackend::NAME, "", e))?;
        Ok(Self {
            core: Core::new(GcsBackend { store, can_sign }),
        })
    }

    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.core.set_page_size(page_size);
        self
    }
}

fn gcp_credential(vended: VendedCredential) -> Option<GcpCredential> {
    match vended {
        VendedCredential::GcpBearer { token, .. } => Some(GcpCredential { bearer: token }),
        _ => None,
    }
}
