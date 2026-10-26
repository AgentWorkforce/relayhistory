//! Azure Blob Storage.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use object_store::azure::{AzureCredential, MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::path::Path;
use object_store::signer::{Method, Signer};
use url::Url;

use crate::core::{Backend, Core};
use crate::credentials::VendedProvider;
use crate::delegate::delegate_object_store;
use crate::{CredentialScope, CredentialVendor, Error, Result, VendedCredential};

/// How an [`AzureBlobStore`] authenticates.
#[derive(Clone)]
pub enum AzureCredentials {
    /// Environment (`AZURE_*`), workload identity, then managed identity.
    Ambient,
    /// A user-delegation SAS or Entra ID token from a vendor. Presigning needs
    /// a bearer token (to fetch a user delegation key) or an account key.
    Vended(Arc<dyn CredentialVendor>),
    /// A storage account key supplied at runtime (customer config, Azurite).
    /// Held in memory only.
    AccessKey(String),
}

impl fmt::Debug for AzureCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ambient => "Ambient",
            Self::Vended(_) => "Vended(..)",
            Self::AccessKey(_) => "AccessKey(<redacted>)",
        })
    }
}

#[derive(Debug, Clone)]
pub struct AzureBlobConfig {
    pub account: String,
    pub container: String,
    /// Override the blob endpoint (sovereign clouds, private endpoints).
    pub endpoint: Option<String>,
    /// Target the local Azurite emulator (`devstoreaccount1`, plain HTTP).
    pub use_emulator: bool,
    /// Key prefix the credentials are scoped to; passed to the vendor.
    pub scope_prefix: Option<String>,
    pub credentials: AzureCredentials,
}

impl AzureBlobConfig {
    pub fn new(account: impl Into<String>, container: impl Into<String>) -> Self {
        Self {
            account: account.into(),
            container: container.into(),
            endpoint: None,
            use_emulator: false,
            scope_prefix: None,
            credentials: AzureCredentials::Ambient,
        }
    }
}

#[derive(Debug)]
pub(crate) struct AzureBackend(MicrosoftAzure);

#[async_trait]
impl Backend for AzureBackend {
    const NAME: &'static str = "azure";
    const META_SEPARATOR: char = '_';

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

/// [`crate::ObjectStore`] over Azure Blob Storage.
#[derive(Debug)]
pub struct AzureBlobStore {
    core: Core<AzureBackend>,
}

delegate_object_store!(AzureBlobStore);

impl AzureBlobStore {
    pub fn new(config: AzureBlobConfig) -> Result<Self> {
        let mut builder = match &config.credentials {
            AzureCredentials::Ambient => MicrosoftAzureBuilder::from_env(),
            _ => MicrosoftAzureBuilder::new(),
        }
        .with_account(&config.account)
        .with_container_name(&config.container)
        .with_use_emulator(config.use_emulator);
        if let Some(endpoint) = &config.endpoint {
            builder = builder
                .with_endpoint(endpoint.clone())
                .with_allow_http(config.use_emulator);
        }
        match config.credentials {
            AzureCredentials::Ambient => {}
            AzureCredentials::AccessKey(key) => builder = builder.with_access_key(key),
            AzureCredentials::Vended(vendor) => {
                let scope = CredentialScope {
                    backend: AzureBackend::NAME,
                    bucket: config.container.clone(),
                    prefix: config.scope_prefix.clone(),
                };
                builder = builder.with_credentials(Arc::new(VendedProvider::new(
                    vendor,
                    scope,
                    azure_credential,
                )));
            }
        }
        let store = builder
            .build()
            .map_err(|e| Error::from_store(AzureBackend::NAME, "", e))?;
        Ok(Self {
            core: Core::new(AzureBackend(store)),
        })
    }

    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.core.set_page_size(page_size);
        self
    }
}

fn azure_credential(vended: VendedCredential) -> Option<AzureCredential> {
    match vended {
        VendedCredential::AzureSas { query_pairs, .. } => {
            Some(AzureCredential::SASToken(query_pairs))
        }
        VendedCredential::AzureBearer { token, .. } => Some(AzureCredential::BearerToken(token)),
        _ => None,
    }
}
