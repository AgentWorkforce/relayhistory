//! Short-lived, scoped credentials vended by the cloud (or a customer's
//! broker), refreshed in memory and never persisted.
//!
//! Each adapter accepts either *ambient* credentials (instance profile, IRSA /
//! workload identity, managed identity, environment) or a [`CredentialVendor`].
//! Neither path writes credential material anywhere: adapter configs have no
//! serializable secret fields, and `Debug` output redacts secrets, so nothing
//! that reaches `history.db` or a log can carry a key.

use std::fmt;
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
use tokio::sync::Mutex;

use crate::Result;

/// Refresh a vended credential this long before it expires, so an upload that
/// starts just before expiry still finishes with a valid signature.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
const REFRESH_BEFORE_EXPIRY_SECS: i64 = 5 * 60;

/// What a vended credential must be scoped to: the bucket and the key prefix
/// this adapter writes (`{prefix}/v1/ws={workspace_id}/m={machine_id}`).
/// The vendor should mint a credential that cannot read or write outside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialScope {
    pub backend: &'static str,
    pub bucket: String,
    pub prefix: Option<String>,
}

/// A short-lived credential with an expiry.
#[derive(Clone)]
#[non_exhaustive]
pub enum VendedCredential {
    /// AWS STS temporary credentials (`AssumeRole`/`GetFederationToken`), also
    /// accepted by R2 and MinIO STS.
    Aws {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        expires_at: DateTime<Utc>,
    },
    /// A GCS OAuth2 access token (e.g. from a downscoped token / Credential
    /// Access Boundary).
    GcpBearer {
        token: String,
        expires_at: DateTime<Utc>,
    },
    /// An Azure user-delegation or service SAS, as query pairs.
    AzureSas {
        query_pairs: Vec<(String, String)>,
        expires_at: DateTime<Utc>,
    },
    /// An Azure AD (Entra ID) bearer token.
    AzureBearer {
        token: String,
        expires_at: DateTime<Utc>,
    },
}

impl VendedCredential {
    pub fn expires_at(&self) -> DateTime<Utc> {
        match self {
            Self::Aws { expires_at, .. }
            | Self::GcpBearer { expires_at, .. }
            | Self::AzureSas { expires_at, .. }
            | Self::AzureBearer { expires_at, .. } => *expires_at,
        }
    }

    #[cfg_attr(
        not(any(feature = "s3", feature = "gcs", feature = "azure")),
        allow(dead_code)
    )]
    fn kind(&self) -> &'static str {
        match self {
            Self::Aws { .. } => "aws",
            Self::GcpBearer { .. } => "gcp-bearer",
            Self::AzureSas { .. } => "azure-sas",
            Self::AzureBearer { .. } => "azure-bearer",
        }
    }
}

impl fmt::Debug for VendedCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VendedCredential")
            .field("kind", &self.kind())
            .field("expires_at", &self.expires_at())
            .finish_non_exhaustive()
    }
}

/// Mints scoped, short-lived credentials on demand. Implemented by the
/// delivery host, e.g. by calling the Agent Relay cloud's credential-vending
/// endpoint or a customer's STS broker.
#[async_trait]
pub trait CredentialVendor: Send + Sync {
    async fn vend(&self, scope: &CredentialScope) -> Result<VendedCredential>;
}

#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
pub(crate) use provider::VendedProvider;

#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
mod provider {
    use super::*;

    /// Caches a vended credential and refreshes it ahead of expiry. Converts it to
    /// the `object_store` credential type with `convert`; a vendor returning the
    /// wrong cloud's credential is a configuration error.
    pub(crate) struct VendedProvider<T> {
        vendor: Arc<dyn CredentialVendor>,
        scope: CredentialScope,
        convert: fn(VendedCredential) -> Option<T>,
        cached: Mutex<Option<(Arc<T>, DateTime<Utc>)>>,
    }

    impl<T> VendedProvider<T> {
        pub(crate) fn new(
            vendor: Arc<dyn CredentialVendor>,
            scope: CredentialScope,
            convert: fn(VendedCredential) -> Option<T>,
        ) -> Self {
            Self {
                vendor,
                scope,
                convert,
                cached: Mutex::new(None),
            }
        }
    }

    impl<T> fmt::Debug for VendedProvider<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("VendedProvider")
                .field("scope", &self.scope)
                .finish_non_exhaustive()
        }
    }

    /// A vendor outage (timeout, 503) must stay retryable: carry it as
    /// `Generic` with the original `Error` as source, which `Error::from_store`
    /// classifies by `is_retryable()`. Anything else is an authentication
    /// failure.
    fn vend_error(err: crate::Error) -> object_store::Error {
        if err.is_retryable() {
            object_store::Error::Generic {
                store: "credential-vendor",
                source: Box::new(err),
            }
        } else {
            object_store::Error::Unauthenticated {
                path: String::new(),
                source: Box::new(err),
            }
        }
    }

    #[async_trait]
    impl<T: Send + Sync + 'static> object_store::CredentialProvider for VendedProvider<T> {
        type Credential = T;

        async fn get_credential(&self) -> object_store::Result<Arc<T>> {
            let mut cached = self.cached.lock().await;
            let refresh_at = Utc::now() + chrono::Duration::seconds(REFRESH_BEFORE_EXPIRY_SECS);
            if let Some((credential, expires_at)) = cached.as_ref() {
                if *expires_at > refresh_at {
                    return Ok(Arc::clone(credential));
                }
            }
            let vended = self.vendor.vend(&self.scope).await.map_err(vend_error)?;
            let expires_at = vended.expires_at();
            let kind = vended.kind();
            let credential =
                (self.convert)(vended).ok_or_else(|| object_store::Error::Unauthenticated {
                    path: String::new(),
                    source: format!(
                        "credential vendor returned a {kind} credential for a {} store",
                        self.scope.backend
                    )
                    .into(),
                })?;
            let credential = Arc::new(credential);
            *cached = Some((Arc::clone(&credential), expires_at));
            Ok(credential)
        }
    }
}

#[cfg(all(test, feature = "gcs"))]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use object_store::CredentialProvider as _;

    use super::*;

    struct CountingVendor {
        calls: AtomicUsize,
        ttl: chrono::Duration,
    }

    #[async_trait]
    impl CredentialVendor for CountingVendor {
        async fn vend(&self, _scope: &CredentialScope) -> Result<VendedCredential> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(VendedCredential::GcpBearer {
                token: format!("token-{n}"),
                expires_at: Utc::now() + self.ttl,
            })
        }
    }

    fn provider(ttl: chrono::Duration) -> (Arc<CountingVendor>, VendedProvider<String>) {
        let vendor = Arc::new(CountingVendor {
            calls: AtomicUsize::new(0),
            ttl,
        });
        let scope = CredentialScope {
            backend: "gcs",
            bucket: "b".into(),
            prefix: Some("p/v1/ws=w/m=m".into()),
        };
        let provider = VendedProvider::new(vendor.clone(), scope, |v| match v {
            VendedCredential::GcpBearer { token, .. } => Some(token),
            _ => None,
        });
        (vendor, provider)
    }

    #[tokio::test]
    async fn caches_until_refresh_window() {
        let (vendor, provider) = provider(chrono::Duration::hours(1));
        assert_eq!(*provider.get_credential().await.unwrap(), "token-0");
        assert_eq!(*provider.get_credential().await.unwrap(), "token-0");
        assert_eq!(vendor.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refreshes_inside_refresh_window() {
        // Expires in 2 minutes: inside the 5-minute window, so every call
        // re-vends rather than handing out a credential about to lapse.
        let (vendor, provider) = provider(chrono::Duration::minutes(2));
        assert_eq!(*provider.get_credential().await.unwrap(), "token-0");
        assert_eq!(*provider.get_credential().await.unwrap(), "token-1");
        assert_eq!(vendor.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn rejects_wrong_cloud_credential() {
        let vendor = Arc::new(CountingVendor {
            calls: AtomicUsize::new(0),
            ttl: chrono::Duration::hours(1),
        });
        let scope = CredentialScope {
            backend: "s3",
            bucket: "b".into(),
            prefix: None,
        };
        let provider: VendedProvider<String> =
            VendedProvider::new(vendor, scope, |_| None::<String>);
        let err = provider.get_credential().await.unwrap_err().to_string();
        assert!(
            err.contains("gcp-bearer credential for a s3 store"),
            "{err}"
        );
    }

    struct DownVendor;

    #[async_trait]
    impl CredentialVendor for DownVendor {
        async fn vend(&self, _scope: &CredentialScope) -> Result<VendedCredential> {
            Err(crate::Error::Transient {
                backend: "vendor",
                source: "credential broker returned 503".into(),
            })
        }
    }

    #[tokio::test]
    async fn vendor_outage_stays_retryable() {
        let scope = CredentialScope {
            backend: "gcs",
            bucket: "b".into(),
            prefix: None,
        };
        let provider: VendedProvider<String> =
            VendedProvider::new(Arc::new(DownVendor), scope, |_| None);
        let err = provider.get_credential().await.unwrap_err();
        let mapped = crate::Error::from_store("gcs", "k", err);
        assert!(mapped.is_retryable(), "{mapped:?}");
    }

    #[test]
    fn debug_never_prints_secrets() {
        let credential = VendedCredential::Aws {
            access_key_id: "AKIASECRETID".into(),
            secret_access_key: "very-secret".into(),
            session_token: Some("token-secret".into()),
            expires_at: Utc::now(),
        };
        let printed = format!("{credential:?}");
        assert!(
            !printed.contains("SECRET") && !printed.contains("secret"),
            "{printed}"
        );
    }
}
