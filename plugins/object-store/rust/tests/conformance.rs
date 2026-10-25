//! Runs the shared conformance suite against every adapter.
//!
//! `FsStore` always runs. The cloud adapters run against local emulators when
//! their endpoint variable is set (see `scripts/emulators.sh`); otherwise the
//! test prints that it was skipped. CI sets all of them. Set
//! `RH_OBJSTORE_REQUIRE_EMULATORS=1` to turn a missing endpoint into a failure.
//!
//! | Backend | Emulator          | Variables                                                      |
//! | ------- | ----------------- | -------------------------------------------------------------- |
//! | S3      | MinIO             | `RH_OBJSTORE_S3_ENDPOINT`, `_S3_ACCESS_KEY_ID`, `_S3_SECRET_ACCESS_KEY` |
//! | GCS     | fake-gcs-server   | `RH_OBJSTORE_GCS_BASE_URL`, optional `_GCS_SERVICE_ACCOUNT_KEY_FILE` |
//! | Azure   | Azurite           | `RH_OBJSTORE_AZURITE_ENDPOINT`                                 |

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use relayhistory_object_store::conformance::{run_suite, CaseOutcome, Fetcher, SuiteOptions};
use relayhistory_object_store::*;

const PAGE_SIZE: usize = 3;
const BUCKET: &str = "rh-conformance";

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn emulator(name: &str) -> Option<String> {
    let value = env(name);
    if value.is_none() {
        assert!(
            env("RH_OBJSTORE_REQUIRE_EMULATORS").is_none(),
            "{name} is required when RH_OBJSTORE_REQUIRE_EMULATORS is set"
        );
        eprintln!("skipping: {name} not set");
    }
    value
}

fn run_prefix(backend: &str) -> ObjectKey {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    ObjectKey::new(format!("conformance/{backend}/{nanos}")).unwrap()
}

fn http_fetcher() -> Fetcher {
    let client = reqwest::Client::new();
    Box::new(move |url| {
        let client = client.clone();
        async move {
            let resp = client.get(url).send().await.map_err(|e| e.to_string())?;
            let status = resp.status();
            let body = resp.bytes().await.map_err(|e| e.to_string())?;
            if !status.is_success() {
                return Err(format!("HTTP {status}: {}", String::from_utf8_lossy(&body)));
            }
            Ok(body)
        }
        .boxed()
    })
}

fn file_fetcher() -> Fetcher {
    Box::new(|url| {
        async move {
            let path = url
                .to_file_path()
                .map_err(|()| format!("not a file url: {url}"))?;
            tokio::fs::read(path)
                .await
                .map(Bytes::from)
                .map_err(|e| e.to_string())
        }
        .boxed()
    })
}

async fn assert_conforms(
    backend: &str,
    store: &dyn relayhistory_object_store::ObjectStore,
    fetch: Fetcher,
    skip: Vec<(&'static str, &'static str)>,
) {
    let opts = SuiteOptions {
        prefix: run_prefix(backend),
        page_size: PAGE_SIZE,
        fetch,
        skip,
    };
    let results = run_suite(store, &opts).await;
    let mut failed = Vec::new();
    for result in &results {
        match &result.outcome {
            CaseOutcome::Passed => eprintln!("[{backend}] pass  {}", result.name),
            CaseOutcome::Skipped(why) => eprintln!("[{backend}] SKIP  {} ({why})", result.name),
            CaseOutcome::Failed(msg) => {
                eprintln!("[{backend}] FAIL  {}: {msg}", result.name);
                failed.push(result.name);
            }
        }
    }
    assert!(failed.is_empty(), "[{backend}] failed cases: {failed:?}");
}

#[tokio::test]
async fn fs_store_conforms() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsStore::new(FsConfig::new(dir.path()))
        .unwrap()
        .with_page_size(PAGE_SIZE);
    assert_conforms("fs", &store, file_fetcher(), vec![]).await;
    // Bookkeeping never leaks into listings.
    let all = store
        .list(&ObjectKey::new("conformance").unwrap(), None)
        .await
        .unwrap();
    assert!(all.items.iter().all(|m| !m.key.as_str().contains(".rh")));
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn s3_store_conforms_on_minio() {
    let Some(endpoint) = emulator("RH_OBJSTORE_S3_ENDPOINT") else {
        return;
    };
    let config = S3Config {
        endpoint: Some(endpoint),
        allow_http: true,
        credentials: S3Credentials::Static {
            access_key_id: env("RH_OBJSTORE_S3_ACCESS_KEY_ID").expect("S3 access key id"),
            secret_access_key: env("RH_OBJSTORE_S3_SECRET_ACCESS_KEY")
                .expect("S3 secret access key"),
            session_token: None,
        },
        ..S3Config::new(BUCKET, "us-east-1")
    };
    emulator_setup::create_s3_bucket(&config).await;
    let store = S3Store::new(config).unwrap().with_page_size(PAGE_SIZE);
    assert_conforms("s3", &store, http_fetcher(), vec![]).await;
}

/// Stands in for the cloud's credential-vending endpoint.
struct FakeVendor(VendedCredential);

#[async_trait]
impl CredentialVendor for FakeVendor {
    async fn vend(&self, _scope: &CredentialScope) -> Result<VendedCredential> {
        Ok(self.0.clone())
    }
}

#[cfg(feature = "gcs")]
#[tokio::test]
async fn gcs_store_conforms_on_fake_gcs_server() {
    let Some(base_url) = emulator("RH_OBJSTORE_GCS_BASE_URL") else {
        return;
    };
    emulator_setup::create_gcs_bucket(&base_url).await;
    // Known fake-gcs-server gaps (the same ones object_store's own GCS suite
    // skips). Run `gcs_store_conforms_on_real_gcs` to cover them.
    const NO_PRECONDITIONS: &str =
        "fake-gcs-server ignores x-goog-if-generation-match and returns no generation";
    const NO_MULTIPART: &str = "fake-gcs-server lacks XML multipart uploads (#852)";
    let mut skip = vec![
        ("create_is_idempotent_for_same_content", NO_PRECONDITIONS),
        ("create_conflicts_on_different_content", NO_PRECONDITIONS),
        ("if_match_compare_and_swap", NO_PRECONDITIONS),
        ("if_match_concurrent_single_winner", NO_PRECONDITIONS),
        ("multipart", NO_MULTIPART),
        ("multipart_digest_mismatch_aborts", NO_MULTIPART),
    ];
    let credentials = match env("RH_OBJSTORE_GCS_SERVICE_ACCOUNT_KEY_FILE") {
        Some(path) => GcsCredentials::ServiceAccountKey(std::fs::read_to_string(path).unwrap()),
        None => {
            skip.push((
                "presign_get",
                "no RH_OBJSTORE_GCS_SERVICE_ACCOUNT_KEY_FILE to sign with",
            ));
            GcsCredentials::Vended(Arc::new(FakeVendor(VendedCredential::GcpBearer {
                token: "fake-gcs-server-ignores-auth".into(),
                expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
            })))
        }
    };
    let config = GcsConfig {
        base_url: Some(base_url),
        credentials,
        ..GcsConfig::new(BUCKET)
    };
    let store = GcsStore::new(config).unwrap().with_page_size(PAGE_SIZE);
    assert_conforms("gcs", &store, http_fetcher(), skip).await;
}

#[cfg(feature = "azure")]
#[tokio::test]
async fn azure_store_conforms_on_azurite() {
    let Some(endpoint) = emulator("RH_OBJSTORE_AZURITE_ENDPOINT") else {
        return;
    };
    emulator_setup::create_azure_container(&endpoint).await;
    let config = AzureBlobConfig {
        endpoint: Some(endpoint),
        use_emulator: true,
        credentials: AzureCredentials::AccessKey(emulator_setup::AZURITE_KEY.into()),
        ..AzureBlobConfig::new(emulator_setup::AZURITE_ACCOUNT, BUCKET)
    };
    let store = AzureBlobStore::new(config)
        .unwrap()
        .with_page_size(PAGE_SIZE);
    assert_conforms("azure", &store, http_fetcher(), vec![]).await;
}

/// Opt-in runs against real cloud buckets with ambient credentials (e.g. CI
/// OIDC). Never run by default; each needs its own `RH_OBJSTORE_REAL_*` bucket.
#[cfg(feature = "s3")]
#[tokio::test]
async fn s3_store_conforms_on_real_s3() {
    let (Some(bucket), Some(region)) = (
        env("RH_OBJSTORE_REAL_S3_BUCKET"),
        env("RH_OBJSTORE_REAL_S3_REGION"),
    ) else {
        return;
    };
    let store = S3Store::new(S3Config::new(bucket, region))
        .unwrap()
        .with_page_size(PAGE_SIZE);
    assert_conforms("real-s3", &store, http_fetcher(), vec![]).await;
}

#[cfg(feature = "gcs")]
#[tokio::test]
async fn gcs_store_conforms_on_real_gcs() {
    let Some(bucket) = env("RH_OBJSTORE_REAL_GCS_BUCKET") else {
        return;
    };
    let store = GcsStore::new(GcsConfig::new(bucket))
        .unwrap()
        .with_page_size(PAGE_SIZE);
    assert_conforms("real-gcs", &store, http_fetcher(), vec![]).await;
}

#[cfg(feature = "azure")]
#[tokio::test]
async fn azure_store_conforms_on_real_azure() {
    let (Some(account), Some(container)) = (
        env("RH_OBJSTORE_REAL_AZURE_ACCOUNT"),
        env("RH_OBJSTORE_REAL_AZURE_CONTAINER"),
    ) else {
        return;
    };
    let store = AzureBlobStore::new(AzureBlobConfig::new(account, container))
        .unwrap()
        .with_page_size(PAGE_SIZE);
    assert_conforms("real-azure", &store, http_fetcher(), vec![]).await;
}

/// Bucket/container creation, which the adapters deliberately do not offer:
/// provisioning is the operator's job, not the delivery path's.
mod emulator_setup {
    #[cfg(feature = "azure")]
    pub const AZURITE_ACCOUNT: &str = "devstoreaccount1";
    /// Azurite's published, well-known development key (not a secret).
    #[cfg(feature = "azure")]
    pub const AZURITE_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    /// Presign `PUT /{bucket}/` (the object path `""`) with `object_store`'s
    /// SigV4 signer and send it: S3's `CreateBucket`.
    #[cfg(feature = "s3")]
    pub async fn create_s3_bucket(config: &relayhistory_object_store::S3Config) {
        use object_store::signer::{Method, Signer};
        let relayhistory_object_store::S3Credentials::Static {
            access_key_id,
            secret_access_key,
            ..
        } = &config.credentials
        else {
            panic!("emulator setup needs static credentials");
        };
        let signer = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_endpoint(config.endpoint.clone().unwrap())
            .with_allow_http(true)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key)
            .build()
            .unwrap();
        let url = signer
            .signed_url(
                Method::PUT,
                &object_store::path::Path::from(""),
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
        let resp = reqwest::Client::new()
            .put(url)
            .send()
            .await
            .expect("create bucket");
        let status = resp.status();
        // 409 BucketAlreadyOwnedByYou on re-runs.
        assert!(
            status.is_success() || status.as_u16() == 409,
            "create bucket: HTTP {status}: {}",
            resp.text().await.unwrap_or_default()
        );
    }

    #[cfg(feature = "gcs")]
    pub async fn create_gcs_bucket(base_url: &str) {
        let status = reqwest::Client::new()
            .post(format!("{base_url}/storage/v1/b?project=test"))
            .header("content-type", "application/json")
            .body(format!("{{\"name\":\"{}\"}}", super::BUCKET))
            .send()
            .await
            .expect("create gcs bucket")
            .status();
        assert!(
            status.is_success() || status.as_u16() == 409,
            "create gcs bucket: HTTP {status}"
        );
    }

    /// `Create Container` with Shared Key auth against Azurite.
    #[cfg(feature = "azure")]
    pub async fn create_azure_container(endpoint: &str) {
        use base64::Engine as _;
        use hmac::{Hmac, Mac};

        let endpoint = endpoint.trim_end_matches('/');
        let url = format!("{endpoint}/{}?restype=container", super::BUCKET);
        let date = httpdate::fmt_http_date(std::time::SystemTime::now());
        let version = "2021-08-06";
        let path = url::Url::parse(&url).unwrap().path().to_string();
        let string_to_sign = format!(
            "PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:{version}\n/{AZURITE_ACCOUNT}{path}\nrestype:container"
        );
        let key = base64::engine::general_purpose::STANDARD
            .decode(AZURITE_KEY)
            .unwrap();
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
        mac.update(string_to_sign.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        let resp = reqwest::Client::new()
            .put(&url)
            .header("x-ms-date", date)
            .header("x-ms-version", version)
            .header("content-length", "0")
            .header(
                "authorization",
                format!("SharedKey {AZURITE_ACCOUNT}:{signature}"),
            )
            .send()
            .await
            .expect("create azure container");
        let status = resp.status();
        assert!(
            status.is_success() || status.as_u16() == 409,
            "create azure container: HTTP {status}: {}",
            resp.text().await.unwrap_or_default()
        );
    }
}
