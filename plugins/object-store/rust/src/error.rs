/// Errors returned by every [`crate::ObjectStore`] implementation.
///
/// The delivery coordinator branches on these variants, so each backend maps
/// its native failures onto the same set; see [`Error::is_retryable`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("object not found: {key}")]
    NotFound { key: String },

    /// An `if_none_match` put found an object with *different* content.
    /// (Identical content is an idempotent success, not this error.)
    #[error("object already exists with different content: {key}")]
    AlreadyExists { key: String },

    /// An `if_match` put lost a race: the stored version changed.
    #[error("precondition failed (object changed concurrently): {key}")]
    PreconditionFailed { key: String },

    /// The body did not hash to the caller's `content_sha256`.
    #[error("content sha256 mismatch for {key}: expected {expected}, computed {actual}")]
    ContentMismatch {
        key: String,
        expected: String,
        actual: String,
    },

    #[error("invalid object key {0}")]
    InvalidKey(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The backend or the configured credential type cannot perform the
    /// operation (e.g. presigning with a bearer-only GCS token).
    #[error("{backend} does not support {operation}")]
    Unsupported {
        backend: &'static str,
        operation: String,
    },

    /// Authentication or authorization failed. Never carries credential
    /// material.
    #[error("{backend} denied access: {message}")]
    Unauthorized {
        backend: &'static str,
        message: String,
    },

    /// Network, throttling or server-side failure; safe to retry.
    #[error("{backend} transient failure: {source}")]
    Transient {
        backend: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A manifest compare-and-swap lost every race in its attempt budget.
    /// Retry on the next tick (client) or re-queue (server).
    #[error("manifest {key}: lost {attempts} compare-and-swap races in a row")]
    ManifestContended { key: String, attempts: u32 },

    #[error("{backend} error: {source}")]
    Backend {
        backend: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Whether the coordinator should retry the same request with backoff.
    /// `PreconditionFailed` is not retryable as-is: re-read the manifest first.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Transient { .. } | Self::ManifestContended { .. }
        )
    }

    /// Map an `object_store` error for `key`. `object_store` already retries
    /// transient failures internally (`RetryConfig`); a transport failure or a
    /// 408/429/5xx that reaches here has exhausted those retries and is still
    /// reported as transient so the coordinator reschedules rather than
    /// dead-letters. Other HTTP 4xx and non-HTTP failures are permanent.
    pub(crate) fn from_store(backend: &'static str, key: &str, err: object_store::Error) -> Self {
        use object_store::Error as E;
        match err {
            E::NotFound { .. } => Self::NotFound { key: key.into() },
            E::AlreadyExists { .. } => Self::AlreadyExists { key: key.into() },
            E::Precondition { .. } => Self::PreconditionFailed { key: key.into() },
            E::NotImplemented { operation, .. } => Self::Unsupported { backend, operation },
            E::NotSupported { source } => Self::Unsupported {
                backend,
                operation: source.to_string(),
            },
            E::PermissionDenied { source, .. } | E::Unauthenticated { source, .. } => {
                Self::Unauthorized {
                    backend,
                    message: source.to_string(),
                }
            }
            E::InvalidPath { source } => Self::InvalidKey(source.to_string()),
            E::Generic { source, .. } if is_transient(source.as_ref()) => {
                Self::Transient { backend, source }
            }
            E::Generic { source, .. } => Self::Backend { backend, source },
            other => Self::Backend {
                backend,
                source: Box::new(other),
            },
        }
    }
}

/// `object_store` keeps its HTTP error types crate-private, so classify by
/// walking the source chain: a transport error type, or the status code in the
/// terminal "non-2xx status code: NNN" message.
fn is_transient(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(e) = current {
        // A vended-credential outage surfaces as this crate's own error.
        if let Some(own) = e.downcast_ref::<Error>() {
            return own.is_retryable();
        }
        #[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
        if e.downcast_ref::<object_store::client::HttpError>()
            .is_some()
        {
            return true;
        }
        if let Some(status) = status_in_message(&e.to_string()) {
            return status == 408 || status == 429 || status >= 500;
        }
        current = e.source();
    }
    false
}

fn status_in_message(message: &str) -> Option<u16> {
    const MARKER: &str = "status code: ";
    let start = message.find(MARKER)? + MARKER.len();
    message.get(start..start + 3)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generic(message: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: message.to_string().into(),
        }
    }

    #[test]
    fn classifies_http_failures() {
        let transient = [
            "Server returned non-2xx status code: 503 Service Unavailable: slow down",
            "Server returned non-2xx status code: 429 Too Many Requests: ",
        ];
        for message in transient {
            assert!(
                Error::from_store("s3", "k", generic(message)).is_retryable(),
                "{message}"
            );
        }
        let permanent = [
            "Server returned non-2xx status code: 400 Bad Request: invalid uploadType",
            "Missing version",
        ];
        for message in permanent {
            assert!(
                !Error::from_store("s3", "k", generic(message)).is_retryable(),
                "{message}"
            );
        }
    }

    #[test]
    fn maps_conditional_failures() {
        let err = object_store::Error::Precondition {
            path: "k".into(),
            source: "412".into(),
        };
        assert!(matches!(
            Error::from_store("s3", "k", err),
            Error::PreconditionFailed { .. }
        ));
        let err = object_store::Error::NotFound {
            path: "k".into(),
            source: "404".into(),
        };
        assert!(matches!(
            Error::from_store("s3", "k", err),
            Error::NotFound { .. }
        ));
    }
}
