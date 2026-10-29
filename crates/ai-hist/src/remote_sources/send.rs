//! `send` — deliver one message into a remote session through its connector.
//!
//! Only connectors with an upstream write path answer; the rest refuse as
//! unsupported. The operation is the inverse of `hydrate`: instead of reading
//! message history out, it writes one message in, over the connector's own
//! authenticated transport, with the credential staying on this machine.

use std::path::Path;

use anyhow::Result;

use super::devin_auth::{devin_auth_explicit, devin_config_path, devin_credentials_path};
use super::devin_cloud::{
    send_devin_session_message, send_devin_session_message_at, UreqDevinTransport,
};
use super::DEVIN_CLOUD_CONNECTOR;

/// Post `message` into the remote session `session_id` belongs to, through
/// the connector that owns it. Error strings carry the operation's public
/// code prefix (`INVALID_ARGUMENT:`, `CONNECTOR_NOT_CONFIGURED:`,
/// `SESSION_NOT_FOUND:`, `AUTHENTICATION_EXPIRED:`, `CONNECTOR_FAILURE:`) so a
/// host can map a refusal without guessing.
pub fn send_remote_message_at(
    home: &Path,
    connector: &str,
    session_id: &str,
    message: &str,
) -> Result<()> {
    match connector {
        DEVIN_CLOUD_CONNECTOR => send_devin_session_message_at(
            &devin_credentials_path(home),
            &devin_config_path(home),
            session_id,
            message,
            &UreqDevinTransport,
        ),
        _ => anyhow::bail!(
            "INVALID_ARGUMENT: source connector '{connector}' cannot deliver messages"
        ),
    }
}

/// The credential a Devin Cloud send runs as, resolved by the host instead
/// of the connector's file/env lookup — a workspace credential store, say.
/// It stays in memory for the one call; nothing is persisted or exported.
pub struct RemoteCredential {
    /// The v3 bearer (`Authorization: Bearer …`), a Cognition API key.
    pub bearer: String,
    /// The v3 organization id the request path addresses.
    pub org_id: String,
    /// v3 base URL; `None` — the usual case — is `https://api.devin.ai`.
    pub base_url: Option<String>,
}

/// [`send_remote_message_at`] with a host-resolved credential: the send runs
/// as `credential` rather than whatever the connector's own lookup would
/// find. The error contract is unchanged; an unusable `credential` itself
/// fails `INVALID_ARGUMENT`, never `CONNECTOR_NOT_CONFIGURED`.
pub fn send_remote_message_with(
    connector: &str,
    session_id: &str,
    message: &str,
    credential: &RemoteCredential,
) -> Result<()> {
    match connector {
        DEVIN_CLOUD_CONNECTOR => send_devin_session_message(
            &devin_auth_explicit(
                &credential.bearer,
                &credential.org_id,
                credential.base_url.as_deref(),
            )?,
            session_id,
            message,
            &UreqDevinTransport,
        ),
        _ => anyhow::bail!(
            "INVALID_ARGUMENT: source connector '{connector}' cannot deliver messages"
        ),
    }
}
