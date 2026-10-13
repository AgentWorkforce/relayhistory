//! `send` — deliver one message into a remote session through its connector.
//!
//! Only connectors with an upstream write path answer; the rest refuse as
//! unsupported. The operation is the inverse of `hydrate`: instead of reading
//! message history out, it writes one message in, over the connector's own
//! authenticated transport, with the credential staying on this machine.

use std::path::Path;

use anyhow::Result;

use super::devin_auth::{devin_config_path, devin_credentials_path};
use super::devin_cloud::{send_devin_session_message_at, UreqDevinTransport};
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
