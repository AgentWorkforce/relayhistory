//! Optional provider-native profile metadata, resolved locally without hydration.
use super::{catalog_session, Error, SessionRef, SessionStore, Source};
use serde::{Deserialize, Serialize};

/// A provider's current profile for one catalogued session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionProfile {
    pub name: Option<String>,
    pub avatar: Option<SessionAvatar>,
}

/// Provider-native avatar settings. Shape and color are observed identifiers,
/// not inferred artwork. Consumers own rendering and animation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionAvatar {
    pub shape: Option<String>,
    pub color: Option<String>,
    /// An inline raster image when the provider saved one locally. Never a
    /// remote URL, credential, or SVG capable of loading external resources.
    pub image_data_url: Option<String>,
}

impl SessionStore {
    /// Read optional current profile metadata for a catalogued local session.
    /// Supported by Grok Bot. Reads only its bounded companion roster in the
    /// same account slot, without opening the transcript, hydrating, writing,
    /// making network requests, or accessing credentials. Missing or unreadable
    /// optional metadata returns `None`; catalog errors remain errors.
    pub fn session_profile(&self, session: &SessionRef) -> Result<Option<SessionProfile>, Error> {
        if session.source() != Source::GrokBot {
            return Ok(None);
        }
        let conn = self.read_conn()?;
        let Some(row) = catalog_session(&conn, session, false)? else {
            return Ok(None);
        };
        Ok(row.raw_path.as_deref().and_then(|path| {
            crate::providers::grok_bot::profile::read(path, &row.session_id, &self.roots)
        }))
    }
}
