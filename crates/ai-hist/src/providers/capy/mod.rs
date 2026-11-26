//! Capy sessions live in hosted History. This computer has no Capy transcript files.
use super::*;
use crate::discover::{DiscoveryEnv, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::MessageIdOrigin;
use anyhow::Result;

const ID: &str = "capy";

pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| Box::new(Provider)),
    hydration: Hydration::Parser(ingest),
    transcript_roots: |_| Vec::new(),
    relationships: Relationships::NONE,
    resume: None,
    fixtures: Fixtures::Exempt(
        "Capy transcripts are uploaded to hosted History. This checkout has no local Capy files.",
    ),
};

pub(crate) const METADATA: ProviderMetadata = ProviderMetadata {
    markers: false,
    message_ids: MessageIdOrigin::Provider,
    usage: None,
};

pub(crate) const ADAPTER: Option<ProviderAdapter> = None;

struct Provider;

impl ShallowSessionProvider for Provider {
    fn source(&self) -> &'static str {
        ID
    }

    fn enumerate(
        &self,
        _: &DiscoveryEnv<'_>,
        _: Option<usize>,
    ) -> Result<Vec<crate::discover::Candidate>> {
        Ok(Vec::new())
    }

    fn read_shallow(
        &self,
        _: &crate::discover::ScanEnv<'_>,
        _: Option<&rusqlite::Connection>,
        _: &crate::discover::Candidate,
    ) -> Result<Option<crate::discover::ShallowSession>> {
        Ok(None)
    }
}

/// Nothing local to index. Hosted rows are already stored under source `capy`.
fn ingest(_: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    Ok((IngestOutcome::default(), Vec::new(), None))
}

#[cfg(test)]
mod tests {
    #[test]
    fn capy_is_an_admitted_source_and_not_a_jam_id() {
        assert_eq!(
            crate::session_store::Source::parse("capy"),
            Some(crate::session_store::Source::Capy)
        );
        assert!(crate::SOURCE_CHOICES.contains(&"capy"));
        assert!(crate::session_store::Source::parse("jam_01M6CPFKN8CYWRQFHG2820P5AT").is_none());
        assert!(crate::session_store::Source::parse("capy-01m6cpfk").is_none());
        assert!(matches!(
            crate::sources::catalog::local_source("capy").map(|source| source.fixtures()),
            Some(crate::sources::catalog::Fixtures::Exempt(_))
        ));
    }
}
