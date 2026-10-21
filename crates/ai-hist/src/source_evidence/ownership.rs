//! Ownership projection shared by intake and native reconciliation.
use super::EvidenceKind;
impl EvidenceKind {
    /// Reported ownership fields, excluding receiver-derived caches.
    pub(crate) fn owned_columns(self) -> impl Iterator<Item = &'static str> {
        let spec = self.spec();
        spec.columns
            .split(',')
            .filter(move |column| !spec.derived.split(',').any(|derived| derived == *column))
    }
}
