//! Shared sweep generation includes every admitted provider normalizer.
use crate::ingest::*;

pub(crate) fn sweep_generation() -> String {
    let parts = format!(
        "{}|tool_result_fidelity={TOOL_RESULT_FIDELITY_GENERATION}\
         |raw_message_facts={RAW_MESSAGE_FACTS_GENERATION}\
         |codex_fork_replay={CODEX_FORK_REPLAY_GENERATION}\
         |codex_metadata_backfill={CODEX_METADATA_BACKFILL_GENERATION}",
        SWEEP_PARSER_GENERATIONS
            .iter()
            .map(|s| (*s).to_owned())
            .chain(crate::providers::adapters().map(|(id, a)| format!("{id}:{}", a.generation)))
            .collect::<Vec<_>>()
            .join("|")
    );
    format!(
        "g{:016x}",
        discover::fingerprint_hash(
            "sweep-generation",
            &discover::SHALLOW_SCANNER_VERSION.to_string(),
            &parts,
        )
    )
}

pub(crate) fn source_fingerprint_generation() -> String {
    format!(
        "{}/{}",
        sweep_generation(),
        crate::providers::codex::sqlite::fingerprint_component()
    )
}

pub(crate) fn qualify_source_fingerprint(sources: String) -> Option<String> {
    let fingerprint = format!("{}/{sources}", source_fingerprint_generation());
    crate::providers::codex::sqlite::remember_source_fingerprint(fingerprint.clone());
    Some(fingerprint)
}
