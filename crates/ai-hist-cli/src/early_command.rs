//! Commands that must own their connection before shared CLI query setup.
use super::*;

pub(super) fn run(
    command: &Command,
    path: &Path,
    connectors: &remote::SourceConnectorSelection,
) -> Result<Option<()>> {
    let result = match command {
        Command::Sync { scope } => {
            sync_scoped_at_with_output(path, scope.resolve(), connectors, SyncOutput::Progress)
                .map(|_| ())
        }
        Command::Watch {
            scope,
            interval,
            no_fsevents,
            debounce_ms,
            no_leading_edge,
        } => {
            if scope.resolve() == SessionScope::Remote {
                remote::ensure_selected_remote_connectors_configured_for("sync", &[], connectors)?;
            }
            watch_loop_with_connectors(
                path,
                *interval,
                scope.resolve(),
                connectors,
                WatchDrivers {
                    use_fs_events: !*no_fsevents,
                    debounce_ms: *debounce_ms,
                    leading_edge: !*no_leading_edge,
                },
            )
        }
        Command::Ingest { hook, quiet, json } => run_hook_ingest(path, hook, *quiet, *json),
        Command::Compact { json } => compact(path, *json),
        Command::Retention {
            days,
            apply,
            allow_unuploaded,
            max_rows,
            json,
        } => retention_cli::run(path, *days, *apply, *allow_unuploaded, *max_rows, *json),
        Command::Sessions {
            action: SessionsAction::Discover { scope, source, .. },
        } if scope.resolve() == SessionScope::Remote => {
            remote::ensure_selected_remote_connectors_configured_for(
                "discovery",
                source,
                connectors,
            )?;
            return Ok(None);
        }
        Command::SyncOpencode { opencode_db } => match opencode_db {
            Some(source) => sync_opencode_at(path, source, SyncOutput::Progress),
            None => sync_opencode_with_roots(
                path,
                &ProviderRoots::from_env(home_dir()),
                SyncOutput::Progress,
            ),
        }
        .map(|_| ()),
        _ => return Ok(None),
    };
    result.map(Some)
}
