# Changelog

All notable changes to `ai-hist` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.7] - 2026-06-27

### Added

- **Add cloud-client module with loginCloud and loadStoredRelayhistoryAuth**

## [0.3.5] - 2026-06-24

### Added

- **Add grok history source**

### Fixed

- Address grok review feedback

### Documentation

- Pair + cloud-sync guides + Pair client SDK/hook (#25)

## [0.3.4] - 2026-06-20

### Changed

- Make the public `ai-hist` command Rust-default for the user-facing CLI
  surface, including sync, show/context/session, stats, pack, resume,
  export/import, and tagging commands.
- Add a one-command installer that builds and installs deterministic `ai-hist`,
  `ai-hist-rust`, and `ai-hist-python` launchers without requiring users to run
  Cargo commands manually.
- Keep the legacy Python CLI as an explicit compatibility escape hatch via
  `AI_HIST_CLI=python` or `ai-hist-python`.

### Fixed

- Align Rust default database path with `XDG_DATA_HOME`.
- Create legacy session metadata schema from Rust DB initialization.
- Set WAL mode from Rust DB initialization.
- Keep the legacy Python fallback importable on Python 3.9.6 by avoiding PEP
  604-only annotations.

## [0.3.2] - 2026-06-12

### Added

- **Add MCP project scope argument**

### Dependencies

- Apply pr-reviewer fixes for #11 (#11)
- Apply pr-reviewer fixes for #11 (#11)

## [0.3.1] - 2026-06-06

### Added

- **Add ai-hist-mcp wrapper package**
- **Add ai-hist MCP trajectory source**
- **Add TypeScript MCP server and Smithery config for ai-hist-mcp**

### Documentation

- Keep MCP local-first

### Dependencies

- Apply pr-reviewer fixes for #9 (#9)
- Apply pr-reviewer fixes for #9 (#9)

## [0.2.3] - 2026-05-22

### Changed

- Rewrite listSessions to use window functions (~68x faster)

## [0.2.1] - 2026-05-22

### Added

- **Native JSONL fallback — SDK works without the Python CLI**
