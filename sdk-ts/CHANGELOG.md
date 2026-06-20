# Changelog

All notable changes to `ai-hist` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Add a scoped Rust-first `ai-hist` wrapper with explicit Python fallback for
  commands and flags that are not full Rust parity yet.
- Preserve legacy full-source sync and richer export/import/tagging behavior on
  the Python fallback path during the cutover.

### Fixed

- Align Rust default database path with `XDG_DATA_HOME`.
- Create legacy session metadata schema from Rust DB initialization.
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
