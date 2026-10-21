/** Shared SDK errors, provider vocabulary, and database path defaults. */
import { homedir } from 'node:os';
import { join } from 'node:path';

export const SESSION_CATALOG_CONTRACT_VERSION = 6;
export const SESSION_HYDRATION_CONTRACT_VERSION = 3;
export const SESSION_RELATIONSHIP_CONTRACT_VERSION = 2;
export const SESSION_EVIDENCE_CONTRACT_VERSION = 4;
export const SESSION_USAGE_CONTRACT_VERSION = 3;

/**
 * The provider ids this build knows, as a value. `Source` is derived from it
 * so the runtime checks below and the compile-time type cannot drift apart,
 * and it is the same list the MCP server's `SOURCE` enum publishes.
 */
// BEGIN GENERATED PROVIDER SOURCES
export const SOURCES = Object.freeze([
  'claude',
  'codex',
  'cursor',
  'grok',
  'opencode',
  'muse',
  'devin',
  'pi',
  'hermes',
  'omp',
  'antigravity',
  'copilot',
  'qwen-code',
  'kimi',
  'codebuddy',
  'qoder',
  'vtcode',
