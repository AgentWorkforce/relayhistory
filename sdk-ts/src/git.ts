import { defaultDbPath } from './sdk-common.js';
import { nativeCall } from './native.js';
export interface GitHookOptions {
  repo: string;
  sessionId: string;
  source?: string;
  dbPath?: string;
  prUrl?: string;
}
/** Install a local post-commit recorder for an explicit session. prUrl identifies
 * an existing GitHub PR; linkage is uploaded by the next cloud push. */
export async function installGitHooks(
  options: GitHookOptions,
): Promise<{ hookPath: string; prUrl: string | null }> {
  const result = await nativeCall((native) =>
    native.installGitHooks(
      JSON.stringify({ ...options, dbPath: options.dbPath ?? defaultDbPath() }),
      process.execPath,
      import.meta.url,
    ),
  );
  return JSON.parse(result) as { hookPath: string; prUrl: string | null };
}
export async function linkGitCommit(options: GitHookOptions): Promise<{ commitSha: string }> {
  const commitSha = await nativeCall((native) =>
    native.linkGitCommit(JSON.stringify({ ...options, dbPath: options.dbPath ?? defaultDbPath() })),
  );
  return { commitSha };
}
