import { ensureCloudSession as bundledEnsureCloudSession } from './cloud-auth-bundle.js';

const DEFAULT_CLOUD_API_URL = 'https://agentrelay.com/cloud';
const DEFAULT_LOGIN_TIMEOUT_MS = 5 * 60 * 1000;
const NON_INTERACTIVE_TIMEOUT_MS = 10_000;
const DEFAULT_REFRESH_TIMEOUT_MS = 10_000;

interface CloudSession {
  auth: { accessToken: string };
}

interface EnsureCloudSessionOptions {
  apiUrl: string;
  client: string;
  interactive: boolean;
  device: boolean;
  loginTimeoutMs: number;
  refreshTimeoutMs: number;
  signal: AbortSignal;
  env: NodeJS.ProcessEnv;
}

type EnsureCloudSession = (options: EnsureCloudSessionOptions) => Promise<CloudSession>;

interface PreflightDependencies {
  ensureCloudSession?: EnsureCloudSession;
  interactive?: boolean;
}

function hasTokenFlag(args: readonly string[]): boolean {
  return args.some((arg) => arg === '--token' || arg.startsWith('--token='));
}

/** Whether this command needs Agent Relay Cloud identity prepared by the SDK. */
export function shouldPrepareCloudSession(args: readonly string[], env: NodeJS.ProcessEnv): boolean {
  const command = args[0];
  if (command !== 'enable-cloud' && command !== 'login') return false;
  if (hasTokenFlag(args)) return false;

  // Preserve the existing native environment-token path. It is caller-owned
  // and may intentionally provide only the bearer needed for the exchange.
  if (String(env.CLOUD_API_ACCESS_TOKEN ?? '').trim()) return false;
  return true;
}

/**
 * Prepare Agent Relay Cloud identity before the native RelayHistory exchange.
 * Returns null when the caller supplied credentials that the native layer owns.
 */
export async function prepareCloudSessionForEnableCloud(
  args: readonly string[],
  env: NodeJS.ProcessEnv = process.env,
  dependencies: PreflightDependencies = {},
): Promise<string | null> {
  if (!shouldPrepareCloudSession(args, env)) return null;

  const interactive = dependencies.interactive
    ?? Boolean(process.stdin.isTTY && process.stderr.isTTY);
  const loginTimeoutMs = interactive ? DEFAULT_LOGIN_TIMEOUT_MS : NON_INTERACTIVE_TIMEOUT_MS;
  const ensureCloudSession = dependencies.ensureCloudSession
    ?? bundledEnsureCloudSession as unknown as EnsureCloudSession;
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;

  try {
    const session = await Promise.race([
      ensureCloudSession({
        apiUrl: String(env.CLOUD_API_URL ?? '').trim() || DEFAULT_CLOUD_API_URL,
        client: 'relayhistory',
        interactive,
        device: false,
        loginTimeoutMs,
        refreshTimeoutMs: DEFAULT_REFRESH_TIMEOUT_MS,
        signal: controller.signal,
        env,
      }),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          const error = new Error(`Agent Relay Cloud sign-in timed out after ${loginTimeoutMs}ms`);
          controller.abort(error);
          reject(error);
        }, loginTimeoutMs);
      }),
    ]);
    const accessToken = session.auth.accessToken.trim();
    if (!accessToken) throw new Error('Agent Relay Cloud session did not contain an access token');
    return accessToken;
  } catch (error) {
    if (!interactive) {
      throw new Error(
        'Agent Relay Cloud login requires an interactive terminal. Re-run this command in a terminal '
        + 'to use browser/device login, or provide --token (with --base-url for login) or '
        + 'CLOUD_API_ACCESS_TOKEN for non-interactive use.',
        { cause: error },
      );
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
