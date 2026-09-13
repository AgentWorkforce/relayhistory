/** Shared Node-API loading, contract validation, and error translation. */
import {
  RelayHistoryError,
  UnsupportedPlatformError,
  NativePackageMissingError,
  NativeLoadError,
  NativeContractMismatchError,
  DatabaseOpenError,
  InvalidArgumentError,
  UnsupportedOperationError,
  SessionNotFoundError,
  SessionSourceUnavailableError,
  SessionSourceMismatchError,
  HydrationUnsupportedError,
  HydrationFailedError,
  ConnectorNotConfiguredError,
  AuthenticationExpiredError,
  EvidencePartialError,
  ConnectorFailureError,
} from './sdk-common.js';
import type { CloudPushResult, RelayhistoryAuth, ReplayOptions, ReplayResult } from './cloud-client.js';

export const NATIVE_CONTRACT_VERSION = 13;
type UnknownRecord = Record<string, unknown>;

interface NativeBinding {
  historyDelivery(requestJson: string, dbPath?: string): Promise<string>;
  accessToken(baseUrl?: string): Promise<string>;
  replay(sessionId: string, options: ReplayOptions): Promise<ReplayResult>;
  createShareableTrace(sessionId: string, visibility: string, source?: string, baseUrl?: string): Promise<string>;
  installGitHooks(optionsJson: string, node: string, sdkUrl: string): Promise<string>;
  linkGitCommit(optionsJson: string): Promise<string>;
  cloudLoadAuth(baseUrl?: string): Promise<RelayhistoryAuth | null>;
  cloudResolveSession(baseUrl: string | undefined, now: number): Promise<{ auth?: RelayhistoryAuth | null; detail?: string }>;
  cloudRefreshSession(baseUrl: string, rejectedToken: string): Promise<RelayhistoryAuth | null>;
  cloudValidateExchangeBaseUrl(baseUrl?: string): Promise<void>;
  cloudLogin(options: object): Promise<RelayhistoryAuth>;
  enableCloud(options: object): Promise<CloudPushResult>;
  pushCloud(options: object): Promise<CloudPushResult>;
  nativeContractVersion(): number;
  nativeBuildProfile?(): string;
  search(query: string, options?: object): Promise<UnknownRecord[]>;
  recent(options?: object): Promise<UnknownRecord[]>;
  getSession(sessionId: string, options?: object): Promise<UnknownRecord[]>;
  getSessionEventsPage(sessionId: string, options?: object): Promise<UnknownRecord>;
  getSessionToolCallsPage(source: string, sessionId: string, options?: object): Promise<UnknownRecord>;
  getSessionFileEditsPage(source: string, sessionId: string, options?: object): Promise<UnknownRecord>;
  stats(options?: object): Promise<UnknownRecord>;
  listSessionCatalog(options?: object): Promise<UnknownRecord[]>;
  listSessionCatalogPage(options?: object): Promise<UnknownRecord>;
  discoverSessions(options?: object): Promise<UnknownRecord>;
  hydrateSession(options: object): Promise<UnknownRecord>;
  getSessionRelationships(options: object): Promise<UnknownRecord>;
  getSessionTree(options: object): Promise<UnknownRecord>;
  getSessionChildrenPage(options: object): Promise<UnknownRecord>;
  sync(options?: object): Promise<UnknownRecord>;
}

const SUPPORTED_PLATFORMS = new Set([
  'darwin-arm64', 'darwin-x64',
  'linux-arm64-gnu', 'linux-arm64-musl',
  'linux-x64-gnu', 'linux-x64-musl',
  'win32-x64-msvc',
]);

function linuxLibc(): 'gnu' | 'musl' {
  const report = process.report?.getReport() as { header?: { glibcVersionRuntime?: string } } | undefined;
  return report?.header?.glibcVersionRuntime ? 'gnu' : 'musl';
}

export function runtimePlatform(): string {
  if (process.platform === 'linux') return `linux-${process.arch}-${linuxLibc()}`;
  if (process.platform === 'win32') return `win32-${process.arch}-msvc`;
  return `${process.platform}-${process.arch}`;
}

let nativePromise: Promise<NativeBinding> | null = null;

export function validateNativeContract(actual: number): void {
  if (actual !== NATIVE_CONTRACT_VERSION) {
    throw new NativeContractMismatchError(
      `ai-hist requires native contract ${NATIVE_CONTRACT_VERSION}, but ai-hist-native provides ${actual}. Reinstall matching versions.`,
      'NATIVE_CONTRACT_MISMATCH',
    );
  }
}

async function loadNative(): Promise<NativeBinding> {
  if (nativePromise) return nativePromise;
  nativePromise = (async () => {
    const platform = runtimePlatform();
    if (!SUPPORTED_PLATFORMS.has(platform)) {
      throw new UnsupportedPlatformError(
        `RelayHistory has no native build for ${platform}. Supported platforms: ${[...SUPPORTED_PLATFORMS].join(', ')}.`,
        'UNSUPPORTED_PLATFORM',
      );
    }
    let loaded: unknown;
    try {
      // Kept as a variable so TypeScript does not require native build-time
      // declarations; npm installs this mandatory production dependency.
      const packageName = 'ai-hist-native';
      loaded = await import(packageName);
    } catch (cause) {
      const error = cause as NodeJS.ErrnoException;
      if (error.code === 'ERR_MODULE_NOT_FOUND' || error.code === 'MODULE_NOT_FOUND') {
        throw new NativePackageMissingError(
          `RelayHistory supports ${platform}, but its native package is missing. Reinstall ai-hist with optional dependencies enabled.`,
          'NATIVE_PACKAGE_MISSING',
          { cause },
        );
      }
      throw new NativeLoadError(
        `RelayHistory's native package for ${platform} failed to load: ${error.message}`,
        'NATIVE_LOAD_FAILED',
        { cause },
      );
    }
    const binding = ((loaded as { default?: unknown }).default ?? loaded) as NativeBinding;
    if (typeof binding.nativeContractVersion !== 'function') {
      throw new NativeContractMismatchError(
        'The installed ai-hist-native package does not expose a contract version. Reinstall matching ai-hist packages.',
        'NATIVE_CONTRACT_MISMATCH',
      );
    }
    validateNativeContract(binding.nativeContractVersion());
    return binding;
  })();
  return nativePromise.catch((error) => {
    nativePromise = null;
    throw error;
  });
}

function nativeMessage(error: unknown): { code: string; message: string } | null {
  const message = error instanceof Error ? error.message : String(error);
  const match = message.match(/RELAYHISTORY_NATIVE::([A-Z_]+)::([\s\S]*)/);
  return match ? { code: match[1], message: match[2] } : null;
}

export async function nativeCall<T>(call: (binding: NativeBinding) => Promise<T>): Promise<T> {
  try {
    return await call(await loadNative());
  } catch (error) {
    if (error instanceof RelayHistoryError) throw error;
    const native = nativeMessage(error);
    if (!native) throw new NativeLoadError(String(error), 'NATIVE_CALL_FAILED', { cause: error });
    if (native.code === 'DATABASE_OPEN_FAILED') {
      throw new DatabaseOpenError(native.message, native.code, { cause: error });
    }
    if (native.code === 'INVALID_ARGUMENT') {
      throw new InvalidArgumentError(native.message, native.code, { cause: error });
    }
    if (native.code === 'UNSUPPORTED_OPERATION') {
      throw new UnsupportedOperationError(native.message, native.code, { cause: error });
    }
    if (native.code === 'SESSION_NOT_FOUND') {
      throw new SessionNotFoundError(native.message, native.code, { cause: error });
    }
    if (native.code === 'SESSION_SOURCE_UNAVAILABLE') {
      throw new SessionSourceUnavailableError(native.message, native.code, { cause: error });
    }
    if (native.code === 'SESSION_SOURCE_MISMATCH') {
      throw new SessionSourceMismatchError(native.message, native.code, { cause: error });
    }
    if (native.code === 'HYDRATION_UNSUPPORTED') {
      throw new HydrationUnsupportedError(native.message, native.code, { cause: error });
    }
    if (native.code === 'HYDRATION_FAILED') {
      throw new HydrationFailedError(native.message, native.code, { cause: error });
    }
    if (native.code === 'CONNECTOR_NOT_CONFIGURED') {
      throw new ConnectorNotConfiguredError(native.message, native.code, { cause: error });
    }
    if (native.code === 'AUTHENTICATION_EXPIRED') {
      throw new AuthenticationExpiredError(native.message, native.code, { cause: error });
    }
    if (native.code === 'EVIDENCE_PARTIAL') {
      throw new EvidencePartialError(native.message, native.code, { cause: error });
    }
    if (native.code === 'CONNECTOR_FAILURE') {
      throw new ConnectorFailureError(native.message, native.code, { cause: error });
    }
    throw new RelayHistoryError(native.message, native.code, { cause: error });
  }
}
