/**
 * `ai-hist`'s Relay CLI surface: the command tree `agent-relay sessions` mounts.
 *
 * Nothing here re-implements a command. The local half is the `COMMANDS` table
 * `runCli` already dispatches on, read once and rendered two ways — as the
 * declared tree a host builds help from, and as the argv translation `run`
 * performs. `relay-cli.test.ts` walks both and fails if they disagree.
 *
 * The cloud half is composed in rather than mounted separately, so
 * `agent-relay sessions` is one tree: pass a `@relayhistory/cloud-client` and
 * the `cloud` subcommands appear; leave it out and they do not.
 */
import { createRequire } from 'node:module';
import { Writable } from 'node:stream';

import type {
  RelayCliCommandSpec,
  RelayCliIo,
  RelayCliOptionSpec,
  RelayCliSurface,
} from '@agent-relay/cli-surface';

import {
  COMMANDS, FLAG_SPECS, HOST_OWNED_FLAGS, humanLine, output, runCli,
  type CliIo, type CommandArgSpec, type CommandSpec,
} from './cli.js';
import { cloudErrorStatus, type RelayhistoryCloudClient } from './cloud-contract.js';

/** The published version, read from the same manifest `ai-hist --version` reports. */
const packageVersion = (createRequire(import.meta.url)('../package.json') as { version: string }).version;

export type { RelayhistoryCloudClient } from './cloud-contract.js';

/** Exit code the contract reserves for an unroutable command line. */
const EXIT_UNKNOWN_COMMAND = 2;

/**
 * Exit code for a command line that routed but was refused.
 *
 * `runCli` answers the local tree's usage errors with 2, so the cloud half
 * answers the same mistake with the same code: an undeclared option or a
 * miscounted argument must not exit 2 on one branch of `agent-relay sessions`
 * and 1 on the other, or a script cannot tell a user's typo from a failure
 * the cloud reported.
 */
const EXIT_USAGE = 2;

/**
 * A cloud command line the surface refuses before it reaches the client.
 *
 * Distinct from an error the cloud itself reports, because only this class
 * leaves with `EXIT_USAGE`; a failed request keeps exiting 1.
 */
class CloudUsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CloudUsageError';
  }
}

/** Headings for the two-level groups, which have no row of their own in `COMMANDS`. */
const GROUP_DESCRIPTIONS: Record<string, string> = {
  cloud: 'Read session history from Relayhistory cloud.',
};

// ---------------------------------------------------------------------------
// Cloud commands
// ---------------------------------------------------------------------------

/** Options the cloud commands accept, in the same shape as the local ones. */
const CLOUD_FLAG_SPECS: Record<string, { flags: string; description: string; boolean?: true }> = {
  all: { flags: '--all', description: 'Page through every result, not just the first page.', boolean: true },
  cursor: { flags: '--cursor <cursor>', description: 'Continue from a printed cursor.' },
  date: { flags: '--date <yyyy-mm-dd>', description: 'Digest date. Defaults to today.' },
  json: { flags: '--json', description: 'Emit JSON instead of human-readable text.', boolean: true },
  kind: { flags: '--kind <kind>', description: 'Restrict to one event kind.' },
  kinds: { flags: '--kinds <kinds>', description: 'Event kinds to include, comma-separated.' },
  limit: { flags: '--limit <n>', description: 'Maximum rows per page.' },
  'max-content': { flags: '--max-content <n>', description: 'Truncate event content to this many characters.' },
  'missing-after-seconds': { flags: '--missing-after-seconds <n>', description: 'Treat a machine silent this long as missing.' },
  order: { flags: '--order <order>', description: 'Event order: asc or desc.' },
  project: { flags: '--project <path>', description: 'Only sessions from this project directory.' },
  refresh: { flags: '--refresh', description: 'Recompute the digest instead of serving a cached one.', boolean: true },
  session: { flags: '--session <id>', description: 'Restrict the search to one session.' },
  since: { flags: '--since <timestamp>', description: 'Only entries at or after this time.' },
  source: { flags: '--source <source>', description: 'Restrict to one coding-agent source.' },
  'stale-after-seconds': { flags: '--stale-after-seconds <n>', description: 'Treat a machine silent this long as stale.' },
  tag: { flags: '--tag <tag>', description: 'Restrict to entries carrying this tag.' },
  tz: { flags: '--tz <tz>', description: 'IANA timezone for the digest day.' },
  until: { flags: '--until <timestamp>', description: 'Only entries at or before this time.' },
  'window-hours': { flags: '--window-hours <n>', description: 'Coverage window, in hours.' },
};

/** Flags every recall listing shares. */
const FILTER_FLAGS = ['project', 'source', 'kind', 'tag', 'since', 'until'] as const;
const PAGE_FLAGS = ['limit', 'cursor', 'all'] as const;

interface CloudArgs {
  positional: readonly string[];
  flags: ReadonlyMap<string, string | true>;
}

interface CloudCommandSpec {
  /** Path under the mounted group. Always two tokens: `cloud <verb>`. */
  path: readonly [string, string];
  description: string;
  args?: readonly CommandArgSpec[];
  options: readonly string[];
  run(client: RelayhistoryCloudClient, args: CloudArgs, io: CliIo): Promise<number>;
}

function text(args: CloudArgs, name: string): string | undefined {
  const value = args.flags.get(name);
  return typeof value === 'string' ? value : undefined;
}

function count(args: CloudArgs, name: string): number | undefined {
  const value = text(args, name);
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`--${name} must be a number`);
  return parsed;
}

function filters(args: CloudArgs): Record<string, string | undefined> {
  return {
    project: text(args, 'project'), source: text(args, 'source'), kind: text(args, 'kind'),
    tag: text(args, 'tag'), since: text(args, 'since'), until: text(args, 'until'),
  };
}

function paging(args: CloudArgs): { limit?: number; cursor?: string } {
  return { limit: count(args, 'limit'), cursor: text(args, 'cursor') };
}

/**
 * Print a page, or every page when `--all` was given.
 *
 * A single page is a prefix of the answer, so anything that reads as "the whole
 * session" has to walk the cursor. `--all` is how a caller asks for that
 * explicitly rather than being silently handed the first 200 rows.
 */
async function emit<TPage extends { readonly nextCursor: string | null }>(
  io: CliIo,
  args: CloudArgs,
  page: () => Promise<TPage>,
  rowsOf: (page: TPage) => readonly unknown[],
  rows: () => AsyncIterable<unknown>,
): Promise<number> {
  const json = args.flags.has('json');
  if (args.flags.has('all')) {
    let seen = 0;
    for await (const row of rows()) {
      seen += 1;
      if (json) output(io, row, true);
      else io.stdout(`${humanLine(row)}\n`);
    }
    if (!json && seen === 0) io.stdout('No results.\n');
    return 0;
  }
  const result = await page();
  if (json) {
    output(io, result, true);
    return 0;
  }
  const list = rowsOf(result);
  io.stdout(list.length ? `${list.map(humanLine).join('\n')}\n` : 'No results.\n');
  if (result.nextCursor) io.stdout(`more available: --cursor '${result.nextCursor}'\n`);
  return 0;
}

const CLOUD_COMMANDS: readonly CloudCommandSpec[] = [
  {
    path: ['cloud', 'list'],
    description: 'List sessions held in Relayhistory cloud.',
    options: [...FILTER_FLAGS, ...PAGE_FLAGS, 'json'],
    run: (client, args, io) => {
      const query = { ...filters(args), ...paging(args) };
      return emit(io, args, async () => client.listSessions(query),
        (result) => result.sessions, () => client.iterateSessions(query));
    },
  },
  {
    path: ['cloud', 'events'],
    description: 'Page through one cloud session\'s events.',
    args: [{ name: 'session-id', description: 'Session identifier.', required: true }],
    options: [...PAGE_FLAGS, 'order', 'max-content', 'json'],
    run: (client, args, io) => {
      const id = args.positional[0]!;
      const query = { ...paging(args), order: text(args, 'order'), maxContent: count(args, 'max-content') };
      return emit(io, args, async () => client.getSessionEvents(id, query),
        (result) => result.events, () => client.iterateSessionEvents(id, query));
    },
  },
  {
    path: ['cloud', 'search'],
    description: 'Search events across cloud history.',
    args: [{ name: 'query', description: 'Search terms.', required: true, variadic: true }],
    options: [...FILTER_FLAGS, ...PAGE_FLAGS, 'session', 'order', 'max-content', 'json'],
    run: (client, args, io) => {
      const query = {
        ...filters(args), ...paging(args), q: args.positional.join(' '),
        session: text(args, 'session'), order: text(args, 'order'), maxContent: count(args, 'max-content'),
      };
      return emit(io, args, async () => client.searchEvents(query),
        (result) => result.events, () => client.iterateEvents(query));
    },
  },
  {
    path: ['cloud', 'thread'],
    description: 'Show a cloud session\'s linked thread.',
    args: [{ name: 'session-id', description: 'Session identifier.', required: true }],
    options: ['source', 'since', 'kinds', 'limit', 'cursor', 'json'],
    run: async (client, args, io) => {
      const source = text(args, 'source');
      if (!source) throw new CloudUsageError('cloud thread requires --source');
      // The flag is comma-separated for the command line; the client takes a list.
      const kinds = text(args, 'kinds')?.split(',').map((kind) => kind.trim()).filter(Boolean);
      output(io, await client.getSessionThread(args.positional[0]!, {
        source, since: text(args, 'since'), kinds, ...paging(args),
      }), args.flags.has('json'));
      return 0;
    },
  },
  {
    path: ['cloud', 'turns'],
    description: 'Show a cloud session\'s conversation turns.',
    args: [{ name: 'session-id', description: 'Session identifier.', required: true }],
    options: ['json'],
    run: async (client, args, io) => {
      const id = args.positional[0]!;
      const [page, metadata] = await Promise.all([client.listTurns(id), client.getSessionMetadata(id)]);
      output(io, { ...page, metadata }, args.flags.has('json'));
      return 0;
    },
  },
  {
    path: ['cloud', 'digest'],
    description: 'Show the daily activity digest.',
    options: ['date', 'tz', 'project', 'refresh', 'json'],
    run: async (client, args, io) => {
      output(io, await client.getDailyDigest({
        date: text(args, 'date'), tz: text(args, 'tz'), project: text(args, 'project'),
        refresh: args.flags.has('refresh') || undefined,
      }), args.flags.has('json'));
      return 0;
    },
  },
  {
    path: ['cloud', 'coverage'],
    description: 'Report which machines are reporting history.',
    options: ['stale-after-seconds', 'missing-after-seconds', 'window-hours', 'limit', 'json'],
    run: async (client, args, io) => {
      output(io, await client.listMachines({
        staleAfterSeconds: count(args, 'stale-after-seconds'),
        missingAfterSeconds: count(args, 'missing-after-seconds'),
        windowHours: count(args, 'window-hours'),
        limit: count(args, 'limit'),
      }), args.flags.has('json'));
      return 0;
    },
  },
];

/**
 * Parse a cloud command's argv against its declared options and arguments.
 *
 * Declared-only in both directions: an option this command does not list is an
 * error rather than a silently ignored token, and so is an argument past the
 * last one it declares. `runCli` rejects both for the local tree, and the two
 * halves of `agent-relay sessions` have to answer a mistyped line the same
 * way — silence on one of them is how a typo becomes an empty result set the
 * user believes.
 */
function parseCloud(argv: readonly string[], spec: CloudCommandSpec): CloudArgs {
  const positional: string[] = [];
  const flags = new Map<string, string | true>();
  const allowed = new Set(spec.options);
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index]!;
    if (!token.startsWith('--')) { positional.push(token); continue; }
    const [name, inline] = token.slice(2).split('=', 2);
    if (!allowed.has(name!)) throw new CloudUsageError(`${spec.path.join(' ')} does not accept --${name}`);
    if (CLOUD_FLAG_SPECS[name!]?.boolean) {
      if (inline !== undefined) throw new CloudUsageError(`--${name} does not take a value`);
      flags.set(name!, true);
      continue;
    }
    if (inline !== undefined) { flags.set(name!, inline); continue; }
    const next = argv[index + 1];
    if (next === undefined || next.startsWith('-')) throw new CloudUsageError(`--${name} requires a value`);
    flags.set(name!, next);
    index += 1;
  }
  const declared = spec.args ?? [];
  const required = declared.filter((arg) => arg.required).length;
  if (positional.length < required) {
    throw new CloudUsageError(`${spec.path.join(' ')} requires ${declared.map((arg) => arg.name).join(' and ')}`);
  }
  // A variadic last argument is the only unbounded arity; everything else takes
  // exactly what it declares, in the wording `rejectSurplusPositionals` uses.
  const most = declared.some((arg) => arg.variadic) ? null : declared.length;
  if (most !== null && positional.length > most) {
    throw new CloudUsageError(`${spec.path.join(' ')} does not accept positional argument '${positional[most]}'`);
  }
  return { positional, flags };
}

// ---------------------------------------------------------------------------
// The declared tree, built from the same tables `run` dispatches on
// ---------------------------------------------------------------------------

function toArgSpecs(args: readonly CommandArgSpec[] | undefined): RelayCliCommandSpec['args'] {
  if (!args?.length) return undefined;
  return args.map((arg) => ({
    name: arg.name, description: arg.description, required: arg.required,
    ...(arg.variadic ? { variadic: true } : {}),
  }));
}

function localOptions(spec: CommandSpec): readonly RelayCliOptionSpec[] {
  return spec.allowed
    .filter((name) => !HOST_OWNED_FLAGS.has(name))
    .map((name) => {
      const flag = FLAG_SPECS[name];
      // The drift test asserts this cannot happen; throwing rather than
      // emitting a blank option keeps a missing entry from reaching help.
      if (!flag) throw new Error(`${spec.name} allows --${name}, which FLAG_SPECS does not describe`);
      return { flags: flag.flags, description: flag.description };
    });
}

function cloudOptions(spec: CloudCommandSpec): readonly RelayCliOptionSpec[] {
  return spec.options.map((name) => {
    const flag = CLOUD_FLAG_SPECS[name];
    if (!flag) throw new Error(`${spec.path.join(' ')} allows --${name}, which CLOUD_FLAG_SPECS does not describe`);
    return { flags: flag.flags, description: flag.description };
  });
}

/** One leaf, ready to be placed at its path in the tree. */
interface Leaf {
  path: readonly string[];
  command: RelayCliCommandSpec;
}

function localLeaves(): Leaf[] {
  const leaves: Leaf[] = [];
  for (const spec of COMMANDS.values()) {
    if (!spec.surface) continue;
    leaves.push({
      path: spec.surface,
      command: {
        name: spec.surface[spec.surface.length - 1]!,
        description: spec.description,
        ...(spec.surfaceAliases ? { aliases: spec.surfaceAliases } : {}),
        ...(toArgSpecs(spec.args) ? { args: toArgSpecs(spec.args) } : {}),
        options: localOptions(spec),
      },
    });
  }
  return leaves;
}

function cloudLeaves(): Leaf[] {
  return CLOUD_COMMANDS.map((spec) => ({
    path: spec.path,
    command: {
      name: spec.path[1],
      description: spec.description,
      ...(toArgSpecs(spec.args) ? { args: toArgSpecs(spec.args) } : {}),
      options: cloudOptions(spec),
    },
  }));
}

/** Assemble leaves into the declared tree, creating group nodes as needed. */
function buildTree(leaves: readonly Leaf[]): RelayCliCommandSpec[] {
  const roots: RelayCliCommandSpec[] = [];
  const groups = new Map<string, RelayCliCommandSpec & { subcommands: RelayCliCommandSpec[] }>();
  for (const leaf of leaves) {
    if (leaf.path.length === 1) { roots.push(leaf.command); continue; }
    const [groupName] = leaf.path;
    let group = groups.get(groupName!);
    if (!group) {
      const description = GROUP_DESCRIPTIONS[groupName!];
      if (!description) throw new Error(`group '${groupName}' has no description`);
      group = { name: groupName!, description, subcommands: [] };
      groups.set(groupName!, group);
      roots.push(group);
    }
    group.subcommands.push(leaf.command);
  }
  return roots;
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

type Route =
  | { kind: 'local'; words: readonly string[] }
  | { kind: 'cloud'; spec: CloudCommandSpec };

/**
 * Surface path to what runs it.
 *
 * The mounted spelling and `ai-hist`'s own argv are not the same — `list` here
 * is `ai-hist sessions list` — so the table carries the translation rather than
 * leaving it implied.
 */
function buildRoutes(): Map<string, Route> {
  const routes = new Map<string, Route>();
  for (const [key, spec] of COMMANDS) {
    if (!spec.surface) continue;
    const words = key.split(' ');
    routes.set(spec.surface.join(' '), { kind: 'local', words });
    for (const alias of spec.surfaceAliases ?? []) {
      routes.set([...spec.surface.slice(0, -1), alias].join(' '), { kind: 'local', words });
    }
  }
  for (const spec of CLOUD_COMMANDS) routes.set(spec.path.join(' '), { kind: 'cloud', spec });
  return routes;
}

/** Longest-first match on the leading tokens; `null` when nothing routes. */
function resolve(routes: ReadonlyMap<string, Route>, argv: readonly string[]): { route: Route; rest: readonly string[] } | null {
  for (const depth of [2, 1]) {
    if (argv.length < depth) continue;
    const route = routes.get(argv.slice(0, depth).join(' '));
    if (route) return { route, rest: argv.slice(depth) };
  }
  return null;
}

/**
 * A `Writable` that hands each chunk to the host's sink.
 *
 * `export` with no `--out` writes NDJSON to standard output, which under a
 * mount means the host's `stdout`. It needs a real stream rather than the
 * `io.stdout` callback because the export is unbounded, and only a stream
 * applies backpressure to a producer that would otherwise outrun the sink.
 *
 * Chunks go through as the bytes Node hands us. `RelayCliIo` accepts
 * `Uint8Array` precisely so streamed output survives the mount, and decoding
 * each chunk to a string here would corrupt any multi-byte sequence that
 * straddles a chunk boundary.
 */
function hostStdoutStream(hostIo: RelayCliIo): Writable {
  return new Writable({
    write(chunk: Uint8Array, _encoding, callback): void {
      // A sink that throws must fail the export, not become an uncaught
      // exception: reporting it through the callback makes it the write's
      // rejection, which `runCli` turns into a nonzero exit.
      try {
        hostIo.stdout(chunk);
        callback();
      } catch (error: unknown) {
        callback(error as Error);
      }
    },
  });
}

export interface RelayhistorySurfaceOptions {
  /**
   * A `@relayhistory/cloud-client`. Supply it and the `cloud` subcommands are
   * declared and routable; leave it out and they are declared nowhere, though
   * `run` still recognises them well enough to name the remedy.
   */
  cloud?: RelayhistoryCloudClient;
  /**
   * Cancels a mounted command whose spec is `cancellable`.
   *
   * The contract forbids a surface from installing signal handlers, because
   * the process's signals belong to the host. It also gives `run` no way to
   * carry cancellation per invocation, so the host hands its own signal in
   * here when it builds the surface, exactly as it does its cloud client.
   * Nothing below installs a handler; this is the host's signal, passed
   * through.
   */
  signal?: AbortSignal;
}

/**
 * Build the CLI surface a host mounts as `agent-relay sessions`.
 *
 * @param options - Optional cloud client to compose into the tree, and the
 *   host's cancellation signal for the long-running commands.
 * @returns A surface whose `commands` and `run` are both derived from the
 *   command tables, so neither can describe a command the other does not.
 */
export function createRelayCliSurface(options: RelayhistorySurfaceOptions = {}): RelayCliSurface {
  const cloud = options.cloud;
  const routes = buildRoutes();
  // Help must not advertise what cannot run, so an absent client removes the
  // cloud commands from the declared tree. `run` still knows them, and answers
  // with the remedy rather than "unknown command".
  const commands = buildTree([...localLeaves(), ...(cloud ? cloudLeaves() : [])]);

  return {
    id: 'relayhistory',
    version: packageVersion,
    contract: 1,
    commands,

    async run(argv: readonly string[], hostIo: RelayCliIo): Promise<number> {
      const io: CliIo = { stdout: (chunk) => hostIo.stdout(chunk), stderr: (chunk) => hostIo.stderr(chunk) };
      const resolved = resolve(routes, argv);
      if (!resolved) {
        io.stderr(`ai-hist: unknown command '${argv.join(' ') || '(none)'}'\n`);
        return EXIT_UNKNOWN_COMMAND;
      }
      if (resolved.route.kind === 'local') {
        // The bin's own options stay with the bin: no registry check, and
        // colour is the host's to decide. What a mounted command genuinely
        // needs does cross: somewhere to stream `export` to when no `--out`
        // was given, and the host's cancellation signal. Without both, a
        // command that works as `ai-hist <cmd>` fails as
        // `agent-relay sessions <cmd>`.
        return runCli([...resolved.route.words, ...resolved.rest], io, {
          stdoutStream: hostStdoutStream(hostIo),
          signal: options.signal,
        });
      }
      const spec = resolved.route.spec;
      if (!cloud) {
        io.stderr(`ai-hist: ${spec.path.join(' ')} requires cloud access: run \`agent-relay login\`\n`);
        return EXIT_UNKNOWN_COMMAND;
      }
      try {
        return await spec.run(cloud, parseCloud(resolved.rest, spec), io);
      } catch (error: unknown) {
        // A refused command line is the same mistake whichever half of the tree
        // it was typed against, so it leaves with the code `runCli` gives the
        // local half rather than the one a failed request would.
        if (error instanceof CloudUsageError) {
          io.stderr(`ai-hist: ${error.message}\n`);
          return EXIT_USAGE;
        }
        const status = cloudErrorStatus(error);
        if (status === 401 || status === 403) {
          io.stderr('ai-hist: cloud access was refused: run `agent-relay login`\n');
          return EXIT_UNKNOWN_COMMAND;
        }
        const value = error as { code?: string; message?: string };
        io.stderr(`ai-hist: ${value.code ? `${value.code}: ` : ''}${value.message ?? String(error)}\n`);
        return 1;
      }
    },
  };
}

/** Everything the drift test needs to walk the dispatch side. */
export const __testing = { CLOUD_COMMANDS, CLOUD_FLAG_SPECS, buildRoutes, resolve };
