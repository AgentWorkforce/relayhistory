import { createWriteStream } from 'node:fs';
import { readFile, realpath, rename, rm, stat } from 'node:fs/promises';
import { basename, dirname, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { finished } from 'node:stream/promises';
import type { Writable } from 'node:stream';
import {
  defaultDbPath, exportHistoryNdjson,
  loadHistoryPlugins, InvalidArgumentError,
  type HistoryExportSelection, type HistoryPluginModule,
} from './index.js';

export interface HistoryApplicationConfig { plugins: HistoryPluginModule[] }
export async function loadHistoryApplicationConfig(path: string) {
  const absolute = resolve(path);
  let config: HistoryApplicationConfig;
  try { config = JSON.parse(await readFile(absolute, 'utf8')) as HistoryApplicationConfig; }
  catch { throw new InvalidArgumentError('history config could not be read as JSON', 'INVALID_ARGUMENT'); }
  if (!config || !Array.isArray(config.plugins)) throw new InvalidArgumentError('history config requires an explicit plugins array', 'INVALID_ARGUMENT');
  return { config, registry: await loadHistoryPlugins(config.plugins, { baseDirectory: dirname(absolute) }) };
}

async function write(stream: Writable, chunk: string): Promise<void> {
  await new Promise<void>((resolve, reject) => stream.write(chunk, (error) => error ? reject(error) : resolve()));
}
export async function runHistoryExportCommand(
  options: { dbPath?: string; selectionPath: string; outputPath?: string },
  /** Destination when no `--out` is given. Required unless `outputPath` is set. */
  stdoutStream?: Writable,
): Promise<void> {
  const canonicalTarget = async (path: string): Promise<string> => {
    try { return await realpath(path); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      const parent = dirname(path);
      if (parent === path) throw error;
      return resolve(await canonicalTarget(parent), basename(path));
    }
  };
  const assertSafeOutput = async () => {
    if (!options.outputPath) return;
    const target = resolve(options.outputPath);
    const database = resolve(options.dbPath ?? defaultDbPath());
    const metadata = async (path: string) => stat(path).catch((error: NodeJS.ErrnoException) => {
      if (error.code === 'ENOENT') return null;
      throw error;
    });
    const [targetPath, databasePath, targetStat, databaseStat] = await Promise.all([
      canonicalTarget(target), canonicalTarget(database), metadata(target), metadata(database),
    ]);
    if (targetPath === databasePath || (targetStat && databaseStat && targetStat.dev === databaseStat.dev && targetStat.ino === databaseStat.ino)) {
      throw new InvalidArgumentError('export output must not replace the active history database', 'INVALID_ARGUMENT');
    }
  };
  await assertSafeOutput();
  const selection = JSON.parse(await readFile(options.selectionPath, 'utf8')) as HistoryExportSelection;
  const temporary = options.outputPath ? `${resolve(options.outputPath)}.${randomUUID()}.tmp` : undefined;
  if (!temporary && !stdoutStream) {
    throw new InvalidArgumentError('export without --out requires a destination stream', 'INVALID_ARGUMENT');
  }
  const stream: Writable = temporary ? createWriteStream(temporary, { flags: 'wx', mode: 0o600 }) : stdoutStream!;
  // Stream errors (e.g. a closed pipe) must exit nonzero, not become uncaught
  // events or a false claim that the full export completed.
  let streamError: Error | undefined;
  const errorListener = (error: Error) => { streamError = error; };
  stream.on('error', errorListener);
  try {
    for await (const chunk of exportHistoryNdjson(selection, options)) {
      if (streamError) throw streamError;
      await write(stream, chunk);
    }
    if (temporary) {
      stream.end();
      await finished(stream);
      await assertSafeOutput();
      await rename(temporary, resolve(options.outputPath!));
    }
    if (streamError) throw streamError;
  } finally {
    if (temporary) { stream.destroy(); await rm(temporary, { force: true }); }
    stream.removeListener('error', errorListener);
  }
}
