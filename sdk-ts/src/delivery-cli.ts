import { createWriteStream } from 'node:fs';
import { readFile, rename, rm, stat } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { finished } from 'node:stream/promises';
import type { Writable } from 'node:stream';
import {
  controlHistoryDelivery, createHistoryDelivery, defaultDbPath, drainHistoryDelivery, exportHistoryNdjson,
  historyDeliveryStatus, historyDeliveryRetention, loadHistoryPlugins, runHistoryDelivery, InvalidArgumentError,
  type DeliveryJobConfig, type HistoryExportSelection, type HistoryPluginModule,
} from './index.js';

export interface HistoryApplicationConfig { plugins: HistoryPluginModule[]; job?: DeliveryJobConfig }
export async function loadHistoryApplicationConfig(path: string) {
  const absolute = resolve(path);
  let config: HistoryApplicationConfig;
  try { config = JSON.parse(await readFile(absolute, 'utf8')) as HistoryApplicationConfig; }
  catch { throw new InvalidArgumentError('history config could not be read as JSON', 'INVALID_ARGUMENT'); }
  if (!config || !Array.isArray(config.plugins)) throw new InvalidArgumentError('history config requires an explicit plugins array', 'INVALID_ARGUMENT');
  return { config, registry: await loadHistoryPlugins(config.plugins, { baseDirectory: dirname(absolute) }) };
}

export async function runDeliveryCommand(action: string, options: {
  dbPath?: string; configPath?: string; jobId?: string; pollIntervalMs?: number; requestTimeoutMs?: number;
}): Promise<void> {
  const output = (value: unknown) => process.stdout.write(`${JSON.stringify(value)}\n`);
  if (action === 'status') { output({ jobs: await historyDeliveryStatus(options.jobId, options), retention: await historyDeliveryRetention(options) }); return; }
  if (['pause', 'resume', 'retry', 'cancel'].includes(action)) {
    if (!options.jobId) throw new InvalidArgumentError('delivery control requires a job ID', 'INVALID_ARGUMENT');
    output(await controlHistoryDelivery(options.jobId, action as 'pause' | 'resume' | 'retry' | 'cancel', options));
    return;
  }
  if (!options.configPath) throw new InvalidArgumentError('delivery enable/drain/run requires --config', 'INVALID_ARGUMENT');
  const { config, registry } = await loadHistoryApplicationConfig(options.configPath);
  if (action === 'enable') {
    if (!config.job) throw new InvalidArgumentError('delivery enable requires a job in the config', 'INVALID_ARGUMENT');
    const destination = registry.destination(config.job.destination_id, config.job.instance_id);
    if (!destination || destination.mappingVersion !== config.job.mapping_version) throw new InvalidArgumentError('job requires its configured destination and mapping version', 'INVALID_ARGUMENT');
    output(await createHistoryDelivery(config.job, options));
    return;
  }
  const abort = new AbortController();
  const stop = () => abort.abort();
  process.once('SIGINT', stop); process.once('SIGTERM', stop);
  const runOptions = { ...options, signal: abort.signal, jobIds: options.jobId ? [options.jobId] : undefined };
  try {
    if (action === 'drain') {
      const result = await drainHistoryDelivery(registry, runOptions);
      output(result);
      if (result.issues.length || result.statuses.some((job) => job.state === 'blocked' || job.failure)) process.exitCode = 1;
    } else if (action === 'run') {
      await runHistoryDelivery(registry, { ...runOptions, onProgress: (value) => {
        process.stderr.write(`${JSON.stringify(value)}\n`);
      } });
    } else throw new InvalidArgumentError('unknown delivery command', 'INVALID_ARGUMENT');
  } finally {
    process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop);
  }
}

async function write(stream: Writable, chunk: string): Promise<void> {
  await new Promise<void>((resolve, reject) => stream.write(chunk, (error) => error ? reject(error) : resolve()));
}
export async function runHistoryExportCommand(options: { dbPath?: string; selectionPath: string; outputPath?: string }): Promise<void> {
  if (options.outputPath) {
    const target = resolve(options.outputPath);
    const database = resolve(options.dbPath ?? defaultDbPath());
    const metadata = async (path: string) => stat(path).catch((error: NodeJS.ErrnoException) => {
      if (error.code === 'ENOENT') return null;
      throw error;
    });
    const [targetStat, databaseStat] = await Promise.all([metadata(target), metadata(database)]);
    if (target === database || (targetStat && databaseStat && targetStat.dev === databaseStat.dev && targetStat.ino === databaseStat.ino)) {
      throw new InvalidArgumentError('export output must not replace the active history database', 'INVALID_ARGUMENT');
    }
  }
  const selection = JSON.parse(await readFile(options.selectionPath, 'utf8')) as HistoryExportSelection;
  const temporary = options.outputPath ? `${resolve(options.outputPath)}.${randomUUID()}.tmp` : undefined;
  const stream = temporary ? createWriteStream(temporary, { flags: 'wx', mode: 0o600 }) : process.stdout;
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
      await rename(temporary, resolve(options.outputPath!));
    }
    if (streamError) throw streamError;
  } finally {
    if (temporary) { stream.destroy(); await rm(temporary, { force: true }); }
    stream.removeListener('error', errorListener);
  }
}
