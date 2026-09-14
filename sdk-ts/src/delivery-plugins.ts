import type { HistorySource } from './source-contracts.js';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { createRequire } from 'node:module';
import { InvalidArgumentError, RelayHistoryError } from './sdk-common.js';
import type { HistoryDestination, HistoryPlugin } from './delivery-contracts.js';

const CORE_COMMANDS = ['sessions', 'search', 'recent', 'session', 'events', 'resume', 'pack', 'stats', 'sync', 'export', 'delivery', 'plugin'];
const CORE_TOOLS = ['search_history', 'recent_history', 'list_sessions', 'discover_sessions', 'hydrate_session', 'get_session', 'get_session_events', 'get_session_relationships', 'get_session_tree', 'get_session_tool_calls', 'get_session_file_edits', 'history_stats', 'sync', 'delivery_status', 'delivery_pause', 'delivery_resume', 'delivery_retry'];
function label(value: string): void {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_.:/@-]{1,200}$/.test(value)) {
    throw new InvalidArgumentError('plugin identifiers must be nonempty non-secret labels', 'INVALID_ARGUMENT');
  }
}

/** Per-client registry. Installing a module never registers or starts it. */
export class HistoryPluginRegistry {
  private readonly sources = new Map<string, HistorySource>();
  private readonly destinations = new Map<string, HistoryDestination>();
  private readonly commands = new Map<string, NonNullable<HistoryPlugin['commands']>[number]>();
  private readonly tools = new Map<string, NonNullable<HistoryPlugin['tools']>[number]>();

  register(plugin: HistoryPlugin): void {
    const sources = new Map(this.sources);
    const destinations = new Map(this.destinations);
    const commands = new Map(this.commands);
    const tools = new Map(this.tools);
    for(const source of plugin.sources ?? []) {
      label(source.id);label(source.instanceId);
      const key=JSON.stringify([source.id,source.instanceId]);
      if(sources.has(key)||source.location!=='remote'||!Array.isArray(source.supportedSources)||typeof source.discover!=='function'||typeof source.hydrate!=='function') throw new InvalidArgumentError('invalid or duplicate source connector','INVALID_ARGUMENT');
      sources.set(key,source);
    }
    // Validate the whole registration before mutating this registry.
    for (const { instanceId, destination } of plugin.destinations ?? []) {
      label(instanceId); label(destination.id); label(destination.mappingVersion);
      const key = JSON.stringify([destination.id, instanceId]);
      if (destinations.has(key)) throw new InvalidArgumentError('duplicate destination instance', 'INVALID_ARGUMENT');
      if (!['revision', 'none'].includes(destination.idempotency) || destination.orderedRevisions !== true
        || !Array.isArray(destination.supportedKinds) || typeof destination.supportsTombstones !== 'boolean'
        || typeof destination.prepare !== 'function' || typeof destination.send !== 'function') {
        throw new InvalidArgumentError('destination must declare delivery capabilities and handlers', 'INVALID_ARGUMENT');
      }
      destinations.set(key, destination);
    }
    for (const command of plugin.commands ?? []) {
      label(command.name);
      if (CORE_COMMANDS.includes(command.name) || commands.has(command.name)) throw new InvalidArgumentError(`duplicate command: ${command.name}`, 'INVALID_ARGUMENT');
      commands.set(command.name, { ...command, run: async (args) => {
        try { return await command.run(args); }
        catch { throw new RelayHistoryError(`plugin command ${command.name} failed`, 'HISTORY_PLUGIN_COMMAND_FAILED'); }
      } });
    }
    for (const tool of plugin.tools ?? []) {
      label(tool.name);
      if (CORE_TOOLS.includes(tool.name) || tools.has(tool.name)) throw new InvalidArgumentError(`duplicate tool: ${tool.name}`, 'INVALID_ARGUMENT');
      tools.set(tool.name, { ...tool, run: async (input) => {
        try { return await tool.run(input); }
        catch { throw new RelayHistoryError(`plugin tool ${tool.name} failed`, 'HISTORY_PLUGIN_TOOL_FAILED'); }
      } });
    }
    for (const [key,value] of sources) this.sources.set(key,value);
    for (const [key, value] of destinations) this.destinations.set(key, value);
    for (const [key, value] of commands) this.commands.set(key, value);
    for (const [key, value] of tools) this.tools.set(key, value);
  }

  sourceConnectors(ids?: readonly string[]): HistorySource[] {
    const sources=[...this.sources.values()];
    if(ids===undefined)return sources;
    if(!Array.isArray(ids)||ids.some(id=>typeof id!=='string'||!id||id.trim()!==id)||new Set(ids).size!==ids.length)throw new InvalidArgumentError('sourceConnectors must contain unique nonempty connector IDs','INVALID_ARGUMENT');
    for(const id of ids)if(!sources.some(source=>source.id===id||`${source.id}:${source.instanceId}`===id))throw new InvalidArgumentError(`unconfigured source connector: ${id}`,'INVALID_ARGUMENT');
    return sources.filter(source=>ids.includes(source.id)||ids.includes(`${source.id}:${source.instanceId}`));
  }
  destination(id: string, instanceId: string): HistoryDestination | undefined {
    return this.destinations.get(JSON.stringify([id, instanceId]));
  }
  command(name: string) { return this.commands.get(name); }
  registeredTools() { return [...this.tools.values()]; }
}

export interface HistoryPluginModule { module: string; options?: Record<string, unknown> }
/** Resolve only user-listed installed packages/local modules from the config
 * directory. Plugin factories must be inert: loading is not upload consent. */
export async function loadHistoryPlugins(modules: readonly HistoryPluginModule[], options: { baseDirectory?: string } = {}): Promise<HistoryPluginRegistry> {
  const registry = new HistoryPluginRegistry();
  const directory = resolve(options.baseDirectory ?? process.cwd());
  const require = createRequire(pathToFileURL(resolve(directory, 'package.json')));
  for (const [index, entry] of modules.entries()) {
    if (typeof entry.module !== 'string' || !entry.module || /^(https?:|data:|node:)/.test(entry.module)) {
      throw new InvalidArgumentError('plugin must name an installed package or local module', 'INVALID_ARGUMENT');
    }
    try {
      const path = require.resolve(entry.module.startsWith('.') ? resolve(directory, entry.module) : entry.module);
      const imported = await import(pathToFileURL(path).href) as { createHistoryPlugin?: (options: Record<string, unknown>) => HistoryPlugin | Promise<HistoryPlugin> };
      if (typeof imported.createHistoryPlugin !== 'function') throw new Error('plugin factory missing');
      registry.register(await imported.createHistoryPlugin(entry.options ?? {}));
    } catch (error) {
      if (error instanceof InvalidArgumentError) throw error;
      // Plugin errors can contain auth headers; do not expose arbitrary text.
      throw new RelayHistoryError(`configured history plugin ${index + 1} could not be loaded`, 'HISTORY_PLUGIN_LOAD_FAILED');
    }
  }
  return registry;
}
