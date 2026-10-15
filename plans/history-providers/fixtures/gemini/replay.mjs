// Research-only journal model, independently checked against pinned official reader.
// Not registered with RelayHistory; production acquisition remains deferred.
import assert from 'node:assert/strict';
export function replay(text, {legacy = false, allowPartialTail = false} = {}) {
  if (legacy) return {conversation: JSON.parse(text), deferredTail: false};
  let metadata = {};
  let messages = new Map();
  let deferredTail = false;
  const lines = text.split('\n');
  for (const [index, line] of lines.entries()) {
    if (!line.trim()) continue;
    let record;
    try { record = JSON.parse(line); }
    catch (error) {
      if (allowPartialTail && index === lines.length - 1 && !text.endsWith('\n')) {
        deferredTail = true;
        break;
      }
      throw new Error(`Malformed complete journal record at line ${index + 1}`, {cause: error});
    }
    assert(record && typeof record === 'object' && !Array.isArray(record));
    if (typeof record.$rewindTo === 'string') {
      const ids = [...messages.keys()];
      const position = ids.indexOf(record.$rewindTo);
      if (position < 0) messages.clear();
      else for (const id of ids.slice(position)) messages.delete(id);
    } else if (record.$patch && typeof record.$patch === 'object') {
      const patch = record.$patch;
      for (const update of [...(patch.id ? [patch] : []), ...(patch.updates ?? [])]) {
        const message = messages.get(update.id);
        if (!message) continue;
        if ('content' in update && update.content !== undefined) message.content = update.content;
        for (const callPatch of update.toolCalls ?? []) {
          const call = message.toolCalls?.find(call => call.id === callPatch.id);
          if (call && 'result' in callPatch) call.result = callPatch.result;
        }
      }
      for (const id of patch.removeIds ?? []) messages.delete(id);
      if (patch.orderIds) {
        const ordered = patch.orderIds.filter(id => messages.has(id));
        const prefix = [...messages.keys()].filter(id => !ordered.includes(id));
        messages = new Map([...prefix, ...ordered].map(id => [id, messages.get(id)]));
      }
    } else if (typeof record.id === 'string') {
      assert(['user', 'gemini', 'info', 'error', 'warning'].includes(record.type), 'Unknown message type');
      assert(Number.isFinite(Date.parse(record.timestamp)), 'Invalid message timestamp');
      messages.set(record.id, record);
    } else if (record.$set && typeof record.$set === 'object') {
      const {messages: checkpoint, ...update} = record.$set;
      Object.assign(metadata, update);
      if (checkpoint) messages = new Map(checkpoint.map(message => [message.id, message]));
    } else if (typeof record.sessionId === 'string' && typeof record.projectHash === 'string') {
      const {messages: checkpoint, ...update} = record;
      Object.assign(metadata, update);
      if (checkpoint) messages = new Map(checkpoint.map(message => [message.id, message]));
    } else throw new Error('Unsupported journal opcode');
  }
  assert(typeof metadata.sessionId === 'string' && metadata.sessionId.length > 0, 'Missing full session identity');
  assert(typeof metadata.projectHash === 'string' && metadata.projectHash.length > 0, 'Missing project hash');
  return {conversation: {...metadata, messages: [...messages.values()]}, deferredTail};
}
export function durableProjection(conversation) {
  return Object.fromEntries(['sessionId', 'projectHash', 'startTime', 'lastUpdated', 'summary', 'memoryScratchpad', 'directories', 'kind', 'messages'].filter(key => conversation[key] !== undefined).map(key => [key, conversation[key]]));
}
