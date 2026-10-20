// Fresh-process historical reader from the pinned official distribution.
// The capture script exposes webpack module 81102 by changing only its entry.
const [bundle, file] = process.argv.slice(2);
const api = require(bundle);
(async () => {
  const store = new api.SessionStoreImpl();
  store.logger = { debug() {}, info() {}, warn() {}, error(...args) { console.error(...args); } };
  store.sessionFactory = { async create(value) { return value; } };
  store.restoreHost = { restore() {} };
  const session = await store.deserializeSessionFromPath(file, { historyLimit: 100000 });
  if (!session) throw new Error('Official reader did not restore session');
  session.realUserMessageIds = session.history.filter(item => api.InputItemUtils.isExternalUserPromptItem(item)).map(item=>item.id);
  session.nativeTitle = api.getEffectiveSessionTitle(session.history);
  process.stdout.write(JSON.stringify(session, null, 2) + '\n');
})().catch(error => { console.error(error); process.exitCode = 1; });
