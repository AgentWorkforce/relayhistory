// Source-native recorder: no model, auth, CLI process or network.
const fs = require('node:fs');
const path = require('node:path');
const [bundle, output] = process.argv.slice(2);
if (process.env.CODEBUDDY_FIXTURE_CAPTURE !== '1') throw new Error('Run through capture.py with its isolated environment');
const api = require(bundle);
(async () => {
  for (const name of ['fork', 'pending-rewind', 'compact', 'reasoning', 'fragments', 'metadata', 'string-content']) {
    const storageRoot = path.join(output, name, '.codebuddy', 'projects');
    const key = { projectKey: 'fixture-workspace', sessionId: `fixture-${name}` };
    const store = new api.LocalJsonlSessionStore({ storageRoot, ...(name==='fragments'?{assignParentIds:false,absorbRedeliveries:false}:{}) });
    const entry = (id, role, text, extra = {}) => ({ type: 'message', uuid: id, payload: {
      type: 'message', id, timestamp: 1791720000000 + Number(id.slice(-1)), role,
      content: [{ type: role === 'user' ? 'input_text' : 'output_text', text }],
      sessionId: key.sessionId, cwd: '/fixture/workspace', ...extra,
    } });
    await store.create(key);
    await store.append(key, [entry('u1', 'user', 'Synthetic original prompt.'), entry('a2', 'assistant', 'Synthetic old branch answer.', {parentId:'u1'})]);
    if (name === 'reasoning') {
      await store.append(key,[{type:'reasoning',uuid:'r3',payload:{type:'reasoning',id:'r3',timestamp:1791720000003,summary:[{type:'summary_text',text:'Synthetic reasoning kept opaque.'}],sessionId:key.sessionId,cwd:'/fixture/workspace'}},entry('u4','user','Synthetic internal notice.',{providerData:{skipRun:true,isMeta:true}}),entry('a5','assistant','Synthetic answer after reasoning.')]);
    } else if(name === 'string-content') {
      await store.append(key,[entry('u3','user','',{content:'Discuss <cb_summary_extra> and <teammate-messageboard> literally.'}),entry('a4','assistant','',{content:'Synthetic string-content answer.'}),entry('u5','user','',{content:'<cb_summary>Actual synthetic control summary.</cb_summary>'}),entry('u6','user','',{content:'Explicit native real user <cb_summary> literal.',providerData:{startsNewUserRequest:true,isMeta:true}})]);
    } else if(name === 'fragments') {
      await store.append(key,[entry('a3','assistant','Synthetic first fragment.',{parentId:'a2'}),entry('a3','assistant','Synthetic second fragment.',{parentId:'a2'})]);
    } else if(name === 'metadata') {
      const facts=[{type:'custom-title',id:'title1',timestamp:1791720000010,customTitle:'Synthetic chosen title.'},{type:'ai-title',id:'title2',timestamp:1791720000020,aiTitle:'Synthetic later AI title.'}];
      const nativeMeta=JSON.parse(new api.SessionStoreImpl().metaSnapshotLine(key.sessionId,{'codebuddy.ai/sessionTitle':[{type:'topic',id:'topic1',timestamp:1791720000005,topic:'Synthetic metadata topic.'}]}));
      nativeMeta.id='meta1';nativeMeta.timestamp=1791720000030;
      await store.append(key,[...facts.map(payload=>({type:payload.type,uuid:payload.id,payload})),{type:nativeMeta.type,uuid:nativeMeta.id,payload:nativeMeta}]);
    } else if (name === 'compact') {
      await store.append(key, [entry('s3', 'user', 'Synthetic compacted summary.', { parentId: null, logicalParentId: 'a2', providerData: { agent: 'compact', isCompacted: true, isSummary: true } }), entry('u4', 'user', 'Synthetic post-compaction prompt.'), entry('a5', 'assistant', 'Synthetic post-compaction answer.')]);
    } else {
      const notice = api.InputItemUtils.createResendForkNoticeItem('u1');
      notice.id = 'notice-3'; notice.timestamp = 1791720000003;
      notice.sessionId = key.sessionId; notice.cwd = '/fixture/workspace';
      await store.appendFactAndMoveLane(key, [{ type: notice.type, uuid: notice.id, payload: notice }], 'main', 'u1');
      if (name === 'fork') await store.append(key, [entry('a4', 'assistant', 'Synthetic replacement branch answer.')]);
    }
    // Low-level loadLane is context, not complete historical display. Keep it
    // separately and use the high-level native historical replay as oracle.
    const reader = new api.LocalJsonlSessionStore({ storageRoot });
    const lane = await reader.loadLane(key, 'main');
    const historical = new api.SessionStoreImpl();
    historical.logger = {debug(){},info(){},warn(){},error(...x){console.error(...x)}};
    historical.sessionFactory = {async create(value){return value}};
    historical.restoreHost = {restore(){}};
    const session = await historical.deserializeSessionFromPath(store.filePathFor(key), {historyLimit:100000});
    if(!session) throw new Error('Native historical restore failed');
    const replay = api.HistoryUtils.resolveReplayHistory(session.history, {hasCompactedHistory:session.hasCompactedHistory,lastMessageId:session.lastMessageId,keepCustomItems:false});
    fs.writeFileSync(path.join(output,name,'active-reader.json'), JSON.stringify({session,replay,lane,titleFacts:api.resolveSessionTitleFacts(session.history),nativeTitle:api.getEffectiveSessionTitle(session.history),realUserMessageIds:replay.filter(item=>api.InputItemUtils.isExternalUserPromptItem(item)).map(item=>item.id)},null,2)+'\n');
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
