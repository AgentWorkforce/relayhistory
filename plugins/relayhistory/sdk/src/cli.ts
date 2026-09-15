#!/usr/bin/env node
/** Optional compatibility CLI. Local history commands remain in ai-hist. */
import { accessToken, enableCloud, login, replay } from './cloud-client.js';
import { createHistoryPlugin } from './plugin.js';
async function main() {
  const [command,...args]=process.argv.slice(2).filter(arg=>arg!=='--no-warning');
  const flags=new Map<string,string|true>(); const positionals:string[]=[];
  const booleans=new Set(['json','once','help']);
  for(let index=0;index<args.length;index++) {
    const value=args[index];
    if(value==='-h') {flags.set('help',true);continue;}
    if(!value.startsWith('--')) {positionals.push(value);continue;}
    const [name,inline]=value.slice(2).split(/=(.*)/s);
    if(!['json','once','help','base-url','token','label','db','interval','limit','max-content','out'].includes(name))throw new Error(`Unknown option --${name}`);
    if(booleans.has(name)){flags.set(name,true);continue;}
    const text=inline??args[++index];if(text===undefined)throw new Error(`--${name} requires a value`);flags.set(name,text);
  }
  const text=(name:string)=>typeof flags.get(name)==='string'?flags.get(name) as string:undefined;
  const number=(name:string)=>{const value=text(name);if(value===undefined)return undefined;const n=Number(value);if(!Number.isSafeInteger(n)||n<0)throw new Error(`--${name} must be a non-negative integer`);return n;};
  if(flags.has('help')||!command){process.stdout.write('relayhistory-plugin login|token|replay|enable-cloud [options]\nNew reliable jobs: ai-hist plugin relayhistory-enable --config FILE -- --selection FILE\n');return;}
  const baseUrl=text('base-url');
  if(command==='token'){if([...flags.keys()].some(key=>key!=='base-url'))throw new Error('token accepts only --base-url');if(positionals.length)throw new Error('token takes no positional arguments');process.stdout.write(`${await accessToken({baseUrl})}\n`);return;}
  if(command==='replay'){
    if(positionals.length!==1)throw new Error('replay requires SESSION_ID');
    const result=await replay(positionals[0],{baseUrl,limit:number('limit'),maxContent:number('max-content'),json:flags.has('json'),out:text('out')});
    if(result.transcript!==null)process.stdout.write(result.transcript);return;
  }
  if(command==='login'||command==='enable-cloud'){
    if(positionals.length)throw new Error(`${command} takes no positional arguments`);
    if(command==='login'&&text('token')&&!baseUrl)throw new Error('login requires --base-url with --token');
    // Agent Relay Cloud sign-in, its credential precedence and its destination
    // gate all run in the Rust helper; this only reports whether a terminal is
    // attached, so a browser approval is never started where nobody sees it.
    const token=text('token');
    if(command==='login'){const auth=await login({baseUrl,relayAccessToken:token,label:text('label'),...(token?{}:{interactive:process.stdin.isTTY===true})});process.stdout.write(flags.has('json')?JSON.stringify({ok:true,base_url:auth.baseUrl})+'\n':`Logged in to ${auth.baseUrl} (session stored).\n`);return;}
    const handle=await enableCloud({baseUrl,dbPath:text('db'),relayAccessToken:token,intervalMs:(number('interval')??60)*1000,watch:!flags.has('once'),onPush:result=>process.stdout.write(JSON.stringify({base_url:result.baseUrl,sent:result.sent,accepted:result.accepted,sync_skipped:result.syncSkipped})+'\n')});
    const {stop,...result}=handle;process.stdout.write(JSON.stringify({base_url:result.baseUrl,sent:result.sent,accepted:result.accepted,sync_skipped:result.syncSkipped})+'\n');process.once('SIGINT',()=>void stop());process.once('SIGTERM',()=>void stop());return;
  }
  const operation=createHistoryPlugin({baseUrl}).commands?.find(item=>item.name===command);
  if(!operation)throw new Error('Unknown RelayHistory plugin command');
  process.stdout.write(JSON.stringify(await operation.run(args))+'\n');
}
main().catch(error=>{process.stderr.write((error instanceof Error?error.message:'RelayHistory operation failed')+'\n');process.exitCode=2;});
