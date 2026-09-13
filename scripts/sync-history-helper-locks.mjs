/** npm cannot resolve unpublished optional platform releases while preparing the
 * split. Record their declared registry coordinates and platform constraints.
 * No integrity hash is invented; installation resolves the published artifact.
 * Ordinary CI installs retain optional build tools such as esbuild. Missing
 * unpublished helper artifacts remain optional; CI builds those helpers explicitly.
 */
import { readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
const root=fileURLToPath(new URL('../',import.meta.url));
import { platforms, validatePluginManifest } from './history-package-contract.mjs';
for(const plugin of ['relayhistory','provider-sources']){
  const directory=resolve(root,'plugins',plugin,'sdk');const manifest=JSON.parse(await readFile(resolve(directory,'package.json'),'utf8'));const path=resolve(directory,'package-lock.json');const lock=JSON.parse(await readFile(path,'utf8'));validatePluginManifest(plugin,manifest);
  for(const [name,version] of Object.entries(manifest.optionalDependencies??{})){
    const platform=Object.keys(platforms).find(platform=>name.endsWith('-'+platform));if(!platform)throw new Error('Unexpected helper platform');
    const [os,cpu,libc]=platforms[platform];const basename=name.split('/').at(-1);
    lock.packages['node_modules/'+name]={version,resolved:`https://registry.npmjs.org/${name}/-/${basename}-${version}.tgz`,optional:true,os:[os],cpu:[cpu],...(libc?{libc:[libc]}:{}),license:'MIT'};
  }
  await writeFile(path,JSON.stringify(lock,null,2)+'\n');
}
