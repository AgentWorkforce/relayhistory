import { createRequire } from 'node:module';
import { resolve } from 'node:path';

import { nativeContractVersion } from './history-package-contract.mjs';

const rustVersion = nativeContractVersion('rust');
const sdkVersion = nativeContractVersion('sdk');

const versions = [
  ['Rust binding source', rustVersion],
  ['TypeScript SDK source', sdkVersion],
];

const addonPath = process.argv[2];
if (addonPath) {
  const require = createRequire(import.meta.url);
  const addon = require(resolve(process.cwd(), addonPath));
  if (typeof addon.nativeContractVersion !== 'function') {
    throw new Error(`Built addon at ${addonPath} does not export nativeContractVersion()`);
  }
  versions.push(['built native addon', addon.nativeContractVersion()]);
}

if (new Set(versions.map(([, version]) => version)).size !== 1) {
  throw new Error(
    `Native contract versions disagree: ${versions
      .map(([label, version]) => `${label}=${version}`)
      .join(', ')}`,
  );
}

console.log(`Native contract version ${rustVersion} verified (${versions.map(([label]) => label).join(', ')})`);
