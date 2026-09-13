import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

function readVersion(relativePath, pattern, label) {
  const source = readFileSync(resolve(repositoryRoot, relativePath), 'utf8');
  const match = pattern.exec(source);
  if (!match) {
    throw new Error(`Could not read the native contract version from ${label}`);
  }
  return Number(match[1]);
}

const rustVersion = readVersion(
  'crates/ai-hist-napi/src/lib.rs',
  /pub const NATIVE_CONTRACT_VERSION:\s*u32\s*=\s*(\d+)\s*;/,
  'the Rust binding',
);
const sdkVersion = readVersion(
  'sdk-ts/src/native.ts',
  /export const NATIVE_CONTRACT_VERSION\s*=\s*(\d+)\s*;/,
  'the TypeScript SDK',
);

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
