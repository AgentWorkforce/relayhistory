#!/usr/bin/env node

import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  npmViewVersion,
  REGISTRY_RELEASE_PACKAGES,
} from './registry-clean-install-smoke.mjs';

/** Refuse versions already visible on npm just before the first publish. */
export function checkReleaseVersionAvailable(version, view = npmViewVersion) {
  if (!/^\d+\.\d+\.\d+$/.test(version)) {
    throw new Error(`expected a stable semver release version, received ${version}`);
  }
  const occupied = REGISTRY_RELEASE_PACKAGES.filter(
    (pkg) => view(`${pkg}@${version}`) !== null,
  );
  if (occupied.length) {
    throw new Error(
      `${version} is already visible on npm for: ${occupied.join(', ')}. `
      + 'The version became occupied after resolution; choose a new version and rebuild before publishing.',
    );
  }
  console.log(`All ${REGISTRY_RELEASE_PACKAGES.length} core package names are available at ${version}`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  checkReleaseVersionAvailable(process.env.VERSION ?? process.argv[2]);
}
