// Build-only entrypoint. `scripts/build-cloud-auth.mjs` aliases the package
// barrel to its focused auth module and replaces this emitted module with a
// tree-shaken bundle, so npm users need neither the full Cloud package nor its
// CLI.
export { ensureCloudSession } from '@agent-relay/cloud';
