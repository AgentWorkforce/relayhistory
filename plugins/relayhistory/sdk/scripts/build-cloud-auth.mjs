import { build } from 'esbuild';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';

const require = createRequire(import.meta.url);
const cloudPackageDir = dirname(require.resolve('@agent-relay/cloud/package.json'));

await build({
  entryPoints: ['src/cloud-auth-bundle.ts'],
  outfile: 'dist/cloud-auth-bundle.js',
  bundle: true,
  platform: 'node',
  format: 'esm',
  target: 'node20',
  sourcemap: true,
  alias: {
    '@agent-relay/cloud': join(cloudPackageDir, 'dist', 'auth.js'),
  },
  banner: { js: '/* Agent Relay Cloud SDK; bundled for ai-hist login. */' },
});
