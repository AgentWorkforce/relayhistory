/** Host platform and public-registry install helpers for post-publish smokes. */
import assert from "node:assert/strict";
import { platforms } from "./history-package-contract.mjs";

/** This machine's platform key (`linux-x64-gnu`, `darwin-arm64`, …). */
export function currentPlatform() {
  const libc =
    process.platform === "linux"
      ? (process.report?.getReport()?.header?.glibcVersionRuntime ? "gnu" : "musl")
      : undefined;
  const key = [process.platform, process.arch, libc].filter(Boolean).join("-");
  const known = Object.keys(platforms);
  const match = known.find((candidate) => candidate === key)
    ?? known.find((candidate) => candidate.startsWith(`${process.platform}-${process.arch}`));
  assert.ok(match, `no platform entry for ${key}; known: ${known.join(", ")}`);
  return match;
}

/** npm `libc` field for this machine (`glibc` / `musl`), or undefined off Linux. */
export function hostLibc(platform = currentPlatform()) {
  return platforms[platform]?.[2];
}

/** `npm install` args that select the optional package this runner declares. */
export function hostInstallArgs(project, libc = hostLibc()) {
  const args = ["--prefix", project];
  if (libc) args.push(`--libc=${libc}`);
  return args;
}

/** Drop the publish job's npmrc/token so this is a public registry install. */
export function publicRegistryEnv(base = process.env) {
  const env = { ...base };
  delete env.NODE_AUTH_TOKEN;
  delete env.NPM_CONFIG_USERCONFIG;
  delete env.npm_config_userconfig;
  return env;
}
