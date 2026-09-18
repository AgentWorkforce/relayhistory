/** Stage a portable helper package without installing, publishing or contacting a service. */
import { copyFile, chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve, join } from "node:path";
import {
  packageName,
  plugins as packages,
  platforms,
  validatePluginManifest,
} from "./history-package-contract.mjs";
const [plugin, platform, version, input, output] = process.argv.slice(2);
if (
  !packages[plugin] ||
  !platforms[platform] ||
  !/^\d+\.\d+\.\d+(?:-[\w.-]+)?$/.test(version ?? "") ||
  !input ||
  !output
)
  throw new Error(
    "Usage: package-history-helper PLUGIN PLATFORM VERSION BINARY OUTPUT",
  );
const manifest = JSON.parse(
  await readFile(
    new URL(`../plugins/${plugin}/sdk/package.json`, import.meta.url),
    "utf8",
  ),
);
validatePluginManifest(plugin, manifest);
if (version !== manifest.version)
  throw new Error("Helper version must match its optional SDK package version");
const info = packages[plugin];
const [os, cpu, libc] = platforms[platform];
const binary = info.binary + (os === "win32" ? ".exe" : "");
await mkdir(output, { recursive: true });
await copyFile(resolve(input), join(output, binary));
await chmod(join(output, binary), 0o755);
await writeFile(
  join(output, "package.json"),
  JSON.stringify(
    {
      name: packageName(info, platform),
      version,
      license: "MIT",
      description: `Optional ${info.name} helper for ${platform}`,
      files: [binary],
      os: [os],
      cpu: [cpu],
      ...(libc ? { libc: [libc] } : {}),
      publishConfig: { access: "public" },
    },
    null,
    2,
  ) + "\n",
);
