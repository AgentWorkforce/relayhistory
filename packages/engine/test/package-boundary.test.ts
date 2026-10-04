/**
 * The main entry bundles for Cloudflare Workers and runs on Node, so its module graph
 * uses Web APIs only and depends on nothing but its peers, `hono` and `drizzle-orm`.
 * Node built-ins are confined to `@relayhistory/engine/migrations`, which reads the SQL
 * files from disk. No entry imports a database driver, a deploy tool or a private
 * package: the host injects the database.
 */
import { readFileSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const src = resolve(packageRoot, "src");

const PEERS = /^(hono|drizzle-orm)(\/.*)?$/;
const FORBIDDEN =
  /^(@neondatabase\/|pg$|pg\/|postgres$|wrangler$|@cloudflare\/|@agentworkforce\/|@relayflows\/|@agent-relay\/)/;

interface ModuleGraph {
  /** Package-relative paths of every module reached. */
  modules: string[];
  /** Every non-relative specifier, with the module that imports it. */
  external: { specifier: string; from: string }[];
}

/** Follows relative imports (static, dynamic, `export from`, type-only) from `entry`. */
function moduleGraph(entry: string): ModuleGraph {
  const seen = new Set<string>();
  const external: ModuleGraph["external"] = [];
  const pending = [resolve(src, entry)];
  while (pending.length) {
    const file = pending.pop()!;
    if (seen.has(file)) continue;
    seen.add(file);
    const from = relative(packageRoot, file);
    const { importedFiles } = ts.preProcessFile(
      readFileSync(file, "utf8"),
      true,
      true,
    );
    for (const { fileName: specifier } of importedFiles) {
      if (specifier.startsWith(".")) {
        pending.push(resolve(dirname(file), specifier).replace(/\.js$/, ".ts"));
      } else {
        external.push({ specifier, from });
      }
    }
  }
  return {
    modules: [...seen].map((f) => relative(packageRoot, f)).sort(),
    external,
  };
}

describe("package boundary", () => {
  const main = moduleGraph("index.ts");

  it("reaches the engine's modules from the main entry", () => {
    // A sanity floor: the walk follows `export * from` and `export { } from`.
    expect(main.modules).toEqual(
      expect.arrayContaining([
        "src/index.ts",
        "src/engine.ts",
        "src/middleware/auth.ts",
        "src/auth/bootstrap.ts",
        "src/auth/tokens.ts",
        "src/db/schema.ts",
        "src/lib/delivery.ts",
        "src/routes/delivery.ts",
      ]),
    );
  });

  it("keeps the main entry free of Node built-ins", () => {
    expect(
      main.external.filter(
        ({ specifier }) =>
          specifier.startsWith("node:") || !/^[@a-z]/.test(specifier),
      ),
    ).toEqual([]);
  });

  it("imports only hono and drizzle-orm from the main entry", () => {
    expect(main.external.map(({ specifier }) => specifier)).toEqual(
      expect.arrayContaining(["hono", "drizzle-orm"]),
    );
    expect(
      main.external.filter(({ specifier }) => !PEERS.test(specifier)),
    ).toEqual([]);
  });

  it("does not pull the migration runner into the main entry", () => {
    expect(main.modules.filter((m) => m.startsWith("src/migrate/"))).toEqual(
      [],
    );
  });

  it("keeps the schema entry to drizzle-orm", () => {
    const schema = moduleGraph("db/schema.ts");
    expect(
      schema.external.filter(
        ({ specifier }) => !/^drizzle-orm(\/.*)?$/.test(specifier),
      ),
    ).toEqual([]);
  });

  it("confines the migrations entry to Node built-ins and its peers", () => {
    const migrate = moduleGraph("migrate/index.ts");
    expect(migrate.external.map(({ specifier }) => specifier)).toContain(
      "node:fs",
    );
    expect(
      migrate.external.filter(
        ({ specifier }) =>
          !specifier.startsWith("node:") && !PEERS.test(specifier),
      ),
    ).toEqual([]);
  });

  it("imports no database driver, deploy tool or private package from any entry", () => {
    for (const entry of ["index.ts", "db/schema.ts", "migrate/index.ts"]) {
      expect(
        moduleGraph(entry).external.filter(({ specifier }) =>
          FORBIDDEN.test(specifier),
        ),
      ).toEqual([]);
    }
  });

  it("publishes each entry from the module graph checked here", () => {
    const pkg = JSON.parse(
      readFileSync(resolve(packageRoot, "package.json"), "utf8"),
    ) as {
      exports: Record<string, string | { default: string }>;
      dependencies?: Record<string, string>;
      peerDependencies: Record<string, string>;
    };
    const target = (key: string) => {
      const value = pkg.exports[key]!;
      return typeof value === "string" ? value : value.default;
    };
    expect(target(".")).toBe("./dist/index.js");
    expect(target("./schema")).toBe("./dist/db/schema.js");
    expect(target("./migrations")).toBe("./dist/migrate/index.js");
    expect(Object.keys(pkg.dependencies ?? {})).toEqual([]);
    expect(Object.keys(pkg.peerDependencies).sort()).toEqual([
      "drizzle-orm",
      "hono",
    ]);
  });
});
