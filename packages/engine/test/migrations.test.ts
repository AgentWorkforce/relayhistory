import { readFileSync, readdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  MIGRATIONS_DIR,
  applyMigrations,
  readMigrations,
} from "../src/migrate/index.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

let database: TestDatabase;

beforeAll(async () => {
  database = await createTestDatabase();
});
afterAll(async () => {
  await database?.close();
});

describe("packaged migrations", () => {
  it("ships every migration with a recorded checksum", () => {
    const names = readdirSync(MIGRATIONS_DIR).filter((n) => n.endsWith(".sql"));
    const migrations = readMigrations();
    expect(migrations.map((m) => m.name)).toEqual(names.sort());
    for (const migration of migrations) {
      const bytes = readFileSync(resolve(MIGRATIONS_DIR, migration.name));
      expect(migration.checksum).toBe(
        createHash("sha256").update(bytes).digest("hex"),
      );
    }
  });

  it("applies the full ledger and is idempotent", async () => {
    const ledger = await database.query<{ name: string; checksum: string }>(
      "SELECT name, checksum FROM sessions.__migrations ORDER BY name",
    );
    expect(ledger.rows.map((row) => row.name)).toEqual(
      readMigrations().map((m) => m.name),
    );
    const again = await applyMigrations({
      query: (text) => database.query(text),
    });
    expect(again.applied).toEqual(
      readMigrations().map(({ name, checksum }) => ({ name, checksum })),
    );
  });
});
