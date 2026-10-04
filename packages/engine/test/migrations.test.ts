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

/**
 * The ledger checksum of every applied migration, as deployed. A deployed database
 * refuses to start if an applied file changes, so these bytes are frozen: a schema
 * change is a new file, never an edit. A new migration adds its entry here.
 */
const APPLIED_CHECKSUMS: Record<string, string> = {
  "0002_neon_convergence.sql":
    "f251227af828d711fff16a591b00c3b9e0f87885b13d152217b4a926549e6946",
  "0003_reflex_learnings.sql":
    "036d5f4d5a99e43c81b37079796157d27900e1ba8dd858c9feba9321197d3e48",
  "0004_conversation_turns.sql":
    "219c7a21dab2d573e83733543f265c30a44aa17a4232c58a2b02e2f7b91a6049",
  "0005_neighborhood_memory.sql":
    "2239dc5be17dd46cc9056d752227fb15e4cf5e85c5a4a48fb30c0a6fd031b709",
  "0006_project_id_original.sql":
    "6f56d9fa79fd4a3747f2ca6aec1cfd499a5c6f97f21f1cee1b8ecf67af4fde31",
  "0007_daily_digests.sql":
    "abb04dca9169dbe726026668b059cf8790a464e024a213bc4dc26a79f2c163fa",
  "0008_session_links.sql":
    "2299d35f60b682e8cafbc21112723913724db36bb8287d58dabde7fc07039bb6",
  "0009_durable_delivery.sql":
    "f7586e05afdf160ccc3f7da03adf33359103051fec0b89f00c77271332973178",
  "0010_probe_connections.sql":
    "83809ece30cedb486777bc5e3290abf5b02353f294fdceaee984885637444b39",
  "0011_delivery_session_projection.sql":
    "d1563e17e8bf40aad5ae0e0363910681d7489cb44fb74efc2dea4d13bf8da66a",
  "0012_probe_progress.sql":
    "e76c0afc1204dad1197a4d18d540096c88d2ccf20aa01e410a317efa569289e5",
  "0013_session_evaluations.sql":
    "9079e6fe048dfdee9e51094bd69b21ff7d3d0d0a93807ef2da2363a118f98994",
  "0014_delivery_activity_projection.sql":
    "af2d9ac6bf396438296ad76a8a094b2a041000ee34dbf63a584d96bf991ec9e6",
  "0015_session_analysis.sql":
    "b1ea044ac8c956589727613223abf9b7dd55081a457e1159fb7324a9148204cb",
  "0016_session_analysis_requests.sql":
    "da8ea08a8e09e272cf03e012a829a7ba8cc042340485761b17e30b934a26c6d0",
  "0017_babysitter_delivery_retention.sql":
    "16083a204fb36d59a6ad2f8aed167293da630b15c453b620777874ceb02e5714",
  "0018_session_outcomes_workspace_key.sql":
    "1df54b554a1ce987bacbb8e2e48ad17311b24e7faf576a9be36772fef70dfe55",
  "0019_session_briefs.sql":
    "4dece4f0f3a81ba3d7efd94d8a8c0c260a36e807b659ec310506996be768ef81",
  "0020_conversation_turns_source_key.sql":
    "59c7beaf4582360fe9e94597c1335b2afaa8b7059cf6995d04b9a0dcf05aa1aa",
  "0021_session_outcomes_project_and_usage.sql":
    "5c1035253743c518593e5eb7ce40ac675b8a419607071df8be301b9358d3092a",
  "0022_delivery_session_catalog.sql":
    "94c7d013f139ede60e4fabe9e54e25827fd4521b3e51da70b6eedc5b3ea0f579",
  "0023_delivery_projection_v2.sql":
    "4f9997dafa53cd1306112885dbf6fa695b84df9dcaa7e0813221579ed624ee0f",
  "0024_delivery_settle_changed_rows.sql":
    "0309e725ffff9e31a58c8f33feab22407b86530a7fce1a45532d391f61fc3868",
  "0024_session_brief_jobs.sql":
    "ae20a2153d944abd813fce89e522d3d2b3ad14705f5feda0cf3e30fee85205c1",
  "0026_typed_delivery_conflicts.sql":
    "1d54247d4f5267de8a9f6000d1f44b877636da1db7425e2deff4202d873be66f",
  "0028_convergence_events_search_trgm.sql":
    "c084454cdee446f9b3ba1693bdc0fe0cb56c46e1af396b9ed4b8a3d3283af1ea",
  "0029_delivery_projection_rollout.sql":
    "0ec420f3d6b284a6a5fa56c3e9dc220231bfd9e12b8d7feae87a35a42d8191a8",
  "0030_session_rollups.sql":
    "ff1df82b5e4bcc861f1643df54c414ac7c481259f3a47a1d45421759d7133f0d",
};

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

  it("keeps every applied migration byte-identical", () => {
    const actual = Object.fromEntries(
      readMigrations().map(({ name, checksum }) => [name, checksum]),
    );
    for (const [name, checksum] of Object.entries(APPLIED_CHECKSUMS))
      expect(actual[name], name).toBe(checksum);
    expect(Object.keys(actual).sort()).toEqual(
      Object.keys(APPLIED_CHECKSUMS).sort(),
    );
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
