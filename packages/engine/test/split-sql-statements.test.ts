import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { MIGRATIONS_DIR, splitSqlStatements } from "../src/migrate/index.js";

const migrationsDir = MIGRATIONS_DIR;

const split = splitSqlStatements;

describe("splitSqlStatements", () => {
  it("splits on statement-terminating semicolons", () => {
    expect(split("SELECT 1; SELECT 2;")).toEqual(["SELECT 1", "SELECT 2"]);
  });

  it("keeps a trailing statement that has no semicolon", () => {
    expect(split("SELECT 1;\nSELECT 2")).toEqual(["SELECT 1", "SELECT 2"]);
  });

  it("drops comment-only and empty trailing chunks", () => {
    expect(split("SELECT 1;\n-- done\n")).toEqual(["SELECT 1"]);
    expect(split(";;\n\n")).toEqual([]);
  });

  it("ignores semicolons inside string literals", () => {
    expect(split("INSERT INTO t VALUES ('a;b'); SELECT 1;")).toEqual([
      "INSERT INTO t VALUES ('a;b')",
      "SELECT 1",
    ]);
  });

  it("handles doubled-quote escapes inside literals", () => {
    expect(split("SELECT 'it''s; fine'; SELECT 2;")).toEqual([
      "SELECT 'it''s; fine'",
      "SELECT 2",
    ]);
  });

  it("ignores semicolons inside quoted identifiers", () => {
    expect(split('SELECT "odd;name" FROM t; SELECT 2;')).toEqual([
      'SELECT "odd;name" FROM t',
      "SELECT 2",
    ]);
  });

  it("ignores semicolons inside line and block comments", () => {
    expect(split("SELECT 1; -- trailing; comment\nSELECT 2;")).toEqual([
      "SELECT 1",
      "-- trailing; comment\nSELECT 2",
    ]);
    expect(split("SELECT 1 /* a; b */; SELECT 2;")).toEqual([
      "SELECT 1 /* a; b */",
      "SELECT 2",
    ]);
  });

  it("ignores semicolons inside dollar-quoted blocks", () => {
    const source =
      "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql; SELECT 2;";
    expect(split(source)).toEqual([
      "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql",
      "SELECT 2",
    ]);
  });

  it("preserves the jsonb defaults our migrations actually use", () => {
    expect(
      split("CREATE TABLE t (tags JSONB NOT NULL DEFAULT '[]'::jsonb);"),
    ).toEqual(["CREATE TABLE t (tags JSONB NOT NULL DEFAULT '[]'::jsonb)"]);
  });

  // The regression that broke every deploy from 2026-06-30: each statement must
  // be executable on its own, because Neon's HTTP driver sends one prepared
  // statement per call and Postgres rejects multi-command prepared statements.
  it.each(
    readdirSync(migrationsDir)
      .filter((name) => name.endsWith(".sql"))
      .sort(),
  )("splits %s into single-command statements", (name) => {
    const statements = split(readFileSync(join(migrationsDir, name), "utf8"));

    expect(statements.length).toBeGreaterThan(0);

    for (const statement of statements) {
      const withoutComments = statement
        .replace(/\/\*[\s\S]*?\*\//g, "")
        .replace(/--[^\n]*/g, "");
      const withoutLiterals = withoutComments
        .replace(
          /\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$[\s\S]*?\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/g,
          "",
        )
        .replace(/'(?:''|[^'])*'/g, "")
        .replace(/"(?:""|[^"])*"/g, "");

      expect(withoutLiterals).not.toContain(";");
      expect(withoutComments.trim().length).toBeGreaterThan(0);
    }
  });
});
