/**
 * `relayhistory-server token create|list|revoke`: operator credential commands. They run
 * against the database directly, so they prepare the schema first (granting the runtime
 * role like `serve` does) and check every argument before touching it.
 */
import { parseArgs } from "node:util";
import { databaseUrl, runtimeRole } from "./config.js";
import { openDatabase, prepareDatabase } from "./database.js";
import type { Logger } from "./log.js";
import {
  createTokenFile,
  listTokens,
  revokeToken,
  validateTokenOptions,
} from "./tokens.js";

export class UsageError extends Error {}

function required(values: Record<string, unknown>, name: string): string {
  const value = values[name];
  if (typeof value !== "string" || !value.trim())
    throw new UsageError(`--${name} is required`);
  return value.trim();
}

function wholeDays(value: string): number {
  if (!/^\d+$/.test(value))
    throw new UsageError("--expires-days must be a whole number of days");
  return Number(value);
}

export async function tokenCommand(
  args: string[],
  log: Logger,
  env: Record<string, string | undefined> = process.env,
) {
  const [action, ...rest] = args;
  const { values } = parseArgs({
    args: rest,
    options: {
      org: { type: "string" },
      workspace: { type: "string" },
      label: { type: "string" },
      scopes: { type: "string" },
      "expires-days": { type: "string" },
      out: { type: "string" },
      id: { type: "string" },
    },
    strict: true,
  });
  if (action !== "create" && action !== "list" && action !== "revoke")
    throw new UsageError("token needs create, list or revoke");
  // Every argument is checked before the database is touched.
  const org = required(values, "org");
  const create =
    action === "create"
      ? {
          out: required(values, "out"),
          options: {
            orgId: org,
            workspaceId: required(values, "workspace"),
            label: required(values, "label"),
            ...(values.scopes
              ? {
                  scopes: values.scopes.split(",").map((scope) => scope.trim()),
                }
              : {}),
            ...(values["expires-days"] !== undefined
              ? { expiresInDays: wholeDays(values["expires-days"]) }
              : {}),
          },
        }
      : undefined;
  if (create) validateTokenOptions(create.options);
  const id = action === "revoke" ? required(values, "id") : undefined;
  // Token commands may run before the first `serve`; the schema they write must exist.
  const url = databaseUrl(env);
  const role = runtimeRole(env);
  await prepareDatabase(url, { ...(role ? { runtimeRole: role } : {}), log });
  const database = openDatabase(url, { max: 1, log });
  try {
    if (create) {
      const { out } = create;
      const file = await createTokenFile(
        database.db,
        create.options,
        // `--out -` hands the token file to a pipe; otherwise the secret only goes to disk.
        out === "-"
          ? {
              write: (text) =>
                new Promise<void>((resolve, reject) =>
                  process.stdout.write(text, (error) =>
                    error ? reject(error) : resolve(),
                  ),
                ),
            }
          : { path: out },
      );
      log.info("token created", {
        id: file.id,
        label: file.label,
        scopes: file.scopes,
        expiresAt: file.expiresAt,
        orgId: file.orgId,
        workspaceId: file.workspaceId,
        accountId: file.accountId,
        ...(out === "-" ? {} : { out }),
      });
    } else if (action === "list") {
      const tokens = await listTokens(database.db, org);
      process.stdout.write(`${JSON.stringify(tokens, null, 2)}\n`);
    } else {
      if (!(await revokeToken(database.db, org, id!)))
        throw new UsageError("no active service token with that id");
      log.info("token revoked", { id });
    }
  } finally {
    await database.close();
  }
}
