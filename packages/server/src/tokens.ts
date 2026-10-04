/**
 * Operator credential bootstrap. These are database operations run by whoever holds
 * the deployment's `DATABASE_URL`; no HTTP route mints a token without one.
 */
import { open } from "node:fs/promises";
import {
  bootstrapServiceToken,
  deliveryAccount,
  listServiceTokens,
  revokeServiceToken,
  type BootstrappedServiceToken,
  type HistoryDb,
} from "@relayhistory/engine";

/** What `token create` writes: the secret plus the tenant it is bound to. */
export interface TokenFile {
  version: 1;
  token: string;
  id: string;
  label: string;
  scopes: string[];
  expiresAt: string;
  orgId: string;
  workspaceId: string;
  /** The delivery account an uploader must name; derived from the tenant. */
  accountId: string;
}

export interface CreateTokenOptions {
  orgId: string;
  workspaceId: string;
  label: string;
  scopes?: string[];
  expiresInDays?: number;
}

export async function createToken(
  db: HistoryDb,
  options: CreateTokenOptions,
): Promise<TokenFile> {
  const issued: BootstrappedServiceToken = await bootstrapServiceToken(db, {
    orgId: options.orgId,
    workspaceId: options.workspaceId,
    label: options.label,
    ...(options.scopes ? { scopes: options.scopes } : {}),
    ...(options.expiresInDays !== undefined
      ? { expiresInDays: options.expiresInDays }
      : {}),
  });
  return {
    version: 1,
    token: issued.token,
    id: issued.id,
    label: issued.label,
    scopes: issued.scopes,
    expiresAt: issued.expiresAt,
    orgId: issued.orgId,
    workspaceId: issued.workspaceId,
    accountId: await deliveryAccount({
      userId: "operator",
      orgId: issued.orgId,
      workspaceId: issued.workspaceId,
      tokenSubject: "operator",
      scopes: issued.scopes,
      claims: {},
    }),
  };
}

/**
 * Write a token file readable only by its owner. An existing file is never replaced:
 * a second token for the same path is an operator mistake, not a rotation.
 */
export async function writeTokenFile(path: string, file: TokenFile) {
  const handle = await open(path, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(file, null, 2)}\n`);
  } finally {
    await handle.close();
  }
}

export function listTokens(db: HistoryDb, orgId: string) {
  return listServiceTokens(db, orgId);
}

export function revokeToken(db: HistoryDb, orgId: string, id: string) {
  return revokeServiceToken(db, orgId, id);
}
