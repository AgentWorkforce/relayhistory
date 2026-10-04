/**
 * Operator credential bootstrap. These are database operations run by whoever holds
 * the deployment's `DATABASE_URL`; no HTTP route mints a token without one.
 */
import { open, rm } from "node:fs/promises";
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

/** Where `createTokenFile` delivers the secret. */
export type TokenDestination =
  { path: string } | { write(text: string): Promise<void> };

/**
 * Mint a token and deliver its file, or leave no usable credential behind. A path is
 * opened owner-only and exclusively before anything is minted, so an existing file is
 * refused without touching the database; a delivery that fails after minting revokes
 * the token it could not hand over.
 */
export async function createTokenFile(
  db: HistoryDb,
  options: CreateTokenOptions,
  destination: TokenDestination,
): Promise<TokenFile> {
  const handle =
    "path" in destination
      ? await open(destination.path, "wx", 0o600)
      : undefined;
  let file: TokenFile | undefined;
  try {
    file = await createToken(db, options);
    const text = `${JSON.stringify(file, null, 2)}\n`;
    if (handle) await handle.writeFile(text);
    else
      await (destination as { write(text: string): Promise<void> }).write(text);
    await handle?.close();
    return file;
  } catch (error) {
    await handle?.close().catch(() => {});
    if (handle)
      await rm((destination as { path: string }).path, { force: true });
    if (file)
      await revokeServiceToken(
        db,
        file.orgId,
        file.id,
        "token file could not be delivered",
      );
    throw error;
  }
}

export function listTokens(db: HistoryDb, orgId: string) {
  return listServiceTokens(db, orgId);
}

export function revokeToken(db: HistoryDb, orgId: string, id: string) {
  return revokeServiceToken(db, orgId, id);
}
