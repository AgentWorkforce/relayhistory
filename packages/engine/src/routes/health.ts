import { Hono } from "hono";
import type { HistoryEnv } from "../env.js";

export function createHealthRoutes(): Hono<HistoryEnv> {
  const healthRoutes = new Hono<HistoryEnv>();
  healthRoutes.get("/health", (c) =>
    c.json({ ok: true, service: "relayhistory" }),
  );
  return healthRoutes;
}
