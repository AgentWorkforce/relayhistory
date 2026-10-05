// Stopping a child process the e2e script spawned, whatever state it is in.
import { once } from "node:events";

/**
 * Whether the child is still running. A child that exited has a non-null `exitCode`;
 * one ended by a signal keeps `exitCode` null and has a `signalCode` instead.
 */
export function running(child) {
  return child.exitCode === null && child.signalCode === null;
}

/**
 * Ask a running child to stop with SIGTERM and wait for it to exit. A child that has
 * already exited, by code or by signal, is left alone: its `exit` event has fired, and
 * waiting for it again would never return.
 */
export async function stopChild(child) {
  if (!running(child)) return;
  const exited = once(child, "exit");
  child.kill("SIGTERM");
  await exited;
}
