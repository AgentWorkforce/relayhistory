/**
 * Host callbacks typed to return `void` may still be `async` (TypeScript allows it), so
 * their result can be a rejected promise that no try/catch sees. Attaching a handler
 * keeps such a rejection from becoming an unhandled rejection in the host process.
 */
export function containRejection(
  result: unknown,
  onRejected: () => void,
): void {
  if (
    result !== null &&
    (typeof result === "object" || typeof result === "function") &&
    typeof (result as { then?: unknown }).then === "function"
  ) {
    (result as PromiseLike<unknown>).then(undefined, onRejected);
  }
}
