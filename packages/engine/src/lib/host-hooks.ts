/**
 * Host callbacks typed to return `void` may still be `async` (TypeScript allows it), so
 * their result can be a rejected promise that no try/catch sees. Attaching a handler
 * keeps such a rejection from becoming an unhandled rejection in the host process.
 *
 * `then` is read once and called with the result as its receiver, as promise
 * resolution does, so an accessor cannot pass the check and then hand back something
 * else. A `then` that throws counts as a rejection.
 */
export function containRejection(
  result: unknown,
  onRejected: () => void,
): void {
  if (
    result === null ||
    (typeof result !== "object" && typeof result !== "function")
  )
    return;
  try {
    const then: unknown = (result as { then?: unknown }).then;
    if (typeof then === "function")
      then.call(result, undefined, () => onRejected());
  } catch {
    onRejected();
  }
}
