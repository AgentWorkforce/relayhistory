import { describe, expect, it, vi } from "vitest";
import { containRejection } from "../src/lib/host-hooks.js";

describe("containRejection", () => {
  it("handles a rejected promise", async () => {
    const onRejected = vi.fn();
    containRejection(Promise.reject(new Error("x")), onRejected);
    await new Promise((done) => setTimeout(done, 0));
    expect(onRejected).toHaveBeenCalledTimes(1);
  });

  it("reads then once and calls it with the result as receiver", () => {
    const onRejected = vi.fn();
    let reads = 0;
    const thenable = {
      get then() {
        reads += 1;
        // A second read would hand back something that is not callable.
        if (reads > 1) return undefined;
        return function (this: unknown, _: unknown, reject: () => void) {
          expect(this).toBe(thenable);
          reject();
        };
      },
    };
    containRejection(thenable, onRejected);
    expect(reads).toBe(1);
    expect(onRejected).toHaveBeenCalledTimes(1);
  });

  it("treats a throwing then accessor or then call as a rejection", () => {
    const onRejected = vi.fn();
    containRejection(
      {
        get then() {
          throw new Error("accessor");
        },
      },
      onRejected,
    );
    containRejection(
      {
        then() {
          throw new Error("call");
        },
      },
      onRejected,
    );
    expect(onRejected).toHaveBeenCalledTimes(2);
  });

  it("ignores values that are not thenables", () => {
    const onRejected = vi.fn();
    for (const value of [undefined, null, 1, "x", {}, () => {}])
      containRejection(value, onRejected);
    expect(onRejected).not.toHaveBeenCalled();
  });
});
