import { describe as suite, expect, it } from "vitest";
import { UploadError } from "../src/client.js";
import { describe, exitCode } from "../src/failures.js";
import { SyncError } from "../src/sync.js";

suite("failure reporting", () => {
  it("keeps a failed capture's exit code or signal", () => {
    expect(describe(new SyncError("local sync exited 3"))).toEqual({
      failure: "sync",
      detail: "local sync exited 3",
    });
    expect(
      describe(new SyncError("local sync was ended by SIGKILL")),
    ).toMatchObject({
      detail: "local sync was ended by SIGKILL",
    });
    expect(exitCode(new SyncError("local sync exited 3"))).toBe(1);
  });

  it("reduces errors it did not write to class and code", () => {
    const foreign = Object.assign(
      new Error("open /home/me/secret/path failed"),
      { code: "EACCES" },
    );
    expect(describe(foreign)).toEqual({
      failure: "error",
      error: "Error",
      code: "EACCES",
    });
    expect(JSON.stringify(describe(foreign))).not.toContain("secret");
    expect(
      describe(
        new UploadError("permission_denied", "token lacks permission (403)"),
      ),
    ).toEqual({
      failure: "permission_denied",
      detail: "token lacks permission (403)",
    });
  });
});
