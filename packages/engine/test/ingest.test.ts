import { describe, expect, it } from "vitest";
import { buildReadableContent } from "../src/lib/ingest.js";

describe("hosted ingest mapping helpers", () => {
  it("prefixes task context into readable content for embedding", () => {
    expect(
      buildReadableContent("Choose Hono middleware for auth.", {
        title: "Build CLI login",
        description: "Wire RelayAuth to service-local sessions.",
      }),
    ).toBe(
      [
        "Task: Build CLI login",
        "Task description: Wire RelayAuth to service-local sessions.",
        "",
        "Choose Hono middleware for auth.",
      ].join("\n"),
    );
  });

  it("redacts task context and content together", () => {
    expect(
      buildReadableContent("Contact dev@example.com", {
        title: "Use token ghp_123456789012345678901234567890123456",
      }),
    ).toBe("Task: Use token [REDACTED]\n\nContact [REDACTED]");
  });

  it("does not duplicate a task prefix from an older client", () => {
    expect(
      buildReadableContent("Task: Build CLI login\n\nChoose Hono middleware.", {
        title: "Build CLI login",
      }),
    ).toBe("Task: Build CLI login\n\nChoose Hono middleware.");
  });
});
