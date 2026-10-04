import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // PGlite boot plus the full migration set can exceed Vitest's 10s default when
    // database-backed files start together.
    hookTimeout: 120_000,
    testTimeout: 60_000,
    include: ["test/**/*.test.ts"],
  },
});
