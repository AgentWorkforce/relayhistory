import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    hookTimeout: 120_000,
    testTimeout: 60_000,
    include: ["test/**/*.test.ts"],
  },
});
