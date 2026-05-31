import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    testTimeout: 300_000,
    hookTimeout: 300_000,
  },
  resolve: {
    alias: {
      alchemy: "/Users/nolarose/Projects/alchemy-effect/packages/alchemy/src",
    },
  },
});
