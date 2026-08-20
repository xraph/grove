import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "node",
          include: ["src/__tests__/**/*.test.ts"],
          environment: "node",
        },
      },
      {
        test: {
          name: "dom",
          include: ["src/__tests__/**/*.test.tsx"],
          environment: "jsdom",
          setupFiles: ["./src/__tests__/setup.ts"],
        },
      },
    ],
  },
});
