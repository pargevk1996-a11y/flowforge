import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The dev server proxies the control plane, so `npm run dev` talks to a
// `flowforge api` on :8000 without any CORS ceremony.
const target = process.env.FLOWFORGE_API ?? "http://127.0.0.1:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: Object.fromEntries(
      ["/runs", "/triggers", "/tenants"].map((path) => [path, { target, changeOrigin: true }]),
    ),
  },
  build: { outDir: "dist", sourcemap: true },
  test: { environment: "node", include: ["src/**/*.test.ts"] },
});
