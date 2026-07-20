import { fileURLToPath, URL } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  // Keep assets relative so the bundled dashboard works behind path-based
  // proxies such as code-server's /proxy/<port>/ endpoint.
  base: "./",
  plugins: [react()],
  build: {
    emptyOutDir: true,
    outDir: fileURLToPath(
      new URL("../src/ray/klein/observability/dashboard/static", import.meta.url),
    ),
    sourcemap: false,
  },
});
