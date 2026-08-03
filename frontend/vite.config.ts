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
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
            {
              name: "react-vendor",
              priority: 40,
              test: /node_modules[\\/](react|react-dom|react-router|scheduler)[\\/]/,
            },
            {
              name: "mui-vendor",
              priority: 30,
              test: /node_modules[\\/](@emotion|@mui|@popperjs|react-transition-group)[\\/]/,
            },
          ],
        },
      },
    },
    sourcemap: false,
  },
});
