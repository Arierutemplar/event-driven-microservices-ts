/// <reference types="vitest/config" />
import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// DEV_HOST / DEV_PORT (opcionales, en .env o en el entorno) sobreescriben los valores fijos.
// Con DEV_HOST=true escucha en todas las interfaces (útil en contenedores).
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  const host = env.DEV_HOST === "true" ? true : env.DEV_HOST || "localhost";
  const port = Number(env.DEV_PORT) || 5173;

  return {
    plugins: [react(), tailwindcss()],
    test: { environment: "jsdom", restoreMocks: true, setupFiles: ["./src/test-setup.ts"] },
    server: {
      host,
      port,
      // Falla en lugar de saltar a otro puerto si está ocupado.
      strictPort: true,
    },
    preview: {
      host,
      port: Number(env.DEV_PREVIEW_PORT) || 4173,
      strictPort: true,
    },
  };
});
