import tailwindcss from '@tailwindcss/vite';
import { devtools } from '@tanstack/devtools-vite';
import { tanstackStart } from '@tanstack/react-start/plugin/vite';
import viteReact from '@vitejs/plugin-react';
import { defineConfig, loadEnv } from 'vite';

export default defineConfig(({ mode }) => {
  // Make packages/dashboard/.env (CLICKHOUSE_*, DASHBOARD_PORT) available to
  // server functions via process.env. Real environment variables still win.
  const fileEnv = loadEnv(mode, import.meta.dirname, '');
  for (const key of Object.keys(fileEnv)) {
    if (process.env[key] === undefined) process.env[key] = fileEnv[key];
  }

  return {
    resolve: { tsconfigPaths: true },
    server: { port: Number(process.env.DASHBOARD_PORT ?? 3105) },
    plugins: [devtools(), tailwindcss(), tanstackStart(), viteReact()],
  };
});
