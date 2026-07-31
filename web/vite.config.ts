import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: {
    // Built assets are embedded into the Go binary from here.
    outDir: '../internal/webassets/dist',
    // Set explicitly because outDir is outside Vite's root; without it Vite
    // refuses to clean the directory and stale hashed assets accumulate.
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    strictPort: true,
    // `make dev` runs the Go server with -dev, which proxies to this server.
    // This proxy handles the reverse direction so the SPA's /api calls reach Go.
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8787',
        changeOrigin: false,
      },
    },
  },
})
