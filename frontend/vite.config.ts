import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    // Only localhost and private Codespaces forwarding hosts; allowing every host
    // disables Vite's DNS-rebinding check in front of the proxied health data API.
    allowedHosts: ['.app.github.dev'],
    proxy: {
      '/api': {
        // `just dev-isolated` runs a second backend on another port next to `just dev`.
        target: `http://localhost:${process.env.BACKEND_PORT ?? '8000'}`,
        changeOrigin: true,
      },
    },
  },
})
