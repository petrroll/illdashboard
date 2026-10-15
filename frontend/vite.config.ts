import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { Agent, type IncomingMessage } from 'node:http'
import { performance } from 'node:perf_hooks'

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
        // Without an agent, the proxy forces Connection: close on every request.
        agent: new Agent({ keepAlive: true }),
        configure(proxy) {
          const started = new WeakMap<IncomingMessage, number>()
          proxy.on('proxyReq', (_proxyReq, request) => {
            started.set(request, performance.now())
          })
          proxy.on('proxyRes', (response, request) => {
            const start = started.get(request)
            if (start === undefined) return
            const existing = response.headers['server-timing']
            const upstream = Array.isArray(existing) ? existing.join(', ') : existing
            const timing = `vite;dur=${(performance.now() - start).toFixed(2)};desc="Vite upstream to response headers"`
            response.headers['server-timing'] = upstream ? `${upstream}, ${timing}` : timing
            started.delete(request)
          })
        },
      },
    },
  },
})
