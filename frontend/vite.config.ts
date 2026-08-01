import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/cluster': 'http://localhost:7000',
      '/kv': 'http://localhost:7000',
      '/admin': 'http://localhost:7000',
      '/metrics': 'http://localhost:7000',
    },
  },
})
