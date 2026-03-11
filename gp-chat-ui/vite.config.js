import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  // Widget build: `npm run build:widget`
  if (process.env.BUILD_MODE === 'widget') {
    return {
      plugins: [react()],
      define: {
        'process.env.NODE_ENV': JSON.stringify('production'),
      },
      build: {
        outDir: 'dist-widget',
        lib: {
          entry: 'src/widget.jsx',
          name: 'GPWorkforceChatbot',
          formats: ['iife'],
          fileName: () => 'gp-chatbot-widget.js',
        },
        cssCodeSplit: false,
        rollupOptions: {
          output: {
            // All CSS inlined into the JS bundle — single file embed!
            assetFileNames: 'gp-chatbot-widget.[ext]',
          },
        },
      },
    }
  }

  // Default: full app build
  return {
    plugins: [react()],
  }
})
