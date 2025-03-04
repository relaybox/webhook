import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
  resolve: {
    alias: {
      '@': resolve(__dirname, 'src')
    }
  },
  test: {
    // silent: true,
    globals: true,
    tsconfig: './tsconfig.json',
    include: ['**/*.test.ts', '**/*.spec.ts'],
    environment: 'node',
    setupFiles: ['./vitest.setup.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html'],
      exclude: ['**/node_modules/**', '**/*.test.ts']
    },
    sequence: {
      shuffle: false,
      concurrent: false
    },
    poolOptions: {
      forks: {
        singleFork: true
      }
    }
  }
});
