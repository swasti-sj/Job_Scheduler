import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    setupFiles: ['tests/setup.ts'],
    globals: false,
    include: ['tests/**/*.test.ts'],
    // Integration tests share one Postgres and race on global state (leader
    // lock, worker registry), so they run one file at a time. Unit tests are
    // pure and would happily parallelise, but a single setting keeps CI simple.
    fileParallelism: false,
    testTimeout: 60_000,
    hookTimeout: 60_000,
    reporters: ['default'],
  },
});
