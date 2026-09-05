/**
 * API node: HTTP only.
 *
 * Serves submissions and reads. It does not accept worker sockets and never
 * campaigns for leadership, so it can be scaled purely on request volume.
 */
import { buildServer } from '../api/server.js';
import { runMigrations, waitForDatabase } from '../db/migrate.js';
import { closePool } from '../db/pool.js';
import { closeRedis } from '../redis/client.js';
import { installProcessHandlers } from '../process.js';
import { logger } from '../logger.js';

await waitForDatabase();
await runMigrations();

const server = await buildServer({ enableWorkerPlane: false, leader: null });

installProcessHandlers({
  steps: [
    { name: 'stop accepting http', run: () => server.close() },
    { name: 'close redis', run: () => closeRedis() },
    { name: 'close postgres pool', run: () => closePool() },
  ],
});

logger.info('api node ready');
