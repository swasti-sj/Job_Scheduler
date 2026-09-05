/**
 * Scheduler node.
 *
 * Runs the worker control plane, the dashboard stream and the full HTTP API, and
 * campaigns for leadership. Several of these run at once (three in the compose
 * file); exactly one holds the advisory lock and therefore runs the reaper,
 * breaker evaluation and cron. The others are not idle standbys - they dispatch
 * work and serve reads the whole time.
 */
import { buildServer } from '../api/server.js';
import { SchedulerNode } from '../core/schedulerNode.js';
import { runMigrations, waitForDatabase } from '../db/migrate.js';
import { closePool } from '../db/pool.js';
import { closeRedis } from '../redis/client.js';
import { installProcessHandlers } from '../process.js';
import { logger } from '../logger.js';
import { config } from '../config.js';

await waitForDatabase();
await runMigrations();

const node = new SchedulerNode();
await node.start();

const server = await buildServer({ enableWorkerPlane: true, leader: node.leader });

installProcessHandlers({
  // Order matters. Stop handing out work first, so nothing new is claimed while
  // we are on the way out; then release leadership so a follower can take over
  // immediately; then drop the connections.
  steps: [
    { name: 'stop dispatch and close sockets', run: () => server.close() },
    { name: 'release leadership', run: () => node.stop() },
    { name: 'close redis', run: () => closeRedis() },
    { name: 'close postgres pool', run: () => closePool() },
  ],
});

logger.info({ nodeId: config.nodeId }, 'scheduler node ready');
