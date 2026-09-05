import { runMigrations, waitForDatabase } from '../db/migrate.js';
import { closePool } from '../db/pool.js';
import { logger } from '../logger.js';

await waitForDatabase();
const applied = await runMigrations();
logger.info({ applied, count: applied.length }, 'migrations complete');
await closePool();
