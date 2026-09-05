import pg from 'pg';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { dbQueryDuration, dbPoolWaiting } from '../metrics.js';

const { Pool, Client, types } = pg;

// int8 (bigint) arrives as a string by default so 2^53 is not silently lost.
// Every int8 we select is a count that fits comfortably in a double, so parse it
// to a number and keep the call sites free of `Number(row.count)` noise.
types.setTypeParser(types.builtins.INT8, (v: string) => Number.parseInt(v, 10));

export type PoolClient = pg.PoolClient;
export type QueryResultRow = pg.QueryResultRow;

export const pool = new Pool({
  connectionString: config.databaseUrl,
  max: config.pgPoolMax,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 10_000,
  // Keeps a wedged TCP connection from pinning a claim transaction forever.
  statement_timeout: 30_000,
  application_name: `job-scheduler:${config.nodeId}`,
});

pool.on('error', (err) => {
  // An idle client blew up (server restart, network reset). The pool discards it;
  // we must not let the unhandled 'error' event kill the process.
  logger.error({ err }, 'idle pg client error');
});

setInterval(() => {
  dbPoolWaiting.set(pool.waitingCount);
}, 1000).unref();

/** Pooled query. Never use this for anything that spans multiple statements. */
export async function query<R extends QueryResultRow = QueryResultRow>(
  text: string,
  params: readonly unknown[] = [],
  label = 'query',
): Promise<pg.QueryResult<R>> {
  const end = dbQueryDuration.startTimer({ label });
  try {
    return await pool.query<R>(text, params as unknown[]);
  } finally {
    end();
  }
}

/**
 * Runs `fn` inside an explicit BEGIN/COMMIT on a *dedicated* client checked out
 * of the pool.
 *
 * This is not a stylistic preference. `pool.query()` picks an arbitrary idle
 * connection per call, so `pool.query('BEGIN')` followed by `pool.query('SELECT
 * ... FOR UPDATE')` can easily land on two different backends: the lock is taken
 * on one connection and released immediately by an implicit commit, while the
 * UPDATE runs unprotected on another. SKIP LOCKED then guarantees nothing at
 * all. Checking the client out pins every statement to one backend, which is
 * what makes the row lock span the whole claim.
 */
export async function withTransaction<T>(
  fn: (client: PoolClient) => Promise<T>,
  label = 'tx',
): Promise<T> {
  const end = dbQueryDuration.startTimer({ label });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackErr) {
      // The connection is already broken; releasing with an error destroys it
      // rather than returning a poisoned client to the pool.
      logger.warn({ err: rollbackErr }, 'rollback failed, destroying client');
      client.release(rollbackErr as Error);
      throw err;
    }
    throw err;
  } finally {
    end();
    // release() is idempotent-safe here: if the catch above already released
    // with an error, pg ignores the second call.
    try {
      client.release();
    } catch {
      /* already released */
    }
  }
}

/**
 * A standalone (non-pooled) connection. Used for the leader's advisory lock and
 * for Redis-independent LISTEN/NOTIFY style long-lived sessions: the lock must
 * die with the connection, and a pooled client would be recycled underneath it.
 */
export function createDedicatedClient(name: string): pg.Client {
  return new Client({
    connectionString: config.databaseUrl,
    application_name: `job-scheduler:${config.nodeId}:${name}`,
    // Deliberately no statement_timeout: this session holds a lock, not queries.
    keepAlive: true,
  });
}

export async function closePool(): Promise<void> {
  await pool.end();
}
