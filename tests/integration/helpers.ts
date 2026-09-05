import { randomUUID } from 'node:crypto';
import { pool, query } from '../../src/db/pool.js';
import { runMigrations, waitForDatabase } from '../../src/db/migrate.js';
import { redis } from '../../src/redis/client.js';
import type { Job } from '../../src/domain/job.js';

/**
 * Integration tests run against the real Postgres and Redis from
 * docker-compose.test.yml. No mocks: the behaviour under test - SKIP LOCKED,
 * advisory locks, ON CONFLICT, transaction visibility - is precisely the
 * behaviour a mock would have to invent.
 */
export async function setupDatabase(): Promise<void> {
  await waitForDatabase(30_000);
  await runMigrations();
}

export async function resetDatabase(): Promise<void> {
  await query('TRUNCATE jobs, job_events, workers, circuit_breakers, cron_jobs, tenants CASCADE');
  const keys = await redis.keys('breaker:*');
  const limits = await redis.keys('ratelimit:*');
  if (keys.length + limits.length > 0) await redis.del(...keys, ...limits);
}

export async function teardown(): Promise<void> {
  await pool.end().catch(() => undefined);
  await redis.quit().catch(() => undefined);
}

export interface SeedOptions {
  tenant?: string;
  queue?: string;
  jobType?: string;
  priority?: number;
  maxAttempts?: number;
  createdAt?: string;
  scheduledFor?: string;
  count?: number;
}

/** Inserts jobs directly, bypassing the API, so tests control every column. */
export async function seedJobs(options: SeedOptions = {}): Promise<string[]> {
  const count = options.count ?? 1;
  const ids = Array.from({ length: count }, () => randomUUID());
  await query(
    `INSERT INTO tenants (tenant_id) VALUES ($1) ON CONFLICT DO NOTHING`,
    [options.tenant ?? 'acme'],
  );
  await query(
    `INSERT INTO jobs (id, tenant_id, queue_name, job_type, payload, priority, max_attempts,
                       created_at, scheduled_for, state)
     SELECT unnest($1::uuid[]), $2, $3, $4, '{}'::jsonb, $5, $6,
            COALESCE($7::timestamptz, now()), COALESCE($8::timestamptz, now()), 'PENDING'`,
    [
      ids,
      options.tenant ?? 'acme',
      options.queue ?? 'default',
      options.jobType ?? 'noop',
      options.priority ?? 5,
      options.maxAttempts ?? 5,
      options.createdAt ?? null,
      options.scheduledFor ?? null,
    ],
  );
  return ids;
}

export async function registerWorker(id: string, concurrency = 8, queues = ['default']): Promise<void> {
  await query(
    `INSERT INTO workers (id, hostname, pid, queues, max_concurrency, state, last_heartbeat_at)
     VALUES ($1, 'test-host', 1, $2::text[], $3, 'IDLE', now())
     ON CONFLICT (id) DO UPDATE SET last_heartbeat_at = now(), state = 'IDLE',
       max_concurrency = EXCLUDED.max_concurrency, queues = EXCLUDED.queues`,
    [id, queues, concurrency],
  );
}

export async function getJobRow(id: string): Promise<Job> {
  const { rows } = await query<Job>('SELECT * FROM jobs WHERE id = $1', [id]);
  const row = rows[0];
  if (row === undefined) throw new Error(`job ${id} not found`);
  return row;
}

export async function countByState(): Promise<Record<string, number>> {
  const { rows } = await query<{ state: string; n: number }>(
    'SELECT state::text AS state, count(*)::int AS n FROM jobs GROUP BY state',
  );
  return Object.fromEntries(rows.map((r) => [r.state, r.n]));
}

/** Forces a job's lease into the past without waiting for it to expire. */
export async function expireLease(jobId: string): Promise<void> {
  await query(`UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, [jobId]);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Polls `check` until it returns true or the deadline passes. */
export async function waitFor(
  check: () => Promise<boolean>,
  timeoutMs = 20_000,
  intervalMs = 100,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await sleep(intervalMs);
  }
  throw new Error(`condition not met within ${timeoutMs}ms`);
}
