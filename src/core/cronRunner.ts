import { withTransaction } from '../db/pool.js';
import { logger } from '../logger.js';
import { nextRun } from '../domain/cron.js';
import { publishWorkAvailable } from './events.js';

interface DueCron {
  id: string;
  name: string;
  schedule: string;
  tenant_id: string;
  queue_name: string;
  job_type: string;
  payload: Record<string, unknown>;
  priority: number;
  next_run_at: Date;
}

/**
 * Cron dispatch. Leader-only, but still written so that a second leader briefly
 * co-existing during failover cannot double-fire: the due rows are taken FOR
 * UPDATE SKIP LOCKED and the enqueue carries an idempotency key derived from
 * (cron name, fire time), so a duplicate attempt collapses onto the same row.
 */
export async function runDueCrons(now: Date = new Date()): Promise<number> {
  return withTransaction(async (client) => {
    const { rows } = await client.query<DueCron>(
      `SELECT id, name, schedule, tenant_id, queue_name, job_type, payload, priority, next_run_at
       FROM cron_jobs
       WHERE enabled AND next_run_at <= $1
       ORDER BY next_run_at
       LIMIT 100
       FOR UPDATE SKIP LOCKED`,
      [now],
    );
    if (rows.length === 0) return 0;

    for (const cron of rows) {
      const fireAt = cron.next_run_at;
      const key = `cron:${cron.name}:${fireAt.toISOString()}`;
      await client.query(
        `INSERT INTO jobs (tenant_id, queue_name, job_type, payload, priority, idempotency_key, scheduled_for)
         VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
         ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
        [
          cron.tenant_id,
          cron.queue_name,
          cron.job_type,
          JSON.stringify(cron.payload ?? {}),
          cron.priority,
          key,
          fireAt,
        ],
      );

      let next: Date;
      try {
        next = nextRun(cron.schedule, now);
      } catch (err) {
        logger.error({ err, cron: cron.name }, 'disabling cron with an unusable schedule');
        await client.query('UPDATE cron_jobs SET enabled = false WHERE id = $1', [cron.id]);
        continue;
      }
      await client.query(
        'UPDATE cron_jobs SET last_run_at = $2, next_run_at = $3 WHERE id = $1',
        [cron.id, now, next],
      );
      publishWorkAvailable(cron.queue_name);
    }

    logger.info({ fired: rows.length }, 'cron jobs enqueued');
    return rows.length;
  }, 'cron');
}

export async function upsertCron(input: {
  name: string;
  schedule: string;
  tenant_id: string;
  queue_name?: string;
  job_type: string;
  payload?: Record<string, unknown>;
  priority?: number;
}): Promise<void> {
  const next = nextRun(input.schedule);
  await withTransaction(
    (client) =>
      client.query(
        `INSERT INTO cron_jobs (name, schedule, tenant_id, queue_name, job_type, payload, priority, next_run_at)
         VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
         ON CONFLICT (name) DO UPDATE
         SET schedule = EXCLUDED.schedule, tenant_id = EXCLUDED.tenant_id,
             queue_name = EXCLUDED.queue_name, job_type = EXCLUDED.job_type,
             payload = EXCLUDED.payload, priority = EXCLUDED.priority,
             next_run_at = EXCLUDED.next_run_at, enabled = true`,
        [
          input.name,
          input.schedule,
          input.tenant_id,
          input.queue_name ?? 'default',
          input.job_type,
          JSON.stringify(input.payload ?? {}),
          input.priority ?? 5,
          next,
        ],
      ),
    'cron_upsert',
  );
}
