import { query } from '../db/pool.js';
import { queueDepth, workersByState } from '../metrics.js';
import type { JobState } from '../domain/states.js';

export interface QueueStatRow {
  queue_name: string;
  tenant_id: string;
  state: string;
  count: number;
  oldest_wait_seconds: number | null;
}

/**
 * The per-state counter keys, derived from the job states themselves so a new
 * state cannot be added without this failing to compile.
 */
const COUNT_KEYS = [
  'pending',
  'blocked',
  'claimed',
  'running',
  'succeeded',
  'failed',
  'dead',
  'cancelled',
] as const satisfies ReadonlyArray<Lowercase<JobState>>;

type CountKey = (typeof COUNT_KEYS)[number];

function isCountKey(value: string): value is CountKey {
  return (COUNT_KEYS as readonly string[]).includes(value);
}

export type QueueSummary = {
  queue_name: string;
  oldest_pending_seconds: number;
  tenants: Record<string, number>;
} & Record<CountKey, number>;

/**
 * Queue depths, grouped in one pass.
 *
 * Deliberately a single scan-and-group rather than one count per state: with
 * eight states and N tenants the naive version is 8N round trips, and the
 * dashboard calls this every second.
 */
export async function queueStats(): Promise<QueueStatRow[]> {
  const { rows } = await query<QueueStatRow>(
    `SELECT queue_name, tenant_id, state::text AS state, count(*)::int AS count,
            EXTRACT(EPOCH FROM (now() - min(scheduled_for)))::double precision
              AS oldest_wait_seconds
     FROM jobs
     WHERE state NOT IN ('SUCCEEDED', 'CANCELLED')
        OR completed_at > now() - interval '5 minutes'
     GROUP BY queue_name, tenant_id, state`,
    [],
    'queue_stats',
  );

  queueDepth.reset();
  for (const row of rows) {
    queueDepth.set({ queue: row.queue_name, tenant: row.tenant_id, state: row.state }, row.count);
  }
  return rows;
}

export function summariseQueues(rows: QueueStatRow[]): QueueSummary[] {
  const byQueue = new Map<string, QueueSummary>();
  for (const row of rows) {
    let summary = byQueue.get(row.queue_name);
    if (summary === undefined) {
      summary = {
        queue_name: row.queue_name,
        pending: 0,
        blocked: 0,
        claimed: 0,
        running: 0,
        succeeded: 0,
        failed: 0,
        dead: 0,
        cancelled: 0,
        oldest_pending_seconds: 0,
        tenants: {},
      };
      byQueue.set(row.queue_name, summary);
    }
    const key = row.state.toLowerCase();
    if (isCountKey(key)) summary[key] += row.count;
    summary.tenants[row.tenant_id] = (summary.tenants[row.tenant_id] ?? 0) + row.count;
    if (row.state === 'PENDING' && row.oldest_wait_seconds !== null) {
      summary.oldest_pending_seconds = Math.max(
        summary.oldest_pending_seconds,
        row.oldest_wait_seconds,
      );
    }
  }
  return [...byQueue.values()].sort((a, b) => a.queue_name.localeCompare(b.queue_name));
}

export interface WorkerView {
  id: string;
  hostname: string;
  pid: number;
  queues: string[];
  max_concurrency: number;
  state: string;
  current_job_id: string | null;
  inflight_count: number;
  connected_node: string | null;
  seconds_since_heartbeat: number;
}

export async function workerStats(): Promise<WorkerView[]> {
  const { rows } = await query<WorkerView>(
    `SELECT id, hostname, pid, queues, max_concurrency, state::text AS state,
            current_job_id, inflight_count, connected_node,
            EXTRACT(EPOCH FROM (now() - last_heartbeat_at))::double precision
              AS seconds_since_heartbeat
     FROM workers
     ORDER BY registered_at DESC
     LIMIT 500`,
    [],
    'worker_stats',
  );

  workersByState.reset();
  const counts = new Map<string, number>();
  for (const row of rows) counts.set(row.state, (counts.get(row.state) ?? 0) + 1);
  for (const [state, n] of counts) workersByState.set({ state }, n);
  return rows;
}

export interface RecentFailure {
  id: string;
  job_type: string;
  queue_name: string;
  tenant_id: string;
  state: string;
  attempt_count: number;
  last_error: string | null;
  updated_at: Date;
}

export async function recentFailures(limit = 20): Promise<RecentFailure[]> {
  const { rows } = await query<RecentFailure>(
    `SELECT id, job_type, queue_name, tenant_id, state::text AS state, attempt_count,
            last_error, updated_at
     FROM jobs
     WHERE last_error IS NOT NULL
     ORDER BY updated_at DESC
     LIMIT $1`,
    [limit],
    'recent_failures',
  );
  return rows;
}

/** Jobs/sec over a short trailing window, straight from the audit log. */
export async function throughput(windowSeconds = 10): Promise<{ succeeded: number; failed: number; per_second: number }> {
  const { rows } = await query<{ succeeded: number; failed: number }>(
    `SELECT
       count(*) FILTER (WHERE to_state = 'SUCCEEDED')::int AS succeeded,
       count(*) FILTER (WHERE to_state = 'DEAD')::int AS failed
     FROM job_events
     WHERE at > now() - make_interval(secs => $1::double precision)`,
    [windowSeconds],
    'throughput',
  );
  const row = rows[0] ?? { succeeded: 0, failed: 0 };
  return { ...row, per_second: (row.succeeded + row.failed) / windowSeconds };
}

export async function listDeadLetters(limit = 100, offset = 0): Promise<unknown[]> {
  const { rows } = await query(
    `SELECT id, tenant_id, queue_name, job_type, attempt_count, max_attempts, last_error,
            completed_at
     FROM jobs WHERE state = 'DEAD'
     ORDER BY completed_at DESC NULLS LAST
     LIMIT $1 OFFSET $2`,
    [limit, offset],
    'dead_letters',
  );
  return rows;
}
