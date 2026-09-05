import { withTransaction } from '../db/pool.js';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { backoffIntervalSql } from '../domain/backoff.js';
import { jobsReclaimed } from '../metrics.js';
import { cascadeCancel } from './jobStore.js';
import { publishEvent, publishWorkAvailable } from './events.js';

/**
 * ============================================================================
 * The two reclaim paths, and why both must exist
 * ============================================================================
 *
 * A job held by a worker that will never finish it has to come back. There are
 * two independent mechanisms, and neither one subsumes the other:
 *
 * PATH 1 - IMMEDIATE (reclaimWorkerJobs).
 *   Triggered by the control plane the instant a worker's WebSocket closes, or
 *   when it misses three consecutive 5s heartbeats and we hang up on it. The
 *   node that owned the socket knows, right now, that the worker is gone, so it
 *   returns everything that worker held in a single statement. Recovery latency
 *   is the socket close itself: milliseconds for a clean close or a SIGKILL
 *   (the OS sends FIN/RST when the process dies), ~15s worst case for a silently
 *   partitioned worker. Without this path, every crash would cost a full lease
 *   duration (30s by default) of dead time per job.
 *
 * PATH 2 - LEASE EXPIRY (reclaimExpiredLeases, leader-only reaper).
 *   A sweep for `lease_expires_at < now()`. This is the backstop for everything
 *   path 1 structurally cannot see:
 *     - the *scheduler* node holding that socket was itself SIGKILLed, so the
 *       close event was never processed by anyone;
 *     - a network partition where the socket stays open (no FIN) while the
 *       worker is unreachable;
 *     - a worker that is alive and connected but wedged - blocked on a syscall,
 *       in a long GC pause - so it stops heartbeating without closing;
 *     - a job orphaned by a crash between the claim COMMIT and the dispatch.
 *   It is slower but it depends on nothing except Postgres and the clock, which
 *   is exactly what a backstop must depend on.
 *
 * Path 1 is an optimisation over path 2; path 2 is the correctness guarantee.
 * Both funnel through the same SQL shape, so a job cannot be treated
 * differently depending on which one caught it, and both are safe to run
 * concurrently: SKIP LOCKED plus the `state IN ('CLAIMED','RUNNING')` predicate
 * means whichever gets there first wins and the other sees zero rows.
 */

export type ReclaimPath = 'socket_close' | 'heartbeat_loss' | 'lease_expiry' | 'shutdown';

export interface ReclaimResult {
  requeued: string[];
  dead: string[];
}

interface ReclaimedRow {
  id: string;
  state: string;
  queue_name: string;
  job_type: string;
  attempt_count: number;
}

/**
 * Shared body. `selector` restricts which in-flight rows are candidates; it is
 * always evaluated inside FOR UPDATE SKIP LOCKED so two reclaimers never fight.
 *
 * attempt_count is incremented here: an attempt that ended without a verdict
 * still consumed a slot of the retry budget. A job whose budget is now spent
 * goes straight to DEAD instead of being handed to another worker to die again.
 */
async function reclaim(
  selector: { sql: string; params: readonly unknown[] },
  path: ReclaimPath,
  reason: string,
  limit: number,
): Promise<ReclaimResult> {
  const base = selector.params.length;
  const p = (n: number): string => `$${base + n}`;
  const interval = backoffIntervalSql(
    'attempt_count + 1',
    `${p(3)}::double precision`,
    `${p(4)}::double precision`,
    `${p(5)}::double precision`,
  );

  const rows = await withTransaction(async (client) => {
    const res = await client.query<ReclaimedRow>(
      `WITH victims AS (
         SELECT id FROM jobs
         WHERE state IN ('CLAIMED', 'RUNNING') AND ${selector.sql}
         ORDER BY lease_expires_at ASC NULLS FIRST
         LIMIT ${p(1)}::int
         FOR UPDATE SKIP LOCKED
       ),
       upd AS (
         UPDATE jobs
         SET attempt_count = attempt_count + 1,
             state = CASE WHEN attempt_count + 1 >= max_attempts THEN 'DEAD'::job_state
                          ELSE 'PENDING'::job_state END,
             scheduled_for = CASE WHEN attempt_count + 1 >= max_attempts THEN scheduled_for
                                  ELSE now() + ${interval} END,
             completed_at = CASE WHEN attempt_count + 1 >= max_attempts THEN now() ELSE NULL END,
             last_error = ${p(2)},
             claimed_by = NULL,
             claimed_at = NULL,
             lease_expires_at = NULL,
             updated_at = now()
         FROM victims
         WHERE jobs.id = victims.id
         RETURNING jobs.id, jobs.state::text AS state, jobs.queue_name, jobs.job_type,
                   jobs.attempt_count
       ), ev AS (
         INSERT INTO job_events (job_id, from_state, to_state, node_id, detail)
         SELECT id, NULL, state::job_state, ${p(6)}, ${p(2)} FROM upd
       )
       SELECT * FROM upd`,
      [
        ...selector.params,
        limit,
        reason,
        config.backoffBaseMs,
        config.backoffCapMs,
        Math.random(),
        config.nodeId,
      ],
    );

    const dead = res.rows.filter((r) => r.state === 'DEAD').map((r) => r.id);
    if (dead.length > 0) {
      await cascadeCancel(client, dead, 'dependency died during reclaim');
    }
    return res.rows;
  }, `reclaim_${path}`);

  const requeued = rows.filter((r) => r.state === 'PENDING').map((r) => r.id);
  const dead = rows.filter((r) => r.state === 'DEAD').map((r) => r.id);

  if (requeued.length > 0) jobsReclaimed.inc({ path, outcome: 'requeued' }, requeued.length);
  if (dead.length > 0) jobsReclaimed.inc({ path, outcome: 'dead' }, dead.length);
  for (const queue of new Set(rows.filter((r) => r.state === 'PENDING').map((r) => r.queue_name))) {
    publishWorkAvailable(queue);
  }
  if (rows.length > 0) {
    logger.warn({ path, requeued: requeued.length, dead: dead.length, reason }, 'reclaimed in-flight jobs');
  }

  return { requeued, dead };
}

/** PATH 1: everything a specific worker holds, returned immediately. */
export async function reclaimWorkerJobs(
  workerId: string,
  path: Extract<ReclaimPath, 'socket_close' | 'heartbeat_loss' | 'shutdown'>,
  limit = 1000,
): Promise<ReclaimResult> {
  const result = await reclaim(
    { sql: 'claimed_by = $1', params: [workerId] },
    path,
    `worker ${workerId} lost (${path})`,
    limit,
  );
  const total = result.requeued.length + result.dead.length;
  if (total > 0) {
    publishEvent({
      type: 'worker_dead',
      worker_id: workerId,
      reclaimed: total,
      path,
      at: new Date().toISOString(),
    });
  }
  return result;
}

/** PATH 2: the leader's backstop sweep for expired leases. */
export async function reclaimExpiredLeases(limit = 500): Promise<ReclaimResult> {
  return reclaim(
    { sql: 'lease_expires_at < now()' , params: [] },
    'lease_expiry',
    'lease expired before the worker reported a result',
    limit,
  );
}

/**
 * Marks workers DEAD once they have missed `workerMissedBeats` beats, and
 * reclaims their work. Runs on the leader so a worker whose *scheduler* died
 * (nobody left to observe its socket close) is still cleaned up.
 */
export async function reapDeadWorkers(): Promise<string[]> {
  const staleSeconds = (config.workerHeartbeatIntervalMs * config.workerMissedBeats) / 1000;
  const { rows } = await withTransaction(
    (client) =>
      client
        .query<{ id: string }>(
          `UPDATE workers
           SET state = 'DEAD', current_job_id = NULL, inflight_count = 0
           WHERE state <> 'DEAD'
             AND last_heartbeat_at < now() - make_interval(secs => $1::double precision)
           RETURNING id`,
          [staleSeconds],
        )
        .then((r) => r),
    'reap_workers',
  );

  for (const row of rows) {
    await reclaimWorkerJobs(row.id, 'heartbeat_loss');
  }
  return rows.map((r) => r.id);
}
