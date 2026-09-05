import { query, withTransaction } from '../db/pool.js';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { backoffIntervalSql } from '../domain/backoff.js';
import { JobState } from '../domain/states.js';
import { jobsCompleted, jobDuration, jobsRetried } from '../metrics.js';
import { cascadeCancel } from './jobStore.js';
import { publishEvent, publishWorkAvailable } from './events.js';
import { recordOutcome } from './breaker.js';

/**
 * Every terminal write is guarded by `claimed_by = $worker AND state IN
 * ('CLAIMED','RUNNING')`.
 *
 * That predicate is the fencing token. Consider the worst case: worker A stalls
 * (GC pause, blocked syscall), its lease expires, the reaper returns the job to
 * PENDING and clears claimed_by, worker B claims and finishes it. Worker A then
 * wakes up and reports success for the same job. Its UPDATE matches zero rows,
 * because claimed_by is no longer A - so the late report is dropped instead of
 * overwriting B's result or double-counting the completion. This is what makes
 * "exactly once" hold at the *effect* level even though the job body may have
 * been executed twice by a zombie; the queue never accepts two terminal writes.
 */
export type LifecycleOutcome = 'ok' | 'not_owner';

interface TerminalRow {
  id: string;
  job_type: string;
  queue_name: string;
  tenant_id: string;
  state: string;
  attempt_count: number;
  duration_ms: number | null;
  unblocked_queues?: string[] | null;
}

/** Worker acknowledges it has begun executing: CLAIMED -> RUNNING. */
export async function markRunning(jobId: string, workerId: string): Promise<LifecycleOutcome> {
  const { rowCount } = await query(
    `WITH upd AS (
       UPDATE jobs SET state = 'RUNNING', updated_at = now()
       WHERE id = $1 AND claimed_by = $2 AND state = 'CLAIMED'
       RETURNING id
     ), ev AS (
       INSERT INTO job_events (job_id, from_state, to_state, worker_id, node_id)
       SELECT id, 'CLAIMED', 'RUNNING', $2, $3 FROM upd
     )
     SELECT id FROM upd`,
    [jobId, workerId, config.nodeId],
    'mark_running',
  );
  return rowCount === 1 ? 'ok' : 'not_owner';
}

/**
 * Success is the hottest write in the system, so it is one statement rather than
 * a four-round-trip transaction (BEGIN, update, unblock, COMMIT). A single
 * statement is atomic on its own, so nothing is given up by dropping the
 * explicit transaction.
 *
 * The subtlety is in `unblocked`: CTEs see the snapshot as it was before the
 * statement began, so the job we are completing right now still looks RUNNING to
 * the dependency check. Excluding it explicitly (`p.id <> $1`) and gating the
 * whole CTE on `EXISTS (SELECT 1 FROM upd)` says exactly what we mean - "treat
 * this job as succeeded, but only if it really did succeed".
 */
export async function completeJob(
  jobId: string,
  workerId: string,
  result: unknown,
): Promise<LifecycleOutcome> {
  const { rows } = await query<TerminalRow>(
    `WITH upd AS (
       UPDATE jobs
       SET state = 'SUCCEEDED',
           attempt_count = attempt_count + 1,
           completed_at = now(),
           result = $3::jsonb,
           claimed_by = NULL,
           lease_expires_at = NULL,
           last_error = NULL,
           updated_at = now()
       WHERE id = $1 AND claimed_by = $2 AND state IN ('CLAIMED', 'RUNNING')
       RETURNING id, job_type, queue_name, tenant_id, state, attempt_count,
                 (EXTRACT(EPOCH FROM (now() - claimed_at)) * 1000)::double precision AS duration_ms
     ),
     unblocked AS (
       UPDATE jobs d
       SET state = 'PENDING', updated_at = now(),
           scheduled_for = GREATEST(d.scheduled_for, now())
       WHERE EXISTS (SELECT 1 FROM upd)
         AND d.state = 'BLOCKED'
         AND d.depends_on @> ARRAY[$1]::uuid[]
         AND NOT EXISTS (
           SELECT 1 FROM jobs p
           WHERE p.id = ANY(d.depends_on) AND p.id <> $1 AND p.state <> 'SUCCEEDED'
         )
       RETURNING d.id, d.queue_name
     ),
     ev AS (
       INSERT INTO job_events (job_id, from_state, to_state, worker_id, node_id)
       SELECT id, 'RUNNING', 'SUCCEEDED', $2, $4 FROM upd
     ),
     ev2 AS (
       INSERT INTO job_events (job_id, from_state, to_state, node_id, detail)
       SELECT id, 'BLOCKED', 'PENDING', $4, 'all dependencies succeeded' FROM unblocked
     )
     SELECT upd.*, (SELECT array_agg(queue_name) FROM unblocked) AS unblocked_queues
     FROM upd`,
    [jobId, workerId, JSON.stringify(result ?? null), config.nodeId],
    'complete',
  );

  const outcome = rows[0] ?? null;
  if (outcome !== null) {
    // Fan-out: a completion can release many dependents at once, possibly onto
    // queues no worker is currently polling.
    for (const queue of new Set(outcome.unblocked_queues ?? [])) publishWorkAvailable(queue);
  }

  if (outcome === null) {
    logger.warn({ jobId, workerId }, 'completion rejected: worker no longer owns the job');
    return 'not_owner';
  }

  jobsCompleted.inc({ queue: outcome.queue_name, job_type: outcome.job_type, state: 'SUCCEEDED' });
  if (outcome.duration_ms !== null) {
    jobDuration.observe({ job_type: outcome.job_type, state: 'SUCCEEDED' }, outcome.duration_ms);
  }
  void recordOutcome(outcome.job_type, true);
  return 'ok';
}

export async function failJob(
  jobId: string,
  workerId: string,
  errorMessage: string,
): Promise<LifecycleOutcome> {
  // Full jitter is sampled here rather than with Postgres random() so the
  // distribution is the one unit-tested in domain/backoff.ts and can be stubbed.
  const jitter = Math.random();
  const interval = backoffIntervalSql(
    'attempt_count + 1',
    '$5::double precision',
    '$6::double precision',
    '$4::double precision',
  );

  const outcome = await withTransaction(async (client) => {
    const { rows } = await client.query<TerminalRow>(
      `WITH upd AS (
         UPDATE jobs
         SET attempt_count = attempt_count + 1,
             state = CASE WHEN attempt_count + 1 >= max_attempts THEN 'DEAD'::job_state
                          ELSE 'PENDING'::job_state END,
             scheduled_for = CASE WHEN attempt_count + 1 >= max_attempts THEN scheduled_for
                                  ELSE now() + ${interval} END,
             completed_at = CASE WHEN attempt_count + 1 >= max_attempts THEN now() ELSE NULL END,
             last_error = $3,
             claimed_by = NULL,
             claimed_at = NULL,
             lease_expires_at = NULL,
             updated_at = now()
         WHERE id = $1 AND claimed_by = $2 AND state IN ('CLAIMED', 'RUNNING')
         RETURNING id, job_type, queue_name, tenant_id, state::text AS state, attempt_count,
                   (EXTRACT(EPOCH FROM (now() - claimed_at)) * 1000)::double precision AS duration_ms
       ), ev AS (
         INSERT INTO job_events (job_id, from_state, to_state, worker_id, node_id, detail)
         SELECT id, 'RUNNING', state::job_state, $2, $7, $3 FROM upd
       )
       SELECT * FROM upd`,
      [
        jobId,
        workerId,
        errorMessage.slice(0, 4000),
        jitter,
        config.backoffBaseMs,
        config.backoffCapMs,
        config.nodeId,
      ],
    );

    const row = rows[0];
    if (row === undefined) return null;

    // A job that exhausted its budget takes its dependents down with it.
    if (row.state === JobState.DEAD) {
      await cascadeCancel(client, [jobId], 'dependency moved to the dead letter queue');
    }
    return row;
  }, 'fail');

  if (outcome === null) {
    logger.warn({ jobId, workerId }, 'failure rejected: worker no longer owns the job');
    return 'not_owner';
  }

  const now = new Date().toISOString();
  if (outcome.state === JobState.DEAD) {
    jobsCompleted.inc({ queue: outcome.queue_name, job_type: outcome.job_type, state: 'DEAD' });
    publishEvent({
      type: 'job_dead',
      job_id: jobId,
      job_type: outcome.job_type,
      queue: outcome.queue_name,
      tenant: outcome.tenant_id,
      error: errorMessage,
      at: now,
    });
  } else {
    jobsRetried.inc({ job_type: outcome.job_type });
    publishEvent({
      type: 'job_failed',
      job_id: jobId,
      job_type: outcome.job_type,
      queue: outcome.queue_name,
      tenant: outcome.tenant_id,
      attempt: outcome.attempt_count,
      error: errorMessage,
      at: now,
    });
  }
  if (outcome.duration_ms !== null) {
    jobDuration.observe({ job_type: outcome.job_type, state: outcome.state }, outcome.duration_ms);
  }
  void recordOutcome(outcome.job_type, false);
  return 'ok';
}

/**
 * Heartbeat lease extension.
 *
 * A worker pushes the expiry of everything it holds forward on every beat. The
 * lease is therefore only ever *shorter* than the worker's liveness gap, and a
 * long-running job never gets stolen mid-flight just for being slow - the
 * reaper only ever sees leases belonging to workers that stopped beating.
 */
export async function extendLeases(workerId: string, jobIds: string[]): Promise<string[]> {
  if (jobIds.length === 0) return [];
  const { rows } = await query<{ id: string }>(
    `UPDATE jobs
     SET lease_expires_at = now() + make_interval(secs => $3::double precision),
         updated_at = now()
     WHERE id = ANY($1::uuid[]) AND claimed_by = $2 AND state IN ('CLAIMED', 'RUNNING')
     RETURNING id`,
    [jobIds, workerId, config.leaseDurationSeconds],
    'extend_lease',
  );

  // Anything the worker still thinks it owns but we did not extend has been
  // reclaimed underneath it; the worker is told to abandon those.
  const extended = new Set(rows.map((r) => r.id));
  return jobIds.filter((id) => !extended.has(id));
}
