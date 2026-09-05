import { randomUUID } from 'node:crypto';
import type { PoolClient } from '../db/pool.js';
import { query, withTransaction } from '../db/pool.js';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { CycleError, findCycle, topoSort, type DepGraph } from '../domain/dag.js';
import { JobState } from '../domain/states.js';
import type { Job, Priority } from '../domain/job.js';
import { jobsEnqueued } from '../metrics.js';
import { publishWorkAvailable } from './events.js';

export class ValidationError extends Error {
  constructor(
    message: string,
    readonly details: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = 'ValidationError';
  }
}

export class NotFoundError extends Error {
  constructor(readonly jobId: string) {
    super(`job ${jobId} not found`);
    this.name = 'NotFoundError';
  }
}

export class ConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConflictError';
  }
}

export interface SubmitRequest {
  /** Local name for this job inside a batch, so siblings can depend on it. */
  ref?: string;
  id?: string;
  tenant_id: string;
  queue_name: string;
  job_type: string;
  payload: Record<string, unknown>;
  priority: Priority;
  max_attempts?: number;
  scheduled_for?: string;
  idempotency_key?: string;
  /** Entries are either a sibling's `ref` or an existing job uuid. */
  depends_on?: string[];
}

export interface SubmitResult {
  jobs: Job[];
  /** Ids returned from an existing row because the idempotency key matched. */
  deduplicated: string[];
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Submits one job or a batch that may form a DAG among itself.
 *
 * Cycle detection runs over the *combined* graph: the batch's own edges plus the
 * dependency edges of every already-persisted job the batch points at, pulled
 * transitively with a recursive CTE. A single job depending only on existing
 * jobs cannot create a cycle, but a batch can (a -> b -> a), and so can a client
 * supplying explicit ids that close a loop through persisted rows - which is
 * exactly why the check looks at both halves rather than just the batch.
 */
export async function submitJobs(requests: SubmitRequest[]): Promise<SubmitResult> {
  if (requests.length === 0) return { jobs: [], deduplicated: [] };

  // ---- 1. assign ids and resolve local refs -------------------------------
  const refToId = new Map<string, string>();
  const ids: string[] = [];
  for (const req of requests) {
    const id = req.id ?? randomUUID();
    if (req.id !== undefined && !UUID_RE.test(req.id)) {
      throw new ValidationError(`id must be a uuid: ${req.id}`);
    }
    ids.push(id);
    const ref = req.ref ?? id;
    if (refToId.has(ref)) throw new ValidationError(`duplicate ref in batch: ${ref}`);
    refToId.set(ref, id);
  }

  const resolvedDeps: string[][] = requests.map((req, i) => {
    const own = ids[i] as string;
    const deps = (req.depends_on ?? []).map((d) => {
      const mapped = refToId.get(d);
      if (mapped !== undefined) return mapped;
      if (!UUID_RE.test(d)) {
        throw new ValidationError(`depends_on entry "${d}" is neither a batch ref nor a uuid`);
      }
      return d;
    });
    if (deps.includes(own)) throw new CycleError([own, own]);
    return [...new Set(deps)];
  });

  const external = new Set<string>();
  const batchIds = new Set(ids);
  for (const deps of resolvedDeps) {
    for (const d of deps) if (!batchIds.has(d)) external.add(d);
  }

  return withTransaction(async (client) => {
    // ---- 2. pull the persisted half of the graph --------------------------
    const graph = new Map<string, string[]>();
    requests.forEach((_, i) => graph.set(ids[i] as string, resolvedDeps[i] as string[]));

    if (external.size > 0) {
      const { rows } = await client.query<{ id: string; depends_on: string[] }>(
        `WITH RECURSIVE anc AS (
           SELECT id, depends_on FROM jobs WHERE id = ANY($1::uuid[])
           UNION
           SELECT j.id, j.depends_on FROM jobs j JOIN anc a ON j.id = ANY(a.depends_on)
         )
         SELECT id, depends_on FROM anc`,
        [[...external]],
      );
      const known = new Set(rows.map((r) => r.id));
      for (const dep of external) {
        if (!known.has(dep)) {
          throw new ValidationError(`depends_on references unknown job ${dep}`, { job_id: dep });
        }
      }
      for (const row of rows) {
        if (!graph.has(row.id)) graph.set(row.id, row.depends_on);
      }
    }

    // ---- 3. cycle detection ----------------------------------------------
    const cycle = findCycle(graph);
    if (cycle !== null) throw new CycleError(cycle);

    // Insert dependencies before dependents so a FK-less reader never observes a
    // BLOCKED job whose dependency row does not exist yet.
    const order = topoSort(graph).filter((id) => batchIds.has(id));
    const indexById = new Map(ids.map((id, i) => [id, i]));
    const ordered = order.map((id) => indexById.get(id) as number);

    // ---- 4. insert --------------------------------------------------------
    const tenants = [...new Set(requests.map((r) => r.tenant_id))];
    await client.query(
      `INSERT INTO tenants (tenant_id) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING`,
      [tenants],
    );

    const cols = {
      id: [] as string[],
      tenant: [] as string[],
      queue: [] as string[],
      type: [] as string[],
      payload: [] as string[],
      priority: [] as number[],
      maxAttempts: [] as number[],
      scheduledFor: [] as (string | null)[],
      idempotency: [] as (string | null)[],
      dependsOn: [] as string[],
      state: [] as string[],
    };

    for (const i of ordered) {
      const req = requests[i] as SubmitRequest;
      const deps = resolvedDeps[i] as string[];
      cols.id.push(ids[i] as string);
      cols.tenant.push(req.tenant_id);
      cols.queue.push(req.queue_name);
      cols.type.push(req.job_type);
      cols.payload.push(JSON.stringify(req.payload ?? {}));
      cols.priority.push(req.priority);
      cols.maxAttempts.push(req.max_attempts ?? config.defaultMaxAttempts);
      cols.scheduledFor.push(req.scheduled_for ?? null);
      cols.idempotency.push(req.idempotency_key ?? null);
      // Postgres array literal for uuid[]; empty deps become '{}'.
      cols.dependsOn.push(`{${deps.join(',')}}`);
      cols.state.push(deps.length > 0 ? JobState.BLOCKED : JobState.PENDING);
    }

    // depends_on is uuid[] *per row*, so it cannot travel through unnest() as a
    // uuid[][] - Postgres flattens multidimensional arrays. Each row's array is
    // shipped as its literal text ('{a,b}') and cast back inside the SELECT.
    let inserted;
    try {
      inserted = await client.query<Job>(
        `INSERT INTO jobs (
           id, tenant_id, queue_name, job_type, payload, priority, max_attempts,
           scheduled_for, idempotency_key, depends_on, state
         )
         SELECT t.id, t.tenant_id, t.queue_name, t.job_type, t.payload, t.priority,
                t.max_attempts, COALESCE(t.scheduled_for, now()), t.idempotency_key,
                t.depends_on::uuid[], t.state
         FROM unnest(
           $1::uuid[], $2::text[], $3::text[], $4::text[], $5::jsonb[], $6::smallint[],
           $7::int[], $8::timestamptz[], $9::text[], $10::text[], $11::job_state[]
         ) AS t(id, tenant_id, queue_name, job_type, payload, priority, max_attempts,
                scheduled_for, idempotency_key, depends_on, state)
         ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL
         DO NOTHING
         RETURNING *`,
        [
          cols.id,
          cols.tenant,
          cols.queue,
          cols.type,
          cols.payload,
          cols.priority,
          cols.maxAttempts,
          cols.scheduledFor,
          cols.idempotency,
          cols.dependsOn,
          cols.state,
        ],
      );
    } catch (err) {
      // Only one conflict target may be inferred, so a collision on the primary
      // key (a caller-supplied id that already exists) still raises 23505.
      if ((err as { code?: string }).code === '23505') {
        throw new ConflictError('a job with that id already exists');
      }
      throw err;
    }

    // ---- 5. idempotency: re-select whatever DO NOTHING swallowed ----------
    const insertedIds = new Set(inserted.rows.map((r) => r.id));
    const dedupTenants: string[] = [];
    const dedupKeys: string[] = [];
    for (let n = 0; n < cols.id.length; n += 1) {
      if (insertedIds.has(cols.id[n] as string)) continue;
      const key = cols.idempotency[n];
      if (key === null || key === undefined) {
        // No idempotency key, yet the row did not land: should be unreachable.
        throw new ConflictError('job insert was silently dropped');
      }
      dedupTenants.push(cols.tenant[n] as string);
      dedupKeys.push(key);
    }

    const deduplicated: string[] = [];
    const jobs: Job[] = [...inserted.rows];
    if (dedupKeys.length > 0) {
      const existing = await client.query<Job>(
        `SELECT j.* FROM jobs j
         JOIN unnest($1::text[], $2::text[]) AS k(tenant_id, idempotency_key)
           ON k.tenant_id = j.tenant_id AND k.idempotency_key = j.idempotency_key`,
        [dedupTenants, dedupKeys],
      );
      jobs.push(...existing.rows);
      deduplicated.push(...existing.rows.map((r) => r.id));
    }

    // ---- 6. resolve dependencies that are already settled -----------------
    if (cols.state.includes(JobState.BLOCKED)) {
      const doomed = await resolveNewlyBlocked(client, cols.id);
      // A job cancelled because its dependency already failed must in turn
      // cancel anything in this batch that was waiting on *it*.
      await cascadeCancel(client, doomed, 'upstream dependency failed');
    }

    return { jobs, deduplicated };
  }, 'submit');
}

/**
 * A submitted BLOCKED job may already be runnable (all deps SUCCEEDED) or already
 * doomed (a dep is DEAD/CANCELLED). Settle both cases at insert time so no job
 * waits for an event that has already happened.
 */
async function resolveNewlyBlocked(client: PoolClient, ids: string[]): Promise<string[]> {
  const cancelled = await client.query<{ id: string }>(
    `WITH doomed AS (
       UPDATE jobs d
       SET state = 'CANCELLED', completed_at = now(), updated_at = now(),
           last_error = 'dependency reached a terminal failure state'
       WHERE d.id = ANY($1::uuid[]) AND d.state = 'BLOCKED'
         AND EXISTS (
           SELECT 1 FROM jobs p
           WHERE p.id = ANY(d.depends_on) AND p.state IN ('DEAD', 'CANCELLED')
         )
       RETURNING d.id
     ), ev AS (
       INSERT INTO job_events (job_id, from_state, to_state, node_id, detail)
       SELECT id, 'BLOCKED', 'CANCELLED', $2, 'dependency failed at submit' FROM doomed
     )
     SELECT id FROM doomed`,
    [ids, config.nodeId],
  );

  await client.query(
    `WITH ready AS (
       UPDATE jobs d
       SET state = 'PENDING', updated_at = now()
       WHERE d.id = ANY($1::uuid[]) AND d.state = 'BLOCKED'
         AND NOT EXISTS (
           SELECT 1 FROM jobs p
           WHERE p.id = ANY(d.depends_on) AND p.state <> 'SUCCEEDED'
         )
       RETURNING d.id
     )
     INSERT INTO job_events (job_id, from_state, to_state, node_id, detail)
     SELECT id, 'BLOCKED', 'PENDING', $2, 'dependencies already satisfied' FROM ready`,
    [ids, config.nodeId],
  );

  return cancelled.rows.map((r) => r.id);
}

/**
 * The hot enqueue path: one job, no dependencies, done in a single statement.
 *
 * The general path costs five round trips (BEGIN, tenant upsert, insert,
 * dedup select, COMMIT). At the enqueue rates this system targets, round trips
 * *are* the cost - so the common case is collapsed into one statement using
 * data-modifying CTEs. A single statement is atomic on its own, which is why no
 * explicit transaction is needed here.
 *
 * The `existing` CTE is what implements idempotency. It reads the pre-statement
 * snapshot, so it cannot see the row `ins` may just have written: if `ins`
 * produced a row we use that, and if it did not, the conflicting row must have
 * existed before this statement began. Exactly one of the two branches yields a
 * row, with no read-then-write race in between.
 */
async function submitSimpleJob(req: SubmitRequest): Promise<{ job: Job; deduplicated: boolean }> {
  const { rows } = await query<Job & { was_inserted: boolean }>(
    `WITH t AS (
       INSERT INTO tenants (tenant_id) VALUES ($1) ON CONFLICT DO NOTHING
     ),
     ins AS (
       INSERT INTO jobs (id, tenant_id, queue_name, job_type, payload, priority,
                         max_attempts, scheduled_for, idempotency_key, state)
       VALUES (COALESCE($9::uuid, gen_random_uuid()), $1, $2, $3, $4::jsonb, $5, $6,
               COALESCE($7::timestamptz, now()), $8, 'PENDING')
       ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL
       DO NOTHING
       RETURNING *
     ),
     existing AS (
       SELECT j.* FROM jobs j
       WHERE $8::text IS NOT NULL
         AND j.tenant_id = $1 AND j.idempotency_key = $8
         AND NOT EXISTS (SELECT 1 FROM ins)
     ),
     ev AS (
       INSERT INTO job_events (job_id, from_state, to_state, node_id, detail)
       SELECT id, NULL, 'PENDING', $10, 'submitted' FROM ins
     )
     SELECT ins.*, true AS was_inserted FROM ins
     UNION ALL
     SELECT existing.*, false AS was_inserted FROM existing`,
    [
      req.tenant_id,
      req.queue_name,
      req.job_type,
      JSON.stringify(req.payload ?? {}),
      req.priority,
      req.max_attempts ?? config.defaultMaxAttempts,
      req.scheduled_for ?? null,
      req.idempotency_key ?? null,
      req.id ?? null,
      config.nodeId,
    ],
    'submit_fast',
  );

  const row = rows[0];
  if (row === undefined) {
    // Neither branch produced a row: a primary-key collision on a caller
    // supplied id, which ON CONFLICT could not absorb.
    throw new ConflictError('a job with that id already exists');
  }
  const { was_inserted, ...job } = row;
  return { job: job, deduplicated: !was_inserted };
}

/** Convenience wrapper for the single-job API path. */
export async function submitJob(req: SubmitRequest): Promise<{ job: Job; deduplicated: boolean }> {
  const simple = (req.depends_on ?? []).length === 0;
  const { job, deduplicated } = simple
    ? await submitSimpleJob(req)
    : await (async () => {
        const result = await submitJobs([req]);
        const first = result.jobs[0];
        if (first === undefined) throw new Error('submit returned no job');
        return { job: first, deduplicated: result.deduplicated.includes(first.id) };
      })();
  jobsEnqueued.inc({
    tenant: req.tenant_id,
    queue: req.queue_name,
    job_type: req.job_type,
    outcome: deduplicated ? 'duplicate' : 'accepted',
  });
  if (!deduplicated && job.state === JobState.PENDING) {
    // Wake idle dispatchers now instead of waiting for the next poll tick.
    publishWorkAvailable(job.queue_name);
  }
  return { job, deduplicated };
}

export async function getJob(id: string): Promise<Job> {
  const { rows } = await query<Job>('SELECT * FROM jobs WHERE id = $1', [id], 'get_job');
  const job = rows[0];
  if (job === undefined) throw new NotFoundError(id);
  return job;
}

export async function getJobEvents(id: string, limit = 100): Promise<unknown[]> {
  const { rows } = await query(
    'SELECT from_state, to_state, worker_id, node_id, detail, at FROM job_events WHERE job_id = $1 ORDER BY id ASC LIMIT $2',
    [id, limit],
    'job_events',
  );
  return rows;
}

/**
 * Cancels a job that has not started. In-flight jobs are refused rather than
 * silently marked CANCELLED while a worker is still executing them - the queue
 * cannot un-run side effects, so pretending otherwise would be a lie.
 * Cancelling cascades to everything transitively blocked on this job.
 */
export async function cancelJob(id: string): Promise<Job> {
  return withTransaction(async (client) => {
    const { rows } = await client.query<Job>(
      `WITH upd AS (
         UPDATE jobs SET state = 'CANCELLED', completed_at = now(), updated_at = now()
         WHERE id = $1 AND state IN ('PENDING', 'BLOCKED')
         RETURNING *
       ), ev AS (
         INSERT INTO job_events (job_id, from_state, to_state, node_id, detail)
         SELECT id, NULL, 'CANCELLED', $2, 'cancelled via api' FROM upd
       )
       SELECT * FROM upd`,
      [id, config.nodeId],
    );

    const job = rows[0];
    if (job === undefined) {
      const current = await client.query<{ state: string }>('SELECT state FROM jobs WHERE id = $1', [id]);
      const state = current.rows[0]?.state;
      if (state === undefined) throw new NotFoundError(id);
      throw new ConflictError(`job is ${state}; only PENDING or BLOCKED jobs can be cancelled`);
    }

    await cascadeCancel(client, [id], 'upstream job cancelled');
    return job;
  }, 'cancel');
}

/**
 * Cancels everything transitively blocked on `sourceIds`.
 *
 * A recursive CTE walks the reverse edges via the GIN index on depends_on. It is
 * done in SQL rather than JS because the fan-out can be wide and each level would
 * otherwise be a round trip.
 */
export async function cascadeCancel(
  client: PoolClient,
  sourceIds: string[],
  reason: string,
): Promise<string[]> {
  if (sourceIds.length === 0) return [];
  const { rows } = await client.query<{ id: string }>(
    `WITH RECURSIVE affected AS (
       SELECT id FROM jobs WHERE id = ANY($1::uuid[])
       UNION
       SELECT d.id FROM jobs d
       JOIN affected a ON d.depends_on @> ARRAY[a.id]::uuid[]
       WHERE d.state IN ('BLOCKED', 'PENDING')
     ),
     upd AS (
       UPDATE jobs SET state = 'CANCELLED', completed_at = now(), updated_at = now(),
                       last_error = $2
       WHERE id IN (SELECT id FROM affected)
         AND id <> ALL($1::uuid[])
         AND state IN ('BLOCKED', 'PENDING')
       RETURNING id
     ), ev AS (
       INSERT INTO job_events (job_id, from_state, to_state, node_id, detail)
       SELECT id, NULL, 'CANCELLED', $3, $2 FROM upd
     )
     SELECT id FROM upd`,
    [sourceIds, reason, config.nodeId],
  );
  if (rows.length > 0) {
    logger.info({ count: rows.length, sourceIds }, 'cascade cancelled dependents');
  }
  return rows.map((r) => r.id);
}

/**
 * Unblocks every job whose dependencies are now all SUCCEEDED (fan-out: one
 * completion may release many dependents; fan-in: a dependent is only released
 * once its last dependency lands).
 */
export async function unblockDependents(client: PoolClient, sourceIds: string[]): Promise<string[]> {
  if (sourceIds.length === 0) return [];
  const { rows } = await client.query<{ id: string; queue_name: string }>(
    `WITH upd AS (
       UPDATE jobs d
       SET state = 'PENDING', updated_at = now(),
           scheduled_for = GREATEST(d.scheduled_for, now())
       WHERE d.state = 'BLOCKED'
         AND d.depends_on && $1::uuid[]
         AND NOT EXISTS (
           SELECT 1 FROM jobs p
           WHERE p.id = ANY(d.depends_on) AND p.state <> 'SUCCEEDED'
         )
       RETURNING d.id, d.queue_name
     ), ev AS (
       INSERT INTO job_events (job_id, from_state, to_state, node_id, detail)
       SELECT id, 'BLOCKED', 'PENDING', $2, 'all dependencies succeeded' FROM upd
     )
     SELECT id, queue_name FROM upd`,
    [sourceIds, config.nodeId],
  );
  for (const queue of new Set(rows.map((r) => r.queue_name))) publishWorkAvailable(queue);
  return rows.map((r) => r.id);
}

/** Manual requeue out of the dead letter queue. Resets the retry budget. */
export async function requeueJob(id: string, resetAttempts = true): Promise<Job> {
  const { rows } = await query<Job>(
    `WITH upd AS (
       UPDATE jobs
       SET state = 'PENDING',
           scheduled_for = now(),
           attempt_count = CASE WHEN $2::boolean THEN 0 ELSE attempt_count END,
           claimed_by = NULL, claimed_at = NULL, lease_expires_at = NULL,
           completed_at = NULL, updated_at = now()
       WHERE id = $1 AND state = 'DEAD'
       RETURNING *
     ), ev AS (
       INSERT INTO job_events (job_id, from_state, to_state, node_id, detail)
       SELECT id, 'DEAD', 'PENDING', $3, 'manual requeue' FROM upd
     )
     SELECT * FROM upd`,
    [id, resetAttempts, config.nodeId],
    'requeue',
  );

  const job = rows[0];
  if (job === undefined) {
    const current = await query<{ state: string }>('SELECT state FROM jobs WHERE id = $1', [id]);
    const state = current.rows[0]?.state;
    if (state === undefined) throw new NotFoundError(id);
    throw new ConflictError(`job is ${state}; only DEAD jobs can be requeued`);
  }
  publishWorkAvailable(job.queue_name);
  return job;
}

export function buildGraphForTest(edges: Record<string, string[]>): DepGraph {
  return new Map(Object.entries(edges));
}
