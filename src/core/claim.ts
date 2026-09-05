import type { PoolClient } from '../db/pool.js';
import { withTransaction } from '../db/pool.js';
import { config } from '../config.js';
import { agingSqlExpression } from '../domain/priority.js';
import { rateLimiter } from '../redis/rateLimiter.js';
import { claimBatchSize, claimDuration, jobsClaimed, schedulingLatency } from '../metrics.js';
import type { JobAssignment } from '../domain/job.js';

export interface ClaimedRow {
  id: string;
  tenant_id: string;
  queue_name: string;
  job_type: string;
  payload: Record<string, unknown>;
  attempt_count: number;
  max_attempts: number;
  priority: number;
  lease_expires_at: Date;
  scheduling_latency_ms: number;
}

export interface ClaimOptions {
  workerId: string;
  queue: string;
  limit: number;
  /** Overrides the configured lease length; used by tests. */
  leaseSeconds?: number;
}

/**
 * The claim query.
 *
 * Structure, top to bottom:
 *
 *  1. capacity / active / inflight / caps - fair share. "caps" gives each tenant
 *     its remaining headroom: its weighted slice of total worker capacity minus
 *     what it already holds, but only *while another tenant has ready work*.
 *     Under no contention headroom is unbounded and a single tenant may use the
 *     whole fleet, so the cap never wastes capacity on an idle tenant.
 *  2. cand - candidate pull. A LATERAL over generate_series(0,9) takes the K
 *     oldest ready jobs at each priority level. This is the trick that makes
 *     aging both correct and index-friendly: ordering by the aging expression
 *     directly is unindexable, but aging is monotonic in created_at, so the
 *     best-aged job at each level is always among that level's K oldest. Ten
 *     bounded index range scans replace one unbounded sort over the whole queue.
 *  3. ranked - applies the aging expression to those candidates, trims each
 *     tenant to its headroom with a per-tenant row_number, and keeps the best
 *     ones overall.
 *  4. locked - re-reads exactly those rows FOR UPDATE SKIP LOCKED. The
 *     state = 'PENDING' recheck is load-bearing: SKIP LOCKED steps over rows a
 *     concurrent claimer holds *right now*, but a claimer that committed between
 *     step 2 and step 4 has already released its lock, so we would find the row
 *     unlocked and already CLAIMED. The recheck is what turns "skip locked" into
 *     "claim exactly once".
 *  5. upd / ev - flips them to CLAIMED with a lease and records the transition in
 *     the same statement, so the audit log can never disagree with the row.
 *
 * Why SKIP LOCKED instead of an advisory lock per job, or a bare
 * UPDATE ... WHERE state='PENDING' RETURNING: twenty workers all want the same
 * head-of-queue rows, and under any *blocking* lock they serialise - every
 * worker waits for the first one's transaction to commit, so throughput is one
 * batch per round trip no matter how many workers you add. SKIP LOCKED lets N
 * workers take N disjoint batches concurrently with zero lock waits, which is
 * exactly the access pattern a work queue needs.
 */
/**
 * Parameter slots are allocated rather than hard-coded, because the fair-share
 * CTEs are the only consumers of $9 and $10. Numbering them statically and then
 * omitting those CTEs leaves gaps, and Postgres rejects a statement whose
 * parameters are not contiguous ("could not determine data type of parameter
 * $9") - which silently turns every claim into an error.
 */
interface ClaimPlan {
  sql: string;
  fairShare: boolean;
}

function buildClaimSql(): ClaimPlan {
  //  $1 queue  $2 candidatesPerPriority  $3 limit  $4 workerId  $5 leaseSeconds
  //  $6 agingInterval  $7 agingMaxBoost   $8 nodeId
  //  with fair share:    $9 fairSharePct  $10 hbWindow  $11 overselectFactor
  //  without fair share: $9 overselectFactor
  const eff = agingSqlExpression(
    'cand.priority',
    'cand.created_at',
    '$6::double precision',
    '$7::double precision',
  );

  const overselectParam = config.fairShareEnabled ? '$11' : '$9';

  const fairShareCtes = config.fairShareEnabled
    ? [
        "capacity AS MATERIALIZED (",
        "  SELECT GREATEST(COALESCE(SUM(max_concurrency), 0), 1)::int AS slots",
        "  FROM workers",
        "  WHERE state <> 'DEAD'",
        "    AND last_heartbeat_at > now() - make_interval(secs => $10::double precision)",
        "),",
        "active AS MATERIALIZED (",
        "  SELECT t.tenant_id, GREATEST(t.weight, 0.0001)::double precision AS w",
        "  FROM tenants t",
        "  WHERE EXISTS (",
        "    SELECT 1 FROM jobs j",
        "    WHERE j.tenant_id = t.tenant_id AND j.state IN ('PENDING', 'CLAIMED', 'RUNNING')",
        "  )",
        "),",
        "sumw AS MATERIALIZED (SELECT GREATEST(COALESCE(SUM(w), 1), 0.0001) AS s FROM active),",
        "ready_tenants AS MATERIALIZED (",
        "  SELECT a.tenant_id FROM active a",
        "  WHERE EXISTS (",
        "    SELECT 1 FROM jobs j",
        "    WHERE j.tenant_id = a.tenant_id AND j.state = 'PENDING' AND j.scheduled_for <= now()",
        "  )",
        "),",
        "inflight AS MATERIALIZED (",
        "  SELECT tenant_id, count(*)::int AS c",
        "  FROM jobs WHERE state IN ('CLAIMED', 'RUNNING') GROUP BY tenant_id",
        "),",
        // headroom = how many more slots this tenant may take right now.
        // Unlimited (int max) when no other tenant has ready work, so an idle
        // fleet is never partitioned for the benefit of a tenant with nothing
        // to run. Otherwise: weighted slice of total worker capacity, minus what
        // the tenant already holds.
        "caps AS MATERIALIZED (",
        "  SELECT a.tenant_id,",
        "         CASE WHEN EXISTS (SELECT 1 FROM ready_tenants r WHERE r.tenant_id <> a.tenant_id)",
        "              THEN GREATEST(0,",
        "                     GREATEST(1, ceil(c.slots * LEAST($9::double precision, a.w / s.s)))::int",
        "                     - COALESCE(i.c, 0))",
        "              ELSE 2147483647",
        "         END AS headroom",
        "  FROM active a",
        "  CROSS JOIN sumw s",
        "  CROSS JOIN capacity c",
        "  LEFT JOIN inflight i ON i.tenant_id = a.tenant_id",
        "),",
      ].join('\n  ')
    : '';

  // Fair share is enforced in two places, and it needs both.
  //
  // (a) Inside the candidate pull. The per-priority LATERAL takes the K oldest
  //     ready jobs, and a tenant with a 100k-job backlog owns every one of those
  //     K slots - so a tenant that is already at its cap must be excluded here,
  //     or the window fills with ineligible rows and the claim returns nothing
  //     instead of returning the *other* tenants' work.
  //
  // (b) Inside the ranking, as a per-tenant row limit. Without it the cap is only
  //     enforced once per statement: a batch of 32 against a tenant with 5 slots
  //     of headroom would take all 32, because the CTE sees the in-flight count
  //     as it was at the start of the statement.
  const rankedCapJoin = config.fairShareEnabled
    ? "  JOIN caps k ON k.tenant_id = x.tenant_id AND x.rn <= k.headroom"
    : '  WHERE true';

  const capFilter = config.fairShareEnabled
    ? 'AND NOT EXISTS (SELECT 1 FROM caps k WHERE k.tenant_id = c.tenant_id AND k.headroom <= 0)'
    : '';

  const sql = [
    'WITH ' + fairShareCtes,
    'cand AS MATERIALIZED (',
    '  SELECT j.id, j.tenant_id, j.priority, j.scheduled_for, j.created_at',
    '  FROM generate_series(0, 9) AS lvl(pr)',
    '  CROSS JOIN LATERAL (',
    '    SELECT c.id, c.tenant_id, c.priority, c.scheduled_for, c.created_at',
    '    FROM jobs c',
    "    WHERE c.state = 'PENDING'",
    '      AND c.queue_name = $1',
    '      AND c.priority = lvl.pr',
    '      AND c.scheduled_for <= now()',
    '      AND NOT EXISTS (',
    '        SELECT 1 FROM circuit_breakers b',
    "        WHERE b.job_type = c.job_type AND b.state = 'OPEN'",
    '      )',
    '      ' + capFilter,
    '    ORDER BY c.scheduled_for ASC',
    '    LIMIT $2::int',
    '  ) j',
    '),',
    'ranked AS MATERIALIZED (',
    '  SELECT x.id, x.eff, x.scheduled_for',
    '  FROM (',
    '    SELECT cand.id, cand.tenant_id, cand.scheduled_for, ' + eff + ' AS eff,',
    '           row_number() OVER (',
    '             PARTITION BY cand.tenant_id',
    '             ORDER BY ' + eff + ' ASC, cand.scheduled_for ASC',
    '           ) AS rn',
    '    FROM cand',
    '  ) x',
    rankedCapJoin,
    '  ORDER BY x.eff ASC, x.scheduled_for ASC',
    // Deliberately wider than the batch. If the candidate set were exactly the
    // batch size, every concurrent claimer would rank the same N rows, the first
    // would lock all of them and SKIP LOCKED would hand everyone else an empty
    // result - turning the parallel claim into a serial one. Over-selecting
    // gives each claimer somewhere to go once the head of the queue is taken.
    // The cost is a slight softening of strict priority order under contention:
    // a worker may take the 30th-best job rather than the 5th-best because the
    // better ones are locked. That is the right trade - the alternative is that
    // 19 of 20 workers sit idle in front of a full queue.
    '  LIMIT ($3::int * ' + overselectParam + '::int)',
    '),',
    'locked AS (',
    '  SELECT j.id',
    '  FROM jobs j',
    '  JOIN ranked r ON r.id = j.id',
    "  WHERE j.state = 'PENDING' AND j.scheduled_for <= now()",
    '  ORDER BY r.eff ASC, r.scheduled_for ASC',
    '  FOR UPDATE OF j SKIP LOCKED',
    '  LIMIT $3::int',
    '),',
    'upd AS (',
    '  UPDATE jobs',
    "  SET state = 'CLAIMED',",
    '      claimed_by = $4,',
    '      claimed_at = now(),',
    '      lease_expires_at = now() + make_interval(secs => $5::double precision),',
    '      updated_at = now()',
    '  FROM locked',
    '  WHERE jobs.id = locked.id',
    '  RETURNING jobs.id, jobs.tenant_id, jobs.queue_name, jobs.job_type, jobs.payload,',
    '            jobs.attempt_count, jobs.max_attempts, jobs.priority, jobs.lease_expires_at,',
    '            (EXTRACT(EPOCH FROM (now() - jobs.scheduled_for)) * 1000)::double precision',
    '              AS scheduling_latency_ms',
    '),',
    'ev AS (',
    '  INSERT INTO job_events (job_id, from_state, to_state, worker_id, node_id)',
    "  SELECT id, 'PENDING', 'CLAIMED', $4, $8 FROM upd",
    ')',
    'SELECT * FROM upd',
  ].join('\n  ');

  return { sql, fairShare: config.fairShareEnabled };
}

let cachedPlan: ClaimPlan | null = null;

export function claimPlan(): ClaimPlan {
  cachedPlan ??= buildClaimSql();
  return cachedPlan;
}

/** Test hook: drop the memoised SQL after config has been stubbed. */
export function resetClaimSqlCache(): void {
  cachedPlan = null;
}

/**
 * Claims up to `limit` ready jobs from one queue for one worker.
 *
 * One queue per call, deliberately: an equality predicate on queue_name lets the
 * partial index (queue_name, priority, scheduled_for) drive every one of the ten
 * LATERAL probes, and it gives the per-queue token bucket an exact budget to
 * meter. Workers listening to several queues issue one call per queue.
 *
 * Runs inside an explicit BEGIN/COMMIT on a client checked out of the pool - see
 * withTransaction() for why a pooled query() would silently break the locking.
 */
export async function claimJobs(opts: ClaimOptions): Promise<ClaimedRow[]> {
  const requested = Math.max(0, Math.min(opts.limit, config.claimBatchSize));
  if (requested === 0) return [];

  let budget = requested;
  if (config.rateLimitEnabled) {
    const grant = await rateLimiter.take(opts.queue, requested, {
      capacity: config.rateLimitCapacity,
      refillPerSecond: config.rateLimitRefillPerSec,
    });
    budget = grant.granted;
    if (budget === 0) return [];
  }

  const leaseSeconds = opts.leaseSeconds ?? config.leaseDurationSeconds;
  const heartbeatWindow = (config.workerHeartbeatIntervalMs * config.workerMissedBeats) / 1000;
  const stopTimer = claimDuration.startTimer();

  const plan = claimPlan();
  const overselect = Math.max(1, config.claimOverselectFactor);
  const params: unknown[] = [
    opts.queue,
    config.claimCandidatesPerPriority,
    budget,
    opts.workerId,
    leaseSeconds,
    config.agingIntervalSeconds,
    config.agingMaxBoost,
    config.nodeId,
  ];
  if (plan.fairShare) {
    params.push(config.fairShareMaxPct, heartbeatWindow);
  }
  params.push(overselect);

  const rows = await withTransaction(async (client: PoolClient) => {
    const res = await client.query<ClaimedRow>(plan.sql, params);
    return res.rows;
  }, 'claim');

  stopTimer();
  claimBatchSize.observe(rows.length);
  if (rows.length > 0) {
    jobsClaimed.inc({ queue: opts.queue }, rows.length);
    for (const row of rows) {
      schedulingLatency.observe({ queue: opts.queue }, Math.max(0, row.scheduling_latency_ms));
    }
  }
  return rows;
}

export function toAssignment(row: ClaimedRow): JobAssignment {
  return {
    id: row.id,
    tenant_id: row.tenant_id,
    queue_name: row.queue_name,
    job_type: row.job_type,
    payload: row.payload,
    attempt_count: row.attempt_count,
    max_attempts: row.max_attempts,
    lease_expires_at: row.lease_expires_at.toISOString(),
    scheduling_latency_ms: row.scheduling_latency_ms,
  };
}
