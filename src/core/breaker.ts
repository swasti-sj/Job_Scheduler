import { redis } from '../redis/client.js';
import { query } from '../db/pool.js';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { breakerState } from '../metrics.js';
import { publishEvent } from './events.js';

/**
 * Per-job_type circuit breaker over a sliding window.
 *
 * The window is kept as one-second counter buckets in Redis rather than a list
 * of individual outcomes. At 5K jobs/sec a 60s window of individual entries is
 * ~300K members per job type, all of which have to be trimmed on every read;
 * bucketed counters make a record an INCR and an evaluation a single MGET of
 * `window` keys, with expiry doing the trimming for free. The cost is one second
 * of granularity at the window edge, which is irrelevant for a breaker.
 *
 * Evaluation is leader-only, so exactly one process decides the state, and the
 * decision is written to Postgres where every claimer reads it - the claim query
 * excludes OPEN job types directly, so a tripped breaker stops dispatch fleet
 * wide without any node needing to be told.
 *
 * OPEN -> HALF_OPEN -> CLOSED: after the cooldown the breaker half-opens and the
 * window is cleared, so recovery is judged on fresh traffic. If the failures
 * continue the next evaluation trips it straight back to OPEN.
 */

const TYPES_KEY = 'breaker:types';

function bucketKey(jobType: string, outcome: 'ok' | 'fail', second: number): string {
  return `breaker:${jobType}:${outcome}:${second}`;
}

/**
 * Records one attempt outcome. Fire-and-forget from the lifecycle path: the
 * breaker is an availability heuristic, and a Redis blip must never fail a job
 * completion that Postgres already committed.
 */
export async function recordOutcome(jobType: string, success: boolean): Promise<void> {
  if (!config.breakerEnabled) return;
  const second = Math.floor(Date.now() / 1000);
  const key = bucketKey(jobType, success ? 'ok' : 'fail', second);
  try {
    const pipeline = redis.pipeline();
    pipeline.incr(key);
    pipeline.expire(key, config.breakerWindowSeconds + 10);
    pipeline.sadd(TYPES_KEY, jobType);
    pipeline.expire(TYPES_KEY, 3600);
    await pipeline.exec();
  } catch (err) {
    logger.debug({ err, jobType }, 'breaker outcome not recorded');
  }
}

export interface WindowStats {
  ok: number;
  fail: number;
  total: number;
  failureRate: number;
}

export async function windowStats(jobType: string): Promise<WindowStats> {
  const now = Math.floor(Date.now() / 1000);
  const keys: string[] = [];
  for (let s = now - config.breakerWindowSeconds + 1; s <= now; s += 1) {
    keys.push(bucketKey(jobType, 'ok', s));
    keys.push(bucketKey(jobType, 'fail', s));
  }
  const values = await redis.mget(keys);

  let ok = 0;
  let fail = 0;
  for (let i = 0; i < values.length; i += 2) {
    ok += Number.parseInt(values[i] ?? '0', 10) || 0;
    fail += Number.parseInt(values[i + 1] ?? '0', 10) || 0;
  }
  const total = ok + fail;
  return { ok, fail, total, failureRate: total === 0 ? 0 : fail / total };
}

async function clearWindow(jobType: string): Promise<void> {
  const now = Math.floor(Date.now() / 1000);
  const keys: string[] = [];
  for (let s = now - config.breakerWindowSeconds - 10; s <= now; s += 1) {
    keys.push(bucketKey(jobType, 'ok', s));
    keys.push(bucketKey(jobType, 'fail', s));
  }
  if (keys.length > 0) await redis.del(...keys);
}

export type BreakerState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

interface BreakerRow {
  job_type: string;
  state: BreakerState;
  reopen_after: Date | null;
}

/** One evaluation pass over every job type seen in the window. Leader only. */
export async function evaluateBreakers(): Promise<void> {
  if (!config.breakerEnabled) return;

  const [types, current] = await Promise.all([
    redis.smembers(TYPES_KEY),
    query<BreakerRow>('SELECT job_type, state, reopen_after FROM circuit_breakers', [], 'breakers'),
  ]);

  const byType = new Map(current.rows.map((r) => [r.job_type, r]));
  const now = Date.now();

  for (const jobType of types) {
    const row = byType.get(jobType);
    const state: BreakerState = row?.state ?? 'CLOSED';

    // Cooldown elapsed: half-open on a clean window so recovery is judged on
    // fresh traffic rather than the failures that tripped it.
    if (state === 'OPEN') {
      const reopenAt = row?.reopen_after?.getTime() ?? 0;
      if (now >= reopenAt) {
        await clearWindow(jobType);
        await upsertBreaker(jobType, 'HALF_OPEN', 0, 0);
        breakerState.set({ job_type: jobType }, 0);
        logger.info({ jobType }, 'circuit breaker half-open');
      }
      continue;
    }

    const stats = await windowStats(jobType);
    if (stats.total < config.breakerMinSamples) {
      if (state === 'HALF_OPEN') continue; // not enough evidence either way yet
      continue;
    }

    if (stats.failureRate > config.breakerFailureRate) {
      await upsertBreaker(jobType, 'OPEN', stats.failureRate, stats.total);
      breakerState.set({ job_type: jobType }, 1);
      logger.warn({ jobType, ...stats }, 'circuit breaker opened; job type paused');
      publishEvent({
        type: 'breaker_opened',
        job_type: jobType,
        failure_rate: stats.failureRate,
        samples: stats.total,
        at: new Date().toISOString(),
      });
    } else if (state !== 'CLOSED') {
      await upsertBreaker(jobType, 'CLOSED', stats.failureRate, stats.total);
      breakerState.set({ job_type: jobType }, 0);
      logger.info({ jobType, ...stats }, 'circuit breaker closed');
      publishEvent({ type: 'breaker_closed', job_type: jobType, at: new Date().toISOString() });
    }
  }
}

async function upsertBreaker(
  jobType: string,
  state: BreakerState,
  failureRate: number,
  samples: number,
): Promise<void> {
  await query(
    `INSERT INTO circuit_breakers (job_type, state, failure_rate, samples, opened_at, reopen_after, updated_at)
     VALUES ($1, $2, $3, $4,
             CASE WHEN $2 = 'OPEN' THEN now() ELSE NULL END,
             CASE WHEN $2 = 'OPEN' THEN now() + make_interval(secs => $5::double precision) ELSE NULL END,
             now())
     ON CONFLICT (job_type) DO UPDATE
     SET state = EXCLUDED.state,
         failure_rate = EXCLUDED.failure_rate,
         samples = EXCLUDED.samples,
         opened_at = EXCLUDED.opened_at,
         reopen_after = EXCLUDED.reopen_after,
         updated_at = now()`,
    [jobType, state, failureRate, samples, config.breakerCooldownSeconds],
    'breaker_upsert',
  );
}

export async function listBreakers(): Promise<BreakerRow[]> {
  const { rows } = await query<BreakerRow>(
    'SELECT job_type, state, reopen_after FROM circuit_breakers ORDER BY job_type',
    [],
    'breaker_list',
  );
  return rows;
}

/** Operator override, used by tests and by the admin endpoint. */
export async function setBreaker(jobType: string, state: BreakerState): Promise<void> {
  await upsertBreaker(jobType, state, 0, 0);
  if (state !== 'OPEN') await clearWindow(jobType);
  breakerState.set({ job_type: jobType }, state === 'OPEN' ? 1 : 0);
}
