import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { query } from '../../src/db/pool.js';
import { submitJob } from '../../src/core/jobStore.js';
import {
  buildOnce,
  isLeader,
  sigkill,
  startScheduler,
  startWorker,
  stopAll,
  waitForHttp,
} from './cluster.js';
import { resetDatabase, setupDatabase, sleep, teardown, waitFor } from './helpers.js';

beforeAll(async () => {
  await setupDatabase();
  buildOnce();
}, 180_000);
afterAll(teardown);
beforeEach(resetDatabase);
afterEach(stopAll);

const base = {
  tenant_id: 'acme',
  queue_name: 'default',
  job_type: 'sleep',
  priority: 5 as const,
  depends_on: [],
};

async function terminalCounts(): Promise<Record<string, number>> {
  const { rows } = await query<{ state: string; n: number }>(
    'SELECT state::text AS state, count(*)::int AS n FROM jobs GROUP BY state',
  );
  return Object.fromEntries(rows.map((r) => [r.state, r.n]));
}

describe('worker SIGKILL', () => {
  it('completes a job exactly once after the worker running it is killed', async () => {
    const scheduler = startScheduler('sched-a', 3101);
    await waitForHttp(3101);

    const worker = startWorker('victim', 3101);
    await waitFor(async () => {
      const { rows } = await query<{ n: number }>(
        `SELECT count(*)::int AS n FROM workers WHERE state <> 'DEAD'`,
      );
      return (rows[0]?.n ?? 0) > 0;
    }, 20_000);

    // A job long enough that we can reliably kill the worker mid-execution.
    const { job } = await submitJob({ ...base, payload: { ms: 5000 }, max_attempts: 5 });

    await waitFor(async () => {
      const { rows } = await query<{ state: string }>('SELECT state::text AS state FROM jobs WHERE id = $1', [
        job.id,
      ]);
      return rows[0]?.state === 'RUNNING' || rows[0]?.state === 'CLAIMED';
    }, 20_000);

    sigkill(worker);

    // The socket close is observed immediately, so the job is back in the queue
    // long before its 30s lease would have expired.
    await waitFor(async () => {
      const { rows } = await query<{ state: string }>('SELECT state::text AS state FROM jobs WHERE id = $1', [
        job.id,
      ]);
      return rows[0]?.state === 'PENDING';
    }, 10_000);

    // A replacement worker picks it up and finishes it.
    startWorker('replacement', 3101);
    await waitFor(async () => {
      const { rows } = await query<{ state: string }>('SELECT state::text AS state FROM jobs WHERE id = $1', [
        job.id,
      ]);
      return rows[0]?.state === 'SUCCEEDED';
    }, 40_000);

    // Exactly once: the audit log holds a single SUCCEEDED transition even
    // though the job body was started twice.
    const { rows } = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM job_events WHERE job_id = $1 AND to_state = 'SUCCEEDED'`,
      [job.id],
    );
    expect(rows[0]?.n).toBe(1);
    expect(await terminalCounts()).toEqual({ SUCCEEDED: 1 });
    expect(scheduler.child.exitCode).toBeNull();
  }, 120_000);

  it('loses no jobs when a worker is killed holding a full batch', async () => {
    startScheduler('sched-b', 3102);
    await waitForHttp(3102);
    const worker = startWorker('bulk-victim', 3102, { WORKER_CONCURRENCY: '8' });

    for (let i = 0; i < 24; i += 1) {
      await submitJob({ ...base, payload: { ms: 4000 } });
    }

    await waitFor(async () => {
      const { rows } = await query<{ n: number }>(
        `SELECT count(*)::int AS n FROM jobs WHERE state IN ('CLAIMED','RUNNING')`,
      );
      return (rows[0]?.n ?? 0) >= 4;
    }, 20_000);

    sigkill(worker);

    // Everything it held comes straight back; nothing is stuck in flight.
    await waitFor(async () => {
      const { rows } = await query<{ n: number }>(
        `SELECT count(*)::int AS n FROM jobs WHERE state IN ('CLAIMED','RUNNING')`,
      );
      return (rows[0]?.n ?? 0) === 0;
    }, 20_000);

    const counts = await terminalCounts();
    const accounted = Object.values(counts).reduce((a, b) => a + b, 0);
    expect(accounted).toBe(24);
    expect(counts['DEAD'] ?? 0).toBe(0);
  }, 120_000);
});

describe('scheduler SIGKILL', () => {
  it('fails leadership over and keeps scheduling, with no duplicate work', async () => {
    const a = startScheduler('sched-1', 3111);
    const b = startScheduler('sched-2', 3112);
    const c = startScheduler('sched-3', 3113);
    await Promise.all([waitForHttp(3111), waitForHttp(3112), waitForHttp(3113)]);

    const ports = [3111, 3112, 3113];
    const nodes = [a, b, c];
    await waitFor(async () => {
      const flags = await Promise.all(ports.map(isLeader));
      return flags.filter(Boolean).length === 1;
    }, 20_000);

    const leaderIndex = (await Promise.all(ports.map(isLeader))).findIndex(Boolean);
    expect(leaderIndex).toBeGreaterThanOrEqual(0);

    startWorker('w-1', ports[(leaderIndex + 1) % 3] as number);
    for (let i = 0; i < 40; i += 1) {
      await submitJob({ ...base, payload: { ms: 50 } });
    }

    // Kill the leader outright while work is flowing.
    const leaderNode = nodes[leaderIndex];
    if (leaderNode === undefined) throw new Error('no leader to kill');
    const killedAt = Date.now();
    sigkill(leaderNode);

    const survivors = ports.filter((_, i) => i !== leaderIndex);
    await waitFor(async () => {
      const flags = await Promise.all(survivors.map(isLeader));
      return flags.some(Boolean);
    }, 10_000, 25);

    const failoverMs = Date.now() - killedAt;
    expect(failoverMs).toBeLessThan(2000);

    // Exactly one leader afterwards - never two reapers running at once.
    const flags = await Promise.all(survivors.map(isLeader));
    expect(flags.filter(Boolean)).toHaveLength(1);

    // Work keeps flowing on the survivors, and every job lands exactly once.
    await waitFor(async () => {
      const counts = await terminalCounts();
      return (counts['SUCCEEDED'] ?? 0) === 40;
    }, 60_000);

    const { rows } = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM (
         SELECT job_id FROM job_events WHERE to_state = 'SUCCEEDED'
         GROUP BY job_id HAVING count(*) > 1
       ) d`,
    );
    expect(rows[0]?.n).toBe(0);
  }, 180_000);

  it('survives a scheduler dying while it holds worker sockets', async () => {
    // Path 1 (socket close) is unavailable here: the process that would have
    // observed the close is the one that died. Lease expiry - path 2 - is the
    // only thing that can recover this, which is exactly why it exists.
    const doomed = startScheduler('sched-doomed', 3121, { LEASE_DURATION_SECONDS: '5' });
    const survivor = startScheduler('sched-survivor', 3122, { LEASE_DURATION_SECONDS: '5' });
    await Promise.all([waitForHttp(3121), waitForHttp(3122)]);

    startWorker('orphan-worker', 3121, { WORKER_CONCURRENCY: '2' });
    const { job } = await submitJob({ ...base, payload: { ms: 30_000 }, max_attempts: 5 });

    await waitFor(async () => {
      const { rows } = await query<{ state: string }>(
        'SELECT state::text AS state FROM jobs WHERE id = $1',
        [job.id],
      );
      return rows[0]?.state === 'RUNNING' || rows[0]?.state === 'CLAIMED';
    }, 20_000);

    sigkill(doomed);
    await sleep(500);

    // Nobody saw a socket close, so the job stays in flight until its lease
    // runs out - then the surviving leader's reaper returns it.
    await waitFor(async () => {
      const { rows } = await query<{ state: string }>(
        'SELECT state::text AS state FROM jobs WHERE id = $1',
        [job.id],
      );
      return rows[0]?.state === 'PENDING' || rows[0]?.state === 'CLAIMED';
    }, 30_000);

    const { rows } = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM job_events
       WHERE job_id = $1 AND detail LIKE '%lease expired%'`,
      [job.id],
    );
    expect(rows[0]?.n).toBeGreaterThanOrEqual(1);
    expect(survivor.child.exitCode).toBeNull();
  }, 180_000);
});
