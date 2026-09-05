import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { claimJobs } from '../../src/core/claim.js';
import { completeJob, extendLeases, failJob, markRunning } from '../../src/core/lifecycle.js';
import { reclaimExpiredLeases, reclaimWorkerJobs } from '../../src/core/reclaim.js';
import { query } from '../../src/db/pool.js';
import {
  countByState,
  expireLease,
  getJobRow,
  registerWorker,
  resetDatabase,
  seedJobs,
  setupDatabase,
  teardown,
} from './helpers.js';

beforeAll(setupDatabase);
afterAll(teardown);
beforeEach(resetDatabase);

describe('claiming', () => {
  it('claims a pending job and puts a lease on it', async () => {
    const [id] = await seedJobs();
    await registerWorker('w1');

    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 10 });
    expect(claimed.map((c) => c.id)).toEqual([id]);

    const row = await getJobRow(id as string);
    expect(row.state).toBe('CLAIMED');
    expect(row.claimed_by).toBe('w1');
    expect(row.lease_expires_at).not.toBeNull();
    expect((row.lease_expires_at as Date).getTime()).toBeGreaterThan(Date.now());
  });

  it('does not claim a job scheduled for the future', async () => {
    await seedJobs({ scheduledFor: new Date(Date.now() + 60_000).toISOString() });
    await registerWorker('w1');
    expect(await claimJobs({ workerId: 'w1', queue: 'default', limit: 10 })).toEqual([]);
  });

  it('does not claim from another queue', async () => {
    await seedJobs({ queue: 'other' });
    await registerWorker('w1');
    expect(await claimJobs({ workerId: 'w1', queue: 'default', limit: 10 })).toEqual([]);
  });

  it('does not claim a BLOCKED job', async () => {
    const [id] = await seedJobs();
    await query(`UPDATE jobs SET state = 'BLOCKED' WHERE id = $1`, [id]);
    await registerWorker('w1');
    expect(await claimJobs({ workerId: 'w1', queue: 'default', limit: 10 })).toEqual([]);
  });

  it('respects the batch limit', async () => {
    await seedJobs({ count: 50 });
    await registerWorker('w1');
    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 7 });
    expect(claimed).toHaveLength(7);
  });

  it('skips a job type whose circuit breaker is open', async () => {
    await seedJobs({ jobType: 'flaky', count: 3 });
    await seedJobs({ jobType: 'noop', count: 2 });
    await registerWorker('w1');
    await query(`INSERT INTO circuit_breakers (job_type, state) VALUES ('flaky', 'OPEN')`);

    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 10 });
    expect(claimed).toHaveLength(2);
    expect(claimed.every((c) => c.job_type === 'noop')).toBe(true);
  });
});

describe('priority and aging in the claim query', () => {
  it('claims the highest-priority job first', async () => {
    await seedJobs({ priority: 9, count: 1 });
    const [urgent] = await seedJobs({ priority: 0, count: 1 });
    await registerWorker('w1');

    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    expect(claimed[0]?.id).toBe(urgent);
  });

  it('lets an aged low-priority job overtake a fresh high-priority one', async () => {
    // Aging interval defaults to 30s: a priority-9 job that has waited 10
    // minutes has an effective priority of 0 and must win.
    const [aged] = await seedJobs({
      priority: 9,
      createdAt: new Date(Date.now() - 600_000).toISOString(),
    });
    await seedJobs({ priority: 2 });
    await registerWorker('w1');

    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    expect(claimed[0]?.id).toBe(aged);
  });

  it('is FIFO within one priority band', async () => {
    const [older] = await seedJobs({
      priority: 5,
      createdAt: new Date(Date.now() - 5_000).toISOString(),
      scheduledFor: new Date(Date.now() - 5_000).toISOString(),
    });
    await seedJobs({ priority: 5 });
    await registerWorker('w1');

    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    expect(claimed[0]?.id).toBe(older);
  });
});

describe('lease handling', () => {
  it('extends leases on heartbeat', async () => {
    const [id] = await seedJobs();
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });

    const before = await getJobRow(id as string);
    await query(`UPDATE jobs SET lease_expires_at = now() + interval '1 second' WHERE id = $1`, [id]);
    const revoked = await extendLeases('w1', [id as string]);

    expect(revoked).toEqual([]);
    const after = await getJobRow(id as string);
    expect((after.lease_expires_at as Date).getTime()).toBeGreaterThan(
      (before.lease_expires_at as Date).getTime() - 2000,
    );
  });

  it('reports a job as revoked when the worker no longer owns it', async () => {
    const [id] = await seedJobs();
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    await expireLease(id as string);
    await reclaimExpiredLeases();

    // w1 still thinks it owns the job; the heartbeat tells it otherwise.
    expect(await extendLeases('w1', [id as string])).toEqual([id]);
  });
});

describe('ownership fencing', () => {
  it('rejects a completion from a worker that lost the job', async () => {
    const [id] = await seedJobs();
    await registerWorker('w1');
    await registerWorker('w2');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });

    // w1 stalls; its lease expires and w2 picks the job up.
    await expireLease(id as string);
    await reclaimExpiredLeases();
    // The reclaim applied a jittered backoff; skip past it, we are testing
    // ownership here, not scheduling.
    await query(`UPDATE jobs SET scheduled_for = now() WHERE id = $1`, [id]);
    const reclaimed = await claimJobs({ workerId: 'w2', queue: 'default', limit: 1 });
    expect(reclaimed[0]?.id).toBe(id);

    // The zombie wakes up and reports success. It must be ignored.
    expect(await completeJob(id as string, 'w1', { from: 'zombie' })).toBe('not_owner');
    expect((await getJobRow(id as string)).state).toBe('CLAIMED');

    // The real owner's report is accepted.
    expect(await completeJob(id as string, 'w2', { from: 'w2' })).toBe('ok');
    expect((await getJobRow(id as string)).state).toBe('SUCCEEDED');
  });

  it('records exactly one terminal transition even after a zombie report', async () => {
    const [id] = await seedJobs();
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    await completeJob(id as string, 'w1', null);
    await completeJob(id as string, 'w1', null); // duplicate report

    const { rows } = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM job_events WHERE job_id = $1 AND to_state = 'SUCCEEDED'`,
      [id],
    );
    expect(rows[0]?.n).toBe(1);
  });

  it('rejects markRunning from a non-owner', async () => {
    const [id] = await seedJobs();
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    expect(await markRunning(id as string, 'someone-else')).toBe('not_owner');
    expect(await markRunning(id as string, 'w1')).toBe('ok');
  });
});

describe('retries and the dead letter queue', () => {
  it('reschedules a failed job into the future with backoff', async () => {
    const [id] = await seedJobs({ maxAttempts: 3 });
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });

    expect(await failJob(id as string, 'w1', 'boom')).toBe('ok');
    const row = await getJobRow(id as string);
    expect(row.state).toBe('PENDING');
    expect(row.attempt_count).toBe(1);
    expect(row.last_error).toBe('boom');
    expect(row.claimed_by).toBeNull();
    expect(row.lease_expires_at).toBeNull();
    // Full jitter can legitimately produce a delay of 0, so assert the bound.
    expect(row.scheduled_for.getTime()).toBeLessThanOrEqual(Date.now() + 500);
  });

  it('moves a job to DEAD once the retry budget is spent', async () => {
    const [id] = await seedJobs({ maxAttempts: 2 });
    await registerWorker('w1');

    for (let attempt = 0; attempt < 2; attempt += 1) {
      await query(`UPDATE jobs SET scheduled_for = now() WHERE id = $1`, [id]);
      const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
      expect(claimed).toHaveLength(1);
      await failJob(id as string, 'w1', `attempt ${attempt}`);
    }

    const row = await getJobRow(id as string);
    expect(row.state).toBe('DEAD');
    expect(row.attempt_count).toBe(2);
    expect(row.completed_at).not.toBeNull();
  });
});

describe('reclaim paths', () => {
  it('path 2 (lease expiry) returns the job and charges an attempt', async () => {
    const [id] = await seedJobs({ maxAttempts: 5 });
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    await expireLease(id as string);

    const result = await reclaimExpiredLeases();
    expect(result.requeued).toEqual([id]);

    const row = await getJobRow(id as string);
    expect(row.state).toBe('PENDING');
    expect(row.attempt_count).toBe(1);
    expect(row.claimed_by).toBeNull();
  });

  it('path 1 (worker loss) returns everything that worker held, immediately', async () => {
    await seedJobs({ count: 5 });
    await registerWorker('w1');
    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 5 });
    expect(claimed).toHaveLength(5);

    // No lease has expired - this path does not wait for one.
    const result = await reclaimWorkerJobs('w1', 'socket_close');
    expect(result.requeued).toHaveLength(5);
    expect(await countByState()).toEqual({ PENDING: 5 });
  });

  it('sends a job straight to DEAD if reclaiming would exceed its budget', async () => {
    const [id] = await seedJobs({ maxAttempts: 1 });
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });

    const result = await reclaimWorkerJobs('w1', 'socket_close');
    expect(result.dead).toEqual([id]);
    expect((await getJobRow(id as string)).state).toBe('DEAD');
  });

  it('is safe when both paths race on the same job', async () => {
    await seedJobs({ count: 10 });
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 10 });
    await query(`UPDATE jobs SET lease_expires_at = now() - interval '1 second'`);

    const [immediate, expiry] = await Promise.all([
      reclaimWorkerJobs('w1', 'socket_close'),
      reclaimExpiredLeases(),
    ]);

    // Whichever gets there first wins; the total is exactly ten, never twenty.
    const total =
      immediate.requeued.length + immediate.dead.length + expiry.requeued.length + expiry.dead.length;
    expect(total).toBe(10);

    const { rows } = await query<{ max: number }>(
      'SELECT max(attempt_count)::int AS max FROM jobs',
    );
    expect(rows[0]?.max).toBe(1);
  });
});
