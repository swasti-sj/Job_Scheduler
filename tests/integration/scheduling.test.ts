import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { claimJobs } from '../../src/core/claim.js';
import { submitJob } from '../../src/core/jobStore.js';
import { requeueJob, ConflictError } from '../../src/core/jobStore.js';
import { failJob } from '../../src/core/lifecycle.js';
import { evaluateBreakers, recordOutcome, setBreaker, windowStats } from '../../src/core/breaker.js';
import { TokenBucketLimiter } from '../../src/redis/rateLimiter.js';
import { redis } from '../../src/redis/client.js';
import { query } from '../../src/db/pool.js';
import {
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

const base = {
  tenant_id: 'acme',
  queue_name: 'default',
  job_type: 'noop',
  payload: {},
  priority: 5 as const,
  depends_on: [],
};

describe('idempotency', () => {
  it('returns the original job for a duplicate key instead of creating a second', async () => {
    const first = await submitJob({ ...base, idempotency_key: 'order-42' });
    expect(first.deduplicated).toBe(false);

    const second = await submitJob({ ...base, idempotency_key: 'order-42' });
    expect(second.deduplicated).toBe(true);
    expect(second.job.id).toBe(first.job.id);

    const { rows } = await query<{ n: number }>('SELECT count(*)::int AS n FROM jobs');
    expect(rows[0]?.n).toBe(1);
  });

  it('survives a concurrent burst of the same key', async () => {
    // ON CONFLICT DO NOTHING plus a re-select is what makes this safe; a
    // check-then-insert would create duplicates here.
    const results = await Promise.all(
      Array.from({ length: 20 }, () => submitJob({ ...base, idempotency_key: 'burst' })),
    );
    const ids = new Set(results.map((r) => r.job.id));
    expect(ids.size).toBe(1);
    expect(results.filter((r) => !r.deduplicated)).toHaveLength(1);
  });

  it('scopes keys per tenant, so two tenants may reuse the same key', async () => {
    const a = await submitJob({ ...base, tenant_id: 't1', idempotency_key: 'shared' });
    const b = await submitJob({ ...base, tenant_id: 't2', idempotency_key: 'shared' });
    expect(a.job.id).not.toBe(b.job.id);
  });

  it('does not deduplicate jobs without a key', async () => {
    const a = await submitJob({ ...base });
    const b = await submitJob({ ...base });
    expect(a.job.id).not.toBe(b.job.id);
  });
});

describe('fair share across tenants', () => {
  it('caps a tenant that is monopolising capacity while another has work waiting', async () => {
    // Capacity is 10 slots (one worker, concurrency 10). With two active tenants
    // of equal weight the cap is 50% => 5 slots each.
    await registerWorker('w1', 10);
    await seedJobs({ tenant: 'hog', count: 100 });
    await seedJobs({ tenant: 'small', count: 10 });

    const first = await claimJobs({ workerId: 'w1', queue: 'default', limit: 5 });
    expect(first).toHaveLength(5);

    // Whatever the first batch was, once one tenant reaches its slice the claim
    // must start serving the other one.
    const claimedTenants = new Set(first.map((r) => r.tenant_id));
    const second = await claimJobs({ workerId: 'w1', queue: 'default', limit: 5 });
    for (const row of second) claimedTenants.add(row.tenant_id);

    expect(claimedTenants.size).toBe(2);

    const { rows } = await query<{ tenant_id: string; n: number }>(
      `SELECT tenant_id, count(*)::int AS n FROM jobs
       WHERE state = 'CLAIMED' GROUP BY tenant_id`,
    );
    for (const row of rows) {
      expect(row.n).toBeLessThanOrEqual(5);
    }
  });

  it('lets one tenant use the whole fleet when nobody else is waiting', async () => {
    // The cap binds only under contention: reserving 50% for an idle tenant
    // would waste half the fleet.
    await registerWorker('w1', 10);
    await seedJobs({ tenant: 'solo', count: 50 });

    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 10 });
    expect(claimed).toHaveLength(10);
    expect(new Set(claimed.map((c) => c.tenant_id))).toEqual(new Set(['solo']));
  });

  it('gives a heavier tenant a larger slice', async () => {
    await registerWorker('w1', 20);
    await seedJobs({ tenant: 'big', count: 100 });
    await seedJobs({ tenant: 'small', count: 100 });
    // weight 3 vs 1 => big's slice is min(0.5, 0.75) = 0.5 => 10 of 20 slots,
    // small's is min(0.5, 0.25) = 0.25 => 5 slots.
    await query(`UPDATE tenants SET weight = 3 WHERE tenant_id = 'big'`);
    await query(`UPDATE tenants SET weight = 1 WHERE tenant_id = 'small'`);

    for (let i = 0; i < 4; i += 1) {
      await claimJobs({ workerId: 'w1', queue: 'default', limit: 20 });
    }

    const { rows } = await query<{ tenant_id: string; n: number }>(
      `SELECT tenant_id, count(*)::int AS n FROM jobs
       WHERE state = 'CLAIMED' GROUP BY tenant_id ORDER BY tenant_id`,
    );
    const counts = Object.fromEntries(rows.map((r) => [r.tenant_id, r.n]));
    // The headroom limit is exact, not approximate: big gets 10 of 20 slots
    // (capped by fairShareMaxPct), small gets its weighted 5.
    expect(counts['big']).toBe(10);
    expect(counts['small']).toBe(5);
  });
});

describe('token bucket rate limiter', () => {
  const limiter = new TokenBucketLimiter(redis);

  it('grants up to the capacity and then refuses', async () => {
    await limiter.reset('rl-test');
    const first = await limiter.take('rl-test', 10, { capacity: 10, refillPerSecond: 0.0001 });
    expect(first.granted).toBe(10);

    const second = await limiter.take('rl-test', 5, { capacity: 10, refillPerSecond: 0.0001 });
    expect(second.granted).toBe(0);
  });

  it('grants partially rather than all-or-nothing', async () => {
    await limiter.reset('rl-partial');
    await limiter.take('rl-partial', 8, { capacity: 10, refillPerSecond: 0.0001 });
    const partial = await limiter.take('rl-partial', 5, { capacity: 10, refillPerSecond: 0.0001 });
    expect(partial.granted).toBe(2);
  });

  it('refills over time', async () => {
    await limiter.reset('rl-refill');
    await limiter.take('rl-refill', 10, { capacity: 10, refillPerSecond: 100 });
    await new Promise((r) => setTimeout(r, 150));
    const after = await limiter.take('rl-refill', 10, { capacity: 10, refillPerSecond: 100 });
    expect(after.granted).toBeGreaterThan(0);
  });

  it('is atomic under concurrent takes', async () => {
    // The reason this is a Lua script: 20 parallel read-modify-writes must not
    // collectively hand out more than the bucket holds.
    await limiter.reset('rl-atomic');
    const results = await Promise.all(
      Array.from({ length: 20 }, () =>
        limiter.take('rl-atomic', 5, { capacity: 50, refillPerSecond: 0.0001 }),
      ),
    );
    const total = results.reduce((sum, r) => sum + r.granted, 0);
    expect(total).toBe(50);
  });
});

describe('circuit breaker', () => {
  it('opens a job type once the failure rate crosses the threshold', async () => {
    for (let i = 0; i < 20; i += 1) await recordOutcome('bad_type', false);
    for (let i = 0; i < 2; i += 1) await recordOutcome('bad_type', true);

    const stats = await windowStats('bad_type');
    expect(stats.total).toBe(22);
    expect(stats.failureRate).toBeGreaterThan(0.5);

    await evaluateBreakers();
    const { rows } = await query<{ state: string }>(
      `SELECT state FROM circuit_breakers WHERE job_type = 'bad_type'`,
    );
    expect(rows[0]?.state).toBe('OPEN');
  });

  it('leaves a healthy job type closed', async () => {
    for (let i = 0; i < 30; i += 1) await recordOutcome('good_type', true);
    for (let i = 0; i < 2; i += 1) await recordOutcome('good_type', false);
    await evaluateBreakers();
    const { rows } = await query<{ state: string }>(
      `SELECT state FROM circuit_breakers WHERE job_type = 'good_type'`,
    );
    expect(rows[0]?.state ?? 'CLOSED').toBe('CLOSED');
  });

  it('does not trip on too few samples', async () => {
    for (let i = 0; i < 3; i += 1) await recordOutcome('rare_type', false);
    await evaluateBreakers();
    const { rows } = await query<{ state: string }>(
      `SELECT state FROM circuit_breakers WHERE job_type = 'rare_type'`,
    );
    expect(rows[0]?.state ?? 'CLOSED').toBe('CLOSED');
  });

  it('stops dispatching a paused job type without touching the others', async () => {
    await seedJobs({ jobType: 'paused_type', count: 5 });
    await seedJobs({ jobType: 'fine_type', count: 5 });
    await registerWorker('w1', 20);
    await setBreaker('paused_type', 'OPEN');

    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 20 });
    expect(claimed).toHaveLength(5);
    expect(new Set(claimed.map((c) => c.job_type))).toEqual(new Set(['fine_type']));

    // Closing it releases the backlog.
    await setBreaker('paused_type', 'CLOSED');
    const after = await claimJobs({ workerId: 'w1', queue: 'default', limit: 20 });
    expect(after).toHaveLength(5);
  });
});

describe('dead letter requeue', () => {
  it('requeues a DEAD job and resets its retry budget', async () => {
    const [id] = await seedJobs({ maxAttempts: 1 });
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    await failJob(id as string, 'w1', 'boom');
    expect((await getJobRow(id as string)).state).toBe('DEAD');

    const job = await requeueJob(id as string);
    expect(job.state).toBe('PENDING');
    expect(job.attempt_count).toBe(0);
    expect(job.completed_at).toBeNull();

    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    expect(claimed[0]?.id).toBe(id);
  });

  it('refuses to requeue a job that is not DEAD', async () => {
    const [id] = await seedJobs();
    await expect(requeueJob(id as string)).rejects.toThrow(ConflictError);
  });

  it('can requeue without resetting attempts', async () => {
    const [id] = await seedJobs({ maxAttempts: 1 });
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    await failJob(id as string, 'w1', 'boom');

    const job = await requeueJob(id as string, false);
    expect(job.attempt_count).toBe(1);
  });
});
