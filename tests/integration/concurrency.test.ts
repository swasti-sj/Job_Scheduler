import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { claimJobs } from '../../src/core/claim.js';
import { query } from '../../src/db/pool.js';
import { registerWorker, resetDatabase, seedJobs, setupDatabase, teardown } from './helpers.js';

beforeAll(setupDatabase);
afterAll(teardown);
beforeEach(resetDatabase);

/**
 * The core exactly-once claim: N workers hitting the same queue at the same
 * instant must partition the work, never share it.
 *
 * This is the test that would fail if the claim ran on a pooled `query()`
 * instead of a checked-out client, because BEGIN and the locking SELECT would
 * land on different connections and the row locks would not span the claim.
 */
describe('20 workers claiming simultaneously', () => {
  it('never hands the same job to two workers', async () => {
    const total = 400;
    const ids = await seedJobs({ count: total });
    const workers = Array.from({ length: 20 }, (_, i) => `w${i}`);
    await Promise.all(workers.map((w) => registerWorker(w, 32)));

    // One synchronised burst, then keep going until the queue is drained.
    const claimedBy = new Map<string, string>();
    const duplicates: Array<{ job: string; first: string; second: string }> = [];

    for (let round = 0; round < 20; round += 1) {
      const results = await Promise.all(
        workers.map(async (workerId) => ({
          workerId,
          rows: await claimJobs({ workerId, queue: 'default', limit: 20 }),
        })),
      );

      for (const { workerId, rows } of results) {
        for (const row of rows) {
          const existing = claimedBy.get(row.id);
          if (existing !== undefined) {
            duplicates.push({ job: row.id, first: existing, second: workerId });
          } else {
            claimedBy.set(row.id, workerId);
          }
        }
      }
      if (claimedBy.size === total) break;
    }

    expect(duplicates).toEqual([]);
    expect(claimedBy.size).toBe(total);

    // And the database agrees: every job is claimed exactly once, by one worker.
    const { rows } = await query<{ state: string; n: number; owners: number }>(
      `SELECT state::text AS state, count(*)::int AS n, count(DISTINCT claimed_by)::int AS owners
       FROM jobs GROUP BY state`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]?.state).toBe('CLAIMED');
    expect(rows[0]?.n).toBe(total);

    // No job ever received two CLAIMED events.
    const { rows: events } = await query<{ job_id: string; n: number }>(
      `SELECT job_id, count(*)::int AS n FROM job_events
       WHERE to_state = 'CLAIMED' GROUP BY job_id HAVING count(*) > 1`,
    );
    expect(events).toEqual([]);
    expect(new Set(ids).size).toBe(total);
  });

  it('does not block: concurrent claimers all make progress in one round', async () => {
    // SKIP LOCKED's real value over a blocking lock. If claimers serialised,
    // only the first would get rows in a single simultaneous round.
    await seedJobs({ count: 200 });
    const workers = Array.from({ length: 10 }, (_, i) => `p${i}`);
    await Promise.all(workers.map((w) => registerWorker(w, 32)));

    const results = await Promise.all(
      workers.map((workerId) => claimJobs({ workerId, queue: 'default', limit: 5 })),
    );

    const productive = results.filter((rows) => rows.length > 0).length;
    expect(productive).toBeGreaterThanOrEqual(8);

    // Not necessarily the full 50: claimers rank overlapping candidate windows,
    // so one that arrives late may find part of its window locked and come back
    // short. That is fine and self-correcting - the dispatcher simply claims
    // again - but it is why this asserts "most" rather than "all".
    const total = results.flat().length;
    expect(total).toBeGreaterThanOrEqual(35);
    expect(total).toBeLessThanOrEqual(50);

    // Whatever was handed out was handed out exactly once.
    expect(new Set(results.flat().map((r) => r.id)).size).toBe(total);
  });

  it('leaves nothing claimed twice when claims race with reclaims', async () => {
    await seedJobs({ count: 100 });
    const workers = ['a', 'b', 'c', 'd'];
    await Promise.all(workers.map((w) => registerWorker(w, 32)));

    for (let round = 0; round < 5; round += 1) {
      await Promise.all(workers.map((w) => claimJobs({ workerId: w, queue: 'default', limit: 25 })));
      // Half the leases expire under everyone's feet.
      await query(
        `UPDATE jobs SET lease_expires_at = now() - interval '1 second'
         WHERE state = 'CLAIMED' AND random() < 0.5`,
      );
      const { reclaimExpiredLeases } = await import('../../src/core/reclaim.js');
      await reclaimExpiredLeases(200);
      await query(`UPDATE jobs SET scheduled_for = now() WHERE state = 'PENDING'`);
    }

    // A job may legitimately be claimed many times across rounds - that is what
    // a retry is. The invariant that must hold is that a claim is only ever
    // handed out again after the previous holder was charged an attempt, so:
    //
    //     claims <= attempts_charged + (1 if currently held)
    //
    // A double-claim would break this by producing a claim nobody paid for.
    const { rows } = await query<{ job_id: string; claims: number; budget: number }>(
      `SELECT j.id AS job_id,
              (SELECT count(*)::int FROM job_events e
               WHERE e.job_id = j.id AND e.to_state = 'CLAIMED') AS claims,
              j.attempt_count + (CASE WHEN j.state IN ('CLAIMED','RUNNING') THEN 1 ELSE 0 END)
                AS budget
       FROM jobs j`,
    );
    const violations = rows.filter((r) => r.claims > r.budget);
    expect(violations).toEqual([]);

    // And no in-flight job is ever ownerless.
    const { rows: overlap } = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM jobs
       WHERE state IN ('CLAIMED','RUNNING') AND claimed_by IS NULL`,
    );
    expect(overlap[0]?.n).toBe(0);
  });
});
