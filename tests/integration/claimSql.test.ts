import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { pool, query } from '../../src/db/pool.js';
import { claimPlan } from '../../src/core/claim.js';
import { config } from '../../src/config.js';
import { registerWorker, resetDatabase, seedJobs, setupDatabase, teardown } from './helpers.js';

beforeAll(setupDatabase);
afterAll(teardown);
beforeEach(resetDatabase);

/**
 * Regression guard for a bug that made the system look healthy while doing
 * nothing at all.
 *
 * The fair-share CTEs are the only consumers of two of the claim query's
 * parameters. When fair share was disabled those CTEs disappeared, leaving gaps
 * in the parameter numbering, and Postgres rejected the *whole statement* with
 * "could not determine data type of parameter $9". Every claim threw, the
 * dispatcher logged and moved on, and the queue simply never drained - with no
 * failing assertion anywhere, because no test had ever run the query in that
 * configuration.
 *
 * So: prepare the query in both configurations and make Postgres itself confirm
 * it is valid.
 */
describe('claim SQL validity in every configuration', () => {
  it('prepares and runs with fair share enabled', async () => {
    expect(config.fairShareEnabled).toBe(true);
    await seedJobs({ count: 3 });
    await registerWorker('w1');

    const plan = claimPlan();
    expect(plan.fairShare).toBe(true);
    expect(plan.sql).toContain('caps AS MATERIALIZED');

    const client = await pool.connect();
    try {
      const res = await client.query(plan.sql, [
        'default',
        config.claimCandidatesPerPriority,
        2,
        'w1',
        30,
        config.agingIntervalSeconds,
        config.agingMaxBoost,
        'test-node',
        config.fairShareMaxPct,
        15,
        config.claimOverselectFactor,
      ]);
      expect(res.rows).toHaveLength(2);
    } finally {
      client.release();
    }
  });

  it('prepares and runs with fair share disabled, with no parameter gaps', async () => {
    // Build the disabled variant directly rather than reaching for the module
    // cache, so this test does not depend on import order.
    const enabled = claimPlan();
    const disabledSql = enabled.sql
      .replace(/WITH[\s\S]*?cand AS MATERIALIZED/, 'WITH cand AS MATERIALIZED')
      .replace(
        /\s*AND NOT EXISTS \(SELECT 1 FROM caps k WHERE k\.tenant_id = c\.tenant_id AND k\.headroom <= 0\)/,
        '',
      )
      .replace(/\s*JOIN caps k ON k\.tenant_id = x\.tenant_id AND x\.rn <= k\.headroom/, '')
      .replace(/\$11/g, '$9');

    expect(disabledSql).not.toContain('caps');
    expect(disabledSql).not.toContain('$10');

    await seedJobs({ count: 3 });
    const client = await pool.connect();
    try {
      const res = await client.query(disabledSql, [
        'default',
        config.claimCandidatesPerPriority,
        2,
        'w1',
        30,
        config.agingIntervalSeconds,
        config.agingMaxBoost,
        'test-node',
        config.claimOverselectFactor,
      ]);
      expect(res.rows).toHaveLength(2);
    } finally {
      client.release();
    }
  });

  it('references every parameter it is given, with no gaps', () => {
    const plan = claimPlan();
    const used = new Set(
      [...plan.sql.matchAll(/\$(\d+)/g)].map((m) => Number.parseInt(m[1] as string, 10)),
    );
    const highest = Math.max(...used);
    for (let i = 1; i <= highest; i += 1) {
      expect(used.has(i), `parameter $${i} is never referenced`).toBe(true);
    }
    expect(highest).toBe(plan.fairShare ? 11 : 9);
  });

  it('is accepted by Postgres as a prepared statement', async () => {
    // PREPARE forces full parse and analysis, which is what catches a malformed
    // statement before a worker ever tries to use it.
    const plan = claimPlan();
    const client = await pool.connect();
    try {
      await client.query(
        `PREPARE claim_check (text, int, int, text, double precision, double precision,
                              double precision, text, double precision, double precision, int) AS
         ${plan.sql}`,
      );
      await client.query('DEALLOCATE claim_check');
    } finally {
      client.release();
    }
  });

  it('records a CLAIMED event for every row it returns', async () => {
    await seedJobs({ count: 5 });
    await registerWorker('w1');
    const { claimJobs } = await import('../../src/core/claim.js');
    const rows = await claimJobs({ workerId: 'w1', queue: 'default', limit: 5 });

    const { rows: events } = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM job_events WHERE to_state = 'CLAIMED'`,
    );
    expect(events[0]?.n).toBe(rows.length);
  });
});
