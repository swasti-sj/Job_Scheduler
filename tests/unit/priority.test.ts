import { describe, expect, it } from 'vitest';
import {
  compareForClaim,
  effectivePriority,
  agingSqlExpression,
  waitSeconds,
} from '../../src/domain/priority.js';

const params = { agingIntervalSeconds: 30, agingMaxBoost: 9 };

describe('effectivePriority', () => {
  it('returns the raw priority for a job that has not waited', () => {
    expect(effectivePriority(5, 0, params)).toBe(5);
    expect(effectivePriority(0, 29, params)).toBe(0);
  });

  it('gains exactly one level per aging interval, on the interval boundary', () => {
    expect(effectivePriority(5, 29.999, params)).toBe(5);
    expect(effectivePriority(5, 30, params)).toBe(4);
    expect(effectivePriority(5, 59, params)).toBe(4);
    expect(effectivePriority(5, 60, params)).toBe(3);
    expect(effectivePriority(5, 150, params)).toBe(0);
  });

  it('clamps the boost so an ancient job cannot outrank everything forever', () => {
    // Without the clamp a job waiting a week would sit at priority -20160 and
    // permanently precede every future job, turning starvation of the low band
    // into starvation of the high band.
    expect(effectivePriority(9, 86_400, { agingIntervalSeconds: 30, agingMaxBoost: 9 })).toBe(0);
    expect(effectivePriority(3, 86_400, { agingIntervalSeconds: 30, agingMaxBoost: 2 })).toBe(1);
  });

  it('treats negative waits as zero rather than penalising the job', () => {
    expect(effectivePriority(4, -100, params)).toBe(4);
  });

  it('rejects a non-positive aging interval instead of dividing by zero', () => {
    expect(() => effectivePriority(1, 10, { agingIntervalSeconds: 0, agingMaxBoost: 9 })).toThrow();
  });

  it('honours a configured aging interval', () => {
    const slow = { agingIntervalSeconds: 300, agingMaxBoost: 9 };
    expect(effectivePriority(9, 299, slow)).toBe(9);
    expect(effectivePriority(9, 300, slow)).toBe(8);
  });
});

describe('starvation', () => {
  it('lets a waiting low-priority job overtake a fresh high-priority job', () => {
    const now = new Date('2026-01-01T00:10:00Z');
    // priority 9, waited 5 minutes -> effective 0
    const old = {
      priority: 9,
      created_at: new Date('2026-01-01T00:05:00Z'),
      scheduled_for: new Date('2026-01-01T00:05:00Z'),
    };
    // priority 1, just arrived -> effective 1
    const fresh = {
      priority: 1,
      created_at: now,
      scheduled_for: now,
    };
    expect(compareForClaim(old, fresh, params, now)).toBeLessThan(0);
  });

  it('keeps FIFO order within the same effective priority', () => {
    const now = new Date('2026-01-01T00:00:10Z');
    const earlier = {
      priority: 5,
      created_at: new Date('2026-01-01T00:00:00Z'),
      scheduled_for: new Date('2026-01-01T00:00:00Z'),
    };
    const later = {
      priority: 5,
      created_at: new Date('2026-01-01T00:00:05Z'),
      scheduled_for: new Date('2026-01-01T00:00:05Z'),
    };
    expect(compareForClaim(earlier, later, params, now)).toBeLessThan(0);
    expect(compareForClaim(later, earlier, params, now)).toBeGreaterThan(0);
  });

  it('still prefers the higher-priority job when neither has aged', () => {
    const now = new Date('2026-01-01T00:00:00Z');
    const high = { priority: 0, created_at: now, scheduled_for: now };
    const low = { priority: 9, created_at: now, scheduled_for: now };
    expect(compareForClaim(high, low, params, now)).toBeLessThan(0);
  });
});

describe('waitSeconds', () => {
  it('measures elapsed seconds and floors at zero for future timestamps', () => {
    const now = new Date('2026-01-01T00:01:00Z');
    expect(waitSeconds(new Date('2026-01-01T00:00:00Z'), now)).toBe(60);
    expect(waitSeconds(new Date('2026-01-01T00:02:00Z'), now)).toBe(0);
  });
});

describe('agingSqlExpression', () => {
  it('mirrors the TypeScript formula, including the LEAST clamp', () => {
    // The claim query orders by this expression; if the two definitions drift,
    // the queue behaves differently from every unit test above.
    const sql = agingSqlExpression('priority', 'created_at', '$6', '$7');
    expect(sql).toContain('priority - LEAST(');
    expect(sql).toContain('floor(EXTRACT(EPOCH FROM (now() - created_at)) / $6)');
    expect(sql).toContain('$7');
  });
});
