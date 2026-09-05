import { describe, expect, it } from 'vitest';
import {
  backoffCeilingMs,
  backoffIntervalSql,
  fullJitterBackoffMs,
  nextAttemptAt,
} from '../../src/domain/backoff.js';

const params = { baseMs: 250, capMs: 60_000 };

describe('backoffCeilingMs', () => {
  it('doubles per attempt', () => {
    expect(backoffCeilingMs(0, params)).toBe(250);
    expect(backoffCeilingMs(1, params)).toBe(500);
    expect(backoffCeilingMs(2, params)).toBe(1000);
    expect(backoffCeilingMs(3, params)).toBe(2000);
  });

  it('saturates at the cap instead of growing without bound', () => {
    expect(backoffCeilingMs(20, params)).toBe(60_000);
    expect(backoffCeilingMs(1000, params)).toBe(60_000);
    expect(Number.isFinite(backoffCeilingMs(1000, params))).toBe(true);
  });

  it('treats a negative attempt as the first attempt', () => {
    expect(backoffCeilingMs(-5, params)).toBe(250);
  });
});

describe('fullJitterBackoffMs', () => {
  it('samples the whole range [0, ceiling)', () => {
    expect(fullJitterBackoffMs(2, params, () => 0)).toBe(0);
    expect(fullJitterBackoffMs(2, params, () => 0.5)).toBe(500);
    expect(fullJitterBackoffMs(2, params, () => 0.999999)).toBe(999);
  });

  it('never exceeds the ceiling for any random value', () => {
    for (let attempt = 0; attempt < 12; attempt += 1) {
      for (let i = 0; i < 200; i += 1) {
        const delay = fullJitterBackoffMs(attempt, params);
        expect(delay).toBeGreaterThanOrEqual(0);
        expect(delay).toBeLessThan(Math.max(1, backoffCeilingMs(attempt, params)));
      }
    }
  });

  it('spreads a correlated failure burst instead of replaying it', () => {
    // This is the property full jitter exists for: 500 jobs that failed at the
    // same instant must not all retry at the same instant. With deterministic
    // exponential backoff every one of these would land in a single bucket.
    const attempt = 5; // ceiling = 8000ms
    const delays = Array.from({ length: 500 }, () => fullJitterBackoffMs(attempt, params));
    const buckets = new Set(delays.map((d) => Math.floor(d / 500)));
    expect(buckets.size).toBeGreaterThan(10);

    // And the spread should cover most of the window, not cluster at one end.
    const min = Math.min(...delays);
    const max = Math.max(...delays);
    expect(min).toBeLessThan(1000);
    expect(max).toBeGreaterThan(7000);
  });

  it('has a mean near half the ceiling, as uniform sampling implies', () => {
    const attempt = 4; // ceiling = 4000ms
    const samples = Array.from({ length: 5000 }, () => fullJitterBackoffMs(attempt, params));
    const mean = samples.reduce((a, b) => a + b, 0) / samples.length;
    expect(mean).toBeGreaterThan(1700);
    expect(mean).toBeLessThan(2300);
  });
});

describe('nextAttemptAt', () => {
  it('schedules relative to now', () => {
    const now = new Date('2026-01-01T00:00:00.000Z');
    const at = nextAttemptAt(3, params, now, () => 0.5);
    expect(at.toISOString()).toBe('2026-01-01T00:00:01.000Z');
  });
});

describe('backoffIntervalSql', () => {
  it('mirrors the TypeScript formula so retries behave the same in SQL', () => {
    const sql = backoffIntervalSql('attempt_count + 1', '$5', '$6', '$4');
    expect(sql).toContain('make_interval');
    expect(sql).toContain('$4 * LEAST($6, $5 * power(2, LEAST(attempt_count + 1, 30)))');
    expect(sql).toContain('/ 1000.0');
  });
});
