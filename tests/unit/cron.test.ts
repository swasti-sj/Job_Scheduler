import { describe, expect, it } from 'vitest';
import { CronParseError, nextRun, parseCron } from '../../src/domain/cron.js';

describe('parseCron', () => {
  it('expands wildcards', () => {
    const fields = parseCron('* * * * *');
    expect(fields.minute.size).toBe(60);
    expect(fields.hour.size).toBe(24);
    expect(fields.domRestricted).toBe(false);
    expect(fields.dowRestricted).toBe(false);
  });

  it('expands steps, ranges and lists', () => {
    expect([...parseCron('*/15 * * * *').minute]).toEqual([0, 15, 30, 45]);
    expect([...parseCron('10-13 * * * *').minute]).toEqual([10, 11, 12, 13]);
    expect([...parseCron('0,30 * * * *').minute]).toEqual([0, 30]);
    expect([...parseCron('0-10/5 * * * *').minute]).toEqual([0, 5, 10]);
  });

  it('normalises Sunday given as 7', () => {
    expect(parseCron('0 0 * * 7').dayOfWeek.has(0)).toBe(true);
  });

  it('rejects malformed expressions rather than scheduling something surprising', () => {
    expect(() => parseCron('* * * *')).toThrow(CronParseError);
    expect(() => parseCron('60 * * * *')).toThrow(CronParseError);
    expect(() => parseCron('* 24 * * *')).toThrow(CronParseError);
    expect(() => parseCron('* * 0 * *')).toThrow(CronParseError);
    expect(() => parseCron('*/0 * * * *')).toThrow(CronParseError);
    expect(() => parseCron('10-5 * * * *')).toThrow(CronParseError);
  });
});

describe('nextRun', () => {
  it('is strictly after the given instant', () => {
    const from = new Date('2026-03-01T10:00:00.000Z');
    expect(nextRun('* * * * *', from).toISOString()).toBe('2026-03-01T10:01:00.000Z');
  });

  it('ignores sub-minute precision in the source instant', () => {
    const from = new Date('2026-03-01T10:00:30.500Z');
    expect(nextRun('* * * * *', from).toISOString()).toBe('2026-03-01T10:01:00.000Z');
  });

  it('rolls forward to the next matching hour', () => {
    expect(nextRun('30 2 * * *', new Date('2026-03-01T10:00:00Z')).toISOString()).toBe(
      '2026-03-02T02:30:00.000Z',
    );
  });

  it('rolls forward to the next matching day of week', () => {
    // 2026-03-01 is a Sunday; the next Monday 09:00 is the 2nd.
    expect(nextRun('0 9 * * 1', new Date('2026-03-01T10:00:00Z')).toISOString()).toBe(
      '2026-03-02T09:00:00.000Z',
    );
  });

  it('handles a leap-year-only schedule without scanning minute by minute', () => {
    const started = Date.now();
    const next = nextRun('0 0 29 2 *', new Date('2026-03-01T00:00:00Z'));
    expect(next.toISOString()).toBe('2028-02-29T00:00:00.000Z');
    expect(Date.now() - started).toBeLessThan(500);
  });

  it('matches either day field when both are restricted, as cron does', () => {
    // "1st of the month OR any Monday" - the 2nd is a Monday.
    const next = nextRun('0 0 1 * 1', new Date('2026-03-01T12:00:00Z'));
    expect(next.toISOString()).toBe('2026-03-02T00:00:00.000Z');
  });

  it('throws for an expression that can never fire', () => {
    expect(() => nextRun('0 0 30 2 *', new Date('2026-01-01T00:00:00Z'))).toThrow(CronParseError);
  });

  it('produces a strictly increasing sequence when chained', () => {
    let cursor = new Date('2026-01-01T00:00:00Z');
    for (let i = 0; i < 50; i += 1) {
      const next = nextRun('*/7 * * * *', cursor);
      expect(next.getTime()).toBeGreaterThan(cursor.getTime());
      expect(next.getUTCMinutes() % 7).toBe(0);
      cursor = next;
    }
  });
});
