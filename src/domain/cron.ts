/**
 * A minimal 5-field cron parser (minute hour day-of-month month day-of-week).
 *
 * Written rather than pulled in because the leader needs exactly this and
 * nothing else, and a scheduling primitive whose behaviour we assert in tests is
 * worth ~100 lines. Supports `*`, `a`, `a-b`, `a-b/n`, `*` + `/n`, and
 * comma-separated lists of those. Day-of-week 0 and 7 both mean Sunday.
 *
 * `nextRun` steps field by field (bump the month, then the day, then the hour)
 * instead of ticking one minute at a time, so finding "02:30 on the 29th of
 * February" costs a handful of iterations rather than two million.
 */

export interface CronFields {
  minute: Set<number>;
  hour: Set<number>;
  dayOfMonth: Set<number>;
  month: Set<number>;
  dayOfWeek: Set<number>;
  /** True when the field was `*`; a restricted dom OR dow is the cron rule. */
  domRestricted: boolean;
  dowRestricted: boolean;
}

export class CronParseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CronParseError';
  }
}

const RANGES: Record<string, [number, number]> = {
  minute: [0, 59],
  hour: [0, 23],
  dayOfMonth: [1, 31],
  month: [1, 12],
  dayOfWeek: [0, 6],
};

function parseField(field: string, name: string): Set<number> {
  const range = RANGES[name];
  if (range === undefined) throw new CronParseError(`unknown cron field ${name}`);
  const [min, max] = range;
  const out = new Set<number>();

  for (const part of field.split(',')) {
    const [spec, stepRaw] = part.split('/');
    if (spec === undefined || spec === '') throw new CronParseError(`bad cron field "${field}"`);
    const step = stepRaw === undefined ? 1 : Number.parseInt(stepRaw, 10);
    if (!Number.isInteger(step) || step < 1) throw new CronParseError(`bad step in "${part}"`);

    let lo: number;
    let hi: number;
    if (spec === '*') {
      lo = min;
      hi = max;
    } else if (spec.includes('-')) {
      const [a, b] = spec.split('-');
      lo = Number.parseInt(a ?? '', 10);
      hi = Number.parseInt(b ?? '', 10);
    } else {
      lo = Number.parseInt(spec, 10);
      hi = lo;
    }
    if (!Number.isInteger(lo) || !Number.isInteger(hi)) {
      throw new CronParseError(`bad cron value "${part}"`);
    }
    // Sunday is both 0 and 7 in the wild; normalise to 0.
    if (name === 'dayOfWeek') {
      if (lo === 7) lo = 0;
      if (hi === 7) hi = 0;
    }
    if (lo < min || hi > max || hi < lo) {
      throw new CronParseError(`cron value out of range in "${part}" for ${name}`);
    }
    for (let v = lo; v <= hi; v += step) out.add(v);
  }

  if (out.size === 0) throw new CronParseError(`cron field "${field}" matches nothing`);
  return out;
}

export function parseCron(expression: string): CronFields {
  const parts = expression.trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new CronParseError(`expected 5 cron fields, got ${parts.length}: "${expression}"`);
  }
  const [minute, hour, dom, month, dow] = parts as [string, string, string, string, string];
  return {
    minute: parseField(minute, 'minute'),
    hour: parseField(hour, 'hour'),
    dayOfMonth: parseField(dom, 'dayOfMonth'),
    month: parseField(month, 'month'),
    dayOfWeek: parseField(dow, 'dayOfWeek'),
    domRestricted: dom !== '*',
    dowRestricted: dow !== '*',
  };
}

function dayMatches(fields: CronFields, date: Date): boolean {
  const dom = fields.dayOfMonth.has(date.getUTCDate());
  const dow = fields.dayOfWeek.has(date.getUTCDay());
  // Standard cron semantics: when both day fields are restricted the day matches
  // if *either* does; when only one is restricted, only that one counts.
  if (fields.domRestricted && fields.dowRestricted) return dom || dow;
  if (fields.domRestricted) return dom;
  if (fields.dowRestricted) return dow;
  return true;
}

/** Next matching instant strictly after `from`, in UTC. */
export function nextRun(expression: string, from: Date = new Date()): Date {
  const fields = parseCron(expression);
  const cursor = new Date(from.getTime());
  cursor.setUTCSeconds(0, 0);
  cursor.setUTCMinutes(cursor.getUTCMinutes() + 1);

  // Four years covers every leap-year edge case; beyond that the expression
  // matches nothing (e.g. "0 0 30 2 *") and we say so rather than spinning.
  const limit = new Date(cursor.getTime() + 4 * 366 * 24 * 3600 * 1000);

  while (cursor < limit) {
    if (!fields.month.has(cursor.getUTCMonth() + 1)) {
      cursor.setUTCMonth(cursor.getUTCMonth() + 1, 1);
      cursor.setUTCHours(0, 0, 0, 0);
      continue;
    }
    if (!dayMatches(fields, cursor)) {
      cursor.setUTCDate(cursor.getUTCDate() + 1);
      cursor.setUTCHours(0, 0, 0, 0);
      continue;
    }
    if (!fields.hour.has(cursor.getUTCHours())) {
      cursor.setUTCHours(cursor.getUTCHours() + 1, 0, 0, 0);
      continue;
    }
    if (!fields.minute.has(cursor.getUTCMinutes())) {
      cursor.setUTCMinutes(cursor.getUTCMinutes() + 1, 0, 0);
      continue;
    }
    return cursor;
  }

  throw new CronParseError(`cron expression "${expression}" has no next occurrence`);
}
