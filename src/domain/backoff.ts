/**
 * Exponential backoff with *full* jitter:
 *
 *     delay = random(0, min(cap, base * 2^attempt))
 *
 * Full jitter (rather than equal jitter or a fixed exponential) because the
 * failure mode we actually care about is correlated retry storms: when a
 * downstream dependency blips, every in-flight job of that type fails within a
 * few milliseconds of the others. Deterministic backoff replays that thundering
 * herd at t+1s, t+2s, t+4s ... forever. Sampling uniformly from [0, ceiling)
 * spreads the herd flat across the whole window, which is the property that
 * matters; the slightly lower average delay is a good trade.
 */

export interface BackoffParams {
  baseMs: number;
  capMs: number;
}

export function backoffCeilingMs(attempt: number, { baseMs, capMs }: BackoffParams): number {
  const safeAttempt = Math.max(0, Math.min(attempt, 30)); // 2^30 * base stays finite
  return Math.min(capMs, baseMs * 2 ** safeAttempt);
}

export function fullJitterBackoffMs(
  attempt: number,
  params: BackoffParams,
  random: () => number = Math.random,
): number {
  return Math.floor(random() * backoffCeilingMs(attempt, params));
}

export function nextAttemptAt(
  attempt: number,
  params: BackoffParams,
  now: Date = new Date(),
  random: () => number = Math.random,
): Date {
  return new Date(now.getTime() + fullJitterBackoffMs(attempt, params, random));
}

/**
 * The SQL form of the backoff, so the retry decision and the reschedule happen
 * in one statement (we need the row's attempt_count, which we do not know before
 * the UPDATE runs). The jitter factor is supplied by the caller from JS rather
 * than Postgres random(), which keeps it injectable and makes the behaviour
 * unit-testable against fullJitterBackoffMs above.
 */
export function backoffIntervalSql(
  attemptColumn: string,
  baseMsParam: string,
  capMsParam: string,
  jitterParam: string,
): string {
  return (
    'make_interval(secs => (' +
    jitterParam +
    ' * LEAST(' +
    capMsParam +
    ', ' +
    baseMsParam +
    ' * power(2, LEAST(' +
    attemptColumn +
    ', 30)))) / 1000.0)'
  );
}
