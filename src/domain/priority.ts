/**
 * Effective priority = aging.
 *
 * Raw priority is 0 (most urgent) .. 9 (least urgent). A job's *effective*
 * priority decreases - i.e. becomes more urgent - the longer it has waited:
 *
 *     effective = priority - floor(wait_seconds / aging_interval_seconds)
 *
 * clamped so a job cannot age past `priority - agingMaxBoost` (and never below
 * MIN_PRIORITY - agingMaxBoost). Without the clamp an old low-priority job would
 * eventually outrank *every* future high-priority job forever, which converts
 * starvation of the low band into starvation of the high band.
 *
 * The same expression is inlined into the claim query's ORDER BY (see
 * `agingSqlExpression`) so aging needs no background pass: it is a pure function
 * of `created_at` and `now()`, evaluated at claim time.
 */

export interface AgingParams {
  /** Seconds of waiting that buy one priority level. Must be > 0. */
  agingIntervalSeconds: number;
  /** Maximum number of levels a job may gain. */
  agingMaxBoost: number;
}

export function effectivePriority(
  priority: number,
  waitSeconds: number,
  { agingIntervalSeconds, agingMaxBoost }: AgingParams,
): number {
  if (agingIntervalSeconds <= 0) throw new Error('agingIntervalSeconds must be > 0');
  const wait = Math.max(0, waitSeconds);
  const boost = Math.min(Math.floor(wait / agingIntervalSeconds), Math.max(0, agingMaxBoost));
  return priority - boost;
}

/**
 * The SQL form of `effectivePriority`, used verbatim inside the claim query's
 * ORDER BY. `$${p}` placeholders are filled by the caller with the aging
 * interval and the max boost, keeping the TS and SQL definitions in lockstep.
 */
export function agingSqlExpression(
  priorityColumn: string,
  createdAtColumn: string,
  intervalParam: string,
  maxBoostParam: string,
): string {
  return (
    `(${priorityColumn} - LEAST(` +
    `floor(EXTRACT(EPOCH FROM (now() - ${createdAtColumn})) / ${intervalParam}), ${maxBoostParam}` +
    `))::double precision`
  );
}

/** How long a job has been waiting, in seconds, at `now`. */
export function waitSeconds(createdAt: Date, now: Date = new Date()): number {
  return Math.max(0, (now.getTime() - createdAt.getTime()) / 1000);
}

/**
 * Ordering comparator matching the claim query: lower effective priority first,
 * ties broken by earlier `scheduled_for` (FIFO within a band).
 */
export function compareForClaim(
  a: { priority: number; created_at: Date; scheduled_for: Date },
  b: { priority: number; created_at: Date; scheduled_for: Date },
  params: AgingParams,
  now: Date = new Date(),
): number {
  const ea = effectivePriority(a.priority, waitSeconds(a.created_at, now), params);
  const eb = effectivePriority(b.priority, waitSeconds(b.created_at, now), params);
  if (ea !== eb) return ea - eb;
  return a.scheduled_for.getTime() - b.scheduled_for.getTime();
}
