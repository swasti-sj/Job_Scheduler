/**
 * Job / worker state machines.
 *
 * States are a const object plus a string-literal union (not a TS `enum`) so the
 * values are exactly the Postgres `job_state` enum labels - no mapping layer, and
 * a row from `pg` can be narrowed directly.
 *
 * The transition table below is `as const`, which makes it a *type* as well as a
 * value. `transition()` is generic over the source state, so an illegal edge is a
 * compile error, not a runtime surprise:
 *
 *     transition(JobState.SUCCEEDED, JobState.PENDING)  // ts(2345): terminal
 *     transition(JobState.PENDING, JobState.RUNNING)    // ts(2345): must CLAIM first
 */

export const JobState = {
  /** Waiting on `depends_on`; not claimable. */
  BLOCKED: 'BLOCKED',
  /** Claimable once `scheduled_for <= now()`. */
  PENDING: 'PENDING',
  /** Locked to a worker, lease running, execution not yet acknowledged. */
  CLAIMED: 'CLAIMED',
  /** Worker acknowledged and is executing. */
  RUNNING: 'RUNNING',
  /** Terminal, success. */
  SUCCEEDED: 'SUCCEEDED',
  /** One attempt failed. Transient: immediately re-routed to PENDING or DEAD. */
  FAILED: 'FAILED',
  /** Terminal-ish: dead letter queue. Only a manual requeue leaves it. */
  DEAD: 'DEAD',
  /** Terminal, cancelled by an operator or by a dead/cancelled dependency. */
  CANCELLED: 'CANCELLED',
} as const;

export type JobState = (typeof JobState)[keyof typeof JobState];

export const WorkerState = {
  IDLE: 'IDLE',
  BUSY: 'BUSY',
  DEAD: 'DEAD',
} as const;

export type WorkerState = (typeof WorkerState)[keyof typeof WorkerState];

/**
 * The complete, explicit transition table. Every edge in the system appears here
 * exactly once; `satisfies` guarantees no state is forgotten when one is added.
 */
export const JOB_TRANSITIONS = {
  // Dependencies all SUCCEEDED -> runnable. Any dep DEAD/CANCELLED -> cascade.
  BLOCKED: ['PENDING', 'CANCELLED'],
  // Claiming is the only way out of the ready queue; cancel while still waiting.
  PENDING: ['CLAIMED', 'CANCELLED'],
  // Worker acks -> RUNNING. Lease expiry / socket close -> back to PENDING (retry
  // budget permitting) or straight to DEAD when the budget is spent.
  CLAIMED: ['RUNNING', 'PENDING', 'FAILED', 'DEAD'],
  // Execution outcome. RUNNING -> PENDING/DEAD also covers reclaim of a worker
  // that died mid-execution.
  RUNNING: ['SUCCEEDED', 'FAILED', 'PENDING', 'DEAD'],
  // FAILED is a routing state: retry (PENDING) or give up (DEAD).
  FAILED: ['PENDING', 'DEAD'],
  // Terminal.
  SUCCEEDED: [],
  // Dead letter: manual requeue only.
  DEAD: ['PENDING'],
  // Terminal.
  CANCELLED: [],
} as const satisfies Record<JobState, readonly JobState[]>;

/** All states legally reachable from `S`. */
export type NextState<S extends JobState> = (typeof JOB_TRANSITIONS)[S][number];

/** States from which no further transition is possible. */
export type TerminalState = Extract<JobState, 'SUCCEEDED' | 'CANCELLED'>;

export const TERMINAL_STATES: readonly JobState[] = [
  JobState.SUCCEEDED,
  JobState.CANCELLED,
  JobState.DEAD,
];

/** DEAD is terminal for scheduling purposes but a manual requeue can revive it. */
export function isTerminal(state: JobState): boolean {
  return TERMINAL_STATES.includes(state);
}

export function isInFlight(state: JobState): boolean {
  return state === JobState.CLAIMED || state === JobState.RUNNING;
}

export class IllegalTransitionError extends Error {
  constructor(
    readonly from: JobState,
    readonly to: JobState,
  ) {
    super(`illegal job transition ${from} -> ${to}`);
    this.name = 'IllegalTransitionError';
  }
}

/**
 * Compile-time-checked transition. `to` is constrained to the edges declared for
 * `from`, so a bad edge cannot be written; the runtime check exists only for
 * states that arrive as plain data (a row read out of Postgres).
 */
export function transition<S extends JobState, T extends NextState<S>>(from: S, to: T): T {
  if (!canTransition(from, to)) throw new IllegalTransitionError(from, to);
  return to;
}

/** Runtime edge check for states whose value is not known statically. */
export function canTransition(from: JobState, to: JobState): boolean {
  const allowed: readonly JobState[] = JOB_TRANSITIONS[from];
  return allowed.includes(to);
}

/** Runtime-checked variant for dynamically typed input (HTTP, DB rows). */
export function assertTransition(from: JobState, to: JobState): JobState {
  if (!canTransition(from, to)) throw new IllegalTransitionError(from, to);
  return to;
}

const JOB_STATE_VALUES: readonly string[] = Object.values(JobState);

export function isJobState(v: unknown): v is JobState {
  return typeof v === 'string' && JOB_STATE_VALUES.includes(v);
}
