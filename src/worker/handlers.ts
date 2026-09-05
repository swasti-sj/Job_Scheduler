import { setTimeout as delay } from 'node:timers/promises';
import type { JobAssignment } from '../domain/job.js';

export type JobHandler = (job: JobAssignment) => Promise<unknown>;

/**
 * Built-in handlers.
 *
 * `noop`, `sleep` and `flaky` exist so the load test, the chaos harness and the
 * integration tests can drive real work through the real pipeline without a
 * separate fixture app. `flaky` is what the circuit-breaker test uses to push a
 * job type past its failure threshold.
 *
 * Everything here is async and non-blocking on purpose: a handler that busy-loops
 * would show up directly in event_loop_lag_ms and stall the worker's heartbeat,
 * which is exactly the failure mode the lag metric is there to catch.
 */
export const handlers: Record<string, JobHandler> = {
  noop: async () => ({ ok: true }),

  sleep: async (job) => {
    const ms = Number(job.payload['ms'] ?? 10);
    await delay(Math.max(0, Math.min(ms, 60_000)));
    return { slept_ms: ms };
  },

  flaky: async (job) => {
    const failureRate = Number(job.payload['failure_rate'] ?? 0.5);
    await delay(Number(job.payload['ms'] ?? 5));
    if (Math.random() < failureRate) throw new Error('flaky handler failed on purpose');
    return { ok: true };
  },

  always_fail: async () => {
    throw new Error('this job type always fails');
  },

  echo: async (job) => job.payload,
};

export function resolveHandler(jobType: string): JobHandler {
  return (
    handlers[jobType] ??
    (async () => {
      throw new Error(`no handler registered for job_type "${jobType}"`);
    })
  );
}
