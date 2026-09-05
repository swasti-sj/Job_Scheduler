import { monitorEventLoopDelay, type IntervalHistogram } from 'node:perf_hooks';
import {
  Registry,
  Counter,
  Gauge,
  Histogram,
  collectDefaultMetrics,
} from 'prom-client';

export const registry = new Registry();
registry.setDefaultLabels({ service: 'job-scheduler' });
collectDefaultMetrics({ register: registry });

// ---------------------------------------------------------------------------
// Job lifecycle
// ---------------------------------------------------------------------------

export const jobsEnqueued = new Counter({
  name: 'jobs_enqueued_total',
  help: 'Jobs accepted by the API',
  labelNames: ['tenant', 'queue', 'job_type', 'outcome'] as const,
  registers: [registry],
});

export const jobsClaimed = new Counter({
  name: 'jobs_claimed_total',
  help: 'Jobs claimed by workers',
  labelNames: ['queue'] as const,
  registers: [registry],
});

export const jobsCompleted = new Counter({
  name: 'jobs_completed_total',
  help: 'Jobs that reached a terminal state',
  labelNames: ['queue', 'job_type', 'state'] as const,
  registers: [registry],
});

export const jobsReclaimed = new Counter({
  name: 'jobs_reclaimed_total',
  help: 'In-flight jobs returned to the queue',
  // path: 'socket_close' (immediate) | 'heartbeat_loss' (immediate) | 'lease_expiry' (reaper)
  labelNames: ['path', 'outcome'] as const,
  registers: [registry],
});

export const jobsRetried = new Counter({
  name: 'jobs_retried_total',
  help: 'Failed attempts rescheduled with backoff',
  labelNames: ['job_type'] as const,
  registers: [registry],
});

/**
 * The headline SLO: time from a job becoming *ready to run* to a worker
 * receiving it. Buckets are dense below 30ms because that is the p99 target.
 */
export const schedulingLatency = new Histogram({
  name: 'job_scheduling_latency_ms',
  help: 'Milliseconds from scheduled_for to worker pickup',
  labelNames: ['queue'] as const,
  buckets: [1, 2, 5, 10, 15, 20, 30, 50, 75, 100, 250, 500, 1000, 5000],
  registers: [registry],
});

export const jobDuration = new Histogram({
  name: 'job_execution_duration_ms',
  help: 'Milliseconds spent executing a job',
  labelNames: ['job_type', 'state'] as const,
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30_000, 120_000],
  registers: [registry],
});

export const queueDepth = new Gauge({
  name: 'queue_depth',
  help: 'Jobs waiting in a queue by state',
  labelNames: ['queue', 'tenant', 'state'] as const,
  registers: [registry],
});

export const claimBatchSize = new Histogram({
  name: 'claim_batch_size',
  help: 'Jobs returned per claim transaction',
  buckets: [0, 1, 2, 4, 8, 16, 32, 64, 128],
  registers: [registry],
});

export const claimDuration = new Histogram({
  name: 'claim_transaction_duration_ms',
  help: 'Milliseconds spent inside the claim BEGIN/COMMIT',
  buckets: [1, 2, 5, 10, 20, 50, 100, 250, 500, 1000],
  registers: [registry],
});

// ---------------------------------------------------------------------------
// Cluster / control plane
// ---------------------------------------------------------------------------

export const workersConnected = new Gauge({
  name: 'workers_connected',
  help: 'Workers with a live control-plane socket to this node',
  registers: [registry],
});

export const workersByState = new Gauge({
  name: 'workers_by_state',
  help: 'Registered workers by state',
  labelNames: ['state'] as const,
  registers: [registry],
});

export const isLeader = new Gauge({
  name: 'scheduler_is_leader',
  help: '1 when this node holds the leader advisory lock',
  registers: [registry],
});

export const leaderTransitions = new Counter({
  name: 'scheduler_leader_transitions_total',
  help: 'Leadership acquisitions and losses',
  labelNames: ['direction'] as const,
  registers: [registry],
});

export const leaderFailoverSeconds = new Gauge({
  name: 'scheduler_leader_acquire_seconds',
  help: 'Seconds this node spent campaigning before acquiring leadership',
  registers: [registry],
});

export const breakerState = new Gauge({
  name: 'circuit_breaker_open',
  help: '1 when the circuit breaker for a job_type is open',
  labelNames: ['job_type'] as const,
  registers: [registry],
});

export const rateLimitRejections = new Counter({
  name: 'rate_limit_rejections_total',
  help: 'Claim slots denied by the per-queue token bucket',
  labelNames: ['queue'] as const,
  registers: [registry],
});

export const dashboardFramesDropped = new Counter({
  name: 'dashboard_frames_dropped_total',
  help: 'Dashboard frames dropped because a client was too slow',
  labelNames: ['reason'] as const,
  registers: [registry],
});

export const dashboardClients = new Gauge({
  name: 'dashboard_clients',
  help: 'Connected dashboard websocket clients',
  registers: [registry],
});

// ---------------------------------------------------------------------------
// Process health
// ---------------------------------------------------------------------------

export const dbQueryDuration = new Histogram({
  name: 'db_query_duration_seconds',
  help: 'Postgres query/transaction wall time',
  labelNames: ['label'] as const,
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5],
  registers: [registry],
});

export const dbPoolWaiting = new Gauge({
  name: 'db_pool_waiting',
  help: 'Requests queued waiting for a pg connection',
  registers: [registry],
});

export const eventLoopLag = new Gauge({
  name: 'event_loop_lag_ms',
  help: 'Event loop delay percentiles over the last scrape interval',
  labelNames: ['quantile'] as const,
  registers: [registry],
});

/**
 * Event loop lag.
 *
 * `monitorEventLoopDelay` samples in libuv (C++), so measuring costs nothing on
 * the JS side - unlike a setInterval-drift probe, which itself competes for the
 * loop it is trying to measure. Percentiles are read and the histogram reset on
 * every Prometheus scrape, so each scrape reports the window since the last one.
 *
 * Caveat worth knowing when reading the numbers: the sampler cannot resolve a
 * delay finer than its own resolution, and on hosts with a coarse timer (Windows
 * schedules at ~15.6ms; virtualised clocks are often worse) there is a constant
 * offset on top of that which is the platform, not the process. A p50 and p99
 * that sit almost on top of each other at roughly the resolution value means the
 * loop is idle and you are reading the floor, not real lag.
 */
let loopHistogram: IntervalHistogram | null = null;

export function startEventLoopMonitor(
  resolutionMs = Number.parseInt(process.env['EVENT_LOOP_RESOLUTION_MS'] ?? '5', 10),
): void {
  if (loopHistogram !== null) return;
  loopHistogram = monitorEventLoopDelay({ resolution: resolutionMs });
  loopHistogram.enable();
}

export function sampleEventLoopLag(): void {
  if (loopHistogram === null) return;
  const toMs = (ns: number): number => (Number.isFinite(ns) ? ns / 1e6 : 0);
  eventLoopLag.set({ quantile: '0.5' }, toMs(loopHistogram.percentile(50)));
  eventLoopLag.set({ quantile: '0.9' }, toMs(loopHistogram.percentile(90)));
  eventLoopLag.set({ quantile: '0.99' }, toMs(loopHistogram.percentile(99)));
  eventLoopLag.set({ quantile: 'max' }, toMs(loopHistogram.max));
  loopHistogram.reset();
}

export async function renderMetrics(): Promise<string> {
  sampleEventLoopLag();
  return registry.metrics();
}

export const metricsContentType = registry.contentType;
