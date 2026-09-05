/**
 * Load test harness.
 *
 * Measures the three numbers the SLOs are stated in:
 *
 *   1. enqueue throughput      - autocannon against POST /jobs
 *   2. scheduling latency      - enqueue -> worker pickup, measured at a steady
 *                                arrival rate the fleet can absorb
 *   3. processing throughput   - terminal jobs per second while draining a
 *                                deliberately oversized backlog
 *
 * Latency samples are taken from Postgres rather than from the client, as
 * job_events(CLAIMED).at - jobs.created_at. Both timestamps come from the same
 * database clock, so the measurement has no client-skew and no client-side
 * queueing baked into it: it is the scheduler's latency, not the loadgen's.
 *
 * Usage:
 *   npm run loadtest -- --jobs=50000 --workers=5 --connections=64
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { randomUUID } from 'node:crypto';
import autocannon from 'autocannon';
import { Supervisor } from '../chaos/processes.js';
import { pool, query } from '../db/pool.js';
import { runMigrations, waitForDatabase } from '../db/migrate.js';
import { closeRedis } from '../redis/client.js';
import { renderHistogram, percentiles } from './histogram.js';
import { publishWorkAvailable } from '../core/events.js';

interface Args {
  jobs: number;
  workers: number;
  schedulers: number;
  connections: number;
  duration: number;
  basePort: number;
  jobMs: number;
  timeoutMs: number;
  arrivalRate: number;
  latencySeconds: number;
}

function parseArgs(argv: string[]): Args {
  const get = (name: string, fallback: number): number => {
    const raw = argv.find((a) => a.startsWith(`--${name}=`));
    if (raw === undefined) return fallback;
    const value = Number.parseInt(raw.split('=')[1] ?? '', 10);
    return Number.isFinite(value) ? value : fallback;
  };
  return {
    jobs: get('jobs', 50_000),
    workers: get('workers', 5),
    schedulers: get('schedulers', 3),
    connections: get('connections', 64),
    duration: get('duration', 10),
    basePort: get('base-port', 3300),
    jobMs: get('job-ms', 0),
    timeoutMs: get('timeout', 600_000),
    arrivalRate: get('arrival-rate', 100),
    latencySeconds: get('latency-seconds', 20),
  };
}

interface Result {
  enqueueHttpPerSecond: number;
  enqueueHttpP99Ms: number;
  enqueueBulkPerSecond: number;
  processedPerSecond: number;
  latency: ReturnType<typeof percentiles>;
  eventLoopLagP99Ms: number;
  eventLoopLagWorstMs: number;
  jobs: number;
}

async function bulkEnqueue(count: number, jobMs: number): Promise<number> {
  const chunkSize = 2000;
  const started = Date.now();
  await query(`INSERT INTO tenants (tenant_id) VALUES ('load') ON CONFLICT DO NOTHING`);
  let done = 0;
  while (done < count) {
    const size = Math.min(chunkSize, count - done);
    const ids = Array.from({ length: size }, () => randomUUID());
    const priorities = Array.from({ length: size }, (_, i) => (done + i) % 10);
    await query(
      `INSERT INTO jobs (id, tenant_id, queue_name, job_type, payload, priority, max_attempts, state)
       SELECT t.id, 'load', 'default', 'sleep', $3::jsonb, t.priority, 5, 'PENDING'
       FROM unnest($1::uuid[], $2::smallint[]) AS t(id, priority)`,
      [ids, priorities, JSON.stringify({ ms: jobMs })],
    );
    done += size;
  }
  return count / ((Date.now() - started) / 1000);
}

async function scrapeEventLoopLag(port: number): Promise<number> {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/metrics`);
    const body = await res.text();
    const line = body
      .split('\n')
      .find((l) => l.startsWith('event_loop_lag_ms{') && l.includes('quantile="0.99"'));
    const value = Number.parseFloat(line?.split(' ').pop() ?? '0');
    return Number.isFinite(value) ? value : 0;
  } catch {
    return 0;
  }
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  console.log('load test:', args);

  await waitForDatabase();
  await runMigrations();
  await query('TRUNCATE jobs, job_events, workers, circuit_breakers, tenants CASCADE');

  const supervisor = new Supervisor({
    verbose: false,
    env: {
      DATABASE_URL: process.env['DATABASE_URL'] ?? '',
      REDIS_URL: process.env['REDIS_URL'] ?? '',
      LOG_LEVEL: 'error',
      CLAIM_POLL_INTERVAL_MS: '20',
      CLAIM_BATCH_SIZE: '64',
      // Fair share costs several aggregates per claim and this run is
      // single-tenant, so it is switched off to measure the claim path itself.
      FAIR_SHARE_ENABLED: 'false',
    },
  });

  const ports: number[] = [];
  for (let i = 0; i < args.schedulers; i += 1) {
    const port = args.basePort + i;
    ports.push(port);
    supervisor.start(`ls-${i}`, 'scheduler', port, { HTTP_PORT: String(port) });
  }
  await delay(4000);
  const apiPort = ports[0] as number;

  // ---- 1. enqueue throughput over HTTP ------------------------------------
  console.log('\n[1/3] enqueue throughput (autocannon on POST /jobs)...');
  const cannon = await autocannon({
    url: `http://127.0.0.1:${apiPort}/jobs`,
    connections: args.connections,
    duration: args.duration,
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      tenant_id: 'load',
      queue_name: 'bench',
      job_type: 'noop',
      priority: 5,
      payload: { via: 'http' },
    }),
  });
  const enqueueHttpPerSecond = cannon.requests.average;
  const enqueueHttpP99Ms = cannon.latency.p99;
  console.log(`      ${enqueueHttpPerSecond.toFixed(0)} req/s, p99 ${enqueueHttpP99Ms}ms`);

  // ---- 2. steady-state scheduling latency ---------------------------------
  //
  // Measured with the workers already running and *not* saturated. Latency has
  // to be measured below capacity or it is not latency at all: dump a 20k
  // backlog on five workers and the numbers you get back are queueing delay,
  // which says how long the backlog was, not how fast the scheduler dispatches.
  // So: start the workers, let them idle, then feed jobs at a fixed rate the
  // fleet can comfortably absorb, and measure enqueue -> pickup on those.
  console.log(`\n[2/4] starting ${args.workers} workers...`);
  await query(`DELETE FROM jobs WHERE queue_name = 'bench'`);
  for (let i = 0; i < args.workers; i += 1) {
    supervisor.start(`lw-${i}`, 'worker', undefined, {
      SCHEDULER_WS_URL: `ws://127.0.0.1:${ports[i % ports.length] as number}/worker`,
      WORKER_CONCURRENCY: '32',
      WORKER_QUEUES: 'default,latency',
      LOG_LEVEL: 'error',
    });
  }
  await delay(5000);

  console.log(`[3/4] steady-state latency at ${args.arrivalRate}/s for ${args.latencySeconds}s...`);
  const latencyStart = Date.now();
  await query(`INSERT INTO tenants (tenant_id) VALUES ('load') ON CONFLICT DO NOTHING`);
  const perTick = Math.max(1, Math.round(args.arrivalRate / 10));
  const ticks = args.latencySeconds * 10;
  for (let tick = 0; tick < ticks; tick += 1) {
    const tickStart = Date.now();
    const ids = Array.from({ length: perTick }, () => randomUUID());
    await query(
      `INSERT INTO jobs (id, tenant_id, queue_name, job_type, payload, priority, max_attempts, state)
       SELECT t.id, 'load', 'latency', 'sleep', $2::jsonb, 5, 5, 'PENDING'
       FROM unnest($1::uuid[]) AS t(id)`,
      [ids, JSON.stringify({ ms: args.jobMs })],
    );
    // Real clients enqueue through POST /jobs, which publishes a wakeup so idle
    // dispatchers claim immediately instead of waiting for their next poll. This
    // harness writes rows directly for speed, so it has to publish too - without
    // this it measures the fallback poll interval, not scheduling latency.
    publishWorkAvailable('latency');
    const elapsed = Date.now() - tickStart;
    if (elapsed < 100) await delay(100 - elapsed);
  }
  await delay(3000);
  console.log(`      submitted ${perTick * ticks} jobs over ${((Date.now() - latencyStart) / 1000).toFixed(1)}s`);

  const { rows: latencySamples } = await query<{ ms: number }>(
    `SELECT EXTRACT(EPOCH FROM (e.at - j.created_at)) * 1000 AS ms
     FROM job_events e JOIN jobs j ON j.id = e.job_id
     WHERE e.to_state = 'CLAIMED' AND j.queue_name = 'latency'`,
  );
  const latencies = latencySamples.map((r) => Number(r.ms)).filter((n) => Number.isFinite(n) && n >= 0);

  // ---- 4. saturation throughput -------------------------------------------
  console.log(`\n[4/4] bulk enqueue of ${args.jobs} jobs, then drain...`);
  const enqueueBulkPerSecond = await bulkEnqueue(args.jobs, args.jobMs);
  console.log(`      bulk enqueue ${enqueueBulkPerSecond.toFixed(0)} jobs/s`);

  const drainStart = Date.now();
  let processed = 0;
  const lagSamples: number[] = [];
  while (Date.now() - drainStart < args.timeoutMs) {
    await delay(1000);
    const { rows } = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM jobs
       WHERE queue_name = 'default' AND state IN ('SUCCEEDED','DEAD','CANCELLED')`,
    );
    processed = rows[0]?.n ?? 0;
    lagSamples.push(await scrapeEventLoopLag(apiPort));
    const seconds = (Date.now() - drainStart) / 1000;
    process.stdout.write(`\r      ${processed}/${args.jobs} (${(processed / seconds).toFixed(0)}/s)   `);
    if (processed >= args.jobs) break;
  }
  const drainSeconds = (Date.now() - drainStart) / 1000;
  const processedPerSecond = processed / drainSeconds;
  console.log(`\n      drained ${processed} in ${drainSeconds.toFixed(1)}s (${processedPerSecond.toFixed(0)}/s)`);

  mkdirSync('docs', { recursive: true });
  const stats = renderHistogram(latencies, 'docs/latency-histogram.png', {
    title: 'SCHEDULING LATENCY',
    subtitle:
      `ENQUEUE TO WORKER PICKUP AT ${args.arrivalRate}/S STEADY STATE - ` +
      `${args.workers} WORKERS, ${args.schedulers} SCHEDULERS`,
    xLabel: 'LATENCY',
  });

  // Each scrape reports the p99 for the window since the previous scrape; the
  // headline number is the median of those, with the worst window alongside it
  // so a single stall is visible rather than averaged away.
  const sortedLag = [...lagSamples].sort((a, b) => a - b);
  const lagP99 = sortedLag[Math.floor(sortedLag.length / 2)] ?? 0;
  const lagWorst = sortedLag[sortedLag.length - 1] ?? 0;

  const result: Result = {
    enqueueHttpPerSecond,
    enqueueHttpP99Ms,
    enqueueBulkPerSecond,
    processedPerSecond,
    latency: stats,
    eventLoopLagP99Ms: lagP99,
    eventLoopLagWorstMs: lagWorst,
    jobs: args.jobs,
  };

  mkdirSync('loadtest-results', { recursive: true });
  writeFileSync('loadtest-results/latest.json', JSON.stringify(result, null, 2));

  console.log('\n--- results ---');
  console.log(`enqueue (http)        : ${enqueueHttpPerSecond.toFixed(0)} jobs/s (p99 ${enqueueHttpP99Ms}ms)`);
  console.log(`enqueue (bulk)        : ${enqueueBulkPerSecond.toFixed(0)} jobs/s`);
  console.log(`processed             : ${processedPerSecond.toFixed(0)} jobs/s`);
  console.log(`scheduling latency p50: ${stats.p50.toFixed(1)}ms`);
  console.log(`scheduling latency p99: ${stats.p99.toFixed(1)}ms`);
  console.log(`scheduling latency max: ${stats.max.toFixed(1)}ms`);
  console.log(`event loop lag p99    : ${lagP99.toFixed(1)}ms typical, ${lagWorst.toFixed(1)}ms worst window`);
  console.log('histogram             : docs/latency-histogram.png');

  await supervisor.stopAll();
  await closeRedis();
  await pool.end();
}

await main();
