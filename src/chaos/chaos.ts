/**
 * Chaos harness.
 *
 * Submits N jobs, runs a real cluster of scheduler and worker processes, and
 * SIGKILLs random members of it every few seconds until the queue drains. At the
 * end it asserts the property the whole system exists to provide:
 *
 *     every job reached a terminal state, exactly once.
 *
 * "Exactly once" is checked against the append-only job_events log, not the jobs
 * table: a job that was executed twice would still show a single row in `jobs`
 * (the second write would overwrite the first), but it cannot hide two terminal
 * transitions in the audit log. That is the difference between checking the
 * answer and checking the working.
 *
 * Usage:
 *   npm run chaos -- --jobs=100000 --workers=5 --schedulers=3 --kill-every=4000
 */
import { setTimeout as delay } from 'node:timers/promises';
import { randomUUID } from 'node:crypto';
import { Supervisor } from './processes.js';
import { pool, query } from '../db/pool.js';
import { runMigrations, waitForDatabase } from '../db/migrate.js';
import { closeRedis } from '../redis/client.js';

interface Args {
  jobs: number;
  workers: number;
  schedulers: number;
  killEvery: number;
  basePort: number;
  jobMs: number;
  maxAttempts: number;
  tenants: number;
  timeoutMs: number;
  verbose: boolean;
  noChaos: boolean;
}

function parseArgs(argv: string[]): Args {
  const get = (name: string, fallback: number): number => {
    const raw = argv.find((a) => a.startsWith(`--${name}=`));
    if (raw === undefined) return fallback;
    const value = Number.parseInt(raw.split('=')[1] ?? '', 10);
    return Number.isFinite(value) ? value : fallback;
  };
  return {
    jobs: get('jobs', 100_000),
    workers: get('workers', 5),
    schedulers: get('schedulers', 3),
    killEvery: get('kill-every', 4000),
    basePort: get('base-port', 3200),
    jobMs: get('job-ms', 1),
    maxAttempts: get('max-attempts', 10),
    tenants: get('tenants', 3),
    timeoutMs: get('timeout', 900_000),
    verbose: argv.includes('--verbose'),
    noChaos: argv.includes('--no-chaos'),
  };
}

/**
 * Bulk enqueue. A producer pushing 100k jobs would batch, not issue 100k HTTP
 * calls, so this inserts in chunks of 2000 via a single multi-row statement -
 * which is also the only way the setup phase finishes in seconds rather than
 * minutes.
 */
async function enqueue(args: Args): Promise<void> {
  const chunkSize = 2000;
  const tenants = Array.from({ length: args.tenants }, (_, i) => `tenant-${i}`);
  await query(
    `INSERT INTO tenants (tenant_id) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING`,
    [tenants],
  );

  let inserted = 0;
  while (inserted < args.jobs) {
    const size = Math.min(chunkSize, args.jobs - inserted);
    const ids: string[] = [];
    const tenantCol: string[] = [];
    const priorities: number[] = [];
    for (let i = 0; i < size; i += 1) {
      ids.push(randomUUID());
      tenantCol.push(tenants[(inserted + i) % tenants.length] as string);
      priorities.push((inserted + i) % 10);
    }
    await query(
      `INSERT INTO jobs (id, tenant_id, queue_name, job_type, payload, priority, max_attempts, state)
       SELECT t.id, t.tenant_id, 'default', 'sleep', $4::jsonb, t.priority, $5, 'PENDING'
       FROM unnest($1::uuid[], $2::text[], $3::smallint[]) AS t(id, tenant_id, priority)`,
      [ids, tenantCol, priorities, JSON.stringify({ ms: args.jobMs }), args.maxAttempts],
    );
    inserted += size;
    if (inserted % 20_000 === 0) console.log(`  enqueued ${inserted}/${args.jobs}`);
  }
}

interface Progress {
  terminal: number;
  succeeded: number;
  dead: number;
  cancelled: number;
  inflight: number;
  pending: number;
}

async function progress(): Promise<Progress> {
  const { rows } = await query<{ state: string; n: number }>(
    'SELECT state::text AS state, count(*)::int AS n FROM jobs GROUP BY state',
  );
  const by = Object.fromEntries(rows.map((r) => [r.state, r.n]));
  return {
    succeeded: by['SUCCEEDED'] ?? 0,
    dead: by['DEAD'] ?? 0,
    cancelled: by['CANCELLED'] ?? 0,
    terminal: (by['SUCCEEDED'] ?? 0) + (by['DEAD'] ?? 0) + (by['CANCELLED'] ?? 0),
    inflight: (by['CLAIMED'] ?? 0) + (by['RUNNING'] ?? 0),
    pending: (by['PENDING'] ?? 0) + (by['BLOCKED'] ?? 0),
  };
}

async function main(): Promise<number> {
  const args = parseArgs(process.argv.slice(2));
  console.log('chaos run:', args);

  await waitForDatabase();
  await runMigrations();
  console.log('resetting database...');
  await query('TRUNCATE jobs, job_events, workers, circuit_breakers, tenants CASCADE');

  console.log(`enqueuing ${args.jobs} jobs...`);
  const enqueueStart = Date.now();
  await enqueue(args);
  const enqueueSeconds = (Date.now() - enqueueStart) / 1000;
  console.log(`enqueued in ${enqueueSeconds.toFixed(1)}s (${Math.round(args.jobs / enqueueSeconds)}/s)`);

  const supervisor = new Supervisor({
    verbose: args.verbose,
    env: {
      DATABASE_URL: process.env['DATABASE_URL'] ?? '',
      REDIS_URL: process.env['REDIS_URL'] ?? '',
      LOG_LEVEL: args.verbose ? 'info' : 'error',
      // A short lease makes the lease-expiry reclaim path observable inside a
      // run of this length; production defaults to 30s.
      LEASE_DURATION_SECONDS: '10',
      LEADER_POLL_INTERVAL_MS: '250',
      REAPER_INTERVAL_MS: '500',
    },
  });

  const ports: number[] = [];
  for (let i = 0; i < args.schedulers; i += 1) {
    const port = args.basePort + i;
    ports.push(port);
    supervisor.start(`sched-${i}`, 'scheduler', port, { HTTP_PORT: String(port) });
  }
  await delay(3000);

  for (let i = 0; i < args.workers; i += 1) {
    const port = ports[i % ports.length] as number;
    supervisor.start(`worker-${i}`, 'worker', undefined, {
      SCHEDULER_WS_URL: `ws://127.0.0.1:${port}/worker`,
      WORKER_CONCURRENCY: '16',
    });
  }

  const kills = { scheduler: 0, worker: 0 };
  const chaosTimer = args.noChaos
    ? null
    : setInterval(() => {
        // Never kill the last scheduler: with none left there is nothing to
        // recover *to*, which tests the orchestrator rather than the scheduler.
        const killScheduler = Math.random() < 0.35 && supervisor.byKind('scheduler').length > 1;
        const pool_ = killScheduler ? supervisor.byKind('scheduler') : supervisor.byKind('worker');
        const victim = pool_[Math.floor(Math.random() * pool_.length)];
        if (victim === undefined) return;
        if (supervisor.kill(victim.name)) {
          kills[victim.kind] += 1;
          console.log(`  SIGKILL ${victim.name}`);
        }
      }, args.killEvery);

  const startedAt = Date.now();
  let last: Progress = await progress();
  let lastReport = Date.now();

  while (Date.now() - startedAt < args.timeoutMs) {
    await delay(1000);
    const now = await progress();
    if (Date.now() - lastReport >= 5000) {
      const rate = (now.terminal - last.terminal) / ((Date.now() - lastReport) / 1000);
      console.log(
        `  ${now.terminal}/${args.jobs} terminal  ` +
          `(pending ${now.pending}, in-flight ${now.inflight}, ${rate.toFixed(0)}/s)`,
      );
      last = now;
      lastReport = Date.now();
    }
    if (now.terminal >= args.jobs) break;
  }

  if (chaosTimer !== null) clearInterval(chaosTimer);
  const elapsedSeconds = (Date.now() - startedAt) / 1000;

  // Let any final in-flight work settle before the audit.
  await delay(2000);
  const final = await progress();

  console.log('\n--- verification ---');
  const { rows: dupes } = await query<{ n: number }>(
    `SELECT count(*)::int AS n FROM (
       SELECT job_id FROM job_events
       WHERE to_state IN ('SUCCEEDED', 'DEAD', 'CANCELLED')
       GROUP BY job_id HAVING count(*) > 1
     ) d`,
  );
  const { rows: orphans } = await query<{ n: number }>(
    `SELECT count(*)::int AS n FROM jobs
     WHERE state IN ('CLAIMED','RUNNING') AND claimed_by IS NULL`,
  );
  const { rows: unterminated } = await query<{ n: number }>(
    `SELECT count(*)::int AS n FROM jobs
     WHERE state NOT IN ('SUCCEEDED','DEAD','CANCELLED')`,
  );
  const { rows: total } = await query<{ n: number }>('SELECT count(*)::int AS n FROM jobs');

  const duplicateTerminals = dupes[0]?.n ?? 0;
  const orphaned = orphans[0]?.n ?? 0;
  const stillRunning = unterminated[0]?.n ?? 0;
  const totalJobs = total[0]?.n ?? 0;

  console.log(`jobs submitted        : ${args.jobs}`);
  console.log(`jobs in table         : ${totalJobs}`);
  console.log(`terminal              : ${final.terminal} (succeeded ${final.succeeded}, dead ${final.dead}, cancelled ${final.cancelled})`);
  console.log(`not terminal          : ${stillRunning}`);
  console.log(`duplicate terminals   : ${duplicateTerminals}`);
  console.log(`orphaned in-flight    : ${orphaned}`);
  console.log(`SIGKILLs              : ${kills.scheduler} schedulers, ${kills.worker} workers`);
  console.log(`wall time             : ${elapsedSeconds.toFixed(1)}s (${(final.terminal / elapsedSeconds).toFixed(0)} jobs/s)`);

  await supervisor.stopAll();
  await closeRedis();
  await pool.end();

  const ok =
    totalJobs === args.jobs &&
    final.terminal === args.jobs &&
    duplicateTerminals === 0 &&
    orphaned === 0 &&
    stillRunning === 0;

  console.log(ok ? '\nRESULT: PASS - zero job loss, zero duplicate execution' : '\nRESULT: FAIL');
  return ok ? 0 : 1;
}

process.exitCode = await main();
