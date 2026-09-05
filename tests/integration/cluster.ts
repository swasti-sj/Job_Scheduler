import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';

/**
 * Spawns real scheduler and worker *processes* so the chaos tests can SIGKILL
 * them.
 *
 * They run the compiled `dist/` output rather than through tsx: that is what
 * production runs, and it means the pid we kill is the pid running the code -
 * a loader wrapper in between would absorb the signal and we would be testing
 * the wrapper's shutdown, not ours.
 */
const TEST_ENV = {
  DATABASE_URL: process.env['DATABASE_URL'] ?? 'postgres://scheduler:scheduler@localhost:5433/scheduler_test',
  REDIS_URL: process.env['REDIS_URL'] ?? 'redis://localhost:6380',
  LOG_LEVEL: process.env['CLUSTER_LOG_LEVEL'] ?? 'warn',
};

let built = false;

export function buildOnce(): void {
  if (built && existsSync('dist/bin/scheduler.js')) return;
  const result = spawnSync('npx', ['tsc', '-p', 'tsconfig.build.json'], {
    stdio: 'inherit',
    shell: true,
  });
  if (result.status !== 0) throw new Error('build failed');
  built = true;
}

export interface Managed {
  name: string;
  child: ChildProcess;
  port?: number;
}

const managed: Managed[] = [];

function spawnNode(name: string, script: string, env: Record<string, string>, port?: number): Managed {
  const child = spawn(process.execPath, [script], {
    env: { ...process.env, ...TEST_ENV, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout?.on('data', (chunk: Buffer) => {
    if (process.env['CLUSTER_VERBOSE'] === '1') process.stdout.write(`[${name}] ${chunk}`);
  });
  child.stderr?.on('data', (chunk: Buffer) => {
    if (process.env['CLUSTER_VERBOSE'] === '1') process.stderr.write(`[${name}] ${chunk}`);
  });
  const entry: Managed = port === undefined ? { name, child } : { name, child, port };
  managed.push(entry);
  return entry;
}

export function startScheduler(name: string, port: number, extra: Record<string, string> = {}): Managed {
  return spawnNode(
    name,
    'dist/bin/scheduler.js',
    {
      HTTP_PORT: String(port),
      NODE_ID: name,
      LEADER_POLL_INTERVAL_MS: '250',
      REAPER_INTERVAL_MS: '500',
      ...extra,
    },
    port,
  );
}

export function startWorker(name: string, schedulerPort: number, extra: Record<string, string> = {}): Managed {
  return spawnNode(name, 'dist/bin/worker.js', {
    NODE_ID: name,
    SCHEDULER_WS_URL: `ws://127.0.0.1:${schedulerPort}/worker`,
    WORKER_CONCURRENCY: '4',
    WORKER_QUEUES: 'default',
    ...extra,
  });
}

/** Ungraceful termination - the path the chaos requirements care about. */
export function sigkill(entry: Managed): void {
  entry.child.kill('SIGKILL');
}

export function sigterm(entry: Managed): void {
  entry.child.kill('SIGTERM');
}

export async function waitForHttp(port: number, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/health`);
      if (res.ok) return;
    } catch {
      /* not up yet */
    }
    await delay(150);
  }
  throw new Error(`node on port ${port} did not become healthy`);
}

export async function isLeader(port: number): Promise<boolean> {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/health`);
    const body = (await res.json()) as { is_leader?: boolean };
    return body.is_leader === true;
  } catch {
    return false;
  }
}

export async function stopAll(): Promise<void> {
  const dying = managed.splice(0);
  for (const entry of dying) {
    if (entry.child.exitCode === null && entry.child.signalCode === null) {
      entry.child.kill('SIGKILL');
    }
  }
  // Wait for the OS to actually reap them. Returning while a killed scheduler is
  // still holding a Postgres connection lets it interfere with the next test.
  await Promise.all(
    dying.map(
      (entry) =>
        new Promise<void>((resolve) => {
          if (entry.child.exitCode !== null || entry.child.signalCode !== null) return resolve();
          entry.child.once('exit', () => resolve());
          setTimeout(resolve, 5000).unref();
        }),
    ),
  );
  await delay(200);
}
