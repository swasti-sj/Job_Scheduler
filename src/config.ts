import { hostname } from 'node:os';
import { randomUUID } from 'node:crypto';

function str(name: string, fallback: string): string {
  const v = process.env[name];
  return v === undefined || v === '' ? fallback : v;
}

function int(name: string, fallback: number): number {
  const v = process.env[name];
  if (v === undefined || v === '') return fallback;
  const n = Number.parseInt(v, 10);
  if (!Number.isFinite(n)) throw new Error(`env ${name} must be an integer, got ${JSON.stringify(v)}`);
  return n;
}

function num(name: string, fallback: number): number {
  const v = process.env[name];
  if (v === undefined || v === '') return fallback;
  const n = Number.parseFloat(v);
  if (!Number.isFinite(n)) throw new Error(`env ${name} must be a number, got ${JSON.stringify(v)}`);
  return n;
}

function bool(name: string, fallback: boolean): boolean {
  const v = process.env[name];
  if (v === undefined || v === '') return fallback;
  return v === '1' || v.toLowerCase() === 'true' || v.toLowerCase() === 'yes';
}

function list(name: string, fallback: string[]): string[] {
  const v = process.env[name];
  if (v === undefined || v === '') return fallback;
  return v.split(',').map((s) => s.trim()).filter((s) => s.length > 0);
}

export interface Config {
  readonly nodeId: string;
  readonly hostname: string;
  readonly databaseUrl: string;
  readonly pgPoolMax: number;
  readonly redisUrl: string;
  readonly httpHost: string;
  readonly httpPort: number;
  readonly logLevel: string;

  readonly leaseDurationSeconds: number;
  readonly claimBatchSize: number;
  readonly claimCandidatesPerPriority: number;
  readonly claimOverselectFactor: number;
  readonly claimPollIntervalMs: number;
  readonly claimPollJitterMs: number;
  readonly claimPollMaxIntervalMs: number;

  readonly agingIntervalSeconds: number;
  readonly agingMaxBoost: number;

  readonly fairShareEnabled: boolean;
  readonly fairShareMaxPct: number;

  readonly rateLimitEnabled: boolean;
  readonly rateLimitCapacity: number;
  readonly rateLimitRefillPerSec: number;

  readonly backoffBaseMs: number;
  readonly backoffCapMs: number;
  readonly defaultMaxAttempts: number;

  readonly breakerEnabled: boolean;
  readonly breakerWindowSeconds: number;
  readonly breakerMinSamples: number;
  readonly breakerFailureRate: number;
  readonly breakerCooldownSeconds: number;

  readonly workerHeartbeatIntervalMs: number;
  readonly workerMissedBeats: number;

  readonly leaderLockKey: number;
  readonly leaderPollIntervalMs: number;
  readonly reaperIntervalMs: number;

  readonly dashboardRingSize: number;
  readonly dashboardMaxBufferedBytes: number;
  readonly dashboardBroadcastIntervalMs: number;

  readonly workerConcurrency: number;
  readonly workerQueues: string[];
  readonly schedulerWsUrl: string;
}

export function loadConfig(): Config {
  return Object.freeze({
    nodeId: str('NODE_ID', `${hostname()}-${process.pid}-${randomUUID().slice(0, 8)}`),
    hostname: hostname(),
    databaseUrl: str('DATABASE_URL', 'postgres://scheduler:scheduler@localhost:5432/scheduler'),
    pgPoolMax: int('PG_POOL_MAX', 20),
    redisUrl: str('REDIS_URL', 'redis://localhost:6379'),
    httpHost: str('HTTP_HOST', '0.0.0.0'),
    httpPort: int('HTTP_PORT', 3000),
    logLevel: str('LOG_LEVEL', 'info'),

    leaseDurationSeconds: int('LEASE_DURATION_SECONDS', 30),
    claimBatchSize: int('CLAIM_BATCH_SIZE', 32),
    claimCandidatesPerPriority: int('CLAIM_CANDIDATES_PER_PRIORITY', 64),
    claimOverselectFactor: int('CLAIM_OVERSELECT_FACTOR', 8),
    claimPollIntervalMs: int('CLAIM_POLL_INTERVAL_MS', 50),
    claimPollJitterMs: int('CLAIM_POLL_JITTER_MS', 25),
    claimPollMaxIntervalMs: int('CLAIM_POLL_MAX_INTERVAL_MS', 500),

    agingIntervalSeconds: int('AGING_INTERVAL_SECONDS', 30),
    agingMaxBoost: int('AGING_MAX_BOOST', 9),

    fairShareEnabled: bool('FAIR_SHARE_ENABLED', true),
    fairShareMaxPct: num('FAIR_SHARE_MAX_PCT', 0.5),

    rateLimitEnabled: bool('RATE_LIMIT_ENABLED', false),
    rateLimitCapacity: int('RATE_LIMIT_CAPACITY', 10_000),
    rateLimitRefillPerSec: num('RATE_LIMIT_REFILL_PER_SEC', 5000),

    backoffBaseMs: int('BACKOFF_BASE_MS', 250),
    backoffCapMs: int('BACKOFF_CAP_MS', 60_000),
    defaultMaxAttempts: int('DEFAULT_MAX_ATTEMPTS', 5),

    breakerEnabled: bool('BREAKER_ENABLED', true),
    breakerWindowSeconds: int('BREAKER_WINDOW_SECONDS', 60),
    breakerMinSamples: int('BREAKER_MIN_SAMPLES', 20),
    breakerFailureRate: num('BREAKER_FAILURE_RATE', 0.5),
    breakerCooldownSeconds: int('BREAKER_COOLDOWN_SECONDS', 60),

    workerHeartbeatIntervalMs: int('WORKER_HEARTBEAT_INTERVAL_MS', 5000),
    workerMissedBeats: int('WORKER_MISSED_BEATS', 3),

    leaderLockKey: int('LEADER_LOCK_KEY', 911_001),
    leaderPollIntervalMs: int('LEADER_POLL_INTERVAL_MS', 500),
    reaperIntervalMs: int('REAPER_INTERVAL_MS', 1000),

    dashboardRingSize: int('DASHBOARD_RING_SIZE', 64),
    dashboardMaxBufferedBytes: int('DASHBOARD_MAX_BUFFERED_BYTES', 262_144),
    dashboardBroadcastIntervalMs: int('DASHBOARD_BROADCAST_INTERVAL_MS', 1000),

    workerConcurrency: int('WORKER_CONCURRENCY', 8),
    workerQueues: list('WORKER_QUEUES', ['default']),
    schedulerWsUrl: str('SCHEDULER_WS_URL', 'ws://localhost:3000/worker'),
  });
}

export const config: Config = loadConfig();
