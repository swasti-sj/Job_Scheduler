import { Redis } from 'ioredis';
import { config } from '../config.js';
import { logger } from '../logger.js';

function build(name: string): Redis {
  const client = new Redis(config.redisUrl, {
    lazyConnect: false,
    maxRetriesPerRequest: 3,
    enableAutoPipelining: true,
    connectionName: `job-scheduler:${config.nodeId}:${name}`,
    retryStrategy: (times: number) => Math.min(times * 100, 3000),
  });
  client.on('error', (err: unknown) => logger.warn({ err, name }, 'redis error'));
  return client;
}

/** Command connection: token buckets, breaker windows, counters. */
export const redis: Redis = build('cmd');

/**
 * Dedicated subscriber. A connection in subscribe mode cannot issue normal
 * commands, so pub/sub always needs its own socket.
 */
export const subscriber: Redis = build('sub');

/** Publisher, separate from `redis` so a slow Lua call cannot delay a wakeup. */
export const publisher: Redis = build('pub');

export const CHANNELS = {
  /** Payload: queue name. Fired on enqueue so idle nodes dispatch immediately. */
  workAvailable: 'sched:work',
  /** Payload: JSON event for dashboards (failures, breaker trips, leadership). */
  events: 'sched:events',
} as const;

export async function closeRedis(): Promise<void> {
  await Promise.allSettled([redis.quit(), subscriber.quit(), publisher.quit()]);
}
