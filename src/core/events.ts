import { CHANNELS, publisher, subscriber } from '../redis/client.js';
import { logger } from '../logger.js';

export type SchedulerEvent =
  | { type: 'job_failed'; job_id: string; job_type: string; queue: string; tenant: string; attempt: number; error: string; at: string }
  | { type: 'job_dead'; job_id: string; job_type: string; queue: string; tenant: string; error: string; at: string }
  | { type: 'breaker_opened'; job_type: string; failure_rate: number; samples: number; at: string }
  | { type: 'breaker_closed'; job_type: string; at: string }
  | { type: 'worker_dead'; worker_id: string; reclaimed: number; path: string; at: string }
  | { type: 'leader_changed'; node_id: string; is_leader: boolean; at: string };

/**
 * Enqueue wakeup. Fire-and-forget on purpose: the enqueue path must not pay a
 * Redis round trip, and a lost notification is harmless because every dispatcher
 * also polls on a jittered timer. Redis pub/sub is at-most-once, so it is used
 * strictly as a latency optimisation, never as the source of truth.
 */
export function publishWorkAvailable(queue: string): void {
  publisher.publish(CHANNELS.workAvailable, queue).catch((err: unknown) => {
    logger.debug({ err, queue }, 'work-available publish failed (dispatcher poll will cover it)');
  });
}

export function publishEvent(event: SchedulerEvent): void {
  publisher.publish(CHANNELS.events, JSON.stringify(event)).catch((err: unknown) => {
    logger.debug({ err, type: event.type }, 'event publish failed');
  });
}

export async function subscribeWorkAvailable(handler: (queue: string) => void): Promise<void> {
  await subscriber.subscribe(CHANNELS.workAvailable);
  subscriber.on('message', (channel: string, message: string) => {
    if (channel === CHANNELS.workAvailable) handler(message);
  });
}

export async function subscribeEvents(handler: (event: SchedulerEvent) => void): Promise<void> {
  await subscriber.subscribe(CHANNELS.events);
  subscriber.on('message', (channel: string, message: string) => {
    if (channel !== CHANNELS.events) return;
    try {
      handler(JSON.parse(message) as SchedulerEvent);
    } catch (err) {
      logger.warn({ err }, 'malformed scheduler event');
    }
  });
}
