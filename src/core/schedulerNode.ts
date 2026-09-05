import { config } from '../config.js';
import { logger } from '../logger.js';
import { AdvisoryLockLeader } from './leader.js';
import { reapDeadWorkers, reclaimExpiredLeases } from './reclaim.js';
import { evaluateBreakers } from './breaker.js';
import { runDueCrons } from './cronRunner.js';
import { queueStats } from './stats.js';

/**
 * The singleton duties.
 *
 * Everything here must run on exactly one node at a time, so it is all gated on
 * the advisory lock. Note what is *not* here: claiming, dispatch, worker sockets
 * and API reads all keep running on followers, because Postgres - not the leader
 * - arbitrates those. That split is what keeps a leader failover invisible to
 * clients: losing the leader pauses reaping and cron for at most one poll
 * interval, and pauses nothing else at all.
 *
 * Each duty is scheduled with its own self-rescheduling timer rather than one
 * shared interval, so a slow reaper pass cannot delay breaker evaluation, and a
 * pass that overruns its interval simply starts the next one late instead of
 * stacking up concurrent copies of itself.
 */
export class SchedulerNode {
  readonly leader = new AdvisoryLockLeader();
  private readonly timers = new Map<string, NodeJS.Timeout>();
  private running = false;

  async start(): Promise<void> {
    this.running = true;
    this.leader.on('elected', () => logger.info('assuming leader duties: reaper, breakers, cron'));
    this.leader.on('deposed', () => logger.warn('leader duties suspended'));
    await this.leader.start();

    // Reaper: the lease-expiry reclaim path plus dead-worker detection.
    this.every(config.reaperIntervalMs, 'reaper', async () => {
      const result = await reclaimExpiredLeases();
      if (result.requeued.length + result.dead.length > 0) {
        logger.warn(
          { requeued: result.requeued.length, dead: result.dead.length },
          'reaper returned expired leases',
        );
      }
      await reapDeadWorkers();
    });

    this.every(5000, 'breakers', () => evaluateBreakers());
    this.every(1000, 'cron', () => runDueCrons().then(() => undefined));

    // Keeps queue_depth gauges fresh on the leader even with no dashboard open.
    this.every(5000, 'stats', () => queueStats().then(() => undefined));
  }

  /** Runs `fn` every `intervalMs`, but only while this node holds the lock. */
  private every(intervalMs: number, name: string, fn: () => Promise<void>): void {
    const tick = async (): Promise<void> => {
      if (!this.running) return;
      if (this.leader.isLeader) {
        try {
          await fn();
        } catch (err) {
          logger.error({ err, duty: name }, 'leader duty failed');
        }
      }
      if (this.running) {
        const timer = setTimeout(() => void tick(), intervalMs);
        timer.unref();
        this.timers.set(name, timer);
      }
    };
    void tick();
  }

  async stop(): Promise<void> {
    this.running = false;
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
    await this.leader.stop();
  }
}
