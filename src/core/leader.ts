import { EventEmitter } from 'node:events';
import type pg from 'pg';
import { createDedicatedClient } from '../db/pool.js';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { isLeader, leaderTransitions, leaderFailoverSeconds } from '../metrics.js';
import { publishEvent } from './events.js';

/**
 * Leader election on a Postgres advisory lock.
 *
 * `pg_try_advisory_lock` is session scoped: the lock lives exactly as long as the
 * backend holding it. That property is the whole point. If the leader process is
 * SIGKILLed, or its host drops off the network, Postgres tears the backend down
 * and the lock is released by the database itself - there is no lease to expire,
 * no TTL to tune, and no way for a partitioned old leader to keep believing it
 * is leader while still holding the lock, because holding the lock *is* holding
 * the connection. That is the fencing behaviour we want, for free.
 *
 * It must therefore live on a dedicated, long-lived client, never on a pooled
 * one. A pooled client is returned to the pool after each query and may be
 * handed to unrelated code, reset, or reaped by idleTimeoutMillis - any of which
 * would drop the lock while this process still thought it was the leader. That
 * is a split brain: two reapers, two cron schedulers, duplicate work.
 *
 * Failover budget: the lock is released the moment the old leader's TCP
 * connection dies, and every follower retries every `leaderPollIntervalMs`
 * (500ms), so a new leader is elected in well under the 2s target. Followers
 * keep serving API reads and worker sockets throughout - only the singleton
 * duties (reaper, breaker evaluation, cron) are gated on leadership.
 */
export interface LeaderElection {
  readonly isLeader: boolean;
  start(): Promise<void>;
  stop(): Promise<void>;
  on(event: 'elected' | 'deposed', listener: () => void): this;
}

export class AdvisoryLockLeader extends EventEmitter implements LeaderElection {
  private client: pg.Client | null = null;
  private timer: NodeJS.Timeout | null = null;
  private leader = false;
  private stopping = false;
  private campaignStartedAt = Date.now();
  /**
   * The tick currently in flight, if any.
   *
   * Two things depend on this. First, ticks must not overlap: setInterval fires
   * on a wall clock but a tick does I/O, so a slow round trip would let the next
   * tick issue a query on the *same* dedicated client while the previous one is
   * still running - which pg reports as "client is already executing a query"
   * and which leaves the lock session in an indeterminate state.
   *
   * Second, and more importantly, shutdown has to be able to *wait* for it. A
   * tick that is midway through connecting when stop() is called would otherwise
   * go on to acquire the lock immediately afterwards, on a client that stop()
   * has already stopped tracking - leaving this process holding leadership after
   * it believes it has released it, with no one able to take over until the
   * process exits.
   */
  private inFlight: Promise<void> | null = null;

  constructor(
    private readonly lockKey: number = config.leaderLockKey,
    private readonly pollIntervalMs: number = config.leaderPollIntervalMs,
  ) {
    super();
  }

  get isLeader(): boolean {
    return this.leader;
  }

  async start(): Promise<void> {
    this.stopping = false;
    this.campaignStartedAt = Date.now();
    isLeader.set(0);
    await this.tick();
    this.timer = setInterval(() => {
      void this.tick();
    }, this.pollIntervalMs);
    this.timer.unref();
  }

  private async ensureClient(): Promise<pg.Client> {
    if (this.client !== null) return this.client;
    const client = createDedicatedClient('leader');
    // If this connection dies for any reason, we are no longer the leader -
    // the lock went with it. Step down loudly rather than drifting.
    client.on('error', (err) => {
      logger.warn({ err }, 'leader lock connection failed');
      this.handleConnectionLoss();
    });
    client.on('end', () => this.handleConnectionLoss());
    await client.connect();
    this.client = client;
    return client;
  }

  private handleConnectionLoss(): void {
    this.client = null;
    if (this.leader) this.depose('lock connection lost');
  }

  private tick(): Promise<void> {
    if (this.stopping) return Promise.resolve();
    if (this.inFlight !== null) return this.inFlight;
    const run = this.runTick().finally(() => {
      this.inFlight = null;
    });
    this.inFlight = run;
    return run;
  }

  private async runTick(): Promise<void> {
    try {
      const client = await this.ensureClient();

      // Connecting took a round trip, and stop() may have been called during it.
      if (this.stopping) return;

      if (this.leader) {
        // Cheap liveness probe on the *lock* connection specifically. If it has
        // silently gone away we must find out within one poll interval, not on
        // the next thing that happens to need the leader.
        await client.query('SELECT 1');
        return;
      }

      const { rows } = await client.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_lock($1) AS locked',
        [this.lockKey],
      );
      if (rows[0]?.locked !== true) return;

      // We won the election - but if a shutdown started while that query was in
      // flight, winning is the wrong outcome. Hand it straight back so a node
      // that is actually staying up can take it.
      if (this.stopping) {
        await client
          .query('SELECT pg_advisory_unlock($1)', [this.lockKey])
          .catch(() => undefined);
        return;
      }
      this.promote();
    } catch (err) {
      logger.warn({ err }, 'leader election tick failed');
      this.client = null;
      if (this.leader) this.depose('election tick failed');
    }
  }

  private promote(): void {
    this.leader = true;
    const seconds = (Date.now() - this.campaignStartedAt) / 1000;
    isLeader.set(1);
    leaderTransitions.inc({ direction: 'acquired' });
    leaderFailoverSeconds.set(seconds);
    logger.info({ lockKey: this.lockKey, campaignSeconds: seconds }, 'became leader');
    publishEvent({
      type: 'leader_changed',
      node_id: config.nodeId,
      is_leader: true,
      at: new Date().toISOString(),
    });
    this.emit('elected');
  }

  private depose(reason: string): void {
    this.leader = false;
    this.campaignStartedAt = Date.now();
    isLeader.set(0);
    leaderTransitions.inc({ direction: 'lost' });
    logger.warn({ reason }, 'lost leadership');
    publishEvent({
      type: 'leader_changed',
      node_id: config.nodeId,
      is_leader: false,
      at: new Date().toISOString(),
    });
    this.emit('deposed');
  }

  /**
   * Graceful release. Unlocking explicitly (rather than just closing) hands
   * leadership over in one poll interval instead of waiting for the backend to
   * notice a half-closed socket.
   */
  async stop(): Promise<void> {
    // Set the flag first, so an in-flight tick sees it at its next checkpoint...
    this.stopping = true;
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
    // ...then wait for that tick to finish before touching the client. Reading
    // this.client before the tick settles is what previously let a shutdown
    // "release" a lock the process was about to acquire a millisecond later.
    await this.inFlight?.catch(() => undefined);

    const client = this.client;
    this.client = null;
    if (client === null) return;
    try {
      if (this.leader) {
        await client.query('SELECT pg_advisory_unlock($1)', [this.lockKey]);
        this.depose('graceful shutdown');
      }
    } catch (err) {
      logger.debug({ err }, 'advisory unlock failed; connection close will release it');
    } finally {
      await client.end().catch(() => undefined);
    }
  }
}
