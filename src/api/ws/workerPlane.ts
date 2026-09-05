import type { WebSocket } from 'ws';
import { config } from '../../config.js';
import { logger } from '../../logger.js';
import { query } from '../../db/pool.js';
import { claimJobs, toAssignment } from '../../core/claim.js';
import { completeJob, extendLeases, failJob, markRunning } from '../../core/lifecycle.js';
import { reclaimWorkerJobs } from '../../core/reclaim.js';
import { subscribeWorkAvailable } from '../../core/events.js';
import { workersConnected } from '../../metrics.js';
import { parseWorkerMessage, type SchedulerMessage, type WorkerMessage } from './protocol.js';
import { frameToString } from './rawData.js';

interface WorkerConnection {
  id: string;
  socket: WebSocket;
  queues: string[];
  maxConcurrency: number;
  inflight: Set<string>;
  lastBeat: number;
  /** Next queue to try, so a multi-queue worker round-robins fairly. */
  cursor: number;
  hungry: boolean;
  closing: boolean;
}

/**
 * Worker control plane.
 *
 * Every node (leader or follower) runs one of these; workers connect to whichever
 * they were load balanced onto. Nothing here needs leadership because claiming is
 * arbitrated by Postgres, not by the process - which is exactly why a follower
 * losing the leader does not interrupt dispatch.
 */
export class WorkerHub {
  private readonly connections = new Map<string, WorkerConnection>();
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private pollTimer: NodeJS.Timeout | null = null;
  private draining = false;
  private stopped = false;
  /** Current fallback-poll interval; grows while idle, resets on any signal. */
  private pollIntervalMs = config.claimPollIntervalMs;
  private readonly maxPollIntervalMs = Math.max(
    config.claimPollIntervalMs,
    config.claimPollMaxIntervalMs,
  );

  async start(): Promise<void> {
    // Wake up the moment work is enqueued anywhere in the cluster.
    await subscribeWorkAvailable((queue) => this.onWorkAvailable(queue));

    this.heartbeatTimer = setInterval(() => {
      this.checkHeartbeats();
    }, config.workerHeartbeatIntervalMs);
    this.heartbeatTimer.unref();

    // Fallback poll. Redis pub/sub is at-most-once, and a job can also become
    // ready simply because its scheduled_for elapsed (a retry backoff, a delayed
    // job), which produces no notification at all - so a timer is required for
    // correctness, not merely as a safety net.
    //
    // The interval adapts. Polling flat out at 20ms against an empty queue costs
    // a claim query per worker per tick forever: pure load on Postgres and pure
    // garbage on our own event loop, in exchange for nothing. So an empty poll
    // doubles the interval up to a ceiling, and anything that suggests work
    // exists - a Redis wakeup, a finished job, a fresh connection - resets it to
    // the floor. Idle costs almost nothing; busy still dispatches at full speed.
    // Jitter keeps the nodes in the cluster from synchronising onto one tick.
    const schedulePoll = (): void => {
      if (this.stopped) return;
      const jitter = Math.floor(Math.random() * config.claimPollJitterMs);
      this.pollTimer = setTimeout(() => {
        for (const conn of this.connections.values()) conn.hungry = true;
        void this.drain().finally(schedulePoll);
      }, this.pollIntervalMs + jitter);
      this.pollTimer.unref();
    };
    schedulePoll();
  }

  async stop(reason = 'scheduler shutting down'): Promise<void> {
    this.stopped = true;
    if (this.heartbeatTimer !== null) clearInterval(this.heartbeatTimer);
    if (this.pollTimer !== null) clearTimeout(this.pollTimer);
    for (const conn of this.connections.values()) {
      conn.closing = true;
      this.send(conn, { type: 'shutdown', reason });
      conn.socket.close(1001, reason);
    }
  }

  get size(): number {
    return this.connections.size;
  }

  // -------------------------------------------------------------------------
  // Connection lifecycle
  // -------------------------------------------------------------------------

  handleConnection(socket: WebSocket): void {
    // Registration is async (it writes to Postgres), so anything that arrives
    // between `hello` and the row being written waits on the same promise rather
    // than being rejected for "no hello yet".
    let registering: Promise<WorkerConnection> | null = null;
    let conn: WorkerConnection | null = null;

    // A socket that never identifies itself is a resource leak; close it.
    const helloTimer = setTimeout(() => {
      if (registering === null) socket.close(1002, 'no hello');
    }, 10_000);
    helloTimer.unref();

    socket.on('message', (data) => {
      void (async () => {
        let message: WorkerMessage;
        try {
          message = parseWorkerMessage(frameToString(data));
        } catch (err) {
          logger.warn({ err }, 'unparseable worker message');
          return;
        }

        if (message.type === 'hello') {
          clearTimeout(helloTimer);
          registering ??= this.register(socket, message);
          conn = await registering;
          return;
        }

        if (registering === null) {
          this.rawSend(socket, { type: 'error', message: 'hello required first' });
          return;
        }
        const active = conn ?? (await registering);
        conn = active;
        await this.handleMessage(active, message);
      })().catch((err: unknown) => {
        logger.error({ err, workerId: conn?.id }, 'worker message handling failed');
      });
    });

    socket.on('close', () => {
      clearTimeout(helloTimer);
      const closed = conn;
      if (closed !== null) {
        void this.onClose(closed);
      } else if (registering !== null) {
        // Closed mid-registration: wait for the row, then clean it up.
        void registering.then((c) => this.onClose(c)).catch(() => undefined);
      }
    });

    socket.on('error', (err) => {
      logger.warn({ err, workerId: conn?.id }, 'worker socket error');
    });
  }

  private async register(
    socket: WebSocket,
    hello: Extract<WorkerMessage, { type: 'hello' }>,
  ): Promise<WorkerConnection> {
    // A reconnecting worker with the same id replaces its old socket; the old
    // one is closed so we never dispatch down a dead connection.
    const previous = this.connections.get(hello.worker_id);
    if (previous !== undefined && previous.socket !== socket) {
      previous.closing = true;
      previous.socket.close(1012, 'replaced by a newer connection');
    }

    await query(
      `INSERT INTO workers (id, hostname, pid, queues, max_concurrency, state, connected_node,
                            last_heartbeat_at, registered_at)
       VALUES ($1, $2, $3, $4::text[], $5, 'IDLE', $6, now(), now())
       ON CONFLICT (id) DO UPDATE
       SET hostname = EXCLUDED.hostname, pid = EXCLUDED.pid, queues = EXCLUDED.queues,
           max_concurrency = EXCLUDED.max_concurrency, state = 'IDLE',
           connected_node = EXCLUDED.connected_node, last_heartbeat_at = now(),
           inflight_count = 0, current_job_id = NULL`,
      [
        hello.worker_id,
        hello.hostname,
        hello.pid,
        hello.queues,
        hello.max_concurrency,
        config.nodeId,
      ],
      'worker_register',
    );

    const conn: WorkerConnection = {
      id: hello.worker_id,
      socket,
      queues: hello.queues.length > 0 ? hello.queues : ['default'],
      maxConcurrency: Math.max(1, hello.max_concurrency),
      inflight: new Set(),
      lastBeat: Date.now(),
      cursor: 0,
      hungry: true,
      closing: false,
    };
    this.connections.set(conn.id, conn);
    workersConnected.set(this.connections.size);
    logger.info({ workerId: conn.id, queues: conn.queues }, 'worker connected');

    this.resetPollInterval();
    this.send(conn, {
      type: 'welcome',
      node_id: config.nodeId,
      heartbeat_interval_ms: config.workerHeartbeatIntervalMs,
      lease_seconds: config.leaseDurationSeconds,
    });
    void this.drain();
    return conn;
  }

  /**
   * RECLAIM PATH 1a - socket close.
   *
   * The instant the transport drops we know this worker cannot finish anything,
   * so its jobs go back immediately rather than waiting out the lease. A
   * SIGKILLed worker gets here too: the kernel closes its sockets on process
   * death, so the FIN/RST arrives in milliseconds.
   */
  private async onClose(conn: WorkerConnection): Promise<void> {
    if (this.connections.get(conn.id) === conn) {
      this.connections.delete(conn.id);
      workersConnected.set(this.connections.size);
    } else {
      // Superseded by a reconnect; the new socket owns this worker now.
      return;
    }

    logger.warn({ workerId: conn.id, inflight: conn.inflight.size }, 'worker disconnected');
    try {
      await query(
        `UPDATE workers SET state = 'DEAD', current_job_id = NULL, inflight_count = 0
         WHERE id = $1 AND connected_node = $2`,
        [conn.id, config.nodeId],
        'worker_dead',
      );
      await reclaimWorkerJobs(conn.id, 'socket_close');
    } catch (err) {
      // The lease reaper (path 2) is the backstop when this fails.
      logger.error({ err, workerId: conn.id }, 'immediate reclaim failed; lease expiry will cover it');
    }
  }

  /**
   * RECLAIM PATH 1b - heartbeat loss.
   *
   * A worker can be wedged with its socket still open (blocked syscall, long GC,
   * a partition that has not produced a FIN yet). After three missed 5s beats we
   * stop believing it and hang up, which routes into onClose above. Detection is
   * therefore bounded at ~15s even when the transport lies to us.
   */
  private checkHeartbeats(): void {
    const deadline = Date.now() - config.workerHeartbeatIntervalMs * config.workerMissedBeats;
    for (const conn of this.connections.values()) {
      if (conn.closing || conn.lastBeat >= deadline) continue;
      logger.warn(
        { workerId: conn.id, silentMs: Date.now() - conn.lastBeat },
        'worker missed heartbeats; terminating socket',
      );
      conn.closing = true;
      // terminate(), not close(): a wedged peer may never complete the closing
      // handshake, and we are not willing to wait for it.
      conn.socket.terminate();
    }
  }

  // -------------------------------------------------------------------------
  // Messages
  // -------------------------------------------------------------------------

  private async handleMessage(conn: WorkerConnection, message: WorkerMessage): Promise<void> {
    switch (message.type) {
      case 'hello':
        return;

      case 'heartbeat': {
        conn.lastBeat = Date.now();
        const revoked = await extendLeases(conn.id, message.inflight);
        for (const id of revoked) conn.inflight.delete(id);
        await query(
          `UPDATE workers SET last_heartbeat_at = now(), inflight_count = $2,
                              state = CASE WHEN $2 > 0 THEN 'BUSY'::worker_state ELSE 'IDLE'::worker_state END
           WHERE id = $1`,
          [conn.id, conn.inflight.size],
          'worker_heartbeat',
        );
        this.send(conn, { type: 'heartbeat_ack', revoked });
        return;
      }

      case 'ack': {
        await markRunning(message.job_id, conn.id);
        return;
      }

      case 'result': {
        const outcome = message.ok
          ? await completeJob(message.job_id, conn.id, message.result ?? null)
          : await failJob(message.job_id, conn.id, message.error);
        conn.inflight.delete(message.job_id);
        if (outcome === 'not_owner') {
          logger.warn({ jobId: message.job_id, workerId: conn.id }, 'stale result discarded');
        }
        conn.hungry = true;
        this.resetPollInterval();
        void this.drain();
        return;
      }

      case 'pull': {
        this.resetPollInterval();
        conn.hungry = true;
        void this.drain();
        return;
      }

      default: {
        const exhaustive: never = message;
        logger.warn({ message: exhaustive }, 'unknown worker message type');
      }
    }
  }

  /** Something suggests there is work: go back to polling at full speed. */
  private resetPollInterval(): void {
    this.pollIntervalMs = config.claimPollIntervalMs;
  }

  private onWorkAvailable(queue: string): void {
    this.resetPollInterval();
    let any = false;
    for (const conn of this.connections.values()) {
      if (conn.queues.includes(queue) && conn.inflight.size < conn.maxConcurrency) {
        conn.hungry = true;
        any = true;
      }
    }
    if (any) void this.drain();
  }

  // -------------------------------------------------------------------------
  // Dispatch
  // -------------------------------------------------------------------------

  /**
   * Drains the hungry set.
   *
   * Single-flight (`draining`) so a burst of enqueue notifications collapses into
   * one pass instead of N concurrent claim storms. Every DB call is awaited, and
   * a `setImmediate` yield between workers hands the loop back to the HTTP server
   * - this is the loop that would otherwise starve the event loop under load,
   * which is why the event_loop_lag_ms metric watches it.
   */
  private async drain(): Promise<void> {
    if (this.draining || this.stopped) return;
    this.draining = true;
    try {
      let progressed = true;
      while (progressed && !this.stopped) {
        progressed = false;

        const hungry = [...this.connections.values()].filter((conn) => {
          if (!conn.hungry || conn.closing) return false;
          if (conn.maxConcurrency - conn.inflight.size <= 0) {
            conn.hungry = false;
            return false;
          }
          return true;
        });
        if (hungry.length === 0) break;

        // Claims for different workers are independent transactions on separate
        // pooled connections, and SKIP LOCKED means they cannot collide - so
        // they go out concurrently. Doing this serially would make dispatch
        // latency the sum of every worker's round trip instead of the max of
        // them, which is the difference between a queue that keeps up and one
        // that falls behind as workers are added.
        const dispatched = await Promise.all(
          hungry.map(async (conn) => {
            const slots = conn.maxConcurrency - conn.inflight.size;
            const count = await this.dispatchTo(conn, slots);
            if (count === 0) conn.hungry = false;
            return count;
          }),
        );
        if (dispatched.some((n) => n > 0)) {
          progressed = true;
          this.resetPollInterval();
        }

        // Hand the loop back to the HTTP server and the sockets between passes.
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
      if (!progressed) {
        this.pollIntervalMs = Math.min(this.pollIntervalMs * 2, this.maxPollIntervalMs);
      }
    } catch (err) {
      logger.error({ err }, 'dispatch drain failed');
    } finally {
      this.draining = false;
    }
  }

  private async dispatchTo(conn: WorkerConnection, slots: number): Promise<number> {
    // Round-robin the worker's queues so a busy first queue cannot starve the
    // rest of that worker's subscriptions.
    for (let i = 0; i < conn.queues.length; i += 1) {
      const queue = conn.queues[(conn.cursor + i) % conn.queues.length];
      if (queue === undefined) continue;

      const rows = await claimJobs({ workerId: conn.id, queue, limit: slots });
      if (rows.length === 0) continue;

      conn.cursor = (conn.cursor + i + 1) % conn.queues.length;
      for (const row of rows) conn.inflight.add(row.id);

      const delivered = this.send(conn, { type: 'assign', jobs: rows.map(toAssignment) });
      if (!delivered) {
        // The socket died between claim and send. Give the work straight back
        // instead of letting it sit until the lease expires.
        for (const row of rows) conn.inflight.delete(row.id);
        await reclaimWorkerJobs(conn.id, 'socket_close');
        return 0;
      }
      return rows.length;
    }
    return 0;
  }

  // -------------------------------------------------------------------------

  private send(conn: WorkerConnection, message: SchedulerMessage): boolean {
    return this.rawSend(conn.socket, message);
  }

  private rawSend(socket: WebSocket, message: SchedulerMessage): boolean {
    if (socket.readyState !== socket.OPEN) return false;
    try {
      socket.send(JSON.stringify(message));
      return true;
    } catch (err) {
      logger.warn({ err, type: message.type }, 'worker send failed');
      return false;
    }
  }
}
