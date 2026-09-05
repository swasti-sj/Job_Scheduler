import { hostname } from 'node:os';
import { randomUUID } from 'node:crypto';
import WebSocket from 'ws';
import { config } from '../config.js';
import { logger } from '../logger.js';
import type { JobAssignment } from '../domain/job.js';
import { resolveHandler, type JobHandler } from './handlers.js';
import type { SchedulerMessage, WorkerMessage } from '../api/ws/protocol.js';
import { frameToString } from '../api/ws/rawData.js';

export interface WorkerOptions {
  id?: string;
  url?: string;
  queues?: string[];
  concurrency?: number;
  handlers?: Record<string, JobHandler>;
}

/**
 * Worker process.
 *
 * Holds one persistent WebSocket to a scheduler. Its contract with the server:
 *
 *  - identify once (`hello`), then heartbeat every 5s carrying the ids of
 *    everything still in flight, which is what extends those leases;
 *  - `ack` a job when execution actually starts (CLAIMED -> RUNNING), so a job
 *    that was dispatched but never picked up is distinguishable from one that
 *    genuinely ran;
 *  - report exactly one `result` per job, and honour `revoked` by abandoning
 *    silently - the job has already been given to someone else, and reporting on
 *    it would be rejected by the ownership guard anyway.
 *
 * On disconnect it reconnects with jittered backoff and, critically, abandons
 * everything it was holding: those jobs were reclaimed the moment the socket
 * closed, so finishing them would be duplicate work at best.
 */
export class Worker {
  readonly id: string;
  private readonly url: string;
  private readonly queues: string[];
  private readonly concurrency: number;
  private readonly handlers: Record<string, JobHandler> | undefined;

  private socket: WebSocket | null = null;
  private heartbeat: NodeJS.Timeout | null = null;
  private readonly inflight = new Map<string, AbortController>();
  private stopping = false;
  private reconnectDelay = 250;

  constructor(options: WorkerOptions = {}) {
    this.id = options.id ?? `worker-${hostname()}-${process.pid}-${randomUUID().slice(0, 8)}`;
    this.url = options.url ?? config.schedulerWsUrl;
    this.queues = options.queues ?? config.workerQueues;
    this.concurrency = options.concurrency ?? config.workerConcurrency;
    this.handlers = options.handlers;
  }

  get inflightCount(): number {
    return this.inflight.size;
  }

  start(): void {
    this.stopping = false;
    this.connect();
  }

  private connect(): void {
    if (this.stopping) return;
    logger.info({ workerId: this.id, url: this.url }, 'connecting to scheduler');
    const socket = new WebSocket(this.url);
    this.socket = socket;

    socket.on('open', () => {
      this.reconnectDelay = 250;
      this.send({
        type: 'hello',
        worker_id: this.id,
        hostname: hostname(),
        pid: process.pid,
        queues: this.queues,
        max_concurrency: this.concurrency,
      });
      this.startHeartbeat();
      this.requestWork();
    });

    socket.on('message', (data) => {
      let message: SchedulerMessage;
      try {
        message = JSON.parse(frameToString(data)) as SchedulerMessage;
      } catch (err) {
        logger.warn({ err }, 'unparseable scheduler message');
        return;
      }
      this.handleMessage(message);
    });

    socket.on('close', (code) => {
      this.stopHeartbeat();
      // Everything in flight has already been reclaimed server side; running it
      // to completion would be duplicated work with no one to report to.
      for (const controller of this.inflight.values()) controller.abort();
      this.inflight.clear();

      if (this.stopping) return;
      const wait = this.reconnectDelay + Math.floor(Math.random() * 250);
      logger.warn({ code, retryInMs: wait }, 'scheduler socket closed; reconnecting');
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 10_000);
      setTimeout(() => this.connect(), wait).unref();
    });

    socket.on('error', (err) => logger.warn({ err }, 'scheduler socket error'));
  }

  private handleMessage(message: SchedulerMessage): void {
    switch (message.type) {
      case 'welcome':
        logger.info({ node: message.node_id }, 'registered with scheduler');
        return;
      case 'assign':
        for (const job of message.jobs) void this.execute(job);
        return;
      case 'heartbeat_ack':
        for (const id of message.revoked) {
          const controller = this.inflight.get(id);
          if (controller !== undefined) {
            logger.warn({ jobId: id }, 'job revoked by scheduler; abandoning');
            controller.abort();
            this.inflight.delete(id);
          }
        }
        return;
      case 'shutdown':
        logger.info({ reason: message.reason }, 'scheduler is shutting down');
        return;
      case 'error':
        logger.warn({ message: message.message }, 'scheduler reported an error');
        return;
      default: {
        const exhaustive: never = message;
        logger.warn({ message: exhaustive }, 'unknown scheduler message');
      }
    }
  }

  private async execute(job: JobAssignment): Promise<void> {
    const controller = new AbortController();
    this.inflight.set(job.id, controller);
    this.send({ type: 'ack', job_id: job.id });

    const handler =
      this.handlers?.[job.job_type] ?? resolveHandler(job.job_type);

    try {
      const result = await handler(job);
      if (controller.signal.aborted) return;
      this.send({ type: 'result', job_id: job.id, ok: true, result });
    } catch (err) {
      if (controller.signal.aborted) return;
      this.send({
        type: 'result',
        job_id: job.id,
        ok: false,
        error: err instanceof Error ? `${err.name}: ${err.message}` : String(err),
      });
    } finally {
      this.inflight.delete(job.id);
      this.requestWork();
    }
  }

  private requestWork(): void {
    const slots = this.concurrency - this.inflight.size;
    if (slots > 0) this.send({ type: 'pull', slots });
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeat = setInterval(() => {
      this.send({ type: 'heartbeat', inflight: [...this.inflight.keys()] });
    }, config.workerHeartbeatIntervalMs);
    this.heartbeat.unref();
  }

  private stopHeartbeat(): void {
    if (this.heartbeat !== null) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }
  }

  private send(message: WorkerMessage): void {
    const socket = this.socket;
    if (socket === null || socket.readyState !== WebSocket.OPEN) return;
    try {
      socket.send(JSON.stringify(message));
    } catch (err) {
      logger.warn({ err, type: message.type }, 'worker send failed');
    }
  }

  /**
   * Graceful stop: stop pulling new work, let what is in flight finish and
   * report, then close. Anything still running past the grace window is left to
   * the server's reclaim paths.
   */
  async stop(graceMs = 15_000): Promise<void> {
    this.stopping = true;
    const deadline = Date.now() + graceMs;
    while (this.inflight.size > 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    this.stopHeartbeat();
    this.socket?.close(1000, 'worker shutting down');
    this.socket = null;
  }
}
