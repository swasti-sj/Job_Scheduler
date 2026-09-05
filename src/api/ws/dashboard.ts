import type { WebSocket } from 'ws';
import { config } from '../../config.js';
import { logger } from '../../logger.js';
import { queueStats, recentFailures, summariseQueues, throughput, workerStats } from '../../core/stats.js';
import { subscribeEvents, type SchedulerEvent } from '../../core/events.js';
import { dashboardClients, dashboardFramesDropped } from '../../metrics.js';

/**
 * A bounded ring buffer of outbound frames.
 *
 * When it is full, the *oldest* frame is discarded, not the newest. For a live
 * dashboard the newest frame supersedes everything behind it - a viewer who
 * fell behind wants current queue depths, not a replay of the last minute - so
 * dropping from the front is both cheaper and more useful.
 */
export class RingBuffer<T> {
  private readonly items: (T | undefined)[];
  private head = 0;
  private count = 0;

  constructor(private readonly capacity: number) {
    this.items = new Array<T | undefined>(capacity);
  }

  /** Returns true if pushing evicted an older item. */
  push(item: T): boolean {
    const evicted = this.count === this.capacity;
    const index = (this.head + this.count) % this.capacity;
    if (evicted) {
      this.items[this.head] = item;
      this.head = (this.head + 1) % this.capacity;
    } else {
      this.items[index] = item;
      this.count += 1;
    }
    return evicted;
  }

  shift(): T | undefined {
    if (this.count === 0) return undefined;
    const item = this.items[this.head];
    this.items[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this.count -= 1;
    return item;
  }

  get size(): number {
    return this.count;
  }
}

interface DashboardClient {
  socket: WebSocket;
  queue: RingBuffer<string>;
  dropped: number;
}

/**
 * /dashboard stream.
 *
 * Backpressure is the whole design constraint here. A dashboard client on a
 * hotel wifi connection must never be able to grow this process's heap: `ws`
 * happily buffers everything you hand it, so a socket that acknowledges nothing
 * turns an unthrottled 1Hz broadcast into an unbounded memory leak that ends in
 * an OOM kill of a *scheduler*. Two rules prevent that:
 *
 *   1. Before every send, check `ws.bufferedAmount`. Above the threshold the
 *      client is behind, so the frame goes into its ring buffer instead of into
 *      the socket.
 *   2. The ring buffer is fixed size and evicts from the front. A client that
 *      stays behind loses old frames; memory per client is capped at
 *      ringSize frames no matter how long it misbehaves.
 *
 * The scheduler is therefore never blocked by a slow consumer, and the worst a
 * bad client can do to itself is see a gap in its history.
 */
export class DashboardHub {
  private readonly clients = new Set<DashboardClient>();
  private timer: NodeJS.Timeout | null = null;
  private broadcasting = false;

  async start(): Promise<void> {
    await subscribeEvents((event) => this.broadcastEvent(event));
    this.timer = setInterval(() => {
      void this.broadcastSnapshot();
    }, config.dashboardBroadcastIntervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer !== null) clearInterval(this.timer);
    for (const client of this.clients) client.socket.close(1001, 'shutting down');
    this.clients.clear();
    dashboardClients.set(0);
  }

  handleConnection(socket: WebSocket): void {
    const client: DashboardClient = {
      socket,
      queue: new RingBuffer<string>(config.dashboardRingSize),
      dropped: 0,
    };
    this.clients.add(client);
    dashboardClients.set(this.clients.size);

    socket.on('close', () => {
      this.clients.delete(client);
      dashboardClients.set(this.clients.size);
      if (client.dropped > 0) {
        logger.info({ dropped: client.dropped }, 'slow dashboard client disconnected');
      }
    });
    socket.on('error', (err) => logger.debug({ err }, 'dashboard socket error'));
    // Drained sockets get their backlog immediately rather than on the next tick.
    socket.on('drain', () => this.flush(client));

    void this.snapshot().then((frame) => this.enqueue(client, frame));
  }

  private async snapshot(): Promise<string> {
    const [rows, workers, failures, rate] = await Promise.all([
      queueStats(),
      workerStats(),
      recentFailures(20),
      throughput(10),
    ]);
    return JSON.stringify({
      type: 'snapshot',
      at: new Date().toISOString(),
      queues: summariseQueues(rows),
      by_tenant: rows,
      workers,
      throughput: rate,
      recent_failures: failures,
    });
  }

  private async broadcastSnapshot(): Promise<void> {
    if (this.clients.size === 0 || this.broadcasting) return;
    this.broadcasting = true;
    try {
      const frame = await this.snapshot();
      for (const client of this.clients) this.enqueue(client, frame);
    } catch (err) {
      logger.warn({ err }, 'dashboard snapshot failed');
    } finally {
      this.broadcasting = false;
    }
  }

  private broadcastEvent(event: SchedulerEvent): void {
    this.broadcastFrame(JSON.stringify({ type: 'event', event }));
  }

  /** Sends one prepared frame to every client, applying backpressure per client. */
  broadcastFrame(frame: string): void {
    if (this.clients.size === 0) return;
    for (const client of this.clients) this.enqueue(client, frame);
  }

  /** Frames a client has been unable to accept, for tests and diagnostics. */
  droppedFor(socket: WebSocket): number {
    for (const client of this.clients) {
      if (client.socket === socket) return client.dropped;
    }
    return 0;
  }

  get clientCount(): number {
    return this.clients.size;
  }

  private enqueue(client: DashboardClient, frame: string): void {
    if (client.socket.readyState !== client.socket.OPEN) return;

    // Rule 1: only write when the socket is actually keeping up.
    if (client.socket.bufferedAmount <= config.dashboardMaxBufferedBytes && client.queue.size === 0) {
      this.write(client, frame);
      return;
    }

    // Rule 2: bounded buffering, oldest frame evicted first.
    const evicted = client.queue.push(frame);
    if (evicted) {
      client.dropped += 1;
      dashboardFramesDropped.inc({ reason: 'ring_full' });
    }
    this.flush(client);
  }

  private flush(client: DashboardClient): void {
    while (
      client.queue.size > 0 &&
      client.socket.readyState === client.socket.OPEN &&
      client.socket.bufferedAmount <= config.dashboardMaxBufferedBytes
    ) {
      const frame = client.queue.shift();
      if (frame === undefined) break;
      this.write(client, frame);
    }
  }

  private write(client: DashboardClient, frame: string): void {
    try {
      client.socket.send(frame);
    } catch (err) {
      logger.debug({ err }, 'dashboard send failed');
      dashboardFramesDropped.inc({ reason: 'send_error' });
    }
  }
}
