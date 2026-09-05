import { describe, expect, it } from 'vitest';
import type { WebSocket } from 'ws';
import { DashboardHub, RingBuffer } from '../../src/api/ws/dashboard.js';
import { config } from '../../src/config.js';

describe('RingBuffer', () => {
  it('fills without evicting up to capacity', () => {
    const ring = new RingBuffer<number>(3);
    expect(ring.push(1)).toBe(false);
    expect(ring.push(2)).toBe(false);
    expect(ring.push(3)).toBe(false);
    expect(ring.size).toBe(3);
  });

  it('evicts the OLDEST item once full, not the newest', () => {
    // For a live dashboard the newest frame supersedes the ones behind it, so a
    // client that fell behind should be shown current state, not a replay.
    const ring = new RingBuffer<number>(3);
    for (const n of [1, 2, 3]) ring.push(n);
    expect(ring.push(4)).toBe(true);

    expect(ring.shift()).toBe(2);
    expect(ring.shift()).toBe(3);
    expect(ring.shift()).toBe(4);
    expect(ring.shift()).toBeUndefined();
  });

  it('never exceeds its capacity however much is pushed', () => {
    const ring = new RingBuffer<number>(8);
    for (let i = 0; i < 100_000; i += 1) ring.push(i);
    expect(ring.size).toBe(8);
    // And it holds exactly the last 8.
    const drained: number[] = [];
    let item = ring.shift();
    while (item !== undefined) {
      drained.push(item);
      item = ring.shift();
    }
    expect(drained).toEqual([99_992, 99_993, 99_994, 99_995, 99_996, 99_997, 99_998, 99_999]);
  });

  it('survives interleaved push and shift without corrupting order', () => {
    const ring = new RingBuffer<number>(4);
    ring.push(1);
    ring.push(2);
    expect(ring.shift()).toBe(1);
    ring.push(3);
    ring.push(4);
    ring.push(5);
    expect(ring.size).toBe(4);
    expect(ring.shift()).toBe(2);
    expect(ring.shift()).toBe(3);
  });
});

/** Minimal stand-in for a `ws` socket, with a controllable send buffer. */
class FakeSocket {
  readonly OPEN = 1;
  readyState = 1;
  bufferedAmount = 0;
  readonly sent: string[] = [];
  private readonly listeners = new Map<string, Array<(...args: unknown[]) => void>>();

  send(frame: string): void {
    this.sent.push(frame);
    // A real socket grows its buffer until the OS drains it.
    this.bufferedAmount += frame.length;
  }

  on(event: string, listener: (...args: unknown[]) => void): this {
    const list = this.listeners.get(event) ?? [];
    list.push(listener);
    this.listeners.set(event, list);
    return this;
  }

  emit(event: string): void {
    for (const listener of this.listeners.get(event) ?? []) listener();
  }

  close(): void {
    this.readyState = 3;
    this.emit('close');
  }

  /** Simulates the OS draining the send buffer. */
  drain(): void {
    this.bufferedAmount = 0;
    this.emit('drain');
  }

  asWebSocket(): WebSocket {
    return this as unknown as WebSocket;
  }
}

describe('dashboard backpressure', () => {
  it('writes straight through while the client keeps up', () => {
    const hub = new DashboardHub();
    const socket = new FakeSocket();
    hub.handleConnection(socket.asWebSocket());

    for (let i = 0; i < 5; i += 1) {
      socket.bufferedAmount = 0; // drained between frames
      hub.broadcastFrame(`frame-${i}`);
    }
    expect(socket.sent).toEqual(['frame-0', 'frame-1', 'frame-2', 'frame-3', 'frame-4']);
    expect(hub.droppedFor(socket.asWebSocket())).toBe(0);
  });

  it('stops writing once bufferedAmount exceeds the threshold', () => {
    const hub = new DashboardHub();
    const socket = new FakeSocket();
    hub.handleConnection(socket.asWebSocket());

    // A client that is not draining at all.
    socket.bufferedAmount = config.dashboardMaxBufferedBytes + 1;
    for (let i = 0; i < 10; i += 1) hub.broadcastFrame(`frame-${i}`);

    // Nothing was pushed into the socket - the scheduler is not blocked and the
    // socket's own buffer is not grown further.
    expect(socket.sent).toHaveLength(0);
  });

  it('caps memory for a stalled client instead of growing without bound', () => {
    const hub = new DashboardHub();
    const socket = new FakeSocket();
    hub.handleConnection(socket.asWebSocket());

    socket.bufferedAmount = config.dashboardMaxBufferedBytes + 1;
    const overflow = config.dashboardRingSize * 20;
    for (let i = 0; i < overflow; i += 1) hub.broadcastFrame(`frame-${i}`);

    // Everything past the ring's capacity was dropped rather than retained.
    const dropped = hub.droppedFor(socket.asWebSocket());
    expect(dropped).toBe(overflow - config.dashboardRingSize);
    expect(socket.sent).toHaveLength(0);
  });

  it('delivers the NEWEST frames when a stalled client recovers', () => {
    const hub = new DashboardHub();
    const socket = new FakeSocket();
    hub.handleConnection(socket.asWebSocket());

    socket.bufferedAmount = config.dashboardMaxBufferedBytes + 1;
    const total = config.dashboardRingSize + 10;
    for (let i = 0; i < total; i += 1) hub.broadcastFrame(`frame-${i}`);

    // The client catches up: it should see recent state, not ancient history.
    socket.bufferedAmount = 0;
    socket.emit('drain');

    expect(socket.sent.length).toBeGreaterThan(0);
    expect(socket.sent[0]).toBe(`frame-${total - config.dashboardRingSize}`);
    expect(socket.sent).not.toContain('frame-0');
  });

  it('ignores a closed socket rather than throwing', () => {
    const hub = new DashboardHub();
    const socket = new FakeSocket();
    hub.handleConnection(socket.asWebSocket());
    socket.close();

    expect(() => hub.broadcastFrame('after-close')).not.toThrow();
    expect(socket.sent).toHaveLength(0);
    expect(hub.clientCount).toBe(0);
  });

  it('keeps one slow client from affecting a healthy one', () => {
    const hub = new DashboardHub();
    const slow = new FakeSocket();
    const fast = new FakeSocket();
    hub.handleConnection(slow.asWebSocket());
    hub.handleConnection(fast.asWebSocket());

    slow.bufferedAmount = config.dashboardMaxBufferedBytes + 1;
    for (let i = 0; i < 5; i += 1) {
      fast.bufferedAmount = 0;
      hub.broadcastFrame(`frame-${i}`);
    }

    expect(slow.sent).toHaveLength(0);
    expect(fast.sent).toHaveLength(5);
  });
});
