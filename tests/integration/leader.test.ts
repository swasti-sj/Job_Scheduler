import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { AdvisoryLockLeader } from '../../src/core/leader.js';
import { query } from '../../src/db/pool.js';
import { resetDatabase, setupDatabase, sleep, teardown, waitFor } from './helpers.js';

beforeAll(setupDatabase);
afterAll(teardown);
beforeEach(resetDatabase);

// Each test campaigns on its own lock key. Sharing one key couples the tests
// through global database state: a candidate that is still finishing its last
// tick when the next test starts can hold the lock for a poll interval, and the
// next test then sits there waiting for a lock nobody is going to release.
let LOCK_KEY = 987_000;
const running: AdvisoryLockLeader[] = [];

beforeEach(() => {
  LOCK_KEY += 1;
});

afterEach(async () => {
  await Promise.all(running.splice(0).map((l) => l.stop()));

  // Guard against the real failure this could be hiding: a stop() path that
  // leaves the advisory lock held. If that ever regresses, fail here rather
  // than as a mystery timeout in whichever test happens to run next.
  const { rows } = await query<{ n: number }>(
    `SELECT count(*)::int AS n FROM pg_locks
     WHERE locktype = 'advisory' AND objid = $1 AND granted`,
    [LOCK_KEY],
  );
  expect(rows[0]?.n, 'leader stop() leaked the advisory lock').toBe(0);
});

function makeLeader(pollMs = 200): AdvisoryLockLeader {
  const leader = new AdvisoryLockLeader(LOCK_KEY, pollMs);
  running.push(leader);
  return leader;
}

describe('advisory lock leader election', () => {
  it('elects exactly one leader out of three candidates', async () => {
    const nodes = [makeLeader(), makeLeader(), makeLeader()];
    await Promise.all(nodes.map((n) => n.start()));
    await waitFor(async () => nodes.some((n) => n.isLeader), 5000, 50);

    expect(nodes.filter((n) => n.isLeader)).toHaveLength(1);

    // And Postgres agrees there is exactly one holder of the lock.
    const { rows } = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM pg_locks
       WHERE locktype = 'advisory' AND objid = $1 AND granted`,
      [LOCK_KEY],
    );
    expect(rows[0]?.n).toBe(1);
  });

  it('keeps leadership stable while the leader is healthy', async () => {
    const nodes = [makeLeader(), makeLeader()];
    await Promise.all(nodes.map((n) => n.start()));
    await waitFor(async () => nodes.some((n) => n.isLeader), 5000, 50);

    const first = nodes.findIndex((n) => n.isLeader);
    await sleep(1500);
    expect(nodes.findIndex((n) => n.isLeader)).toBe(first);
    expect(nodes.filter((n) => n.isLeader)).toHaveLength(1);
  });

  it('fails over in under two seconds when the leader releases', async () => {
    const first = makeLeader(200);
    await first.start();
    await waitFor(async () => first.isLeader, 5000, 25);

    const second = makeLeader(200);
    await second.start();
    expect(second.isLeader).toBe(false);

    const startedAt = Date.now();
    await first.stop(); // graceful: unlocks explicitly
    await waitFor(async () => second.isLeader, 5000, 10);
    const failoverMs = Date.now() - startedAt;

    expect(second.isLeader).toBe(true);
    expect(failoverMs).toBeLessThan(2000);
  });

  it('releases the lock when the connection dies, without any lease to expire', async () => {
    // This is the property the whole design rests on: the lock is session
    // scoped, so killing the backend - the moral equivalent of SIGKILLing the
    // leader - frees it immediately. There is no TTL to wait out and no way for
    // a partitioned old leader to still believe it holds the lock.
    const first = makeLeader(200);
    await first.start();
    await waitFor(async () => first.isLeader, 5000, 25);

    const second = makeLeader(200);
    await second.start();
    expect(second.isLeader).toBe(false);

    let deposals = 0;
    first.on('deposed', () => {
      deposals += 1;
    });

    // Kill exactly the backend that holds the lock - the database-side
    // equivalent of SIGKILLing the leader process.
    const startedAt = Date.now();
    const killed = await query<{ pid: number }>(
      `SELECT pg_terminate_backend(l.pid) AS ok, l.pid FROM pg_locks l
       WHERE l.locktype = 'advisory' AND l.objid = $1 AND l.granted`,
      [LOCK_KEY],
    );
    expect(killed.rows).toHaveLength(1);

    // The old leader must first notice it was deposed - it does not get to
    // carry on believing it leads - and then someone must pick the lock back up.
    // Both together are the failover, and both must fit inside the 2s budget.
    await waitFor(async () => deposals >= 1, 5000, 5);
    await waitFor(async () => first.isLeader || second.isLeader, 5000, 5);
    expect(Date.now() - startedAt).toBeLessThan(2000);

    // And there is still exactly one leader, in the process and in Postgres.
    expect([first, second].filter((n) => n.isLeader)).toHaveLength(1);
    const { rows } = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM pg_locks
       WHERE locktype = 'advisory' AND objid = $1 AND granted`,
      [LOCK_KEY],
    );
    expect(rows[0]?.n).toBe(1);
  });

  it('does not acquire the lock during shutdown, even if a tick is mid-flight', async () => {
    // The race this pins: stop() is called while a tick is between "connect" and
    // "pg_try_advisory_lock". Without the in-flight handshake, the tick goes on
    // to win the election on a client stop() has already stopped tracking, and
    // the process holds leadership - and the connection - for the rest of its
    // life, so no other node can ever take over.
    const node = makeLeader(50);
    const startPromise = node.start();

    // Stop the instant the campaign begins, i.e. while a tick is in flight.
    await node.stop();
    await startPromise.catch(() => undefined);

    // Give any escaped tick ample time to acquire behind our back.
    await sleep(500);

    expect(node.isLeader).toBe(false);
    const { rows } = await query<{ n: number }>(
      `SELECT count(*)::int AS n FROM pg_locks
       WHERE locktype = 'advisory' AND objid = $1 AND granted`,
      [LOCK_KEY],
    );
    expect(rows[0]?.n).toBe(0);

    // And the key is genuinely free: a fresh candidate can take it.
    const successor = makeLeader(50);
    await successor.start();
    await waitFor(async () => successor.isLeader, 5000, 10);
    expect(successor.isLeader).toBe(true);
  });

  it('never allows two leaders during a handover', async () => {
    const nodes = [makeLeader(100), makeLeader(100), makeLeader(100)];
    await Promise.all(nodes.map((n) => n.start()));
    await waitFor(async () => nodes.some((n) => n.isLeader), 20_000, 25);

    let maxConcurrentLeaders = 0;
    const sampler = setInterval(() => {
      maxConcurrentLeaders = Math.max(
        maxConcurrentLeaders,
        nodes.filter((n) => n.isLeader).length,
      );
    }, 5);

    // Two handovers is the most three nodes can do; a third stop would
    // leave nobody to take over.
    for (let round = 0; round < 2; round += 1) {
      const current = nodes.find((n) => n.isLeader);
      if (current === undefined) break;
      await current.stop();
      // Generous: the full suite leaves other processes competing for CPU,
      // and this asserts the *invariant*, not the speed (that is the test above).
      await waitFor(async () => nodes.some((n) => n.isLeader && n !== current), 20_000, 10);
    }
    clearInterval(sampler);

    expect(maxConcurrentLeaders).toBe(1);
  });
});
