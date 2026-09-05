import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { claimJobs } from '../../src/core/claim.js';
import { completeJob, failJob } from '../../src/core/lifecycle.js';
import { cancelJob, submitJob, submitJobs, ValidationError } from '../../src/core/jobStore.js';
import { CycleError } from '../../src/domain/dag.js';
import { query } from '../../src/db/pool.js';
import { getJobRow, registerWorker, resetDatabase, setupDatabase, teardown } from './helpers.js';

beforeAll(setupDatabase);
afterAll(teardown);
beforeEach(resetDatabase);

const base = {
  tenant_id: 'acme',
  queue_name: 'default',
  job_type: 'noop',
  payload: {},
  priority: 5 as const,
  depends_on: [],
};

/** Runs a job end to end through the real claim/complete path. */
async function runToSuccess(jobId: string, workerId = 'w1'): Promise<void> {
  await registerWorker(workerId);
  await query(`UPDATE jobs SET scheduled_for = now() WHERE id = $1`, [jobId]);

  // A batched claim may already have picked this job up on an earlier call, so
  // only claim if it is still pending.
  const current = await getJobRow(jobId);
  if (current.state === 'PENDING') {
    await claimJobs({ workerId, queue: 'default', limit: 50 });
  }
  const after = await getJobRow(jobId);
  if (after.claimed_by !== workerId) {
    throw new Error(`job ${jobId} is ${after.state}, not claimable by ${workerId}`);
  }
  await completeJob(jobId, workerId, null);
}

describe('dependencies', () => {
  it('starts a job with dependencies in BLOCKED and does not dispatch it', async () => {
    const { job: parent } = await submitJob({ ...base });
    const { job: child } = await submitJob({ ...base, depends_on: [parent.id] });

    expect(child.state).toBe('BLOCKED');
    await registerWorker('w1');
    const claimed = await claimJobs({ workerId: 'w1', queue: 'default', limit: 10 });
    expect(claimed.map((c) => c.id)).toEqual([parent.id]);
  });

  it('unblocks the dependent when its dependency succeeds', async () => {
    const { job: parent } = await submitJob({ ...base });
    const { job: child } = await submitJob({ ...base, depends_on: [parent.id] });

    await runToSuccess(parent.id);
    expect((await getJobRow(child.id)).state).toBe('PENDING');
  });

  it('fan-out: one completion releases many dependents', async () => {
    const { job: parent } = await submitJob({ ...base });
    const children = await Promise.all(
      [0, 1, 2, 3, 4].map(() => submitJob({ ...base, depends_on: [parent.id] })),
    );
    for (const { job } of children) expect(job.state).toBe('BLOCKED');

    await runToSuccess(parent.id);
    for (const { job } of children) {
      expect((await getJobRow(job.id)).state).toBe('PENDING');
    }
  });

  it('fan-in: the dependent waits for its last dependency', async () => {
    const parents = await Promise.all([0, 1, 2].map(() => submitJob({ ...base })));
    const { job: sink } = await submitJob({
      ...base,
      depends_on: parents.map((p) => p.job.id),
    });

    await runToSuccess(parents[0]?.job.id as string);
    expect((await getJobRow(sink.id)).state).toBe('BLOCKED');
    await runToSuccess(parents[1]?.job.id as string);
    expect((await getJobRow(sink.id)).state).toBe('BLOCKED');
    await runToSuccess(parents[2]?.job.id as string);
    expect((await getJobRow(sink.id)).state).toBe('PENDING');
  });

  it('runs a whole batch DAG submitted in one request', async () => {
    const result = await submitJobs([
      { ...base, ref: 'extract' },
      { ...base, ref: 'transform', depends_on: ['extract'] },
      { ...base, ref: 'load', depends_on: ['transform'] },
    ]);
    expect(result.jobs).toHaveLength(3);

    const byRef = new Map(result.jobs.map((j) => [j.id, j]));
    const states = [...byRef.values()].map((j) => j.state).sort();
    expect(states).toEqual(['BLOCKED', 'BLOCKED', 'PENDING']);
  });

  it('starts a job PENDING when its dependencies already succeeded', async () => {
    const { job: parent } = await submitJob({ ...base });
    await runToSuccess(parent.id);

    const { job: late } = await submitJob({ ...base, depends_on: [parent.id] });
    expect(late.state).toBe('BLOCKED');
    // resolveNewlyBlocked settles it inside the same transaction.
    expect((await getJobRow(late.id)).state).toBe('PENDING');
  });
});

describe('cancellation cascade', () => {
  it('cancels dependents when a dependency is cancelled', async () => {
    const { job: parent } = await submitJob({ ...base });
    const { job: child } = await submitJob({ ...base, depends_on: [parent.id] });
    const { job: grandchild } = await submitJob({ ...base, depends_on: [child.id] });

    await cancelJob(parent.id);

    expect((await getJobRow(parent.id)).state).toBe('CANCELLED');
    expect((await getJobRow(child.id)).state).toBe('CANCELLED');
    // Transitive: the cascade walks the whole reverse-dependency tree.
    expect((await getJobRow(grandchild.id)).state).toBe('CANCELLED');
  });

  it('cancels dependents when a dependency lands in the dead letter queue', async () => {
    const { job: parent } = await submitJob({ ...base, max_attempts: 1 });
    const { job: child } = await submitJob({ ...base, depends_on: [parent.id] });

    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 5 });
    await failJob(parent.id, 'w1', 'permanent failure');

    expect((await getJobRow(parent.id)).state).toBe('DEAD');
    expect((await getJobRow(child.id)).state).toBe('CANCELLED');
  });

  it('cancels a newly submitted job whose dependency is already dead', async () => {
    const { job: parent } = await submitJob({ ...base, max_attempts: 1 });
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 5 });
    await failJob(parent.id, 'w1', 'permanent failure');

    const { job: child } = await submitJob({ ...base, depends_on: [parent.id] });
    expect((await getJobRow(child.id)).state).toBe('CANCELLED');
  });

  it('refuses to cancel a job that is already running', async () => {
    const { job } = await submitJob({ ...base });
    await registerWorker('w1');
    await claimJobs({ workerId: 'w1', queue: 'default', limit: 1 });
    await expect(cancelJob(job.id)).rejects.toThrow(/CLAIMED/);
  });
});

describe('cycle detection at submission', () => {
  it('rejects a two-node cycle in a batch and names the path', async () => {
    await expect(
      submitJobs([
        { ...base, ref: 'a', depends_on: ['b'] },
        { ...base, ref: 'b', depends_on: ['a'] },
      ]),
    ).rejects.toThrow(CycleError);

    // Nothing was written: the whole submit is one transaction.
    const { rows } = await query<{ n: number }>('SELECT count(*)::int AS n FROM jobs');
    expect(rows[0]?.n).toBe(0);
  });

  it('rejects a longer cycle', async () => {
    await expect(
      submitJobs([
        { ...base, ref: 'a', depends_on: ['c'] },
        { ...base, ref: 'b', depends_on: ['a'] },
        { ...base, ref: 'c', depends_on: ['b'] },
      ]),
    ).rejects.toThrow(/dependency cycle detected/);
  });

  it('rejects a self-dependency', async () => {
    const id = '11111111-1111-4111-8111-111111111111';
    await expect(submitJobs([{ ...base, id, depends_on: [id] }])).rejects.toThrow(CycleError);
  });

  it('rejects a cycle that closes through an already-persisted job', async () => {
    // a exists. Submitting b -> a and, in the same batch, redefining nothing,
    // but giving b an explicit id that a already depends on, closes the loop
    // through the persisted half of the graph.
    const aId = '22222222-2222-4222-8222-222222222222';
    const bId = '33333333-3333-4333-8333-333333333333';
    await query(
      `INSERT INTO tenants (tenant_id) VALUES ('acme') ON CONFLICT DO NOTHING`,
    );
    await query(
      `INSERT INTO jobs (id, tenant_id, queue_name, job_type, priority, state, depends_on)
       VALUES ($1, 'acme', 'default', 'noop', 5, 'BLOCKED', ARRAY[$2]::uuid[])`,
      [aId, bId],
    );

    await expect(
      submitJobs([{ ...base, id: bId, depends_on: [aId] }]),
    ).rejects.toThrow(CycleError);
  });

  it('accepts a diamond, which is not a cycle', async () => {
    const result = await submitJobs([
      { ...base, ref: 'root' },
      { ...base, ref: 'left', depends_on: ['root'] },
      { ...base, ref: 'right', depends_on: ['root'] },
      { ...base, ref: 'join', depends_on: ['left', 'right'] },
    ]);
    expect(result.jobs).toHaveLength(4);
  });

  it('rejects a dependency on a job that does not exist', async () => {
    await expect(
      submitJob({ ...base, depends_on: ['44444444-4444-4444-8444-444444444444'] }),
    ).rejects.toThrow(ValidationError);
  });

  it('rejects a depends_on entry that is neither a ref nor a uuid', async () => {
    await expect(submitJob({ ...base, depends_on: ['not-a-uuid'] })).rejects.toThrow(
      ValidationError,
    );
  });
});
