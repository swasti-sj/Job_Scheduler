import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import {
  cancelJob,
  ConflictError,
  getJob,
  getJobEvents,
  NotFoundError,
  requeueJob,
  submitJob,
  submitJobs,
  ValidationError,
  type SubmitRequest,
} from '../../core/jobStore.js';
import { CycleError } from '../../domain/dag.js';
import { listDeadLetters } from '../../core/stats.js';
import type { Priority } from '../../domain/job.js';

const jobInput = z.object({
  ref: z.string().min(1).max(200).optional(),
  id: z.string().uuid().optional(),
  tenant_id: z.string().min(1).max(200),
  queue_name: z.string().min(1).max(200).default('default'),
  job_type: z.string().min(1).max(200),
  payload: z.record(z.unknown()).default({}),
  priority: z.number().int().min(0).max(9).default(5),
  max_attempts: z.number().int().min(1).max(100).optional(),
  scheduled_for: z.string().datetime().optional(),
  idempotency_key: z.string().min(1).max(500).optional(),
  depends_on: z.array(z.string().min(1)).max(1000).default([]),
});

const submitBody = z.union([jobInput, z.object({ jobs: z.array(jobInput).min(1).max(1000) })]);

function toSubmitRequest(input: z.infer<typeof jobInput>): SubmitRequest {
  // exactOptionalPropertyTypes means an absent field must be absent, not
  // explicitly undefined, so the optional keys are spread in conditionally.
  return {
    tenant_id: input.tenant_id,
    queue_name: input.queue_name,
    job_type: input.job_type,
    payload: input.payload,
    priority: input.priority as Priority,
    depends_on: input.depends_on,
    ...(input.ref !== undefined ? { ref: input.ref } : {}),
    ...(input.id !== undefined ? { id: input.id } : {}),
    ...(input.max_attempts !== undefined ? { max_attempts: input.max_attempts } : {}),
    ...(input.scheduled_for !== undefined ? { scheduled_for: input.scheduled_for } : {}),
    ...(input.idempotency_key !== undefined ? { idempotency_key: input.idempotency_key } : {}),
  };
}

export function registerJobRoutes(app: FastifyInstance): void {
  /**
   * POST /jobs - submit one job, or a batch that may form a DAG among itself.
   *
   * Returns 200 (not 201) when an idempotency key matched an existing job, so a
   * retrying client can tell "I created this" from "this already existed"
   * without a second lookup.
   */
  app.post('/jobs', async (request, reply) => {
    const parsed = submitBody.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({ error: 'invalid_request', details: parsed.error.flatten() });
    }

    try {
      if ('jobs' in parsed.data) {
        const result = await submitJobs(parsed.data.jobs.map(toSubmitRequest));
        return reply.code(result.deduplicated.length === parsed.data.jobs.length ? 200 : 201).send({
          jobs: result.jobs,
          deduplicated: result.deduplicated,
        });
      }

      const { job, deduplicated } = await submitJob(toSubmitRequest(parsed.data));
      return reply.code(deduplicated ? 200 : 201).send({ job, deduplicated });
    } catch (err) {
      return replyForError(reply, err);
    }
  });

  app.get('/jobs/:id', async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).safeParse(request.params);
    if (!params.success) return reply.code(400).send({ error: 'invalid_job_id' });
    const withEvents = z
      .object({ events: z.enum(['true', 'false']).optional() })
      .safeParse(request.query);

    try {
      const job = await getJob(params.data.id);
      if (withEvents.success && withEvents.data.events === 'true') {
        return reply.send({ job, events: await getJobEvents(params.data.id) });
      }
      return reply.send({ job });
    } catch (err) {
      return replyForError(reply, err);
    }
  });

  app.delete('/jobs/:id', async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).safeParse(request.params);
    if (!params.success) return reply.code(400).send({ error: 'invalid_job_id' });
    try {
      return reply.send({ job: await cancelJob(params.data.id) });
    } catch (err) {
      return replyForError(reply, err);
    }
  });

  app.post('/jobs/:id/requeue', async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).safeParse(request.params);
    if (!params.success) return reply.code(400).send({ error: 'invalid_job_id' });
    const body = z
      .object({ reset_attempts: z.boolean().default(true) })
      .safeParse(request.body ?? {});
    try {
      const job = await requeueJob(params.data.id, body.success ? body.data.reset_attempts : true);
      return reply.send({ job });
    } catch (err) {
      return replyForError(reply, err);
    }
  });

  app.get('/dead-letters', async (request, reply) => {
    const q = z
      .object({
        limit: z.coerce.number().int().min(1).max(500).default(100),
        offset: z.coerce.number().int().min(0).default(0),
      })
      .safeParse(request.query);
    const { limit, offset } = q.success ? q.data : { limit: 100, offset: 0 };
    return reply.send({ jobs: await listDeadLetters(limit, offset) });
  });
}

interface ErrorReply {
  code(status: number): ErrorReply;
  send(body: unknown): unknown;
}

function replyForError(reply: ErrorReply, err: unknown): unknown {
  if (err instanceof CycleError) {
    return reply.code(400).send({
      error: 'dependency_cycle',
      message: err.message,
      cycle: err.cycle,
    });
  }
  if (err instanceof ValidationError) {
    return reply.code(400).send({ error: 'invalid_request', message: err.message, ...err.details });
  }
  if (err instanceof NotFoundError) {
    return reply.code(404).send({ error: 'not_found', message: err.message });
  }
  if (err instanceof ConflictError) {
    return reply.code(409).send({ error: 'conflict', message: err.message });
  }
  throw err;
}
