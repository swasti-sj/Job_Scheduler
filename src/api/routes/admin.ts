import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { queueStats, summariseQueues, throughput, workerStats } from '../../core/stats.js';
import { listBreakers, setBreaker, windowStats } from '../../core/breaker.js';
import { upsertCron } from '../../core/cronRunner.js';
import { metricsContentType, renderMetrics } from '../../metrics.js';
import { config } from '../../config.js';
import type { AdvisoryLockLeader } from '../../core/leader.js';

export function registerAdminRoutes(app: FastifyInstance, leader: AdvisoryLockLeader | null): void {
  app.get('/queues', async (_request, reply) => {
    const rows = await queueStats();
    return reply.send({
      queues: summariseQueues(rows),
      by_tenant: rows,
      throughput: await throughput(10),
    });
  });

  app.get('/workers', async (_request, reply) => {
    return reply.send({ workers: await workerStats() });
  });

  app.get('/breakers', async (_request, reply) => {
    const breakers = await listBreakers();
    const withWindows = await Promise.all(
      breakers.map(async (b) => ({ ...b, window: await windowStats(b.job_type) })),
    );
    return reply.send({ breakers: withWindows });
  });

  app.post('/breakers/:jobType', async (request, reply) => {
    const params = z.object({ jobType: z.string().min(1) }).safeParse(request.params);
    const body = z.object({ state: z.enum(['OPEN', 'CLOSED', 'HALF_OPEN']) }).safeParse(request.body);
    if (!params.success || !body.success) {
      return reply.code(400).send({ error: 'invalid_request' });
    }
    await setBreaker(params.data.jobType, body.data.state);
    return reply.send({ job_type: params.data.jobType, state: body.data.state });
  });

  app.post('/crons', async (request, reply) => {
    const body = z
      .object({
        name: z.string().min(1).max(200),
        schedule: z.string().min(1).max(200),
        tenant_id: z.string().min(1),
        queue_name: z.string().min(1).default('default'),
        job_type: z.string().min(1),
        payload: z.record(z.unknown()).default({}),
        priority: z.number().int().min(0).max(9).default(5),
      })
      .safeParse(request.body);
    if (!body.success) {
      return reply.code(400).send({ error: 'invalid_request', details: body.error.flatten() });
    }
    try {
      await upsertCron(body.data);
      return reply.code(201).send({ ok: true });
    } catch (err) {
      return reply.code(400).send({ error: 'invalid_schedule', message: (err as Error).message });
    }
  });

  /** Prometheus scrape target. Also the only place event loop lag is sampled. */
  app.get('/metrics', async (_request, reply) => {
    return reply.header('content-type', metricsContentType).send(await renderMetrics());
  });

  app.get('/health', async (_request, reply) => {
    return reply.send({ ok: true, node_id: config.nodeId, is_leader: leader?.isLeader ?? false });
  });
}
