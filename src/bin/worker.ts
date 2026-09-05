/**
 * Worker process entrypoint.
 *
 * Multi-core is achieved by running more of these (container replicas, or
 * `cluster` forks), not worker_threads: jobs are I/O bound and each worker keeps
 * its own socket and its own claim budget, so separate processes give real
 * parallelism plus fault isolation - one worker segfaulting takes its own jobs
 * down and nothing else, and its socket closing triggers immediate reclaim.
 */
import cluster from 'node:cluster';
import { availableParallelism } from 'node:os';
import { Worker } from '../worker/worker.js';
import { installProcessHandlers } from '../process.js';
import { logger } from '../logger.js';
import { closeRedis } from '../redis/client.js';

const forks = Number.parseInt(process.env['WORKER_PROCESSES'] ?? '1', 10);

if (forks > 1 && cluster.isPrimary) {
  const count = Math.min(forks, availableParallelism());
  logger.info({ count }, 'forking worker processes');
  for (let i = 0; i < count; i += 1) cluster.fork();
  cluster.on('exit', (worker, code, signal) => {
    logger.warn({ pid: worker.process.pid, code, signal }, 'worker process exited; replacing');
    if (!worker.exitedAfterDisconnect) cluster.fork();
  });
} else {
  const worker = new Worker();
  worker.start();

  installProcessHandlers({
    steps: [
      { name: 'drain in-flight jobs', run: () => worker.stop() },
      { name: 'close redis', run: () => closeRedis() },
    ],
    graceMs: 25_000,
  });

  logger.info({ workerId: worker.id }, 'worker ready');
}
