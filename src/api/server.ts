import Fastify, { type FastifyInstance } from 'fastify';
import { WebSocketServer } from 'ws';
import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { registerJobRoutes } from './routes/jobs.js';
import { registerAdminRoutes } from './routes/admin.js';
import { WorkerHub } from './ws/workerPlane.js';
import { DashboardHub } from './ws/dashboard.js';
import type { AdvisoryLockLeader } from '../core/leader.js';
import { DASHBOARD_HTML } from './dashboardPage.js';

export interface ServerHandles {
  app: FastifyInstance;
  workerHub: WorkerHub;
  dashboardHub: DashboardHub;
  close(): Promise<void>;
}

export interface ServerOptions {
  /** Present on scheduler nodes; null on pure API nodes. */
  leader?: AdvisoryLockLeader | null;
  /** API-only nodes serve HTTP but do not accept worker sockets. */
  enableWorkerPlane?: boolean;
}

/**
 * Builds the HTTP + WebSocket server.
 *
 * Both WebSocket endpoints hang off one HTTP server via `noServer: true` and a
 * manual upgrade handler, rather than @fastify/websocket, so the two channels
 * stay independent: /worker and /dashboard have entirely different lifetimes,
 * backpressure policies and failure semantics, and routing the upgrade by path
 * here makes that separation explicit.
 */
export async function buildServer(options: ServerOptions = {}): Promise<ServerHandles> {
  const app = Fastify({
    logger: false,
    // Trust the proxy in front of us for client addresses in logs.
    trustProxy: true,
    bodyLimit: 1_048_576,
  });

  const workerHub = new WorkerHub();
  const dashboardHub = new DashboardHub();

  app.setErrorHandler((err, request, reply) => {
    logger.error({ err, url: request.url }, 'unhandled request error');
    void reply.code(500).send({ error: 'internal_error' });
  });

  registerJobRoutes(app);
  registerAdminRoutes(app, options.leader ?? null);

  app.get('/', async (_request, reply) => {
    return reply.header('content-type', 'text/html; charset=utf-8').send(DASHBOARD_HTML);
  });

  await app.ready();

  const workerWss = new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 * 1024 });
  const dashboardWss = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 });

  const enableWorkerPlane = options.enableWorkerPlane ?? true;

  app.server.on('upgrade', (request: IncomingMessage, socket: Duplex, head: Buffer) => {
    const path = (request.url ?? '').split('?')[0];

    if (path === '/worker' && enableWorkerPlane) {
      workerWss.handleUpgrade(request, socket, head, (ws) => workerHub.handleConnection(ws));
      return;
    }
    if (path === '/dashboard') {
      dashboardWss.handleUpgrade(request, socket, head, (ws) => dashboardHub.handleConnection(ws));
      return;
    }
    socket.destroy();
  });

  if (enableWorkerPlane) await workerHub.start();
  await dashboardHub.start();

  await app.listen({ host: config.httpHost, port: config.httpPort });
  logger.info(
    { port: config.httpPort, workerPlane: enableWorkerPlane },
    'http + websocket server listening',
  );

  return {
    app,
    workerHub,
    dashboardHub,
    async close(): Promise<void> {
      await workerHub.stop();
      dashboardHub.stop();
      workerWss.close();
      dashboardWss.close();
      await app.close();
    },
  };
}
