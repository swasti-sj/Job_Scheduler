import { logger } from './logger.js';
import { startEventLoopMonitor } from './metrics.js';

export interface ShutdownOptions {
  /** Run in order; each gets `graceMs` before the next is forced. */
  steps: Array<{ name: string; run: () => Promise<void> | void }>;
  graceMs?: number;
}

let shuttingDown = false;

/**
 * Process-level lifecycle.
 *
 * Two rules, both deliberate:
 *
 * 1. SIGTERM drains. Stop claiming first, then let in-flight jobs finish, then
 *    release the advisory lock, then close sockets, then exit. Releasing the
 *    lock explicitly hands leadership to a follower in one poll interval instead
 *    of making it wait for the connection to be noticed as dead.
 *
 * 2. uncaughtException and unhandledRejection log and exit non-zero. They are
 *    never swallowed. After an unexpected throw the process may be holding a
 *    half-applied transaction, a claimed job it will never run, or a corrupted
 *    in-memory view of which worker owns what - and continuing from there risks
 *    exactly the double-execution the whole system exists to prevent. Dying is
 *    safe because every in-flight job is protected by a lease: the reaper
 *    returns the work within one lease period, and the orchestrator restarts us.
 *    This is the same path the chaos harness exercises with SIGKILL.
 */
export function installProcessHandlers(options: ShutdownOptions): void {
  startEventLoopMonitor();

  const graceMs = options.graceMs ?? 20_000;

  const shutdown = (signal: string): void => {
    if (shuttingDown) {
      logger.warn({ signal }, 'second shutdown signal; exiting immediately');
      process.exit(1);
    }
    shuttingDown = true;
    logger.info({ signal }, 'graceful shutdown started');

    // Hard deadline: if a step wedges (a job that never returns, a socket that
    // never closes) we still exit rather than hanging until the orchestrator
    // SIGKILLs us at an arbitrary point.
    const hardStop = setTimeout(() => {
      logger.error('graceful shutdown timed out; forcing exit');
      process.exit(1);
    }, graceMs);
    hardStop.unref();

    void (async () => {
      for (const step of options.steps) {
        try {
          await step.run();
          logger.info({ step: step.name }, 'shutdown step complete');
        } catch (err) {
          logger.error({ err, step: step.name }, 'shutdown step failed');
        }
      }
      clearTimeout(hardStop);
      logger.info('graceful shutdown complete');
      process.exit(0);
    })();
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  process.on('unhandledRejection', (reason) => {
    logger.fatal({ err: reason }, 'unhandled promise rejection; exiting');
    process.exit(1);
  });

  process.on('uncaughtException', (err) => {
    logger.fatal({ err }, 'uncaught exception; exiting');
    process.exit(1);
  });
}
