import { spawn, type ChildProcess } from 'node:child_process';

export interface ManagedProcess {
  name: string;
  kind: 'scheduler' | 'worker';
  port: number | undefined;
  child: ChildProcess;
  startedAt: number;
  restarts: number;
}

export interface SpawnOptions {
  verbose: boolean;
  env: Record<string, string>;
}

/**
 * Process supervisor for the chaos harness.
 *
 * Runs the compiled dist/ entrypoints as separate OS processes so they can be
 * SIGKILLed for real. A killed process is respawned under the same logical name,
 * mirroring what an orchestrator would do, so the run keeps making progress
 * while being repeatedly savaged.
 */
export class Supervisor {
  private readonly processes = new Map<string, ManagedProcess>();
  private stopping = false;

  constructor(private readonly options: SpawnOptions) {}

  get all(): ManagedProcess[] {
    return [...this.processes.values()];
  }

  byKind(kind: ManagedProcess['kind']): ManagedProcess[] {
    return this.all.filter((p) => p.kind === kind);
  }

  start(name: string, kind: ManagedProcess['kind'], port: number | undefined, env: Record<string, string>): ManagedProcess {
    const script = kind === 'scheduler' ? 'dist/bin/scheduler.js' : 'dist/bin/worker.js';
    const child = spawn(process.execPath, [script], {
      env: { ...process.env, ...this.options.env, ...env, NODE_ID: name },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    if (this.options.verbose) {
      child.stdout?.on('data', (c: Buffer) => process.stdout.write(`[${name}] ${c}`));
    }
    child.stderr?.on('data', (c: Buffer) => process.stderr.write(`[${name}] ${c}`));

    const existing = this.processes.get(name);
    const entry: ManagedProcess = {
      name,
      kind,
      port,
      child,
      startedAt: Date.now(),
      restarts: existing === undefined ? 0 : existing.restarts + 1,
    };
    this.processes.set(name, entry);

    child.on('exit', () => {
      // Respawn unless we are deliberately tearing the cluster down. This is the
      // orchestrator's job in production; here it keeps the chaos loop honest by
      // ensuring capacity comes back rather than draining to zero.
      if (this.stopping) return;
      if (this.processes.get(name) !== entry) return;
      setTimeout(() => {
        if (!this.stopping) this.start(name, kind, port, env);
      }, 250).unref();
    });

    return entry;
  }

  kill(name: string): boolean {
    const entry = this.processes.get(name);
    if (entry === undefined) return false;
    entry.child.kill('SIGKILL');
    return true;
  }

  async stopAll(): Promise<void> {
    this.stopping = true;
    for (const entry of this.processes.values()) {
      entry.child.kill('SIGKILL');
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
    this.processes.clear();
  }
}
