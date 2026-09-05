import type { Redis } from 'ioredis';
import { redis } from './client.js';
import { rateLimitRejections } from '../metrics.js';

/**
 * Per-queue token bucket, as a Redis Lua script.
 *
 * Why Lua: read-modify-write on the bucket must be atomic across every scheduler
 * node. Doing it with GET/SET or even WATCH/MULTI means either a lost update
 * (two nodes both read 10 tokens and both spend them) or a retry loop whose cost
 * grows with contention - and the bucket is touched on every claim, i.e. the
 * hottest path in the system. Redis runs a script to completion with nothing
 * interleaved, so the whole refill-and-spend is one indivisible step.
 *
 * The bucket is lazily refilled: instead of a timer topping every bucket up, the
 * elapsed time since the last touch is converted to tokens on read. Idle queues
 * therefore cost nothing at all.
 */
const TOKEN_BUCKET_LUA = `
local key        = KEYS[1]
local capacity   = tonumber(ARGV[1])
local refill     = tonumber(ARGV[2])   -- tokens per second
local now_ms     = tonumber(ARGV[3])
local requested  = tonumber(ARGV[4])
local ttl_ms     = tonumber(ARGV[5])

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])

if tokens == nil or ts == nil then
  tokens = capacity
  ts = now_ms
end

-- Lazy refill for the elapsed window, clamped at capacity.
local elapsed = math.max(0, now_ms - ts)
tokens = math.min(capacity, tokens + (elapsed / 1000.0) * refill)

local granted = math.floor(math.min(tokens, requested))
if granted < 0 then granted = 0 end
tokens = tokens - granted

redis.call('HSET', key, 'tokens', tokens, 'ts', now_ms)
redis.call('PEXPIRE', key, ttl_ms)

return {granted, math.floor(tokens)}
`;

export interface TokenBucketOptions {
  capacity: number;
  refillPerSecond: number;
}

export interface TokenGrant {
  granted: number;
  remaining: number;
}

export class TokenBucketLimiter {
  private readonly client: Redis;
  private sha: string | null = null;

  constructor(client: Redis = redis) {
    this.client = client;
  }

  private async scriptSha(): Promise<string> {
    if (this.sha === null) {
      this.sha = await this.client.script('LOAD', TOKEN_BUCKET_LUA) as string;
    }
    return this.sha;
  }

  /**
   * Requests up to `requested` slots for `queue`. Returns however many the bucket
   * could pay for, which may be 0. Callers size their claim batch by the grant
   * rather than claiming first and throwing work away.
   */
  async take(queue: string, requested: number, opts: TokenBucketOptions): Promise<TokenGrant> {
    if (requested <= 0) return { granted: 0, remaining: 0 };
    const key = `ratelimit:queue:${queue}`;
    const args = [
      String(opts.capacity),
      String(opts.refillPerSecond),
      String(Date.now()),
      String(requested),
      // Two full refills of idle time is plenty; the bucket rebuilds from empty.
      String(Math.max(60_000, Math.ceil((opts.capacity / Math.max(opts.refillPerSecond, 0.001)) * 2000))),
    ];

    let raw: unknown;
    try {
      raw = await this.client.evalsha(await this.scriptSha(), 1, key, ...args);
    } catch (err) {
      // NOSCRIPT: the server was flushed or restarted. Reload and retry once.
      if (err instanceof Error && err.message.includes('NOSCRIPT')) {
        this.sha = null;
        raw = await this.client.eval(TOKEN_BUCKET_LUA, 1, key, ...args);
      } else {
        throw err;
      }
    }

    const [granted, remaining] = raw as [number, number];
    if (granted < requested) rateLimitRejections.inc({ queue }, requested - granted);
    return { granted, remaining };
  }

  /** Test hook: reset a queue's bucket to full. */
  async reset(queue: string): Promise<void> {
    await this.client.del(`ratelimit:queue:${queue}`);
  }
}

export const rateLimiter = new TokenBucketLimiter();
