import type { RawData } from 'ws';

/**
 * Normalises a `ws` frame to a string.
 *
 * `RawData` is `Buffer | ArrayBuffer | Buffer[]`, and calling `.toString()` on it
 * blindly is a latent bug: for the fragmented `Buffer[]` case it produces
 * comma-joined garbage rather than the concatenated payload, which shows up as
 * an unparseable message only under fragmentation - exactly when a large payload
 * is being sent, and exactly the case that is hardest to reproduce.
 */
export function frameToString(data: RawData): string {
  if (typeof data === 'string') return data;
  if (Array.isArray(data)) return Buffer.concat(data).toString('utf8');
  if (Buffer.isBuffer(data)) return data.toString('utf8');
  return Buffer.from(data).toString('utf8');
}
