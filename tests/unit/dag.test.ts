import { describe, expect, it } from 'vitest';
import { assertAcyclic, CycleError, findCycle, topoSort } from '../../src/domain/dag.js';

function graph(edges: Record<string, string[]>): Map<string, string[]> {
  return new Map(Object.entries(edges));
}

describe('findCycle', () => {
  it('accepts an empty graph', () => {
    expect(findCycle(graph({}))).toBeNull();
  });

  it('accepts a simple chain', () => {
    expect(findCycle(graph({ c: ['b'], b: ['a'], a: [] }))).toBeNull();
  });

  it('accepts fan-out: one job unblocking many', () => {
    expect(findCycle(graph({ a: [], b: ['a'], c: ['a'], d: ['a'] }))).toBeNull();
  });

  it('accepts fan-in: many jobs unblocking one', () => {
    expect(findCycle(graph({ a: [], b: [], c: [], d: ['a', 'b', 'c'] }))).toBeNull();
  });

  it('accepts a diamond (a node reachable by two paths is not a cycle)', () => {
    // The classic false positive for a naive "already visited" check.
    expect(findCycle(graph({ a: [], b: ['a'], c: ['a'], d: ['b', 'c'] }))).toBeNull();
  });

  it('detects a self-loop', () => {
    expect(findCycle(graph({ a: ['a'] }))).toEqual(['a', 'a']);
  });

  it('detects a two-node cycle', () => {
    const cycle = findCycle(graph({ a: ['b'], b: ['a'] }));
    expect(cycle).not.toBeNull();
    expect(cycle?.[0]).toBe(cycle?.[cycle.length - 1]);
    expect(new Set(cycle)).toEqual(new Set(['a', 'b']));
  });

  it('names the exact path of a longer cycle', () => {
    const cycle = findCycle(graph({ a: ['b'], b: ['c'], c: ['d'], d: ['b'] }));
    expect(cycle).not.toBeNull();
    // The reported path closes on itself and contains only the looping nodes -
    // 'a' leads into the cycle but is not part of it.
    expect(cycle?.[0]).toBe(cycle?.[cycle.length - 1]);
    expect(cycle).toContain('b');
    expect(cycle).toContain('c');
    expect(cycle).toContain('d');
  });

  it('finds a cycle that is not reachable from the first root visited', () => {
    const cycle = findCycle(graph({ a: [], b: ['c'], c: ['b'] }));
    expect(cycle).not.toBeNull();
  });

  it('treats ids that are not nodes as external leaves, not cycles', () => {
    // Depending on an already-persisted job is normal and must not trip this.
    expect(findCycle(graph({ a: ['persisted-elsewhere'] }))).toBeNull();
  });

  it('handles a 50k-node chain without blowing the stack', () => {
    // The reason the search is an explicit-stack DFS: a recursive one dies with
    // RangeError somewhere around 10k frames, and a 50k-job fan-in pipeline is a
    // perfectly ordinary thing for a client to submit.
    const edges: Record<string, string[]> = {};
    for (let i = 0; i < 50_000; i += 1) edges[`n${i}`] = i === 0 ? [] : [`n${i - 1}`];
    expect(() => findCycle(graph(edges))).not.toThrow();
    expect(findCycle(graph(edges))).toBeNull();
  });

  it('finds a cycle closed at the far end of a very deep chain', () => {
    const edges: Record<string, string[]> = {};
    for (let i = 0; i < 20_000; i += 1) edges[`n${i}`] = i === 0 ? [] : [`n${i - 1}`];
    edges['n0'] = ['n19999'];
    const cycle = findCycle(graph(edges));
    expect(cycle).not.toBeNull();
    expect(cycle?.[0]).toBe(cycle?.[cycle.length - 1]);
  });

  it('is linear rather than exponential on a dense DAG', () => {
    // Layered graph where each node points at every node in the previous layer:
    // a memoryless DFS would re-explore subtrees exponentially.
    const edges: Record<string, string[]> = {};
    const width = 30;
    const depth = 30;
    for (let layer = 0; layer < depth; layer += 1) {
      for (let n = 0; n < width; n += 1) {
        edges[`l${layer}n${n}`] =
          layer === 0
            ? []
            : Array.from({ length: width }, (_, prev) => `l${layer - 1}n${prev}`);
      }
    }
    const started = Date.now();
    expect(findCycle(graph(edges))).toBeNull();
    expect(Date.now() - started).toBeLessThan(2000);
  });
});

describe('assertAcyclic', () => {
  it('throws a CycleError naming the path', () => {
    expect(() => assertAcyclic(graph({ x: ['y'], y: ['x'] }))).toThrow(CycleError);
    try {
      assertAcyclic(graph({ x: ['y'], y: ['x'] }));
    } catch (err) {
      expect((err as CycleError).message).toMatch(/dependency cycle detected: .* -> .*/);
      expect((err as CycleError).cycle.length).toBeGreaterThan(1);
    }
  });
});

describe('topoSort', () => {
  it('orders dependencies before dependents', () => {
    const order = topoSort(graph({ c: ['b'], b: ['a'], a: [] }));
    expect(order.indexOf('a')).toBeLessThan(order.indexOf('b'));
    expect(order.indexOf('b')).toBeLessThan(order.indexOf('c'));
  });

  it('places every dependency of a fan-in node before it', () => {
    const order = topoSort(graph({ sink: ['a', 'b', 'c'], a: [], b: [], c: [] }));
    for (const dep of ['a', 'b', 'c']) {
      expect(order.indexOf(dep)).toBeLessThan(order.indexOf('sink'));
    }
  });

  it('ignores external dependencies when ordering', () => {
    const order = topoSort(graph({ a: ['already-persisted'] }));
    expect(order).toEqual(['a']);
  });

  it('throws on a cyclic graph', () => {
    expect(() => topoSort(graph({ a: ['b'], b: ['a'] }))).toThrow(CycleError);
  });
});
