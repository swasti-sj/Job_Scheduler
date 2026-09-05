/**
 * DAG dependency handling.
 *
 * `depends_on` edges point from a dependent to its dependencies. A cycle makes a
 * job set permanently unrunnable (every member waits on another member), so it
 * is rejected at submission time rather than discovered as a stuck queue later.
 *
 * The search is an *iterative* DFS with an explicit stack. A recursive DFS blows
 * the V8 stack somewhere around 10k frames; a fan-in chain of 50k jobs is a
 * perfectly reasonable pipeline and must not crash the API process. The
 * three-colour scheme (white = unvisited, grey = on the current path, black =
 * fully explored) detects a back edge in O(V + E) and the grey stack *is* the
 * cycle path, so we can name it in the error.
 */

export type DepGraph = ReadonlyMap<string, readonly string[]>;

export class CycleError extends Error {
  constructor(readonly cycle: readonly string[]) {
    super(`dependency cycle detected: ${cycle.join(' -> ')}`);
    this.name = 'CycleError';
  }
}

const enum Colour {
  White = 0,
  Grey = 1,
  Black = 2,
}

interface Frame {
  node: string;
  edgeIndex: number;
}

/**
 * Returns the cycle as a path `[a, b, c, a]` if one exists, otherwise null.
 * Nodes not present as keys in `graph` are treated as leaves (already-persisted
 * jobs whose own dependencies are resolved, or unknown ids).
 */
export function findCycle(graph: DepGraph): string[] | null {
  const colour = new Map<string, Colour>();
  for (const node of graph.keys()) colour.set(node, Colour.White);

  for (const root of graph.keys()) {
    if (colour.get(root) !== Colour.White) continue;

    // Explicit stack; `path` mirrors the grey nodes in visit order.
    const stack: Frame[] = [{ node: root, edgeIndex: 0 }];
    const path: string[] = [root];
    colour.set(root, Colour.Grey);

    while (stack.length > 0) {
      const frame = stack[stack.length - 1];
      if (frame === undefined) break;
      const edges = graph.get(frame.node) ?? [];

      if (frame.edgeIndex >= edges.length) {
        colour.set(frame.node, Colour.Black);
        stack.pop();
        path.pop();
        continue;
      }

      const next = edges[frame.edgeIndex];
      frame.edgeIndex += 1;
      if (next === undefined) continue;

      // A node with no outgoing edges in this graph is a leaf; nothing to visit.
      if (!graph.has(next)) continue;

      const c = colour.get(next) ?? Colour.White;
      if (c === Colour.Grey) {
        // Back edge: `next` is on the current path. Slice the path from `next`
        // and close the loop so the error names the exact cycle.
        const start = path.indexOf(next);
        return [...path.slice(start === -1 ? 0 : start), next];
      }
      if (c === Colour.Black) continue;

      colour.set(next, Colour.Grey);
      path.push(next);
      stack.push({ node: next, edgeIndex: 0 });
    }
  }

  return null;
}

export function assertAcyclic(graph: DepGraph): void {
  const cycle = findCycle(graph);
  if (cycle !== null) throw new CycleError(cycle);
}

/**
 * Topological order (dependencies before dependents). Kahn's algorithm; throws
 * CycleError if the graph is cyclic. Used to insert a submitted batch in an
 * order where a dependency row always exists before the row referencing it.
 */
export function topoSort(graph: DepGraph): string[] {
  const indegree = new Map<string, number>();
  const dependents = new Map<string, string[]>();

  for (const node of graph.keys()) {
    indegree.set(node, 0);
    dependents.set(node, []);
  }
  for (const [node, deps] of graph) {
    for (const dep of deps) {
      if (!graph.has(dep)) continue; // external, already satisfied
      indegree.set(node, (indegree.get(node) ?? 0) + 1);
      dependents.get(dep)?.push(node);
    }
  }

  const queue: string[] = [];
  for (const [node, deg] of indegree) if (deg === 0) queue.push(node);

  const order: string[] = [];
  for (let head = 0; head < queue.length; head += 1) {
    const node = queue[head];
    if (node === undefined) continue;
    order.push(node);
    for (const child of dependents.get(node) ?? []) {
      const deg = (indegree.get(child) ?? 0) - 1;
      indegree.set(child, deg);
      if (deg === 0) queue.push(child);
    }
  }

  if (order.length !== graph.size) {
    const cycle = findCycle(graph);
    throw new CycleError(cycle ?? [...graph.keys()].filter((n) => !order.includes(n)));
  }
  return order;
}
