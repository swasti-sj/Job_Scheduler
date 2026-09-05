import { describe, expect, it } from 'vitest';
import {
  canTransition,
  IllegalTransitionError,
  isInFlight,
  isTerminal,
  JOB_TRANSITIONS,
  JobState,
  transition,
  assertTransition,
  isJobState,
} from '../../src/domain/states.js';

describe('transition table', () => {
  it('declares an edge list for every state', () => {
    for (const state of Object.values(JobState)) {
      expect(JOB_TRANSITIONS[state]).toBeDefined();
    }
  });

  it('never points at a state that does not exist', () => {
    const valid = new Set<string>(Object.values(JobState));
    for (const [from, tos] of Object.entries(JOB_TRANSITIONS)) {
      for (const to of tos) {
        expect(valid.has(to), `${from} -> ${to}`).toBe(true);
      }
    }
  });

  it('allows the happy path', () => {
    expect(canTransition(JobState.PENDING, JobState.CLAIMED)).toBe(true);
    expect(canTransition(JobState.CLAIMED, JobState.RUNNING)).toBe(true);
    expect(canTransition(JobState.RUNNING, JobState.SUCCEEDED)).toBe(true);
  });

  it('allows both reclaim paths to return in-flight work to the queue', () => {
    expect(canTransition(JobState.CLAIMED, JobState.PENDING)).toBe(true);
    expect(canTransition(JobState.RUNNING, JobState.PENDING)).toBe(true);
    // ...and to give up on it when the retry budget is spent.
    expect(canTransition(JobState.CLAIMED, JobState.DEAD)).toBe(true);
    expect(canTransition(JobState.RUNNING, JobState.DEAD)).toBe(true);
  });

  it('allows the DAG edges', () => {
    expect(canTransition(JobState.BLOCKED, JobState.PENDING)).toBe(true);
    expect(canTransition(JobState.BLOCKED, JobState.CANCELLED)).toBe(true);
  });

  it('allows a dead-letter job to be requeued manually', () => {
    expect(canTransition(JobState.DEAD, JobState.PENDING)).toBe(true);
  });

  it('refuses to skip the claim step', () => {
    expect(canTransition(JobState.PENDING, JobState.RUNNING)).toBe(false);
    expect(canTransition(JobState.PENDING, JobState.SUCCEEDED)).toBe(false);
  });

  it('refuses to leave a terminal state', () => {
    expect(JOB_TRANSITIONS.SUCCEEDED).toEqual([]);
    expect(JOB_TRANSITIONS.CANCELLED).toEqual([]);
    expect(canTransition(JobState.SUCCEEDED, JobState.PENDING)).toBe(false);
    expect(canTransition(JobState.CANCELLED, JobState.PENDING)).toBe(false);
  });

  it('refuses to cancel work that is already executing', () => {
    // Cancelling a RUNNING job would be a lie: the side effects are already in
    // flight and the queue cannot un-run them.
    expect(canTransition(JobState.RUNNING, JobState.CANCELLED)).toBe(false);
    expect(canTransition(JobState.CLAIMED, JobState.CANCELLED)).toBe(false);
  });
});

describe('transition()', () => {
  it('returns the target state for a legal edge', () => {
    expect(transition(JobState.PENDING, JobState.CLAIMED)).toBe('CLAIMED');
  });

  it('rejects an illegal edge supplied as runtime data', () => {
    expect(() => assertTransition(JobState.SUCCEEDED, JobState.PENDING)).toThrow(
      IllegalTransitionError,
    );
  });

  it('carries the offending edge on the error', () => {
    try {
      assertTransition(JobState.CANCELLED, JobState.RUNNING);
      expect.unreachable('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(IllegalTransitionError);
      expect((err as IllegalTransitionError).from).toBe('CANCELLED');
      expect((err as IllegalTransitionError).to).toBe('RUNNING');
    }
  });

  // Compile-time enforcement is the real guarantee; these are the cases that
  // must NOT typecheck. Uncommenting any of them should fail `npm run typecheck`:
  //
  //   transition(JobState.SUCCEEDED, JobState.PENDING);   // ts(2345)
  //   transition(JobState.PENDING, JobState.RUNNING);     // ts(2345)
  //   transition(JobState.RUNNING, JobState.CANCELLED);   // ts(2345)
  it('constrains the target type to the declared edges', () => {
    // @ts-expect-error PENDING cannot go straight to RUNNING
    expect(() => transition(JobState.PENDING, JobState.RUNNING)).toThrow();
    // @ts-expect-error SUCCEEDED is terminal
    expect(() => transition(JobState.SUCCEEDED, JobState.PENDING)).toThrow();
  });
});

describe('state predicates', () => {
  it('classifies terminal states', () => {
    expect(isTerminal(JobState.SUCCEEDED)).toBe(true);
    expect(isTerminal(JobState.CANCELLED)).toBe(true);
    expect(isTerminal(JobState.DEAD)).toBe(true);
    expect(isTerminal(JobState.PENDING)).toBe(false);
    expect(isTerminal(JobState.RUNNING)).toBe(false);
    expect(isTerminal(JobState.BLOCKED)).toBe(false);
  });

  it('classifies in-flight states, which are exactly the leased ones', () => {
    expect(isInFlight(JobState.CLAIMED)).toBe(true);
    expect(isInFlight(JobState.RUNNING)).toBe(true);
    expect(isInFlight(JobState.PENDING)).toBe(false);
  });

  it('validates strings arriving from the database', () => {
    expect(isJobState('PENDING')).toBe(true);
    expect(isJobState('pending')).toBe(false);
    expect(isJobState('NOPE')).toBe(false);
    expect(isJobState(7)).toBe(false);
  });
});
