-- 003_events.sql : append-only audit of state transitions.
--
-- Used by the chaos harness to prove "exactly once": a job that ran twice would
-- show two RUNNING -> SUCCEEDED transitions here even if the jobs row itself
-- only records the last one.
CREATE TABLE IF NOT EXISTS job_events (
  id          bigserial PRIMARY KEY,
  job_id      uuid NOT NULL,
  from_state  job_state,
  to_state    job_state NOT NULL,
  worker_id   text,
  node_id     text,
  detail      text,
  at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events (job_id, id);
CREATE INDEX IF NOT EXISTS job_events_at_idx ON job_events (at DESC);
