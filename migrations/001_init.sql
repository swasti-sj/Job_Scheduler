-- 001_init.sql : core schema
-- Plain numbered SQL. Applied in lexical order by src/db/migrate.ts inside a
-- transaction guarded by a Postgres advisory lock, so concurrent boots are safe.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$ BEGIN
  CREATE TYPE job_state AS ENUM (
    'PENDING', 'BLOCKED', 'CLAIMED', 'RUNNING',
    'SUCCEEDED', 'FAILED', 'DEAD', 'CANCELLED'
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE worker_state AS ENUM ('IDLE', 'BUSY', 'DEAD');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS tenants (
  tenant_id   text PRIMARY KEY,
  weight      real NOT NULL DEFAULT 1 CHECK (weight > 0),
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id         text        NOT NULL,
  queue_name        text        NOT NULL,
  job_type          text        NOT NULL,
  payload           jsonb       NOT NULL DEFAULT '{}'::jsonb,
  priority          smallint    NOT NULL DEFAULT 5 CHECK (priority BETWEEN 0 AND 9),
  state             job_state   NOT NULL DEFAULT 'PENDING',
  attempt_count     integer     NOT NULL DEFAULT 0,
  max_attempts      integer     NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
  created_at        timestamptz NOT NULL DEFAULT now(),
  scheduled_for     timestamptz NOT NULL DEFAULT now(),
  claimed_at        timestamptz,
  lease_expires_at  timestamptz,
  completed_at      timestamptz,
  idempotency_key   text,
  depends_on        uuid[]      NOT NULL DEFAULT '{}',
  last_error        text,
  claimed_by        text,
  result            jsonb,
  updated_at        timestamptz NOT NULL DEFAULT now(),

  -- A claimed/running job must always carry a lease and an owner. This is the
  -- invariant both reclaim paths depend on; enforcing it in the database means a
  -- buggy code path fails loudly instead of orphaning a job forever.
  CONSTRAINT jobs_lease_present CHECK (
    (state NOT IN ('CLAIMED', 'RUNNING'))
    OR (claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS workers (
  id               text PRIMARY KEY,
  hostname         text NOT NULL,
  pid              integer NOT NULL,
  queues           text[] NOT NULL DEFAULT '{}',
  max_concurrency  integer NOT NULL DEFAULT 1 CHECK (max_concurrency >= 1),
  state            worker_state NOT NULL DEFAULT 'IDLE',
  current_job_id   uuid,
  inflight_count   integer NOT NULL DEFAULT 0,
  connected_node   text,
  last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
  registered_at    timestamptz NOT NULL DEFAULT now()
);

-- Per job_type circuit breaker. Leader-owned; every node reads it.
CREATE TABLE IF NOT EXISTS circuit_breakers (
  job_type      text PRIMARY KEY,
  state         text NOT NULL DEFAULT 'CLOSED' CHECK (state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
  failure_rate  real NOT NULL DEFAULT 0,
  samples       integer NOT NULL DEFAULT 0,
  opened_at     timestamptz,
  reopen_after  timestamptz,
  updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Cron definitions evaluated by the leader only.
CREATE TABLE IF NOT EXISTS cron_jobs (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name          text UNIQUE NOT NULL,
  schedule      text NOT NULL,
  tenant_id     text NOT NULL,
  queue_name    text NOT NULL DEFAULT 'default',
  job_type      text NOT NULL,
  payload       jsonb NOT NULL DEFAULT '{}'::jsonb,
  priority      smallint NOT NULL DEFAULT 5,
  enabled       boolean NOT NULL DEFAULT true,
  last_run_at   timestamptz,
  next_run_at   timestamptz NOT NULL DEFAULT now()
);
