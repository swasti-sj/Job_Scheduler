-- 002_indexes.sql : indexes that make the hot paths index-only-ish
--
-- The claim query pulls candidates per priority level with a LATERAL over
-- generate_series(0,9); this partial index is what makes each of those ten
-- probes an index range scan over ready work only.
CREATE INDEX IF NOT EXISTS jobs_claim_idx
  ON jobs (queue_name, priority, scheduled_for)
  WHERE state = 'PENDING';

-- Reaper: find expired leases. Tiny partial index - only in-flight rows.
CREATE INDEX IF NOT EXISTS jobs_lease_idx
  ON jobs (lease_expires_at)
  WHERE state IN ('CLAIMED', 'RUNNING');

-- Immediate reclaim path: "everything this worker holds".
CREATE INDEX IF NOT EXISTS jobs_claimed_by_idx
  ON jobs (claimed_by)
  WHERE state IN ('CLAIMED', 'RUNNING');

-- Fair-share CTE: per-tenant in-flight counts.
CREATE INDEX IF NOT EXISTS jobs_inflight_tenant_idx
  ON jobs (tenant_id)
  WHERE state IN ('CLAIMED', 'RUNNING');

-- Fan-out unblocking: "which jobs depend on X". GIN over the uuid[] column.
CREATE INDEX IF NOT EXISTS jobs_depends_on_gin
  ON jobs USING gin (depends_on)
  WHERE state = 'BLOCKED';

-- Idempotency. Scoped per tenant: two tenants may legitimately reuse a key.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_uniq
  ON jobs (tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- Queue depth / dashboard aggregates.
CREATE INDEX IF NOT EXISTS jobs_stats_idx ON jobs (queue_name, tenant_id, state);

-- Dead letter listing and recent-failure feed.
CREATE INDEX IF NOT EXISTS jobs_terminal_idx
  ON jobs (completed_at DESC)
  WHERE state IN ('DEAD', 'FAILED');

CREATE INDEX IF NOT EXISTS workers_heartbeat_idx ON workers (last_heartbeat_at)
  WHERE state <> 'DEAD';
