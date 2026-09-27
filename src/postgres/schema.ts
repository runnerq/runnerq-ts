// Baseline only: the separate-input schema is intentionally incompatible with old Go deployments.
export const schema = `
CREATE TABLE runnerq_activities (
 id UUID PRIMARY KEY, queue_name TEXT NOT NULL, activity_type TEXT NOT NULL,
 priority INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'pending',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), scheduled_at TIMESTAMPTZ,
 started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, lease_deadline_ms BIGINT,
 current_worker_id TEXT, last_worker_id TEXT, retry_count INTEGER NOT NULL DEFAULT 0,
 max_retries INTEGER NOT NULL DEFAULT 0, timeout_seconds BIGINT NOT NULL DEFAULT 300,
 retry_delay_seconds BIGINT NOT NULL DEFAULT 60, max_retry_delay_seconds BIGINT NOT NULL DEFAULT 0,
 last_error TEXT, last_error_at TIMESTAMPTZ, metadata JSONB, idempotency_key TEXT,
 parent_activity_id UUID, root_activity_id UUID, depth SMALLINT NOT NULL DEFAULT 0,
 waiting_result_id UUID
);
CREATE TABLE runnerq_inputs (
 activity_id UUID PRIMARY KEY, queue_name TEXT NOT NULL, payload JSONB NOT NULL,
 serialization TEXT NOT NULL DEFAULT 'json-v1'
);
CREATE TABLE runnerq_idempotency (
 queue_name TEXT NOT NULL, idempotency_key TEXT NOT NULL, activity_id UUID NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY(queue_name, idempotency_key)
);
CREATE TABLE runnerq_events (
 id BIGSERIAL PRIMARY KEY, activity_id UUID NOT NULL, queue_name TEXT NOT NULL,
 event_type TEXT NOT NULL, worker_id TEXT, detail JSONB,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE runnerq_results (
 activity_id UUID PRIMARY KEY, queue_name TEXT NOT NULL, state TEXT NOT NULL,
 data JSONB, serialization TEXT NOT NULL DEFAULT 'json-v1', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), owner_activity_id UUID, step TEXT
);
CREATE TABLE runnerq_worker_pools (
 pool_id UUID PRIMARY KEY, queue_name TEXT NOT NULL, max_workers INTEGER NOT NULL,
 activity_types TEXT[], started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE runnerq_dependencies (
 queue_name TEXT NOT NULL, waiter_activity_id UUID NOT NULL, result_id UUID NOT NULL,
 producer_activity_id UUID, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY(queue_name, waiter_activity_id, result_id)
);
CREATE INDEX idx_runnerq_dequeue_effective_v2 ON runnerq_activities
 (queue_name, activity_type, priority DESC, retry_count DESC, COALESCE(scheduled_at, created_at) ASC)
 WHERE status IN ('pending','scheduled','retrying','waiting');
CREATE INDEX idx_runnerq_dequeue_order_v2 ON runnerq_activities
 (queue_name, priority DESC, retry_count DESC, COALESCE(scheduled_at, created_at) ASC)
 WHERE status IN ('pending','scheduled','retrying','waiting');
CREATE INDEX idx_runnerq_activities_processing ON runnerq_activities(queue_name, lease_deadline_ms) WHERE status='processing';
CREATE INDEX idx_runnerq_completed_non_cron ON runnerq_activities(queue_name, completed_at DESC, created_at DESC)
 WHERE status IN ('completed','failed') AND (metadata->>'source') IS DISTINCT FROM 'cron';
CREATE INDEX idx_runnerq_completed_cron ON runnerq_activities(queue_name, completed_at DESC, created_at DESC)
 WHERE status IN ('completed','failed') AND metadata->>'source'='cron';
CREATE INDEX idx_runnerq_dead_letter ON runnerq_activities(queue_name, completed_at DESC) WHERE status='dead_letter';
CREATE INDEX idx_runnerq_parent_id ON runnerq_activities(parent_activity_id) WHERE parent_activity_id IS NOT NULL;
CREATE INDEX idx_runnerq_root_id ON runnerq_activities(root_activity_id) WHERE root_activity_id IS NOT NULL;
CREATE INDEX idx_runnerq_root_only ON runnerq_activities(queue_name, created_at DESC) WHERE parent_activity_id IS NULL;
CREATE INDEX idx_runnerq_root_status ON runnerq_activities(queue_name, status) WHERE parent_activity_id IS NULL;
CREATE INDEX idx_runnerq_events_activity ON runnerq_events(activity_id, created_at DESC);
CREATE INDEX idx_runnerq_events_queue_seq ON runnerq_events(queue_name, id);
CREATE INDEX idx_runnerq_results_owner ON runnerq_results(queue_name, owner_activity_id) WHERE owner_activity_id IS NOT NULL;
CREATE INDEX idx_runnerq_root_terminal_age ON runnerq_activities(queue_name, status, completed_at)
 WHERE parent_activity_id IS NULL AND status IN ('completed','failed','dead_letter');
CREATE INDEX idx_runnerq_worker_pools_queue_alive ON runnerq_worker_pools(queue_name, last_seen_at);
CREATE INDEX idx_runnerq_dependencies_result ON runnerq_dependencies(queue_name, result_id);
CREATE INDEX idx_runnerq_dependencies_producer ON runnerq_dependencies(queue_name, producer_activity_id);
`;
export const schemaLock = "5932734182207934753"; // 0x52554E4E45525121, shared with Go.
export const tableNames = [...schema.matchAll(/CREATE TABLE (\w+)/g)].map(
  (m) => m[1]!,
);
export const indexNames = [...schema.matchAll(/CREATE INDEX (\w+)/g)].map(
  (m) => m[1]!,
);
