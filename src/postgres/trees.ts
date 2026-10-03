// Whole-tree deletion, shared by retention and the delete command (runnerq-go's
// storage/postgres/trees.go). Callers hold the root row FOR UPDATE.
import type { PoolClient } from "pg";

/** The statuses an activity never leaves (bar a retry command). */
export const terminalSQL = "('completed','failed','dead_letter','cancelled')";

/**
 * The workflow rooted at $2 in queue $1: the root by id, its descendants through
 * idx_runnerq_root_children.
 */
export const treeSQL = `SELECT a.id,a.idempotency_key FROM runnerq_activities a WHERE a.queue_name=$1
  AND (a.id=$2 OR (a.root_activity_id=$2 AND a.parent_activity_id IS NOT NULL))`;

/**
 * Locks the tree's idempotency keys, then reports whether a live workflow in another tree
 * depends on a result this tree produced (the tree must then be kept). Key reuse registers
 * its dependency holding the key row, so locking the keys first orders it against this check.
 */
export async function lockTree(
  c: PoolClient,
  queue: string,
  root: string,
): Promise<boolean> {
  // A key's activity carries the key, so each key is a primary-key probe.
  await c.query(
    `SELECT 1 FROM runnerq_idempotency k JOIN (${treeSQL}) t
    ON k.queue_name=$1 AND k.idempotency_key=t.idempotency_key AND k.activity_id=t.id FOR UPDATE OF k`,
    [queue, root],
  );
  const r = await c.query(
    `SELECT EXISTS(SELECT 1 FROM (${treeSQL}) t
    JOIN runnerq_dependencies d ON d.queue_name=$1 AND d.result_id=t.id AND d.producer_activity_id=t.id
    JOIN runnerq_activities waiter ON waiter.id=d.waiter_activity_id AND waiter.queue_name=$1
    WHERE waiter.root_activity_id<>$2
    AND EXISTS(SELECT 1 FROM runnerq_activities live WHERE live.queue_name=$1
      AND (live.id=waiter.root_activity_id
        OR (live.root_activity_id=waiter.root_activity_id AND live.parent_activity_id IS NOT NULL))
      AND live.status NOT IN ${terminalSQL})) AS pinned`,
    [queue, root],
  );
  return r.rows[0].pinned;
}

/** Deletes the tree with its inputs, results, events, keys and dependencies. */
export async function deleteTree(
  c: PoolClient,
  queue: string,
  root: string,
): Promise<void> {
  await c.query(
    `WITH tree AS (${treeSQL}),
    del_dependencies AS (DELETE FROM runnerq_dependencies WHERE queue_name=$1
      AND (waiter_activity_id IN (SELECT id FROM tree)
        OR (result_id IN (SELECT id FROM tree) AND producer_activity_id IS NOT NULL))),
    del_results AS (DELETE FROM runnerq_results WHERE queue_name=$1
      AND (activity_id IN (SELECT id FROM tree) OR owner_activity_id IN (SELECT id FROM tree)) RETURNING activity_id),
    del_events AS (DELETE FROM runnerq_events WHERE queue_name=$1
      AND (activity_id IN (SELECT id FROM tree) OR activity_id IN (SELECT activity_id FROM del_results))),
    del_idem AS (DELETE FROM runnerq_idempotency k USING tree
      WHERE k.queue_name=$1 AND k.idempotency_key=tree.idempotency_key AND k.activity_id=tree.id),
    del_inputs AS (DELETE FROM runnerq_inputs WHERE activity_id IN (SELECT id FROM tree))
    DELETE FROM runnerq_activities WHERE queue_name=$1 AND id IN (SELECT id FROM tree)`,
    [queue, root],
  );
}

/** $2 and $3 are the completed and failed ages in milliseconds (0 keeps them). */
const expiredCondSQL = `r.status IN ${terminalSQL}
  AND r.completed_at<NOW()-(CASE WHEN r.status='completed' THEN $2::bigint ELSE $3::bigint END)*INTERVAL '1 millisecond'
  AND CASE WHEN r.status='completed' THEN $2::bigint ELSE $3::bigint END>0`;

/**
 * The oldest expired root of queue $1 with no live descendant ($4 lists roots already passed
 * over): one short walk of idx_runnerq_root_terminal per terminal status.
 */
export const expiredRootSQL = `SELECT r.id FROM unnest(ARRAY['completed','failed','dead_letter','cancelled']) s(status)
  CROSS JOIN LATERAL (SELECT r.id,r.completed_at FROM runnerq_activities r
    WHERE r.queue_name=$1 AND r.parent_activity_id IS NULL AND r.status IN ${terminalSQL} AND r.status=s.status
    AND r.completed_at<NOW()-(CASE WHEN s.status='completed' THEN $2::bigint ELSE $3::bigint END)*INTERVAL '1 millisecond'
    AND r.id<>ALL($4::uuid[])
    AND NOT EXISTS(SELECT 1 FROM runnerq_activities c WHERE c.root_activity_id=r.id AND c.parent_activity_id IS NOT NULL
      AND c.status NOT IN ${terminalSQL})
    ORDER BY r.completed_at LIMIT 1) r
  WHERE CASE WHEN s.status='completed' THEN $2::bigint ELSE $3::bigint END>0
  ORDER BY r.completed_at LIMIT 1`;

/** Locks and rechecks a candidate root ($4) that expiredRootSQL read unlocked. */
export const lockExpiredRootSQL = `SELECT EXISTS(SELECT 1 FROM runnerq_activities r
  WHERE r.id=$4 AND r.queue_name=$1 AND r.parent_activity_id IS NULL AND ${expiredCondSQL}
  FOR UPDATE SKIP LOCKED) AS locked`;

/**
 * Deletes up to $3 events older than $2 milliseconds whose activity has finished (or is
 * gone), oldest first along idx_runnerq_events_queue_seq. Events of unfinished activities
 * stay: a waiting activity's latest Yielded is its waiting reason.
 */
export const trimEventsSQL = `DELETE FROM runnerq_events WHERE id IN (SELECT e.id FROM runnerq_events e
  WHERE e.queue_name=$1 AND e.created_at<NOW()-$2::bigint*INTERVAL '1 millisecond'
  AND NOT EXISTS(SELECT 1 FROM runnerq_activities a WHERE a.id=e.activity_id AND a.status NOT IN ${terminalSQL})
  ORDER BY e.queue_name,e.id LIMIT $3)`;
