// Whole-tree deletion, shared by retention and the delete command (runnerq-go's
// storage/postgres/trees.go). Callers hold the root row FOR UPDATE.
import type { PoolClient } from "pg";

/** The statuses an activity never leaves (bar a retry command). */
export const terminalSQL = "('completed','failed','dead_letter','cancelled')";

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
  await c.query(
    `SELECT 1 FROM runnerq_idempotency WHERE queue_name=$1 AND activity_id IN
    (SELECT id FROM runnerq_activities WHERE queue_name=$1 AND (id=$2 OR root_activity_id=$2)) FOR UPDATE`,
    [queue, root],
  );
  const r = await c.query(
    `SELECT EXISTS(SELECT 1 FROM runnerq_dependencies d
    JOIN runnerq_activities producer ON producer.id=d.producer_activity_id AND producer.queue_name=$1
    JOIN runnerq_activities waiter ON waiter.id=d.waiter_activity_id AND waiter.queue_name=$1
    WHERE d.queue_name=$1 AND COALESCE(producer.root_activity_id,producer.id)=$2
    AND COALESCE(waiter.root_activity_id,waiter.id)<>$2
    AND EXISTS(SELECT 1 FROM runnerq_activities live WHERE live.queue_name=$1
      AND COALESCE(live.root_activity_id,live.id)=COALESCE(waiter.root_activity_id,waiter.id)
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
    `WITH tree AS (SELECT id FROM runnerq_activities WHERE queue_name=$1 AND (id=$2 OR root_activity_id=$2)),
    del_dependencies AS (DELETE FROM runnerq_dependencies WHERE queue_name=$1
      AND (waiter_activity_id IN (SELECT id FROM tree) OR producer_activity_id IN (SELECT id FROM tree))),
    del_results AS (DELETE FROM runnerq_results WHERE queue_name=$1
      AND (activity_id IN (SELECT id FROM tree) OR owner_activity_id IN (SELECT id FROM tree)) RETURNING activity_id),
    del_events AS (DELETE FROM runnerq_events WHERE queue_name=$1
      AND (activity_id IN (SELECT id FROM tree) OR activity_id IN (SELECT activity_id FROM del_results))),
    del_idem AS (DELETE FROM runnerq_idempotency WHERE queue_name=$1 AND activity_id IN (SELECT id FROM tree)),
    del_inputs AS (DELETE FROM runnerq_inputs WHERE activity_id IN (SELECT id FROM tree))
    DELETE FROM runnerq_activities WHERE queue_name=$1 AND id IN (SELECT id FROM tree)`,
    [queue, root],
  );
}
