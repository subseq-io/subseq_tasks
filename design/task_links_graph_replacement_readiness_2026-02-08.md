# task_links Replacement Readiness (Graph-backed)

Date: 2026-02-08
Repo: `/Users/teague/Subsequent/backend/subseq_tasks`
Dependency audited: `/Users/teague/Subsequent/backend/subseq_graph`

## Goal

Replace `tasks.task_links` as storage of task relationships and use graph-backed links as the source of truth.

Relationship layers:
- `subtask_of` -> tree (project-level forest via synthetic root node)
- `depends_on` -> DAG
- `related_to` / `assignment_order` -> directed

## Current Reality

`tasks.task_links` is currently authoritative for:
- link create/delete APIs
- task details (`links_out`, `links_in`)
- subtree queries for archive/unarchive/delete/impact

`subseq_graph` is currently used as an invariant engine (projection validation), not as task-link storage.

## What Existing `subseq_graph` Can Already Do

Available and usable today:
- Persist graph kind (`tree`, `dag`, `directed`).
- Full-graph read/write:
  - `create_graph`
  - `get_graph`
  - `update_graph` (replace full nodes+edges)
  - `delete_graph`
  - `extend_graph` (additive merge)
- Fast preflight checks:
  - `GraphMutationIndex::would_add_edge_violations`
  - `GraphMutationIndex::would_remove_edge_violations`
  - `GraphMutationIndex::would_remove_edge_isolate_subgraph`

## Required to Replace `task_links`

At minimum, the app must provide:
1. Graph ownership/mapping strategy:
- Per project, per layer graph IDs (at least 3 graph IDs per project).
- Stable task-node identity inside each project-layer graph.
- Because graph node IDs are globally unique in `graph.nodes.id`, a task appearing in multiple projects cannot share one node ID across project graphs.

2. Node lifecycle:
- Ensure task node exists in all relevant project-layer graphs on task create/project attach.
- Subtask tree graph must maintain a synthetic root node and synthetic edges to each root task.

3. Link mutation orchestration:
- For create/delete/reparent, load graph, mutate edges (and possibly nodes), validate, then replace graph.
- Mirror operations across all shared project graphs and block on any violation.

4. Read-path reconstruction:
- Rebuild task `links_out`/`links_in` from graph edges (including `link_type` and `subtask_parent_state` metadata).
- Resolve how to present duplicates/conflicts when a task exists in multiple independent project graphs.

5. Subtree operations:
- Implement subtree traversal from tree graph(s) for archive/unarchive/delete/impact count.
- Define behavior when task appears in multiple project trees (union/intersection/project-scoped).

## Why This Is More Than a Translational Shim

A pure shim would map each table write to a direct graph edge mutation call.
Current APIs do not provide that shape. Today each mutation requires read-modify-replace of entire graph payloads plus app-side orchestration.

This is an integration redesign, not a direct storage swap.

## Missing `subseq_graph` Entrypoints (DX Gaps)

These are the key gaps if we want a low-complexity replacement:

1. Delta edge mutation APIs (transactional):
- `add_edge(graph_id, from, to, metadata)`
- `remove_edge(graph_id, from, to)`
- `upsert_edge_metadata(...)`
- Optional: `move_edge`/reparent helper for tree graphs

2. Delta node mutation APIs:
- `upsert_node(graph_id, node_id, label, metadata)`
- `remove_node(graph_id, node_id)` with predictable cascade semantics

3. External identity lookup helpers:
- Query node by metadata key/value (example: `taskId`) or allow external ID field.
- Query incident edges for a node without fetching full graph.

4. Read helpers for task-centric access:
- `list_edges_for_node(graph_id, node_id, direction)`
- Optional filters by metadata (`link_type`, state metadata).

5. Multi-graph atomic/batched mutation support:
- Execute mirrored mutations across graph IDs in one transaction boundary, or expose compensating-batch primitive.

6. Transaction composition:
- DB entrypoints that accept caller-owned transaction/executor, so task + graph writes can be one ACID unit.

7. Optional optimistic concurrency:
- Version/etag precondition on graph update to prevent lost updates in concurrent read-modify-replace flows.

## Recommendation

Two viable paths:

1. Keep `task_links` authoritative short-term (current state), continue using graph checks for invariants, and defer storage migration until graph delta APIs exist.

2. If migration must start now, do a staged approach:
- Stage A: introduce project-layer graph mapping + deterministic node IDs.
- Stage B: dual-write (`task_links` + graph edges), verify parity.
- Stage C: switch reads to graph-backed views.
- Stage D: remove `task_links` table only after parity and cascade/impact semantics are stable.

Given current APIs, path (1) is lower risk.
