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

## Reassessment After Graph Delta Updates

The previously missing high-impact DX entrypoints are now implemented in `subseq_graph`.

Implemented and verified in code:
- Delta edge mutation APIs:
  - `add_edge` / `add_edge_tx`
  - `remove_edge` / `remove_edge_tx`
  - `upsert_edge_metadata` / `upsert_edge_metadata_tx`
  - Source: `../subseq_graph/src/db.rs`
- Delta node mutation APIs:
  - `upsert_node` / `upsert_node_tx`
  - `remove_node` / `remove_node_tx`
  - Source: `../subseq_graph/src/db.rs`
- Caller-owned transaction composition:
  - all `_tx` variants accept caller transaction
  - Source: `../subseq_graph/src/db.rs`
- Cross-graph atomic delta batching:
  - `apply_graph_delta_batch_tx(commands: &[GraphDeltaCommand])`
  - each command carries its own `graph_id`
  - Source: `../subseq_graph/src/db.rs`, `../subseq_graph/src/models.rs`
- Optimistic concurrency:
  - optional `expected_updated_at` on write payloads
  - stale writes fail with `error.code = stale_graph_update`
  - Source: `../subseq_graph/src/db.rs`, `../subseq_graph/src/models.rs`
- Task-centric lookup helpers:
  - node by external id
  - incident edges by node id and by external id
  - Source: `../subseq_graph/src/db.rs`, `../subseq_graph/src/api.rs`
- Metadata filter helpers and indexes:
  - `nodes @> metadata_contains`, `edges @> metadata_contains`
  - GIN indexes + external_id expression index
  - Source: `../subseq_graph/src/db.rs`, `../subseq_graph/migrations/20260208000500_graph_metadata_indexes.sql`
- Existing invariant preflight helpers remain available:
  - `would_add_edge_violations`
  - `would_remove_edge_violations`
  - `would_remove_edge_isolate_subgraph`
  - Source: `../subseq_graph/src/invariants.rs`

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

## What Is Now a Thin Shim

For `depends_on`, `related_to`, and `assignment_order`, storage cutover can now be a mostly translational shim:
- upsert task nodes by stable external ID metadata
- add/remove edges via delta ops
- mirror across shared project graphs inside one caller-owned SQL transaction
- query incident edges or metadata-filtered edges for read assembly

## Remaining Non-Shim Gap

Tree reparent/detach flows (`subtask_of`) still need one additional primitive or fallback path.

Why:
- `add_edge_tx` and `remove_edge_tx` validate tree invariants on each individual mutation.
- reparent requires changing parent edge(s) as a set.
- valid final state can require invalid intermediate states:
  - add new parent before removing old parent can violate in-degree (2 parents)
  - remove old parent before adding new parent/root can violate single-root/connected-tree constraints

Result:
- a pure sequence of current tree delta operations cannot always represent reparent atomically.

Missing entrypoint for a pure shim:
- either a dedicated tree reparent/move helper (`move_edge` or `reparent_subtree`) that validates final state
- or a batch mode that applies a command set then validates invariants once on the resulting graph state

Current workaround:
- for reparent/subtree surgery, use guarded full-graph replace (`update_graph_with_guard`) for that graph, while using delta ops for simpler mutations.

## Recommendation

The delta API surface is now strong enough to start graph-backed storage migration with substantially less glue code than before.

Recommended path:
1. Move non-subtask link types to graph SOT first using delta APIs.
2. Keep `subtask_of` on guarded replace path or retain table authority until reparent primitive lands.
3. Dual-write and parity-check read paths.
4. Remove `tasks.task_links` once subtree/archive/delete flows are fully graph-native.
