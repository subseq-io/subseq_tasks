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

## Current Reality (2026-02-08 snapshot)

`tasks.task_links` was authoritative at this snapshot for:
- link create/delete APIs
- task details (`links_out`, `links_in`)
- subtree queries for archive/unarchive/delete/impact

`subseq_graph` was used as an invariant engine (projection validation), not as task-link storage.

This section is historical; see `Implementation Status (2026-02-09)` for the current state.

## Reassessment After Graph Delta/Reparent Updates

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
- Atomic tree reparent/detach APIs:
  - `reparent_node` / `reparent_node_tx`
  - validates tree invariants on final edge set (not remove/add intermediate states)
  - Source: `../subseq_graph/src/db.rs`, `../subseq_graph/src/api.rs`, `../subseq_graph/src/operations.rs`
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
- For create/delete/reparent, issue delta mutations (including `reparent_node` for tree parent changes).
- Mirror operations across all shared project graphs and block on any violation.

4. Read-path reconstruction:
- Rebuild task `links_out`/`links_in` from graph edges (including `link_type` and `subtask_parent_state` metadata).
- Resolve how to present duplicates/conflicts when a task exists in multiple independent project graphs.

5. Subtree operations:
- Implement subtree traversal from tree graph(s) for archive/unarchive/delete/impact count.
- Define behavior when task appears in multiple project trees (union/intersection/project-scoped).

## What Is Now a Thin Shim

For all link types (`subtask_of`, `depends_on`, `related_to`, `assignment_order`), storage cutover can now be mostly translational:
- upsert task nodes by stable external ID metadata
- add/remove edges via delta ops
- perform tree parent changes with `reparent_node`
- mirror across shared project graphs inside one caller-owned SQL transaction
- query incident edges or metadata-filtered edges for read assembly

## Tree Caveat

`reparent_node` supports detach (`newParentNodeId = null`), but tree invariants still apply to the final state.

Implication:
- detach is valid only when resulting root/connected constraints remain satisfied (or when app reattaches to the project synthetic root as part of the same logical operation).

## Recommendation

The graph API surface is now sufficient to begin a true graph-SOT migration without relying on full-graph replace for normal link updates.

Recommended path:
1. Introduce graph node/edge identity mapping per project layer and dual-write `task_links` + graph mutations.
2. Use delta ops for add/remove and `reparent_node` for subtask parent changes across mirrored project graphs.
3. Switch read paths (`links_out`, `links_in`, subtree impact/cascade) to graph-backed queries once parity checks pass.
4. Remove `tasks.task_links` once graph-backed behavior is stable under archive/unarchive/delete/reparent flows.

Status: completed on 2026-02-09 (see section below).

## Implementation Status (2026-02-09)

Implemented in `/Users/teague/Subsequent/backend/subseq_tasks`:
- Added graph mapping tables:
  - `tasks.project_link_graphs` (per-project/per-link-type graph bindings)
  - `tasks.task_link_graph_nodes` (task-to-node mappings)
  - Migration: `migrations/20260209010100_task_link_graph_storage.sql`
- Switched task link operations to graph APIs:
  - create/delete link paths use `upsert_edge_metadata` / `remove_edge` / `reparent_node`
  - task detail link hydration (`links_out`, `links_in`) is rebuilt from graph edges
  - subtree traversal for impact/cascade uses graph-backed subtask edges
- Initialized project link graphs at project creation and materialized task nodes at task creation.
- Removed legacy table authority:
  - dropped `tasks.task_links`
  - refreshed `tasks.task_presentation` without table dependency
  - migration: `migrations/20260209010200_drop_task_links_table.sql`
- Verified by test run: `cargo test` (9 passed, 0 failed).
