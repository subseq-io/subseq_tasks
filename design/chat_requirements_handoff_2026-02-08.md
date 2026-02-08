# subseq_tasks Migration + Workflow Requirements (Chat Handoff)

Date: 2026-02-08

## Context
This repository was newly initialized from older code and needed migration to the newer service pattern used by sibling repositories.

## Core Migration Objectives
1. Follow modular structure by seam:
- `api` modules
- `models` modules
- `db` modules

2. Use `sqlx` patterns (replace Diesel usage).

3. Replace legacy ownership/util dependencies with `subseq_auth`.

4. Use `subseq_graph` integration and reference it in Cargo via git:
- `https://github.com/subseq-io/subseq_graph.git`

5. Keep domain focused on:
- tasks
- milestones
- projects

6. Remove prompt-driven placeholders and retain working parts.

7. Preserve tightly-scoped `NameId` newtypes.

## Role/Permission Requirements
1. Use hard-coded role strings (example: `task_update`) for action-level authorization.
2. Enforce checks at security boundaries.
3. Users always have read/write access to their own tasks.
4. Write implies read.
5. Include helper APIs for role sets and role strings, including:
- `full_permissions`
- `write_permissions`
- `read_permissions`
6. Document role list and behavior in README.
7. Include graph permission coupling where operations read/validate underlying task graph data.

## Task Workflow Requirements (State Machine)
All non-terminal states can short-circuit to `Done` or `Rejected`.

States and transitions:
- `Open` -> `Todo`
- `Todo` -> `Assigned | Open | In Progress`
- `Assigned` -> `In Progress | Todo`
- `In Progress` -> `Todo | Acceptance`
- `Acceptance` -> `In Progress | Done` (plus global short-circuit rule)
- Any non-terminal -> `Done | Rejected`

Transition metadata expectations:
- `Done`: optional by whom, optional timestamp
- `Rejected`: optional reason
- `Open -> Todo`: optional when
- `Todo -> Assigned`: required assignee
- `Todo -> Open`: optional deferral reason
- `Todo -> In Progress`: self-assignment semantics
- `Assigned -> In Progress`: optional estimate
- `Assigned -> Todo`: optional can't-do reason
- `In Progress -> Todo`: optional can't-do reason
- `In Progress -> Acceptance`: optional work log details
- `Acceptance -> In Progress`: optional feedback

Behavioral requirements:
1. Transition side effects should update task fields when there is a matching field (example: assignee on assignment transitions).
2. If no matching field exists, actor should leave a comment.
3. All task actions/updates/transitions must be appended to task log.

## Additional Task Structures
1. Keep/support task comments as first-class records.
2. Keep/support task log (history of all updates/actions).
3. Task link data must encode task state for subtask context:
- `subtask_parent_state`

## Graph-Layer Conceptual Direction (Follow-up)
User clarified relationship semantics should be treated as separate overlay graph layers:

1. `subtask_of`
- Conceptually a **tree** (not DAG).
- Parent-child hierarchy semantics.
- User suggested root tracking to bound subtrees and support queries like:
  - "I am done if all my subtasks in this state are done."
- Suggested field concept: root identifier per subtree membership.

2. `depends_on`
- Conceptually a **DAG**.
- Independent from subtask hierarchy.

3. `related_to`
- Conceptually a **directed knowledge graph**.
- Backlinks/cycles acceptable.

## Graph-Layer Clarifications (Confirmed)
Date: 2026-02-08

1. Project-level structure for `subtask_of` is a **forest of trees**, not a single global tree.
2. Projects are independent graph scopes.
3. If a task is present in multiple projects, each project stores an independent duplicate node entry for that task.
4. Graph mutations for multi-project tasks must be mirrored across each project graph and coordinated as **all-or-nothing**:
- If any project graph would violate invariants, the operation is blocked.
5. Subtask reparenting is a **full subtree migration**, not a single-node move.
6. If a task/subtree is detached and becomes isolated from its previous root, it becomes a new top-level root tree in that project forest.
7. If a task is created as its own root tree and later attached under another task, its previous standalone tree is merged into the destination tree; remove the old graph object only when empty after merge.
8. Archival and deletion operations cascade through the entire subtree.
9. UI needs a preflight/count API for destructive subtree operations:
- \"How many tasks are affected by this operation?\"
10. Edge directions are fixed:
- `subtask_of`: `parent -> child`
- `depends_on`: `prerequisite -> dependent`
11. Invariant checks should run in the same transaction context as write operations when feasible.

## Graph-Layer Clarifications (Additional)
Date: 2026-02-08

1. Forest conceptual model:
- Treat the project-level subtask structure as a forest container over root trees.
- Project graph conceptually points to all root trees.
- Exact storage/modeling details for this forest container are still open and require follow-up design.
2. Cross-project mirrored mutations:
- Prefer correctness over parallelism; run mirrored updates with one transactional unit of work at a time.
3. Archive/delete/unarchive semantics:
- Delete excises subtree tasks and corresponding graph participation.
- Archive cascades to subtree but archived tasks are excluded from active dependency/blocking considerations.
- Unarchive reinvolves tasks in graph considerations and must pass invariant checks before becoming active.

## Recommended `subseq_graph` Invariant Work (for separate worker)
1. Add graph kinds: `tree`, `dag`, `directed`.
2. Persist graph kind in schema/model.
3. Enforce invariants on create/update/extend/write paths.
4. Provide edge validation API with machine-readable violation reason.
5. `tree`:
- acyclic
- no self-loop
- in-degree <= 1
- stable/rooted subtree semantics
6. `dag`:
- acyclic
- no self-loop
7. `directed`:
- cycles allowed
8. Keep auth behavior unchanged; invariant violations should be validation errors.
9. Add comprehensive tests for all invariant classes and write paths.

## Implementation Status Summary (this repo)
Implemented in this repo during this chat:
- Modularized `api/models/db` by tasks/projects/milestones seams.
- Added hard-coded scoped role permission helpers and write-implies-read logic.
- Added task comments and task log support.
- Added workflow states and transition endpoint behavior with logging/comment capture.
- Added `subtask_parent_state` support on task links.
- Updated README with role and workflow behavior.
- Added graph-backed constraint validation for task links using `subseq_graph` invariants at mutation boundaries:
  - `subtask_of` modeled as project-level forest through synthetic project-root tree validation.
  - `depends_on` validated as DAG.
  - `related_to`/`assignment_order` validated as directed graph.
- Added shared-project mirrored validation for link changes with transactional locking and all-or-nothing blocking on any violating shared project.
- Implemented cascading subtree archive/unarchive/delete behavior from `subtask_of` structure.
- Added cascade impact preflight API support for UI confirmation:
  - `GET /task/{task_id}/impact?operation=archive|unarchive|delete`.

## Process Preference Captured
Future requirement/design discussions should be written to a Markdown file in the repo (for example under `/design`) rather than only existing in chat text.

## Presentation Shaping Addendum
Date: 2026-02-08

To reduce API-side fanout and repeated joins while assembling task presentation payloads:
- Use a Postgres denormalized view (`tasks.task_presentation`) as the source for task detail rendering.
- Include user display fields, milestone display fields, project summaries, and task-adjacent record aggregates in that view.
- Keep canonical normalized tables as write sources; the view is read-oriented presentation infrastructure.

Extended follow-up:
- Apply the same denormalized presentation pattern to project and milestone read responses.
- Project reads should include owner display context (username/group display name) and aggregate counts from a presentation view.
- Milestone reads should include project display context and project-owner display context from a presentation view.
