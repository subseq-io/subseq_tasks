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

## Process Preference Captured
Future requirement/design discussions should be written to a Markdown file in the repo (for example under `/design`) rather than only existing in chat text.
