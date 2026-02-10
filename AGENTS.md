# Agent Guidelines (subseq_tasks)

This file stores durable, repo-specific guardrails for subseq_tasks.

## Architecture
- Keep subseq_tasks on sqlx and maintain module seams by domain (`projects`, `milestones`, `tasks`) with shared helpers in each module root.
- Do not reintroduce legacy Diesel schema/model patterns or prompt-coupled task behavior.

## Authorization and Scope
- Keep task domain authorization on scoped roles (`project_*`, `milestone_*`, `task_*`) with explicit scope checks.
- Keep read access implied by relevant write roles in domain permission helpers.
- Keep task-author override behavior for own-task read/write paths where intentionally supported.
- Any task/project operation that reads or validates graph references must satisfy graph read access checks.

## Task Link Storage Contract
- Treat graph-backed storage as source of truth for task links.
- Keep `tasks.project_link_graphs` and `tasks.task_link_graph_nodes` as the mapping layer.
- Do not reintroduce runtime reads/writes to legacy `tasks.task_links`.
- Keep drop-legacy migration safeguards that fail fast if unmigrated legacy rows exist.

## Read Models and API Shaping
- Use presentation views for read-side denormalization (`tasks.task_presentation`, `tasks.project_presentation`, `tasks.milestone_presentation`).
- Keep normalized tables as write sources.

## Tests and Migrations
- Keep DB integration tests on isolated per-test databases via test harness.
- Preserve migration version uniqueness across cooperating crates (`subseq_auth`, `subseq_graph`, `subseq_tasks`) that share migration state.

## Design Governance
- For substantive architecture/requirements decisions, update a markdown artifact under `/design` in the same round.
