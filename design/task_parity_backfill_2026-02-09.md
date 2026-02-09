# Task/Milestone Parity Backfill (2026-02-09)

## Objective

Close parity gaps from the legacy task and milestone surfaces while preserving the new `sqlx`/scoped-role/graph-SOT architecture.

## Implemented

1. Task read parity
- Task get/read now supports UUID-or-slug lookup (`/task/{task_id}` read path accepts either).
- Markdown export restored:
  - `GET /task/{task_id}/export`
  - `GET /task/{task_id}/export/markdown`

2. Task comments parity
- Added comment update/delete APIs:
  - `PUT /task/{task_id}/comments/{comment_id}`
  - `DELETE /task/{task_id}/comments/{comment_id}`
- Comment create/update/delete actions append task-log entries.

3. Task search/filter parity
- Expanded list query surface for `/task` and `/task/list`:
  - `query`, `projectId`, `projectIds`, `order`
  - `filterRule` + `filterRuleData` parsing in API model
- Supported filters:
  - `archived`
  - `assignedTo`
  - `assignedToMe`
  - `closed`
  - `notClosed`
  - `created`
  - `updated`
  - `nodeId`
  - `inProgress`
  - `open`

4. Attachments parity
- Added task attachment storage table: `tasks.task_attachments`.
- Added attachment endpoints:
  - `POST /task/{task_id}/attachment/{file_id}`
  - `GET /task/{task_id}/attachment/list`
  - `DELETE /task/{task_id}/attachment/{file_id}`

5. Milestone parity
- Added milestone recurrence link fields:
  - `nextMilestoneId`
  - `previousMilestoneId`
- Added validation that linked milestones remain within the same project.
- Expanded milestone list query with text search + due-date filter.

## Migration Notes

- Added migration: `20260209020100_task_attachments_and_milestone_links.sql`
  - Adds milestone link columns + indexes.
  - Creates `tasks.task_attachments`.
  - Rebuilds `tasks.milestone_presentation` to include new fields.

## Non-Goals

- Legacy external integrations and orchestration surfaces (run/replan/etc.) remain out of scope.
- Task link graph authority remains in `subseq_graph` integration paths already implemented in prior rounds.
