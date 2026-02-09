# subseq_tasks

`subseq_tasks` uses scoped group roles from `subseq_auth` (`auth.group_roles`) to authorize group-member actions.

## Role Scopes

Role checks in this crate support three scopes:

- `tasks` with scope id `global`: app-wide permission for a group.
- `project` with scope id `<project_uuid>`: permission limited to one project.
- `task` with scope id `<task_uuid>`: permission limited to one task.

Checks are additive. A matching permission in any of the supported scopes grants access.

## Role Strings

The following hard-coded role names are defined in `/Users/teague/Subsequent/backend/subseq_tasks/src/permissions.rs`.

- `project_create`: create a project owned by a group.
- `project_read`: read/list project records.
- `project_update`: update project metadata and graph assignments.
- `project_delete`: soft-delete a project.
- `milestone_create`: create milestones in a project.
- `milestone_read`: read/list milestones.
- `milestone_update`: update milestones.
- `milestone_delete`: soft-delete milestones.
- `task_create`: create tasks in a project.
- `task_read`: read/list tasks.
- `task_update`: update task fields.
- `task_delete`: soft-delete tasks.
- `task_link`: create/delete task links.
- `task_transition`: move a task through the workflow state machine.

## Access Behavior

Write implies read for each domain boundary:

- Project reads accept any of: `project_read`, `project_create`, `project_update`, `project_delete`.
- Milestone reads accept any of: `milestone_read`, `milestone_create`, `milestone_update`, `milestone_delete`.
- Task reads accept any of: `task_read`, `task_create`, `task_update`, `task_delete`, `task_link`, `task_transition`.

## Ownership Rule

Task owners always have read/write access to their own tasks, even when group-role checks would otherwise deny access.

## Task Workflow

The task lifecycle is state-machine based:

- `open -> todo`
- `todo -> assigned | open | in_progress`
- `assigned -> in_progress | todo`
- `in_progress -> todo | acceptance`
- `acceptance -> in_progress`
- Any non-terminal state can short-circuit to `done` or `rejected`.

Transitions apply structured side effects:

- Assignment transitions set task assignee fields.
- Done transitions set completion fields (`completed_by_user_id`, `completed_at`).
- Rejected transitions set `rejected_reason`.
- Transition-only context (for example feedback or work-log details) is captured in task comments when provided.
- Every task mutation and transition is appended to `tasks.task_log`.

Task comments now support full lifecycle operations:

- `POST /task/{task_id}/comments` creates comments.
- `PUT /task/{task_id}/comments/{comment_id}` updates comments.
- `DELETE /task/{task_id}/comments/{comment_id}` soft-deletes comments.
- `GET /task/{task_id}/comments` lists active comments.

Comment create/update/delete operations are logged to `tasks.task_log`.

## Task Search And Filters

Task list supports both `/task` and `/task/list` with richer query fields:

- `query`: text match across title/description/slug.
- `projectId`: single project scope filter.
- `projectIds`: comma-delimited project UUID list scope filter.
- `order`: `created`, `updated`, `priority`, or `due_date`.
- `filterRule` + `filterRuleData`:
  - `archived`
  - `assignedTo` (requires `filterRuleData=<user_uuid>`)
  - `assignedToMe`
  - `closed`
  - `notClosed`
  - `created` (requires RFC3339 timestamp data)
  - `updated` (requires RFC3339 timestamp data)
  - `nodeId` (requires graph node UUID data)
  - `inProgress`
  - `open`

Filters apply in addition to existing permission boundaries.

## Task Attachments And Markdown Export

Attachments are tracked in `tasks.task_attachments`:

- `POST /task/{task_id}/attachment/{file_id}` attaches a file UUID to the task.
- `GET /task/{task_id}/attachment/list` lists task attachments.
- `DELETE /task/{task_id}/attachment/{file_id}` removes an attachment.

Markdown export is available at:

- `GET /task/{task_id}/export`
- `GET /task/{task_id}/export/markdown`

Task lookup for read/export supports UUID or slug references.

## Subtasks By Parent State

`subtask_of` edges persist `subtask_parent_state` in graph edge metadata so subtasks can be tied to a specific parent workflow state.

For non-`subtask_of` links, this metadata field is unset.

## Link Graph Constraints

Task links are stored per project in graph-backed layers (`tasks.project_link_graphs` + `tasks.task_link_graph_nodes`) and validated with `subseq_graph` invariants before writes:

- `subtask_of` is validated as a project-level forest modeled as a tree with a synthetic project-root node.
- `depends_on` is validated as a DAG.
- `related_to` and `assignment_order` are validated as directed graphs.

When tasks belong to multiple shared projects, link writes are mirrored logically across all shared projects and blocked if any project graph would violate constraints.

`subtask_of` add operations perform reparent semantics for the child node (full subtree migration behavior).

`subtask_of` delete operations allow detaching a subtree; detached nodes become top-level roots in the project forest model.

## Cascade Operations

Archive/delete operations cascade through `subtask_of` descendants:

- Archive cascades archived state through subtree.
- Unarchive cascades unarchive state and revalidates project graph integrity before reactivation.
- Delete cascades soft deletion through subtree and excises associated task links.

For UI confirmation flows, use:

- `GET /task/{task_id}/impact?operation=archive`
- `GET /task/{task_id}/impact?operation=unarchive`
- `GET /task/{task_id}/impact?operation=delete`

to retrieve affected task counts.

## Task Presentation View

Task details presentation is denormalized in Postgres view `tasks.task_presentation`.

The view pre-joins and aggregates:

- Usernames for task actors (`author`, `assignee`, `completed_by`) and comment/log actors.
- Milestone display fields (`milestone_name`, `milestone_type`).
- Project summaries and project IDs for the task.
- Task-adjacent tables (`task_graph_assignments`, `task_comments`, `task_log`) as JSON arrays.

Task link arrays are assembled from graph storage at read time after graph read-access checks.

`GET /task/{task_id}` reads from this view to assemble the details response in one DB query path after authorization.

## Project And Milestone Presentation Views

Project and milestone responses are also denormalized for UI-friendly presentation:

- `tasks.project_presentation` enriches projects with owner username/group display name and aggregate counts.
- `tasks.milestone_presentation` enriches milestones with project display context and project owner display fields.

`GET /project/{project_id}`, `GET /project`, `GET /milestone/{milestone_id}`, and `GET /milestone` read from these views after authorization checks.

Milestones now expose recurrence linkage fields:

- `nextMilestoneId`
- `previousMilestoneId`

Both fields are constrained to milestones inside the same project.

Milestone list now supports due-date range filtering:

- `dueStart`: include milestones with `dueDate >= dueStart`
- `dueEnd`: include milestones with `dueDate <= dueEnd`

If both are provided, `dueStart` must be earlier than or equal to `dueEnd`.

## Task Graph Permission Coupling

When task/project operations read or validate underlying graphs, this crate also enforces graph access via `subseq_graph` using `graph_read_access_roles()`.

This applies to:

- Project create/update graph assignment validation.
- Project read/list responses (graph IDs are part of the response model).
- Task creation entry-node resolution.
- Task details reads that include graph assignments.

## Permission Helpers

`/Users/teague/Subsequent/backend/subseq_tasks/src/permissions.rs` provides helper APIs for applications:

- Scope helpers:
  - `scope_tasks()`
  - `scope_project()`
  - `scope_task()`
  - `scope_id_global()`
- Role helpers (static string access):
  - `project_create()`, `project_read()`, `project_update()`, `project_delete()`
  - `milestone_create()`, `milestone_read()`, `milestone_update()`, `milestone_delete()`
  - `task_create()`, `task_read()`, `task_update()`, `task_delete()`, `task_link()`, `task_transition()`
- Access-role helpers:
  - `project_read_access_roles()`
  - `milestone_read_access_roles()`
  - `task_read_access_roles()`
  - `access_roles(permission)`
- Role-set helpers:
  - `read_permissions()`
  - `write_permissions()`
  - `full_permissions()`

## Usage Pattern

When assigning a permission for a group, create `auth.group_roles` entries for one of:

- `(group_id, 'tasks', 'global', '<role_name>')`
- `(group_id, 'project', '<project_uuid>', '<role_name>')`
- `(group_id, 'task', '<task_uuid>', '<role_name>')`

Use whichever scope granularity you need.

## Mutation Hooks

`TasksApp` exposes hooks so applications can react to project, milestone, and task mutations.

Trait methods:

- `on_project_update(project_id, actor_id, project_update)`
- `on_milestone_update(project_id, milestone_id, actor_id, milestone_update)`
- `on_task_update(project_id, task_id, actor_id, task_update)`

Project hook payloads:

- `ProjectUpdate::ProjectCreate { payload }`
- `ProjectUpdate::ProjectUpdated { payload }`
- `ProjectUpdate::ProjectArchive`

Milestone hook payloads:

- `MilestoneUpdate::MilestoneCreate { payload }`
- `MilestoneUpdate::MilestoneUpdated { payload }`
- `MilestoneUpdate::MilestoneArchive`

Task hook payloads:

- `TaskUpdate::TaskCreate { payload }`
- `TaskUpdate::TaskUpdated { payload }`
- `TaskUpdate::TaskArchive { payload }`
- `TaskUpdate::TaskUnarchive { payload }`
- `TaskUpdate::TaskDeleted`
- `TaskUpdate::TaskTransitioned { payload }`
- `TaskUpdate::TaskLinkCreated { payload }`
- `TaskUpdate::TaskLinkDeleted { other_task_id }`
- `TaskUpdate::TaskCommentCreated { comment_id }`
- `TaskUpdate::TaskCommentUpdated { comment_id }`
- `TaskUpdate::TaskCommentDeleted { comment_id }`
- `TaskUpdate::TaskAttachmentAdded { file_id }`
- `TaskUpdate::TaskAttachmentRemoved { file_id }`

Hooks are invoked after successful mutation handlers.
