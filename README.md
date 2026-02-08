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
- `task_transition`: change a task graph node assignment.

## Access Behavior

Write implies read for each domain boundary:

- Project reads accept any of: `project_read`, `project_create`, `project_update`, `project_delete`.
- Milestone reads accept any of: `milestone_read`, `milestone_create`, `milestone_update`, `milestone_delete`.
- Task reads accept any of: `task_read`, `task_create`, `task_update`, `task_delete`, `task_link`, `task_transition`.

## Ownership Rule

Task owners always have read/write access to their own tasks, even when group-role checks would otherwise deny access.

## Task Graph Permission Coupling

When task/project operations read or validate underlying graphs, this crate also enforces graph access via `subseq_graph` using `graph_read_access_roles()`.

This applies to:

- Project create/update graph assignment validation.
- Project read/list responses (graph IDs are part of the response model).
- Task creation entry-node resolution.
- Task details reads that include graph assignments.
- Task transitions that validate target graph nodes.

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
