# Mutation Hook Trait (2026-02-09)

## Objective

Expose trait-level hooks so host applications can subscribe to project, milestone, and task mutations without coupling directly to DB internals.

## Contract

`TasksApp` exposes:

- `on_project_update(project_id, actor_id, project_update)`
- `on_milestone_update(project_id, milestone_id, actor_id, milestone_update)`
- `on_task_update(project_id, task_id, actor_id, task_update)`

Arguments:

- `project_id: ProjectId`
- `project_update: ProjectUpdate`
- `milestone_id: MilestoneId`
- `milestone_update: MilestoneUpdate`
- `task_id: TaskId`
- `actor_id: UserId`
- `task_update: TaskUpdate`

`ProjectUpdate` variants represent project mutation classes:

- `ProjectCreate`
- `ProjectUpdated`
- `ProjectArchive`

`MilestoneUpdate` variants represent milestone mutation classes:

- `MilestoneCreate`
- `MilestoneUpdated`
- `MilestoneArchive`

`TaskUpdate` variants represent mutation classes:

- `TaskCreate`
- `TaskUpdated`
- `TaskArchive`
- `TaskUnarchive`
- `TaskDeleted`
- `TaskTransitioned`
- `TaskLinkCreated`
- `TaskLinkDeleted`
- `TaskCommentCreated`
- `TaskCommentUpdated`
- `TaskCommentDeleted`
- `TaskAttachmentAdded`
- `TaskAttachmentRemoved`

## Emission Behavior

- Hooks are called only after successful mutation operations.
- Project and milestone delete endpoints emit archive-class events because those resources are soft-deleted.
- Milestone list adds due range filtering with `dueStart`/`dueEnd` (`dueDate >= dueStart && dueDate <= dueEnd`).
- Emission is deduplicated and performed once per project associated with the task.
- Read/list endpoints do not emit events.

## Integration Notes

- The default `TasksApp::on_project_update`, `TasksApp::on_milestone_update`, and `TasksApp::on_task_update` implementations are no-op and return `Ok(())`.
- Host apps can override this method to publish events to queues, websocket buses, analytics, or audit sinks.
