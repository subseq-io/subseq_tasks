# Task Update Hook Trait (2026-02-09)

## Objective

Expose a trait-level hook so host applications can subscribe to task mutations without coupling directly to DB internals.

## Contract

`TasksApp` exposes:

- `on_task_update(project_id, task_id, actor_id, task_update)`

Arguments:

- `project_id: ProjectId`
- `task_id: TaskId`
- `actor_id: UserId`
- `task_update: TaskUpdate`

`TaskUpdate` variants represent mutation classes:

- `TaskCreated`
- `TaskUpdated`
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

- Hook is called only after successful task mutation operations.
- Emission is deduplicated and performed once per project associated with the task.
- Read/list endpoints do not emit events.

## Integration Notes

- The default `TasksApp::on_task_update` implementation is no-op and returns `Ok(())`.
- Host apps can override this method to publish events to queues, websocket buses, analytics, or audit sinks.
