ALTER TABLE tasks.milestones
    ADD COLUMN IF NOT EXISTS next_milestone_id UUID REFERENCES tasks.milestones(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS previous_milestone_id UUID REFERENCES tasks.milestones(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_tasks_milestones_next_milestone_id
    ON tasks.milestones (next_milestone_id)
    WHERE next_milestone_id IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_milestones_previous_milestone_id
    ON tasks.milestones (previous_milestone_id)
    WHERE previous_milestone_id IS NOT NULL AND deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS tasks.task_attachments (
    task_id UUID NOT NULL REFERENCES tasks.tasks(id) ON DELETE CASCADE,
    file_id UUID NOT NULL,
    added_by_user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE RESTRICT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    PRIMARY KEY (task_id, file_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_task_attachments_task_id
    ON tasks.task_attachments (task_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_task_attachments_file_id
    ON tasks.task_attachments (file_id)
    WHERE deleted_at IS NULL;

DROP VIEW IF EXISTS tasks.milestone_presentation;

CREATE VIEW tasks.milestone_presentation AS
SELECT
    m.id,
    m.project_id,
    p.name AS project_name,
    p.slug AS project_slug,
    p.owner_user_id AS project_owner_user_id,
    owner_user.username AS project_owner_username,
    p.owner_group_id AS project_owner_group_id,
    owner_group.display_name AS project_owner_group_display_name,
    m.milestone_type,
    m.name,
    m.description,
    m.due_date,
    m.start_date,
    m.started,
    m.completed,
    m.completed_date,
    m.repeat_interval_seconds,
    m.repeat_end,
    m.repeat_schema,
    m.next_milestone_id,
    m.previous_milestone_id,
    m.metadata,
    m.created_at,
    m.updated_at
FROM tasks.milestones m
JOIN tasks.projects p
  ON p.id = m.project_id
 AND p.deleted_at IS NULL
LEFT JOIN auth.users owner_user
  ON owner_user.id = p.owner_user_id
LEFT JOIN auth.groups owner_group
  ON owner_group.id = p.owner_group_id
WHERE m.deleted_at IS NULL;
