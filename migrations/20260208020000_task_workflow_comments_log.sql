ALTER TABLE tasks.tasks
    DROP CONSTRAINT IF EXISTS tasks_tasks_state_check;
ALTER TABLE tasks.tasks
    DROP CONSTRAINT IF EXISTS tasks_state_check;

UPDATE tasks.tasks
SET state = 'done'
WHERE state = 'closed';

ALTER TABLE tasks.tasks
    ADD COLUMN IF NOT EXISTS completed_by_user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS rejected_reason TEXT;

ALTER TABLE tasks.tasks
    ADD CONSTRAINT tasks_tasks_state_check
    CHECK (state IN ('open', 'todo', 'assigned', 'in_progress', 'acceptance', 'done', 'rejected'));

CREATE INDEX IF NOT EXISTS idx_tasks_tasks_completed_by_user_id
    ON tasks.tasks (completed_by_user_id)
    WHERE completed_by_user_id IS NOT NULL AND deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS tasks.task_comments (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks.tasks(id) ON DELETE CASCADE,
    author_user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE RESTRICT,
    body TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    CONSTRAINT task_comments_body_not_empty CHECK (length(trim(body)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_tasks_task_comments_task_id
    ON tasks.task_comments (task_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_task_comments_author_user_id
    ON tasks.task_comments (author_user_id)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS tasks.task_log (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks.tasks(id) ON DELETE CASCADE,
    actor_user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE RESTRICT,
    action TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT task_log_from_state_valid CHECK (
        from_state IS NULL OR from_state IN ('open', 'todo', 'assigned', 'in_progress', 'acceptance', 'done', 'rejected')
    ),
    CONSTRAINT task_log_to_state_valid CHECK (
        to_state IS NULL OR to_state IN ('open', 'todo', 'assigned', 'in_progress', 'acceptance', 'done', 'rejected')
    )
);

CREATE INDEX IF NOT EXISTS idx_tasks_task_log_task_id_created_at
    ON tasks.task_log (task_id, created_at DESC);

ALTER TABLE tasks.task_links
    ADD COLUMN IF NOT EXISTS subtask_parent_state TEXT;

UPDATE tasks.task_links
SET subtask_parent_state = 'todo'
WHERE link_type = 'subtask_of'
  AND subtask_parent_state IS NULL;

ALTER TABLE tasks.task_links
    DROP CONSTRAINT IF EXISTS task_links_subtask_parent_state_valid;
ALTER TABLE tasks.task_links
    DROP CONSTRAINT IF EXISTS task_links_subtask_parent_state_required;
ALTER TABLE tasks.task_links
    DROP CONSTRAINT IF EXISTS task_links_non_subtask_parent_state_null;

ALTER TABLE tasks.task_links
    ADD CONSTRAINT task_links_subtask_parent_state_valid
    CHECK (
        subtask_parent_state IS NULL OR subtask_parent_state IN ('open', 'todo', 'assigned', 'in_progress', 'acceptance', 'done', 'rejected')
    );
ALTER TABLE tasks.task_links
    ADD CONSTRAINT task_links_subtask_parent_state_required
    CHECK (link_type <> 'subtask_of' OR subtask_parent_state IS NOT NULL);
ALTER TABLE tasks.task_links
    ADD CONSTRAINT task_links_non_subtask_parent_state_null
    CHECK (link_type = 'subtask_of' OR subtask_parent_state IS NULL);

CREATE INDEX IF NOT EXISTS idx_tasks_task_links_subtask_parent_state
    ON tasks.task_links (subtask_parent_state)
    WHERE link_type = 'subtask_of';
