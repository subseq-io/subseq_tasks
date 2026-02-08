CREATE SCHEMA IF NOT EXISTS tasks;

CREATE TABLE IF NOT EXISTS tasks.projects (
    id UUID PRIMARY KEY,
    owner_user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    owner_group_id UUID REFERENCES auth.groups(id) ON DELETE SET NULL,
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    task_state_graph_id UUID NOT NULL REFERENCES graph.graphs(id) ON DELETE RESTRICT,
    task_graph_id UUID REFERENCES graph.graphs(id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tasks_projects_owner_user_id
    ON tasks.projects (owner_user_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_projects_owner_group_id
    ON tasks.projects (owner_group_id)
    WHERE owner_group_id IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_projects_task_state_graph_id
    ON tasks.projects (task_state_graph_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_projects_slug
    ON tasks.projects (slug)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS tasks.milestones (
    id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES tasks.projects(id) ON DELETE CASCADE,
    milestone_type TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    due_date TIMESTAMP,
    start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started BOOLEAN NOT NULL DEFAULT FALSE,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    completed_date TIMESTAMP,
    repeat_interval_seconds BIGINT,
    repeat_end TIMESTAMP,
    repeat_schema JSONB,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tasks_milestones_project_id
    ON tasks.milestones (project_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_milestones_due_date
    ON tasks.milestones (due_date)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS tasks.tasks (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    author_user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE RESTRICT,
    assignee_user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    due_date TIMESTAMP,
    milestone_id UUID REFERENCES tasks.milestones(id) ON DELETE SET NULL,
    state TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'in_progress', 'closed')),
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tasks_tasks_author_user_id
    ON tasks.tasks (author_user_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_tasks_assignee_user_id
    ON tasks.tasks (assignee_user_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_tasks_state
    ON tasks.tasks (state)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_tasks_updated_at
    ON tasks.tasks (updated_at DESC)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS tasks.task_projects (
    task_id UUID NOT NULL REFERENCES tasks.tasks(id) ON DELETE CASCADE,
    project_id UUID NOT NULL REFERENCES tasks.projects(id) ON DELETE CASCADE,
    order_added INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (task_id, project_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_task_projects_project_id
    ON tasks.task_projects (project_id);

CREATE TABLE IF NOT EXISTS tasks.task_graph_assignments (
    task_id UUID NOT NULL REFERENCES tasks.tasks(id) ON DELETE CASCADE,
    graph_id UUID NOT NULL REFERENCES graph.graphs(id) ON DELETE CASCADE,
    current_node_id UUID,
    order_added INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (task_id, graph_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_task_graph_assignments_graph_id
    ON tasks.task_graph_assignments (graph_id);

CREATE TABLE IF NOT EXISTS tasks.task_links (
    task_from_id UUID NOT NULL REFERENCES tasks.tasks(id) ON DELETE CASCADE,
    task_to_id UUID NOT NULL REFERENCES tasks.tasks(id) ON DELETE CASCADE,
    link_type TEXT NOT NULL CHECK (link_type IN ('subtask_of', 'depends_on', 'related_to', 'assignment_order')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (task_from_id, task_to_id, link_type),
    CONSTRAINT task_links_not_self CHECK (task_from_id <> task_to_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_task_links_from
    ON tasks.task_links (task_from_id);
CREATE INDEX IF NOT EXISTS idx_tasks_task_links_to
    ON tasks.task_links (task_to_id);
