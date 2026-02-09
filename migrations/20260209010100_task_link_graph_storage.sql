CREATE TABLE IF NOT EXISTS tasks.project_link_graphs (
    project_id UUID NOT NULL REFERENCES tasks.projects(id) ON DELETE CASCADE,
    link_type TEXT NOT NULL CHECK (link_type IN ('subtask_of', 'depends_on', 'related_to', 'assignment_order')),
    graph_id UUID NOT NULL REFERENCES graph.graphs(id) ON DELETE CASCADE,
    root_node_id UUID,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, link_type),
    UNIQUE (graph_id),
    CONSTRAINT project_link_graphs_subtask_root_required
        CHECK (link_type <> 'subtask_of' OR root_node_id IS NOT NULL),
    CONSTRAINT project_link_graphs_non_subtask_root_null
        CHECK (link_type = 'subtask_of' OR root_node_id IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_tasks_project_link_graphs_project_id
    ON tasks.project_link_graphs (project_id);

CREATE TABLE IF NOT EXISTS tasks.task_link_graph_nodes (
    project_id UUID NOT NULL,
    link_type TEXT NOT NULL CHECK (link_type IN ('subtask_of', 'depends_on', 'related_to', 'assignment_order')),
    task_id UUID NOT NULL REFERENCES tasks.tasks(id) ON DELETE CASCADE,
    node_id UUID NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, link_type, task_id),
    UNIQUE (project_id, link_type, node_id),
    CONSTRAINT task_link_graph_nodes_project_link_graph_fk
        FOREIGN KEY (project_id, link_type)
        REFERENCES tasks.project_link_graphs(project_id, link_type)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tasks_task_link_graph_nodes_task_id
    ON tasks.task_link_graph_nodes (task_id);
