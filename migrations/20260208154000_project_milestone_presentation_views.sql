DROP VIEW IF EXISTS tasks.project_presentation;
DROP VIEW IF EXISTS tasks.milestone_presentation;

CREATE VIEW tasks.project_presentation AS
SELECT
    p.id,
    p.owner_user_id,
    owner_user.username AS owner_username,
    p.owner_group_id,
    owner_group.display_name AS owner_group_display_name,
    p.name,
    p.slug,
    p.description,
    p.task_state_graph_id,
    p.task_graph_id,
    p.metadata,
    p.created_at,
    p.updated_at,
    COALESCE(task_counts.task_count, 0) AS task_count,
    COALESCE(milestone_counts.milestone_count, 0) AS milestone_count
FROM tasks.projects p
LEFT JOIN auth.users owner_user
  ON owner_user.id = p.owner_user_id
LEFT JOIN auth.groups owner_group
  ON owner_group.id = p.owner_group_id
LEFT JOIN (
    SELECT
        tp.project_id,
        COUNT(DISTINCT tp.task_id)::bigint AS task_count
    FROM tasks.task_projects tp
    JOIN tasks.tasks t
      ON t.id = tp.task_id
     AND t.deleted_at IS NULL
    GROUP BY tp.project_id
) task_counts
  ON task_counts.project_id = p.id
LEFT JOIN (
    SELECT
        m.project_id,
        COUNT(*)::bigint AS milestone_count
    FROM tasks.milestones m
    WHERE m.deleted_at IS NULL
    GROUP BY m.project_id
) milestone_counts
  ON milestone_counts.project_id = p.id
WHERE p.deleted_at IS NULL;

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
