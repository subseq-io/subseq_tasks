DROP VIEW IF EXISTS tasks.task_presentation;

CREATE VIEW tasks.task_presentation AS
SELECT
    t.id,
    t.slug,
    t.title,
    t.description,
    t.author_user_id,
    author_user.username AS author_username,
    t.assignee_user_id,
    assignee_user.username AS assignee_username,
    t.priority,
    t.due_date,
    t.milestone_id,
    m.name AS milestone_name,
    m.milestone_type AS milestone_type,
    t.state,
    t.archived,
    t.completed_by_user_id,
    completed_user.username AS completed_by_username,
    t.completed_at,
    t.rejected_reason,
    t.metadata,
    t.created_at,
    t.updated_at,
    COALESCE(project_ids.project_ids, ARRAY[]::uuid[]) AS project_ids,
    COALESCE(project_details.projects, '[]'::jsonb) AS projects,
    COALESCE(graph_details.graph_assignments, '[]'::jsonb) AS graph_assignments,
    COALESCE(links_out.links_out, '[]'::jsonb) AS links_out,
    COALESCE(links_in.links_in, '[]'::jsonb) AS links_in,
    COALESCE(comment_details.comments, '[]'::jsonb) AS comments,
    COALESCE(log_details.log, '[]'::jsonb) AS log
FROM tasks.tasks t
LEFT JOIN auth.users author_user
  ON author_user.id = t.author_user_id
LEFT JOIN auth.users assignee_user
  ON assignee_user.id = t.assignee_user_id
LEFT JOIN auth.users completed_user
  ON completed_user.id = t.completed_by_user_id
LEFT JOIN tasks.milestones m
  ON m.id = t.milestone_id
 AND m.deleted_at IS NULL
LEFT JOIN LATERAL (
    SELECT ARRAY_AGG(tp.project_id ORDER BY tp.order_added ASC, tp.project_id ASC) AS project_ids
    FROM tasks.task_projects tp
    JOIN tasks.projects p
      ON p.id = tp.project_id
     AND p.deleted_at IS NULL
    WHERE tp.task_id = t.id
) project_ids ON TRUE
LEFT JOIN LATERAL (
    SELECT JSONB_AGG(
               JSONB_BUILD_OBJECT(
                   'id', p.id,
                   'name', p.name,
                   'slug', p.slug,
                   'orderAdded', tp.order_added
               )
               ORDER BY tp.order_added ASC, p.id ASC
           ) AS projects
    FROM tasks.task_projects tp
    JOIN tasks.projects p
      ON p.id = tp.project_id
     AND p.deleted_at IS NULL
    WHERE tp.task_id = t.id
) project_details ON TRUE
LEFT JOIN LATERAL (
    SELECT JSONB_AGG(
               JSONB_BUILD_OBJECT(
                   'graphId', tga.graph_id,
                   'currentNodeId', tga.current_node_id,
                   'orderAdded', tga.order_added,
                   'updatedAt', tga.updated_at
               )
               ORDER BY tga.order_added ASC, tga.graph_id ASC
           ) AS graph_assignments
    FROM tasks.task_graph_assignments tga
    WHERE tga.task_id = t.id
) graph_details ON TRUE
LEFT JOIN LATERAL (
    SELECT JSONB_AGG(
               JSONB_BUILD_OBJECT(
                   'taskFromId', tl.task_from_id,
                   'taskToId', tl.task_to_id,
                   'linkType', tl.link_type,
                   'subtaskParentState', tl.subtask_parent_state,
                   'otherTaskTitle', linked.title,
                   'createdAt', tl.created_at
               )
               ORDER BY tl.created_at DESC, tl.task_to_id ASC
           ) AS links_out
    FROM tasks.task_links tl
    JOIN tasks.tasks linked
      ON linked.id = tl.task_to_id
     AND linked.deleted_at IS NULL
    WHERE tl.task_from_id = t.id
) links_out ON TRUE
LEFT JOIN LATERAL (
    SELECT JSONB_AGG(
               JSONB_BUILD_OBJECT(
                   'taskFromId', tl.task_from_id,
                   'taskToId', tl.task_to_id,
                   'linkType', tl.link_type,
                   'subtaskParentState', tl.subtask_parent_state,
                   'otherTaskTitle', linked.title,
                   'createdAt', tl.created_at
               )
               ORDER BY tl.created_at DESC, tl.task_from_id ASC
           ) AS links_in
    FROM tasks.task_links tl
    JOIN tasks.tasks linked
      ON linked.id = tl.task_from_id
     AND linked.deleted_at IS NULL
    WHERE tl.task_to_id = t.id
) links_in ON TRUE
LEFT JOIN LATERAL (
    SELECT JSONB_AGG(
               JSONB_BUILD_OBJECT(
                   'id', tc.id,
                   'taskId', tc.task_id,
                   'authorUserId', tc.author_user_id,
                   'authorUsername', comment_author.username,
                   'body', tc.body,
                   'metadata', tc.metadata,
                   'createdAt', tc.created_at,
                   'updatedAt', tc.updated_at
               )
               ORDER BY tc.created_at ASC, tc.id ASC
           ) AS comments
    FROM tasks.task_comments tc
    LEFT JOIN auth.users comment_author
      ON comment_author.id = tc.author_user_id
    WHERE tc.task_id = t.id
      AND tc.deleted_at IS NULL
) comment_details ON TRUE
LEFT JOIN LATERAL (
    SELECT JSONB_AGG(
               JSONB_BUILD_OBJECT(
                   'id', tlg.id,
                   'taskId', tlg.task_id,
                   'actorUserId', tlg.actor_user_id,
                   'actorUsername', log_actor.username,
                   'action', tlg.action,
                   'fromState', tlg.from_state,
                   'toState', tlg.to_state,
                   'details', tlg.details,
                   'createdAt', tlg.created_at
               )
               ORDER BY tlg.created_at ASC, tlg.id ASC
           ) AS log
    FROM tasks.task_log tlg
    LEFT JOIN auth.users log_actor
      ON log_actor.id = tlg.actor_user_id
    WHERE tlg.task_id = t.id
) log_details ON TRUE
WHERE t.deleted_at IS NULL;
