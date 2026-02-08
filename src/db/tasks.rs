use super::*;

async fn entry_node_for_graph(
    pool: &PgPool,
    actor: UserId,
    graph_id: GraphId,
) -> Result<Option<GraphNodeId>> {
    let graph = get_graph(pool, actor, graph_id, graph_perm::graph_read_access_roles())
        .await
        .map_err(LibError::from)?;
    Ok(graph.nodes.first().map(|node| node.id))
}

async fn validate_milestone_access_for_task(
    pool: &PgPool,
    actor: UserId,
    milestone_id: MilestoneId,
) -> Result<()> {
    let _ = load_accessible_milestone(pool, actor, milestone_id, perm::milestone_read()).await?;
    Ok(())
}

pub async fn create_task_with_roles(
    pool: &PgPool,
    actor: UserId,
    payload: CreateTaskPayload,
) -> Result<TaskDetails> {
    let project =
        load_accessible_project(pool, actor, payload.project_id, perm::task_create()).await?;

    let title = normalize_task_title(&payload.title)?;
    let description = normalize_description(payload.description);

    if let Some(milestone_id) = payload.milestone_id {
        validate_milestone_access_for_task(pool, actor, milestone_id).await?;
    }

    let task_id = TaskId(Uuid::new_v4());
    let state = payload.state.unwrap_or(TaskState::Open);
    let metadata = payload.metadata.unwrap_or_else(|| json!({}));

    let mut graph_assignments: Vec<(GraphId, Option<GraphNodeId>, i32)> = Vec::new();
    let state_graph_id = GraphId(project.task_state_graph_id);
    graph_assignments.push((
        state_graph_id,
        entry_node_for_graph(pool, actor, state_graph_id).await?,
        0,
    ));

    if let Some(task_graph_id) = project.task_graph_id {
        let task_graph_id = GraphId(task_graph_id);
        if task_graph_id != state_graph_id {
            graph_assignments.push((
                task_graph_id,
                entry_node_for_graph(pool, actor, task_graph_id).await?,
                1,
            ));
        }
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|err| db_err("Failed to start transaction", err))?;

    sqlx::query(
        r#"
        SELECT id
        FROM tasks.projects
        WHERE id = $1
          AND deleted_at IS NULL
        FOR UPDATE
        "#,
    )
    .bind(payload.project_id.0)
    .execute(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to lock project", err))?;

    let row: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*)::bigint
        FROM tasks.task_projects tp
        JOIN tasks.tasks t
          ON t.id = tp.task_id
        WHERE tp.project_id = $1
          AND t.deleted_at IS NULL
        "#,
    )
    .bind(payload.project_id.0)
    .fetch_one(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to build project task slug", err))?;

    let order_added = (row.0 + 1) as i32;
    let slug = format!("{}-{}", project.slug, order_added);

    sqlx::query(
        r#"
        INSERT INTO tasks.tasks (
            id,
            slug,
            title,
            description,
            author_user_id,
            assignee_user_id,
            priority,
            due_date,
            milestone_id,
            state,
            archived,
            metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, FALSE, $11)
        "#,
    )
    .bind(task_id.0)
    .bind(slug)
    .bind(title)
    .bind(description)
    .bind(actor.0)
    .bind(payload.assignee_user_id.map(|id| id.0))
    .bind(payload.priority.unwrap_or(0))
    .bind(payload.due_date.map(|value| value.naive_utc()))
    .bind(payload.milestone_id.map(|id| id.0))
    .bind(state.as_str())
    .bind(metadata)
    .execute(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to create task", err))?;

    sqlx::query(
        r#"
        INSERT INTO tasks.task_projects (task_id, project_id, order_added)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(task_id.0)
    .bind(payload.project_id.0)
    .bind(order_added)
    .execute(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to attach task to project", err))?;

    for (graph_id, current_node_id, order) in graph_assignments {
        sqlx::query(
            r#"
            INSERT INTO tasks.task_graph_assignments (
                task_id,
                graph_id,
                current_node_id,
                order_added
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (task_id, graph_id)
            DO UPDATE SET
                current_node_id = EXCLUDED.current_node_id,
                order_added = EXCLUDED.order_added,
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(task_id.0)
        .bind(graph_id.0)
        .bind(current_node_id.map(|id| id.0))
        .bind(order)
        .execute(&mut *tx)
        .await
        .map_err(|err| db_err("Failed to attach task graph assignment", err))?;
    }

    tx.commit()
        .await
        .map_err(|err| db_err("Failed to commit transaction", err))?;

    get_task_with_roles(pool, actor, task_id).await
}

pub async fn get_task_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
) -> Result<TaskDetails> {
    let row = load_accessible_task(pool, actor, task_id, perm::task_read()).await?;
    let task = to_task(row)?;
    let project_ids = get_task_project_ids(pool, task_id).await?;
    let graph_assignments = get_task_graph_assignments(pool, task_id).await?;
    ensure_task_assignment_graph_access(pool, actor, &graph_assignments).await?;
    let links_out = get_task_links_out(pool, task_id).await?;
    let links_in = get_task_links_in(pool, task_id).await?;

    Ok(TaskDetails {
        task,
        project_ids,
        graph_assignments,
        links_out,
        links_in,
    })
}

pub async fn list_tasks_with_roles(
    pool: &PgPool,
    actor: UserId,
    project_id: Option<ProjectId>,
    assignee_user_id: Option<UserId>,
    state: Option<TaskState>,
    archived: Option<bool>,
    page: u32,
    limit: u32,
) -> Result<Paged<Task>> {
    let allowed_project_ids = accessible_project_ids(pool, actor, perm::task_read()).await?;

    let offset = (page.saturating_sub(1) as i64).saturating_mul(limit as i64);

    let rows = sqlx::query_as::<_, TaskRow>(
        r#"
        SELECT
            t.id,
            t.slug,
            t.title,
            t.description,
            t.author_user_id,
            t.assignee_user_id,
            t.priority,
            t.due_date,
            t.milestone_id,
            t.state,
            t.archived,
            t.metadata,
            t.created_at,
            t.updated_at
        FROM tasks.tasks t
        WHERE t.deleted_at IS NULL
          AND (
              t.author_user_id = $2
              OR EXISTS (
                  SELECT 1
                  FROM tasks.task_projects tp
                  WHERE tp.task_id = t.id
                    AND tp.project_id = ANY($1)
              )
          )
          AND (
              $3::uuid IS NULL
              OR EXISTS (
                  SELECT 1
                  FROM tasks.task_projects tp2
                  WHERE tp2.task_id = t.id
                    AND tp2.project_id = $3
              )
          )
          AND ($4::uuid IS NULL OR t.assignee_user_id = $4)
          AND ($5::text IS NULL OR t.state = $5)
          AND ($6::bool IS NULL OR t.archived = $6)
        ORDER BY t.updated_at DESC, t.id DESC
        LIMIT $7 OFFSET $8
        "#,
    )
    .bind(&allowed_project_ids)
    .bind(actor.0)
    .bind(project_id.map(|id| id.0))
    .bind(assignee_user_id.map(|id| id.0))
    .bind(state.map(|value| value.as_str()))
    .bind(archived)
    .bind(limit as i64)
    .bind(offset)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to list tasks", err))?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(to_task(row)?);
    }

    Ok(Paged { page, limit, items })
}

pub async fn update_task_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    payload: UpdateTaskPayload,
) -> Result<TaskDetails> {
    let existing = load_accessible_task(pool, actor, task_id, perm::task_update()).await?;

    let title = payload
        .title
        .map(|value| normalize_task_title(&value))
        .transpose()?
        .unwrap_or(existing.title);

    let description = payload
        .description
        .map(|value| value.trim().to_string())
        .unwrap_or(existing.description);

    let assignee_user_id = if payload.clear_assignee.unwrap_or(false) {
        None
    } else {
        payload
            .assignee_user_id
            .map(|id| id.0)
            .or(existing.assignee_user_id)
    };

    let due_date = if payload.clear_due_date.unwrap_or(false) {
        None
    } else {
        payload
            .due_date
            .map(|value| value.naive_utc())
            .or(existing.due_date)
    };

    let milestone_id = if payload.clear_milestone.unwrap_or(false) {
        None
    } else {
        payload
            .milestone_id
            .map(|id| id.0)
            .or(existing.milestone_id)
    };

    if let Some(milestone_id) = milestone_id {
        validate_milestone_access_for_task(pool, actor, MilestoneId(milestone_id)).await?;
    }

    let state = payload
        .state
        .map(|value| value.as_str().to_string())
        .unwrap_or(existing.state);

    let archived = payload.archived.unwrap_or(existing.archived);
    let metadata = payload.metadata.unwrap_or(existing.metadata);

    sqlx::query(
        r#"
        UPDATE tasks.tasks
        SET
            title = $1,
            description = $2,
            assignee_user_id = $3,
            priority = $4,
            due_date = $5,
            milestone_id = $6,
            state = $7,
            archived = $8,
            metadata = $9,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $10
          AND deleted_at IS NULL
        "#,
    )
    .bind(title)
    .bind(description)
    .bind(assignee_user_id)
    .bind(payload.priority.unwrap_or(existing.priority))
    .bind(due_date)
    .bind(milestone_id)
    .bind(state)
    .bind(archived)
    .bind(metadata)
    .bind(task_id.0)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to update task", err))?;

    get_task_with_roles(pool, actor, task_id).await
}

pub async fn delete_task_with_roles(pool: &PgPool, actor: UserId, task_id: TaskId) -> Result<()> {
    load_accessible_task(pool, actor, task_id, perm::task_delete()).await?;

    sqlx::query(
        r#"
        UPDATE tasks.tasks
        SET
            deleted_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
          AND deleted_at IS NULL
        "#,
    )
    .bind(task_id.0)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to delete task", err))?;

    Ok(())
}

pub async fn create_task_link_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    payload: CreateTaskLinkPayload,
) -> Result<TaskLink> {
    load_accessible_task(pool, actor, task_id, perm::task_link()).await?;
    load_accessible_task(pool, actor, payload.other_task_id, perm::task_link()).await?;

    if task_id == payload.other_task_id {
        return Err(LibError::invalid(
            "Tasks cannot link to themselves",
            anyhow!("attempted self-link for task {task_id}"),
        ));
    }

    sqlx::query(
        r#"
        INSERT INTO tasks.task_links (
            task_from_id,
            task_to_id,
            link_type
        )
        VALUES ($1, $2, $3)
        ON CONFLICT (task_from_id, task_to_id, link_type)
        DO NOTHING
        "#,
    )
    .bind(task_id.0)
    .bind(payload.other_task_id.0)
    .bind(payload.link_type.as_db_value())
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to create task link", err))?;

    let row = sqlx::query_as::<_, TaskLinkRow>(
        r#"
        SELECT
            task_from_id,
            task_to_id,
            link_type,
            created_at
        FROM tasks.task_links
        WHERE task_from_id = $1
          AND task_to_id = $2
          AND link_type = $3
        LIMIT 1
        "#,
    )
    .bind(task_id.0)
    .bind(payload.other_task_id.0)
    .bind(payload.link_type.as_db_value())
    .fetch_one(pool)
    .await
    .map_err(|err| db_err("Failed to fetch task link", err))?;

    to_task_link(row)
}

pub async fn delete_task_links_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    other_task_id: TaskId,
) -> Result<()> {
    load_accessible_task(pool, actor, task_id, perm::task_link()).await?;
    load_accessible_task(pool, actor, other_task_id, perm::task_link()).await?;

    sqlx::query(
        r#"
        DELETE FROM tasks.task_links
        WHERE (task_from_id = $1 AND task_to_id = $2)
           OR (task_from_id = $2 AND task_to_id = $1)
        "#,
    )
    .bind(task_id.0)
    .bind(other_task_id.0)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to delete task links", err))?;

    Ok(())
}

pub async fn transition_task_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    payload: TransitionTaskPayload,
) -> Result<TaskDetails> {
    load_accessible_task(pool, actor, task_id, perm::task_transition()).await?;

    let assignments = get_task_graph_assignments(pool, task_id).await?;
    let graph_id = payload
        .graph_id
        .or_else(|| assignments.first().map(|a| a.graph_id));
    let graph_id = graph_id.ok_or_else(|| {
        LibError::invalid(
            "Task has no graph assignment",
            anyhow!("task {task_id} has no graph assignments"),
        )
    })?;

    let graph = get_graph(pool, actor, graph_id, graph_perm::graph_read_access_roles())
        .await
        .map_err(LibError::from)?;

    let node_exists = graph.nodes.iter().any(|node| node.id == payload.node_id);
    if !node_exists {
        return Err(LibError::invalid(
            "Node does not belong to this graph",
            anyhow!("node {} is not in graph {}", payload.node_id, graph_id),
        ));
    }

    let default_order = assignments
        .iter()
        .find(|assignment| assignment.graph_id == graph_id)
        .map(|assignment| assignment.order_added)
        .unwrap_or(0);

    sqlx::query(
        r#"
        INSERT INTO tasks.task_graph_assignments (
            task_id,
            graph_id,
            current_node_id,
            order_added
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (task_id, graph_id)
        DO UPDATE SET
            current_node_id = EXCLUDED.current_node_id,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(task_id.0)
    .bind(graph_id.0)
    .bind(payload.node_id.0)
    .bind(default_order)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to transition task", err))?;

    get_task_with_roles(pool, actor, task_id).await
}
