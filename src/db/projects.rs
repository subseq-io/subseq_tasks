use super::*;

pub async fn create_project_with_roles(
    pool: &PgPool,
    actor: UserId,
    payload: CreateProjectPayload,
) -> Result<Project> {
    let name = normalize_name(&payload.name, "Project")?;
    let description = normalize_description(payload.description);
    let owner_group_id = payload.owner_group_id;

    if let Some(group_id) = owner_group_id {
        ensure_group_permission_for_new_resource(pool, actor, group_id, perm::project_create())
            .await?;
    }

    get_graph(
        pool,
        actor,
        payload.task_state_graph_id,
        graph_perm::graph_read_access_roles(),
    )
    .await
    .map_err(LibError::from)?;

    if let Some(task_graph_id) = payload.task_graph_id {
        get_graph(
            pool,
            actor,
            task_graph_id,
            graph_perm::graph_read_access_roles(),
        )
        .await
        .map_err(LibError::from)?;
    }

    let project_id = ProjectId(Uuid::new_v4());
    let slug = slug_from_name(&name);
    let metadata = payload.metadata.unwrap_or_else(|| json!({}));

    sqlx::query(
        r#"
        INSERT INTO tasks.projects (
            id,
            owner_user_id,
            owner_group_id,
            name,
            slug,
            description,
            task_state_graph_id,
            task_graph_id,
            metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(project_id.0)
    .bind(actor.0)
    .bind(owner_group_id.map(|id| id.0))
    .bind(name)
    .bind(slug)
    .bind(description)
    .bind(payload.task_state_graph_id.0)
    .bind(payload.task_graph_id.map(|id| id.0))
    .bind(metadata)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to create project", err))?;

    get_project_with_roles(pool, actor, project_id).await
}

pub async fn get_project_with_roles(
    pool: &PgPool,
    actor: UserId,
    project_id: ProjectId,
) -> Result<Project> {
    let row = load_accessible_project(pool, actor, project_id, perm::project_read()).await?;
    ensure_project_graph_access(
        pool,
        actor,
        GraphId(row.task_state_graph_id),
        row.task_graph_id.map(GraphId),
    )
    .await?;
    Ok(to_project(row))
}

pub async fn list_projects_with_roles(
    pool: &PgPool,
    actor: UserId,
    page: u32,
    limit: u32,
) -> Result<Paged<ProjectSummary>> {
    let offset = (page.saturating_sub(1) as i64).saturating_mul(limit as i64);
    let allowed_project_ids = accessible_project_ids(pool, actor, perm::project_read()).await?;
    if allowed_project_ids.is_empty() {
        return Ok(Paged {
            page,
            limit,
            items: Vec::new(),
        });
    }

    let rows = sqlx::query_as::<_, ProjectSummaryRow>(
        r#"
        SELECT
            p.id,
            p.owner_user_id,
            p.owner_group_id,
            p.name,
            p.slug,
            p.description,
            p.task_state_graph_id,
            p.task_graph_id,
            p.created_at,
            p.updated_at,
            COALESCE(task_counts.task_count, 0) AS task_count,
            COALESCE(milestone_counts.milestone_count, 0) AS milestone_count
        FROM tasks.projects p
        LEFT JOIN (
            SELECT
                tp.project_id,
                COUNT(DISTINCT tp.task_id)::bigint AS task_count
            FROM tasks.task_projects tp
            JOIN tasks.tasks t
              ON t.id = tp.task_id
            WHERE t.deleted_at IS NULL
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
        WHERE p.deleted_at IS NULL
          AND p.id = ANY($1)
        ORDER BY p.updated_at DESC, p.id DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(&allowed_project_ids)
    .bind(limit as i64)
    .bind(offset)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to list projects", err))?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let access = ensure_project_graph_access(
            pool,
            actor,
            GraphId(row.task_state_graph_id),
            row.task_graph_id.map(GraphId),
        )
        .await;
        match access {
            Ok(()) => items.push(to_project_summary(row)),
            Err(err) if err.kind == ErrorKind::Forbidden => continue,
            Err(err) => return Err(err),
        }
    }

    Ok(Paged { page, limit, items })
}

pub async fn update_project_with_roles(
    pool: &PgPool,
    actor: UserId,
    project_id: ProjectId,
    payload: UpdateProjectPayload,
) -> Result<Project> {
    let existing = load_accessible_project(pool, actor, project_id, perm::project_update()).await?;

    let name = match payload.name {
        Some(name) => normalize_name(&name, "Project")?,
        None => existing.name,
    };

    let description = payload
        .description
        .map(|desc| desc.trim().to_string())
        .unwrap_or(existing.description);

    let owner_group_id = if payload.clear_owner_group.unwrap_or(false) {
        None
    } else {
        payload
            .owner_group_id
            .map(|id| id.0)
            .or(existing.owner_group_id)
    };

    if let Some(group_id) = owner_group_id {
        ensure_group_permission_for_new_resource(
            pool,
            actor,
            GroupId(group_id),
            perm::project_update(),
        )
        .await?;
    }

    let task_state_graph_id = payload
        .task_state_graph_id
        .map(|id| id.0)
        .unwrap_or(existing.task_state_graph_id);

    get_graph(
        pool,
        actor,
        GraphId(task_state_graph_id),
        graph_perm::graph_read_access_roles(),
    )
    .await
    .map_err(LibError::from)?;

    let task_graph_id = if payload.clear_task_graph.unwrap_or(false) {
        None
    } else {
        payload
            .task_graph_id
            .map(|id| id.0)
            .or(existing.task_graph_id)
    };

    if let Some(graph_id) = task_graph_id {
        get_graph(
            pool,
            actor,
            GraphId(graph_id),
            graph_perm::graph_read_access_roles(),
        )
        .await
        .map_err(LibError::from)?;
    }

    let metadata = payload.metadata.unwrap_or(existing.metadata);

    sqlx::query(
        r#"
        UPDATE tasks.projects
        SET
            name = $1,
            description = $2,
            owner_group_id = $3,
            task_state_graph_id = $4,
            task_graph_id = $5,
            metadata = $6,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $7
        "#,
    )
    .bind(name)
    .bind(description)
    .bind(owner_group_id)
    .bind(task_state_graph_id)
    .bind(task_graph_id)
    .bind(metadata)
    .bind(project_id.0)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to update project", err))?;

    get_project_with_roles(pool, actor, project_id).await
}

pub async fn delete_project_with_roles(
    pool: &PgPool,
    actor: UserId,
    project_id: ProjectId,
) -> Result<()> {
    load_accessible_project(pool, actor, project_id, perm::project_delete()).await?;

    sqlx::query(
        r#"
        UPDATE tasks.projects
        SET
            deleted_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
          AND deleted_at IS NULL
        "#,
    )
    .bind(project_id.0)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to delete project", err))?;

    Ok(())
}
