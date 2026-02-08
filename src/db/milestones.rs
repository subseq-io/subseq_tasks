use anyhow::anyhow;

use super::*;

pub async fn create_milestone_with_roles(
    pool: &PgPool,
    actor: UserId,
    payload: CreateMilestonePayload,
) -> Result<Milestone> {
    load_accessible_project(pool, actor, payload.project_id, perm::milestone_create()).await?;

    let name = normalize_name(&payload.name, "Milestone")?;
    let description = normalize_description(payload.description);
    let milestone_id = MilestoneId(Uuid::new_v4());

    sqlx::query(
        r#"
        INSERT INTO tasks.milestones (
            id,
            project_id,
            milestone_type,
            name,
            description,
            due_date,
            start_date,
            started,
            completed,
            completed_date,
            repeat_interval_seconds,
            repeat_end,
            repeat_schema,
            metadata
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14
        )
        "#,
    )
    .bind(milestone_id.0)
    .bind(payload.project_id.0)
    .bind(payload.milestone_type.to_db_value())
    .bind(name)
    .bind(description)
    .bind(payload.due_date.map(|v| v.naive_utc()))
    .bind(payload.start_date.map(|v| v.naive_utc()))
    .bind(payload.started.unwrap_or(false))
    .bind(payload.completed.unwrap_or(false))
    .bind(payload.completed_date.map(|v| v.naive_utc()))
    .bind(payload.repeat_interval_seconds)
    .bind(payload.repeat_end.map(|v| v.naive_utc()))
    .bind(
        payload
            .repeat_schema
            .map(|value| serde_json::to_value(value).unwrap()),
    )
    .bind(payload.metadata.unwrap_or_else(|| json!({})))
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to create milestone", err))?;

    get_milestone_with_roles(pool, actor, milestone_id).await
}

pub async fn get_milestone_with_roles(
    pool: &PgPool,
    actor: UserId,
    milestone_id: MilestoneId,
) -> Result<Milestone> {
    let _ = load_accessible_milestone(pool, actor, milestone_id, perm::milestone_read()).await?;
    let presentation = load_milestone_presentation_row(pool, milestone_id).await?;
    match presentation {
        Some(row) => to_milestone_from_presentation_row(row),
        None => Err(LibError::database(
            "Failed to load milestone details",
            anyhow!(
                "milestone {milestone_id} was accessible but missing from milestone presentation"
            ),
        )),
    }
}

pub async fn list_milestones_with_roles(
    pool: &PgPool,
    actor: UserId,
    project_id: Option<ProjectId>,
    completed: Option<bool>,
    page: u32,
    limit: u32,
) -> Result<Paged<Milestone>> {
    let allowed_project_ids = accessible_project_ids(pool, actor, perm::milestone_read()).await?;
    if allowed_project_ids.is_empty() {
        return Ok(Paged {
            page,
            limit,
            items: Vec::new(),
        });
    }

    if let Some(project_id) = project_id {
        if !allowed_project_ids.contains(&project_id.0) {
            return Ok(Paged {
                page,
                limit,
                items: Vec::new(),
            });
        }
    }

    let offset = (page.saturating_sub(1) as i64).saturating_mul(limit as i64);

    let rows = sqlx::query_as::<_, MilestonePresentationRow>(
        r#"
        SELECT
            id,
            project_id,
            project_name,
            project_slug,
            project_owner_user_id,
            project_owner_username,
            project_owner_group_id,
            project_owner_group_display_name,
            milestone_type,
            name,
            description,
            due_date,
            start_date,
            started,
            completed,
            completed_date,
            repeat_interval_seconds,
            repeat_end,
            repeat_schema,
            metadata,
            created_at,
            updated_at
        FROM tasks.milestone_presentation
        WHERE project_id = ANY($1)
          AND ($2::uuid IS NULL OR project_id = $2)
          AND ($3::bool IS NULL OR completed = $3)
        ORDER BY due_date ASC NULLS LAST, created_at DESC, id DESC
        LIMIT $4 OFFSET $5
        "#,
    )
    .bind(&allowed_project_ids)
    .bind(project_id.map(|id| id.0))
    .bind(completed)
    .bind(limit as i64)
    .bind(offset)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to list milestones", err))?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(to_milestone_from_presentation_row(row)?);
    }

    Ok(Paged { page, limit, items })
}

pub async fn update_milestone_with_roles(
    pool: &PgPool,
    actor: UserId,
    milestone_id: MilestoneId,
    payload: UpdateMilestonePayload,
) -> Result<Milestone> {
    let existing =
        load_accessible_milestone(pool, actor, milestone_id, perm::milestone_update()).await?;

    let milestone_type = payload
        .milestone_type
        .map(|value| value.to_db_value())
        .unwrap_or(existing.milestone_type);

    let name = payload
        .name
        .map(|value| normalize_name(&value, "Milestone"))
        .transpose()?
        .unwrap_or(existing.name);

    let description = payload
        .description
        .map(|value| value.trim().to_string())
        .unwrap_or(existing.description);

    let due_date = if payload.clear_due_date.unwrap_or(false) {
        None
    } else {
        payload
            .due_date
            .map(|value| value.naive_utc())
            .or(existing.due_date)
    };

    let completed_date = if payload.clear_completed_date.unwrap_or(false) {
        None
    } else {
        payload
            .completed_date
            .map(|value| value.naive_utc())
            .or(existing.completed_date)
    };

    let (repeat_interval_seconds, repeat_end, repeat_schema) =
        if payload.clear_repeat.unwrap_or(false) {
            (None, None, None)
        } else {
            (
                payload
                    .repeat_interval_seconds
                    .or(existing.repeat_interval_seconds),
                payload
                    .repeat_end
                    .map(|value| value.naive_utc())
                    .or(existing.repeat_end),
                match payload.repeat_schema {
                    Some(schema) => Some(serde_json::to_value(schema).unwrap()),
                    None => existing.repeat_schema,
                },
            )
        };

    let metadata = payload.metadata.unwrap_or(existing.metadata);

    sqlx::query(
        r#"
        UPDATE tasks.milestones
        SET
            milestone_type = $1,
            name = $2,
            description = $3,
            due_date = $4,
            start_date = $5,
            started = $6,
            completed = $7,
            completed_date = $8,
            repeat_interval_seconds = $9,
            repeat_end = $10,
            repeat_schema = $11,
            metadata = $12,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $13
          AND deleted_at IS NULL
        "#,
    )
    .bind(milestone_type)
    .bind(name)
    .bind(description)
    .bind(due_date)
    .bind(
        payload
            .start_date
            .map(|value| value.naive_utc())
            .or(Some(existing.start_date)),
    )
    .bind(payload.started.unwrap_or(existing.started))
    .bind(payload.completed.unwrap_or(existing.completed))
    .bind(completed_date)
    .bind(repeat_interval_seconds)
    .bind(repeat_end)
    .bind(repeat_schema)
    .bind(metadata)
    .bind(milestone_id.0)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to update milestone", err))?;

    get_milestone_with_roles(pool, actor, milestone_id).await
}

pub async fn delete_milestone_with_roles(
    pool: &PgPool,
    actor: UserId,
    milestone_id: MilestoneId,
) -> Result<()> {
    load_accessible_milestone(pool, actor, milestone_id, perm::milestone_delete()).await?;

    sqlx::query(
        r#"
        UPDATE tasks.milestones
        SET
            deleted_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
          AND deleted_at IS NULL
        "#,
    )
    .bind(milestone_id.0)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to delete milestone", err))?;

    Ok(())
}
