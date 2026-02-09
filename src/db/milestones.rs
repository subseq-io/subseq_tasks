use anyhow::anyhow;

use super::*;

async fn validate_milestone_link_target(
    pool: &PgPool,
    actor: UserId,
    project_id: ProjectId,
    linked_milestone_id: MilestoneId,
    link_field: &str,
) -> Result<()> {
    let linked =
        load_accessible_milestone(pool, actor, linked_milestone_id, perm::milestone_read()).await?;
    if linked.project_id != project_id.0 {
        return Err(LibError::invalid(
            "Milestone links must reference milestones in the same project",
            anyhow!(
                "milestone {} for {} belongs to project {}, expected {}",
                linked_milestone_id,
                link_field,
                linked.project_id,
                project_id
            ),
        ));
    }
    Ok(())
}

pub async fn create_milestone_with_roles(
    pool: &PgPool,
    actor: UserId,
    payload: CreateMilestonePayload,
) -> Result<Milestone> {
    load_accessible_project(pool, actor, payload.project_id, perm::milestone_create()).await?;

    if let (Some(next_id), Some(previous_id)) =
        (payload.next_milestone_id, payload.previous_milestone_id)
        && next_id == previous_id
    {
        return Err(LibError::invalid(
            "nextMilestoneId and previousMilestoneId cannot be the same value",
            anyhow!("create milestone received identical next/previous milestone IDs"),
        ));
    }
    if let Some(next_id) = payload.next_milestone_id {
        validate_milestone_link_target(pool, actor, payload.project_id, next_id, "nextMilestoneId")
            .await?;
    }
    if let Some(previous_id) = payload.previous_milestone_id {
        validate_milestone_link_target(
            pool,
            actor,
            payload.project_id,
            previous_id,
            "previousMilestoneId",
        )
        .await?;
    }

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
            next_milestone_id,
            previous_milestone_id,
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
            $14,
            $15,
            $16
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
    .bind(payload.next_milestone_id.map(|id| id.0))
    .bind(payload.previous_milestone_id.map(|id| id.0))
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
    query: Option<String>,
    due: Option<chrono::NaiveDateTime>,
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
    let search_query = query
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let search_like = search_query.as_ref().map(|value| format!("%{}%", value));

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
            next_milestone_id,
            previous_milestone_id,
            metadata,
            created_at,
            updated_at
        FROM tasks.milestone_presentation
        WHERE project_id = ANY($1)
          AND ($2::uuid IS NULL OR project_id = $2)
          AND ($3::bool IS NULL OR completed = $3)
          AND (
              $4::text IS NULL
              OR name ILIKE $5
              OR description ILIKE $5
              OR milestone_type ILIKE $5
          )
          AND ($6::timestamp IS NULL OR due_date >= $6)
        ORDER BY due_date ASC NULLS LAST, created_at DESC, id DESC
        LIMIT $7 OFFSET $8
        "#,
    )
    .bind(&allowed_project_ids)
    .bind(project_id.map(|id| id.0))
    .bind(completed)
    .bind(search_query)
    .bind(search_like)
    .bind(due)
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

    let next_milestone_id = if payload.clear_next_milestone.unwrap_or(false) {
        None
    } else {
        payload
            .next_milestone_id
            .map(|id| id.0)
            .or(existing.next_milestone_id)
    };
    let previous_milestone_id = if payload.clear_previous_milestone.unwrap_or(false) {
        None
    } else {
        payload
            .previous_milestone_id
            .map(|id| id.0)
            .or(existing.previous_milestone_id)
    };

    if next_milestone_id == Some(milestone_id.0) {
        return Err(LibError::invalid(
            "Milestone cannot reference itself as next milestone",
            anyhow!(
                "milestone {} next_milestone_id self-reference",
                milestone_id
            ),
        ));
    }
    if previous_milestone_id == Some(milestone_id.0) {
        return Err(LibError::invalid(
            "Milestone cannot reference itself as previous milestone",
            anyhow!(
                "milestone {} previous_milestone_id self-reference",
                milestone_id
            ),
        ));
    }
    if let (Some(next_id), Some(previous_id)) = (next_milestone_id, previous_milestone_id)
        && next_id == previous_id
    {
        return Err(LibError::invalid(
            "nextMilestoneId and previousMilestoneId cannot be the same value",
            anyhow!(
                "milestone {} update uses identical next/previous milestone IDs",
                milestone_id
            ),
        ));
    }

    if let Some(next_id) = next_milestone_id {
        validate_milestone_link_target(
            pool,
            actor,
            ProjectId(existing.project_id),
            MilestoneId(next_id),
            "nextMilestoneId",
        )
        .await?;
    }
    if let Some(previous_id) = previous_milestone_id {
        validate_milestone_link_target(
            pool,
            actor,
            ProjectId(existing.project_id),
            MilestoneId(previous_id),
            "previousMilestoneId",
        )
        .await?;
    }

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
            next_milestone_id = $12,
            previous_milestone_id = $13,
            metadata = $14,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $15
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
    .bind(next_milestone_id)
    .bind(previous_milestone_id)
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
