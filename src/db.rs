use std::collections::HashSet;

use anyhow::anyhow;
use once_cell::sync::Lazy;
use serde_json::json;
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use subseq_auth::group_id::GroupId;
use subseq_auth::user_id::UserId;
use subseq_graph::db::get_graph;
use subseq_graph::models::{GraphId, GraphNodeId};
use subseq_graph::permissions as graph_perm;

use crate::error::{ErrorKind, LibError, Result};
use crate::models::{
    CreateMilestonePayload, CreateProjectPayload, CreateTaskLinkPayload, CreateTaskPayload,
    Milestone, MilestoneId, MilestoneType, Paged, Project, ProjectId, ProjectSummary, RepeatSchema,
    Task, TaskDetails, TaskGraphAssignment, TaskId, TaskLink, TaskLinkType, TaskState,
    TransitionTaskPayload, UpdateMilestonePayload, UpdateProjectPayload, UpdateTaskPayload,
};
use crate::permissions as perm;

pub static MIGRATOR: Lazy<Migrator> = Lazy::new(|| {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
});

pub async fn create_task_tables(pool: &PgPool) -> std::result::Result<(), MigrateError> {
    subseq_auth::db::create_user_tables(pool).await?;
    subseq_graph::db::create_graph_tables(pool).await?;
    MIGRATOR.run(pool).await
}

#[derive(Debug, Clone, FromRow)]
struct ProjectRow {
    id: Uuid,
    owner_user_id: Uuid,
    owner_group_id: Option<Uuid>,
    name: String,
    slug: String,
    description: String,
    task_state_graph_id: Uuid,
    task_graph_id: Option<Uuid>,
    metadata: serde_json::Value,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, FromRow)]
struct ProjectSummaryRow {
    id: Uuid,
    owner_user_id: Uuid,
    owner_group_id: Option<Uuid>,
    name: String,
    slug: String,
    description: String,
    task_state_graph_id: Uuid,
    task_graph_id: Option<Uuid>,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    task_count: i64,
    milestone_count: i64,
}

#[derive(Debug, Clone, FromRow)]
struct MilestoneRow {
    id: Uuid,
    project_id: Uuid,
    milestone_type: String,
    name: String,
    description: String,
    due_date: Option<chrono::NaiveDateTime>,
    start_date: chrono::NaiveDateTime,
    started: bool,
    completed: bool,
    completed_date: Option<chrono::NaiveDateTime>,
    repeat_interval_seconds: Option<i64>,
    repeat_end: Option<chrono::NaiveDateTime>,
    repeat_schema: Option<serde_json::Value>,
    metadata: serde_json::Value,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, FromRow)]
struct MilestoneAccessRow {
    id: Uuid,
    project_id: Uuid,
    milestone_type: String,
    name: String,
    description: String,
    due_date: Option<chrono::NaiveDateTime>,
    start_date: chrono::NaiveDateTime,
    started: bool,
    completed: bool,
    completed_date: Option<chrono::NaiveDateTime>,
    repeat_interval_seconds: Option<i64>,
    repeat_end: Option<chrono::NaiveDateTime>,
    repeat_schema: Option<serde_json::Value>,
    metadata: serde_json::Value,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    project_owner_user_id: Uuid,
    project_owner_group_id: Option<Uuid>,
}

#[derive(Debug, Clone, FromRow)]
struct TaskRow {
    id: Uuid,
    slug: String,
    title: String,
    description: String,
    author_user_id: Uuid,
    assignee_user_id: Option<Uuid>,
    priority: i32,
    due_date: Option<chrono::NaiveDateTime>,
    milestone_id: Option<Uuid>,
    state: String,
    archived: bool,
    metadata: serde_json::Value,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, FromRow)]
struct TaskLinkRow {
    task_from_id: Uuid,
    task_to_id: Uuid,
    link_type: String,
    created_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, FromRow)]
struct TaskGraphAssignmentRow {
    graph_id: Uuid,
    current_node_id: Option<Uuid>,
    order_added: i32,
    updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, FromRow)]
struct TaskProjectPermissionRow {
    project_id: Uuid,
    owner_user_id: Uuid,
    owner_group_id: Option<Uuid>,
}

fn db_err(public: &'static str, err: sqlx::Error) -> LibError {
    LibError::database(public, anyhow!(err))
}

fn normalize_name(name: &str, kind: &'static str) -> Result<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(LibError::invalid(
            format!("{kind} name is required"),
            anyhow!("empty {kind} name"),
        ));
    }

    if trimmed.len() > 128 {
        return Err(LibError::invalid(
            format!("{kind} name is too long"),
            anyhow!("{kind} name exceeds 128 characters"),
        ));
    }

    Ok(trimmed.to_string())
}

fn normalize_task_title(title: &str) -> Result<String> {
    let trimmed = title.trim();
    if trimmed.is_empty() {
        return Err(LibError::invalid(
            "Task title is required",
            anyhow!("empty task title"),
        ));
    }

    if trimmed.len() > 256 {
        return Err(LibError::invalid(
            "Task title is too long",
            anyhow!("task title exceeds 256 characters"),
        ));
    }

    Ok(trimmed.to_string())
}

fn normalize_description(description: Option<String>) -> String {
    description.unwrap_or_default().trim().to_string()
}

fn slug_from_name(name: &str) -> String {
    let mut slug = String::new();
    let mut previous_was_dash = false;

    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_uppercase());
            previous_was_dash = false;
        } else if !previous_was_dash {
            slug.push('-');
            previous_was_dash = true;
        }
    }

    while slug.ends_with('-') {
        slug.pop();
    }

    while slug.starts_with('-') {
        slug.remove(0);
    }

    if slug.is_empty() {
        "PROJECT".to_string()
    } else {
        slug
    }
}

fn milestone_type_from_row(value: &str) -> Result<MilestoneType> {
    MilestoneType::from_db_value(value).ok_or_else(|| {
        LibError::database(
            "Failed to decode milestone type",
            anyhow!("invalid milestone_type value {value}"),
        )
    })
}

fn repeat_schema_from_row(value: Option<serde_json::Value>) -> Result<Option<RepeatSchema>> {
    match value {
        Some(value) => serde_json::from_value(value).map(Some).map_err(|err| {
            LibError::database(
                "Failed to decode milestone repeat schema",
                anyhow!("invalid repeat schema payload: {err}"),
            )
        }),
        None => Ok(None),
    }
}

fn task_state_from_row(value: &str) -> Result<TaskState> {
    TaskState::from_db_value(value).ok_or_else(|| {
        LibError::database(
            "Failed to decode task state",
            anyhow!("invalid task state value {value}"),
        )
    })
}

fn task_link_type_from_row(value: &str) -> Result<TaskLinkType> {
    TaskLinkType::from_db_value(value).ok_or_else(|| {
        LibError::database(
            "Failed to decode task link type",
            anyhow!("invalid task link type value {value}"),
        )
    })
}

fn to_project(row: ProjectRow) -> Project {
    Project {
        id: ProjectId(row.id),
        owner_user_id: UserId(row.owner_user_id),
        owner_group_id: row.owner_group_id.map(GroupId),
        name: row.name,
        slug: row.slug,
        description: row.description,
        task_state_graph_id: GraphId(row.task_state_graph_id),
        task_graph_id: row.task_graph_id.map(GraphId),
        metadata: row.metadata,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn to_project_summary(row: ProjectSummaryRow) -> ProjectSummary {
    ProjectSummary {
        id: ProjectId(row.id),
        owner_user_id: UserId(row.owner_user_id),
        owner_group_id: row.owner_group_id.map(GroupId),
        name: row.name,
        slug: row.slug,
        description: row.description,
        task_state_graph_id: GraphId(row.task_state_graph_id),
        task_graph_id: row.task_graph_id.map(GraphId),
        created_at: row.created_at,
        updated_at: row.updated_at,
        task_count: row.task_count,
        milestone_count: row.milestone_count,
    }
}

fn to_milestone(row: MilestoneRow) -> Result<Milestone> {
    Ok(Milestone {
        id: MilestoneId(row.id),
        project_id: ProjectId(row.project_id),
        milestone_type: milestone_type_from_row(&row.milestone_type)?,
        name: row.name,
        description: row.description,
        due_date: row.due_date,
        start_date: row.start_date,
        started: row.started,
        completed: row.completed,
        completed_date: row.completed_date,
        repeat_interval_seconds: row.repeat_interval_seconds,
        repeat_end: row.repeat_end,
        repeat_schema: repeat_schema_from_row(row.repeat_schema)?,
        metadata: row.metadata,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn to_task(row: TaskRow) -> Result<Task> {
    Ok(Task {
        id: TaskId(row.id),
        slug: row.slug,
        title: row.title,
        description: row.description,
        author_user_id: UserId(row.author_user_id),
        assignee_user_id: row.assignee_user_id.map(UserId),
        priority: row.priority,
        due_date: row.due_date,
        milestone_id: row.milestone_id.map(MilestoneId),
        state: task_state_from_row(&row.state)?,
        archived: row.archived,
        metadata: row.metadata,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn to_task_graph_assignment(row: TaskGraphAssignmentRow) -> TaskGraphAssignment {
    TaskGraphAssignment {
        graph_id: GraphId(row.graph_id),
        current_node_id: row.current_node_id.map(GraphNodeId),
        order_added: row.order_added,
        updated_at: row.updated_at,
    }
}

fn to_task_link(row: TaskLinkRow) -> Result<TaskLink> {
    Ok(TaskLink {
        task_from_id: TaskId(row.task_from_id),
        task_to_id: TaskId(row.task_to_id),
        link_type: task_link_type_from_row(&row.link_type)?,
        created_at: row.created_at,
    })
}

async fn project_exists(pool: &PgPool, project_id: ProjectId) -> Result<bool> {
    let exists: (bool,) = sqlx::query_as(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM tasks.projects p
            WHERE p.id = $1
              AND p.deleted_at IS NULL
        )
        "#,
    )
    .bind(project_id.0)
    .fetch_one(pool)
    .await
    .map_err(|err| db_err("Failed to query project", err))?;

    Ok(exists.0)
}

async fn milestone_exists(pool: &PgPool, milestone_id: MilestoneId) -> Result<bool> {
    let exists: (bool,) = sqlx::query_as(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM tasks.milestones m
            WHERE m.id = $1
              AND m.deleted_at IS NULL
        )
        "#,
    )
    .bind(milestone_id.0)
    .fetch_one(pool)
    .await
    .map_err(|err| db_err("Failed to query milestone", err))?;

    Ok(exists.0)
}

async fn task_exists(pool: &PgPool, task_id: TaskId) -> Result<bool> {
    let exists: (bool,) = sqlx::query_as(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM tasks.tasks t
            WHERE t.id = $1
              AND t.deleted_at IS NULL
        )
        "#,
    )
    .bind(task_id.0)
    .fetch_one(pool)
    .await
    .map_err(|err| db_err("Failed to query task", err))?;

    Ok(exists.0)
}

async fn group_has_permission(
    pool: &PgPool,
    actor: UserId,
    group_id: GroupId,
    permission: &str,
    project_id: Option<ProjectId>,
    task_id: Option<TaskId>,
) -> Result<bool> {
    let permission_roles: Vec<String> = perm::access_roles(permission)
        .iter()
        .map(|role| (*role).to_string())
        .collect();
    if permission_roles.is_empty() {
        return Ok(false);
    }

    let project_scope_id = project_id.map(|id| id.0.to_string());
    let task_scope_id = task_id.map(|id| id.0.to_string());
    let row: (bool,) = sqlx::query_as(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM auth.group_memberships gm
            JOIN auth.groups g
              ON g.id = gm.group_id
            JOIN auth.users u
              ON u.id = gm.user_id
            WHERE gm.group_id = $1
              AND gm.user_id = $2
              AND g.active = TRUE
              AND u.active = TRUE
              AND EXISTS (
                  SELECT 1
                  FROM auth.group_roles gr
                  WHERE gr.group_id = gm.group_id
                    AND gr.role_name = ANY($3)
                    AND (
                        (gr.scope = $4 AND gr.scope_id = $5)
                        OR ($6::text IS NOT NULL AND gr.scope = $7 AND gr.scope_id = $6)
                        OR ($8::text IS NOT NULL AND gr.scope = $9 AND gr.scope_id = $8)
                    )
              )
        )
        "#,
    )
    .bind(group_id.0)
    .bind(actor.0)
    .bind(permission_roles)
    .bind(perm::scope_tasks())
    .bind(perm::scope_id_global())
    .bind(project_scope_id)
    .bind(perm::scope_project())
    .bind(task_scope_id)
    .bind(perm::scope_task())
    .fetch_one(pool)
    .await
    .map_err(|err| db_err("Failed to query group permission", err))?;

    Ok(row.0)
}

async fn ensure_graph_read_access(pool: &PgPool, actor: UserId, graph_id: GraphId) -> Result<()> {
    let _ = get_graph(pool, actor, graph_id, graph_perm::graph_read_access_roles())
        .await
        .map_err(LibError::from)?;
    Ok(())
}

async fn ensure_project_graph_access(
    pool: &PgPool,
    actor: UserId,
    task_state_graph_id: GraphId,
    task_graph_id: Option<GraphId>,
) -> Result<()> {
    ensure_graph_read_access(pool, actor, task_state_graph_id).await?;
    if let Some(task_graph_id) = task_graph_id {
        if task_graph_id != task_state_graph_id {
            ensure_graph_read_access(pool, actor, task_graph_id).await?;
        }
    }
    Ok(())
}

async fn ensure_task_assignment_graph_access(
    pool: &PgPool,
    actor: UserId,
    assignments: &[TaskGraphAssignment],
) -> Result<()> {
    let mut checked_graph_ids = HashSet::new();
    for assignment in assignments {
        if checked_graph_ids.insert(assignment.graph_id.0) {
            ensure_graph_read_access(pool, actor, assignment.graph_id).await?;
        }
    }
    Ok(())
}

async fn ensure_group_permission_for_new_resource(
    pool: &PgPool,
    actor: UserId,
    group_id: GroupId,
    permission: &str,
) -> Result<()> {
    if group_has_permission(pool, actor, group_id, permission, None, None).await? {
        Ok(())
    } else {
        Err(LibError::forbidden(
            "You do not have group permissions for this action",
            anyhow!("user {actor} is missing group permission {permission} in group {group_id}"),
        ))
    }
}

async fn ensure_project_owner_or_group_permission(
    pool: &PgPool,
    actor: UserId,
    project_id: ProjectId,
    owner_user_id: Uuid,
    owner_group_id: Option<Uuid>,
    permission: &str,
) -> Result<()> {
    if owner_user_id == actor.0 {
        return Ok(());
    }

    if let Some(group_id) = owner_group_id {
        if group_has_permission(
            pool,
            actor,
            GroupId(group_id),
            permission,
            Some(project_id),
            None,
        )
        .await?
        {
            Ok(())
        } else {
            Err(LibError::forbidden(
                "You do not have project permissions for this action",
                anyhow!(
                    "user {actor} is missing project permission {permission} on project {project_id}"
                ),
            ))
        }
    } else {
        Err(LibError::forbidden(
            "You do not have project permissions for this action",
            anyhow!("user {actor} does not own project {project_id}"),
        ))
    }
}

async fn load_project_row(pool: &PgPool, project_id: ProjectId) -> Result<Option<ProjectRow>> {
    sqlx::query_as::<_, ProjectRow>(
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
            p.metadata,
            p.created_at,
            p.updated_at
        FROM tasks.projects p
        WHERE p.id = $1
          AND p.deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(project_id.0)
    .fetch_optional(pool)
    .await
    .map_err(|err| db_err("Failed to query project", err))
}

async fn load_accessible_project(
    pool: &PgPool,
    actor: UserId,
    project_id: ProjectId,
    permission: &str,
) -> Result<ProjectRow> {
    let row = load_project_row(pool, project_id).await?;
    if let Some(row) = row {
        ensure_project_owner_or_group_permission(
            pool,
            actor,
            project_id,
            row.owner_user_id,
            row.owner_group_id,
            permission,
        )
        .await?;
        Ok(row)
    } else if project_exists(pool, project_id).await? {
        Err(LibError::forbidden(
            "You do not have access to this project",
            anyhow!("project {project_id} is not accessible by user {actor}"),
        ))
    } else {
        Err(LibError::not_found(
            "Project not found",
            anyhow!("project {project_id} does not exist"),
        ))
    }
}

async fn load_accessible_milestone(
    pool: &PgPool,
    actor: UserId,
    milestone_id: MilestoneId,
    permission: &str,
) -> Result<MilestoneRow> {
    let row = sqlx::query_as::<_, MilestoneAccessRow>(
        r#"
        SELECT
            m.id,
            m.project_id,
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
            m.updated_at,
            p.owner_user_id AS project_owner_user_id,
            p.owner_group_id AS project_owner_group_id
        FROM tasks.milestones m
        JOIN tasks.projects p
          ON p.id = m.project_id
        WHERE m.id = $1
          AND m.deleted_at IS NULL
          AND p.deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(milestone_id.0)
    .fetch_optional(pool)
    .await
    .map_err(|err| db_err("Failed to query milestone", err))?;

    if let Some(row) = row {
        ensure_project_owner_or_group_permission(
            pool,
            actor,
            ProjectId(row.project_id),
            row.project_owner_user_id,
            row.project_owner_group_id,
            permission,
        )
        .await?;

        Ok(MilestoneRow {
            id: row.id,
            project_id: row.project_id,
            milestone_type: row.milestone_type,
            name: row.name,
            description: row.description,
            due_date: row.due_date,
            start_date: row.start_date,
            started: row.started,
            completed: row.completed,
            completed_date: row.completed_date,
            repeat_interval_seconds: row.repeat_interval_seconds,
            repeat_end: row.repeat_end,
            repeat_schema: row.repeat_schema,
            metadata: row.metadata,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    } else if milestone_exists(pool, milestone_id).await? {
        Err(LibError::forbidden(
            "You do not have access to this milestone",
            anyhow!("milestone {milestone_id} is not accessible by user {actor}"),
        ))
    } else {
        Err(LibError::not_found(
            "Milestone not found",
            anyhow!("milestone {milestone_id} does not exist"),
        ))
    }
}

async fn get_task_project_permission_rows(
    pool: &PgPool,
    task_id: TaskId,
) -> Result<Vec<TaskProjectPermissionRow>> {
    sqlx::query_as::<_, TaskProjectPermissionRow>(
        r#"
        SELECT DISTINCT
            p.id AS project_id,
            p.owner_user_id,
            p.owner_group_id
        FROM tasks.task_projects tp
        JOIN tasks.projects p
          ON p.id = tp.project_id
        WHERE tp.task_id = $1
          AND p.deleted_at IS NULL
        ORDER BY p.id ASC
        "#,
    )
    .bind(task_id.0)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query task projects for permissions", err))
}

async fn ensure_task_permission(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    author_user_id: Uuid,
    permission: &str,
) -> Result<()> {
    if author_user_id == actor.0 {
        return Ok(());
    }

    let project_rows = get_task_project_permission_rows(pool, task_id).await?;
    for row in project_rows {
        if row.owner_user_id == actor.0 {
            return Ok(());
        }
        if let Some(group_id) = row.owner_group_id {
            if group_has_permission(
                pool,
                actor,
                GroupId(group_id),
                permission,
                Some(ProjectId(row.project_id)),
                Some(task_id),
            )
            .await?
            {
                return Ok(());
            }
        }
    }

    Err(LibError::forbidden(
        "You do not have task permissions for this action",
        anyhow!("user {actor} is missing task permission {permission} for task {task_id}"),
    ))
}

async fn load_accessible_task(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    permission: &str,
) -> Result<TaskRow> {
    let row = sqlx::query_as::<_, TaskRow>(
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
        WHERE t.id = $1
          AND t.deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(task_id.0)
    .fetch_optional(pool)
    .await
    .map_err(|err| db_err("Failed to query task", err))?;

    if let Some(row) = row {
        ensure_task_permission(pool, actor, task_id, row.author_user_id, permission).await?;
        Ok(row)
    } else if task_exists(pool, task_id).await? {
        Err(LibError::forbidden(
            "You do not have access to this task",
            anyhow!("task {task_id} is not accessible by user {actor}"),
        ))
    } else {
        Err(LibError::not_found(
            "Task not found",
            anyhow!("task {task_id} does not exist"),
        ))
    }
}

async fn accessible_project_ids(
    pool: &PgPool,
    actor: UserId,
    permission: &str,
) -> Result<Vec<Uuid>> {
    let rows = sqlx::query_as::<_, (Uuid, Uuid, Option<Uuid>)>(
        r#"
        SELECT p.id, p.owner_user_id, p.owner_group_id
        FROM tasks.projects p
        WHERE p.deleted_at IS NULL
        ORDER BY p.id ASC
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query projects", err))?;

    let mut ids = Vec::with_capacity(rows.len());
    for (project_id, owner_user_id, owner_group_id) in rows {
        if owner_user_id == actor.0 {
            ids.push(project_id);
            continue;
        }
        if let Some(group_id) = owner_group_id {
            if group_has_permission(
                pool,
                actor,
                GroupId(group_id),
                permission,
                Some(ProjectId(project_id)),
                None,
            )
            .await?
            {
                ids.push(project_id);
            }
        }
    }

    Ok(ids)
}

async fn get_task_project_ids(pool: &PgPool, task_id: TaskId) -> Result<Vec<ProjectId>> {
    let rows = sqlx::query_as::<_, (Uuid,)>(
        r#"
        SELECT tp.project_id
        FROM tasks.task_projects tp
        JOIN tasks.projects p
          ON p.id = tp.project_id
        WHERE tp.task_id = $1
          AND p.deleted_at IS NULL
        ORDER BY tp.order_added ASC, tp.project_id ASC
        "#,
    )
    .bind(task_id.0)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query task projects", err))?;

    Ok(rows.into_iter().map(|row| ProjectId(row.0)).collect())
}

async fn get_task_graph_assignments(
    pool: &PgPool,
    task_id: TaskId,
) -> Result<Vec<TaskGraphAssignment>> {
    let rows = sqlx::query_as::<_, TaskGraphAssignmentRow>(
        r#"
        SELECT
            graph_id,
            current_node_id,
            order_added,
            updated_at
        FROM tasks.task_graph_assignments
        WHERE task_id = $1
        ORDER BY order_added ASC, graph_id ASC
        "#,
    )
    .bind(task_id.0)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query task graph assignments", err))?;

    Ok(rows.into_iter().map(to_task_graph_assignment).collect())
}

async fn get_task_links_out(pool: &PgPool, task_id: TaskId) -> Result<Vec<TaskLink>> {
    let rows = sqlx::query_as::<_, TaskLinkRow>(
        r#"
        SELECT
            task_from_id,
            task_to_id,
            link_type,
            created_at
        FROM tasks.task_links
        WHERE task_from_id = $1
        ORDER BY created_at DESC, task_to_id ASC
        "#,
    )
    .bind(task_id.0)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query outgoing task links", err))?;

    rows.into_iter().map(to_task_link).collect()
}

async fn get_task_links_in(pool: &PgPool, task_id: TaskId) -> Result<Vec<TaskLink>> {
    let rows = sqlx::query_as::<_, TaskLinkRow>(
        r#"
        SELECT
            task_from_id,
            task_to_id,
            link_type,
            created_at
        FROM tasks.task_links
        WHERE task_to_id = $1
        ORDER BY created_at DESC, task_from_id ASC
        "#,
    )
    .bind(task_id.0)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query incoming task links", err))?;

    rows.into_iter().map(to_task_link).collect()
}

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
    let row = load_accessible_milestone(pool, actor, milestone_id, perm::milestone_read()).await?;
    to_milestone(row)
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

    let rows = sqlx::query_as::<_, MilestoneRow>(
        r#"
        SELECT
            m.id,
            m.project_id,
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
        WHERE m.deleted_at IS NULL
          AND p.deleted_at IS NULL
          AND p.id = ANY($1)
          AND ($2::uuid IS NULL OR m.project_id = $2)
          AND ($3::bool IS NULL OR m.completed = $3)
        ORDER BY m.due_date ASC NULLS LAST, m.created_at DESC, m.id DESC
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
        items.push(to_milestone(row)?);
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
