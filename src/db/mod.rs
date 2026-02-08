use std::collections::HashSet;

use anyhow::anyhow;
use once_cell::sync::Lazy;
use serde::de::DeserializeOwned;
use serde_json::json;
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use subseq_auth::group_id::GroupId;
use subseq_auth::user_id::UserId;
use subseq_graph::db::get_graph;
use subseq_graph::models::GraphId;
use subseq_graph::permissions as graph_perm;

use crate::error::{ErrorKind, LibError, Result};
use crate::models::{
    CreateMilestonePayload, CreateProjectPayload, CreateTaskCommentPayload, CreateTaskLinkPayload,
    CreateTaskPayload, Milestone, MilestoneId, MilestoneType, Paged, Project, ProjectId,
    ProjectSummary, RepeatSchema, Task, TaskCascadeImpact, TaskCascadeImpactQuery,
    TaskCascadeOperation, TaskComment, TaskCommentId, TaskDetails, TaskGraphAssignment, TaskId,
    TaskLink, TaskLinkType, TaskLogEntry, TaskLogId, TaskProjectPresentation, TaskState,
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
    completed_by_user_id: Option<Uuid>,
    completed_at: Option<chrono::NaiveDateTime>,
    rejected_reason: Option<String>,
    metadata: serde_json::Value,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, FromRow)]
struct TaskPresentationRow {
    id: Uuid,
    slug: String,
    title: String,
    description: String,
    author_user_id: Uuid,
    author_username: Option<String>,
    assignee_user_id: Option<Uuid>,
    assignee_username: Option<String>,
    priority: i32,
    due_date: Option<chrono::NaiveDateTime>,
    milestone_id: Option<Uuid>,
    milestone_name: Option<String>,
    milestone_type: Option<String>,
    state: String,
    archived: bool,
    completed_by_user_id: Option<Uuid>,
    completed_by_username: Option<String>,
    completed_at: Option<chrono::NaiveDateTime>,
    rejected_reason: Option<String>,
    metadata: serde_json::Value,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    project_ids: Vec<Uuid>,
    projects: serde_json::Value,
    graph_assignments: serde_json::Value,
    links_out: serde_json::Value,
    links_in: serde_json::Value,
    comments: serde_json::Value,
    log: serde_json::Value,
}

#[derive(Debug, Clone, FromRow)]
struct TaskLinkRow {
    task_from_id: Uuid,
    task_to_id: Uuid,
    link_type: String,
    subtask_parent_state: Option<String>,
    created_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, FromRow)]
struct TaskCommentRow {
    id: Uuid,
    task_id: Uuid,
    author_user_id: Uuid,
    body: String,
    metadata: serde_json::Value,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, FromRow)]
struct TaskLogRow {
    id: Uuid,
    task_id: Uuid,
    actor_user_id: Uuid,
    action: String,
    from_state: Option<String>,
    to_state: Option<String>,
    details: serde_json::Value,
    created_at: chrono::NaiveDateTime,
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
        author_username: None,
        assignee_user_id: row.assignee_user_id.map(UserId),
        assignee_username: None,
        priority: row.priority,
        due_date: row.due_date,
        milestone_id: row.milestone_id.map(MilestoneId),
        milestone_name: None,
        milestone_type: None,
        state: task_state_from_row(&row.state)?,
        archived: row.archived,
        completed_by_user_id: row.completed_by_user_id.map(UserId),
        completed_by_username: None,
        completed_at: row.completed_at,
        rejected_reason: row.rejected_reason,
        metadata: row.metadata,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn to_task_link(row: TaskLinkRow) -> Result<TaskLink> {
    Ok(TaskLink {
        task_from_id: TaskId(row.task_from_id),
        task_to_id: TaskId(row.task_to_id),
        link_type: task_link_type_from_row(&row.link_type)?,
        subtask_parent_state: row
            .subtask_parent_state
            .as_deref()
            .map(task_state_from_row)
            .transpose()?,
        other_task_title: None,
        created_at: row.created_at,
    })
}

fn to_task_comment(row: TaskCommentRow) -> TaskComment {
    TaskComment {
        id: TaskCommentId(row.id),
        task_id: TaskId(row.task_id),
        author_user_id: UserId(row.author_user_id),
        author_username: None,
        body: row.body,
        metadata: row.metadata,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn to_task_log(row: TaskLogRow) -> Result<TaskLogEntry> {
    Ok(TaskLogEntry {
        id: TaskLogId(row.id),
        task_id: TaskId(row.task_id),
        actor_user_id: UserId(row.actor_user_id),
        actor_username: None,
        action: row.action,
        from_state: row
            .from_state
            .as_deref()
            .map(task_state_from_row)
            .transpose()?,
        to_state: row
            .to_state
            .as_deref()
            .map(task_state_from_row)
            .transpose()?,
        details: row.details,
        created_at: row.created_at,
    })
}

fn parse_json_array<T>(value: serde_json::Value, context: &'static str) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    match value {
        serde_json::Value::Null => Ok(Vec::new()),
        serde_json::Value::Array(_) => serde_json::from_value(value).map_err(|err| {
            LibError::database(
                format!("Failed to decode {context}"),
                anyhow!("invalid {context} payload: {err}"),
            )
        }),
        other => Err(LibError::database(
            format!("Failed to decode {context}"),
            anyhow!("expected JSON array for {context}, got {other}"),
        )),
    }
}

fn to_task_from_presentation_row(row: &TaskPresentationRow) -> Result<Task> {
    Ok(Task {
        id: TaskId(row.id),
        slug: row.slug.clone(),
        title: row.title.clone(),
        description: row.description.clone(),
        author_user_id: UserId(row.author_user_id),
        author_username: row.author_username.clone(),
        assignee_user_id: row.assignee_user_id.map(UserId),
        assignee_username: row.assignee_username.clone(),
        priority: row.priority,
        due_date: row.due_date,
        milestone_id: row.milestone_id.map(MilestoneId),
        milestone_name: row.milestone_name.clone(),
        milestone_type: row.milestone_type.clone(),
        state: task_state_from_row(&row.state)?,
        archived: row.archived,
        completed_by_user_id: row.completed_by_user_id.map(UserId),
        completed_by_username: row.completed_by_username.clone(),
        completed_at: row.completed_at,
        rejected_reason: row.rejected_reason.clone(),
        metadata: row.metadata.clone(),
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn to_task_details_from_presentation_row(row: TaskPresentationRow) -> Result<TaskDetails> {
    let task = to_task_from_presentation_row(&row)?;
    let project_ids = row.project_ids.into_iter().map(ProjectId).collect();
    let projects: Vec<TaskProjectPresentation> = parse_json_array(row.projects, "task projects")?;
    let graph_assignments: Vec<TaskGraphAssignment> =
        parse_json_array(row.graph_assignments, "task graph assignments")?;
    let links_out: Vec<TaskLink> = parse_json_array(row.links_out, "outgoing task links")?;
    let links_in: Vec<TaskLink> = parse_json_array(row.links_in, "incoming task links")?;
    let comments: Vec<TaskComment> = parse_json_array(row.comments, "task comments")?;
    let log: Vec<TaskLogEntry> = parse_json_array(row.log, "task log entries")?;

    Ok(TaskDetails {
        task,
        project_ids,
        projects,
        graph_assignments,
        links_out,
        links_in,
        comments,
        log,
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
            t.completed_by_user_id,
            t.completed_at,
            t.rejected_reason,
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

async fn load_task_presentation_row(
    pool: &PgPool,
    task_id: TaskId,
) -> Result<Option<TaskPresentationRow>> {
    sqlx::query_as::<_, TaskPresentationRow>(
        r#"
        SELECT
            id,
            slug,
            title,
            description,
            author_user_id,
            author_username,
            assignee_user_id,
            assignee_username,
            priority,
            due_date,
            milestone_id,
            milestone_name,
            milestone_type,
            state,
            archived,
            completed_by_user_id,
            completed_by_username,
            completed_at,
            rejected_reason,
            metadata,
            created_at,
            updated_at,
            project_ids,
            projects,
            graph_assignments,
            links_out,
            links_in,
            comments,
            log
        FROM tasks.task_presentation
        WHERE id = $1
        LIMIT 1
        "#,
    )
    .bind(task_id.0)
    .fetch_optional(pool)
    .await
    .map_err(|err| db_err("Failed to query task presentation", err))
}

async fn append_task_log(
    pool: &PgPool,
    task_id: TaskId,
    actor: UserId,
    action: &str,
    from_state: Option<TaskState>,
    to_state: Option<TaskState>,
    details: serde_json::Value,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO tasks.task_log (
            id,
            task_id,
            actor_user_id,
            action,
            from_state,
            to_state,
            details
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(task_id.0)
    .bind(actor.0)
    .bind(action)
    .bind(from_state.map(|state| state.as_str().to_string()))
    .bind(to_state.map(|state| state.as_str().to_string()))
    .bind(details)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to append task log", err))?;

    Ok(())
}

async fn insert_task_comment(
    pool: &PgPool,
    task_id: TaskId,
    actor: UserId,
    body: String,
    metadata: serde_json::Value,
) -> Result<TaskComment> {
    let row = sqlx::query_as::<_, TaskCommentRow>(
        r#"
        INSERT INTO tasks.task_comments (
            id,
            task_id,
            author_user_id,
            body,
            metadata
        )
        VALUES ($1, $2, $3, $4, $5)
        RETURNING
            id,
            task_id,
            author_user_id,
            body,
            metadata,
            created_at,
            updated_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(task_id.0)
    .bind(actor.0)
    .bind(body)
    .bind(metadata)
    .fetch_one(pool)
    .await
    .map_err(|err| db_err("Failed to create task comment", err))?;

    Ok(to_task_comment(row))
}

async fn get_task_comments(pool: &PgPool, task_id: TaskId, limit: i64) -> Result<Vec<TaskComment>> {
    let rows = sqlx::query_as::<_, TaskCommentRow>(
        r#"
        SELECT
            id,
            task_id,
            author_user_id,
            body,
            metadata,
            created_at,
            updated_at
        FROM tasks.task_comments
        WHERE task_id = $1
          AND deleted_at IS NULL
        ORDER BY created_at ASC, id ASC
        LIMIT $2
        "#,
    )
    .bind(task_id.0)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query task comments", err))?;

    Ok(rows.into_iter().map(to_task_comment).collect())
}

async fn get_task_log_entries(
    pool: &PgPool,
    task_id: TaskId,
    limit: i64,
) -> Result<Vec<TaskLogEntry>> {
    let rows = sqlx::query_as::<_, TaskLogRow>(
        r#"
        SELECT
            id,
            task_id,
            actor_user_id,
            action,
            from_state,
            to_state,
            details,
            created_at
        FROM tasks.task_log
        WHERE task_id = $1
        ORDER BY created_at ASC, id ASC
        LIMIT $2
        "#,
    )
    .bind(task_id.0)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query task log", err))?;

    let mut entries = Vec::with_capacity(rows.len());
    for row in rows {
        entries.push(to_task_log(row)?);
    }

    Ok(entries)
}

mod milestones;
mod projects;
mod tasks;

pub use milestones::{
    create_milestone_with_roles, delete_milestone_with_roles, get_milestone_with_roles,
    list_milestones_with_roles, update_milestone_with_roles,
};
pub use projects::{
    create_project_with_roles, delete_project_with_roles, get_project_with_roles,
    list_projects_with_roles, update_project_with_roles,
};
pub use tasks::{
    create_task_comment_with_roles, create_task_link_with_roles, create_task_with_roles,
    delete_task_links_with_roles, delete_task_with_roles, get_task_log_with_roles,
    get_task_with_roles, list_task_comments_with_roles, list_tasks_with_roles,
    task_cascade_impact_with_roles, transition_task_with_roles, update_task_with_roles,
};
