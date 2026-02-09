use anyhow::anyhow;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{
        StatusCode,
        header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use subseq_auth::prelude::AuthenticatedUser;

use crate::db;
use crate::models::{
    CreateTaskCommentPayload, CreateTaskLinkPayload, CreateTaskPayload, ListTasksQuery,
    TaskAttachmentFileId, TaskCascadeImpactQuery, TaskCommentId, TaskId, TransitionTaskPayload,
    UpdateTaskCommentPayload, UpdateTaskPayload,
};
use crate::{error::LibError, models::TaskFilterRule};

use super::{AppError, TasksApp};

async fn create_task_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Json(payload): Json<CreateTaskPayload>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let task = db::create_task_with_roles(&app.pool(), auth_user.id(), payload).await?;
    Ok((StatusCode::CREATED, Json(task)))
}

async fn list_tasks_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Query(query): Query<ListTasksQuery>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let (page, limit) = query.pagination();
    let filter_rule: Option<TaskFilterRule> = query.extract_filter_rule().map_err(|err| {
        AppError(LibError::invalid(
            "Invalid task filter rule",
            anyhow!("invalid task list filter rule: {}", err),
        ))
    })?;
    let tasks = db::list_tasks_with_roles(
        &app.pool(),
        auth_user.id(),
        query.project_id,
        query.project_ids.clone(),
        query.assignee_user_id,
        query.state,
        query.archived,
        query.query.clone(),
        query.order,
        filter_rule,
        page,
        limit,
    )
    .await?;
    Ok(Json(tasks))
}

async fn get_task_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_ref): Path<String>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let task = db::get_task_by_ref_with_roles(&app.pool(), auth_user.id(), &task_ref).await?;
    Ok(Json(task))
}

async fn update_task_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_id): Path<TaskId>,
    Json(payload): Json<UpdateTaskPayload>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let task = db::update_task_with_roles(&app.pool(), auth_user.id(), task_id, payload).await?;
    Ok(Json(task))
}

async fn delete_task_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_id): Path<TaskId>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    db::delete_task_with_roles(&app.pool(), auth_user.id(), task_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_task_link_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_id): Path<TaskId>,
    Json(payload): Json<CreateTaskLinkPayload>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let link =
        db::create_task_link_with_roles(&app.pool(), auth_user.id(), task_id, payload).await?;
    Ok((StatusCode::CREATED, Json(link)))
}

async fn create_task_comment_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_id): Path<TaskId>,
    Json(payload): Json<CreateTaskCommentPayload>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let comment =
        db::create_task_comment_with_roles(&app.pool(), auth_user.id(), task_id, payload).await?;
    Ok((StatusCode::CREATED, Json(comment)))
}

async fn list_task_comments_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_id): Path<TaskId>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let comments = db::list_task_comments_with_roles(&app.pool(), auth_user.id(), task_id).await?;
    Ok(Json(comments))
}

async fn update_task_comment_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path((task_id, comment_id)): Path<(TaskId, TaskCommentId)>,
    Json(payload): Json<UpdateTaskCommentPayload>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let comment = db::update_task_comment_with_roles(
        &app.pool(),
        auth_user.id(),
        task_id,
        comment_id,
        payload,
    )
    .await?;
    Ok(Json(comment))
}

async fn delete_task_comment_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path((task_id, comment_id)): Path<(TaskId, TaskCommentId)>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    db::delete_task_comment_with_roles(&app.pool(), auth_user.id(), task_id, comment_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_task_log_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_id): Path<TaskId>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let log = db::get_task_log_with_roles(&app.pool(), auth_user.id(), task_id).await?;
    Ok(Json(log))
}

async fn get_task_cascade_impact_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_id): Path<TaskId>,
    Query(query): Query<TaskCascadeImpactQuery>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let impact =
        db::task_cascade_impact_with_roles(&app.pool(), auth_user.id(), task_id, query).await?;
    Ok(Json(impact))
}

async fn delete_task_links_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path((task_id, other_task_id)): Path<(TaskId, TaskId)>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    db::delete_task_links_with_roles(&app.pool(), auth_user.id(), task_id, other_task_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn transition_task_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_id): Path<TaskId>,
    Json(payload): Json<TransitionTaskPayload>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let task =
        db::transition_task_with_roles(&app.pool(), auth_user.id(), task_id, payload).await?;
    Ok(Json(task))
}

async fn export_task_markdown_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_ref): Path<String>,
) -> Result<Response, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let (filename, markdown) =
        db::export_task_markdown_with_roles(&app.pool(), auth_user.id(), &task_ref).await?;
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/markdown; charset=utf-8")
        .header(
            CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .body(markdown.into())
        .map_err(|err| {
            AppError(LibError::unknown(
                "Failed to build export response",
                anyhow!("failed to build markdown export response: {}", err),
            ))
        })
}

async fn create_task_attachment_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path((task_id, file_id)): Path<(TaskId, TaskAttachmentFileId)>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let attachment =
        db::create_task_attachment_with_roles(&app.pool(), auth_user.id(), task_id, file_id)
            .await?;
    Ok((StatusCode::CREATED, Json(attachment)))
}

async fn list_task_attachments_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_id): Path<TaskId>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let attachments =
        db::list_task_attachments_with_roles(&app.pool(), auth_user.id(), task_id).await?;
    Ok(Json(attachments))
}

async fn delete_task_attachment_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path((task_id, file_id)): Path<(TaskId, TaskAttachmentFileId)>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    db::delete_task_attachment_with_roles(&app.pool(), auth_user.id(), task_id, file_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) fn routes<S>() -> Router<S>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(
            "/task",
            get(list_tasks_handler::<S>).post(create_task_handler::<S>),
        )
        .route("/task/list", get(list_tasks_handler::<S>))
        .route(
            "/task/{task_id}",
            get(get_task_handler::<S>)
                .put(update_task_handler::<S>)
                .delete(delete_task_handler::<S>),
        )
        .route("/task/{task_id}/link", post(create_task_link_handler::<S>))
        .route(
            "/task/{task_id}/comments",
            get(list_task_comments_handler::<S>).post(create_task_comment_handler::<S>),
        )
        .route(
            "/task/{task_id}/comments/{comment_id}",
            axum::routing::put(update_task_comment_handler::<S>)
                .delete(delete_task_comment_handler::<S>),
        )
        .route(
            "/task/{task_id}/attachment/{file_id}",
            post(create_task_attachment_handler::<S>).delete(delete_task_attachment_handler::<S>),
        )
        .route(
            "/task/{task_id}/attachment/list",
            get(list_task_attachments_handler::<S>),
        )
        .route(
            "/task/{task_id}/impact",
            get(get_task_cascade_impact_handler::<S>),
        )
        .route(
            "/task/{task_id}/link/{other_task_id}",
            delete(delete_task_links_handler::<S>),
        )
        .route("/task/{task_id}/log", get(get_task_log_handler::<S>))
        .route(
            "/task/{task_id}/transition",
            post(transition_task_handler::<S>),
        )
        .route(
            "/task/{task_id}/export",
            get(export_task_markdown_handler::<S>),
        )
        .route(
            "/task/{task_id}/export/markdown",
            get(export_task_markdown_handler::<S>),
        )
}
