use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use subseq_auth::prelude::AuthenticatedUser;

use crate::db;
use crate::models::{
    CreateTaskCommentPayload, CreateTaskLinkPayload, CreateTaskPayload, ListTasksQuery,
    TaskCascadeImpactQuery, TaskId, TransitionTaskPayload, UpdateTaskPayload,
};

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
    let tasks = db::list_tasks_with_roles(
        &app.pool(),
        auth_user.id(),
        query.project_id,
        query.assignee_user_id,
        query.state,
        query.archived,
        page,
        limit,
    )
    .await?;
    Ok(Json(tasks))
}

async fn get_task_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(task_id): Path<TaskId>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let task = db::get_task_with_roles(&app.pool(), auth_user.id(), task_id).await?;
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

pub(super) fn routes<S>() -> Router<S>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(
            "/task",
            get(list_tasks_handler::<S>).post(create_task_handler::<S>),
        )
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
}
