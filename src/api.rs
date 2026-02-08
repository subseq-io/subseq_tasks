use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use subseq_auth::prelude::{AuthenticatedUser, ValidatesIdentity};

use crate::db;
use crate::error::{ErrorKind, LibError};
use crate::models::{
    CreateMilestonePayload, CreateProjectPayload, CreateTaskLinkPayload, CreateTaskPayload,
    ListMilestonesQuery, ListQuery, ListTasksQuery, MilestoneId, ProjectId, TaskId,
    TransitionTaskPayload, UpdateMilestonePayload, UpdateProjectPayload, UpdateTaskPayload,
};

#[derive(Debug)]
pub struct AppError(pub LibError);

impl From<LibError> for AppError {
    fn from(value: LibError) -> Self {
        Self(value)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match self.0.kind {
            ErrorKind::Database => StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::Forbidden => StatusCode::FORBIDDEN,
            ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
            ErrorKind::NotFound => StatusCode::NOT_FOUND,
            ErrorKind::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
        };

        tracing::error!(kind = ?self.0.kind, error = %self.0.source, "tasks api request failed");
        (status, self.0.public).into_response()
    }
}

pub trait HasPool {
    fn pool(&self) -> Arc<sqlx::PgPool>;
}

pub trait TasksApp: HasPool + ValidatesIdentity {}

async fn create_project_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Json(payload): Json<CreateProjectPayload>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let project = db::create_project_with_roles(&app.pool(), auth_user.id(), payload).await?;
    Ok((StatusCode::CREATED, Json(project)))
}

async fn list_projects_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Query(query): Query<ListQuery>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let (page, limit) = query.pagination();
    let projects = db::list_projects_with_roles(&app.pool(), auth_user.id(), page, limit).await?;
    Ok(Json(projects))
}

async fn get_project_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(project_id): Path<ProjectId>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let project = db::get_project_with_roles(&app.pool(), auth_user.id(), project_id).await?;
    Ok(Json(project))
}

async fn update_project_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(project_id): Path<ProjectId>,
    Json(payload): Json<UpdateProjectPayload>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let project =
        db::update_project_with_roles(&app.pool(), auth_user.id(), project_id, payload).await?;
    Ok(Json(project))
}

async fn delete_project_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(project_id): Path<ProjectId>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    db::delete_project_with_roles(&app.pool(), auth_user.id(), project_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_milestone_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Json(payload): Json<CreateMilestonePayload>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let milestone = db::create_milestone_with_roles(&app.pool(), auth_user.id(), payload).await?;
    Ok((StatusCode::CREATED, Json(milestone)))
}

async fn list_milestones_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Query(query): Query<ListMilestonesQuery>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let (page, limit) = query.pagination();
    let milestones = db::list_milestones_with_roles(
        &app.pool(),
        auth_user.id(),
        query.project_id,
        query.completed,
        page,
        limit,
    )
    .await?;
    Ok(Json(milestones))
}

async fn get_milestone_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(milestone_id): Path<MilestoneId>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let milestone = db::get_milestone_with_roles(&app.pool(), auth_user.id(), milestone_id).await?;
    Ok(Json(milestone))
}

async fn update_milestone_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(milestone_id): Path<MilestoneId>,
    Json(payload): Json<UpdateMilestonePayload>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    let milestone =
        db::update_milestone_with_roles(&app.pool(), auth_user.id(), milestone_id, payload).await?;
    Ok(Json(milestone))
}

async fn delete_milestone_handler<S>(
    State(app): State<S>,
    auth_user: AuthenticatedUser,
    Path(milestone_id): Path<MilestoneId>,
) -> Result<impl IntoResponse, AppError>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    db::delete_milestone_with_roles(&app.pool(), auth_user.id(), milestone_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

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

pub fn routes<S>() -> Router<S>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    tracing::info!("Registering route /project [GET,POST]");
    tracing::info!("Registering route /project/{{project_id}} [GET,PUT,DELETE]");
    tracing::info!("Registering route /milestone [GET,POST]");
    tracing::info!("Registering route /milestone/{{milestone_id}} [GET,PUT,DELETE]");
    tracing::info!("Registering route /task [GET,POST]");
    tracing::info!("Registering route /task/{{task_id}} [GET,PUT,DELETE]");
    tracing::info!("Registering route /task/{{task_id}}/link [POST]");
    tracing::info!("Registering route /task/{{task_id}}/link/{{other_task_id}} [DELETE]");
    tracing::info!("Registering route /task/{{task_id}}/transition [POST]");

    Router::new()
        .route(
            "/project",
            get(list_projects_handler::<S>).post(create_project_handler::<S>),
        )
        .route(
            "/project/{project_id}",
            get(get_project_handler::<S>)
                .put(update_project_handler::<S>)
                .delete(delete_project_handler::<S>),
        )
        .route(
            "/milestone",
            get(list_milestones_handler::<S>).post(create_milestone_handler::<S>),
        )
        .route(
            "/milestone/{milestone_id}",
            get(get_milestone_handler::<S>)
                .put(update_milestone_handler::<S>)
                .delete(delete_milestone_handler::<S>),
        )
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
        .route(
            "/task/{task_id}/link",
            axum::routing::post(create_task_link_handler::<S>),
        )
        .route(
            "/task/{task_id}/link/{other_task_id}",
            axum::routing::delete(delete_task_links_handler::<S>),
        )
        .route(
            "/task/{task_id}/transition",
            axum::routing::post(transition_task_handler::<S>),
        )
}
