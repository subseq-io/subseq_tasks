use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use subseq_auth::prelude::AuthenticatedUser;

use crate::db;
use crate::models::{CreateProjectPayload, ListQuery, ProjectId, UpdateProjectPayload};

use super::{AppError, TasksApp};

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

pub(super) fn routes<S>() -> Router<S>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
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
}
