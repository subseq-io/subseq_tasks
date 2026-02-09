use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use subseq_auth::prelude::AuthenticatedUser;

use crate::db;
use crate::models::{
    CreateMilestonePayload, ListMilestonesQuery, MilestoneId, UpdateMilestonePayload,
};

use super::{AppError, TasksApp};

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
        query.query.clone(),
        query.due.map(|value| value.naive_utc()),
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

pub(super) fn routes<S>() -> Router<S>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    Router::new()
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
}
