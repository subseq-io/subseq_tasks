use anyhow::anyhow;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use subseq_auth::prelude::AuthenticatedUser;

use crate::db;
use crate::error::LibError;
use crate::models::{
    CreateMilestonePayload, ListMilestonesQuery, MilestoneId, MilestoneUpdate,
    UpdateMilestonePayload,
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
    let payload_for_event = payload.clone();
    let milestone = db::create_milestone_with_roles(&app.pool(), auth_user.id(), payload).await?;
    app.on_milestone_update(
        milestone.project_id,
        milestone.id,
        auth_user.id(),
        MilestoneUpdate::MilestoneCreate {
            payload: payload_for_event,
        },
    )
    .await?;
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
    let due_start = query.due_start.or(query.due).map(|value| value.naive_utc());
    let due_end = query.due_end.map(|value| value.naive_utc());
    if let (Some(start), Some(end)) = (due_start, due_end)
        && start > end
    {
        return Err(AppError(LibError::invalid(
            "Invalid milestone due-date range",
            anyhow!("dueStart must be earlier than or equal to dueEnd"),
        )));
    }

    let (page, limit) = query.pagination();
    let milestones = db::list_milestones_with_roles(
        &app.pool(),
        auth_user.id(),
        query.project_id,
        query.completed,
        query.query.clone(),
        due_start,
        due_end,
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
    let payload_for_event = payload.clone();
    let milestone =
        db::update_milestone_with_roles(&app.pool(), auth_user.id(), milestone_id, payload).await?;
    app.on_milestone_update(
        milestone.project_id,
        milestone.id,
        auth_user.id(),
        MilestoneUpdate::MilestoneUpdated {
            payload: payload_for_event,
        },
    )
    .await?;
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
    let existing = db::get_milestone_with_roles(&app.pool(), auth_user.id(), milestone_id).await?;
    db::delete_milestone_with_roles(&app.pool(), auth_user.id(), milestone_id).await?;
    app.on_milestone_update(
        existing.project_id,
        milestone_id,
        auth_user.id(),
        MilestoneUpdate::MilestoneArchive,
    )
    .await?;
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
