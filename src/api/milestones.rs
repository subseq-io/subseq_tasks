use axum::{
    Router,
    extract::{Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
};
use subseq_util::api::{AppState, AuthenticatedUser, RejectReason};
use url::Url;

use crate::paged_response;

use super::PAGE_SIZE;
use zini_core::{
    models::milestones::*,
    tables::{MilestoneId, UpdateMilestone},
};

async fn create_milestone_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Json(payload): Json<NewMilestone>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let milestone_tx = app.router.announce();
    let milestone = create_milestone(&mut conn, auth_user.id(), payload, milestone_tx).await?;
    Ok(Json(milestone))
}

async fn get_milestone_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(milestone_id): Path<MilestoneId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let milestone = get_milestone(&mut conn, auth_user.id(), milestone_id).await?;
    Ok(Json(milestone))
}

fn append_url_query_params(base: &Url, page: u32, limit: u32, query: &SearchQuery) -> Url {
    let mut url = base.clone();
    {
        let mut query_pairs = url.query_pairs_mut();
        query_pairs.append_pair("page", page.to_string().as_str());
        query_pairs.append_pair("limit", limit.to_string().as_str());
        if let Some(search) = query.query.as_ref() {
            query_pairs.append_pair("query", search);
        }
    }
    url
}

async fn search_milestones_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let milestones = search_milestones(&mut conn, auth_user.id(), &query).await?;

    let page = query.page.unwrap_or(1);
    let limit = query.limit.unwrap_or(PAGE_SIZE);

    let mut base_url = Url::parse(&app.base_url)
        .map_err(|_| RejectReason::anyhow(anyhow::anyhow!("Invalid base URL")))?;
    base_url.set_path("/api/v1/milestone/list");
    let search_query_fn = Box::new(move |url: &Url, page: u32, limit: u32| {
        append_url_query_params(url, page, limit, &query)
    });
    let response = paged_response(milestones, page, limit, &base_url, Some(search_query_fn));
    Ok(Json(response))
}

async fn update_milestone_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(milestone_id): Path<MilestoneId>,
    Json(payload): Json<UpdateMilestone>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let milestone_tx = app.router.announce();
    update_milestone(
        &mut conn,
        auth_user.id(),
        milestone_id,
        payload,
        milestone_tx,
    )
    .await?;
    Ok(StatusCode::OK)
}

async fn delete_milestone_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(milestone_id): Path<MilestoneId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    delete_milestone(&mut conn, auth_user.id(), milestone_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub fn routes() -> Router<AppState> {
    tracing::debug!("Included milestones routes");
    Router::new()
        .route("/milestone", post(create_milestone_handler))
        .route("/milestone/{milestone_id}", get(get_milestone_handler))
        .route(
            "/milestone/{milestone_id}",
            delete(delete_milestone_handler),
        )
        .route("/milestone/list", get(search_milestones_handler))
        .route("/milestone/{milestone_id}", put(update_milestone_handler))
}
