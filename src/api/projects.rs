use axum::{
    Router,
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
};
use serde_json::json;
use subseq_util::api::{AppState, AuthenticatedUser, RejectReason};
use uuid::Uuid;
use zini_core::models::projects::*;
use zini_integrations::projects::*;
use zini_ownership::{
    Organization, Project, ProjectId, UpdateProject,
    projects::{
        ProjectPayload, UpdateImport, UpdateMetadata, add_project_to_org, get_active_project,
        get_project, list_org_projects, list_projects, project_metadata, update_import,
        update_metadata, update_project,
    },
};
use zini_swimlanes::models::set_active_swimlane_by_project_id;

async fn create_project_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Json(payload): Json<ProjectPayload>,
) -> Result<impl IntoResponse, RejectReason> {
    let project_id = ProjectId(Uuid::new_v4());
    let project_tx = app.router.announce();
    let project = create_project(
        app.db_pool.clone(),
        payload,
        auth_user.id(),
        project_id,
        project_tx,
    )
    .await?;
    Ok(Json(project))
}

async fn delete_project_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(project_id): Path<ProjectId>,
) -> Result<impl IntoResponse, RejectReason> {
    let user_id = auth_user.id();
    delete_project(app.db_pool.clone(), user_id, project_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn add_project_to_org_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(project_id): Path<ProjectId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    add_project_to_org(&mut conn, project_id, auth_user.id()).await?;
    Ok(StatusCode::OK)
}

async fn list_projects_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(page): Path<u32>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let projects = list_projects(&mut conn, auth_user.id(), page).await?;
    Ok(Json(projects))
}

async fn list_org_projects_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(page): Path<u32>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let projects = list_org_projects(&mut conn, auth_user.id(), page).await?;
    Ok(Json(projects))
}

async fn get_project_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(project_id): Path<ProjectId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let project = get_project(&mut conn, auth_user.id(), project_id).await?;
    Ok(Json(project))
}

async fn update_project_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(project_id): Path<ProjectId>,
    Json(payload): Json<UpdateProject>,
) -> Result<impl IntoResponse, RejectReason> {
    tracing::debug!("Updating project: {:?} with {:?}", project_id, payload);
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let project_tx = app.router.announce();
    let project =
        update_project(&mut conn, auth_user.id(), project_id, payload, project_tx).await?;
    Ok(Json(project))
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProjectIdJson {
    project_id: ProjectId,
}

async fn set_active_project_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Json(payload): Json<ProjectIdJson>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let project = match Project::get(&mut conn, payload.project_id).await {
        Some(proj) => proj,
        None => {
            return Err(RejectReason::not_found(format!(
                "Project {:?} not found",
                payload.project_id
            )));
        }
    };
    project
        .set_active_project(&mut conn, auth_user.id())
        .await
        .map_err(RejectReason::database_error)?;
    set_active_swimlane_by_project_id(&mut conn, auth_user.id(), payload.project_id).await?;

    Ok(StatusCode::NO_CONTENT)
}

async fn get_active_project_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let project = get_active_project(&mut conn, auth_user.id()).await?;
    Ok(Json(project))
}

async fn get_metadata_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(project_id): Path<ProjectId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    Project::api_get(&mut conn, auth_user.id(), project_id).await?;
    let metadata = project_metadata(&mut conn, project_id).await?;
    Ok(Json(metadata))
}

async fn update_metadata_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(project_id): Path<ProjectId>,
    Json(payload): Json<UpdateMetadata>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let org = Organization::get_user_org(&mut conn, auth_user.id())
        .await
        .ok_or_else(|| RejectReason::not_found("organization".to_string()))?;
    if !org.has_project(&mut conn, project_id).await {
        return Err(RejectReason::forbidden(
            auth_user.id(),
            "User does not have access to project",
        ));
    }
    update_metadata(&mut conn, auth_user.id(), project_id, payload).await?;
    Ok(Json(()))
}

async fn update_import_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path((project_id, repo_id)): Path<(ProjectId, Uuid)>,
    Json(payload): Json<UpdateImport>,
) -> Result<impl IntoResponse, RejectReason> {
    tracing::debug!(
        "Updating import for project: {:?} and repo: {:?} with {:?}",
        project_id,
        repo_id,
        payload
    );
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let import_tx = app
        .router
        .get_address()
        .ok_or_else(|| RejectReason::anyhow(anyhow::anyhow!("import router not set up")))?;
    let import_state = update_import(
        &mut conn,
        auth_user.id(),
        project_id,
        repo_id,
        payload,
        import_tx,
    )
    .await?;
    Ok(Json(import_state))
}

async fn get_priority_levels_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(project_id): Path<ProjectId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let project = Project::api_get(&mut conn, auth_user.id(), project_id).await?;
    let priority_levels = project.get_priority_levels(&mut conn).await;
    Ok(Json(json!({"priorityLevels": priority_levels})))
}

pub fn routes() -> Router<AppState> {
    tracing::debug!("Included projects routes");
    Router::new()
        .route("/project", post(create_project_handler))
        .route("/project/{project_id}", delete(delete_project_handler))
        .route("/project/{project_id}", get(get_project_handler))
        .route("/project/{project_id}", put(update_project_handler))
        .route(
            "/project/{project_id}/to_org",
            post(add_project_to_org_handler),
        )
        .route("/project/list/{page}", get(list_projects_handler))
        .route("/project/org_list/{page}", get(list_org_projects_handler))
        .route("/project/active", put(set_active_project_handler))
        .route("/project/active", get(get_active_project_handler))
        .route(
            "/project/{project_id}/import/{repo_id}",
            put(update_import_handler),
        )
        .route("/project/{project_id}/metadata", get(get_metadata_handler))
        .route(
            "/project/{project_id}/metadata",
            put(update_metadata_handler),
        )
        .route(
            "/project/{project_id}/priority_levels",
            get(get_priority_levels_handler),
        )
}
