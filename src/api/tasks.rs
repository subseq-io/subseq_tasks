use std::{collections::HashMap, sync::Arc};

use axum::{
    Router,
    extract::{Json, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use serde_json::json;
use subseq_util::{
    UserId,
    api::{AppState, AuthenticatedUser, RejectReason},
};
use url::Url;
use uuid::Uuid;

use zini_core::{
    models::{
        continuation::Continuation,
        files::ApiUploadedFile,
        organizations::planning::{ChartPlanMetadata, PlannerBuilder, execute_planner},
        plans::{PlanTaskPayload, replan},
        tasks::*,
        tools::detect_duplicate,
    },
    tables::*,
};
use zini_integrations::TaskAssociation;
use zini_ownership::{
    ActiveProject, Organization, OrganizationId, PriorityLevel, Project, User,
    organizations::get_org_for_planner,
};

use crate::paged_response;

use super::PAGE_SIZE;

async fn infallable_expand_description(
    pool: Arc<DbPool>,
    user_id: UserId,
    org_id: OrganizationId,
    description: &str,
    description_generated: bool,
) -> String {
    if description_generated {
        return description.to_string();
    }
    if let Some(description) = expand_description(pool, user_id, org_id, description).await {
        description
    } else {
        description.to_string()
    }
}

async fn infallable_title_from_description(
    pool: Arc<DbPool>,
    user_id: UserId,
    title: &str,
    description: &str,
) -> String {
    if !title.is_empty() {
        return title.to_string();
    }

    if let Some(title) = title_from_description(pool, user_id, description).await {
        title
    } else {
        "Pending...".to_string()
    }
}

async fn infallable_priority_from_description(
    pool: Arc<DbPool>,
    user_id: UserId,
    org_id: OrganizationId,
    priority_levels: &[PriorityLevel],
    priority: Option<i32>,
    description: &str,
) -> i32 {
    if let Some(priority) = priority {
        return priority;
    }
    let priority_levels: Vec<_> = priority_levels
        .iter()
        .map(|level| (level.name.clone(), level.value))
        .collect();
    priority_from_description(pool, user_id, org_id, &priority_levels, description)
        .await
        .unwrap_or_default()
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum CreateTaskResponse {
    Task(DenormalizedTask),
    Continuation(Continuation),
}

async fn create_task_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Json(payload): Json<TaskPayload>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let TaskPayload {
        project_id,
        title,
        description,
        description_generated,
        assignee_id,
        priority,
        due_date,
    } = payload;

    let user_id = auth_user.id();
    let org = Organization::get_user_org(&mut conn, user_id)
        .await
        .ok_or_else(|| RejectReason::not_found(format!("Organization for {}", auth_user.id())))?;
    let org_id = org.id;
    let project = Project::api_get(&mut conn, auth_user.id(), project_id).await?;

    let priority_levels = project.get_priority_levels(&mut conn).await;
    let title = title.unwrap_or_default();
    use futures::join;
    let (title, description, priority) = join!(
        infallable_title_from_description(app.db_pool.clone(), user_id, &title, &description),
        infallable_expand_description(
            app.db_pool.clone(),
            user_id,
            org_id,
            &description,
            description_generated
        ),
        infallable_priority_from_description(
            app.db_pool.clone(),
            user_id,
            org_id,
            &priority_levels,
            priority,
            &description
        )
    );

    if let Some(duplicate_id) =
        detect_duplicate(app.db_pool.clone(), org_id, &title, &description).await
    {
        let task = Task::api_get(&mut conn, auth_user.id(), duplicate_id).await?;
        let duplicate = DenormalizedTask::denormalize(&mut conn, task.id, false).await?;
        tracing::debug!("Detected duplicate task: {}", duplicate_id);
        return Ok(Json(CreateTaskResponse::Continuation(Continuation::Task {
            duplicate,
            title,
            description,
            project_id,
            creator_id: user_id,
            assignee_id,
            priority,
            due_date,
        })));
    }
    let task = create_task_steps(
        app.db_pool.clone(),
        &app.router,
        auth_user.id(),
        project_id,
        title,
        description,
        assignee_id,
        priority,
        due_date,
    )
    .await?;

    Ok(Json(CreateTaskResponse::Task(task)))
}

async fn update_task_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(task_id): Path<TaskId>,
    Json(payload): Json<TaskUpdate>,
) -> Result<impl IntoResponse, RejectReason> {
    let base_url = app.base_url;

    let task_tx = app.router.announce();
    let notification_tx = app.router.announce();
    let email_tx = app.router.announce();
    tracing::debug!("Updating task: {} -> {:?}", task_id, payload);

    let task = update_task(
        app.db_pool.clone(),
        auth_user.id(),
        task_id,
        payload,
        Some(base_url),
        Some(task_tx),
        notification_tx,
        email_tx,
    )
    .await?;
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let task_tx = app.router.announce();

    if let Some(task) = task {
        let task_denorm = DenormalizedTask::denormalize(&mut conn, task.id, false).await?;
        task_tx.send(task_denorm.clone()).ok();
        Ok(Json(Some(task_denorm)))
    } else {
        Ok(Json(None))
    }
}

async fn replan_task_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(task_id): Path<TaskId>,
    Json(payload): Json<PlanTaskPayload>,
) -> Result<impl IntoResponse, RejectReason> {
    // TODO: Get timezone from organization settings? Currently it comes from local user time

    replan(
        app.db_pool.clone(),
        auth_user.id(),
        task_id,
        payload.today,
        payload.units,
        Tz::UTC,
    )
    .await?;

    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;

    let planned_task = PlannedTask::get(&mut conn, task_id)
        .await
        .ok_or_else(|| RejectReason::not_found("Planned task not found"))?;
    let plan_seconds = planned_task.planned_duration.num_seconds();

    if let Ok(org) = get_org_for_planner(&mut conn, auth_user.id()).await {
        let plans = PlannedTask::list_plans(&mut conn, task_id).await;
        for plan in plans {
            let metadata: ChartPlanMetadata = serde_json::from_value(plan.metadata.clone())
                .map_err(|err| {
                    RejectReason::bad_request(format!(
                        "Bad database entry for serde metadata: {}",
                        err
                    ))
                })?;
            let planner = PlannerBuilder::new(auth_user.id(), metadata, org.clone(), false);
            execute_planner(app.db_pool.clone(), plan, planner).await?;
        }
    }

    Ok(Json(json!({ "seconds": plan_seconds })))
}

async fn delete_task_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(task_id): Path<TaskId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let task = Task::api_get(&mut conn, auth_user.id(), task_id).await?;
    task.delete(&mut conn)
        .await
        .map_err(RejectReason::database_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_task_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Response, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let task_id = match Uuid::parse_str(&task_id) {
        Ok(task_id) => TaskId(task_id),
        Err(_) => {
            // Assume this is the task slug
            let mut query_dict = HashMap::new();
            query_dict.insert("slug".to_string(), serde_json::json!(task_id));
            Task::query(
                &mut conn,
                auth_user.id(),
                &query_dict,
                OrderBy::Created,
                1,
                1,
            )
            .await
            .map_err(RejectReason::anyhow)?
            .pop()
            .ok_or(RejectReason::not_found("Task not found"))?
        }
    };

    let task = Task::api_get(&mut conn, auth_user.id(), task_id).await?;
    let task_details = DenormalizedTaskDetails::denormalize::<TaskAssociation>(
        app.db_pool.clone(),
        auth_user.id(),
        &task,
    )
    .await?;

    Ok(Json(task_details).into_response())
}

fn append_url_query_params(base: &Url, page: u32, limit: u32, query: Option<&SearchQuery>) -> Url {
    let mut url = base.clone();
    {
        let mut query_pairs = url.query_pairs_mut();
        query_pairs.append_pair("page", page.to_string().as_str());
        query_pairs.append_pair("limit", limit.to_string().as_str());

        if let Some(query) = query {
            if let Some(search) = query.query.as_ref() {
                query_pairs.append_pair("query", search);
            }
            if let Some(project_ids) = query.project_ids.as_ref() {
                for proj_id in project_ids {
                    query_pairs.append_pair("projectId", proj_id.to_string().as_str());
                }
            }
            if let Some(order) = query.order.as_ref() {
                query_pairs.append_pair(
                    "order",
                    serde_json::to_string(order).expect("valid json").as_str(),
                );
            }
            if let Some(filter) = query.extract_filter_rule() {
                match filter {
                    FilterRule::NodeId(node_id) => {
                        query_pairs.append_pair("filterRule", "nodeId");
                        query_pairs.append_pair("filterRuleData", node_id.to_string().as_str());
                    }
                    FilterRule::Created(created) => {
                        query_pairs.append_pair("filterRule", "created");
                        query_pairs.append_pair("filterRuleData", created.to_string().as_str());
                    }
                    FilterRule::Updated(updated) => {
                        query_pairs.append_pair("filterRule", "updated");
                        query_pairs.append_pair("filterRuleData", updated.to_string().as_str());
                    }
                    FilterRule::AssignedTo(user_id) => {
                        query_pairs.append_pair("filterRule", "assignedTo");
                        query_pairs.append_pair("filterRuleData", user_id.to_string().as_str());
                    }
                    filter => {
                        query_pairs.append_pair(
                            "filterRule",
                            serde_json::to_string(&filter).expect("valid json").as_str(),
                        );
                    }
                }
            }
        }
    }
    url
}

async fn search_tasks_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Result<impl IntoResponse, RejectReason> {
    tracing::debug!("Searching tasks with query: {:?}", query);
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let denorm_tasks = search_tasks(auth_user.id(), &mut conn, &query).await?;

    let page = query.page.unwrap_or(1) as u32;
    let limit = query.limit.unwrap_or(PAGE_SIZE) as u32;
    let query_param_fn = Box::new(move |base: &Url, page: u32, limit: u32| {
        append_url_query_params(base, page, limit, Some(&query))
    });
    let mut base_url = app
        .base_url
        .clone()
        .parse::<Url>()
        .map_err(|_| RejectReason::anyhow(anyhow::anyhow!("Invalid base URL")))?;
    base_url.set_path("/api/v1/task/list");
    let response = paged_response(denorm_tasks, page, limit, &base_url, Some(query_param_fn));
    Ok(Json(response))
}

async fn run_task_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(task_id): Path<TaskId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let task = Task::api_get(&mut conn, auth_user.id(), task_id).await?;
    if task.assignee_id.is_none() {
        return Err(RejectReason::bad_request("Task must have an assignee"));
    }
    let submitted_user = User::api_get(&mut conn, auth_user.id()).await?;
    let active_project = ActiveProject::api_get(&mut conn, submitted_user.id).await?;
    let state = DenormalizedTaskState::denormalize(
        app.db_pool.clone(),
        submitted_user,
        active_project.project_id,
        &task,
    )
    .await?;

    let run = TaskRun { state };
    app.router.announce().send(run.clone()).ok();
    Ok(Json(run))
}

async fn get_comments_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(task_id): Path<TaskId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let task = Task::api_get(&mut conn, auth_user.id(), task_id).await?;
    let reply = denormalized_comments(&mut conn, &task).await?;
    Ok(Json(reply))
}

async fn export_task_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(task_id): Path<TaskId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let project_id = ActiveProject::api_get(&mut conn, auth_user.id())
        .await?
        .project_id;
    let project = Project::api_get(&mut conn, auth_user.id(), project_id).await?;
    let priority_levels = project.get_priority_levels(&mut conn).await;
    let priority_levels: Vec<_> = priority_levels
        .iter()
        .map(|level| (level.name.clone(), level.value))
        .collect();
    let task = Task::api_get(&mut conn, auth_user.id(), task_id).await?;

    let yaml_data = export_task(
        app.db_pool.clone(),
        auth_user.id(),
        &task,
        &priority_levels,
        true,
    )
    .await?;
    let filename = format!("task-{}.yaml", task_id);
    Ok(Response::builder()
        .header("Content-Type", "application/x-yaml")
        .header(
            "Content-Disposition",
            format!("attachment; filename={}", filename),
        )
        .body(yaml_data)
        .expect("valid response"))
}

/*
async fn import_task_handler(
    _auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(project_id): Path<ProjectId>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|err| RejectReason::bad_request(format!("Multipart error: {:?}", err)))?
    {
        let data = match field.bytes().await {
            Ok(data) => data,
            Err(err) => {
                return Err(RejectReason::bad_request(format!(
                    "Error reading field: {:?}",
                    err
                )))
            }
        };
        let data = data.to_vec();
        let data = String::from_utf8(data)
            .map_err(|err| RejectReason::bad_request(format!("{:?}", err)))?;
        let task_export: TaskExport = serde_yml::from_str(&data)
            .map_err(|err| RejectReason::bad_request(format!("{:?}", err)))?;
        let TaskExport { spec, comments } = task_export;

        let author_id = spec.task.author.id;
        let task_id = TaskId(Uuid::new_v4());

        let title = spec.task.title;
        let description = spec.task.description;
        let assignee_id = spec.task.assignee.map(|user| user.id);
        let priority = spec.task.priority;
        let due_date = spec.task.due_date.and_then(|date| {
            NaiveDateTime::parse_from_str(&date, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|date| date.and_utc())
        });

        let task = create_task(
            &mut conn,
            author_id,
            task_id,
            project_id,
            title,
            description,
            assignee_id,
            priority,
            due_date,
        )
        .await?;
        for comment in comments {
            task.add_comment(&mut conn, comment.author.id, &comment.content, None)
                .await
                .map_err(RejectReason::database_error)?;
        }
    }
    Ok(StatusCode::CREATED)
}
*/

async fn task_attachment_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path((task_id, file_id)): Path<(TaskId, Uuid)>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    Task::api_get(&mut conn, auth_user.id(), task_id).await?;
    let org = Organization::get_user_org(&mut conn, auth_user.id())
        .await
        .ok_or_else(|| RejectReason::not_found("Organization for task_attachment_handler"))?;
    FileUpload::api_get(&mut conn, file_id, org.id).await?;
    TaskAttachment::create(&mut conn, task_id, file_id)
        .await
        .map_err(RejectReason::database_error)?;

    Ok(StatusCode::CREATED)
}

async fn task_attachment_list_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(task_id): Path<TaskId>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    // Make sure task exists
    Task::api_get(&mut conn, auth_user.id(), task_id).await?;
    let attachments = TaskAttachment::list(&mut conn, task_id).await;
    let mut files = Vec::new();
    for attachment in attachments {
        let file = ApiUploadedFile::from_file_upload(&mut conn, attachment).await?;
        files.push(file);
    }
    Ok(Json(files))
}

async fn task_attachment_remove_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path((task_id, file_id)): Path<(TaskId, Uuid)>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    Task::api_get(&mut conn, auth_user.id(), task_id).await?;
    TaskAttachment::delete(&mut conn, task_id, file_id)
        .await
        .map_err(RejectReason::database_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn my_tasks_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
) -> Result<impl IntoResponse, RejectReason> {
    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let tasks = DenormalizedTask::next_pending_work(&mut conn, auth_user.id(), 10).await?;
    Ok(Json(tasks))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListQuery {
    page: Option<usize>,
    limit: Option<usize>,
}

async fn get_task_list_url_handler(
    auth_user: AuthenticatedUser,
    State(app): State<AppState>,
    Path(list_id): Path<Uuid>,
    Query(query): Query<ListQuery>,
) -> Result<impl IntoResponse, RejectReason> {
    let page = query.page.unwrap_or(1);
    let limit = query.limit.unwrap_or(PAGE_SIZE as usize);

    let mut conn = app.db_pool.get().await.map_err(RejectReason::pool_error)?;
    let tasks = fetch_task_url(&mut conn, auth_user.id(), list_id, page, limit).await?;

    let mut base_url = app
        .base_url
        .clone()
        .parse::<Url>()
        .map_err(|_| RejectReason::anyhow(anyhow::anyhow!("Invalid base URL")))?;
    base_url.set_path(&format!("/api/v1/task/list/{}", list_id));

    let response = paged_response(tasks, page as u32, limit as u32, &base_url, None);
    Ok(Json(response))
}

pub fn routes() -> Router<AppState> {
    tracing::debug!("Included tasks routes");
    Router::new()
        .route("/task", post(create_task_handler))
        .route("/task/{task_id}", put(update_task_handler))
        .route("/task/{task_id}", delete(delete_task_handler))
        .route("/task/{task_id}", get(get_task_handler))
        .route("/task/list", get(search_tasks_handler))
        .route("/task/mine", get(my_tasks_handler))
        .route("/task/list/{list_id}", get(get_task_list_url_handler))
        .route("/task/{task_id}/export", get(export_task_handler))
        .route("/task/{task_id}/replan", put(replan_task_handler))
        .route("/task/{task_id}/run", post(run_task_handler))
        .route(
            "/task/{task_id}/attachment/{file_id}",
            post(task_attachment_handler),
        )
        .route(
            "/task/{task_id}/attachment/list",
            get(task_attachment_list_handler),
        )
        .route(
            "/task/{task_id}/attachment/{file_id}",
            delete(task_attachment_remove_handler),
        )
        .route("/task/{task_id}/comments", get(get_comments_handler))
}
