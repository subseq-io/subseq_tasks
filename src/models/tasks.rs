use std::boxed::Box;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use diesel::result::QueryResult;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use futures::Future;
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use subseq_scheduling::prelude::{Attention, Problem};
use subseq_util::{api::RejectReason, tables::DbPool, ChannelRouter, UserId};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task::spawn,
    time::timeout,
};
use uuid::Uuid;
use zini_graph::{
    DenormalizedPattern, DenormalizedThread, GraphEdge, GraphId, GraphInstance, GraphNode,
    GraphNodeId, NodeType,
};
use zini_ownership::{
    organizations::get_org_for_planner, projects::DenormalizedProject, OrganizationId, ProjectId,
};
use zini_ownership::{
    ActiveProject, Organization, OrganizationMembership, Project, UpdateProject, User,
};

use super::notifications::{notify_task_change, ApiNotification};
use super::plans::{today, Worker};
use super::projects::create_task_graph;
use super::{
    organizations::planning::{ChartPlanMetadata, NUM_DAYS_IN_WEEK},
    plans::link_new_planned_task,
};
use super::{MAX_PAGE_SIZE, PAGE_SIZE};
use crate::models::projects::{get_active_project, ActiveProjectData};
use crate::prompts::tools::TitleSummaries;
use crate::prompts::{
    embeddings::nearest_tasks,
    run::{handle_new_prompt, PromptRequest, WithTools},
    DETERMINE_RELATIONSHIP_PROMPT,
};
use crate::prompts::{
    EXPAND_DESCRIPTION_PROMPT, PRIORITY_FROM_DESCRIPTION_PROMPT, SET_TITLE_SUMMARIES_PROMPT,
    TITLE_FROM_DESCRIPTION_PROMPT,
};
use crate::{
    email::SubseqEmail,
    models::users::DenormalizedUser,
    prompts::embeddings::{fetch_documents, nearest_documents},
    prompts::{run::PromptResponse, validate_prompt_response},
    socket::interop::FrontEndMessage,
    tables::*,
};

pub(in crate::models) mod features {
    use crate::models::onboarding::DescriptionStep;
    pub(in crate::models) const FEATURE_ASSIGN_SELF: DescriptionStep =
        DescriptionStep::new("Assign this task to yourself");
    pub(in crate::models) const FEATURE_IN_PROGRESS: DescriptionStep =
        DescriptionStep::new("Move the task to in progress");
    pub(in crate::models) const FEATURE_CREATE_TASK: DescriptionStep =
        DescriptionStep::new("Create a new task by clicking the + button");
    pub(in crate::models) const FEATURE_AUTOMATIC_TITLE: DescriptionStep =
        DescriptionStep::new("Write a description like 'My first task!' notice after you save it the title is written for you");
    pub(in crate::models) const FEATURE_LINK_TASKS: DescriptionStep =
        DescriptionStep::new("Link the new task to this one");
    pub(in crate::models) const FEATURE_DUE_DATE: DescriptionStep =
        DescriptionStep::new("Add a due date by clicking the calendar icon");
    pub(in crate::models) const FEATURE_COMMENTS: DescriptionStep =
        DescriptionStep::new("Comment on the task");
    pub(in crate::models) const FEATURE_CLOSE_TASK: DescriptionStep =
        DescriptionStep::new("Close the task and you're done!");
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskPayload {
    pub project_id: ProjectId,
    pub title: Option<String>,
    pub description: String,
    pub description_generated: bool,
    pub assignee_id: Option<UserId>,
    pub priority: Option<i32>,
    pub due_date: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InnerTaskPayload {
    pub user_id: UserId,
    pub task_id: TaskId,
    pub project_id: ProjectId,
    pub title: Option<String>,
    pub description: String,
    pub tags: Vec<String>,
    pub components: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InnerUpdateTaskPayload {
    pub user_id: UserId,
    pub task_id: TaskId,
    pub update: TaskUpdate,
}

#[derive(Deserialize, Serialize, Default)]
pub struct TitleJson {
    pub title: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DescriptionJson {
    pub scratchpad: String,
    pub description: String,
    pub links: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TaskRun {
    pub state: DenormalizedTaskState,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum DirectionalLink {
    From(TaskLink),
    To(TaskLink),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedTaskLink {
    pub link: DenormalizedTask,
    pub link_type: TaskLinkType,
}

impl DenormalizedTaskLink {
    async fn denormalize(
        conn: &mut AsyncPgConnection,
        link: DirectionalLink,
    ) -> Result<Self, RejectReason> {
        let (task_id, link_type) = match link {
            DirectionalLink::From(task_link) => (task_link.task_from_id, task_link.link_type),
            DirectionalLink::To(task_link) => (task_link.task_to_id, task_link.link_type),
        };
        let task_deno = DenormalizedTask::denormalize(conn, task_id, true)
            .await
            .map_err(|_| RejectReason::not_found(format!("Task {} not found", task_id)))?;

        Ok(Self {
            link: task_deno,
            link_type,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SerializableTaskPlan {
    pub task_id: TaskId,

    pub expected_assignee_id: Option<UserId>,
    pub expected_start: DateTime<Utc>,
    pub assignment_dependency: Option<TaskId>,
    pub metadata: Value,

    pub actual_start: Option<DateTime<Utc>>,
    pub actual_assignee_id: Option<UserId>,

    pub planned_start: DateTime<Utc>,
    pub parent_task_id: Option<TaskId>,

    pub expected_duration: i64,
    pub actual_duration: Option<i64>,
    pub planned_duration: i64,

    pub explanation: Option<Explanation>,
}

impl From<PlannedTask> for SerializableTaskPlan {
    fn from(plan: PlannedTask) -> Self {
        Self {
            task_id: plan.task_id,
            expected_assignee_id: plan.expected_assignee_id,
            expected_start: plan.expected_start.and_utc(),
            assignment_dependency: plan.assignment_dependency,
            metadata: plan.metadata,
            actual_start: plan.actual_start.map(|d| d.and_utc()),
            actual_assignee_id: plan.actual_assignee_id,
            planned_start: plan.planned_start.and_utc(),
            parent_task_id: plan.parent_task_id,
            expected_duration: plan.expected_duration.num_seconds(),
            actual_duration: plan.actual_duration.map(|d| d.num_seconds()),
            planned_duration: plan.planned_duration.num_seconds(),
            explanation: plan
                .explanation
                .map(serde_json::from_value)
                .transpose()
                .unwrap(),
        }
    }
}

pub trait TaskAssociation {
    fn get(
        pool: Arc<DbPool>,
        task_id: TaskId,
    ) -> Pin<Box<dyn Future<Output = Option<Value>> + Send>>;
}

pub struct NoTaskAssociation;

impl TaskAssociation for NoTaskAssociation {
    fn get(
        _pool: Arc<DbPool>,
        _task_id: TaskId,
    ) -> Pin<Box<dyn Future<Output = Option<Value>> + Send>> {
        Box::pin(async { None })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedTaskDetailsMinimal {
    pub task: DenormalizedTaskMinimal,
    pub metadata: Vec<Value>,
    pub watchers: Vec<String>,
    pub state: String,
    pub valid_next_states: Vec<String>,
    pub links_out: Vec<String>,
    pub links_in: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<SerializableTaskPlan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub association: Option<Value>,
}

impl DenormalizedTaskDetailsMinimal {
    pub fn from_task_details(
        details: DenormalizedTaskDetails,
        priority_levels: &[(String, i32)],
    ) -> Self {
        let task = DenormalizedTaskMinimal::from_task(details.task, priority_levels);
        let watchers = details
            .watchers
            .iter()
            .map(|w| w.username.clone())
            .collect();
        let state = details.state.node_name.clone();
        let valid_next_states = details
            .valid_transitions
            .iter()
            .map(|s| s.node_name.clone())
            .collect();
        let links_out = details
            .links_out
            .iter()
            .map(|l| l.link.title.clone())
            .collect();
        let links_in = details
            .links_in
            .iter()
            .map(|l| l.link.title.clone())
            .collect();

        Self {
            task,
            metadata: details.metadata.clone(),
            watchers,
            state,
            valid_next_states,
            links_out,
            links_in,
            plan: details.plan.clone(),
            association: details.association,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedTaskDetails {
    pub task: DenormalizedTask,
    pub metadata: Vec<Value>,
    pub watchers: Vec<DenormalizedUser>,
    pub state: GraphNode,
    pub blockers: Vec<DenormalizedTask>,
    pub projects: Vec<DenormalizedProject>,
    pub links_out: Vec<DenormalizedTaskLink>,
    pub links_in: Vec<DenormalizedTaskLink>,
    pub valid_transitions: Vec<GraphNode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<SerializableTaskPlan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub association: Option<Value>,
}

impl DenormalizedTaskDetails {
    pub async fn denormalize_from_denormalized_task<T: TaskAssociation>(
        pool: Arc<DbPool>,
        user_id: UserId,
        task: &DenormalizedTask,
    ) -> Result<Self, RejectReason> {
        let mut conn = pool.get().await.map_err(RejectReason::pool_error)?;
        let task = Task::get(&mut conn, task.id)
            .await
            .ok_or_else(|| RejectReason::not_found(format!("Task {} not found", task.id)))?;
        Self::denormalize::<T>(pool, user_id, &task).await
    }

    pub async fn denormalize<T: TaskAssociation>(
        pool: Arc<DbPool>,
        user_id: UserId,
        task: &Task,
    ) -> Result<Self, RejectReason> {
        let mut conn = pool.get().await.map_err(RejectReason::pool_error)?;
        let graphs = task
            .graphs(&mut conn)
            .await
            .map_err(RejectReason::database_error)?;
        let metadata = task.metadata(&mut conn).await.ok().unwrap_or_default();

        let mut watchers = vec![];
        for watcher in task
            .watchers(&mut conn)
            .await
            .map_err(RejectReason::database_error)?
        {
            if let Some(user) = DenormalizedUser::denormalize(&mut conn, watcher.id, false).await {
                watchers.push(user);
            }
        }
        let state = TaskGraph::get_active_node(&mut conn, &graphs)
            .await
            .map_err(RejectReason::database_error)?;
        let valid_transitions = GraphEdge::edges(&mut conn, state.id)
            .await
            .map_err(RejectReason::database_error)?;

        let blocker_ids = Task::blockers(&mut conn, task.id).await.unwrap_or_default();
        let blockers = DenormalizedTask::denormalize_all(&mut conn, &blocker_ids, None, true)
            .await
            .unwrap_or_default();

        let mut links_out = vec![];
        for link in TaskLink::get_outgoing(&mut conn, task.id)
            .await
            .map_err(RejectReason::database_error)?
        {
            let link =
                DenormalizedTaskLink::denormalize(&mut conn, DirectionalLink::To(link)).await?;
            links_out.push(link);
        }
        let mut links_in = vec![];
        for task_link in TaskLink::get_incoming(&mut conn, task.id)
            .await
            .map_err(RejectReason::database_error)?
        {
            let link =
                DenormalizedTaskLink::denormalize(&mut conn, DirectionalLink::From(task_link))
                    .await?;
            links_in.push(link);
        }
        let plan = PlannedTask::get(&mut conn, task.id).await;
        let association = T::get(pool.clone(), task.id).await;

        let projects = TaskProject::list(&mut conn, task.id).await;
        let mut denorm_projects = vec![];
        for project in projects {
            if project.is_member(&mut conn, user_id).await {
                let project = DenormalizedProject::denormalize(&mut conn, &project).await;
                denorm_projects.push(project);
            }
        }

        Ok(Self {
            task: DenormalizedTask::denormalize(&mut conn, task.id, false).await?,
            metadata,
            watchers,
            state,
            projects: denorm_projects,
            blockers,
            links_out,
            links_in,
            valid_transitions,
            plan: plan.map(|p| p.into()),
            association,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedTaskMinimal {
    pub slug: String,
    pub created: DateTime<Utc>,
    pub title: String,
    pub description: String,
    pub author_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee_name: Option<String>,
    pub state: AbstractState,
    pub priority: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_date: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub milestone_name: Option<String>,
}

impl DenormalizedTaskMinimal {
    pub fn from_task(task: DenormalizedTask, priority_levels: &[(String, i32)]) -> Self {
        let author_name = task.author.username.clone();
        let assignee_name = task.assignee.map(|a| a.username.clone());
        let priority = match priority_levels.iter().find(|(_, p)| *p > task.priority) {
            Some((name, _)) => name.clone(),
            None => "Normal".to_string(),
        };
        let milestone_name = task.milestone.map(|m| m.name);
        Self {
            slug: task.slug,
            created: task.created,
            title: task.title,
            description: task.description,
            author_name,
            assignee_name,
            state: task.abstract_state,
            priority: priority.to_string(),
            due_date: task.due_date,
            milestone_name,
        }
    }
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedTaskState {
    pub submitted_by: DenormalizedUser,
    pub active_project: ProjectId,
    pub details: DenormalizedTaskDetails,
}

impl DenormalizedTaskState {
    pub async fn denormalize(
        pool: Arc<DbPool>,
        submitted_by: User,
        active_project: ProjectId,
        task: &Task,
    ) -> Result<Self, RejectReason> {
        let user_id = submitted_by.id;
        let mut conn = pool.get().await.map_err(RejectReason::pool_error)?;
        let submitted_by = DenormalizedUser::denormalize(&mut conn, user_id, false)
            .await
            .ok_or_else(|| RejectReason::not_found(format!("User {} not found", user_id)))?;
        Ok(Self {
            submitted_by,
            active_project,
            details: DenormalizedTaskDetails::denormalize::<NoTaskAssociation>(
                pool.clone(),
                user_id,
                task,
            )
            .await?,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedComment {
    pub id: Uuid,
    pub author: DenormalizedUser,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl DenormalizedComment {
    pub async fn denormalize(
        conn: &mut AsyncPgConnection,
        comment: Comment,
    ) -> Result<Self, RejectReason> {
        let author = DenormalizedUser::denormalize(conn, comment.author_id, false)
            .await
            .ok_or_else(|| {
                RejectReason::not_found(format!("User {} not found", comment.author_id))
            })?;
        Ok(Self {
            id: comment.id,
            author,
            content: comment.content,
            created_at: comment.created_at.and_utc(),
            updated_at: comment.updated_at.and_utc(),
        })
    }
}

fn deserialize_project_ids<'de, D>(deserializer: D) -> Result<Option<Vec<ProjectId>>, D::Error>
where
    D: Deserializer<'de>,
{
    // Deserialize an optional string (e.g. "uuid1,uuid2,uuid3")
    let opt = Option::<String>::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(s) => {
            let mut ids: Vec<ProjectId> = Vec::new();
            for raw in s.split(',') {
                let trimmed: &str = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let uuid: Uuid = Uuid::parse_str(trimmed).map_err(|e| {
                    serde::de::Error::custom(format!(
                        "Invalid UUID format in project_ids: {} - {}",
                        trimmed, e
                    ))
                })?;
                ids.push(ProjectId(uuid));
            }
            Ok(Some(ids))
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchQuery {
    #[serde(deserialize_with = "deserialize_project_ids")]
    pub project_ids: Option<Vec<ProjectId>>,
    pub query: Option<String>,
    pub order: Option<OrderBy>,
    pub page: Option<u32>,
    pub limit: Option<u32>,
    pub filter_rule: Option<String>,
    pub filter_rule_data: Option<String>,
}

impl SearchQuery {
    pub fn extract_filter_rule(&self) -> Option<FilterRule> {
        if let Some(filter_rule) = self.filter_rule.as_ref() {
            let json_data =
                json!({"filterRule": filter_rule, "filterRuleData": self.filter_rule_data});
            serde_json::from_value(json_data).ok()
        } else {
            None
        }
    }
}

#[derive(Serialize)]
pub struct QueryReply {
    pub tasks: Vec<DenormalizedTask>,
    pub total: Option<usize>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryPayload {
    pub page: u32,
    pub limit: u32,
    pub query: HashMap<String, Value>,
    pub order: OrderBy,
}

pub async fn create_task(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    task_id: TaskId,
    project_id: ProjectId,
    title: String,
    description: String,
    assignee_id: Option<UserId>,
    priority: i32,
    due_date: Option<DateTime<Utc>>,
) -> Result<Task, RejectReason> {
    let project = Project::api_get(conn, user_id, project_id).await?;
    let user = User::api_get(conn, user_id).await?;
    let mut task = Task::create(conn, task_id, project.id, &title, &description, user.id)
        .await
        .map_err(|err| {
            tracing::error!("Task creation failed: {:?}", err);
            RejectReason::conflict("Task creation failed")
        })?;

    if let Some(assignee_id) = assignee_id {
        if let Err(e) = task
            .update(
                conn,
                user_id,
                TaskUpdate::AssignOther {
                    user_id: assignee_id,
                },
            )
            .await
            .map_err(|err| {
                tracing::error!("Task assignee update failed: {:?}", err);
                RejectReason::database_error(err)
            })
        {
            tracing::error!("Task assignee update failed: {:?}", e);
        };
    }
    if let Some(due_date) = due_date {
        task.update(
            conn,
            user_id,
            TaskUpdate::ChangeDueDate {
                due_date: Some(due_date),
            },
        )
        .await
        .map_err(|err| {
            tracing::error!("Task due date update failed: {:?}", err);
            RejectReason::database_error(err)
        })?;
    }
    task.update(
        conn,
        user_id,
        TaskUpdate::ChangePriority { value: priority },
    )
    .await
    .map_err(|err| {
        tracing::error!("Task priority update failed: {:?}", err);
        RejectReason::database_error(err)
    })?;

    let graph_id = if let Some(graph_id) = project.task_graph_id {
        GraphId(graph_id)
    } else {
        let org = Organization::get_user_org(conn, user_id)
            .await
            .ok_or_else(|| {
                RejectReason::not_found(format!("Organization for user {} not found", user_id))
            })?;
        let graph = create_task_graph(conn, org.id, project.id).await?;
        project
            .update(
                conn,
                user_id,
                UpdateProject::TaskGraph {
                    graph_id: graph.id.0,
                },
            )
            .await
            .map_err(RejectReason::database_error)?;
        graph.id
    };
    Task::insert_into_graph(conn, task.id, &task.slug, graph_id, project_id)
        .await
        .map_err(RejectReason::database_error)?;

    Ok(task)
}

pub async fn search_tasks(
    user_id: UserId,
    conn: &mut AsyncPgConnection,
    query: &SearchQuery,
) -> Result<Vec<DenormalizedTask>, RejectReason> {
    let project_ids = match query.project_ids.as_ref() {
        Some(project_ids) => project_ids.clone(),
        None => vec![ActiveProject::api_get(conn, user_id).await?.project_id],
    };

    let limit = query.limit.unwrap_or(PAGE_SIZE).min(MAX_PAGE_SIZE);
    let page = query.page.unwrap_or(1);
    let order = query.order.unwrap_or(OrderBy::Created);
    let filter_rule = query.extract_filter_rule();
    let query = query.query.clone();
    let mut tasks: Vec<TaskId> = vec![];

    if let Some(query) = query {
        tasks.extend(
            Task::search(
                conn,
                Some(&query),
                user_id,
                order,
                filter_rule.clone(),
                project_ids.clone(),
                page,
                limit,
            )
            .await
            .map_err(RejectReason::database_error)?,
        );

        let tasks_from_tags = Task::search_tags(
            conn,
            &query,
            user_id,
            order,
            filter_rule,
            project_ids,
            page,
            limit,
        )
        .await
        .map_err(RejectReason::database_error)?;

        // Merge the two sets of tasks
        let id_set: std::collections::HashSet<TaskId> = tasks.iter().copied().collect();
        tasks.extend(
            tasks_from_tags
                .into_iter()
                .filter(|task| !id_set.contains(task)),
        );
    } else {
        let mut query_dict = HashMap::new();
        query_dict.insert("project_ids".to_string(), serde_json::json!(project_ids));
        if let Some(filter_rule) = filter_rule {
            query_dict.insert("filter_rule".to_string(), serde_json::json!(filter_rule));
        }
        tasks.extend(
            Task::query(conn, user_id, &query_dict, order, page, limit)
                .await
                .map_err(RejectReason::anyhow)?,
        );
    };

    DenormalizedTask::denormalize_all(conn, &tasks, None, false).await
}

#[derive(Debug, Clone)]
pub struct TaskChanged {
    pub user_id: UserId,
    pub task_id: TaskId,
    pub update: Option<TaskUpdate>,
    pub previous: Option<TaskUpdate>,
}

pub async fn update_task(
    pool: Arc<DbPool>,
    user_id: UserId,
    task_id: TaskId,
    update: TaskUpdate,
    base_url: Option<String>,
    task_tx: Option<broadcast::Sender<TaskChanged>>,
    notification_tx: broadcast::Sender<ApiNotification>,
    email_tx: broadcast::Sender<SubseqEmail>,
) -> Result<Option<Task>, RejectReason> {
    let mut conn = pool.get().await.map_err(RejectReason::pool_error)?;
    let mut task = Task::api_get(&mut conn, user_id, task_id).await?;
    match task.update(&mut conn, user_id, update.clone()).await {
        Ok(Some(previous)) => {
            if let TaskUpdate::ChangeTitle { title } = &update {
                let pool = pool.clone();
                let title = title.clone();
                spawn(async move {
                    title_summaries(user_id, pool, task_id, &title).await;
                });
            }

            if let Some(task_tx) = task_tx {
                task_tx
                    .send(TaskChanged {
                        user_id,
                        task_id,
                        update: Some(update.clone()),
                        previous: Some(previous),
                    })
                    .ok();
            }

            if let Some(base_url) = base_url {
                notify_task_change(
                    &mut conn,
                    user_id,
                    task_id,
                    &update,
                    &base_url,
                    notification_tx,
                    email_tx,
                )
                .await;
            }
            Ok(Some(task))
        }
        Ok(None) => Ok(None),
        Err(err) => {
            tracing::error!("Update error: {}", err);
            Err(RejectReason::database_error(err))
        }
    }
}

pub async fn title_from_description(
    pool: Arc<DbPool>,
    user_id: UserId,
    description: &str,
) -> Option<String> {
    let prompt_request = PromptRequest {
        prompt_name: TITLE_FROM_DESCRIPTION_PROMPT.to_string(),
        user_prompt: description.to_string(),
        with_tools: None,
    };
    let response = timeout(
        Duration::from_secs(30),
        handle_new_prompt(pool, user_id, prompt_request),
    )
    .await;
    Some(validate_prompt_response::<TitleJson>(response)?.title)
}

pub async fn title_summaries(
    user_id: UserId,
    pool: Arc<DbPool>,
    task_id: TaskId,
    title: &str,
) -> bool {
    let title_summaries = Arc::new(TitleSummaries::new(pool.clone(), task_id));
    let prompt_request = PromptRequest {
        prompt_name: SET_TITLE_SUMMARIES_PROMPT.to_string(),
        user_prompt: title.to_string(),
        with_tools: Some(WithTools {
            functions: vec![title_summaries],
        }),
    };
    let response = timeout(
        Duration::from_secs(30),
        handle_new_prompt(pool, user_id, prompt_request),
    )
    .await;

    match response {
        Ok(Ok(PromptResponse::ToolResults(_))) => {
            // There is no additional information in the tool results if they succeed
            true
        }
        Ok(Ok(ok)) => {
            tracing::warn!(
                "Unexpected response type from title summaries prompt: {:?}",
                ok
            );
            false
        }
        Ok(Err(err)) => {
            tracing::error!("Error in title summary response: {}", err);
            false
        }
        Err(_) => {
            tracing::warn!("Timed out waiting for title summaries");
            false
        }
    }
}

const NEAREST_DOCUMENTS_N: usize = 100;
const NEAREST_DOCUMENTS_N_LIMIT: usize = 10;
const NEAREST_DOCUMENTS_SIMILARITY: f32 = 0.4;

pub async fn expand_description(
    pool: Arc<DbPool>,
    user_id: UserId,
    org_id: OrganizationId,
    description: &str,
) -> Option<String> {
    let mut conn = pool.get().await.ok()?;
    let user_prompt = UserPrompt::get(&mut conn, org_id, UserPromptType::TaskDescription)
        .await
        .map(|prompt| prompt.prompt)
        .unwrap_or_default();

    drop(conn);
    let documents = nearest_documents(
        pool.clone(),
        org_id,
        NEAREST_DOCUMENTS_N,
        0,
        NEAREST_DOCUMENTS_SIMILARITY,
        description,
    )
    .await
    .unwrap_or_default()
    .into_iter()
    .take(NEAREST_DOCUMENTS_N_LIMIT)
    .collect::<Vec<_>>();
    let documents = fetch_documents(pool.clone(), user_id, documents)
        .await
        .unwrap_or_default();

    let prompt_string = serde_json::to_string(&serde_json::json!({
        "additional_requirements": user_prompt,
        "related_documents": documents,
        "description": description,
    }))
    .expect("Valid JSON");
    let prompt_request = PromptRequest {
        prompt_name: EXPAND_DESCRIPTION_PROMPT.to_string(),
        user_prompt: prompt_string,
        with_tools: None,
    };
    let response = timeout(
        Duration::from_secs(60),
        handle_new_prompt(pool, user_id, prompt_request),
    )
    .await;
    validate_prompt_response::<DescriptionJson>(response).map(|json| json.description)
}

pub async fn priority_from_description(
    pool: Arc<DbPool>,
    user_id: UserId,
    org_id: OrganizationId,
    priority_levels: &[(String, i32)],
    description: &str,
) -> Option<i32> {
    let mut conn = pool.get().await.ok()?;
    let user_prompt = UserPrompt::get(&mut conn, org_id, UserPromptType::TaskPriority)
        .await
        .map(|prompt| prompt.prompt)
        .unwrap_or_default();

    let priorities = serde_json::to_string(&serde_json::json!({
        "priorities": priority_levels.iter().map(|(name, _)| name.as_str()).collect::<Vec<&str>>(),
        "additional_requirements": user_prompt,
        "description": description,
    }))
    .expect("Valid JSON");

    let prompt_request = PromptRequest {
        prompt_name: PRIORITY_FROM_DESCRIPTION_PROMPT.to_string(),
        user_prompt: priorities,
        with_tools: None,
    };
    drop(conn);
    let response = timeout(
        Duration::from_secs(30),
        handle_new_prompt(pool, user_id, prompt_request),
    )
    .await;

    #[derive(Deserialize)]
    struct PriorityJson {
        priority: String,
    }
    let priority = validate_prompt_response::<PriorityJson>(response)?;

    let priority = priority_levels
        .iter()
        .find(|(name, _)| name == &priority.priority)?;
    Some(priority.1)
}

pub type QueryChannel = (
    QueryPayload,
    oneshot::Sender<Result<QueryReply, RejectReason>>,
);

fn transform_segments(segments: &[StoredEventSegment], offset: i64) -> Vec<StoredEventSegment> {
    segments
        .iter()
        .map(|segment| StoredEventSegment {
            start: segment.start + offset as f64,
            duration: segment.duration,
        })
        .collect()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedTaskPlan {
    pub task_id: TaskId,
    pub title: String,
    pub title_summaries: Vec<String>,
    pub slug: String,
    pub state: AbstractState,
    pub priority: i64,
    pub attention: Attention,
    pub row: u32,

    pub assignee: Option<DenormalizedUser>,
    pub expected_start: DateTime<Utc>,
    pub actual_start: Option<DateTime<Utc>>,
    pub expected_duration: i64,
    pub actual_duration: Option<i64>,

    pub dependencies: Vec<TaskId>,
    pub children: Vec<TaskId>,
    pub segments: Vec<StoredEventSegment>,
    pub due_date: Option<DateTime<Utc>>,

    pub explanation: Option<Explanation>,
}

impl DenormalizedTaskPlan {
    async fn denormalize(
        conn: &mut AsyncPgConnection,
        plan_id: Uuid,
        task_plan: PlannedTask,
    ) -> Result<DenormalizedTaskPlan, RejectReason> {
        let mut dependencies = vec![];
        let mut children = vec![];

        let chart = ChartPlan::get(conn, plan_id)
            .await
            .ok_or_else(|| RejectReason::not_found(format!("ChartPlan {}", plan_id)))?;

        let chart_task_plan = ChartPlanTask::get(conn, plan_id, task_plan.task_id)
            .await
            .map_err(|_| {
                RejectReason::not_found(format!(
                    "ChartPlanTask ({}, {})",
                    plan_id, task_plan.task_id
                ))
            })?;

        let task = Task::get(conn, task_plan.task_id)
            .await
            .ok_or_else(|| RejectReason::not_found(format!("Task {}", task_plan.task_id)))?;
        let title_summaries = task.title_summaries(conn).await;

        for task_link in TaskLink::get_outgoing(conn, task_plan.task_id)
            .await
            .map_err(RejectReason::database_error)?
        {
            if task_link.link_type == TaskLinkType::DependsOn {
                dependencies.push(task_link.task_to_id)
            }
        }

        for task_link in TaskLink::get_incoming(conn, task_plan.task_id)
            .await
            .map_err(RejectReason::database_error)?
        {
            if task_link.link_type == TaskLinkType::SubtaskOf {
                children.push(task_link.task_from_id)
            }
        }

        let assignee = match task_plan.actual_assignee_id {
            Some(user_id) => DenormalizedUser::denormalize(conn, user_id, false).await,
            None => match task_plan.expected_assignee_id {
                Some(user_id) => DenormalizedUser::denormalize(conn, user_id, false).await,
                None => None,
            },
        };
        let (slug, title, state) = Task::get_plan_data(conn, task_plan.task_id)
            .await
            .map_err(RejectReason::database_error)?;

        let chart_plan_metadata: ChartPlanMetadata = serde_json::from_value(chart.metadata)
            .map_err(|err| RejectReason::bad_request(format!("Invalid metadata: {}", err)))?;
        let time_zone = chart_plan_metadata.time_zone;

        let metadata = match serde_json::from_value::<ChartPlanTaskMetadata>(task_plan.metadata) {
            Ok(metadata) => metadata,
            Err(err) => {
                return Err(RejectReason::bad_request(format!(
                    "Invalid metadata: {}",
                    err
                )));
            }
        };
        let start_date = metadata.plan_start.unwrap_or_else(|| today(&time_zone));
        let today = today(&time_zone);

        let offset = start_date - today;
        let offset_units = match metadata.units {
            None => offset.num_hours(),
            Some(seconds) => offset.num_seconds() / seconds,
        };
        let segments = transform_segments(&metadata.segments, offset_units);

        Ok(Self {
            task_id: task_plan.task_id,
            slug,
            title,
            title_summaries,
            state,
            attention: Attention::default(), // TODO
            priority: task.priority as i64,
            row: chart_task_plan.row as u32,
            assignee,
            expected_start: task_plan.expected_start.and_utc(),
            actual_start: task_plan.actual_start.map(|d| d.and_utc()),
            expected_duration: task_plan.expected_duration.num_seconds(),
            actual_duration: task_plan
                .actual_duration
                .map(|duration| duration.num_seconds()),
            dependencies,
            children,
            segments,
            due_date: task.due_date.map(|d| d.and_utc()),
            explanation: task_plan
                .explanation
                .map(serde_json::from_value)
                .transpose()
                .unwrap(),
        })
    }
}

pub async fn update_task_plan(
    conn: &mut AsyncPgConnection,
    plan_id: Uuid,
    task_id: TaskId,
    user_id: UserId,
    update: ChartPlanUpdate,
) -> Result<(), RejectReason> {
    get_org_for_planner(conn, user_id).await?;
    Task::get_secure(conn, user_id, task_id)
        .await
        .map_err(|_| RejectReason::not_found(format!("Task {} for {}", task_id, user_id)))?;
    tracing::debug!(
        "Updating PlannedTask{{plan_id: {}, task_id: {}}} -> {:?}",
        plan_id,
        task_id,
        update
    );
    PlannedTask::update(conn, task_id, update)
        .await
        .map_err(RejectReason::database_error)
        .map(|_| ())
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedChartPlan {
    pub plan_id: Uuid,
    pub org_id: OrganizationId,
    pub created_at: DateTime<Utc>,
    pub created_by: DenormalizedUser,
    pub projects: Vec<DenormalizedProject>,
    pub metadata: ChartPlanMetadata,
    pub processing_finished_at: Option<DateTime<Utc>>,
}

impl DenormalizedChartPlan {
    pub async fn denormalize(
        conn: &mut AsyncPgConnection,
        plan: ChartPlan,
    ) -> Result<Self, RejectReason> {
        let metadata: ChartPlanMetadata = serde_json::from_value(plan.metadata)
            .map_err(|err| RejectReason::bad_request(format!("Invalid metadata: {}", err)))?;
        let mut projects = vec![];
        for project_id in metadata.project_ids.iter() {
            let project = match Project::get(conn, *project_id).await {
                Some(project) => project,
                None => continue,
            };
            projects.push(DenormalizedProject::denormalize(conn, &project).await);
        }

        let created_by = DenormalizedUser::denormalize(conn, plan.created_by, false)
            .await
            .ok_or_else(|| {
                RejectReason::not_found(format!("User {} not found", plan.created_by))
            })?;

        Ok(Self {
            plan_id: plan.id,
            org_id: plan.org_id,
            created_at: Utc.from_utc_datetime(&plan.created_at),
            created_by,
            projects,
            metadata,
            processing_finished_at: plan
                .processing_finished_at
                .map(|d| Utc.from_utc_datetime(&d)),
        })
    }
}

pub async fn get_chart_plans(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
) -> Result<Vec<DenormalizedChartPlan>, RejectReason> {
    let mut chart_plans = vec![];
    let org = Organization::get_user_org(conn, user_id)
        .await
        .ok_or_else(|| RejectReason::not_found(format!("Organization for User {}", user_id)))?;
    for plan in ChartPlan::list(conn, org.id)
        .await
        .map_err(RejectReason::database_error)?
    {
        let plan = DenormalizedChartPlan::denormalize(conn, plan).await;
        if let Ok(plan) = plan {
            chart_plans.push(plan);
        }
    }
    Ok(chart_plans)
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ChartPlanTasks {
    pub plan_id: Uuid,
    pub completed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_on: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub seed: String,
    pub tasks: Vec<DenormalizedTaskPlan>,
    pub workers: Vec<Worker>,

    pub problems: Vec<Problem>,
    pub utilization: HashMap<UserId, f64>,

    pub work_days: [bool; NUM_DAYS_IN_WEEK],
    pub work_start: u8,
    pub work_end: u8,
}

pub async fn get_task_plans(
    conn: &mut AsyncPgConnection,
    plan_id: Uuid,
) -> Result<ChartPlanTasks, RejectReason> {
    let plan = ChartPlan::get(conn, plan_id)
        .await
        .ok_or_else(|| RejectReason::not_found(format!("ChartPlan {}", plan_id)))?;

    let mut plan_response = ChartPlanTasks {
        plan_id,
        completed: plan.processing_finished_at.is_some(),
        completed_on: plan
            .processing_finished_at
            .map(|d| Utc.from_utc_datetime(&d)),
        error: plan.error.clone(),
        seed: hex::encode(&plan.seed),
        tasks: vec![],
        workers: Worker::denormalize_list(conn, &plan).await?,
        problems: vec![],
        utilization: HashMap::new(),
        work_days: [false, true, true, true, true, true, false],
        work_start: 9,
        work_end: 17,
    };

    if !plan_response.completed {
        return Ok(plan_response);
    }

    match serde_json::from_value::<ChartPlanMetadata>(plan.metadata) {
        Ok(metadata) => {
            for utilization in metadata.utilization.unwrap_or_default() {
                plan_response
                    .utilization
                    .insert(UserId(utilization.worker_id), utilization.utilization_rate);
            }
            plan_response
                .problems
                .extend(metadata.problems.unwrap_or_default().into_iter());
            plan_response.work_start = metadata.working_start;
            plan_response.work_end = metadata.working_end;
            plan_response.work_days = metadata.working_days;
        }
        Err(err) => {
            plan_response.problems.push(Problem {
                event_id: plan.id,
                problem: format!("Invalid metadata: {}", err),
            });
        }
    }

    for task_plan in ChartPlanTask::list(conn, plan_id)
        .await
        .map_err(RejectReason::database_error)?
    {
        let id = task_plan.task_id;
        let task_plan = match DenormalizedTaskPlan::denormalize(conn, plan_id, task_plan).await {
            Ok(task_plan) => task_plan,
            Err(err) => {
                plan_response.problems.push(Problem {
                    event_id: id.0,
                    problem: format!("TaskPlan: {:?}", err),
                });
                tracing::warn!("TaskPlan: {:?}", err);
                continue;
            }
        };
        plan_response.tasks.push(task_plan);
    }
    Ok(plan_response)
}

pub async fn get_task_plan(
    conn: &mut AsyncPgConnection,
    plan_id: Uuid,
    task_id: TaskId,
) -> Result<DenormalizedTaskPlan, RejectReason> {
    let task_plan = PlannedTask::get(conn, task_id)
        .await
        .ok_or_else(|| RejectReason::not_found(format!("ChartPlanTask {} {}", plan_id, task_id)))?;
    DenormalizedTaskPlan::denormalize(conn, plan_id, task_plan).await
}

pub async fn new_tasks_front_end(
    user_id: UserId,
    db_pool: Arc<DbPool>,
    mut rx: broadcast::Receiver<DenormalizedTask>,
    update_tx: mpsc::Sender<FrontEndMessage>,
) {
    loop {
        let message = match rx.recv().await {
            Ok(message) => message,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        };
        // Perform security checks to make sure the user is allowed to see this message
        // before sending it to the front end.
        let mut conn = db_pool.get().await.expect("Failed to get connection");
        let projects = TaskProject::list(&mut conn, message.id).await;
        let mut allowed = false;
        for project in projects {
            if project.owner_id == user_id {
                allowed = true;
                break;
            }
            if project.is_member(&mut conn, user_id).await {
                allowed = true;
                break;
            }
        }

        if allowed {
            let message = FrontEndMessage::AddTask(message);
            if update_tx.send(message).await.is_err() {
                // The front end has disconnected, so we can stop sending messages.
                break;
            }
        }
    }
    tracing::info!("New tasks handler exited");
}

pub async fn denormalized_comments(
    conn: &mut AsyncPgConnection,
    task: &Task,
) -> Result<Vec<DenormalizedComment>, RejectReason> {
    let comments = task.comments(conn).await;
    let mut reply = vec![];
    for comment in comments {
        let denorm_comment = DenormalizedComment::denormalize(conn, comment).await?;
        reply.push(denorm_comment);
    }
    Ok(reply)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskExport {
    pub spec: DenormalizedTaskDetailsMinimal,
    pub comments: Vec<DenormalizedComment>,
}

pub async fn export_task(
    pool: Arc<DbPool>,
    user_id: UserId,
    task: &Task,
    priority_levels: &[(String, i32)],
    include_comments: bool,
) -> Result<String, RejectReason> {
    let spec =
        DenormalizedTaskDetails::denormalize::<NoTaskAssociation>(pool.clone(), user_id, task)
            .await?;
    let spec = DenormalizedTaskDetailsMinimal::from_task_details(spec, priority_levels);
    let mut conn = pool.get().await.map_err(RejectReason::pool_error)?;
    let comments = if include_comments {
        denormalized_comments(&mut conn, task).await?
    } else {
        vec![]
    };
    let export = TaskExport { spec, comments };

    serde_yml::to_string(&export).map_err(|_| RejectReason::bad_request("Failed to serialize task"))
}

fn export_denormalized_task(task: &DenormalizedTask) -> Result<String, RejectReason> {
    serde_yml::to_string(task)
        .map_err(|_| RejectReason::bad_request("Failed to serialize denormalized task"))
}

pub async fn create_task_steps(
    pool: Arc<DbPool>,
    router: &ChannelRouter,
    creator_id: UserId,
    project_id: ProjectId,
    title: String,
    description: String,
    assignee_id: Option<UserId>,
    priority: i32,
    due_date: Option<DateTime<Utc>>,
) -> Result<DenormalizedTask, RejectReason> {
    let task_id = TaskId(Uuid::new_v4());
    let mut conn = pool.get().await.map_err(RejectReason::pool_error)?;
    let task = create_task(
        &mut conn,
        creator_id,
        task_id,
        project_id,
        title,
        description,
        assignee_id,
        priority,
        due_date,
    )
    .await?;
    let tx = router.announce();
    {
        let user_id = creator_id;
        let pool = pool.clone();
        let task_id = task.id;
        let title = task.title.clone();
        tokio::task::spawn(async move {
            title_summaries(user_id, pool, task_id, &title).await;
        });
    }
    tx.send(TaskChanged {
        user_id: creator_id,
        task_id,
        update: None,
        previous: None,
    })
    .ok();

    link_new_planned_task(pool.clone(), creator_id, &task);
    DenormalizedTask::denormalize(&mut conn, task.id, true).await
}

pub async fn task_priority_levels(
    conn: &mut AsyncPgConnection,
    task_id: TaskId,
) -> Vec<(String, i32)> {
    let projects = TaskProject::list(conn, task_id).await;
    if let Some(project) = projects.first() {
        let prios = project.get_priority_levels(conn).await;
        prios.into_iter().map(|p| (p.name, p.value)).collect()
    } else {
        vec![]
    }
}

pub async fn fetch_task_url(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    link_id: Uuid,
    page: usize,
    limit: usize,
) -> Result<Vec<DenormalizedTask>, RejectReason> {
    let task_link_url = TaskListUrl::get(conn, link_id)
        .await
        .map_err(|_| RejectReason::not_found(format!("TaskLinkUrl {}", link_id)))?;

    if user_id != task_link_url.user_id {
        let org = Organization::get_user_org(conn, task_link_url.user_id)
            .await
            .ok_or_else(|| RejectReason::not_found(format!("Organization for User {}", user_id)))?;

        if !org.has_user(conn, user_id).await {
            return Err(RejectReason::forbidden(
                user_id,
                String::from("Access denied"),
            ));
        }
    }

    let offset = page.saturating_sub(1) * limit;
    task_link_url.fetch_tasks(conn, offset, limit).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipJson {
    pub relationship: Option<TaskLinkType>,
}

async fn determine_relationship(
    pool: Arc<DbPool>,
    user_id: UserId,
    export_left: &str,
    export_right: &str,
) -> Result<Option<TaskLinkType>, RejectReason> {
    let prompt = PromptRequest {
        prompt_name: DETERMINE_RELATIONSHIP_PROMPT.to_string(),
        user_prompt: format!("{}\n---\n{}", export_left, export_right),
        with_tools: None,
    };
    let response = timeout(
        Duration::from_secs(30),
        handle_new_prompt(pool, user_id, prompt),
    )
    .await;
    let response_link = validate_prompt_response::<RelationshipJson>(response)
        .ok_or_else(|| RejectReason::bad_request("Failed to determine relationship"))?;

    Ok(response_link.relationship)
}

async fn find_and_link_related(db_pool: Arc<DbPool>, user_id: UserId, task: Task) {
    let mut conn = db_pool.get().await.expect("Failed to get connection");
    let project_ids = TaskProject::list(&mut conn, task.id)
        .await
        .into_iter()
        .map(|project| project.id)
        .collect::<Vec<_>>();
    let denormalized_task = DenormalizedTask::denormalize(&mut conn, task.id, true)
        .await
        .expect("Failed to denormalize task");
    let exported = export_denormalized_task(&denormalized_task).expect("Failed to export task");
    let org = Organization::get_user_org(&mut conn, user_id)
        .await
        .expect("Failed to get organization");
    let related_tasks = nearest_tasks(
        db_pool.clone(),
        org.id,
        &project_ids,
        10,
        0,
        0.5,
        &exported,
        &[],
        &[],
    )
    .await
    .expect("Failed to find related tasks");

    for related_task in related_tasks {
        let related_export =
            export_denormalized_task(&related_task).expect("Failed to export related task");
        let relationship = match determine_relationship(
            db_pool.clone(),
            user_id,
            &exported,
            &related_export,
        )
        .await
        {
            Ok(result) => result,
            Err(err) => {
                tracing::error!("Failed to determine relationship: {:?}", err);
                continue;
            }
        };
        if let Some(relationship) = relationship {
            Task::add_link(&mut conn, task.id, related_task.id, relationship)
                .await
                .ok();
        }
    }
}

pub fn find_related_tasks(db_pool: Arc<DbPool>, user_id: UserId, task: &Task) {
    let task = task.clone();
    spawn(find_and_link_related(db_pool, user_id, task));
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskIdJson {
    task_id: TaskId,
}

pub async fn finalize_graph(
    conn: &mut AsyncPgConnection,
    author_id: UserId,
    graph_instance: GraphInstance,
) -> Result<(), RejectReason> {
    let mut task_map = HashMap::new();
    let GraphInstance {
        nodes, connections, ..
    } = graph_instance;

    let task_ref_type: i32 = NodeType::Task.into();
    for node in nodes.into_iter() {
        if node.ref_type != task_ref_type {
            continue;
        }
        let task_id_json: TaskIdJson = match serde_json::from_value(node.metadata) {
            Ok(task_id_json) => task_id_json,
            Err(_) => continue,
        };
        task_map.insert(node.id, task_id_json.task_id);
    }

    let mut task_pairs = vec![];
    for pair in connections.into_iter() {
        let GraphEdge {
            from_node_id,
            to_node_id,
            ..
        } = pair;
        let from_task_id = match task_map.get(&from_node_id) {
            Some(task_id) => *task_id,
            None => continue,
        };
        let to_task_id = match task_map.get(&to_node_id) {
            Some(task_id) => *task_id,
            None => continue,
        };
        task_pairs.push((to_task_id, from_task_id));
    }
    let task_set = task_map.values().cloned().collect::<HashSet<_>>();

    if !task_set.is_empty() {
        Task::relink(conn, author_id, &task_set, &task_pairs)
            .await
            .map_err(RejectReason::database_error)?;
    }

    Ok(())
}

pub async fn finalize_thread(
    _conn: &mut AsyncPgConnection,
    _thread_instance: DenormalizedThread,
) -> Result<(), RejectReason> {
    // TODO
    Ok(())
}

pub async fn finalize_pattern(
    _conn: &mut AsyncPgConnection,
    _pattern_instance: DenormalizedPattern,
) -> Result<(), RejectReason> {
    // TODO
    Ok(())
}

pub async fn remap_task_state_graph(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    old_graph_id: GraphId,
    new_graph: &GraphInstance,
    node_map: HashMap<GraphNodeId, GraphNodeId>,
    project_id: ProjectId,
) -> Result<(), RejectReason> {
    if tracing::enabled!(tracing::Level::DEBUG) {
        let old_graph_nodes = GraphInstance::nodes(conn, old_graph_id)
            .await
            .map_err(RejectReason::database_error)?;

        for (old_mapped_id, new_mapped_id) in node_map.iter() {
            if let Some(old_node) = old_graph_nodes.iter().find(|n| n.id == *old_mapped_id) {
                if let Some(new_node) = new_graph.nodes.iter().find(|n| n.id == *new_mapped_id) {
                    tracing::debug!(
                        "Remapping node {} ({}) -> {} ({})",
                        old_node.node_name,
                        old_node.id,
                        new_node.node_name,
                        new_node.id,
                    );
                } else {
                    tracing::warn!(
                        "No new node found for remapping {} -> {}",
                        old_mapped_id,
                        new_mapped_id
                    );
                }
            } else {
                tracing::warn!("No old node found for remapping {}", old_mapped_id);
            }
        }
    }
    conn.transaction(|conn| {
        async move {
            let project = Project::get(conn, project_id)
                .await
                .ok_or_else(|| diesel::result::Error::NotFound)?;

            if project.task_state_graph_id != old_graph_id.0 {
                return Err(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::CheckViolation,
                    Box::new(ValidationErrorMessage {
                        message: format!(
                            "Project {} does not use graph {}",
                            project_id, old_graph_id
                        ),
                        column: "task_graph_id".to_string(),
                        constraint_name: "valid_task_graph_id".to_string(),
                    }),
                ));
            }

            project
                .update(
                    conn,
                    user_id,
                    UpdateProject::TaskStateGraph {
                        graph_id: new_graph.graph_id.0,
                    },
                )
                .await?;

            let task_ids = TaskProject::all_tasks_in_project(conn, project_id).await;
            tracing::debug!(
                "Found {:?} tasks in project {} for remapping",
                task_ids,
                project_id
            );
            for task_id in task_ids {
                if let Some(graph) = TaskGraph::get(conn, task_id, old_graph_id).await {
                    let new_node_id = graph
                        .current_node_id
                        .map(|id| node_map.get(&id).cloned())
                        .flatten();
                    graph
                        .update_graph(conn, new_graph.graph_id, new_node_id)
                        .await?;
                } else {
                    tracing::warn!(
                        "No task graph found for task {} in project {} with old graph {}",
                        task_id,
                        project_id,
                        old_graph_id
                    );
                }

                let transitions = TaskGraphTransition::list(conn, task_id).await;
                for mut transition in transitions {
                    if transition.graph_id != old_graph_id {
                        continue;
                    }
                    let new_from_node_id = node_map
                        .get(&transition.from_node_id)
                        .cloned()
                        .ok_or_else(|| {
                            let kind = diesel::result::DatabaseErrorKind::CheckViolation;
                            let msg = Box::new(ValidationErrorMessage {
                                message: format!(
                                    "No valid transition from {} found",
                                    transition.from_node_id
                                ),
                                column: "from_node_id".to_string(),
                                constraint_name: "valid_transition_from_node_id".to_string(),
                            });
                            diesel::result::Error::DatabaseError(kind, msg)
                        })?;
                    let new_to_node_id =
                        node_map
                            .get(&transition.to_node_id)
                            .cloned()
                            .ok_or_else(|| {
                                let kind = diesel::result::DatabaseErrorKind::CheckViolation;
                                let msg = Box::new(ValidationErrorMessage {
                                    message: format!(
                                        "No valid transition from {} found",
                                        transition.to_node_id
                                    ),
                                    column: "to_node_id".to_string(),
                                    constraint_name: "valid_transition_to_node_id".to_string(),
                                });
                                diesel::result::Error::DatabaseError(kind, msg)
                            })?;
                    transition
                        .update_graph(conn, new_graph.graph_id, new_from_node_id, new_to_node_id)
                        .await?;
                }
            }
            QueryResult::Ok(())
        }
        .scope_boxed()
    })
    .await
    .map_err(RejectReason::database_error)
    .map(|_| ())
}

/// Internal administration function to initialize a task graph for a project
/// Not to be used on the API
pub async fn initialize_task_graphs(
    conn: &mut AsyncPgConnection,
    org: &Organization,
) -> Result<(), RejectReason> {
    let owner = OrganizationMembership::get_owner(conn, org.id)
        .await
        .ok_or_else(|| RejectReason::not_found(format!("Admin for Organization {}", org.id)))?;

    let projects = org.projects(conn, 1, u32::MAX).await;
    for project in projects {
        if project.task_graph_id.is_some() {
            // Already has a task graph
            continue;
        }
        let graph = create_task_graph(conn, org.id, project.id).await?;
        project
            .update(
                conn,
                owner.user_id,
                UpdateProject::TaskGraph {
                    graph_id: graph.id.0,
                },
            )
            .await
            .map_err(RejectReason::database_error)?;
        cleanup_task_graph(conn, org, graph.id).await?;
    }
    Ok(())
}

pub async fn cleanup_task_graph(
    conn: &mut AsyncPgConnection,
    org: &Organization,
    graph_id: GraphId,
) -> Result<(), RejectReason> {
    // Is a task graph
    for project in Project::get_by_task_graph_id(conn, graph_id.0).await {
        // Is in the user's organization
        if !org.has_project(conn, project.id).await {
            continue;
        }

        // Cleanup the task graph
        Task::relink_task_graph_nodes(conn, project.id, graph_id)
            .await
            .map_err(RejectReason::database_error)?;
    }
    Ok(())
}

pub async fn create_task_from_msg(
    pool: Arc<DbPool>,
    msg: String,
    user_id: UserId,
    active_project: Option<ActiveProjectData>,
) -> Result<Task, RejectReason> {
    tracing::debug!("Creating task from message: {}", msg);

    let ActiveProjectData {
        project_id,
        priority_levels,
        org_id,
    } = if let Some(active_project) = active_project {
        active_project
    } else {
        get_active_project(pool.clone(), user_id).await?
    };

    let description = expand_description(pool.clone(), user_id, org_id, &msg)
        .await
        .unwrap_or(msg);
    let title = title_from_description(pool.clone(), user_id, &description)
        .await
        .ok_or_else(|| {
            RejectReason::anyhow(anyhow::anyhow!("Failed to generate title".to_string()))
        })?;

    let task_id = TaskId(Uuid::new_v4());

    let mut task = {
        let mut conn = match pool.get().await {
            Ok(conn) => conn,
            Err(err) => {
                return Err(RejectReason::anyhow(anyhow::anyhow!(format!(
                    "Out of pool resources: {:?}",
                    err
                ))));
            }
        };
        tracing::debug!("create_task_from_msg::create_task: {}", title);
        match create_task(
            &mut conn,
            user_id,
            task_id,
            project_id,
            title,
            description,
            None,
            0,
            None,
        )
        .await
        {
            Ok(task) => task,
            Err(err) => {
                return Err(RejectReason::anyhow(anyhow::anyhow!(format!(
                    "Create task failed: {:?}",
                    err
                ))));
            }
        }
    };

    if let Some(priority) = priority_from_description(
        pool.clone(),
        user_id,
        org_id,
        &priority_levels,
        &task.description,
    )
    .await
    {
        let mut conn = match pool.get().await {
            Ok(conn) => conn,
            Err(err) => {
                return Err(RejectReason::anyhow(anyhow::anyhow!(format!(
                    "Updating task failed: {:?}",
                    err
                ))));
            }
        };
        task.update(
            &mut conn,
            user_id,
            TaskUpdate::ChangePriority { value: priority },
        )
        .await
        .ok();
    }

    {
        let pool = pool.clone();
        let task_id = task.id;
        let title = task.title.clone();
        tokio::task::spawn(async move {
            title_summaries(user_id, pool, task_id, &title).await;
        });
    }

    find_related_tasks(pool.clone(), user_id, &task);
    link_new_planned_task(pool, user_id, &task);

    Ok(task)
}
