use std::collections::{HashMap, HashSet};
use std::time::Instant;

use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
use diesel::debug_query;
use diesel::dsl::count_star;
use diesel::pg::Pg;
use diesel::sql_types::{Array, Bool, Text, Uuid as DieselUuid};
use diesel::{dsl::sql, prelude::*};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use futures::{stream::BoxStream, StreamExt};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use subseq_util::api::RejectReason;
use subseq_util::UserId;
use uuid::Uuid;
use zini_graph::{Graph, GraphEdge, GraphId, GraphInstance, GraphNode, GraphNodeId};
use zini_ownership::{
    add_org_component, update_project_metadata, DenormalizedUser, Organization, OrganizationId,
    Project, ProjectId, ProjectMetadata, ProjectMetadataProps, User,
};

use super::states::UpdateSource;
use crate::tables::{
    AbstractState, ChartPlanUpdate, Comment, CommentThread, DenormalizedMilestone, DocPair,
    DocumentType, FileUpload, MetadataSource, Milestone, MilestoneId, PlannedTask, TaskId,
    TaskLinkType, TaskListUrl, TaskUpdate, UserVectorEdges, ValidationErrorMessage,
};

pub fn slug_regex(maybe_slug: &str) -> Option<String> {
    let re: Regex = Regex::new(r"^[A-Z]+-\d+$").unwrap();
    if re.is_match(maybe_slug) {
        Some(maybe_slug.to_string())
    } else {
        None
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum SearchFilterRule {
    CreatedBy(UserId),
    AssignedTo(UserId),
    Unassigned,
    PriorityHigher(i32),
    PriorityLower(i32),
    State(AbstractState),
    CreatedAfter(DateTime<Utc>),
    CreatedBefore(DateTime<Utc>),
    DueBy(DateTime<Utc>),
    Labels(Vec<String>),
    Components(Vec<String>),

    // New 06-23-2025
    UpdatedAfter(DateTime<Utc>),
    WatchedBy(UserId),
    NodeId(GraphNodeId),
    Archived,
}

impl SearchFilterRule {
    pub fn from_filter_rule(rule: FilterRule, user_id: UserId) -> SearchFilterRule {
        match rule {
            FilterRule::Archived => SearchFilterRule::Archived,
            FilterRule::AssignedTo(uid) => SearchFilterRule::AssignedTo(uid),
            FilterRule::AssignedToMe => SearchFilterRule::AssignedTo(user_id),
            FilterRule::Closed => SearchFilterRule::State(AbstractState::Closed),
            FilterRule::NotClosed => SearchFilterRule::State(AbstractState::NotClosed),
            FilterRule::Created(dt) => SearchFilterRule::CreatedAfter(dt),
            FilterRule::NodeId(id) => SearchFilterRule::NodeId(id),
            FilterRule::InProgress => SearchFilterRule::State(AbstractState::InProgress),
            FilterRule::Open => SearchFilterRule::State(AbstractState::Open),
            FilterRule::Updated(dt) => SearchFilterRule::UpdatedAfter(dt),
            FilterRule::Watching => SearchFilterRule::WatchedBy(user_id),
        }
    }
}

#[derive(PartialEq, Queryable, Insertable, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::task_projects)]
pub struct TaskProject {
    pub task_id: TaskId,
    pub project_id: ProjectId,
}

impl TaskProject {
    pub async fn list(conn: &mut AsyncPgConnection, task_id: TaskId) -> Vec<Project> {
        use crate::schema::projects;
        use crate::schema::task_projects;
        task_projects::table
            .inner_join(projects::table)
            .filter(task_projects::dsl::task_id.eq(task_id))
            .select(projects::all_columns)
            .load::<Project>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn filter_by_project(
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
        task_ids: &[TaskId],
    ) -> Vec<TaskId> {
        use crate::schema::task_projects;
        task_projects::table
            .filter(task_projects::project_id.eq(project_id))
            .filter(task_projects::task_id.eq_any(task_ids))
            .select(task_projects::task_id)
            .load::<TaskId>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn all_tasks_in_project(
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
    ) -> Vec<TaskId> {
        use crate::schema::task_projects;
        task_projects::table
            .filter(task_projects::project_id.eq(project_id))
            .select(task_projects::task_id)
            .load::<TaskId>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn archive_tasks_by_project(
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
    ) -> QueryResult<()> {
        use crate::schema::task_projects;
        use crate::schema::tasks;

        let single_project_tasks = task_projects::table
            .filter(task_projects::project_id.eq(project_id))
            .select(task_projects::task_id)
            .group_by(task_projects::task_id)
            .having(count_star().eq(1));

        diesel::update(tasks::table.filter(tasks::id.eq_any(single_project_tasks)))
            .set(tasks::deleted.eq(true))
            .execute(conn)
            .await
            .map(|_| ())
    }

    pub async fn filter_by_projects(
        conn: &mut AsyncPgConnection,
        project_ids: &[ProjectId],
        task_ids: &[TaskId],
    ) -> Vec<TaskId> {
        use crate::schema::task_projects;
        task_projects::table
            .filter(task_projects::project_id.eq_any(project_ids))
            .filter(task_projects::task_id.eq_any(task_ids))
            .select(task_projects::task_id)
            .load::<TaskId>(conn)
            .await
            .unwrap_or_default()
    }
}

#[derive(Queryable, Insertable, QueryableByName, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::task_changes)]
pub struct TaskChange {
    pub id: Uuid,
    pub task_id: TaskId,
    pub user_id: UserId,
    pub created: NaiveDateTime,
    pub source: UpdateSource,
    pub change: Value,
    pub previous: Value,
}

impl TaskChange {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        user_id: UserId,
        source: UpdateSource,
        change: Value,
        previous: Value,
    ) -> QueryResult<Self> {
        let change = Self {
            id: Uuid::new_v4(),
            task_id,
            user_id,
            created: chrono::Utc::now().naive_utc(),
            source,
            change,
            previous,
        };
        diesel::insert_into(crate::schema::task_changes::table)
            .values(&change)
            .execute(conn)
            .await?;
        Ok(change)
    }

    pub async fn from_time_range(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        from: NaiveDateTime,
        to: NaiveDateTime,
    ) -> Vec<Self> {
        use crate::schema::task_changes::dsl as task_changes;
        task_changes::task_changes
            .filter(task_changes::task_id.eq(task_id))
            .filter(task_changes::created.ge(from))
            .filter(task_changes::created.le(to))
            .load(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn last(conn: &mut AsyncPgConnection, task_id: TaskId) -> Option<Self> {
        use crate::schema::task_changes::dsl as task_changes;
        task_changes::task_changes
            .filter(task_changes::task_id.eq(task_id))
            .order(task_changes::created.desc())
            .first(conn)
            .await
            .optional()
            .ok()
            .flatten()
    }
}

#[derive(Queryable, Debug, QueryableByName)]
#[diesel(table_name = crate::schema::denorm_task)]
pub struct DenormTask {
    pub id: TaskId,
    pub archived: bool,
    pub slug: String,
    pub summaries: Vec<String>,
    pub created: NaiveDateTime,
    pub updated: Option<NaiveDateTime>,
    pub title: String,
    pub description: String,
    pub labels: Vec<String>,
    pub components: Vec<String>,

    pub author_id: UserId,
    pub author_username: String,
    pub assignee_id: Option<UserId>,
    pub assignee_username: Option<String>,
    pub abstract_state: AbstractState,
    pub priority: i32,
    pub due_date: Option<NaiveDateTime>,

    // Milestone
    pub milestone_id: Option<Uuid>,
    pub milestone_org_id: Option<Uuid>,
    pub milestone_type: Option<String>,
    pub milestone_name: Option<String>,
    pub milestone_description: Option<String>,
    pub milestone_created: Option<NaiveDateTime>,
    pub milestone_due_date: Option<NaiveDateTime>,
    pub milestone_start_date: Option<NaiveDateTime>,
    pub milestone_started: Option<bool>,
    pub milestone_completed_date: Option<NaiveDateTime>,
    pub milestone_completed: Option<bool>,
    pub milestone_repeat_interval: Option<TimeDelta>,
    pub milestone_repeat_end: Option<NaiveDateTime>,
    pub milestone_repeat_schema: Option<Value>,
    pub milestone_next_milestone_id: Option<Uuid>,
    pub milestone_prev_milestone_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DenormalizedUserMinimal {
    pub id: UserId,
    pub username: String,
}

impl From<DenormalizedUser> for DenormalizedUserMinimal {
    fn from(user: DenormalizedUser) -> Self {
        Self {
            id: user.id,
            username: user.username,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedTask {
    pub id: TaskId,
    pub slug: String,
    pub archived: bool,
    pub title_summaries: Vec<String>,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
    pub title: String,
    pub description: String,
    pub author: DenormalizedUserMinimal,
    pub assignee: Option<DenormalizedUserMinimal>,
    pub abstract_state: AbstractState,
    pub priority: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_date: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub milestone: Option<DenormalizedMilestone>,
    pub labels: Vec<String>,
    pub components: Vec<String>,
}

fn opt_milestone(row: &DenormTask) -> Option<DenormalizedMilestone> {
    let id = MilestoneId(row.milestone_id?);
    let org_id = OrganizationId(row.milestone_org_id?);
    let milestone_type = row.milestone_type.clone()?;
    let name = row.milestone_name.clone()?;
    let description = row.milestone_description.clone()?;
    let created = row.milestone_created?;
    let completed = row.milestone_completed?;
    let start_date = row.milestone_start_date?;
    let started = row.milestone_started?;

    Some(DenormalizedMilestone::denormalize(Milestone {
        id,
        org_id,
        milestone_type,
        name,
        description,
        due_date: row.milestone_due_date,
        created,
        completed,
        completed_date: row.milestone_completed_date,
        start_date,
        started,
        repeat_interval: row.milestone_repeat_interval,
        repeat_end: row.milestone_repeat_end,
        repeat_schema: row.milestone_repeat_schema.clone(),
        next_milestone_id: row.milestone_next_milestone_id.map(MilestoneId),
        prev_milestone_id: row.milestone_prev_milestone_id.map(MilestoneId),
    }))
}

fn opt_denormalized_user(row: &DenormTask) -> Option<DenormalizedUserMinimal> {
    let user_id = row.assignee_id?;
    let username = row.assignee_username.clone()?;
    Some(DenormalizedUserMinimal {
        id: user_id,
        username,
    })
}

#[derive(Debug, Deserialize, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OrderBy {
    Alphabetical,
    Created,
    Due,
    Priority,
}
impl OrderBy {
    pub fn column_order(&self) -> &'static str {
        match self {
            OrderBy::Alphabetical => "title ASC",
            OrderBy::Created => "created DESC",
            OrderBy::Due => "due_date ASC",
            OrderBy::Priority => "priority ASC",
        }
    }
}

impl DenormalizedTask {
    const MAX_DESCRIPTION_LENGTH: usize = 2048;

    pub fn from_row(row: DenormTask, truncate: bool) -> Self {
        let milestone = opt_milestone(&row);
        let assignee = opt_denormalized_user(&row);

        Self {
            id: row.id,
            slug: row.slug,
            archived: row.archived,
            title_summaries: row.summaries,
            created: row.created.and_utc(),
            updated: row
                .updated
                .map(|u| u.and_utc())
                .unwrap_or(row.created.and_utc()),
            title: row.title,
            description: {
                if truncate && row.description.len() > Self::MAX_DESCRIPTION_LENGTH {
                    let mut description = row.description.clone();
                    description.truncate(Self::MAX_DESCRIPTION_LENGTH);
                    description.push_str("\n(truncated)");
                    description
                } else {
                    row.description.clone()
                }
            },
            author: DenormalizedUserMinimal {
                id: row.author_id,
                username: row.author_username,
            },
            assignee,
            abstract_state: row.abstract_state,
            priority: row.priority,
            due_date: row.due_date.map(|d| d.and_utc()),
            milestone,
            labels: row.labels,
            components: row.components,
        }
    }

    pub async fn get_by_slug(
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        slug: &str,
    ) -> Result<Self, RejectReason> {
        use crate::schema::task_projects::dsl as task_projects;
        use crate::schema::tasks::dsl as tasks;
        let project_ids = Organization::user_project_ids(conn, user_id).await;

        let task_id = tasks::tasks
            .inner_join(task_projects::task_projects.on(task_projects::task_id.eq(tasks::id)))
            .filter(task_projects::project_id.eq_any(project_ids))
            .filter(tasks::slug.eq(slug))
            .select(tasks::id)
            .first::<TaskId>(conn)
            .await
            .map_err(RejectReason::database_error)?;

        Self::denormalize(conn, task_id, true).await
    }

    pub async fn denormalize(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        truncate: bool,
    ) -> Result<Self, RejectReason> {
        use crate::schema::denorm_task::dsl as denorm_task;
        let row = denorm_task::denorm_task
            .filter(denorm_task::id.eq(task_id))
            .first::<DenormTask>(conn)
            .await
            .map_err(RejectReason::database_error)?;
        Ok(Self::from_row(row, truncate))
    }

    pub async fn denormalize_all(
        conn: &mut AsyncPgConnection,
        tasks: &[TaskId],
        sort: Option<OrderBy>,
        truncate: bool,
    ) -> Result<Vec<Self>, RejectReason> {
        use crate::schema::denorm_task::dsl;
        tracing::trace!("denormalize_all {}", tasks.len());

        let now = Instant::now();
        let mut query = dsl::denorm_task.filter(dsl::id.eq_any(tasks)).into_boxed();

        query = match sort {
            Some(OrderBy::Alphabetical) => query.order(dsl::title.asc()),
            Some(OrderBy::Created) => query.order(dsl::created.asc()),
            Some(OrderBy::Due) => query.order(dsl::due_date.asc()),
            Some(OrderBy::Priority) => query.order(dsl::priority.asc()),
            None => query,
        };

        tracing::trace!("{}", debug_query::<Pg, _>(&query));
        let tasks: Vec<_> = query
            .load::<DenormTask>(conn)
            .await
            .map_err(RejectReason::database_error)
            .map(|rows| {
                rows.into_iter()
                    .map(|row| Self::from_row(row, truncate))
                    .collect()
            })?;
        tracing::trace!(
            "denormalize_all {} took {} ms",
            tasks.len(),
            now.elapsed().as_millis()
        );
        Ok(tasks)
    }

    pub async fn find_duplicates(
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        project_id: Option<ProjectId>,
        dist_threshold: f32,
    ) -> Result<Vec<TaskListUrl>, RejectReason> {
        let org = Organization::get_user_org(conn, user_id)
            .await
            .ok_or_else(|| RejectReason::not_found("Organization not found".to_string()))?;
        let dupes = UserVectorEdges::top_similar_pairs(conn, org.id, 10, Some(DocumentType::Task))
            .await
            .map_err(RejectReason::database_error)?;
        tracing::debug!("Found {:?} duplicate pairs", dupes);
        let mut tasks = vec![];

        for doc_pair in dupes {
            let DocPair {
                doc_a,
                doc_b,
                best_dist,
                ..
            } = doc_pair;
            if best_dist > dist_threshold {
                break;
            }
            let task_ids = vec![TaskId(doc_a), TaskId(doc_b)];
            let task_ids = if let Some(project_id) = project_id {
                let task_ids = TaskProject::filter_by_project(conn, project_id, &task_ids).await;
                task_ids
            } else {
                task_ids
            };
            if task_ids.len() != 2 {
                // Not in same project, so we're ignoring it here.
                continue;
            }
            let id = Uuid::new_v4();
            match TaskListUrl::create(
                conn,
                id,
                user_id,
                format!("/api/v1/task/list/{}", id),
                task_ids,
            )
            .await
            {
                Ok(task_list) => {
                    tasks.push(task_list);
                }
                Err(e) => {
                    tracing::error!("Failed to create task list: {}", e);
                    continue;
                }
            }
        }
        Ok(tasks)
    }

    pub async fn next_pending_work(
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        limit: u32,
    ) -> Result<Vec<Self>, RejectReason> {
        use crate::schema::denorm_task::dsl as tasks;
        use crate::schema::task_projects::dsl as task_projects;

        let project_ids = Organization::user_project_ids(conn, user_id).await;
        let capabilities = User::components(conn, user_id).await;

        let in_progress_tasks = tasks::denorm_task
            .inner_join(task_projects::task_projects.on(task_projects::task_id.eq(tasks::id)))
            .filter(task_projects::project_id.eq_any(&project_ids))
            .filter(tasks::abstract_state.eq(AbstractState::InProgress))
            .filter(tasks::archived.eq(false))
            .filter(tasks::assignee_id.eq(Some(user_id)))
            .order_by((
                tasks::priority.asc(),
                tasks::due_date.asc().nulls_last(),
                tasks::created.asc(),
            ))
            .select(tasks::denorm_task::all_columns())
            .limit(limit as i64)
            .load::<DenormTask>(conn)
            .await
            .map_err(RejectReason::database_error)?;

        let open_tasks = tasks::denorm_task
            .inner_join(task_projects::task_projects.on(task_projects::task_id.eq(tasks::id)))
            .filter(task_projects::project_id.eq_any(&project_ids))
            .filter(tasks::abstract_state.eq(AbstractState::Open))
            .filter(tasks::archived.eq(false))
            .filter(
                tasks::assignee_id
                    .is_null()
                    .or(tasks::assignee_id.eq(Some(user_id))),
            )
            .filter(tasks::components.overlaps_with(capabilities))
            .order_by((
                tasks::assignee_id.eq(Some(user_id)).asc(),
                tasks::priority.asc(),
                tasks::due_date.asc().nulls_last(),
                tasks::created.asc(),
            ))
            .select(tasks::denorm_task::all_columns())
            .limit(limit as i64)
            .load::<DenormTask>(conn)
            .await
            .map_err(RejectReason::database_error)?;

        let mut tasks = Vec::with_capacity(in_progress_tasks.len() + open_tasks.len());
        let mut seen_tasks = HashSet::new();

        for task in in_progress_tasks {
            // Duplicates can happen when a task is in multiple projects.
            if !seen_tasks.contains(&task.id) {
                seen_tasks.insert(task.id);
                tasks.push(Self::from_row(task, true));
            }
        }
        for task in open_tasks {
            // Duplicates can happen when a task is in multiple projects.
            if !seen_tasks.contains(&task.id) {
                seen_tasks.insert(task.id);
                tasks.push(Self::from_row(task, true));
            }
        }
        Ok(tasks.into_iter().take(limit as usize).collect())
    }

    pub async fn search_filter(
        conn: &mut AsyncPgConnection,
        project_ids: Vec<ProjectId>,
        filter_rule: Vec<SearchFilterRule>,
        page: u32,
        page_size: u32,
    ) -> Result<Vec<TaskId>, RejectReason> {
        use crate::schema::denorm_task::dsl as denorm_task;
        use crate::schema::task_projects::dsl as task_projects;

        let mut archived = false;

        let mut query_builder = denorm_task::denorm_task
            .inner_join(task_projects::task_projects.on(task_projects::task_id.eq(denorm_task::id)))
            .filter(task_projects::project_id.eq_any(project_ids))
            .into_boxed();

        let offset = page.saturating_sub(1) * page_size;
        let limit = page_size;
        let mut task_ids = vec![];

        for rule in filter_rule {
            match rule {
                SearchFilterRule::PriorityHigher(priority) => {
                    query_builder = query_builder.filter(denorm_task::priority.le(priority));
                }
                SearchFilterRule::PriorityLower(priority) => {
                    query_builder = query_builder.filter(denorm_task::priority.ge(priority));
                }
                SearchFilterRule::CreatedBy(user_id) => {
                    query_builder = query_builder.filter(denorm_task::author_id.eq(user_id));
                }
                SearchFilterRule::AssignedTo(user_id) => {
                    query_builder = query_builder.filter(denorm_task::assignee_id.eq(user_id));
                }
                SearchFilterRule::Unassigned => {
                    query_builder = query_builder.filter(denorm_task::assignee_id.is_null());
                }
                SearchFilterRule::State(state) => match state {
                    AbstractState::NotClosed => {
                        query_builder = query_builder
                            .filter(denorm_task::abstract_state.ne(AbstractState::Closed));
                    }
                    state => {
                        query_builder = query_builder.filter(denorm_task::abstract_state.eq(state));
                    }
                },
                SearchFilterRule::CreatedAfter(date) => {
                    query_builder = query_builder.filter(denorm_task::created.ge(date.naive_utc()));
                }
                SearchFilterRule::CreatedBefore(date) => {
                    query_builder = query_builder.filter(denorm_task::created.le(date.naive_utc()));
                }
                SearchFilterRule::DueBy(date) => {
                    query_builder =
                        query_builder.filter(denorm_task::due_date.le(date.naive_utc()));
                }
                SearchFilterRule::Labels(labels) => {
                    query_builder = query_builder.filter(denorm_task::labels.overlaps_with(labels));
                }
                SearchFilterRule::Components(components) => {
                    query_builder =
                        query_builder.filter(denorm_task::components.overlaps_with(components));
                }
                SearchFilterRule::UpdatedAfter(date) => {
                    query_builder = query_builder.filter(denorm_task::updated.ge(date.naive_utc()));
                }
                SearchFilterRule::WatchedBy(user_id) => {
                    use crate::schema::task_watchers::dsl as task_watchers;
                    use crate::schema::tasks::dsl as tasks;

                    task_ids.extend(
                        tasks::tasks
                            .inner_join(
                                task_watchers::task_watchers
                                    .on(task_watchers::task_id.eq(tasks::id)),
                            )
                            .filter(task_watchers::watcher_id.eq(user_id))
                            .select(tasks::id)
                            .load::<TaskId>(conn)
                            .await
                            .map_err(RejectReason::database_error)?,
                    );
                }
                SearchFilterRule::NodeId(node_id) => {
                    use crate::schema::task_graphs::dsl as task_graphs;
                    use crate::schema::tasks::dsl as tasks;

                    task_ids.extend(
                        tasks::tasks
                            .inner_join(
                                task_graphs::task_graphs.on(task_graphs::task_id.eq(tasks::id)),
                            )
                            .filter(task_graphs::current_node_id.eq(node_id))
                            .select(tasks::id)
                            .load::<TaskId>(conn)
                            .await
                            .map_err(RejectReason::database_error)?,
                    );
                }
                SearchFilterRule::Archived => {
                    archived = true;
                }
            }
        }
        query_builder = query_builder.filter(denorm_task::archived.eq(archived));
        if !task_ids.is_empty() {
            query_builder = query_builder.filter(denorm_task::id.eq_any(task_ids));
        }

        Ok(query_builder
            .offset(offset as i64)
            .limit(limit as i64)
            .select(denorm_task::id)
            .load(conn)
            .await
            .map_err(RejectReason::database_error)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(
    rename_all = "camelCase",
    tag = "filterRule",
    content = "filterRuleData"
)]
pub enum FilterRule {
    Archived,
    AssignedTo(UserId),
    AssignedToMe,
    Closed,
    NotClosed,
    Created(DateTime<Utc>),
    NodeId(GraphNodeId),
    InProgress,
    Open,
    Updated(DateTime<Utc>),
    Watching,
}

impl ToString for FilterRule {
    fn to_string(&self) -> String {
        match self {
            FilterRule::Archived => "archived".to_string(),
            FilterRule::AssignedTo(user_id) => format!("assignedTo({})", user_id.0),
            FilterRule::AssignedToMe => "assignedToMe".to_string(),
            FilterRule::Closed => "closed".to_string(),
            FilterRule::NotClosed => "notClosed".to_string(),
            FilterRule::Created(date) => format!("created({})", date.to_rfc3339()),
            FilterRule::NodeId(id) => format!("nodeId({})", id.0),
            FilterRule::InProgress => "inProgress".to_string(),
            FilterRule::Open => "open".to_string(),
            FilterRule::Updated(date) => format!("updated({})", date.to_rfc3339()),
            FilterRule::Watching => "watching".to_string(),
        }
    }
}

#[derive(Queryable, Insertable, QueryableByName, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::tasks)]
pub struct Task {
    pub id: TaskId,
    pub slug: String,
    pub created: NaiveDateTime,
    pub title: String,
    pub description: String,
    pub author_id: UserId,
    pub assignee_id: Option<UserId>,
    pub deleted: bool,
    pub priority: i32,

    pub due_date: Option<NaiveDateTime>,
    pub milestone_id: Option<MilestoneId>,
    pub abstract_state: AbstractState,
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.slug == other.slug
            && self.created.and_utc().timestamp_micros()
                == other.created.and_utc().timestamp_micros()
            && self.title == other.title
            && self.description == other.description
            && self.author_id == other.author_id
            && self.assignee_id == other.assignee_id
    }
}

impl Task {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        project_id: ProjectId,
        title: &str,
        description: &str,
        author_id: UserId,
    ) -> QueryResult<Self> {
        use crate::schema::task_projects::dsl as task_projects;

        let task = conn
            .transaction(|transact| {
                async move {
                    let task_project = TaskProject {
                        task_id,
                        project_id,
                    };

                    diesel::sql_query("LOCK TABLE task_projects IN EXCLUSIVE MODE;")
                        .execute(transact)
                        .await?;
                    diesel::insert_into(task_projects::task_projects)
                        .values(&task_project)
                        .execute(transact)
                        .await?;
                    let count = task_projects::task_projects
                        .filter(task_projects::project_id.eq(project_id))
                        .count()
                        .get_result::<i64>(transact)
                        .await?;

                    let project = Project::get(transact, project_id)
                        .await
                        .ok_or_else(|| diesel::result::Error::NotFound)?;

                    let slug = format!(
                        "{}-{}",
                        project.slug.as_ref().unwrap_or(&project.name),
                        count
                    );

                    let task = Self {
                        id: task_id,
                        slug,
                        created: chrono::Utc::now().naive_utc(),
                        title: title.to_owned(),
                        description: description.to_owned(),
                        author_id,
                        assignee_id: None,
                        deleted: false,
                        priority: 10,
                        due_date: None,
                        milestone_id: None,
                        abstract_state: AbstractState::Open,
                    };

                    let value = serde_json::to_value(TaskUpdate::ChangeTitle {
                        title: title.to_string(),
                    })
                    .unwrap();
                    let task_state_graph_id = project.task_state_graph_id;
                    let graph = Graph::get(transact, GraphId(task_state_graph_id)).await;
                    let task_graph = TaskGraph {
                        task_id: task.id,
                        graph_id: GraphId(task_state_graph_id),
                        current_node_id: graph.map(|f| f.entry_node_id),
                        order_added: 0,
                    };
                    let watcher = TaskWatcher {
                        task_id: task.id,
                        watcher_id: author_id,
                    };

                    diesel::insert_into(crate::schema::tasks::table)
                        .values(&task)
                        .execute(transact)
                        .await?;

                    TaskChange::create(
                        transact,
                        task.id,
                        author_id,
                        UpdateSource::TaskCreate,
                        value,
                        serde_json::json!({}),
                    )
                    .await?;
                    let value = serde_json::to_value(TaskUpdate::ChangeDescription {
                        description: description.to_string(),
                    })
                    .unwrap();
                    TaskChange::create(
                        transact,
                        task.id,
                        author_id,
                        UpdateSource::TaskCreate,
                        value,
                        serde_json::json!({}),
                    )
                    .await?;

                    diesel::insert_into(crate::schema::task_watchers::table)
                        .values(&watcher)
                        .execute(transact)
                        .await?;
                    diesel::insert_into(crate::schema::task_graphs::table)
                        .values(&task_graph)
                        .execute(transact)
                        .await?;
                    QueryResult::Ok(task)
                }
                .scope_boxed()
            })
            .await?;
        Ok(task)
    }

    pub async fn updated_time(&self, conn: &mut AsyncPgConnection) -> NaiveDateTime {
        TaskChange::last(conn, self.id)
            .await
            .map(|change| change.created)
            .unwrap_or(self.created)
    }

    pub async fn get_plan_data(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
    ) -> QueryResult<(String, String, AbstractState)> {
        use crate::schema::tasks::dsl as tasks;
        tasks::tasks
            .filter(tasks::id.eq(task_id))
            .select((tasks::slug, tasks::title, tasks::abstract_state))
            .get_result(conn)
            .await
    }

    pub async fn all(conn: &mut AsyncPgConnection, page: u32, page_size: u32) -> Vec<Uuid> {
        use crate::schema::tasks;
        let offset = page.saturating_sub(1) * page_size;

        tasks::table
            .offset(offset as i64)
            .limit(page_size as i64)
            .select(tasks::dsl::id)
            .load::<Uuid>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn update_plan(
        &self,
        conn: &mut AsyncPgConnection,
        update: ChartPlanUpdate,
    ) -> QueryResult<ChartPlanUpdate> {
        let previous = PlannedTask::update(conn, self.id, update).await?;

        diesel::sql_query(
            r#"
            UPDATE graph_nodes AS gn
            SET metadata = jsonb_set(
                gn.metadata,
                '{value}',
                to_jsonb(EXTRACT(epoch FROM pt.expected_duration)),
                true
            )
            FROM planned_tasks pt
            WHERE (gn.metadata->>'taskId')::uuid = pt.task_id AND pt.task_id = $1;
        "#,
        )
        .bind::<DieselUuid, _>(self.id)
        .execute(conn)
        .await?;

        Ok(previous)
    }

    pub async fn close(&mut self, conn: &mut AsyncPgConnection) -> QueryResult<bool> {
        use crate::schema::tasks::dsl as tasks;
        let now = Utc::now();
        diesel::update(tasks::tasks.filter(tasks::id.eq(self.id)))
            .set(tasks::abstract_state.eq(AbstractState::Closed))
            .execute(conn)
            .await?;
        self.update_plan(conn, ChartPlanUpdate::End(Some(now)))
            .await
            .ok();
        self.abstract_state = AbstractState::Closed;

        for graph in self.graphs(conn).await? {
            if let Some(current_node_id) = graph.current_node_id {
                let exits = GraphInstance::exits(conn, graph.graph_id).await?;
                if let Some(first_exit) = exits.into_iter().next().map(|exit| exit.id) {
                    use crate::schema::task_graphs::dsl as task_graphs;

                    conn.transaction(|conn| {
                        async move {
                            diesel::update(
                                task_graphs::task_graphs
                                    .filter(task_graphs::task_id.eq(self.id))
                                    .filter(task_graphs::graph_id.eq(graph.graph_id)),
                            )
                            .set(task_graphs::current_node_id.eq(Some(first_exit)))
                            .execute(conn)
                            .await?;

                            TaskGraphTransition::create(
                                conn,
                                self.id,
                                graph.graph_id,
                                current_node_id,
                                first_exit,
                            )
                            .await?;

                            Ok::<_, diesel::result::Error>(())
                        }
                        .scope_boxed()
                    })
                    .await?;

                    return Ok(true);
                }
            }
        }
        tracing::warn!("No valid exit found for task {}", self.id);
        Ok(true)
    }

    pub async fn restart(&mut self, conn: &mut AsyncPgConnection) -> QueryResult<bool> {
        for graph in self.graphs(conn).await? {
            let entry_point = match GraphInstance::entry(conn, graph.graph_id).await {
                Ok(node) => node,
                Err(_) => continue,
            };
            self.abstract_state = AbstractState::Open;
            self.update_plan(conn, ChartPlanUpdate::Restart).await.ok();
            conn.transaction(|conn| {
                async move {
                    diesel::update(
                        crate::schema::task_graphs::table
                            .filter(crate::schema::task_graphs::dsl::task_id.eq(self.id))
                            .filter(crate::schema::task_graphs::dsl::graph_id.eq(graph.graph_id)),
                    )
                    .set(crate::schema::task_graphs::dsl::current_node_id.eq(Some(entry_point.id)))
                    .execute(conn)
                    .await?;

                    TaskGraphTransition::create(
                        conn,
                        self.id,
                        graph.graph_id,
                        graph.current_node_id.unwrap_or(entry_point.id),
                        entry_point.id,
                    )
                    .await?;

                    diesel::update(
                        crate::schema::tasks::table
                            .filter(crate::schema::tasks::dsl::id.eq(self.id)),
                    )
                    .set(crate::schema::tasks::dsl::abstract_state.eq(AbstractState::Open))
                    .execute(conn)
                    .await?;

                    Ok::<_, diesel::result::Error>(())
                }
                .scope_boxed()
            })
            .await?;

            return Ok(true);
        }
        Err(diesel::result::Error::NotFound)
    }

    pub async fn delete(&self, conn: &mut AsyncPgConnection) -> QueryResult<bool> {
        use crate::schema::tasks::dsl;
        diesel::update(dsl::tasks.filter(dsl::id.eq(self.id)))
            .set(dsl::deleted.eq(true))
            .execute(conn)
            .await?;
        Ok(true)
    }

    pub async fn restore(&self, conn: &mut AsyncPgConnection) -> QueryResult<bool> {
        use crate::schema::tasks::dsl;
        diesel::update(dsl::tasks.filter(dsl::id.eq(self.id)))
            .set(dsl::deleted.eq(false))
            .execute(conn)
            .await?;
        Ok(true)
    }

    /// Get a task, checking that the user has authorization to access it
    pub async fn get_secure(
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        task_id: TaskId,
    ) -> QueryResult<Self> {
        use crate::schema::task_projects::dsl as task_projects;
        use crate::schema::tasks::dsl as tasks;
        let project_ids = Organization::user_project_ids(conn, user_id).await;
        tasks::tasks
            .inner_join(task_projects::task_projects.on(task_projects::task_id.eq(tasks::id)))
            .filter(task_projects::project_id.eq_any(project_ids))
            .filter(tasks::id.eq(task_id))
            .select(tasks::tasks::all_columns())
            .first::<Self>(conn)
            .await
    }

    async fn get_by_slug(
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        slug: &str,
    ) -> QueryResult<Self> {
        use crate::schema::task_projects::dsl as task_projects;
        use crate::schema::tasks::dsl as tasks;
        let project_ids = Organization::user_project_ids(conn, user_id).await;

        tasks::tasks
            .inner_join(task_projects::task_projects.on(task_projects::task_id.eq(tasks::id)))
            .filter(task_projects::project_id.eq_any(project_ids))
            .filter(tasks::slug.eq(slug))
            .select(tasks::tasks::all_columns())
            .first::<Self>(conn)
            .await
    }

    pub async fn get_bulk(conn: &mut AsyncPgConnection, task_ids: &[Uuid]) -> Vec<Self> {
        use crate::schema::tasks::dsl as tasks;
        tasks::tasks
            .filter(tasks::id.eq_any(task_ids))
            .load::<Self>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn get_result(conn: &mut AsyncPgConnection, task_id: TaskId) -> QueryResult<Self> {
        use crate::schema::tasks::table;
        table.find(task_id).get_result::<Self>(conn).await
    }

    pub async fn add_project(
        &self,
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
    ) -> QueryResult<bool> {
        use crate::schema::task_projects;
        let task_project = TaskProject {
            task_id: self.id,
            project_id,
        };

        diesel::insert_into(task_projects::table)
            .values(&task_project)
            .execute(conn)
            .await?;

        // Add this task to the project's graph if it doesn't already exist
        let project = match Project::get(conn, project_id).await {
            Some(project) => project,
            None => {
                return Ok(true);
            }
        };
        let graph_id = project.task_graph_id;
        if let Some(graph_id) = graph_id {
            Self::insert_into_graph(conn, self.id, &self.slug, GraphId(graph_id), project_id)
                .await?;
        }

        Ok(true)
    }

    pub async fn get_projects(&self, conn: &mut AsyncPgConnection) -> Vec<Project> {
        TaskProject::list(conn, self.id).await
    }

    pub async fn rm_project(
        &self,
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
    ) -> QueryResult<bool> {
        use crate::schema::task_projects;
        diesel::delete(task_projects::table)
            .filter(task_projects::dsl::task_id.eq(self.id))
            .filter(task_projects::dsl::project_id.eq(project_id))
            .execute(conn)
            .await?;
        Ok(true)
    }

    pub async fn add_comment(
        &self,
        conn: &mut AsyncPgConnection,
        author_id: UserId,
        content: &str,
        _parent_id: Option<Uuid>,
    ) -> QueryResult<Comment> {
        Comment::create(conn, author_id, self.id, content.to_string()).await
    }

    pub async fn comment_count(&self, conn: &mut AsyncPgConnection) -> i64 {
        Comment::count(conn, self.id).await
    }

    pub async fn comments(&self, conn: &mut AsyncPgConnection) -> Vec<Comment> {
        Comment::list(conn, self.id).await
    }

    pub async fn add_metadata(
        &self,
        conn: &mut AsyncPgConnection,
        source: MetadataSource,
        metadata: Value,
    ) -> QueryResult<bool> {
        use crate::schema::task_metadata::dsl as task_metadata;

        if let Some(meta) = task_metadata::task_metadata
            .filter(task_metadata::task_id.eq(&self.id))
            .filter(task_metadata::metadata.eq(&metadata))
            .load::<TaskMetadata>(conn)
            .await
            .optional()?
        {
            if !meta.is_empty() {
                tracing::warn!("Metadata already exists: {:?}", meta);
                return Ok(false);
            }
        }

        let metadata = TaskMetadata {
            id: Uuid::new_v4(),
            task_id: self.id,
            metadata,
            source,
        };

        diesel::insert_into(task_metadata::task_metadata)
            .values(&metadata)
            .execute(conn)
            .await?;

        let tag = metadata.metadata.get("label").map(|v| v.as_str().unwrap());

        let org = Organization::get_user_org(conn, self.author_id)
            .await
            .ok_or_else(|| diesel::result::Error::NotFound)?;

        if let Some(tag) = tag {
            let projects = TaskProject::list(conn, self.id).await;

            for project in projects {
                let meta = ProjectMetadata::get(conn, project.id).await;
                let metadata: ProjectMetadataProps = meta.unwrap_or_default();
                update_project_metadata(conn, org.id, project.id, metadata, vec![tag.to_string()])
                    .await?;
            }
        }

        let component = metadata
            .metadata
            .get("component")
            .map(|v| v.as_str().unwrap());
        if let Some(component) = component {
            add_org_component(conn, org.id, component.to_string()).await?;
        }

        Ok(true)
    }

    pub async fn rm_metadata(
        &self,
        conn: &mut AsyncPgConnection,
        metadata: Value,
    ) -> QueryResult<bool> {
        use crate::schema::task_metadata;
        diesel::delete(task_metadata::table)
            .filter(task_metadata::dsl::metadata.eq(metadata))
            .execute(conn)
            .await?;
        Ok(true)
    }

    pub async fn metadata(&self, conn: &mut AsyncPgConnection) -> QueryResult<Vec<Value>> {
        use crate::schema::task_metadata;
        task_metadata::table
            .filter(task_metadata::task_id.eq(&self.id))
            .select(task_metadata::metadata)
            .load::<Value>(conn)
            .await
    }

    pub async fn get_tags(&self, conn: &mut AsyncPgConnection) -> Vec<String> {
        use crate::schema::task_metadata::dsl as task_metadata;

        match task_metadata::task_metadata
            .filter(task_metadata::task_id.eq(&self.id))
            .filter(sql::<Bool>("metadata ? 'label'"))
            .select(task_metadata::task_metadata::all_columns())
            .load::<TaskMetadata>(conn)
            .await
        {
            Ok(metadata) => metadata
                .into_iter()
                .filter_map(|meta| {
                    meta.metadata
                        .get("label")
                        .map(|v| v.as_str().unwrap().to_string())
                })
                .collect(),
            Err(e) => {
                tracing::error!("Error getting tags: {:?}", e);
                vec![]
            }
        }
    }

    pub async fn add_tag(&self, conn: &mut AsyncPgConnection, tag: &str) -> QueryResult<bool> {
        let tag = tag.trim().to_lowercase().replace(" ", "-");
        self.add_metadata(conn, MetadataSource::Tag, json!({"label": tag}))
            .await
    }

    pub async fn add_component(
        &self,
        conn: &mut AsyncPgConnection,
        component: &str,
    ) -> QueryResult<bool> {
        let component = component.trim().to_lowercase().replace(" ", "-");
        self.add_metadata(conn, MetadataSource::Tag, json!({"component": component}))
            .await
    }

    pub async fn get_components(&self, conn: &mut AsyncPgConnection) -> Vec<String> {
        use crate::schema::task_metadata::dsl as task_metadata;

        match task_metadata::task_metadata
            .filter(task_metadata::task_id.eq(&self.id))
            .filter(sql::<Bool>("metadata ? 'component'"))
            .select(task_metadata::task_metadata::all_columns())
            .load::<TaskMetadata>(conn)
            .await
        {
            Ok(metadata) => metadata
                .into_iter()
                .filter_map(|meta| {
                    meta.metadata
                        .get("component")
                        .map(|v| v.as_str().unwrap().to_string())
                })
                .collect(),
            Err(e) => {
                tracing::error!("Error getting components: {:?}", e);
                vec![]
            }
        }
    }

    pub async fn add_watcher(
        &self,
        conn: &mut AsyncPgConnection,
        user_id: UserId,
    ) -> QueryResult<bool> {
        use crate::schema::task_watchers;
        let task_watcher = TaskWatcher {
            task_id: self.id,
            watcher_id: user_id,
        };
        diesel::insert_into(task_watchers::table)
            .values(&task_watcher)
            .execute(conn)
            .await?;
        Ok(true)
    }

    pub async fn rm_watcher(
        &self,
        conn: &mut AsyncPgConnection,
        user_id: UserId,
    ) -> QueryResult<bool> {
        use crate::schema::task_watchers;
        diesel::delete(task_watchers::table)
            .filter(task_watchers::dsl::task_id.eq(self.id))
            .filter(task_watchers::dsl::watcher_id.eq(user_id))
            .execute(conn)
            .await?;
        Ok(true)
    }

    pub async fn watchers(&self, conn: &mut AsyncPgConnection) -> QueryResult<Vec<User>> {
        use crate::schema::auth::users;
        use crate::schema::task_watchers;
        let users = task_watchers::table
            .inner_join(users::table)
            .filter(task_watchers::task_id.eq(&self.id))
            .select(users::all_columns)
            .load::<User>(conn)
            .await?;
        Ok(users)
    }

    // FIXME: This is wrong now that query has been updated
    // They should share the same query logic
    pub async fn query_count(conn: &mut AsyncPgConnection, user_id: UserId) -> usize {
        use crate::schema::task_projects;
        use crate::schema::tasks::dsl as tasks;

        let project = match Project::active(conn, user_id).await {
            Some(project) => project,
            None => return 0,
        };

        tasks::tasks
            .filter(tasks::deleted.eq(false))
            .inner_join(
                task_projects::dsl::task_projects.on(task_projects::dsl::task_id.eq(tasks::id)),
            )
            .filter(task_projects::dsl::project_id.eq(project.id))
            .count()
            .get_result::<i64>(conn)
            .await
            .map(|c| c as usize)
            .unwrap_or(0)
    }

    pub async fn query(
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        query_dict: &HashMap<String, Value>,
        order: OrderBy,
        page: u32,
        page_size: u32,
    ) -> anyhow::Result<Vec<TaskId>> {
        use crate::schema::task_projects;
        use crate::schema::tasks::dsl as tasks;

        // We used to do other stuff in here with a bunch of joins,
        // but we can just do a simple query if we're looking for a specific task.
        // everything else was removed because it was inefficient and not used.

        if let Some(slug) = query_dict.get("slug") {
            let slug: String = serde_json::from_value(slug.clone())
                .map_err(|e| anyhow::anyhow!("Error parsing slug: {:?}", e))?;
            if let Ok(task) = Task::get_by_slug(conn, user_id, &slug).await {
                return Ok(vec![task.id]);
            }
        }

        let offset = page.saturating_sub(1) * page_size;
        let query = tasks::tasks
            .inner_join(
                task_projects::dsl::task_projects.on(task_projects::dsl::task_id.eq(tasks::id)),
            )
            .limit(page_size as i64)
            .offset(offset as i64)
            .select(tasks::id)
            .into_boxed();

        let query = if let Some(projects) = query_dict.get("project_ids") {
            let project_ids: Vec<ProjectId> =
                serde_json::from_value(projects.clone()).unwrap_or_default();
            query.filter(task_projects::project_id.eq_any(project_ids))
        } else {
            let active_project = match Project::active(conn, user_id).await {
                Some(project) => project,
                None => return Ok(vec![]),
            };
            query.filter(task_projects::project_id.eq(active_project.id))
        };

        let query = if let Some(filter_rule) = query_dict.get("filter_rule") {
            let filter_rule: FilterRule = serde_json::from_value(filter_rule.clone())
                .map_err(|e| anyhow::anyhow!("Error parsing filter rule: {:?}", e))?;
            match filter_rule {
                FilterRule::Archived => query.filter(tasks::deleted.eq(true)),
                FilterRule::AssignedToMe => query.filter(
                    tasks::deleted
                        .eq(false)
                        .and(tasks::assignee_id.eq(Some(user_id))),
                ),
                FilterRule::AssignedTo(other) => query.filter(
                    tasks::deleted
                        .eq(false)
                        .and(tasks::assignee_id.eq(Some(other))),
                ),
                FilterRule::Closed => query.filter(
                    tasks::deleted
                        .eq(false)
                        .and(tasks::abstract_state.eq(AbstractState::Closed)),
                ),
                FilterRule::NotClosed => query.filter(
                    tasks::deleted.eq(false).and(
                        tasks::abstract_state
                            .eq(AbstractState::Closed)
                            .or(tasks::abstract_state.eq(AbstractState::InProgress)),
                    ),
                ),
                FilterRule::Created(dt) => {
                    let dt = dt.naive_utc();
                    query.filter(tasks::deleted.eq(false).and(tasks::created.ge(dt)))
                }
                FilterRule::NodeId(graph_node_id) => query.filter(
                    tasks::deleted.eq(false).and(diesel::dsl::exists(
                        crate::schema::task_graphs::table
                            .filter(crate::schema::task_graphs::dsl::task_id.eq(tasks::id))
                            .filter(
                                crate::schema::task_graphs::dsl::current_node_id
                                    .eq(graph_node_id.0),
                            ),
                    )),
                ),
                FilterRule::InProgress => query.filter(
                    tasks::deleted
                        .eq(false)
                        .and(tasks::abstract_state.eq(AbstractState::InProgress)),
                ),
                FilterRule::Open => query.filter(
                    tasks::deleted
                        .eq(false)
                        .and(tasks::abstract_state.eq(AbstractState::Open)),
                ),
                FilterRule::Updated(dt) => {
                    let dt = dt.naive_utc();
                    query.filter(
                        tasks::deleted.eq(false).and(diesel::dsl::exists(
                            crate::schema::task_changes::table
                                .filter(crate::schema::task_changes::dsl::task_id.eq(tasks::id))
                                .filter(crate::schema::task_changes::dsl::created.ge(dt)),
                        )),
                    )
                }
                FilterRule::Watching => query.filter(
                    tasks::deleted.eq(false).and(diesel::dsl::exists(
                        crate::schema::task_watchers::table
                            .filter(crate::schema::task_watchers::dsl::task_id.eq(tasks::id))
                            .filter(crate::schema::task_watchers::dsl::watcher_id.eq(user_id)),
                    )),
                ),
            }
        } else {
            query.filter(tasks::deleted.eq(false))
        };

        let query = match order {
            OrderBy::Due => query.order_by((
                tasks::abstract_state.desc(),
                tasks::due_date.asc(),
                tasks::priority.asc(),
            )),
            OrderBy::Created => query.order_by(tasks::created.desc()),
            OrderBy::Priority => {
                query.order_by((tasks::abstract_state.desc(), tasks::priority.asc()))
            }
            OrderBy::Alphabetical => query.order_by(tasks::title.asc()),
        };

        query
            .load::<TaskId>(conn)
            .await
            .map_err(|err| anyhow::anyhow!("Error querying tasks: {:?}", err))
    }

    pub async fn search(
        conn: &mut AsyncPgConnection,
        query: Option<&str>,
        user_id: UserId,
        order_by: OrderBy,
        filter_rule: Option<FilterRule>,
        project_ids: Vec<ProjectId>,
        page: u32,
        page_size: u32,
    ) -> QueryResult<Vec<TaskId>> {
        let filter_rules = match filter_rule {
            Some(filter_rule) => vec![SearchFilterRule::from_filter_rule(filter_rule, user_id)],
            None => vec![],
        };
        Self::search_multi_filter(
            conn,
            query,
            order_by,
            filter_rules,
            project_ids,
            page,
            page_size,
        )
        .await
    }

    pub async fn search_multi_filter(
        conn: &mut AsyncPgConnection,
        query: Option<&str>,
        order_by: OrderBy,
        filter_rules: Vec<SearchFilterRule>,
        project_ids: Vec<ProjectId>,
        page: u32,
        page_size: u32,
    ) -> QueryResult<Vec<TaskId>> {
        use crate::schema::{
            task_changes, task_graphs, task_metadata, task_projects, task_title_summaries,
            task_watchers, tasks,
        };
        use diesel::dsl::exists;

        // 1) Precompute pagination/fmt variables
        let offset = (page.saturating_sub(1) * page_size) as i64;
        let limit = page_size as i64;
        let search_sql = query.map(|q| format_search_query(q)); // your helper that turns "foo" ⇒ "'foo':*"

        // 2) Extract all the optional filters out of `filter_rule`
        let mut deleted_flag = false;
        let mut priority_lower: Option<i32> = None;
        let mut priority_higher: Option<i32> = None;
        let mut created_by: Option<UserId> = None;
        let mut assignee_opt: Option<UserId> = None;
        let mut unassigned: bool = false;
        let mut abstract_state_opt: Option<AbstractState> = None;
        let mut created_after: Option<NaiveDateTime> = None;
        let mut crated_before: Option<NaiveDateTime> = None;
        let mut due_by: Option<NaiveDateTime> = None;
        let mut labels: Option<Vec<String>> = None;
        let mut components: Option<Vec<String>> = None;
        let mut updated_after: Option<NaiveDateTime> = None;
        let mut watched_by: Option<UserId> = None;
        let mut node_id_opt: Option<Uuid> = None;

        for rule in filter_rules {
            match rule {
                SearchFilterRule::PriorityHigher(priority) => {
                    priority_higher = Some(priority);
                }
                SearchFilterRule::PriorityLower(priority) => {
                    priority_lower = Some(priority);
                }
                SearchFilterRule::CreatedBy(user) => {
                    created_by = Some(user);
                }
                SearchFilterRule::AssignedTo(user) => {
                    assignee_opt = Some(user);
                }
                SearchFilterRule::Unassigned => {
                    unassigned = true;
                }
                SearchFilterRule::State(state) => {
                    abstract_state_opt = Some(state);
                }
                SearchFilterRule::CreatedAfter(dt) => {
                    created_after = Some(dt.naive_utc());
                }
                SearchFilterRule::CreatedBefore(dt) => {
                    crated_before = Some(dt.naive_utc());
                }
                SearchFilterRule::DueBy(dt) => {
                    due_by = Some(dt.naive_utc());
                }
                SearchFilterRule::Labels(lbls) => {
                    labels = Some(lbls);
                }
                SearchFilterRule::Components(comps) => {
                    components = Some(comps);
                }
                SearchFilterRule::UpdatedAfter(dt) => {
                    updated_after = Some(dt.naive_utc());
                }
                SearchFilterRule::WatchedBy(user) => {
                    watched_by = Some(user);
                }
                SearchFilterRule::NodeId(node_id) => {
                    node_id_opt = Some(node_id.0);
                }
                SearchFilterRule::Archived => {
                    deleted_flag = true;
                }
            }
        }

        // 3) Start building the base query: tasks INNER JOIN task_projects LEFT JOIN task_title_summaries
        let mut qb = tasks::table
            .inner_join(task_projects::table.on(task_projects::task_id.eq(tasks::id)))
            .inner_join(task_metadata::table.on(task_metadata::task_id.eq(tasks::id)))
            .left_join(task_title_summaries::table.on(task_title_summaries::task_id.eq(tasks::id)))
            // mandatory filters:
            .filter(tasks::deleted.eq(deleted_flag))
            .filter(task_projects::project_id.eq_any(project_ids))
            .into_boxed();

        // 4) Apply each optional filter as needed:
        if let Some(priority) = priority_lower {
            qb = qb.filter(tasks::priority.le(priority));
        }
        if let Some(priority) = priority_higher {
            qb = qb.filter(tasks::priority.ge(priority));
        }
        if let Some(user) = created_by {
            qb = qb.filter(tasks::author_id.eq(user.0));
        }
        if let Some(assignee) = assignee_opt {
            qb = qb.filter(tasks::assignee_id.eq(assignee.0));
        }
        if unassigned {
            qb = qb.filter(tasks::assignee_id.is_null());
        }
        if let Some(state) = abstract_state_opt {
            match state {
                AbstractState::NotClosed => {
                    qb = qb.filter(
                        tasks::abstract_state
                            .eq(AbstractState::Open)
                            .or(tasks::abstract_state.eq(AbstractState::InProgress)),
                    );
                }
                state => {
                    qb = qb.filter(tasks::abstract_state.eq(state));
                }
            }
        }
        if let Some(dt) = created_after {
            qb = qb.filter(tasks::created.ge(dt));
        }
        if let Some(dt) = crated_before {
            qb = qb.filter(tasks::created.le(dt));
        }
        if let Some(dt) = due_by {
            qb = qb.filter(tasks::due_date.le(dt));
        }
        if let Some(labels) = labels {
            let labels_subquery = sql::<Array<Text>>(
                "(SELECT COALESCE(
                     ARRAY_AGG(tm.metadata ->> 'label'),
                     ARRAY[]::text[]
                   )
                   FROM task_metadata tm
                   WHERE tm.task_id = tasks.id
                     AND tm.metadata ? 'label')",
            );
            qb = qb.filter(labels_subquery.overlaps_with(labels));
        }
        if let Some(components) = components {
            let components_subquery = sql::<Array<Text>>(
                "(SELECT COALESCE(
                     ARRAY_AGG(tm.metadata ->> 'component'),
                     ARRAY[]::text[]
                   )
                   FROM task_metadata tm
                   WHERE tm.task_id = tasks.id
                     AND tm.metadata ? 'component')",
            );
            qb = qb.filter(components_subquery.overlaps_with(components));
        }
        if let Some(dt) = updated_after {
            qb = qb.filter(exists(
                task_changes::table
                    .filter(task_changes::created.ge(dt))
                    .filter(task_changes::task_id.eq(tasks::id)),
            ));
        }
        if let Some(watcher) = watched_by {
            // EXISTS (SELECT 1 FROM task_watchers WHERE watcher_id = $WATCHER AND task_id = tasks.id)
            qb = qb.filter(exists(
                task_watchers::table
                    .filter(task_watchers::watcher_id.eq(watcher.0))
                    .filter(task_watchers::task_id.eq(tasks::id)),
            ));
        }
        if let Some(node_uuid) = node_id_opt {
            qb = qb.filter(exists(
                task_graphs::table
                    .filter(task_graphs::current_node_id.eq(node_uuid))
                    .filter(task_graphs::task_id.eq(tasks::id)),
            ));
        }

        // 5) Full‐text search clause (still a raw‐SQL snippet), only if `search_sql` is Some
        if let Some(ts_q) = search_sql {
            qb = qb.filter(
               sql::<Bool>(
                   "to_tsvector('english', tasks.title || ' ' || tasks.description || ' ' || tasks.slug) \
                    @@ plainto_tsquery('english', "
               )
               .bind::<Text, _>(ts_q.clone())
               .sql(") OR to_tsvector('english', task_title_summaries.summary) @@ plainto_tsquery('english', ")
               .bind::<Text, _>(ts_q)
               .sql(")"),
           );
        }

        // Replace your current “Apply ordering … Finalize” block with something like this:

        let result_ids = match order_by {
            OrderBy::Due => {
                // Select id plus the columns we sort by (abstract_state, due_date, priority)
                let rows: Vec<(Uuid, AbstractState, Option<NaiveDateTime>, i32)> = qb
                    .select((
                        tasks::id,
                        tasks::abstract_state,
                        tasks::due_date.nullable(), // nullable if due_date can be NULL
                        tasks::priority,
                    ))
                    .distinct()
                    .order((
                        tasks::abstract_state.desc(),
                        tasks::due_date.asc(),
                        tasks::priority.asc(),
                    ))
                    .limit(limit)
                    .offset(offset)
                    .load::<(Uuid, AbstractState, Option<NaiveDateTime>, i32)>(conn)
                    .await?;
                rows.into_iter()
                    .map(|(id, _, _, _)| TaskId(id))
                    .collect::<Vec<_>>()
            }

            OrderBy::Created => {
                // Select id plus created timestamp
                let rows: Vec<(Uuid, NaiveDateTime)> = qb
                    .select((tasks::id, tasks::created))
                    .distinct()
                    .order(tasks::created.desc())
                    .limit(limit)
                    .offset(offset)
                    .load::<(Uuid, NaiveDateTime)>(conn)
                    .await?;
                rows.into_iter()
                    .map(|(id, _)| TaskId(id))
                    .collect::<Vec<_>>()
            }

            OrderBy::Priority => {
                // Select id plus (abstract_state, priority) to satisfy DISTINCT + ORDER BY
                let rows: Vec<(Uuid, AbstractState, i32)> = qb
                    .select((tasks::id, tasks::abstract_state, tasks::priority))
                    .distinct()
                    .order((tasks::abstract_state.desc(), tasks::priority.asc()))
                    .limit(limit)
                    .offset(offset)
                    .load::<(Uuid, AbstractState, i32)>(conn)
                    .await?;
                rows.into_iter()
                    .map(|(id, _, _)| TaskId(id))
                    .collect::<Vec<_>>()
            }

            OrderBy::Alphabetical => {
                // Select id plus title
                let rows: Vec<(Uuid, String)> = qb
                    .select((tasks::id, tasks::title))
                    .distinct()
                    .order(tasks::title.asc())
                    .limit(limit)
                    .offset(offset)
                    .load::<(Uuid, String)>(conn)
                    .await?;
                rows.into_iter()
                    .map(|(id, _)| TaskId(id))
                    .collect::<Vec<_>>()
            }
        };
        Ok(result_ids)
    }

    pub async fn search_tags(
        conn: &mut AsyncPgConnection,
        query: &str,
        user_id: UserId,
        order_by: OrderBy,
        filter_rule: Option<FilterRule>,
        project_ids: Vec<ProjectId>,
        page: u32,
        page_size: u32,
    ) -> QueryResult<Vec<TaskId>> {
        use crate::schema::{
            task_changes, task_graphs, task_metadata, task_projects, task_watchers, tasks,
        };
        use diesel::dsl::exists;

        // 1) Pagination calculations
        let offset = (page.saturating_sub(1) * page_size) as i64;
        let limit = page_size as i64;
        // For LIKE‐pattern matching on JSONB fields:
        let like_pattern = format!("%{}%", query);

        // 2) Pull out optional filters from `filter_rule`
        let mut deleted_flag = false;
        let mut abstract_state_opt: Option<AbstractState> = None;
        let mut assignee_opt: Option<UserId> = None;
        let mut watcher_opt: Option<UserId> = None;
        let mut node_id_opt: Option<Uuid> = None;
        let mut created_after: Option<NaiveDateTime> = None;
        let mut updated_after: Option<NaiveDateTime> = None;

        if let Some(rule) = filter_rule {
            match rule {
                FilterRule::Open => {
                    abstract_state_opt = Some(AbstractState::Open);
                }
                FilterRule::InProgress => {
                    abstract_state_opt = Some(AbstractState::InProgress);
                }
                FilterRule::Closed => {
                    abstract_state_opt = Some(AbstractState::Closed);
                }
                FilterRule::NotClosed => {
                    abstract_state_opt = Some(AbstractState::Closed);
                }
                FilterRule::Archived => {
                    deleted_flag = true;
                }
                FilterRule::NodeId(graph_node_id) => {
                    node_id_opt = Some(graph_node_id.0);
                }
                FilterRule::AssignedTo(other) => {
                    assignee_opt = Some(other);
                }
                FilterRule::AssignedToMe => {
                    assignee_opt = Some(user_id);
                }
                FilterRule::Watching => {
                    watcher_opt = Some(user_id);
                }
                FilterRule::Created(dt) => {
                    created_after = Some(dt.naive_utc());
                }
                FilterRule::Updated(dt) => {
                    updated_after = Some(dt.naive_utc());
                }
            }
        }

        // 3) Build the base query with typed joins
        let mut qb = tasks::table
            .inner_join(task_projects::table.on(task_projects::task_id.eq(tasks::id)))
            .inner_join(task_watchers::table.on(task_watchers::task_id.eq(tasks::id)))
            .inner_join(task_metadata::table.on(task_metadata::task_id.eq(tasks::id)))
            // mandatory filters:
            .filter(tasks::deleted.eq(deleted_flag))
            .filter(task_projects::project_id.eq_any(project_ids))
            .into_boxed::<diesel::pg::Pg>();

        // 4) Apply each optional filter
        if let Some(state) = abstract_state_opt {
            match state {
                AbstractState::NotClosed => {
                    qb = qb.filter(
                        tasks::abstract_state
                            .eq(AbstractState::Open)
                            .or(tasks::abstract_state.eq(AbstractState::InProgress)),
                    );
                }
                state => {
                    qb = qb.filter(tasks::abstract_state.eq(state));
                }
            }
        }
        if let Some(assignee) = assignee_opt {
            qb = qb.filter(tasks::assignee_id.eq(assignee.0));
        }
        if let Some(watcher) = watcher_opt {
            // Only keep rows where watcher_id matches
            qb = qb.filter(task_watchers::watcher_id.eq(watcher.0));
        }
        if let Some(node_uuid) = node_id_opt {
            qb = qb.filter(exists(
                task_graphs::table
                    .filter(task_graphs::current_node_id.eq(node_uuid))
                    .filter(task_graphs::task_id.eq(tasks::id)),
            ));
        }
        if let Some(dt) = updated_after {
            qb = qb.filter(exists(
                task_changes::table
                    .filter(task_changes::created.ge(dt))
                    .filter(task_changes::task_id.eq(tasks::id)),
            ));
        }
        if let Some(dt) = created_after {
            qb = qb.filter(tasks::created.ge(dt));
        }

        // 5) JSONB‐field LIKE filter on tags (label OR component)
        let jsonb_like_filter = sql::<Bool>("(task_metadata.metadata->>'label' LIKE ")
            .bind::<Text, _>(&like_pattern)
            .sql(") OR (task_metadata.metadata->>'component' LIKE ")
            .bind::<Text, _>(&like_pattern)
            .sql(")");

        qb = qb.filter(jsonb_like_filter);

        let result_ids = match order_by {
            OrderBy::Due => {
                // Select id plus the columns we sort by (abstract_state, due_date, priority)
                let rows: Vec<(Uuid, AbstractState, Option<NaiveDateTime>, i32)> = qb
                    .select((
                        tasks::id,
                        tasks::abstract_state,
                        tasks::due_date.nullable(), // nullable if due_date can be NULL
                        tasks::priority,
                    ))
                    .distinct()
                    .order((
                        tasks::abstract_state.desc(),
                        tasks::due_date.asc(),
                        tasks::priority.asc(),
                    ))
                    .limit(limit)
                    .offset(offset)
                    .load::<(Uuid, AbstractState, Option<NaiveDateTime>, i32)>(conn)
                    .await?;
                rows.into_iter()
                    .map(|(id, _, _, _)| TaskId(id))
                    .collect::<Vec<_>>()
            }

            OrderBy::Created => {
                // Select id plus created timestamp
                let rows: Vec<(Uuid, NaiveDateTime)> = qb
                    .select((tasks::id, tasks::created))
                    .distinct()
                    .order(tasks::created.desc())
                    .limit(limit)
                    .offset(offset)
                    .load::<(Uuid, NaiveDateTime)>(conn)
                    .await?;
                rows.into_iter()
                    .map(|(id, _)| TaskId(id))
                    .collect::<Vec<_>>()
            }

            OrderBy::Priority => {
                // Select id plus (abstract_state, priority) to satisfy DISTINCT + ORDER BY
                let rows: Vec<(Uuid, AbstractState, i32)> = qb
                    .select((tasks::id, tasks::abstract_state, tasks::priority))
                    .distinct()
                    .order((tasks::abstract_state.desc(), tasks::priority.asc()))
                    .limit(limit)
                    .offset(offset)
                    .load::<(Uuid, AbstractState, i32)>(conn)
                    .await?;
                rows.into_iter()
                    .map(|(id, _, _)| TaskId(id))
                    .collect::<Vec<_>>()
            }

            OrderBy::Alphabetical => {
                // Select id plus title
                let rows: Vec<(Uuid, String)> = qb
                    .select((tasks::id, tasks::title))
                    .distinct()
                    .order(tasks::title.asc())
                    .limit(limit)
                    .offset(offset)
                    .load::<(Uuid, String)>(conn)
                    .await?;
                rows.into_iter()
                    .map(|(id, _)| TaskId(id))
                    .collect::<Vec<_>>()
            }
        };
        Ok(result_ids)
    }

    pub async fn graphs(&self, conn: &mut AsyncPgConnection) -> QueryResult<Vec<TaskGraph>> {
        use crate::schema::task_graphs;
        let mut graphs = task_graphs::table
            .filter(task_graphs::dsl::task_id.eq(self.id))
            .load::<TaskGraph>(conn)
            .await;
        if let Ok(graphs) = graphs.as_mut() {
            graphs.sort_by_key(|item| item.order_added);
        }
        graphs
    }

    pub async fn insert_into_graph(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        slug: &str,
        graph_id: GraphId,
        project_id: ProjectId,
    ) -> QueryResult<()> {
        let node = GraphNode::create_task(conn, slug, task_id.0, project_id).await?;
        let entry = GraphNode::entry(conn, graph_id).await?;
        let exits = GraphNode::exits(conn, graph_id).await?;
        let exit = exits
            .iter()
            .next()
            .expect("Graph must have at least one exit");

        Graph::insert_between(conn, graph_id, &entry, &node, exit).await?;
        Ok(())
    }

    pub async fn add_link(
        conn: &mut AsyncPgConnection,
        task_from_id: TaskId,
        task_to_id: TaskId,
        link_type: TaskLinkType,
    ) -> QueryResult<bool> {
        let from_projects: HashMap<ProjectId, Project> = HashMap::from_iter(
            TaskProject::list(conn, task_from_id)
                .await
                .into_iter()
                .map(|tp| (tp.id, tp)),
        );
        let from_project_set: HashSet<ProjectId> =
            HashSet::from_iter(from_projects.keys().cloned());
        let to_projects: HashMap<ProjectId, Project> = HashMap::from_iter(
            TaskProject::list(conn, task_to_id)
                .await
                .into_iter()
                .map(|tp| (tp.id, tp)),
        );
        let to_project_set: HashSet<ProjectId> = HashSet::from_iter(to_projects.keys().cloned());
        let intersection: HashSet<_> = from_project_set.intersection(&to_project_set).collect();

        if intersection.is_empty() {
            return Ok(false); // No common project, cannot link tasks
        }

        let mut graph_ids: Vec<GraphId> = Vec::new();
        for project_id in intersection {
            if let Some(project) = from_projects.get(project_id) {
                if let Some(graph_id) = project.task_graph_id {
                    graph_ids.push(GraphId(graph_id));
                }
            }
        }

        use crate::schema::task_links;
        let task_link = TaskLink {
            task_from_id,
            task_to_id,
            link_type,
        };
        let data: TaskLinkData = task_link.into();
        diesel::insert_into(task_links::table)
            .values(&data)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?;

        match link_type {
            TaskLinkType::DependsOn => {
                // A dependency from `task_from_id` to `task_to_id` means that `task_to_id` is a
                // is of lower rank (closer to the entry root) in the graph.

                // For each graph we need to:
                for &graph_id in &graph_ids {
                    // 1. Delete any existing link on `entry` to `task_from_id`
                    let entry = GraphNode::entry(conn, graph_id).await?;
                    let task_from_node =
                        GraphNode::node_by_task_id(conn, graph_id, task_from_id.0).await?;
                    let task_to_node =
                        GraphNode::node_by_task_id(conn, graph_id, task_to_id.0).await?;

                    GraphEdge::delete_edge(conn, entry.id, task_from_node.id).await?;
                    // 2. insert a new link from `task_to_id` to `task_from_id`
                    GraphEdge::create_edge(
                        conn,
                        task_to_node.id,
                        task_from_node.id,
                        json!({"taskLink": "dependsOn"}),
                    )
                    .await?;
                    // 3. Delete any existing links on `task_to_id` to `exits`.
                    let exits = GraphNode::exits(conn, graph_id).await?;
                    for exit in exits {
                        GraphEdge::delete_edge(conn, task_to_node.id, exit.id).await?;
                    }
                }
            }
            _ => {
                // For other link types, we don't need to do anything special
                // as they are not part of the graph logic.
            }
        }

        Ok(true)
    }

    pub async fn blockers(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
    ) -> QueryResult<Vec<TaskId>> {
        use crate::schema::task_links::dsl as task_links;
        use crate::schema::tasks::dsl as tasks;

        // Get all tasks that the current task depends on
        let blockers = task_links::task_links
            .filter(task_links::task_from_id.eq(task_id))
            .filter(task_links::link_type.eq(TaskLinkType::DependsOn as i32))
            .inner_join(tasks::tasks.on(task_links::task_to_id.eq(tasks::id)))
            .filter(tasks::abstract_state.ne(AbstractState::Closed))
            .select(task_links::task_to_id)
            .load::<TaskId>(conn)
            .await?;

        Ok(blockers)
    }

    pub async fn rm_link(&self, conn: &mut AsyncPgConnection, task_id: TaskId) -> bool {
        use crate::schema::task_links;
        let num_deleted = (diesel::delete(task_links::table)
            .filter(
                task_links::dsl::task_from_id
                    .eq(self.id)
                    .or(task_links::dsl::task_to_id.eq(task_id)),
            )
            .execute(conn)
            .await)
            .unwrap_or_default();

        num_deleted > 0
    }

    pub async fn progress(&mut self, conn: &mut AsyncPgConnection) -> QueryResult<bool> {
        let mut candidate_node: Option<GraphNodeId> = None;
        let mut candidate_exit: Option<GraphNodeId> = None;

        'outer: for graph in self.graphs(conn).await? {
            let exits = GraphInstance::exits(conn, graph.graph_id).await?;
            if let Some(current_node_id) = graph.current_node_id {
                let valid_transitions = GraphEdge::edges(conn, current_node_id).await?;
                for node in valid_transitions {
                    let is_exit = exits.iter().any(|exit| exit.id == node.id);
                    if !is_exit && candidate_node.is_none() {
                        candidate_node = Some(node.id);
                        break 'outer;
                    } else if is_exit && candidate_exit.is_none() {
                        candidate_exit = Some(node.id);
                    }
                }
            }
        }

        // Prefer transitions to exits
        if let Some(node_id) = candidate_node {
            self.transition(conn, node_id, false).await
        } else if let Some(node_id) = candidate_exit {
            self.transition(conn, node_id, false).await
        } else {
            Ok(false)
        }
    }

    async fn update_node(
        &mut self,
        conn: &mut AsyncPgConnection,
        node_id: GraphNodeId,
        graph: &TaskGraph,
        entry: &GraphNode,
        exits: &[GraphNode],
    ) -> QueryResult<()> {
        use crate::schema::task_graphs::dsl as task_graphs;
        use crate::schema::tasks::dsl as tasks;

        let abstract_state = if node_id == entry.id {
            AbstractState::Open
        } else if exits.iter().any(|exit| exit.id == node_id) {
            AbstractState::Closed
        } else {
            AbstractState::InProgress
        };
        self.abstract_state = abstract_state;

        match abstract_state {
            AbstractState::Closed => {
                self.update_plan(conn, ChartPlanUpdate::End(Some(Utc::now())))
                    .await
                    .ok();
            }
            AbstractState::InProgress => {
                self.update_plan(conn, ChartPlanUpdate::Start(Some(Utc::now())))
                    .await
                    .ok();
            }
            _ => {}
        }

        conn.transaction(|conn| {
            async move {
                TaskGraphTransition::create(
                    conn,
                    self.id,
                    graph.graph_id,
                    graph.current_node_id.unwrap_or(entry.id),
                    node_id,
                )
                .await?;
                diesel::update(
                    task_graphs::task_graphs
                        .filter(task_graphs::task_id.eq(self.id))
                        .filter(task_graphs::graph_id.eq(graph.graph_id)),
                )
                .set(task_graphs::current_node_id.eq(Some(node_id)))
                .execute(conn)
                .await?;

                diesel::update(tasks::tasks.filter(tasks::id.eq(self.id)))
                    .set(tasks::abstract_state.eq(abstract_state))
                    .execute(conn)
                    .await?;
                Ok::<_, diesel::result::Error>(())
            }
            .scope_boxed()
        })
        .await?;

        Ok(())
    }

    pub async fn transition(
        &mut self,
        conn: &mut AsyncPgConnection,
        node_id: GraphNodeId,
        force: bool,
    ) -> QueryResult<bool> {
        for graph in self.graphs(conn).await? {
            let entry = GraphInstance::entry(conn, graph.graph_id).await?;
            let exits = GraphInstance::exits(conn, graph.graph_id).await?;
            if let Some(current_node_id) = graph.current_node_id {
                let valid_transitions = GraphEdge::edges(conn, current_node_id).await?;
                let contains_id = valid_transitions.iter().any(|node| node.id == node_id);
                if contains_id || force {
                    self.update_node(conn, node_id, &graph, &entry, &exits)
                        .await?;
                    return Ok(true);
                }
            }
        }

        let kind = diesel::result::DatabaseErrorKind::CheckViolation;
        let msg = Box::new(ValidationErrorMessage {
            message: format!("No valid transition to {} found", node_id),
            column: "current_node_id".to_string(),
            constraint_name: "valid_transition_current_node_id".to_string(),
        });
        Err(diesel::result::Error::DatabaseError(kind, msg))
    }

    async fn current_node_id(&self, conn: &mut AsyncPgConnection) -> Option<GraphNodeId> {
        for graph in self.graphs(conn).await.ok()? {
            if let Some(node_id) = graph.current_node_id {
                return Some(node_id);
            }
        }
        None
    }

    /// Update the task from fields from a sync which overwrites all fields
    pub async fn update_sync(&self, conn: &mut AsyncPgConnection) -> QueryResult<()> {
        use crate::schema::tasks::dsl as tasks;
        diesel::update(tasks::tasks.filter(tasks::id.eq(self.id)))
            .set((
                tasks::title.eq(&self.title),
                tasks::description.eq(&self.description),
                tasks::priority.eq(self.priority),
                tasks::due_date.eq(self.due_date),
                tasks::assignee_id.eq(self.assignee_id),
                tasks::abstract_state.eq(self.abstract_state),
            ))
            .execute(conn)
            .await
            .map(|_| ())
    }

    async fn update_inner(
        &mut self,
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        update: TaskUpdate,
    ) -> QueryResult<bool> {
        match update {
            TaskUpdate::Restart => self.restart(conn).await,
            TaskUpdate::Archive => self.close(conn).await,
            TaskUpdate::Restore => self.restore(conn).await,
            TaskUpdate::Unassign => self.unassign_user(conn).await,
            TaskUpdate::Undo => Ok(true),
            TaskUpdate::AssignOther { user_id } => self.assign_user(conn, user_id).await,
            TaskUpdate::AssignSelf => self.assign_user(conn, user_id).await,
            TaskUpdate::AssignSelfIfNotAssigned => {
                if self.assignee_id.is_none() {
                    self.assign_user(conn, user_id).await
                } else {
                    Ok(true)
                }
            }
            TaskUpdate::ChangeDescription { description } => {
                self.set_description(conn, &description).await
            }
            TaskUpdate::ChangeTitle { title } => self.set_title(conn, &title).await,
            TaskUpdate::ChangePriority { value } => self.set_priority(conn, value).await,
            TaskUpdate::ChangeDueDate { due_date } => {
                self.set_due_date(conn, due_date.map(|d| d.naive_utc()))
                    .await
            }
            TaskUpdate::ChangeMilestone { milestone_id } => {
                let result = self.set_milestone(conn, milestone_id).await;
                let now = Utc::now().naive_utc();
                if let Some(milestone_id) = milestone_id {
                    if let Some(milestone) = Milestone::get(conn, milestone_id).await {
                        let due_date = milestone.due_date.unwrap_or(now);
                        if result.is_ok()
                            && (self.due_date.is_none()
                                || self.due_date.as_ref().unwrap() > &due_date)
                        {
                            if let Some(due_date) = milestone.due_date {
                                self.set_due_date(conn, Some(due_date)).await?;
                            }
                        }
                    }
                }
                result.map(|_| true)
            }
            TaskUpdate::UpdatePlannedTask(update) => {
                self.update_plan(conn, update).await.ok();
                Ok(true)
            }
            TaskUpdate::AddProject { project_id } => self.add_project(conn, project_id).await,
            TaskUpdate::RemoveProject { project_id } => self.rm_project(conn, project_id).await,
            TaskUpdate::AddComment { content, parent_id } => {
                self.add_comment(conn, user_id, &content, parent_id).await?;
                Ok(true)
            }
            TaskUpdate::EditComment {
                comment_id,
                content,
            } => {
                let mut comment = Comment::get(conn, comment_id).await?;
                if comment.task_id != self.id {
                    return Ok(false);
                }
                self.edit_comment(conn, &mut comment, &content).await?;
                Ok(true)
            }
            TaskUpdate::RemoveComment { comment_id } => {
                let comment = Comment::get(conn, comment_id).await?;
                if comment.task_id != self.id {
                    return Ok(false);
                }
                self.rm_comment(conn, comment_id).await?;
                Ok(true)
            }
            TaskUpdate::AddRelease { release_id } => self.add_release(conn, release_id).await,
            TaskUpdate::RemoveRelease { release_id } => self.rm_release(conn, release_id).await,
            TaskUpdate::Link { task_id, link_type } => {
                Self::add_link(conn, self.id, task_id, link_type).await
            }
            TaskUpdate::Unlink { task_id } => Ok(self.rm_link(conn, task_id).await),
            TaskUpdate::WatchTask { user_id } => self.add_watcher(conn, user_id).await,
            TaskUpdate::StopWatchingTask { user_id } => self.rm_watcher(conn, user_id).await,
            TaskUpdate::Metadata { source, value } => self.add_metadata(conn, source, value).await,
            TaskUpdate::RemoveMetadata { value } => self.rm_metadata(conn, value).await,
            TaskUpdate::Progress => self.progress(conn).await,
            TaskUpdate::Transition { node_id } => self.transition(conn, node_id, false).await,
            TaskUpdate::ForcedTransition { node_id } => self.transition(conn, node_id, true).await,
            TaskUpdate::Close => self.close(conn).await,
        }
    }

    pub async fn previous_update(
        &self,
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        update: &TaskUpdate,
    ) -> Option<TaskUpdate> {
        match update {
            TaskUpdate::AssignOther { user_id } => Some(TaskUpdate::AssignOther {
                user_id: self.assignee_id.unwrap_or(*user_id),
            }),
            TaskUpdate::AssignSelf => Some(TaskUpdate::AssignOther {
                user_id: self.assignee_id.unwrap_or(user_id),
            }),
            TaskUpdate::AssignSelfIfNotAssigned => Some(TaskUpdate::AssignOther {
                user_id: self.assignee_id.unwrap_or(user_id),
            }),
            TaskUpdate::ChangeDescription { .. } => Some(TaskUpdate::ChangeDescription {
                description: self.description.clone(),
            }),
            TaskUpdate::ChangeTitle { .. } => Some(TaskUpdate::ChangeTitle {
                title: self.title.clone(),
            }),
            TaskUpdate::ChangePriority { .. } => Some(TaskUpdate::ChangePriority {
                value: self.priority,
            }),
            TaskUpdate::ChangeDueDate { .. } => Some(TaskUpdate::ChangeDueDate {
                due_date: self.due_date.map(|d| d.and_utc()),
            }),
            TaskUpdate::ChangeMilestone { .. } => Some(TaskUpdate::ChangeMilestone {
                milestone_id: self.milestone_id,
            }),
            TaskUpdate::UpdatePlannedTask(update) => {
                Some(TaskUpdate::UpdatePlannedTask(update.clone()))
            }
            TaskUpdate::AddProject { project_id } => {
                if self.add_project(conn, *project_id).await.ok()? {
                    Some(TaskUpdate::RemoveProject {
                        project_id: *project_id,
                    })
                } else {
                    None
                }
            }
            TaskUpdate::RemoveProject { project_id } => {
                if self.rm_project(conn, *project_id).await.ok()? {
                    Some(TaskUpdate::AddProject {
                        project_id: *project_id,
                    })
                } else {
                    None
                }
            }
            TaskUpdate::AddComment { .. } => None,
            TaskUpdate::EditComment { comment_id, .. } => {
                let comment = Comment::get(conn, *comment_id).await.ok()?;
                Some(TaskUpdate::EditComment {
                    comment_id: *comment_id,
                    content: comment.content.clone(),
                })
            }
            TaskUpdate::RemoveComment { comment_id } => {
                let comment = Comment::get(conn, *comment_id).await.ok()?;
                if comment.task_id != self.id {
                    return None;
                }
                let parent_id = CommentThread::parent(conn, *comment_id).await;
                Some(TaskUpdate::AddComment {
                    content: comment.content.clone(),
                    parent_id,
                })
            }
            TaskUpdate::AddRelease { release_id } => Some(TaskUpdate::RemoveRelease {
                release_id: *release_id,
            }),
            TaskUpdate::RemoveRelease { release_id } => Some(TaskUpdate::AddRelease {
                release_id: *release_id,
            }),
            TaskUpdate::Link { .. } => None,
            TaskUpdate::Unlink { task_id } => {
                let link = match TaskLink::get(conn, self.id, *task_id).await {
                    Some(link) => link,
                    None => return None,
                };
                Some(TaskUpdate::Link {
                    task_id: *task_id,
                    link_type: link.link_type,
                })
            }
            TaskUpdate::WatchTask { user_id } => {
                Some(TaskUpdate::StopWatchingTask { user_id: *user_id })
            }
            TaskUpdate::StopWatchingTask { user_id } => {
                Some(TaskUpdate::WatchTask { user_id: *user_id })
            }
            TaskUpdate::Metadata { .. } => None,
            TaskUpdate::RemoveMetadata { .. } => None,
            TaskUpdate::Progress => None,
            TaskUpdate::Transition { node_id } => {
                let current_node_id = self.current_node_id(conn).await;
                Some(TaskUpdate::Transition {
                    node_id: current_node_id.unwrap_or(*node_id),
                })
            }
            TaskUpdate::ForcedTransition { node_id } => {
                let current_node_id = self.current_node_id(conn).await;
                Some(TaskUpdate::ForcedTransition {
                    node_id: current_node_id.unwrap_or(*node_id),
                })
            }
            TaskUpdate::Close => Some(TaskUpdate::Restart),
            TaskUpdate::Undo => None,
            TaskUpdate::Archive => Some(TaskUpdate::Restore),
            TaskUpdate::Restore => Some(TaskUpdate::Archive),
            TaskUpdate::Unassign => Some(TaskUpdate::AssignOther {
                user_id: self.assignee_id.unwrap_or(user_id),
            }),
            TaskUpdate::Restart => Some(TaskUpdate::Close),
        }
    }

    pub async fn update(
        &mut self,
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        update: TaskUpdate,
    ) -> QueryResult<Option<TaskUpdate>> {
        let previous: Option<TaskUpdate> = match update.clone() {
            TaskUpdate::AssignOther { user_id } => {
                let previous = TaskUpdate::AssignOther {
                    user_id: self.assignee_id.unwrap_or(user_id),
                };
                if self.assign_user(conn, user_id).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::AssignSelf => {
                let previous = TaskUpdate::AssignOther {
                    user_id: self.assignee_id.unwrap_or(user_id),
                };
                if self.assign_user(conn, user_id).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::AssignSelfIfNotAssigned => {
                if self.assignee_id.is_none() {
                    let previous = TaskUpdate::AssignOther {
                        user_id: self.assignee_id.unwrap_or(user_id),
                    };
                    if self.assign_user(conn, user_id).await? {
                        Some(previous)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            TaskUpdate::ChangeDescription { description } => {
                let previous = TaskUpdate::ChangeDescription {
                    description: self.description.clone(),
                };
                if self.set_description(conn, &description).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::ChangeTitle { title } => {
                let previous = TaskUpdate::ChangeTitle {
                    title: self.title.clone(),
                };
                if self.set_title(conn, &title).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::ChangePriority { value } => {
                let previous = TaskUpdate::ChangePriority {
                    value: self.priority,
                };
                if self.set_priority(conn, value).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::ChangeDueDate { due_date } => {
                let prev_due_date = self.due_date.map(|d| d.and_utc());
                let previous = TaskUpdate::ChangeDueDate {
                    due_date: prev_due_date,
                };
                if self
                    .set_due_date(conn, due_date.map(|d| d.naive_utc()))
                    .await?
                {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::ChangeMilestone { milestone_id } => {
                let previous = TaskUpdate::ChangeMilestone {
                    milestone_id: self.milestone_id,
                };
                self.set_milestone(conn, milestone_id).await?;
                let now = Utc::now().naive_utc();
                if let Some(milestone_id) = milestone_id {
                    if let Some(milestone) = Milestone::get(conn, milestone_id).await {
                        let due_date = milestone.due_date.unwrap_or(now);
                        if self.due_date.is_none() || self.due_date.as_ref().unwrap() > &due_date {
                            if let Some(due_date) = milestone.due_date {
                                self.set_due_date(conn, Some(due_date)).await?;
                            }
                        }
                    }
                }
                Some(previous)
            }
            TaskUpdate::UpdatePlannedTask(update) => {
                let update = self.update_plan(conn, update).await?;
                let previous = TaskUpdate::UpdatePlannedTask(update);
                Some(previous)
            }
            TaskUpdate::AddProject { project_id } => {
                let previous = TaskUpdate::RemoveProject { project_id };
                if self.add_project(conn, project_id).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::RemoveProject { project_id } => {
                let previous = TaskUpdate::AddProject { project_id };
                if self.rm_project(conn, project_id).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::AddComment { content, parent_id } => {
                let result = self.add_comment(conn, user_id, &content, parent_id).await?;
                let previous = TaskUpdate::RemoveComment {
                    comment_id: result.id,
                };
                Some(previous)
            }
            TaskUpdate::EditComment {
                comment_id,
                content,
            } => {
                let mut comment = Comment::get(conn, comment_id).await?;
                if comment.task_id != self.id {
                    return Ok(None);
                }
                let previous = TaskUpdate::EditComment {
                    comment_id,
                    content: comment.content.clone(),
                };
                self.edit_comment(conn, &mut comment, &content).await?;
                Some(previous)
            }
            TaskUpdate::RemoveComment { comment_id } => {
                let comment = Comment::get(conn, comment_id).await?;
                if comment.task_id != self.id {
                    return Ok(None);
                }
                let parent_id = CommentThread::parent(conn, comment_id).await;
                let previous = TaskUpdate::AddComment {
                    content: comment.content.clone(),
                    parent_id,
                };
                self.rm_comment(conn, comment_id).await?;
                Some(previous)
            }
            TaskUpdate::AddRelease { release_id } => {
                let previous = TaskUpdate::RemoveRelease { release_id };
                if self.add_release(conn, release_id).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::RemoveRelease { release_id } => {
                let previous = TaskUpdate::AddRelease { release_id };
                if self.rm_release(conn, release_id).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Link { task_id, link_type } => {
                let previous = TaskUpdate::Unlink { task_id };
                if Self::add_link(conn, self.id, task_id, link_type).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::StopWatchingTask {
                user_id: watcher_id,
            } => {
                let previous = TaskUpdate::WatchTask {
                    user_id: watcher_id,
                };
                if watcher_id == user_id {
                    return Ok(None);
                }
                if self.rm_watcher(conn, user_id).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Metadata { source, value } => {
                let previous = TaskUpdate::RemoveMetadata {
                    value: value.clone(),
                };
                if self.add_metadata(conn, source, value).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Progress => {
                let current_node_id = match self.current_node_id(conn).await {
                    Some(node_id) => node_id,
                    None => return Ok(None),
                };

                let result = self.progress(conn).await?;
                let previous = TaskUpdate::Transition {
                    node_id: current_node_id,
                };
                if result {
                    if self.assignee_id.is_none() {
                        self.assign_user(conn, user_id).await?;
                    }
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Transition { node_id } => {
                let current_node_id = match self.current_node_id(conn).await {
                    Some(node_id) => node_id,
                    None => return Ok(None),
                };
                let previous = TaskUpdate::Transition {
                    node_id: current_node_id,
                };
                if self.transition(conn, node_id, false).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::ForcedTransition { node_id } => {
                let current_node_id = match self.current_node_id(conn).await {
                    Some(node_id) => node_id,
                    None => return Ok(None),
                };
                let previous = TaskUpdate::ForcedTransition {
                    node_id: current_node_id,
                };
                if self.transition(conn, node_id, true).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Close => {
                let current_node_id = match self.current_node_id(conn).await {
                    Some(node_id) => node_id,
                    None => return Ok(None),
                };
                let previous = TaskUpdate::Transition {
                    node_id: current_node_id,
                };
                if self.close(conn).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Restart => {
                let current_node_id = match self.current_node_id(conn).await {
                    Some(node_id) => node_id,
                    None => return Ok(None),
                };
                let previous = TaskUpdate::Transition {
                    node_id: current_node_id,
                };
                if self.restart(conn).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Archive => {
                let previous = TaskUpdate::Restore;
                if self.delete(conn).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Restore => {
                let previous = TaskUpdate::Archive;
                if self.restore(conn).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Unassign => {
                let previous = TaskUpdate::AssignOther {
                    user_id: self.assignee_id.unwrap_or(user_id),
                };
                if self.unassign_user(conn).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::WatchTask { user_id } => {
                let previous = TaskUpdate::StopWatchingTask { user_id };
                if self.add_watcher(conn, user_id).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Undo => {
                let last_change = match TaskChange::last(conn, self.id).await {
                    Some(change) => change,
                    None => return Ok(None),
                };
                let update = match serde_json::from_value(last_change.previous) {
                    Ok(update) => update,
                    Err(e) => {
                        tracing::error!("Error parsing TaskUpdate: {:?}", e);
                        return Ok(None);
                    }
                };
                let previous = match serde_json::from_value(last_change.change) {
                    Ok(update) => update,
                    Err(e) => {
                        tracing::error!("Error parsing TaskUpdate: {:?}", e);
                        return Ok(None);
                    }
                };
                if self.update_inner(conn, user_id, update).await? {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::Unlink { task_id } => {
                let link = match TaskLink::get(conn, self.id, task_id).await {
                    Some(link) => link,
                    None => return Ok(None),
                };
                let previous = TaskUpdate::Link {
                    task_id,
                    link_type: link.link_type,
                };
                if self.rm_link(conn, task_id).await {
                    Some(previous)
                } else {
                    None
                }
            }
            TaskUpdate::RemoveMetadata { value } => {
                let previous = TaskUpdate::Metadata {
                    source: MetadataSource::History,
                    value: value.clone(),
                };
                if self.rm_metadata(conn, value).await? {
                    Some(previous)
                } else {
                    None
                }
            }
        };
        if let Some(previous) = previous {
            let value = serde_json::to_value(update.clone()).unwrap();
            let previous_value = serde_json::to_value(previous.clone()).unwrap();
            TaskChange::create(
                conn,
                self.id,
                user_id,
                UpdateSource::TaskUpdate,
                value,
                previous_value,
            )
            .await?;
            Ok(Some(previous))
        } else {
            Ok(None)
        }
    }

    async fn edit_comment(
        &mut self,
        conn: &mut AsyncPgConnection,
        comment: &mut Comment,
        content: &str,
    ) -> QueryResult<bool> {
        comment.update(conn, content).await?;
        Ok(true)
    }

    async fn rm_comment(
        &mut self,
        conn: &mut AsyncPgConnection,
        comment_id: Uuid,
    ) -> QueryResult<bool> {
        let mut comment = Comment::get(conn, comment_id).await?;
        if comment.task_id != self.id {
            return Ok(false);
        }
        comment.delete(conn).await?;
        Ok(true)
    }

    async fn set_priority(
        &mut self,
        conn: &mut AsyncPgConnection,
        value: i32,
    ) -> QueryResult<bool> {
        use crate::schema::tasks::dsl;
        self.priority = value;
        diesel::update(dsl::tasks.filter(dsl::id.eq(self.id)))
            .set(dsl::priority.eq(self.priority))
            .execute(conn)
            .await?;
        Ok(true)
    }

    async fn set_due_date(
        &mut self,
        conn: &mut AsyncPgConnection,
        due_date: Option<NaiveDateTime>,
    ) -> QueryResult<bool> {
        use crate::schema::tasks::dsl;
        self.due_date = due_date;
        diesel::update(dsl::tasks.filter(dsl::id.eq(self.id)))
            .set(dsl::due_date.eq(self.due_date))
            .execute(conn)
            .await?;
        Ok(true)
    }

    async fn set_milestone(
        &mut self,
        conn: &mut AsyncPgConnection,
        milestone_id: Option<MilestoneId>,
    ) -> QueryResult<()> {
        use crate::schema::tasks::dsl;
        self.milestone_id = milestone_id;
        diesel::update(dsl::tasks.filter(dsl::id.eq(self.id)))
            .set(dsl::milestone_id.eq(self.milestone_id))
            .execute(conn)
            .await?;
        Ok(())
    }

    async fn add_release(
        &mut self,
        conn: &mut AsyncPgConnection,
        release_id: Uuid,
    ) -> QueryResult<bool> {
        use crate::schema::release_tasks;
        let task_release = TaskRelease {
            task_id: self.id,
            release_id,
        };
        diesel::insert_into(release_tasks::table)
            .values(&task_release)
            .execute(conn)
            .await?;
        Ok(true)
    }

    async fn rm_release(
        &mut self,
        conn: &mut AsyncPgConnection,
        release_id: Uuid,
    ) -> QueryResult<bool> {
        use crate::schema::release_tasks;
        diesel::delete(
            release_tasks::table.filter(
                release_tasks::dsl::task_id
                    .eq(self.id)
                    .and(release_tasks::dsl::release_id.eq(release_id)),
            ),
        )
        .execute(conn)
        .await?;
        Ok(true)
    }

    pub async fn relink(
        conn: &mut AsyncPgConnection,
        author_id: UserId,
        task_set: &HashSet<TaskId>,
        task_pairs: &[(TaskId, TaskId)],
    ) -> QueryResult<()> {
        use crate::schema::task_links;
        conn.transaction(|conn| {
            async move {
                diesel::delete(
                    task_links::table.filter(
                        task_links::task_from_id
                            .eq_any(task_set)
                            .or(task_links::task_to_id.eq_any(task_set))
                            .and(task_links::link_type.eq(TaskLinkType::DependsOn as i32)),
                    ),
                )
                .execute(conn)
                .await?;

                for (from_id, to_id) in task_pairs {
                    let task_link = TaskLink {
                        task_from_id: *from_id,
                        task_to_id: *to_id,
                        link_type: TaskLinkType::DependsOn,
                    };
                    let data: TaskLinkData = task_link.into();
                    diesel::insert_into(task_links::table)
                        .values(data)
                        .execute(conn)
                        .await?;
                }

                for task_id in task_set {
                    TaskChange::create(
                        conn,
                        *task_id,
                        author_id,
                        UpdateSource::Graph,
                        json!({
                            "type": "relink",
                            "task_pairs": task_pairs,
                        }),
                        serde_json::json!({}),
                    )
                    .await?;
                }
                QueryResult::Ok(())
            }
            .scope_boxed()
        })
        .await
    }

    async fn assign_user(
        &mut self,
        conn: &mut AsyncPgConnection,
        user_id: UserId,
    ) -> QueryResult<bool> {
        use crate::schema::tasks::dsl;
        self.assignee_id = Some(user_id);
        tracing::debug!("Assigning user {} to task {}", user_id, self.id);
        diesel::update(dsl::tasks.filter(dsl::id.eq(self.id)))
            .set(dsl::assignee_id.eq(self.assignee_id))
            .execute(conn)
            .await?;
        self.update_plan(conn, ChartPlanUpdate::ActualAssignee(Some(user_id)))
            .await
            .ok();
        Ok(true)
    }

    async fn unassign_user(&mut self, conn: &mut AsyncPgConnection) -> QueryResult<bool> {
        use crate::schema::tasks::dsl;
        self.assignee_id = None;
        diesel::update(dsl::tasks.filter(dsl::id.eq(self.id)))
            .set(dsl::assignee_id.eq(self.assignee_id))
            .execute(conn)
            .await?;
        self.update_plan(conn, ChartPlanUpdate::ActualAssignee(None))
            .await
            .ok();
        Ok(true)
    }

    async fn set_description(
        &mut self,
        conn: &mut AsyncPgConnection,
        description: &str,
    ) -> QueryResult<bool> {
        use crate::schema::tasks::dsl;
        self.description = description.to_string();
        diesel::update(dsl::tasks.filter(dsl::id.eq(self.id)))
            .set(dsl::description.eq(description))
            .execute(conn)
            .await?;
        Ok(true)
    }
    async fn set_title(&mut self, conn: &mut AsyncPgConnection, title: &str) -> QueryResult<bool> {
        use crate::schema::tasks::dsl;
        self.title = title.to_string();
        diesel::update(dsl::tasks.filter(dsl::id.eq(self.id)))
            .set(dsl::title.eq(title))
            .execute(conn)
            .await?;
        Ok(true)
    }

    pub async fn title_summaries(&self, conn: &mut AsyncPgConnection) -> Vec<String> {
        TaskTitleSummary::get(conn, self.id)
            .await
            .into_iter()
            .map(|s| s.summary)
            .collect()
    }

    pub async fn get(conn: &mut AsyncPgConnection, id: TaskId) -> Option<Self> {
        use crate::schema::tasks::dsl;
        dsl::tasks
            .filter(dsl::id.eq(id))
            .first::<Self>(conn)
            .await
            .optional()
            .unwrap_or(None)
    }

    pub async fn relink_task_graph_nodes(
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
        graph_id: GraphId,
    ) -> QueryResult<()> {
        use diesel::pg::sql_types::Uuid as DieselUuid;
        use diesel::sql_types::Varchar;

        #[derive(Debug, QueryableByName)]
        struct MissingTask {
            #[diesel(sql_type = DieselUuid)]
            id: TaskId,
            #[diesel(sql_type = Varchar)]
            slug: String,
        }

        let tasks: Vec<MissingTask> = diesel::sql_query(
            r#"
            SELECT t.id, t.slug
            FROM tasks t
            JOIN task_projects tp
              ON tp.task_id = t.id
            JOIN projects p
              ON p.id = tp.project_id
            WHERE p.id = $1
              AND NOT EXISTS (
                SELECT 1
                FROM graph_assignments ga
                JOIN graph_nodes gn
                  ON gn.id = ga.node_id
                WHERE ga.graph_id = p.task_graph_id
                  AND (gn.metadata->>'taskId')::uuid = t.id
            )
        "#,
        )
        .bind::<DieselUuid, _>(project_id)
        .get_results::<MissingTask>(conn)
        .await?;
        for MissingTask { id, slug } in tasks {
            Self::insert_into_graph(conn, id, &slug, graph_id, project_id).await?;
        }
        Ok(())
    }

    pub async fn load_amount(
        conn: &mut AsyncPgConnection,
        project_ids: &[ProjectId],
    ) -> Vec<(TaskId, Option<UserId>, TimeDelta)> {
        use crate::schema::planned_tasks as pt;
        use crate::schema::task_projects as tp;
        use crate::schema::tasks as t;

        t::table
            .filter(t::deleted.eq(false))
            .filter(t::abstract_state.eq(AbstractState::InProgress))
            .filter(t::assignee_id.is_not_null())
            .inner_join(tp::table.on(tp::task_id.eq(t::id)))
            .inner_join(pt::table.on(pt::task_id.eq(t::id)))
            .filter(tp::project_id.eq_any(project_ids))
            .select((t::id, t::assignee_id, pt::expected_duration))
            .load::<(TaskId, Option<UserId>, TimeDelta)>(conn)
            .await
            .unwrap_or_default()
    }
}

fn format_search_query(input: &str) -> String {
    input
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>()
        .join(" & ")
}

#[derive(Queryable, Insertable, Debug, Serialize)]
#[diesel(table_name = crate::schema::release_tasks)]
pub struct TaskRelease {
    pub task_id: TaskId,
    pub release_id: Uuid,
}

#[derive(PartialEq, Queryable, Insertable, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::deployment_tasks)]
pub struct DeploymentTask {
    deployment_id: Uuid,
    task_id: TaskId,
}

/// Join table for all labels on the Task
#[derive(Queryable, Insertable, Debug, Serialize, QueryableByName)]
#[diesel(table_name = crate::schema::task_metadata)]
pub struct TaskMetadata {
    pub id: Uuid,
    pub task_id: TaskId,
    pub metadata: Value,
    pub source: MetadataSource,
}

impl TaskMetadata {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        source: MetadataSource,
        metadata: Value,
    ) -> QueryResult<Self> {
        use crate::schema::task_metadata::dsl;
        let metadata = Self {
            id: Uuid::new_v4(),
            task_id,
            metadata,
            source,
        };
        diesel::insert_into(dsl::task_metadata)
            .values(&metadata)
            .execute(conn)
            .await?;
        Ok(metadata)
    }

    pub async fn get_source(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        source: MetadataSource,
    ) -> QueryResult<Self> {
        use crate::schema::task_metadata::dsl;
        dsl::task_metadata
            .filter(dsl::task_id.eq(task_id))
            .filter(dsl::source.eq(source))
            .first::<Self>(conn)
            .await
    }

    pub async fn update_source(
        &mut self,
        conn: &mut AsyncPgConnection,
        metadata: Value,
    ) -> QueryResult<()> {
        use crate::schema::task_metadata::dsl;
        self.metadata = metadata;
        diesel::update(dsl::task_metadata.filter(dsl::id.eq(self.id)))
            .set(dsl::metadata.eq(&self.metadata))
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// Represents a user watching a Task for updates
#[derive(Queryable, Insertable)]
#[diesel(table_name = crate::schema::task_watchers)]
struct TaskWatcher {
    pub task_id: TaskId,
    pub watcher_id: UserId,
}

/// Represents a change in the state of a Task
#[derive(Queryable, Insertable)]
#[diesel(table_name = crate::schema::task_graph_transitions)]
pub struct TaskGraphTransition {
    pub id: Uuid,
    pub task_id: TaskId,
    pub graph_id: GraphId,
    pub from_node_id: GraphNodeId,
    pub to_node_id: GraphNodeId,
    pub transitioned: NaiveDateTime,
}

impl TaskGraphTransition {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        graph_id: GraphId,
        from_node_id: GraphNodeId,
        to_node_id: GraphNodeId,
    ) -> QueryResult<Self> {
        use crate::schema::task_graph_transitions::dsl;
        let transition = Self {
            id: Uuid::new_v4(),
            task_id,
            graph_id,
            from_node_id,
            to_node_id,
            transitioned: chrono::Utc::now().naive_utc(),
        };
        diesel::insert_into(dsl::task_graph_transitions)
            .values(&transition)
            .execute(conn)
            .await?;
        Ok(transition)
    }

    pub async fn get_transitions<'s>(
        conn: &'s mut AsyncPgConnection,
        graph_ids: &[GraphId],
        from: NaiveDateTime,
        to: NaiveDateTime,
        assignee_id: Option<UserId>,
        priority: Option<(i32, i32)>,
    ) -> QueryResult<
        BoxStream<
            's,
            QueryResult<(
                TaskId,
                GraphId,
                GraphNodeId,
                GraphNodeId,
                NaiveDateTime,
                NaiveDateTime,
                Option<UserId>,
                i32,
            )>,
        >,
    > {
        use crate::schema::task_graph_transitions::dsl as tgt;
        use crate::schema::tasks::dsl as t;

        let mut query = tgt::task_graph_transitions
            .filter(tgt::graph_id.eq_any(graph_ids))
            .filter(tgt::transitioned.ge(from).and(tgt::transitioned.le(to)))
            .inner_join(t::tasks.on(t::id.eq(tgt::task_id)))
            .select((
                tgt::task_id,
                tgt::graph_id,
                tgt::from_node_id,
                tgt::to_node_id,
                t::created,
                tgt::transitioned,
                t::assignee_id,
                t::priority,
            ))
            .into_boxed::<Pg>();

        if let Some(assignee_id) = assignee_id {
            query = query.filter(t::assignee_id.eq(assignee_id));
        }
        if let Some((low, high)) = priority {
            query = query
                .filter(t::priority.ge(low))
                .filter(t::priority.le(high));
        }
        let stream = query
            .order_by((tgt::task_id.asc(), tgt::transitioned.asc()))
            .load_stream::<(
                TaskId,
                GraphId,
                GraphNodeId,
                GraphNodeId,
                NaiveDateTime,
                NaiveDateTime,
                Option<UserId>,
                i32,
            )>(conn)
            .await?;
        Ok(stream.boxed())
    }

    pub async fn get_closed_between(
        conn: &mut AsyncPgConnection,
        graph_ids: &[GraphId],
        from: NaiveDateTime,
        to: NaiveDateTime,
    ) -> QueryResult<Vec<TaskId>> {
        use crate::schema::task_graph_transitions::dsl as tgt;
        let mut closed_state_ids = Vec::new();
        for graph_id in graph_ids {
            let this_closed_state_ids = GraphInstance::exits(conn, *graph_id)
                .await?
                .into_iter()
                .map(|node| node.id)
                .collect::<Vec<GraphNodeId>>();
            closed_state_ids.extend(this_closed_state_ids);
        }
        tgt::task_graph_transitions
            .filter(tgt::graph_id.eq_any(graph_ids))
            .filter(tgt::transitioned.ge(from).and(tgt::transitioned.le(to)))
            .filter(tgt::to_node_id.eq_any(&closed_state_ids))
            .select(tgt::task_id)
            .load::<TaskId>(conn)
            .await
    }

    pub async fn get_in_progress_between(
        conn: &mut AsyncPgConnection,
        graph_ids: &[GraphId],
        from: NaiveDateTime,
        to: NaiveDateTime,
    ) -> QueryResult<Vec<TaskId>> {
        use crate::schema::task_graph_transitions::dsl as tgt;
        use diesel::dsl::not;

        let mut closed_state_ids = Vec::new();
        for graph_id in graph_ids {
            let this_closed_state_ids = GraphInstance::exits(conn, *graph_id)
                .await?
                .into_iter()
                .map(|node| node.id)
                .collect::<Vec<GraphNodeId>>();
            closed_state_ids.extend(this_closed_state_ids);
        }

        tgt::task_graph_transitions
            .filter(tgt::graph_id.eq_any(graph_ids))
            .filter(tgt::transitioned.ge(from).and(tgt::transitioned.le(to)))
            .filter(not(tgt::to_node_id.eq_any(&closed_state_ids)))
            .select(tgt::task_id)
            .load::<TaskId>(conn)
            .await
    }

    pub async fn get_last_transition(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
    ) -> QueryResult<Option<Self>> {
        use crate::schema::task_graph_transitions::dsl;
        dsl::task_graph_transitions
            .filter(dsl::task_id.eq(task_id))
            .order_by(dsl::transitioned.desc())
            .first(conn)
            .await
            .optional()
    }

    pub async fn update_graph(
        &mut self,
        conn: &mut AsyncPgConnection,
        graph_id: GraphId,
        from_node_id: GraphNodeId,
        to_node_id: GraphNodeId,
    ) -> QueryResult<()> {
        use crate::schema::task_graph_transitions::dsl;
        self.graph_id = graph_id;
        diesel::update(dsl::task_graph_transitions.filter(dsl::id.eq(self.id)))
            .set((
                dsl::graph_id.eq(self.graph_id),
                dsl::from_node_id.eq(from_node_id),
                dsl::to_node_id.eq(to_node_id),
            ))
            .execute(conn)
            .await?;
        Ok(())
    }

    pub async fn list(conn: &mut AsyncPgConnection, task_id: TaskId) -> Vec<Self> {
        use crate::schema::task_graph_transitions::dsl;
        dsl::task_graph_transitions
            .filter(dsl::task_id.eq(task_id))
            .order_by(dsl::transitioned.asc())
            .load::<Self>(conn)
            .await
            .unwrap_or_default()
    }
}

/// Represents the state of a task in a Graph
#[derive(Queryable, Insertable)]
#[diesel(table_name = crate::schema::task_graphs)]
pub struct TaskGraph {
    pub task_id: TaskId,
    pub graph_id: GraphId,
    pub current_node_id: Option<GraphNodeId>,
    pub order_added: i32,
}

/// Gets the GraphNode active from a list of TaskGraphs
impl TaskGraph {
    pub async fn get_active_node(
        conn: &mut AsyncPgConnection,
        graphs: &[Self],
    ) -> QueryResult<GraphNode> {
        // TODO: check other TaskGraphs
        let graph = match graphs.first() {
            Some(graph) => graph,
            None => {
                let kind = diesel::result::DatabaseErrorKind::CheckViolation;
                let msg = Box::new(ValidationErrorMessage {
                    message: "Task is missing a graph".to_string(),
                    column: "task_id".to_string(),
                    constraint_name: "task_graph_required".to_string(),
                });
                return Err(diesel::result::Error::DatabaseError(kind, msg));
            }
        };

        let node_id = match graph.current_node_id {
            Some(node_id) => node_id,
            None => {
                let kind = diesel::result::DatabaseErrorKind::CheckViolation;
                let msg = Box::new(ValidationErrorMessage {
                    message: "TaskGraph has no valid node id".to_string(),
                    column: "current_node_id".to_string(),
                    constraint_name: "task_graph_current_node_id_null".to_string(),
                });
                return Err(diesel::result::Error::DatabaseError(kind, msg));
            }
        };

        match GraphNode::get(conn, node_id).await {
            Some(node) => Ok(node),
            None => {
                let kind = diesel::result::DatabaseErrorKind::CheckViolation;
                let msg = Box::new(ValidationErrorMessage {
                    message: "GraphNode is missing".to_string(),
                    column: "current_node_id".to_string(),
                    constraint_name: "task_graph_current_node_id_exists".to_string(),
                });
                Err(diesel::result::Error::DatabaseError(kind, msg))
            }
        }
    }

    pub async fn get_graph_ids(conn: &mut AsyncPgConnection, task_id: TaskId) -> Vec<GraphId> {
        use crate::schema::task_graphs::dsl;
        dsl::task_graphs
            .filter(dsl::task_id.eq(task_id))
            .select(dsl::graph_id)
            .load::<GraphId>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn get(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        graph_id: GraphId,
    ) -> Option<Self> {
        use crate::schema::task_graphs::dsl;
        dsl::task_graphs
            .filter(dsl::task_id.eq(task_id).and(dsl::graph_id.eq(graph_id)))
            .first::<Self>(conn)
            .await
            .optional()
            .ok()?
    }

    pub async fn update_graph(
        &self,
        conn: &mut AsyncPgConnection,
        graph_id: GraphId,
        current_node_id: Option<GraphNodeId>,
    ) -> QueryResult<()> {
        use crate::schema::task_graphs::dsl;
        diesel::update(dsl::task_graphs.filter(dsl::task_id.eq(self.task_id)))
            .filter(dsl::graph_id.eq(self.graph_id))
            .set((
                dsl::graph_id.eq(graph_id),
                dsl::current_node_id.eq(current_node_id),
            ))
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// Represents links between tasks for dependencies and subtask behavior
#[derive(Debug, Clone, Copy, Serialize)]
pub struct TaskLink {
    pub task_from_id: TaskId,
    pub task_to_id: TaskId,
    pub link_type: TaskLinkType,
}

impl TaskLink {
    pub async fn get(
        conn: &mut AsyncPgConnection,
        task_from_id: TaskId,
        task_to_id: TaskId,
    ) -> Option<Self> {
        use crate::schema::task_links::dsl;
        let result = dsl::task_links
            .find((task_from_id, task_to_id))
            .get_result::<(Uuid, Uuid, i32)>(conn)
            .await
            .optional()
            .ok()?;

        result.map(|result| Self {
            task_from_id: TaskId(result.0),
            task_to_id: TaskId(result.1),
            link_type: result.2.into(),
        })
    }
}

#[derive(Queryable, Insertable)]
#[diesel(table_name = crate::schema::task_title_summaries)]
pub struct TaskTitleSummary {
    pub task_id: TaskId,
    pub summary_num: i16,
    pub summary: String,
}

impl TaskTitleSummary {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        summary_num: i16,
        summary: &str,
    ) -> QueryResult<Self> {
        use crate::schema::task_title_summaries::dsl;
        let summary = Self {
            task_id,
            summary_num,
            summary: summary.to_string(),
        };
        diesel::insert_into(dsl::task_title_summaries)
            .values(&summary)
            .on_conflict((dsl::task_id, dsl::summary_num))
            .do_update()
            .set(dsl::summary.eq(&summary.summary))
            .execute(conn)
            .await?;
        Ok(summary)
    }

    pub async fn get(conn: &mut AsyncPgConnection, task_id: TaskId) -> Vec<Self> {
        use crate::schema::task_title_summaries::dsl;
        dsl::task_title_summaries
            .filter(dsl::task_id.eq(task_id))
            .order_by(dsl::summary_num.desc())
            .load::<Self>(conn)
            .await
            .unwrap_or_default()
    }
}

#[derive(Queryable, Insertable)]
#[diesel(table_name = crate::schema::task_links)]
struct TaskLinkData {
    task_from_id: TaskId,
    task_to_id: TaskId,
    link_type: i32,
}

impl From<TaskLink> for TaskLinkData {
    fn from(link: TaskLink) -> TaskLinkData {
        let TaskLink {
            task_from_id,
            task_to_id,
            link_type,
        } = link;
        TaskLinkData {
            task_from_id,
            task_to_id,
            link_type: link_type.into(),
        }
    }
}

impl From<TaskLinkData> for TaskLink {
    fn from(link: TaskLinkData) -> TaskLink {
        let TaskLinkData {
            task_from_id,
            task_to_id,
            link_type,
        } = link;
        TaskLink {
            task_from_id,
            task_to_id,
            link_type: link_type.into(),
        }
    }
}

impl TaskLink {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        link_from_id: TaskId,
        link_to_id: TaskId,
        link_type: TaskLinkType,
    ) -> QueryResult<Self> {
        use crate::schema::task_links;
        let task_link = TaskLink {
            task_from_id: link_from_id,
            task_to_id: link_to_id,
            link_type,
        };
        let data: TaskLinkData = task_link.into();
        diesel::insert_into(task_links::table)
            .values(&data)
            .execute(conn)
            .await?;
        Ok(task_link)
    }

    pub async fn get_outgoing(
        conn: &mut AsyncPgConnection,
        link_from: TaskId,
    ) -> QueryResult<Vec<Self>> {
        use crate::schema::task_links;
        let links = task_links::table
            .filter(task_links::dsl::task_from_id.eq(link_from))
            .load::<TaskLinkData>(conn)
            .await?;
        Ok(links.into_iter().map(|link| link.into()).collect())
    }

    /// Get outgoing task links to open tasks
    pub async fn get_outgoing_open(
        conn: &mut AsyncPgConnection,
        link_from: TaskId,
    ) -> QueryResult<Vec<Self>> {
        use crate::schema::task_links;
        use crate::schema::tasks;

        let links = task_links::table
            .filter(task_links::dsl::task_from_id.eq(link_from))
            .inner_join(tasks::table.on(tasks::dsl::id.eq(task_links::dsl::task_to_id)))
            .filter(tasks::abstract_state.ne(AbstractState::Closed))
            .filter(tasks::deleted.eq(false))
            .select(task_links::all_columns)
            .load::<TaskLinkData>(conn)
            .await?;
        Ok(links.into_iter().map(|link| link.into()).collect())
    }

    pub async fn get_incoming(
        conn: &mut AsyncPgConnection,
        link_to: TaskId,
    ) -> QueryResult<Vec<Self>> {
        use crate::schema::task_links;
        let links = task_links::table
            .filter(task_links::dsl::task_to_id.eq(link_to))
            .load::<TaskLinkData>(conn)
            .await?;
        Ok(links.into_iter().map(|link| link.into()).collect())
    }

    /// Get incoming task links to open tasks
    pub async fn get_incoming_open(
        conn: &mut AsyncPgConnection,
        link_to: TaskId,
    ) -> QueryResult<Vec<Self>> {
        use crate::schema::task_links;
        use crate::schema::tasks;

        let links = task_links::table
            .filter(task_links::dsl::task_to_id.eq(link_to))
            .inner_join(tasks::table.on(tasks::dsl::id.eq(task_links::dsl::task_from_id)))
            .filter(tasks::abstract_state.ne(AbstractState::Closed))
            .filter(tasks::deleted.eq(false))
            .select(task_links::all_columns)
            .load::<TaskLinkData>(conn)
            .await?;
        Ok(links.into_iter().map(|link| link.into()).collect())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = crate::schema::task_attachments)]
pub struct TaskAttachment {
    pub task_id: TaskId,
    pub file_id: Uuid,
}

impl TaskAttachment {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        file_id: Uuid,
    ) -> QueryResult<Self> {
        use crate::schema::task_attachments::dsl;
        let attachment = Self { task_id, file_id };
        diesel::insert_into(dsl::task_attachments)
            .values(&attachment)
            .execute(conn)
            .await?;
        Ok(attachment)
    }

    pub async fn get(conn: &mut AsyncPgConnection, task_id: TaskId) -> QueryResult<Vec<Self>> {
        use crate::schema::task_attachments::dsl;
        dsl::task_attachments
            .filter(dsl::task_id.eq(task_id))
            .load(conn)
            .await
    }

    pub async fn list(conn: &mut AsyncPgConnection, task_id: TaskId) -> Vec<FileUpload> {
        use crate::schema::task_attachments::dsl;
        dsl::task_attachments
            .filter(dsl::task_id.eq(task_id))
            .inner_join(crate::schema::file_uploads::table)
            .select(crate::schema::file_uploads::all_columns)
            .load(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn delete(
        conn: &mut AsyncPgConnection,
        task_id: TaskId,
        file_id: Uuid,
    ) -> QueryResult<()> {
        use crate::schema::task_attachments::dsl;
        diesel::delete(
            dsl::task_attachments
                .filter(dsl::task_id.eq(task_id))
                .filter(dsl::file_id.eq(file_id)),
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tables::test::MIGRATIONS;
    use function_name::named;
    use serde_json::json;
    use subseq_util::tables::harness::{to_pg_db_name, DbHarness};
    use uuid::Uuid;
    use zini_graph::test::test_state_graph;
    use zini_ownership::test::basic_setup;

    #[tokio::test]
    #[named]
    async fn test_task_search_tags() {
        let db_name = to_pg_db_name(function_name!());
        let harness = DbHarness::new("localhost", "development", &db_name, Some(MIGRATIONS)).await;
        let mut conn = harness.conn().await;
        let (user, org) = basic_setup(&mut conn).await;
        let graph = test_state_graph(&mut conn, org.id).await;
        let proj = Project::create(
            &mut conn,
            ProjectId(Uuid::new_v4()),
            user.id,
            "proj",
            "",
            "PROJ",
            graph.id.0,
        )
        .await
        .expect("project is created");

        let task0 = Task::create(
            &mut conn,
            TaskId(Uuid::new_v4()),
            proj.id,
            "Task 0",
            "Do this",
            user.id,
        )
        .await
        .expect("task is created");

        let task1 = Task::create(
            &mut conn,
            TaskId(Uuid::new_v4()),
            proj.id,
            "Task 1",
            "Do this again",
            user.id,
        )
        .await
        .expect("task is created");

        task0
            .add_tag(&mut conn, "test")
            .await
            .expect("metadata is added");
        task1
            .add_metadata(&mut conn, MetadataSource::Tag, json!({"component": "test"}))
            .await
            .expect("metadata is added");

        let task2 = Task::create(
            &mut conn,
            TaskId(Uuid::new_v4()),
            proj.id,
            "Task 2",
            "Do that",
            user.id,
        )
        .await
        .expect("task2 is created");

        let metadata = &[
            json!({
                "label": "not match",
            }),
            json!({
                "component": "not match",
            }),
        ];

        for meta in metadata {
            task2
                .add_metadata(&mut conn, MetadataSource::Tag, meta.to_owned())
                .await
                .expect("metadata is added");
        }

        Task::create(
            &mut conn,
            TaskId(Uuid::new_v4()),
            proj.id,
            "Task 3",
            "Do that again",
            user.id,
        )
        .await
        .expect("task2 is created");

        let query = "test";
        let order_by = OrderBy::Alphabetical;
        let tasks = Task::search_tags(
            &mut conn,
            query,
            user.id,
            order_by,
            None,
            vec![proj.id],
            1,
            10,
        )
        .await
        .expect("search should work");
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0], task0.id);
        assert_eq!(tasks[1], task1.id);
    }

    #[tokio::test]
    #[named]
    async fn test_task_search() {
        let db_name = to_pg_db_name(function_name!());
        let harness = DbHarness::new("localhost", "development", &db_name, Some(MIGRATIONS)).await;
        let mut conn = harness.conn().await;
        let (user, org) = basic_setup(&mut conn).await;
        let graph = test_state_graph(&mut conn, org.id).await;
        let proj = Project::create(
            &mut conn,
            ProjectId(Uuid::new_v4()),
            user.id,
            "proj",
            "",
            "PROJ",
            graph.id.0,
        )
        .await
        .expect("proj");
        let task = Task::create(
            &mut conn,
            TaskId(Uuid::new_v4()),
            proj.id,
            "Task 1",
            "Do this",
            user.id,
        )
        .await
        .expect("task is created");

        let query = "Task";
        let order_by = OrderBy::Alphabetical;
        let tasks = Task::search(
            &mut conn,
            Some(query),
            user.id,
            order_by,
            None,
            vec![proj.id],
            1,
            10,
        )
        .await
        .expect("search should work");
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0], task.id);
    }

    #[tokio::test]
    #[named]
    async fn test_task_handle() {
        let db_name = to_pg_db_name(function_name!());
        let harness = DbHarness::new("localhost", "development", &db_name, Some(MIGRATIONS)).await;
        let mut conn = harness.conn().await;
        let (user, org) = basic_setup(&mut conn).await;
        let graph = test_state_graph(&mut conn, org.id).await;

        let proj = Project::create(
            &mut conn,
            ProjectId(Uuid::new_v4()),
            user.id,
            "proj",
            "",
            "PROJ",
            graph.id.0,
        )
        .await
        .expect("proj");
        let task = Task::create(
            &mut conn,
            TaskId(Uuid::new_v4()),
            proj.id,
            "Task 1",
            "Do this aaaa",
            user.id,
        )
        .await
        .expect("task");
        let task2 = Task::get(&mut conn, task.id).await.expect("task2");

        assert_eq!(task.slug, "PROJ-1");
        assert_eq!(task, task2);
    }
}
