use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use subseq_auth::user_id::UserId;
use subseq_graph::models::{GraphId, GraphNodeId};

use super::{MilestoneId, ProjectId, TaskCommentId, TaskId, TaskLinkType, TaskLogId, TaskState};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: TaskId,
    pub slug: String,
    pub title: String,
    pub description: String,
    pub author_user_id: UserId,
    pub assignee_user_id: Option<UserId>,
    pub priority: i32,
    pub due_date: Option<NaiveDateTime>,
    pub milestone_id: Option<MilestoneId>,
    pub state: TaskState,
    pub archived: bool,
    pub completed_by_user_id: Option<UserId>,
    pub completed_at: Option<NaiveDateTime>,
    pub rejected_reason: Option<String>,
    pub metadata: Value,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskGraphAssignment {
    pub graph_id: GraphId,
    pub current_node_id: Option<GraphNodeId>,
    pub order_added: i32,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskLink {
    pub task_from_id: TaskId,
    pub task_to_id: TaskId,
    pub link_type: TaskLinkType,
    pub subtask_parent_state: Option<TaskState>,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskComment {
    pub id: TaskCommentId,
    pub task_id: TaskId,
    pub author_user_id: UserId,
    pub body: String,
    pub metadata: Value,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskLogEntry {
    pub id: TaskLogId,
    pub task_id: TaskId,
    pub actor_user_id: UserId,
    pub action: String,
    pub from_state: Option<TaskState>,
    pub to_state: Option<TaskState>,
    pub details: Value,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDetails {
    pub task: Task,
    pub project_ids: Vec<ProjectId>,
    pub graph_assignments: Vec<TaskGraphAssignment>,
    pub links_out: Vec<TaskLink>,
    pub links_in: Vec<TaskLink>,
    pub comments: Vec<TaskComment>,
    pub log: Vec<TaskLogEntry>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksQuery {
    pub project_id: Option<ProjectId>,
    pub assignee_user_id: Option<UserId>,
    pub state: Option<TaskState>,
    pub archived: Option<bool>,
    pub page: Option<u32>,
    pub limit: Option<u32>,
}

impl ListTasksQuery {
    pub fn pagination(&self) -> (u32, u32) {
        let page = self.page.unwrap_or(1).max(1);
        let limit = self.limit.unwrap_or(25).clamp(1, 200);
        (page, limit)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskPayload {
    pub project_id: ProjectId,
    pub title: String,
    pub description: Option<String>,
    pub assignee_user_id: Option<UserId>,
    pub priority: Option<i32>,
    pub due_date: Option<DateTime<Utc>>,
    pub milestone_id: Option<MilestoneId>,
    pub state: Option<TaskState>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTaskPayload {
    pub title: Option<String>,
    pub description: Option<String>,
    pub assignee_user_id: Option<UserId>,
    pub clear_assignee: Option<bool>,
    pub priority: Option<i32>,
    pub due_date: Option<DateTime<Utc>>,
    pub clear_due_date: Option<bool>,
    pub milestone_id: Option<MilestoneId>,
    pub clear_milestone: Option<bool>,
    pub state: Option<TaskState>,
    pub archived: Option<bool>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransitionTaskPayload {
    pub to_state: TaskState,
    pub when: Option<DateTime<Utc>>,
    pub assigned_to_user_id: Option<UserId>,
    pub deferral_reason: Option<String>,
    pub cant_do_reason: Option<String>,
    pub estimated_time_to_complete: Option<String>,
    pub work_log_details: Option<String>,
    pub feedback: Option<String>,
    pub done_by_user_id: Option<UserId>,
    pub done_at: Option<DateTime<Utc>>,
    pub rejected_reason: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskLinkPayload {
    pub other_task_id: TaskId,
    pub link_type: TaskLinkType,
    pub subtask_parent_state: Option<TaskState>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskCommentPayload {
    pub body: String,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskCascadeOperation {
    Archive,
    Unarchive,
    Delete,
}

impl TaskCascadeOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Archive => "archive",
            Self::Unarchive => "unarchive",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskCascadeImpactQuery {
    pub operation: TaskCascadeOperation,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskCascadeImpact {
    pub task_id: TaskId,
    pub operation: TaskCascadeOperation,
    pub affected_task_count: i64,
}
