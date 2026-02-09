use std::str::FromStr;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use subseq_auth::user_id::UserId;
use subseq_graph::models::{GraphId, GraphNodeId};

use super::{
    MilestoneId, ProjectId, TaskAttachmentFileId, TaskCommentId, TaskId, TaskLinkType, TaskLogId,
    TaskState,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: TaskId,
    pub slug: String,
    pub title: String,
    pub description: String,
    pub author_user_id: UserId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author_username: Option<String>,
    pub assignee_user_id: Option<UserId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee_username: Option<String>,
    pub priority: i32,
    pub due_date: Option<NaiveDateTime>,
    pub milestone_id: Option<MilestoneId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub milestone_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub milestone_type: Option<String>,
    pub state: TaskState,
    pub archived: bool,
    pub completed_by_user_id: Option<UserId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_by_username: Option<String>,
    pub completed_at: Option<NaiveDateTime>,
    pub rejected_reason: Option<String>,
    pub metadata: Value,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskGraphAssignment {
    pub graph_id: GraphId,
    pub current_node_id: Option<GraphNodeId>,
    pub order_added: i32,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskLink {
    pub task_from_id: TaskId,
    pub task_to_id: TaskId,
    pub link_type: TaskLinkType,
    pub subtask_parent_state: Option<TaskState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub other_task_title: Option<String>,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskComment {
    pub id: TaskCommentId,
    pub task_id: TaskId,
    pub author_user_id: UserId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author_username: Option<String>,
    pub body: String,
    pub metadata: Value,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskAttachment {
    pub task_id: TaskId,
    pub file_id: TaskAttachmentFileId,
    pub added_by_user_id: UserId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub added_by_username: Option<String>,
    pub metadata: Value,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskLogEntry {
    pub id: TaskLogId,
    pub task_id: TaskId,
    pub actor_user_id: UserId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_username: Option<String>,
    pub action: String,
    pub from_state: Option<TaskState>,
    pub to_state: Option<TaskState>,
    pub details: Value,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskProjectPresentation {
    pub id: ProjectId,
    pub name: String,
    pub slug: String,
    pub order_added: i32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDetails {
    pub task: Task,
    pub project_ids: Vec<ProjectId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projects: Vec<TaskProjectPresentation>,
    pub graph_assignments: Vec<TaskGraphAssignment>,
    pub links_out: Vec<TaskLink>,
    pub links_in: Vec<TaskLink>,
    pub comments: Vec<TaskComment>,
    pub log: Vec<TaskLogEntry>,
}

fn deserialize_project_ids<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<ProjectId>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let input = Option::<String>::deserialize(deserializer)?;
    match input {
        None => Ok(None),
        Some(value) => {
            let mut output = Vec::new();
            for raw in value.split(',') {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let parsed = ProjectId::from_str(trimmed).map_err(|err| {
                    serde::de::Error::custom(format!(
                        "Invalid project id in projectIds '{}': {}",
                        trimmed, err
                    ))
                })?;
                output.push(parsed);
            }
            Ok(Some(output))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskOrderBy {
    Created,
    Updated,
    Priority,
    DueDate,
}

impl TaskOrderBy {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Updated => "updated",
            Self::Priority => "priority",
            Self::DueDate => "due_date",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskFilterRule {
    Archived,
    AssignedTo(UserId),
    AssignedToMe,
    Closed,
    NotClosed,
    CreatedAfter(DateTime<Utc>),
    UpdatedAfter(DateTime<Utc>),
    NodeId(GraphNodeId),
    InProgress,
    Open,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksQuery {
    #[serde(deserialize_with = "deserialize_project_ids")]
    pub project_ids: Option<Vec<ProjectId>>,
    pub project_id: Option<ProjectId>,
    pub assignee_user_id: Option<UserId>,
    pub state: Option<TaskState>,
    pub archived: Option<bool>,
    pub query: Option<String>,
    pub order: Option<TaskOrderBy>,
    pub filter_rule: Option<String>,
    pub filter_rule_data: Option<String>,
    pub page: Option<u32>,
    pub limit: Option<u32>,
}

impl ListTasksQuery {
    pub fn pagination(&self) -> (u32, u32) {
        let page = self.page.unwrap_or(1).max(1);
        let limit = self.limit.unwrap_or(25).clamp(1, 200);
        (page, limit)
    }

    pub fn extract_filter_rule(&self) -> std::result::Result<Option<TaskFilterRule>, String> {
        let Some(filter_rule) = self.filter_rule.as_deref() else {
            return Ok(None);
        };
        let filter_data = self.filter_rule_data.as_deref();
        let rule = match filter_rule {
            "archived" => TaskFilterRule::Archived,
            "assignedTo" => {
                let user_id = filter_data.ok_or_else(|| {
                    "filterRuleData is required for filterRule=assignedTo".to_string()
                })?;
                let parsed = UserId::from_str(user_id).map_err(|err| {
                    format!(
                        "invalid user id '{}' for filterRule=assignedTo: {}",
                        user_id, err
                    )
                })?;
                TaskFilterRule::AssignedTo(parsed)
            }
            "assignedToMe" => TaskFilterRule::AssignedToMe,
            "closed" => TaskFilterRule::Closed,
            "notClosed" => TaskFilterRule::NotClosed,
            "created" => {
                let raw = filter_data.ok_or_else(|| {
                    "filterRuleData is required for filterRule=created".to_string()
                })?;
                let parsed = DateTime::parse_from_rfc3339(raw).map_err(|err| {
                    format!(
                        "invalid RFC3339 timestamp '{}' for created filter: {}",
                        raw, err
                    )
                })?;
                TaskFilterRule::CreatedAfter(parsed.with_timezone(&Utc))
            }
            "updated" => {
                let raw = filter_data.ok_or_else(|| {
                    "filterRuleData is required for filterRule=updated".to_string()
                })?;
                let parsed = DateTime::parse_from_rfc3339(raw).map_err(|err| {
                    format!(
                        "invalid RFC3339 timestamp '{}' for updated filter: {}",
                        raw, err
                    )
                })?;
                TaskFilterRule::UpdatedAfter(parsed.with_timezone(&Utc))
            }
            "nodeId" => {
                let raw = filter_data.ok_or_else(|| {
                    "filterRuleData is required for filterRule=nodeId".to_string()
                })?;
                let parsed = GraphNodeId::from_str(raw)
                    .map_err(|err| format!("invalid graph node id '{}': {}", raw, err))?;
                TaskFilterRule::NodeId(parsed)
            }
            "inProgress" => TaskFilterRule::InProgress,
            "open" => TaskFilterRule::Open,
            other => return Err(format!("unsupported filterRule '{}'", other)),
        };
        Ok(Some(rule))
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTaskCommentPayload {
    pub body: String,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone)]
pub enum TaskUpdate {
    TaskCreate { payload: CreateTaskPayload },
    TaskUpdated { payload: UpdateTaskPayload },
    TaskArchive { payload: UpdateTaskPayload },
    TaskUnarchive { payload: UpdateTaskPayload },
    TaskDeleted,
    TaskTransitioned { payload: TransitionTaskPayload },
    TaskLinkCreated { payload: CreateTaskLinkPayload },
    TaskLinkDeleted { other_task_id: TaskId },
    TaskCommentCreated { comment_id: TaskCommentId },
    TaskCommentUpdated { comment_id: TaskCommentId },
    TaskCommentDeleted { comment_id: TaskCommentId },
    TaskAttachmentAdded { file_id: TaskAttachmentFileId },
    TaskAttachmentRemoved { file_id: TaskAttachmentFileId },
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

#[cfg(test)]
mod tests {
    use super::*;

    fn base_query() -> ListTasksQuery {
        ListTasksQuery {
            project_ids: None,
            project_id: None,
            assignee_user_id: None,
            state: None,
            archived: None,
            query: None,
            order: None,
            filter_rule: None,
            filter_rule_data: None,
            page: None,
            limit: None,
        }
    }

    #[test]
    fn extract_filter_rule_assigned_to_parses_user() {
        let mut query = base_query();
        query.filter_rule = Some("assignedTo".to_string());
        query.filter_rule_data = Some("4f11c6f8-5cf4-4f0e-b2cf-87d0fd9e7883".to_string());

        let parsed = query
            .extract_filter_rule()
            .expect("expected assignedTo filter to parse");
        match parsed {
            Some(TaskFilterRule::AssignedTo(_)) => {}
            other => panic!("unexpected parsed filter: {:?}", other),
        }
    }

    #[test]
    fn extract_filter_rule_created_requires_rfc3339() {
        let mut query = base_query();
        query.filter_rule = Some("created".to_string());
        query.filter_rule_data = Some("not-a-date".to_string());

        let error = query
            .extract_filter_rule()
            .expect_err("expected invalid RFC3339 date to fail");
        assert!(error.contains("invalid RFC3339"));
    }
}
