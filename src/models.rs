use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use subseq_auth::group_id::GroupId;
use subseq_auth::user_id::UserId;
use subseq_graph::models::{GraphId, GraphNodeId};
use uuid::Uuid;

macro_rules! uuid_id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
        pub struct $name(pub Uuid);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                Uuid::from_str(s).map(Self)
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }
    };
}

uuid_id_type!(ProjectId);
uuid_id_type!(MilestoneId);
uuid_id_type!(TaskId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeadlineSource {
    Commitment,
    Deliverable,
    Event,
    Contract,
    Presentation,
    Production,
    Legal,
}

impl DeadlineSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Commitment => "commitment",
            Self::Deliverable => "deliverable",
            Self::Event => "event",
            Self::Contract => "contract",
            Self::Presentation => "presentation",
            Self::Production => "production",
            Self::Legal => "legal",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "commitment" => Some(Self::Commitment),
            "deliverable" => Some(Self::Deliverable),
            "event" => Some(Self::Event),
            "contract" => Some(Self::Contract),
            "presentation" => Some(Self::Presentation),
            "production" => Some(Self::Production),
            "legal" => Some(Self::Legal),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimelineSource {
    Week,
    Sprint,
    Month,
    Quarter,
    Year,
}

impl TimelineSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Week => "week",
            Self::Sprint => "sprint",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "week" => Some(Self::Week),
            "sprint" => Some(Self::Sprint),
            "month" => Some(Self::Month),
            "quarter" => Some(Self::Quarter),
            "year" => Some(Self::Year),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MilestoneType {
    Checkpoint,
    Deadline(DeadlineSource),
    Decision,
    Epic,
    Launch,
    Review,
    Timeline(TimelineSource),
    Version,
}

impl MilestoneType {
    pub fn to_db_value(&self) -> String {
        match self {
            Self::Checkpoint => "checkpoint".to_string(),
            Self::Deadline(kind) => format!("deadline:{}", kind.as_str()),
            Self::Decision => "decision".to_string(),
            Self::Epic => "epic".to_string(),
            Self::Launch => "launch".to_string(),
            Self::Review => "review".to_string(),
            Self::Timeline(kind) => format!("timeline:{}", kind.as_str()),
            Self::Version => "version".to_string(),
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "checkpoint" => Some(Self::Checkpoint),
            "decision" => Some(Self::Decision),
            "epic" => Some(Self::Epic),
            "launch" => Some(Self::Launch),
            "review" => Some(Self::Review),
            "version" => Some(Self::Version),
            _ if value.starts_with("deadline:") => {
                let (_, kind) = value.split_once(':')?;
                Some(Self::Deadline(DeadlineSource::parse(kind)?))
            }
            _ if value.starts_with("timeline:") => {
                let (_, kind) = value.split_once(':')?;
                Some(Self::Timeline(TimelineSource::parse(kind)?))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RepeatSchema {
    Increment(u32),
    Date(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Open,
    InProgress,
    Closed,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::InProgress => "in_progress",
            Self::Closed => "closed",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "in_progress" => Some(Self::InProgress),
            "closed" => Some(Self::Closed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskLinkType {
    SubtaskOf,
    DependsOn,
    RelatedTo,
    AssignmentOrder,
}

impl TaskLinkType {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::SubtaskOf => "subtask_of",
            Self::DependsOn => "depends_on",
            Self::RelatedTo => "related_to",
            Self::AssignmentOrder => "assignment_order",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "subtask_of" => Some(Self::SubtaskOf),
            "depends_on" => Some(Self::DependsOn),
            "related_to" => Some(Self::RelatedTo),
            "assignment_order" => Some(Self::AssignmentOrder),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Paged<T> {
    pub page: u32,
    pub limit: u32,
    pub items: Vec<T>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListQuery {
    pub page: Option<u32>,
    pub limit: Option<u32>,
}

impl ListQuery {
    pub fn pagination(&self) -> (u32, u32) {
        let page = self.page.unwrap_or(1).max(1);
        let limit = self.limit.unwrap_or(25).clamp(1, 200);
        (page, limit)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: ProjectId,
    pub owner_user_id: UserId,
    pub owner_group_id: Option<GroupId>,
    pub name: String,
    pub slug: String,
    pub description: String,
    pub task_state_graph_id: GraphId,
    pub task_graph_id: Option<GraphId>,
    pub metadata: Value,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectSummary {
    pub id: ProjectId,
    pub owner_user_id: UserId,
    pub owner_group_id: Option<GroupId>,
    pub name: String,
    pub slug: String,
    pub description: String,
    pub task_state_graph_id: GraphId,
    pub task_graph_id: Option<GraphId>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub task_count: i64,
    pub milestone_count: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateProjectPayload {
    pub name: String,
    pub description: Option<String>,
    pub owner_group_id: Option<GroupId>,
    pub task_state_graph_id: GraphId,
    pub task_graph_id: Option<GraphId>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProjectPayload {
    pub name: Option<String>,
    pub description: Option<String>,
    pub owner_group_id: Option<GroupId>,
    pub clear_owner_group: Option<bool>,
    pub task_state_graph_id: Option<GraphId>,
    pub task_graph_id: Option<GraphId>,
    pub clear_task_graph: Option<bool>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Milestone {
    pub id: MilestoneId,
    pub project_id: ProjectId,
    pub milestone_type: MilestoneType,
    pub name: String,
    pub description: String,
    pub due_date: Option<NaiveDateTime>,
    pub start_date: NaiveDateTime,
    pub started: bool,
    pub completed: bool,
    pub completed_date: Option<NaiveDateTime>,
    pub repeat_interval_seconds: Option<i64>,
    pub repeat_end: Option<NaiveDateTime>,
    pub repeat_schema: Option<RepeatSchema>,
    pub metadata: Value,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListMilestonesQuery {
    pub project_id: Option<ProjectId>,
    pub completed: Option<bool>,
    pub page: Option<u32>,
    pub limit: Option<u32>,
}

impl ListMilestonesQuery {
    pub fn pagination(&self) -> (u32, u32) {
        let page = self.page.unwrap_or(1).max(1);
        let limit = self.limit.unwrap_or(25).clamp(1, 200);
        (page, limit)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMilestonePayload {
    pub project_id: ProjectId,
    pub milestone_type: MilestoneType,
    pub name: String,
    pub description: Option<String>,
    pub due_date: Option<DateTime<Utc>>,
    pub start_date: Option<DateTime<Utc>>,
    pub started: Option<bool>,
    pub completed: Option<bool>,
    pub completed_date: Option<DateTime<Utc>>,
    pub repeat_interval_seconds: Option<i64>,
    pub repeat_end: Option<DateTime<Utc>>,
    pub repeat_schema: Option<RepeatSchema>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMilestonePayload {
    pub milestone_type: Option<MilestoneType>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub due_date: Option<DateTime<Utc>>,
    pub clear_due_date: Option<bool>,
    pub start_date: Option<DateTime<Utc>>,
    pub started: Option<bool>,
    pub completed: Option<bool>,
    pub completed_date: Option<DateTime<Utc>>,
    pub clear_completed_date: Option<bool>,
    pub repeat_interval_seconds: Option<i64>,
    pub clear_repeat: Option<bool>,
    pub repeat_end: Option<DateTime<Utc>>,
    pub repeat_schema: Option<RepeatSchema>,
    pub metadata: Option<Value>,
}

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
    pub graph_id: Option<GraphId>,
    pub node_id: GraphNodeId,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskLinkPayload {
    pub other_task_id: TaskId,
    pub link_type: TaskLinkType,
}
