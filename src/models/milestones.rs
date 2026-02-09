use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use subseq_auth::group_id::GroupId;
use subseq_auth::user_id::UserId;

use super::{DeadlineSource, MilestoneId, ProjectId, RepeatSchema, TimelineSource};

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

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Milestone {
    pub id: MilestoneId,
    pub project_id: ProjectId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_slug: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_owner_user_id: Option<UserId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_owner_username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_owner_group_id: Option<GroupId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_owner_group_display_name: Option<String>,
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
    pub next_milestone_id: Option<MilestoneId>,
    pub previous_milestone_id: Option<MilestoneId>,
    pub metadata: Value,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListMilestonesQuery {
    pub project_id: Option<ProjectId>,
    pub completed: Option<bool>,
    pub query: Option<String>,
    pub due: Option<DateTime<Utc>>,
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
    pub next_milestone_id: Option<MilestoneId>,
    pub previous_milestone_id: Option<MilestoneId>,
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
    pub next_milestone_id: Option<MilestoneId>,
    pub clear_next_milestone: Option<bool>,
    pub previous_milestone_id: Option<MilestoneId>,
    pub clear_previous_milestone: Option<bool>,
    pub metadata: Option<Value>,
}
