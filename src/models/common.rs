use serde::{Deserialize, Serialize};

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
