use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use subseq_auth::group_id::GroupId;
use subseq_auth::user_id::UserId;
use subseq_graph::models::GraphId;

use super::ProjectId;

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
