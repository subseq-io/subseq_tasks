use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use subseq_util::UserId;
use uuid::Uuid;
use zini_graph::GraphNodeId;
use zini_ownership::ProjectId;

use super::{ChartPlanUpdate, MetadataSource, MilestoneId, TaskId, TaskLinkType};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TaskUpdate {
    #[serde(rename_all = "camelCase")]
    AssignOther {
        user_id: UserId,
    },
    AssignSelf,
    AssignSelfIfNotAssigned,
    #[serde(rename_all = "camelCase")]
    ChangeDescription {
        description: String,
    },
    #[serde(rename_all = "camelCase")]
    ChangeTitle {
        title: String,
    },
    #[serde(rename_all = "camelCase")]
    ChangePriority {
        value: i32,
    },
    #[serde(rename_all = "camelCase")]
    ChangeDueDate {
        due_date: Option<DateTime<Utc>>,
    },
    #[serde(rename_all = "camelCase")]
    ChangeMilestone {
        milestone_id: Option<MilestoneId>,
    },
    #[serde(rename_all = "camelCase")]
    AddProject {
        project_id: ProjectId,
    },
    #[serde(rename_all = "camelCase")]
    RemoveProject {
        project_id: ProjectId,
    },
    #[serde(rename_all = "camelCase")]
    AddComment {
        content: String,
        parent_id: Option<Uuid>,
    },
    #[serde(rename_all = "camelCase")]
    EditComment {
        comment_id: Uuid,
        content: String,
    },
    #[serde(rename_all = "camelCase")]
    RemoveComment {
        comment_id: Uuid,
    },
    #[serde(rename_all = "camelCase")]
    AddRelease {
        release_id: Uuid,
    },
    #[serde(rename_all = "camelCase")]
    RemoveRelease {
        release_id: Uuid,
    },
    UpdatePlannedTask(ChartPlanUpdate),
    #[serde(rename_all = "camelCase")]
    Link {
        task_id: TaskId,
        link_type: TaskLinkType,
    },
    StopWatchingTask {
        user_id: UserId,
    },
    Metadata {
        source: MetadataSource,
        value: Value,
    },
    Progress,
    #[serde(rename_all = "camelCase")]
    Transition {
        node_id: GraphNodeId,
    },
    #[serde(rename_all = "camelCase")]
    ForcedTransition {
        node_id: GraphNodeId,
    },
    Close,
    Restart,
    Archive,
    Restore,
    Unassign,
    Undo,
    #[serde(rename_all = "camelCase")]
    Unlink {
        task_id: TaskId,
    },
    #[serde(rename_all = "camelCase")]
    RemoveMetadata {
        value: Value,
    },
    WatchTask {
        user_id: UserId,
    },
}

impl TaskUpdate {
    pub fn to_display(&self) -> String {
        match self {
            Self::AssignOther { user_id: _ } => "Assigned someone to this task".to_string(),
            Self::AssignSelf => "Assigned themselves to this task".to_string(),
            Self::AssignSelfIfNotAssigned => {
                "Was assigned to the task transitioning it to an in progress state".to_string()
            }
            Self::ChangeDescription { description: _ } => {
                "Changed the task description".to_string()
            }
            Self::ChangeDueDate { due_date: _ } => "Set the task due date".to_string(),
            Self::ChangePriority { value: _ } => "Set the task priority".to_string(),
            Self::ChangeMilestone { milestone_id: _ } => "Set the task milestone".to_string(),
            Self::ChangeTitle { title: _ } => "Changed the task title".to_string(),
            Self::AddProject { project_id: _ } => "Added the task to a project".to_string(),
            Self::RemoveProject { project_id: _ } => "Removed the task from a project".to_string(),
            Self::AddComment {
                content: _,
                parent_id: _,
            } => "Commented on the task".to_string(),
            Self::EditComment {
                comment_id: _,
                content: _,
            } => "Edited their comment".to_string(),
            Self::RemoveComment { comment_id: _ } => "Removed their comment".to_string(),
            Self::AddRelease { release_id: _ } => "Added the task to a release".to_string(),
            Self::RemoveRelease { release_id: _ } => "Removed a release from the task".to_string(),
            Self::UpdatePlannedTask(_) => "Updated the task's plan times".to_string(),
            Self::Link {
                task_id: _,
                link_type: _,
            } => "Linked the task to another".to_string(),
            Self::StopWatchingTask { user_id: _ } => "Stopped watching the task".to_string(),
            Self::Metadata {
                source: _,
                value: _,
            } => "Added tag metadata".to_string(),
            Self::Progress => "Advanced the task state".to_string(),
            Self::Transition { node_id: _ } => "Set a new state for the task".to_string(),
            Self::ForcedTransition { node_id: _ } => "Set a new state for the task".to_string(),
            Self::Close => "Closed the task".to_string(),
            Self::Restart => "Reopened the task".to_string(),
            Self::Archive => "Archived the task".to_string(),
            Self::Restore => "Restored the task".to_string(),
            Self::Unassign => "Unassigned the task".to_string(),
            Self::Undo => "Undid the last action".to_string(),
            Self::Unlink { task_id: _ } => "Removed a link for the task".to_string(),
            Self::RemoveMetadata { value: _ } => "Removed tag metadata from the task".to_string(),
            Self::WatchTask { user_id: _ } => "Started watching the task".to_string(),
        }
    }

    /// Given 2 sets of tags, return the TaskUpdates needed to make the current tags match the target tags
    pub fn from_tag_differences(current: &[String], target: &[String]) -> Vec<TaskUpdate> {
        let current: HashSet<String> = HashSet::from_iter(current.iter().cloned());
        let target: HashSet<String> = HashSet::from_iter(target.iter().cloned());
        let mut updates = Vec::new();

        for label in &current {
            if !target.contains(label) {
                updates.push(TaskUpdate::RemoveMetadata {
                    value: json!({"label": label}),
                });
            }
        }

        for label in &target {
            if !current.contains(label) {
                updates.push(TaskUpdate::Metadata {
                    source: MetadataSource::Tag,
                    value: json!({"label": label}),
                });
            }
        }

        updates
    }
}
