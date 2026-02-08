#[cfg(feature = "api")]
pub mod api;
#[cfg(feature = "sqlx")]
pub mod db;
pub mod error;
pub mod models;

pub mod prelude {
    #[cfg(feature = "api")]
    pub use crate::api::{HasPool, TasksApp};
    #[cfg(feature = "sqlx")]
    pub use crate::db::{
        create_milestone_with_roles, create_project_with_roles, create_task_link_with_roles,
        create_task_tables, create_task_with_roles, delete_milestone_with_roles,
        delete_project_with_roles, delete_task_links_with_roles, delete_task_with_roles,
        get_milestone_with_roles, get_project_with_roles, get_task_with_roles,
        list_milestones_with_roles, list_projects_with_roles, list_tasks_with_roles,
        transition_task_with_roles, update_milestone_with_roles, update_project_with_roles,
        update_task_with_roles,
    };
    pub use crate::error::{ErrorKind, LibError, Result};
    pub use crate::models::{
        CreateMilestonePayload, CreateProjectPayload, CreateTaskLinkPayload, CreateTaskPayload,
        DeadlineSource, ListMilestonesQuery, ListQuery, ListTasksQuery, Milestone, MilestoneId,
        MilestoneType, Paged, Project, ProjectId, ProjectSummary, RepeatSchema, Task, TaskDetails,
        TaskGraphAssignment, TaskId, TaskLink, TaskLinkType, TaskState, TimelineSource,
        TransitionTaskPayload, UpdateMilestonePayload, UpdateProjectPayload, UpdateTaskPayload,
    };
    pub use subseq_auth::group_id::GroupId;
    pub use subseq_auth::user_id::UserId;
    pub use subseq_graph::models::{GraphId, GraphNodeId};
}
