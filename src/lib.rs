#[cfg(feature = "api")]
pub mod api;
#[cfg(feature = "sqlx")]
pub mod db;
pub mod error;
pub mod models;
pub mod permissions;
#[cfg(test)]
pub mod test_harness;

pub mod prelude {
    #[cfg(feature = "api")]
    pub use crate::api::{
        HasPool, MilestoneUpdateHookFuture, ProjectUpdateHookFuture, TaskUpdateHookFuture, TasksApp,
    };
    #[cfg(feature = "sqlx")]
    pub use crate::db::{
        create_milestone_with_roles, create_project_with_roles, create_task_attachment_with_roles,
        create_task_comment_with_roles, create_task_link_with_roles, create_task_tables,
        create_task_with_roles, delete_milestone_with_roles, delete_project_with_roles,
        delete_task_attachment_with_roles, delete_task_comment_with_roles,
        delete_task_links_with_roles, delete_task_with_roles, export_task_markdown_with_roles,
        get_milestone_with_roles, get_project_with_roles, get_task_by_ref_with_roles,
        get_task_log_with_roles, get_task_with_roles, list_milestones_with_roles,
        list_projects_with_roles, list_task_attachments_with_roles, list_task_comments_with_roles,
        list_tasks_with_roles, task_cascade_impact_with_roles, task_project_ids_with_roles,
        transition_task_with_roles, update_milestone_with_roles, update_project_with_roles,
        update_task_comment_with_roles, update_task_with_roles,
    };
    pub use crate::error::{ErrorKind, LibError, Result};
    pub use crate::models::{
        CreateMilestonePayload, CreateProjectPayload, CreateTaskCommentPayload,
        CreateTaskLinkPayload, CreateTaskPayload, DeadlineSource, ListMilestonesQuery, ListQuery,
        ListTasksQuery, Milestone, MilestoneId, MilestoneType, MilestoneUpdate, Paged, Project,
        ProjectId, ProjectSummary, ProjectUpdate, RepeatSchema, Task, TaskAttachment,
        TaskAttachmentFileId, TaskCascadeImpact, TaskCascadeImpactQuery, TaskCascadeOperation,
        TaskComment, TaskCommentId, TaskDetails, TaskFilterRule, TaskGraphAssignment, TaskId,
        TaskLink, TaskLinkType, TaskLogEntry, TaskLogId, TaskOrderBy, TaskState, TaskUpdate,
        TimelineSource, TransitionTaskPayload, UpdateMilestonePayload, UpdateProjectPayload,
        UpdateTaskCommentPayload, UpdateTaskPayload,
    };
    pub use crate::permissions::{
        access_roles, full_permissions, milestone_create, milestone_delete, milestone_read,
        milestone_read_access_roles, milestone_update, project_create, project_delete,
        project_read, project_read_access_roles, project_update, read_permissions, scope_id_global,
        scope_project, scope_task, scope_tasks, task_create, task_delete, task_link, task_read,
        task_read_access_roles, task_transition, task_update, write_permissions,
    };
    pub use subseq_auth::group_id::GroupId;
    pub use subseq_auth::user_id::UserId;
    pub use subseq_graph::models::{GraphId, GraphNodeId};
}
