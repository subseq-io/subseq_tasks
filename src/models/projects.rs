use std::str::FromStr;

use chrono::{DateTime, Utc};
use diesel_async::AsyncPgConnection;
use futures::Future;
use serde::{Deserialize, Serialize};
use subseq_util::api::RejectReason;
use subseq_util::UserId;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

use crate::import::{ImportRequest, ImportStateInfo};
use crate::{
    update_project_metadata, ActiveProject, DenormalizedUser, ImportState, Organization,
    OrganizationId, OrganizationProject, PriorityLevel, Project, ProjectId, ProjectImportState,
    ProjectMetadata, ProjectMetadataProps, ProjectTracker, ProjectTrackerType, UpdateProject,
};

use super::organizations::validate_org_capacity;

impl Project {
    pub async fn api_get(
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        id: ProjectId,
    ) -> Result<Self, RejectReason> {
        Self::get_secure(conn, user_id, id)
            .await
            .ok_or_else(|| RejectReason::not_found(format!("Project {}", id)))
    }
}

impl ProjectTracker {
    pub async fn api_get(conn: &mut AsyncPgConnection, id: Uuid) -> Result<Self, RejectReason> {
        Self::get(conn, id)
            .await
            .map_err(|_| RejectReason::not_found(format!("ProjectTracker {}", id)))
    }
}

impl ActiveProject {
    pub async fn api_get(
        conn: &mut AsyncPgConnection,
        user_id: UserId,
    ) -> Result<ActiveProject, RejectReason> {
        ActiveProject::get(conn, user_id)
            .await
            .ok_or_else(|| RejectReason::not_found(format!("ActiveProject {}", user_id)))
    }
}

#[derive(Deserialize, Clone, Debug, Serialize)]
pub struct ImportStateSerialized {
    pub state: ImportState,
    pub error: Option<String>,
    pub info: ImportStateInfo,
}

#[derive(Deserialize, Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedRepositoryTracker {
    id: Uuid,
    tracker: ProjectTrackerType,
    repo_git_remote: String,
    repo_web_url: Option<String>,
    is_default: bool,
    import_state: Option<ImportStateSerialized>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedProject {
    pub id: ProjectId,
    pub name: String,
    pub slug: String,
    pub owner: DenormalizedUser,
    pub created: DateTime<Utc>,
    pub description: String,
    pub repos: Vec<SerializedRepositoryTracker>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_id: Option<OrganizationId>,
    pub task_state_graph_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_graph_id: Option<Uuid>,
}

impl DenormalizedProject {
    pub fn into_project(self) -> Project {
        Project {
            id: self.id,
            name: self.name,
            slug: Some(self.slug),
            owner_id: self.owner.id,
            created: self.created.naive_utc(),
            description: self.description,
            task_state_graph_id: self.task_state_graph_id,
            task_graph_id: self.task_graph_id,
            deleted: false,
        }
    }

    pub async fn denormalize(conn: &mut AsyncPgConnection, project: &Project) -> Self {
        let trackers = ProjectTracker::by_project(conn, project.id).await;
        let mut import_states = vec![];
        for import_state in trackers.iter() {
            import_states
                .push(ProjectImportState::by_project(conn, project.id, import_state.id).await);
        }

        let repos = std::iter::zip(trackers.iter(), import_states.iter())
            .map(|(t, s)| SerializedRepositoryTracker {
                id: t.id,
                tracker: ProjectTrackerType::from_str(&t.tracker).expect("All trackers are valid"),
                repo_git_remote: t.repo.clone(),
                repo_web_url: t.repo_url.clone(),
                is_default: t.is_default,
                import_state: s.as_ref().map(|state| {
                    let import_state = ImportState::from_database(&state.import_state);
                    ImportStateSerialized {
                        state: import_state.clone(),
                        error: state.import_error.clone(),
                        info: ImportStateInfo::from_info(&import_state, 0),
                    }
                }),
            })
            .collect();
        let org_id = OrganizationProject::get_by_project_id(conn, project.id)
            .await
            .map(|op| op.org_id);

        let owner = DenormalizedUser::denormalize(conn, project.owner_id, false)
            .await
            .expect("Owner should always exist");

        DenormalizedProject {
            id: project.id,
            name: project.name.clone(),
            slug: project
                .slug
                .clone()
                .unwrap_or_else(|| project.name.to_ascii_uppercase()),
            owner,
            created: project.created.and_utc(),
            description: project.description.clone(),
            repos,
            org_id,
            task_state_graph_id: project.task_state_graph_id,
            task_graph_id: project.task_graph_id,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectPayload {
    pub name: String,
    pub description: Option<String>,
    pub graph_id: Option<Uuid>,
    pub active: Option<bool>,
}

pub struct InnerProjectPayload {
    pub name: String,
    pub description: String,
    pub graph_id: Uuid,
    pub active: bool,
}

pub async fn list_projects(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    page_number: u32,
) -> Result<Vec<DenormalizedProject>, RejectReason> {
    let org_id = Organization::get_user_org(conn, user_id)
        .await
        .map(|org| org.id);
    let mut projects = vec![];
    for project in Project::list(conn, user_id, org_id, page_number, super::PAGE_SIZE).await {
        projects.push(DenormalizedProject::denormalize(conn, &project).await)
    }
    Ok(projects)
}

pub async fn list_org_projects(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    page_number: u32,
) -> Result<Vec<DenormalizedProject>, RejectReason> {
    let mut projects = Vec::new();
    if let Some(org) = Organization::get_user_org(conn, user_id).await {
        for project in org.projects(conn, page_number, super::PAGE_SIZE).await {
            projects.push(DenormalizedProject::denormalize(conn, &project).await);
        }
    }
    Ok(projects)
}

pub async fn get_project(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    project_id: ProjectId,
) -> Result<DenormalizedProject, RejectReason> {
    let project = Project::api_get(conn, user_id, project_id).await?;
    let project = DenormalizedProject::denormalize(conn, &project).await;
    Ok(project)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectChanged {
    pub project: DenormalizedProject,
    pub update: Option<UpdateProject>,
}

pub async fn update_project(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    project_id: ProjectId,
    payload: UpdateProject,
    tx: broadcast::Sender<ProjectChanged>,
) -> Result<DenormalizedProject, RejectReason> {
    let project = Project::api_get(conn, user_id, project_id).await?;
    let (project, changed) = project
        .update(conn, user_id, payload.clone())
        .await
        .map_err(RejectReason::database_error)?;
    let project = DenormalizedProject::denormalize(conn, &project).await;
    if changed {
        tracing::debug!("update_project: ProjectChanged({:?})", project);
        tx.send(ProjectChanged {
            project: project.clone(),
            update: Some(payload),
        })
        .ok();
    }
    Ok(project)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateImport {
    state: String,
}

pub async fn update_import(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    project_id: ProjectId,
    repo_id: Uuid,
    payload: UpdateImport,
    imports: mpsc::Sender<ImportRequest>,
) -> Result<ImportState, RejectReason> {
    let UpdateImport { state } = payload;
    let tracker = ProjectTracker::api_get(conn, repo_id).await?;
    let project = Project::api_get(conn, user_id, project_id).await?;
    let state = ImportState::from_database(&state);

    match ProjectImportState::api_by_project(conn, &project, &tracker).await {
        Ok(mut import_state) => {
            import_state
                .update_state(conn, &state, None)
                .await
                .map_err(RejectReason::database_error)?;
        }
        Err(_) => match state {
            ImportState::Pending => {
                imports
                    .send(ImportRequest {
                        requesting_user_id: user_id,
                        project,
                        project_tracker: tracker,
                    })
                    .await
                    .ok();
            }
            _ => {
                tracing::error!(
                    "No import state found for project {} and tracker {}",
                    project_id,
                    repo_id
                );
                return Err(RejectReason::not_found("No import state found"));
            }
        },
    };

    Ok(state)
}

pub async fn get_active_project(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
) -> Result<DenormalizedProject, RejectReason> {
    let project = match ActiveProject::api_get(conn, user_id).await {
        Ok(active_project) => Project::api_get(conn, user_id, active_project.project_id).await?,
        Err(_) => {
            // If there is no active project, look for the first project
            let org = Organization::get_user_org(conn, user_id).await;
            let projects = Project::list(conn, user_id, org.map(|o| o.id), 0, 1).await;
            let project = match projects.into_iter().next() {
                Some(project) => project,
                None => {
                    return Err(RejectReason::not_found("No active project found"));
                }
            };
            let (project, _) = project
                .update(conn, user_id, UpdateProject::SetActiveProject)
                .await
                .map_err(RejectReason::database_error)?;
            project
        }
    };
    let project = DenormalizedProject::denormalize(conn, &project).await;
    Ok(project)
}

pub async fn project_metadata(
    conn: &mut AsyncPgConnection,
    project_id: ProjectId,
) -> Result<ProjectMetadataProps, RejectReason> {
    let props = ProjectMetadata::get(conn, project_id)
        .await
        .ok_or_else(|| {
            RejectReason::not_found(format!("ProjectMetadata({}) not found", project_id))
        })?;
    Ok(props)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMetadata {
    pub tags: Vec<String>,
}

pub async fn update_metadata(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    project_id: ProjectId,
    payload: UpdateMetadata,
) -> Result<(), RejectReason> {
    Project::api_get(conn, user_id, project_id).await?;
    let props = project_metadata(conn, project_id).await?;
    let UpdateMetadata { tags } = payload;
    let org = Organization::get_user_org(conn, user_id)
        .await
        .ok_or_else(|| {
            RejectReason::not_found(format!(
                "Organization for update_metadata User({})",
                user_id
            ))
        })?;
    update_project_metadata(conn, org.id, project_id, props, tags)
        .await
        .map_err(RejectReason::database_error)
}

pub async fn add_project_to_org(
    conn: &mut AsyncPgConnection,
    project_id: ProjectId,
    user_id: UserId,
) -> Result<(), RejectReason> {
    let project = Project::api_get(conn, user_id, project_id).await?;
    if project.owner_id != user_id {
        tracing::warn!(
            "User({}) tried to add Project({}) they do not own to an organization",
            user_id,
            project_id
        );
        return Err(RejectReason::forbidden(
            user_id,
            "You are not the owner of this project",
        ));
    }
    let org = validate_org_capacity(conn, user_id).await?;
    org.add_project(conn, project.id)
        .await
        .map_err(RejectReason::database_error)?;
    Ok(())
}

pub async fn create_project<F, Fut>(
    conn: &mut AsyncPgConnection,
    gen_slug: F,
    payload: InnerProjectPayload,
    user_id: UserId,
    project_id: ProjectId,
    tx: broadcast::Sender<ProjectChanged>,
) -> Result<DenormalizedProject, RejectReason>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Option<String>>,
{
    let slug = match gen_slug().await {
        Some(slug) => slug,
        None => payload.name.clone(),
    };
    let InnerProjectPayload {
        name,
        description,
        graph_id,
        active,
    } = payload;

    let project = Project::create(
        conn,
        project_id,
        user_id,
        &name,
        &description,
        &slug,
        graph_id,
    )
    .await
    .map_err(RejectReason::database_error)?;
    PriorityLevel::insert_default_priorities(conn, project.id)
        .await
        .map_err(RejectReason::database_error)?;

    if active {
        project
            .set_active_project(conn, user_id)
            .await
            .map_err(RejectReason::database_error)?;
    }
    let project = DenormalizedProject::denormalize(conn, &project).await;
    tracing::debug!("create_project: ProjectChanged({:?})", project);
    tx.send(ProjectChanged {
        project: project.clone(),
        update: None,
    })
    .ok();
    Ok(project)
}
