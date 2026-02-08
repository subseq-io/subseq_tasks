use std::{boxed::Box, io, pin::Pin, sync::Arc};

use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::Future;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use subseq_util::UserId;
use subseq_util::{
    api::RejectReason,
    tables::{DbPool, ValidationErrorMessage},
};
use uuid::Uuid;

use super::{Organization, OrganizationId, ProjectId};

#[derive(Queryable, Insertable, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::projects)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub owner_id: UserId,
    pub created: NaiveDateTime,
    pub description: String,
    pub task_state_graph_id: Uuid,
    pub deleted: bool,
    pub slug: Option<String>,
    pub task_graph_id: Option<Uuid>,
}

impl PartialEq for Project {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.owner_id == other.owner_id
            && self.created.and_utc().timestamp_micros()
                == other.created.and_utc().timestamp_micros()
            && self.description == other.description
            && self.task_state_graph_id == other.task_state_graph_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UpdateProject {
    Name(String),
    Description(String),
    #[serde(rename_all = "camelCase")]
    TaskStateGraph {
        graph_id: Uuid,
    },
    #[serde(rename_all = "camelCase")]
    TaskGraph {
        graph_id: Uuid,
    },
    #[serde(rename_all = "camelCase")]
    AddRepo {
        tracker: ProjectTrackerType,
        repo: String,
        repo_url: Option<String>,
        is_default: bool,
    },
    #[serde(rename_all = "camelCase")]
    UpdateRepo {
        repo_id: Uuid,
        update: ProjectTrackerUpdate,
    },
    #[serde(rename_all = "camelCase")]
    DeleteRepo {
        repo_id: Uuid,
    },
    SetActiveProject,
    UnsetActiveProject,
}

pub async fn is_member(
    conn: &mut AsyncPgConnection,
    project_id: ProjectId,
    user_id: UserId,
) -> bool {
    use crate::schema::organization_memberships::dsl as org_memberships;
    use crate::schema::organization_projects::dsl as org_projects;

    org_projects::organization_projects
        .filter(org_projects::project_id.eq(project_id))
        .inner_join(
            org_memberships::organization_memberships
                .on(org_projects::org_id.eq(org_memberships::org_id)),
        )
        .filter(org_memberships::user_id.eq(user_id))
        .select(org_memberships::user_id)
        .get_result::<UserId>(conn)
        .await
        .is_ok()
}

pub trait ConnectedProject {
    fn id(&self) -> ProjectId;
    fn owner_id(&self) -> UserId;
    fn delete(
        &self,
        pool: Arc<DbPool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), RejectReason>> + Send>>;
}

impl Project {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        id: ProjectId,
        author_id: UserId,
        name: &str,
        description: &str,
        slug: &str,
        graph_id: Uuid,
    ) -> QueryResult<Self> {
        let project = Self {
            id,
            name: name.to_string(),
            owner_id: author_id,
            created: chrono::Utc::now().naive_utc(),
            description: description.to_owned(),
            task_state_graph_id: graph_id,
            deleted: false,
            slug: Some(slug.to_ascii_uppercase()),
            task_graph_id: None,
        };

        if project.name.len() > 64 {
            let kind = diesel::result::DatabaseErrorKind::CheckViolation;
            let msg = Box::new(ValidationErrorMessage {
                message: "Invalid project name".to_string(),
                column: "name".to_string(),
                constraint_name: "name_limits".to_string(),
            });
            return Err(diesel::result::Error::DatabaseError(kind, msg));
        }

        diesel::insert_into(crate::schema::projects::table)
            .values(&project)
            .execute(conn)
            .await?;
        Ok(project)
    }

    pub async fn get_by_graph_id(conn: &mut AsyncPgConnection, graph_id: Uuid) -> Vec<Self> {
        use crate::schema::projects::dsl as projects;
        projects::projects
            .filter(projects::task_state_graph_id.eq(graph_id))
            .load::<Self>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn get_by_task_graph_id(conn: &mut AsyncPgConnection, graph_id: Uuid) -> Vec<Self> {
        use crate::schema::projects::dsl as projects;
        projects::projects
            .filter(projects::task_graph_id.eq(graph_id))
            .load::<Self>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn active(conn: &mut AsyncPgConnection, user_id: UserId) -> Option<Self> {
        use crate::schema::active_projects::dsl as active_projects;
        use crate::schema::projects::dsl as projects;
        active_projects::active_projects
            .filter(active_projects::user_id.eq(user_id))
            .inner_join(projects::projects)
            .select(projects::projects::all_columns())
            .get_result::<Self>(conn)
            .await
            .optional()
            .ok()?
    }

    pub async fn delete(
        pool: Arc<DbPool>,
        project_id: ProjectId,
        connected_project: Box<dyn ConnectedProject + Send>,
    ) -> QueryResult<()> {
        if let Err(e) = connected_project.delete(pool.clone()).await {
            tracing::error!(
                "Error deleting ConnectedProject on Project deletion: {:?}",
                e
            );
        }
        let mut conn = pool.get().await.expect("Failed to get db connection");

        use crate::schema::projects::dsl as projects;
        diesel::update(projects::projects.filter(projects::id.eq(project_id)))
            .set(projects::deleted.eq(true))
            .execute(&mut conn)
            .await?;

        for user_id in Self::user_ids_with_this_active_project(&mut conn, project_id).await {
            Self::unset_active_project(&mut conn, user_id).await?;
        }

        Ok(())
    }

    pub async fn is_member(&self, conn: &mut AsyncPgConnection, user_id: UserId) -> bool {
        user_id == self.owner_id || is_member(conn, self.id, user_id).await
    }

    pub async fn all_by_id(conn: &mut AsyncPgConnection, project_ids: &[ProjectId]) -> Vec<Self> {
        use crate::schema::projects::dsl;
        dsl::projects
            .filter(dsl::id.eq_any(project_ids))
            .load::<Self>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn update(
        mut self,
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        update: UpdateProject,
    ) -> QueryResult<(Self, bool)> {
        use crate::schema::projects::dsl;
        let mut changed = false;
        match update {
            UpdateProject::UnsetActiveProject => {
                Self::unset_active_project(conn, user_id).await?;
            }
            UpdateProject::SetActiveProject => {
                self.set_active_project(conn, user_id).await?;
            }
            UpdateProject::Name(name) => {
                if name != self.name {
                    diesel::update(dsl::projects.filter(dsl::id.eq(self.id)))
                        .set(dsl::name.eq(&name))
                        .execute(conn)
                        .await?;
                    self.name = name;
                    changed = true;
                }
            }
            UpdateProject::Description(desc) => {
                if desc != self.description {
                    diesel::update(dsl::projects.filter(dsl::id.eq(self.id)))
                        .set(dsl::description.eq(&desc))
                        .execute(conn)
                        .await?;
                    self.description = desc;
                    changed = true;
                }
            }
            UpdateProject::TaskStateGraph { graph_id } => {
                if graph_id != self.task_state_graph_id {
                    diesel::update(dsl::projects.filter(dsl::id.eq(self.id)))
                        .set(dsl::task_state_graph_id.eq(graph_id))
                        .execute(conn)
                        .await?;
                    self.task_state_graph_id = graph_id;
                    changed = true;
                }
            }
            UpdateProject::TaskGraph { graph_id } => {
                if Some(graph_id) != self.task_graph_id {
                    diesel::update(dsl::projects.filter(dsl::id.eq(self.id)))
                        .set(dsl::task_graph_id.eq(graph_id))
                        .execute(conn)
                        .await?;
                    self.task_graph_id = Some(graph_id);
                    changed = true;
                }
            }
            UpdateProject::AddRepo {
                tracker,
                repo,
                repo_url,
                is_default,
            } => {
                ProjectTracker::create(conn, self.id, tracker, &repo, repo_url, is_default, None)
                    .await?;
            }
            UpdateProject::UpdateRepo { repo_id, update } => {
                let repo = ProjectTracker::get(conn, repo_id).await?;
                repo.update(conn, update).await?;
            }
            UpdateProject::DeleteRepo { repo_id } => {
                let repo = ProjectTracker::get(conn, repo_id).await?;
                repo.delete(conn).await?;
            }
        }
        Ok((self, changed))
    }

    pub async fn set_active_project(
        &self,
        conn: &mut AsyncPgConnection,
        user_id_: UserId,
    ) -> QueryResult<()> {
        use crate::schema::active_projects::dsl::*;
        diesel::insert_into(active_projects)
            .values(&ActiveProject {
                user_id: user_id_,
                project_id: self.id,
            })
            .on_conflict(user_id)
            .do_update()
            .set(project_id.eq(self.id))
            .execute(conn)
            .await?;
        Ok(())
    }

    pub async fn unset_active_project(
        conn: &mut AsyncPgConnection,
        user_id_: UserId,
    ) -> QueryResult<()> {
        use crate::schema::active_projects::dsl::*;
        diesel::delete(active_projects.filter(user_id.eq(user_id_)))
            .execute(conn)
            .await?;
        Ok(())
    }

    pub async fn user_ids_with_this_active_project(
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
    ) -> Vec<UserId> {
        use crate::schema::active_projects::dsl;
        dsl::active_projects
            .filter(dsl::project_id.eq(project_id))
            .select(dsl::user_id)
            .load::<UserId>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn get_priority_levels(&self, conn: &mut AsyncPgConnection) -> Vec<PriorityLevel> {
        use crate::schema::priority_levels::dsl;
        dsl::priority_levels
            .filter(dsl::project_id.eq(self.id))
            .load::<PriorityLevel>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn list(
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        org_id: Option<OrganizationId>,
        page: u32,
        page_size: u32,
    ) -> Vec<Self> {
        use crate::schema::organization_projects::dsl as org_projects;
        use crate::schema::projects::dsl::{deleted, id, owner_id};
        use crate::schema::projects::table;
        let offset = page.saturating_sub(1) * page_size;

        if let Some(org_id) = org_id {
            use diesel::dsl::not;
            let subquery = org_projects::organization_projects
                .filter(org_projects::org_id.eq(org_id))
                .select(org_projects::project_id);

            table
                .limit(page_size as i64)
                .offset(offset as i64)
                .filter(deleted.eq(false))
                .filter(owner_id.eq(user_id))
                .filter(not(id.eq_any(subquery)))
                .load::<Self>(conn)
                .await
                .unwrap_or_default()
        } else {
            table
                .limit(page_size as i64)
                .offset(offset as i64)
                .filter(deleted.eq(false))
                .filter(owner_id.eq(user_id))
                .load::<Self>(conn)
                .await
                .unwrap_or_default()
        }
    }

    pub async fn get(conn: &mut AsyncPgConnection, project_id: ProjectId) -> Option<Self> {
        use crate::schema::projects::table;
        table
            .find(project_id)
            .get_result::<Self>(conn)
            .await
            .optional()
            .ok()?
    }

    pub async fn get_secure(
        conn: &mut AsyncPgConnection,
        user_id: UserId,
        project_id: ProjectId,
    ) -> Option<Self> {
        let project_ids = Organization::user_project_ids(conn, user_id).await;
        if project_ids.contains(&project_id) {
            Project::get(conn, project_id).await
        } else {
            None
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::active_projects)]
pub struct ActiveProject {
    pub user_id: UserId,
    pub project_id: ProjectId,
}

impl ActiveProject {
    pub async fn get(conn: &mut AsyncPgConnection, user_id: UserId) -> Option<Self> {
        use crate::schema::active_projects::dsl;
        dsl::active_projects
            .filter(dsl::user_id.eq(user_id))
            .get_result::<Self>(conn)
            .await
            .optional()
            .ok()?
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ProjectTrackerType {
    Github,
    Jira,
    Linear,
    Other,
    Zini,
}

impl std::default::Default for ProjectTrackerType {
    fn default() -> Self {
        Self::Other
    }
}

impl std::str::FromStr for ProjectTrackerType {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "zini" => Ok(Self::Zini),
            "github" => Ok(Self::Github),
            "linear" => Ok(Self::Linear),
            "jira" => Ok(Self::Jira),
            _ => Ok(Self::Other),
        }
    }
}

impl ProjectTrackerType {
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Zini => "zini",
            Self::Github => "github",
            Self::Linear => "linear",
            Self::Jira => "jira",
            Self::Other => "other",
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::project_trackers)]
#[serde(rename_all = "camelCase")]
pub struct ProjectTracker {
    pub id: Uuid,
    pub project_id: ProjectId,
    pub tracker: String,
    pub repo: String,
    pub created: NaiveDateTime,
    pub repo_url: Option<String>,
    pub is_default: bool,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProjectTrackerUpdate {
    Tracker(ProjectTrackerType),
    Repo(String),
    RepoUrl(String),
    IsDefault(bool),
}

impl ProjectTracker {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
        tracker: ProjectTrackerType,
        repo: &str,
        repo_url: Option<String>,
        is_default: bool,
        metadata: Option<Value>,
    ) -> QueryResult<Self> {
        let project = Self {
            id: Uuid::new_v4(),
            project_id,
            tracker: tracker.to_str().to_string(),
            repo: repo.to_string(),
            created: chrono::Utc::now().naive_utc(),
            repo_url,
            is_default,
            metadata,
        };

        diesel::insert_into(crate::schema::project_trackers::table)
            .values(&project)
            .execute(conn)
            .await?;
        Ok(project)
    }

    pub async fn get(conn: &mut AsyncPgConnection, repo_id: Uuid) -> QueryResult<Self> {
        use crate::schema::project_trackers::table;
        table.find(repo_id).get_result::<Self>(conn).await
    }

    pub async fn by_project(conn: &mut AsyncPgConnection, project_id: ProjectId) -> Vec<Self> {
        use crate::schema::project_trackers::dsl;
        dsl::project_trackers
            .filter(dsl::project_id.eq(project_id))
            .load::<Self>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn by_github_repo_name(conn: &mut AsyncPgConnection, repo: &str) -> Vec<Self> {
        use crate::schema::project_trackers::dsl;
        dsl::project_trackers
            .filter(dsl::repo.eq(repo))
            .filter(dsl::tracker.eq(ProjectTrackerType::Github.to_str()))
            .load::<Self>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn update(
        &self,
        conn: &mut AsyncPgConnection,
        update: ProjectTrackerUpdate,
    ) -> QueryResult<Self> {
        use crate::schema::project_trackers::dsl;

        let up = diesel::update(dsl::project_trackers.find(self.id));
        match update {
            ProjectTrackerUpdate::Tracker(tracker) => {
                up.set(dsl::tracker.eq(tracker.to_str()))
                    .execute(conn)
                    .await?;
            }
            ProjectTrackerUpdate::Repo(repo) => {
                up.set(dsl::repo.eq(repo)).execute(conn).await?;
            }
            ProjectTrackerUpdate::RepoUrl(repo_url) => {
                up.set(dsl::repo_url.eq(repo_url)).execute(conn).await?;
            }
            ProjectTrackerUpdate::IsDefault(is_default) => {
                up.set(dsl::is_default.eq(is_default)).execute(conn).await?;
            }
        }
        Ok(self.clone())
    }

    pub async fn delete(&self, conn: &mut AsyncPgConnection) -> QueryResult<()> {
        use crate::schema::project_trackers::dsl;
        diesel::delete(dsl::project_trackers.find(self.id))
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::priority_levels)]
pub struct PriorityLevel {
    pub project_id: ProjectId,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    pub value: i32,
}

impl PriorityLevel {
    pub async fn priorities(
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
    ) -> Vec<(String, i32)> {
        use crate::schema::priority_levels::dsl;
        dsl::priority_levels
            .filter(dsl::project_id.eq(project_id))
            .order_by(dsl::value.asc())
            .select((dsl::name, dsl::value))
            .load::<(String, i32)>(conn)
            .await
            .unwrap_or_default()
    }

    pub async fn insert_default_priorities(
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
    ) -> QueryResult<()> {
        let levels = vec![
            ("Low", 20),
            ("Medium", 15),
            ("Standard", 10),
            ("Elevated", 5),
            ("High", 0),
            ("Critical", -5),
            ("Urgent", -10),
            ("Immediately", -15),
        ];
        for (name, value) in levels {
            let level = Self {
                project_id,
                name: name.to_string(),
                icon: None,
                value,
            };
            diesel::insert_into(crate::schema::priority_levels::table)
                .values(&level)
                .execute(conn)
                .await
                .ok();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tables::test::{basic_setup, MIGRATIONS};
    use function_name::named;
    use subseq_util::tables::harness::{to_pg_db_name, DbHarness};

    #[tokio::test]
    #[named]
    async fn test_proj_handle() {
        let db_name = to_pg_db_name(function_name!());
        let harness = DbHarness::new("localhost", "development", &db_name, Some(MIGRATIONS)).await;
        let mut conn = harness.conn().await;
        let (user, _org) = basic_setup(&mut conn).await;
        let graph_id = Uuid::new_v4();
        let project_id = ProjectId(Uuid::new_v4());

        let proj = Project::create(
            &mut conn,
            project_id,
            user.id,
            "test_proj",
            "This is a test",
            "TEST_PROJ",
            graph_id,
        )
        .await
        .expect("proj");
        let proj2 = Project::get(&mut conn, proj.id).await.expect("proj2");
        assert_eq!(proj, proj2);
    }
}
