use std::collections::HashSet;

use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{
    de::{Deserialize as DeserializeTrait, Error as DeserializeError},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;

use crate::tables::{update_org_components, OrganizationId, ProjectId};
use crate::util::extend_as_set;

#[derive(Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProjectMetadataPropsV1 {
    pub tags: HashSet<String>,
    pub components: HashSet<String>,
}

impl ProjectMetadataPropsV1 {
    pub const VERSION: i32 = 1;
}

#[derive(Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProjectMetadataPropsV2 {
    pub tags: HashSet<String>,
}

impl ProjectMetadataPropsV2 {
    pub const VERSION: i32 = 2;
}

pub enum ProjectMetadataProps {
    V1(ProjectMetadataPropsV1),
    V2(ProjectMetadataPropsV2),
}

impl Default for ProjectMetadataProps {
    fn default() -> Self {
        ProjectMetadataProps::V2(ProjectMetadataPropsV2::default())
    }
}

impl Serialize for ProjectMetadataProps {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match self {
            ProjectMetadataProps::V1(props) => props.serialize(serializer),
            ProjectMetadataProps::V2(props) => props.serialize(serializer),
        }
    }
}

impl<'de> DeserializeTrait<'de> for ProjectMetadataProps {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let v2 = serde_json::from_value::<ProjectMetadataPropsV2>(value.clone());
        if let Ok(props) = v2 {
            return Ok(ProjectMetadataProps::V2(props));
        }
        let v1 = serde_json::from_value::<ProjectMetadataPropsV1>(value);
        if let Ok(props) = v1 {
            return Ok(ProjectMetadataProps::V1(props));
        }
        Err(D::Error::custom("Invalid metadata"))
    }
}

pub async fn update_project_metadata(
    conn: &mut AsyncPgConnection,
    org_id: OrganizationId,
    project_id: ProjectId,
    props: ProjectMetadataProps,
    tags: Vec<String>,
) -> QueryResult<()> {
    let metadata = match props {
        ProjectMetadataProps::V1(metadata) => {
            update_org_components(conn, org_id, metadata.components).await?;
            ProjectMetadataPropsV2 {
                tags: extend_as_set(metadata.tags, tags),
            }
        }
        ProjectMetadataProps::V2(mut metadata) => {
            metadata.tags = extend_as_set(metadata.tags, tags);
            metadata
        }
    };

    ProjectMetadata::create(conn, project_id, metadata).await?;
    Ok(())
}

#[derive(Queryable, Insertable, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::project_metadata)]
pub struct ProjectMetadata {
    pub project_id: ProjectId,
    pub metadata: Value,
    pub created: NaiveDateTime,
    pub updated: NaiveDateTime,
    pub metadata_version: i32,
}

impl ProjectMetadata {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
        metadata: ProjectMetadataPropsV2,
    ) -> QueryResult<()> {
        let now = chrono::Utc::now().naive_utc();
        let metadata = serde_json::to_value(&metadata).expect("Valid JSON");
        let meta = Self {
            project_id,
            metadata,
            created: now,
            updated: now,
            metadata_version: ProjectMetadataPropsV2::VERSION,
        };

        #[derive(AsChangeset)]
        #[diesel(table_name = crate::schema::project_metadata)]
        struct Upsert {
            metadata: Value,
            updated: NaiveDateTime,
            metadata_version: i32,
        }

        diesel::insert_into(crate::schema::project_metadata::table)
            .values(&meta)
            .on_conflict(crate::schema::project_metadata::dsl::project_id)
            .do_update()
            .set(Upsert {
                metadata: meta.metadata.clone(),
                updated: now,
                metadata_version: meta.metadata_version,
            })
            .execute(conn)
            .await?;
        Ok(())
    }

    pub async fn get(
        conn: &mut AsyncPgConnection,
        project_id: ProjectId,
    ) -> Option<ProjectMetadataProps> {
        use crate::schema::project_metadata::table;
        let metadata = table
            .find(project_id)
            .get_result::<Self>(conn)
            .await
            .optional()
            .ok()
            .flatten()?;
        match metadata.metadata_version {
            ProjectMetadataPropsV1::VERSION => {
                let props = serde_json::from_value(metadata.metadata).ok()?;
                Some(ProjectMetadataProps::V1(props))
            }
            ProjectMetadataPropsV2::VERSION => {
                let props = serde_json::from_value(metadata.metadata).ok()?;
                Some(ProjectMetadataProps::V2(props))
            }
            _ => None,
        }
    }

    pub async fn delete(&self, conn: &mut AsyncPgConnection) -> QueryResult<()> {
        use crate::schema::project_metadata::dsl;
        diesel::delete(dsl::project_metadata.find(self.project_id))
            .execute(conn)
            .await?;
        Ok(())
    }
}
