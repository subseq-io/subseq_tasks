use std::str::FromStr;

use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
use diesel::prelude::*;
use diesel::{
    AsExpression,
    backend::Backend,
    serialize::{Output, ToSql},
    deserialize::{FromSql, FromSqlRow}
};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt;
use uuid::Uuid;
use zini_ownership::{Organization, OrganizationId, ProjectTracker};

subseq_util::uuid_type!(MilestoneId);

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DenormalizedMilestone {
    pub id: MilestoneId,
    pub milestone_type: MilestoneType,
    pub name: String,
    pub description: String,

    pub created: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_date: Option<DateTime<Utc>>,

    pub start_date: DateTime<Utc>,
    pub started: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_date: Option<DateTime<Utc>>,
    pub completed: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub repeat_interval: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repeat_end: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repeat_schema: Option<RepeatSchema>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_milestone: Option<MilestoneId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_milestone: Option<MilestoneId>,
}

impl DenormalizedMilestone {
    pub fn denormalize(milestone: Milestone) -> Self {
        Self {
            id: milestone.id,
            milestone_type: MilestoneType::from(milestone.milestone_type.as_str()),
            name: milestone.name,
            description: milestone.description,
            created: milestone.created.and_utc(),
            due_date: milestone.due_date.map(|d| d.and_utc()),
            start_date: milestone.start_date.and_utc(),
            started: milestone.started,
            completed_date: milestone.completed_date.map(|d| d.and_utc()),
            completed: milestone.completed,
            repeat_interval: milestone.repeat_interval.map(|d| d.num_seconds()),
            repeat_end: milestone.repeat_end.map(|d| d.and_utc()),
            repeat_schema: milestone
                .repeat_schema
                .as_ref()
                .map(|s| serde_json::from_value(s.clone()).unwrap()),
            next_milestone: milestone.next_milestone_id,
            previous_milestone: milestone.prev_milestone_id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
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

impl fmt::Display for DeadlineSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Commitment => write!(f, "commitment"),
            Self::Contract => write!(f, "contract"),
            Self::Deliverable => write!(f, "deliverable"),
            Self::Event => write!(f, "event"),
            Self::Legal => write!(f, "legal"),
            Self::Presentation => write!(f, "presentation"),
            Self::Production => write!(f, "production"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimelineSource {
    Month,
    Quarter,
    Sprint,
    Week,
    Year,
}

impl fmt::Display for TimelineSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Month => write!(f, "month"),
            Self::Quarter => write!(f, "quarter"),
            Self::Sprint => write!(f, "sprint"),
            Self::Week => write!(f, "week"),
            Self::Year => write!(f, "year"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
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

impl fmt::Display for MilestoneType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Checkpoint => write!(f, "checkpoint"),
            Self::Deadline(deadline) => write!(f, "deadline: {}", deadline),
            Self::Decision => write!(f, "decision"),
            Self::Epic => write!(f, "epic"),
            Self::Launch => write!(f, "launch"),
            Self::Review => write!(f, "review"),
            Self::Timeline(timeline) => write!(f, "timeline: {}", timeline),
            Self::Version => write!(f, "version"),
        }
    }
}

impl From<&str> for MilestoneType {
    fn from(other: &str) -> MilestoneType {
        match other {
            "checkpoint" => Self::Checkpoint,
            "deadline: commitment" => Self::Deadline(DeadlineSource::Commitment),
            "deadline: contract" => Self::Deadline(DeadlineSource::Contract),
            "deadline: deliverable" => Self::Deadline(DeadlineSource::Deliverable),
            "deadline: event" => Self::Deadline(DeadlineSource::Event),
            "deadline: legal" => Self::Deadline(DeadlineSource::Legal),
            "deadline: presentation" => Self::Deadline(DeadlineSource::Presentation),
            "deadline: production" => Self::Deadline(DeadlineSource::Production),
            "decision" => Self::Decision,
            "epic" => Self::Epic,
            "launch" => Self::Launch,
            "review" => Self::Review,
            "timeline: week" => Self::Timeline(TimelineSource::Week),
            "timeline: sprint" => Self::Timeline(TimelineSource::Sprint),
            "timeline: month" => Self::Timeline(TimelineSource::Month),
            "timeline: quarter" => Self::Timeline(TimelineSource::Quarter),
            "timeline: year" => Self::Timeline(TimelineSource::Year),
            "version" => Self::Version,
            _ => Self::Epic,
        }
    }
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum UpdateMilestone {
    Name(String),
    Type(MilestoneType),
    Description(String),
    DueDate(Option<DateTime<Utc>>),
    Start,
    Stop,
    Completed(bool),
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RepeatSchema {
    Increment(u32),
    Date(String),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RepeatSchemaType {
    Increment,
    Date,
}

#[derive(PartialEq, Queryable, QueryableByName, Insertable, Clone, Debug, Deserialize)]
#[diesel(table_name = crate::schema::milestones)]
pub struct Milestone {
    pub id: MilestoneId,
    pub org_id: OrganizationId,
    pub milestone_type: String,
    pub name: String,
    pub description: String,
    pub due_date: Option<NaiveDateTime>,
    pub created: NaiveDateTime,
    pub completed: bool,
    pub completed_date: Option<NaiveDateTime>,
    pub started: bool,
    pub start_date: NaiveDateTime,

    // New Nov 18 2024
    pub repeat_interval: Option<TimeDelta>,
    pub repeat_end: Option<NaiveDateTime>,
    pub repeat_schema: Option<Value>,
    pub next_milestone_id: Option<MilestoneId>,
    pub prev_milestone_id: Option<MilestoneId>,
}

impl Serialize for Milestone {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeStruct;
        let interval = self.repeat_interval.as_ref().map(|ri| ri.to_std().unwrap());

        let mut state = serializer.serialize_struct("Milestone", 14)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("orgId", &self.org_id)?;
        state.serialize_field("milestoneType", &self.milestone_type)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("description", &self.description)?;
        state.serialize_field("dueDate", &self.due_date)?;
        state.serialize_field("created", &self.created)?;
        state.serialize_field("completed", &self.completed)?;
        state.serialize_field("completedDate", &self.completed_date)?;
        state.serialize_field("started", &self.started)?;
        state.serialize_field("startDate", &self.start_date)?;
        state.serialize_field("repeatInterval", &interval)?;
        state.serialize_field("repeatEnd", &self.repeat_end)?;
        state.serialize_field("repeatSchema", &self.repeat_schema)?;
        state.serialize_field("nextMilestoneId", &self.next_milestone_id)?;
        state.serialize_field("prevMilestoneId", &self.prev_milestone_id)?;
        state.end()
    }
}

impl Milestone {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        milestone_id: MilestoneId,
        org: &Organization,
        milestone_type: MilestoneType,
        name: String,
        description: String,
        due_date: Option<NaiveDateTime>,
        start_date: Option<NaiveDateTime>,
        repeat_interval: Option<TimeDelta>,
        repeat_end: Option<NaiveDateTime>,
        repeat_schema: Option<RepeatSchema>,
        parent_milestone: Option<&mut Milestone>,
    ) -> QueryResult<Self> {
        use crate::schema::milestones::dsl as milestones;
        let milestone_type = milestone_type.to_string();
        let now = chrono::Utc::now().naive_utc();
        let start_date = start_date.unwrap_or(now);
        let started = start_date <= now;

        let milestone = Milestone {
            id: milestone_id,
            org_id: org.id,
            milestone_type,
            name,
            description,
            due_date,
            created: now,
            completed: false,
            completed_date: None,
            started,
            start_date,
            repeat_interval,
            repeat_end,
            repeat_schema: repeat_schema.map(|s| serde_json::to_value(s).unwrap()),
            next_milestone_id: None,
            prev_milestone_id: parent_milestone.map(|m| m.id),
        };
        diesel::insert_into(milestones::milestones)
            .values(&milestone)
            .execute(conn)
            .await?;
        Ok(milestone)
    }

    pub async fn delete(self, conn: &mut AsyncPgConnection) -> QueryResult<()> {
        use crate::schema::milestones::dsl as milestones;
        diesel::delete(milestones::milestones.find(self.id))
            .execute(conn)
            .await?;
        Ok(())
    }

    pub async fn search(
        conn: &mut AsyncPgConnection,
        org_id: OrganizationId,
        query: Option<&str>,
        completed: Option<bool>,
        due: Option<NaiveDateTime>,
        page: u32,
        page_size: u32,
    ) -> QueryResult<Vec<Self>> {
        use diesel::sql_types::{Bool, Integer, Nullable, Text, Timestamp, Uuid as DieselUuid};

        let offset = page.saturating_sub(1) * page_size;
        let sql_query = r#"
            SELECT m.* FROM milestones m
            WHERE m.org_id = $1
            AND ($2 IS NULL OR to_tsvector('english', m.name || ' ' || m.description) @@ plainto_tsquery('english', $2))
            AND ($3 IS NULL OR m.completed = $3)
            AND ($4 IS NULL OR m.due_date >= $4)
            OFFSET $5
            LIMIT $6
        "#;

        diesel::sql_query(sql_query)
            .bind::<DieselUuid, _>(org_id)
            .bind::<Nullable<Text>, _>(query)
            .bind::<Nullable<Bool>, _>(completed)
            .bind::<Nullable<Timestamp>, _>(due)
            .bind::<Integer, _>(offset as i32)
            .bind::<Integer, _>(page_size as i32)
            .get_results::<Self>(conn)
            .await
    }

    pub async fn previous_update(&self, update: &UpdateMilestone) -> Option<UpdateMilestone> {
        match update {
            UpdateMilestone::Name(_) => {
                let previous_name = self.name.clone();
                Some(UpdateMilestone::Name(previous_name))
            }
            UpdateMilestone::Type(_) => {
                let previous_type = MilestoneType::from(self.milestone_type.as_str());
                Some(UpdateMilestone::Type(previous_type))
            }
            UpdateMilestone::Description(_) => {
                let previous_description = self.description.clone();
                Some(UpdateMilestone::Description(previous_description))
            }
            UpdateMilestone::DueDate(_) => {
                let previous_due_date = self
                    .due_date
                    .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc));
                Some(UpdateMilestone::DueDate(previous_due_date))
            }
            UpdateMilestone::Start => Some(UpdateMilestone::Stop),
            UpdateMilestone::Stop => Some(UpdateMilestone::Start),
            UpdateMilestone::Completed(_) => {
                let previous_completed = self.completed;
                Some(UpdateMilestone::Completed(previous_completed))
            }
        }
    }

    pub async fn update(
        &mut self,
        conn: &mut AsyncPgConnection,
        update: UpdateMilestone,
    ) -> QueryResult<()> {
        use crate::schema::milestones::dsl as milestones;
        match update {
            UpdateMilestone::Name(name) => {
                self.name = name;
                diesel::update(milestones::milestones.find(self.id))
                    .set(milestones::name.eq(&self.name))
                    .execute(conn)
                    .await?;
            }
            UpdateMilestone::Type(milestone_type) => {
                self.milestone_type = milestone_type.to_string();
                diesel::update(milestones::milestones.find(self.id))
                    .set(milestones::milestone_type.eq(&self.milestone_type))
                    .execute(conn)
                    .await?;
            }
            UpdateMilestone::Description(description) => {
                self.description = description;
                diesel::update(milestones::milestones.find(self.id))
                    .set(milestones::description.eq(&self.description))
                    .execute(conn)
                    .await?;
            }
            UpdateMilestone::DueDate(due_date) => {
                self.due_date = due_date.map(|dt| dt.naive_utc());
                diesel::update(milestones::milestones.find(self.id))
                    .set(milestones::due_date.eq(&self.due_date))
                    .execute(conn)
                    .await?;
            }
            UpdateMilestone::Start => {
                self.started = true;
                self.start_date = chrono::Utc::now().naive_utc();
                diesel::update(milestones::milestones.find(self.id))
                    .set((
                        milestones::started.eq(&self.started),
                        milestones::start_date.eq(&self.start_date),
                    ))
                    .execute(conn)
                    .await?;
            }
            UpdateMilestone::Stop => {
                self.started = false;
                diesel::update(milestones::milestones.find(self.id))
                    .set(milestones::started.eq(&self.started))
                    .execute(conn)
                    .await?;
            }
            UpdateMilestone::Completed(completed) => {
                self.completed = completed;
                if completed {
                    self.completed_date = Some(chrono::Utc::now().naive_utc());
                } else {
                    self.completed_date = None;
                }
                diesel::update(milestones::milestones.find(self.id))
                    .set((
                        milestones::completed.eq(&self.completed),
                        milestones::completed_date.eq(&self.completed_date),
                    ))
                    .execute(conn)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn get(conn: &mut AsyncPgConnection, milestone_id: MilestoneId) -> Option<Self> {
        use crate::schema::milestones::dsl as milestones;
        milestones::milestones
            .find(milestone_id)
            .get_result(conn)
            .await
            .optional()
            .ok()?
    }
}

#[derive(PartialEq, Queryable, Insertable, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::releases)]
pub struct Release {
    id: Uuid,
    tracker_id: Uuid,
    previous_release_id: Option<Uuid>,
    milestone_id: MilestoneId,
    name: String,
    released_date: Option<NaiveDateTime>,
    planned_date: Option<NaiveDateTime>,
    metadata: Value,
}

impl Release {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        tracker: ProjectTracker,
        previous_release: Option<&Release>,
        milestone: &Milestone,
        name: &str,
        planned_date: Option<NaiveDateTime>,
    ) -> QueryResult<Self> {
        let release = Self {
            id: Uuid::new_v4(),
            tracker_id: tracker.id,
            previous_release_id: previous_release.map(|r| r.id),
            milestone_id: milestone.id,
            name: name.to_string(),
            released_date: None,
            planned_date,
            metadata: json!({}),
        };
        diesel::insert_into(crate::schema::releases::table)
            .values(&release)
            .execute(conn)
            .await?;
        Ok(release)
    }
}

#[derive(PartialEq, Queryable, Insertable, Clone, Debug, Serialize)]
#[diesel(table_name = crate::schema::deployments)]
pub struct Deployment {
    id: Uuid,
    release_id: Uuid,
    environment: String,
    deployed_at: NaiveDateTime,
    metadata: Value,
}
