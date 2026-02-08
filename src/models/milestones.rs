use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
use diesel_async::AsyncPgConnection;
use serde::{Deserialize, Serialize};
use subseq_util::{api::RejectReason, UserId};
use tokio::sync::broadcast;
use uuid::Uuid;
use zini_ownership::{organizations::get_org_for_planner, Organization};

use crate::models::tools::HumanTimeString;
use crate::tables::{
    DenormalizedMilestone, Milestone, MilestoneId, MilestoneType, RepeatSchema, UpdateMilestone,
};

pub(in crate::models) mod features {
    use crate::models::onboarding::DescriptionStep;
    pub(in crate::models) const FEATURE_CREATE_MILESTONE: DescriptionStep =
        DescriptionStep::new("Create a milestone for the team project. You can access this from the projects page just above the list of Team projects. Milestones are key points in the project timeline.");
    pub(in crate::models) const FEATURE_PLAN_MILESTONE: DescriptionStep =
        DescriptionStep::new("The new milestone will be visible on the planning page as a background color when it has a due date set.");
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepeatData {
    pub repeats: HumanTimeString,
    pub repeat_end: Option<DateTime<Utc>>,
    pub repeat_schema: RepeatSchema,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NewMilestone {
    pub name: String,
    pub description: String,
    pub milestone_type: MilestoneType,
    pub due_date: Option<DateTime<Utc>>,
    pub start_date: Option<DateTime<Utc>>,
    pub repeat: Option<RepeatData>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchQuery {
    pub query: Option<String>,
    pub completed: Option<bool>,
    pub due: Option<DateTime<Utc>>,
    pub page: Option<u32>,
    pub limit: Option<u32>,
}

#[derive(Clone)]
pub struct MilestoneChanged {
    pub milestone: DenormalizedMilestone,
}

pub async fn create_milestone(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    new_milestone: NewMilestone,
    tx: broadcast::Sender<MilestoneChanged>,
) -> Result<DenormalizedMilestone, RejectReason> {
    let org = get_org_for_planner(conn, user_id).await?;
    let (repeat_interval, repeat_end, repeat_schema): (
        Option<TimeDelta>,
        Option<NaiveDateTime>,
        Option<RepeatSchema>,
    ) = match new_milestone.repeat {
        Some(repeats) => {
            let RepeatData {
                repeats,
                repeat_end,
                repeat_schema,
            } = repeats;

            let repeat_interval = match repeats.as_duration() {
                Ok(duration) => TimeDelta::from_std(duration).unwrap(),
                Err(e) => {
                    return Err(RejectReason::bad_request(format!(
                        "Invalid duration: {}",
                        e
                    )))
                }
            };
            let repeat_end = repeat_end.map(|d| d.naive_utc());
            (Some(repeat_interval), repeat_end, Some(repeat_schema))
        }
        None => (None, None, None),
    };
    let milestone_id = MilestoneId(Uuid::new_v4());

    let milestone = Milestone::create(
        conn,
        milestone_id,
        &org,
        new_milestone.milestone_type,
        new_milestone.name,
        new_milestone.description,
        new_milestone.due_date.map(|d| d.naive_utc()),
        new_milestone.start_date.map(|d| d.naive_utc()),
        repeat_interval,
        repeat_end,
        repeat_schema,
        None,
    )
    .await
    .map_err(RejectReason::database_error)?;

    let milestone = DenormalizedMilestone::denormalize(milestone);
    tx.send(MilestoneChanged {
        milestone: milestone.clone(),
    })
    .ok();
    Ok(milestone)
}

pub async fn delete_milestone(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    milestone_id: MilestoneId,
) -> Result<(), RejectReason> {
    let org = get_org_for_planner(conn, user_id).await?;
    let milestone = Milestone::get(conn, milestone_id)
        .await
        .ok_or_else(|| RejectReason::not_found(format!("Milestone {}", milestone_id)))?;
    if org.id != milestone.org_id {
        return Err(RejectReason::forbidden(
            user_id,
            format!(
                "{:?} does not have permission to delete this milestone",
                user_id
            ),
        ));
    }
    milestone
        .delete(conn)
        .await
        .map_err(RejectReason::database_error)
}

pub async fn search_milestones(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    query: &SearchQuery,
) -> Result<Vec<DenormalizedMilestone>, RejectReason> {
    let org = Organization::get_user_org(conn, user_id)
        .await
        .ok_or_else(|| RejectReason::not_found(format!("Organization for {:?}", user_id)))?;
    let mut milestones = vec![];

    let SearchQuery {
        query,
        completed,
        due,
        page,
        limit,
    } = query;
    let page = page.unwrap_or(1);
    let page_size = limit.unwrap_or(super::PAGE_SIZE);
    let query = query.as_ref().map(|s| s.as_str());
    let due = due.map(|d| d.naive_utc());

    for milestone in Milestone::search(conn, org.id, query, *completed, due, page, page_size)
        .await
        .map_err(RejectReason::database_error)?
    {
        milestones.push(DenormalizedMilestone::denormalize(milestone));
    }
    Ok(milestones)
}

pub async fn get_milestone(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    milestone_id: MilestoneId,
) -> Result<DenormalizedMilestone, RejectReason> {
    let org = Organization::get_user_org(conn, user_id)
        .await
        .ok_or_else(|| RejectReason::not_found(format!("Organization for {:?}", user_id)))?;
    let milestone = Milestone::get(conn, milestone_id)
        .await
        .ok_or_else(|| RejectReason::not_found(format!("Milestone {}", milestone_id)))?;
    if org.id != milestone.org_id {
        return Err(RejectReason::forbidden(
            user_id,
            format!(
                "{:?} does not have permission to view this milestone",
                user_id
            ),
        ));
    }
    Ok(DenormalizedMilestone::denormalize(milestone))
}

pub async fn update_milestone(
    conn: &mut AsyncPgConnection,
    user_id: UserId,
    milestone_id: MilestoneId,
    payload: UpdateMilestone,
    tx: broadcast::Sender<MilestoneChanged>,
) -> Result<(), RejectReason> {
    let org = get_org_for_planner(conn, user_id).await?;
    let mut milestone = Milestone::get(conn, milestone_id)
        .await
        .ok_or_else(|| RejectReason::not_found(format!("Milestone {}", milestone_id)))?;
    if org.id != milestone.org_id {
        return Err(RejectReason::forbidden(
            user_id,
            format!(
                "{:?} does not have permission to update this milestone",
                user_id
            ),
        ));
    }
    milestone
        .update(conn, payload)
        .await
        .map_err(RejectReason::database_error)?;
    tx.send(MilestoneChanged {
        milestone: DenormalizedMilestone::denormalize(milestone),
    })
    .ok();
    Ok(())
}

pub fn export_milestone(milestone: DenormalizedMilestone) -> Result<String, RejectReason> {
    serde_yml::to_string(&milestone).map_err(|_| {
        RejectReason::anyhow(anyhow::anyhow!("Failed to serialize milestone".to_string()))
    })
}
