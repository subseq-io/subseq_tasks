use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel::result::QueryResult;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::Serialize;
use subseq_util::api::RejectReason;
use subseq_util::UserId;
use uuid::Uuid;

use crate::tables::{DenormalizedTask, OrderBy, TaskId};

#[derive(Queryable, Insertable, Debug, Serialize)]
#[diesel(table_name = crate::schema::task_list_urls)]
pub struct TaskListUrl {
    pub id: Uuid,
    pub user_id: UserId,
    pub created_at: NaiveDateTime,
    pub url: String,
    pub task_ids: Vec<TaskId>,
}

impl TaskListUrl {
    pub async fn create(
        conn: &mut AsyncPgConnection,
        id: Uuid,
        user_id: UserId,
        url: String,
        task_ids: Vec<TaskId>,
    ) -> QueryResult<Self> {
        let new_task_list_url = Self {
            id,
            user_id,
            created_at: chrono::Utc::now().naive_utc(),
            url,
            task_ids,
        };

        diesel::insert_into(crate::schema::task_list_urls::table)
            .values(&new_task_list_url)
            .get_result::<Self>(conn)
            .await
    }

    pub async fn get(conn: &mut AsyncPgConnection, id: Uuid) -> QueryResult<Self> {
        use crate::schema::task_list_urls::dsl;
        dsl::task_list_urls.find(id).get_result::<Self>(conn).await
    }

    pub async fn get_task_ids(conn: &mut AsyncPgConnection, id: Uuid) -> QueryResult<Vec<Uuid>> {
        use crate::schema::task_list_urls::dsl;
        dsl::task_list_urls
            .select(dsl::task_ids)
            .find(id)
            .get_result::<Vec<Uuid>>(conn)
            .await
    }

    /// Fetch all tasks from the task_ids field using paging semantics
    pub async fn fetch_tasks(
        &self,
        conn: &mut AsyncPgConnection,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<DenormalizedTask>, RejectReason> {
        let task_ids = self
            .task_ids
            .iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        DenormalizedTask::denormalize_all(conn, &task_ids, Some(OrderBy::Created), true).await
    }
}
