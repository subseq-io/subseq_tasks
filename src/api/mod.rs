use std::{future::Future, pin::Pin, sync::Arc};

use axum::{
    Router,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use subseq_auth::prelude::ValidatesIdentity;
use subseq_auth::user_id::UserId;

use crate::error::{ErrorKind, LibError};
use crate::models::{ProjectId, TaskId, TaskUpdate};

mod milestones;
mod projects;
mod tasks;

#[derive(Debug)]
pub struct AppError(pub LibError);

impl From<LibError> for AppError {
    fn from(value: LibError) -> Self {
        Self(value)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match self.0.kind {
            ErrorKind::Conflict => StatusCode::CONFLICT,
            ErrorKind::Database => StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::Forbidden => StatusCode::FORBIDDEN,
            ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
            ErrorKind::NotFound => StatusCode::NOT_FOUND,
            ErrorKind::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
        };

        tracing::error!(kind = ?self.0.kind, error = %self.0.source, "tasks api request failed");
        (status, self.0.public).into_response()
    }
}

pub trait HasPool {
    fn pool(&self) -> Arc<sqlx::PgPool>;
}

pub type TaskUpdateHookFuture<'a> =
    Pin<Box<dyn Future<Output = crate::error::Result<()>> + Send + 'a>>;

pub trait TasksApp: HasPool + ValidatesIdentity {
    fn on_task_update(
        &self,
        _project_id: ProjectId,
        _task_id: TaskId,
        _actor_id: UserId,
        _task_update: TaskUpdate,
    ) -> TaskUpdateHookFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

pub fn routes<S>() -> Router<S>
where
    S: TasksApp + Clone + Send + Sync + 'static,
{
    tracing::info!("Registering route /project [GET,POST]");
    tracing::info!("Registering route /project/{{project_id}} [GET,PUT,DELETE]");
    tracing::info!("Registering route /milestone [GET,POST]");
    tracing::info!("Registering route /milestone/{{milestone_id}} [GET,PUT,DELETE]");
    tracing::info!("Registering route /task [GET,POST]");
    tracing::info!("Registering route /task/list [GET]");
    tracing::info!("Registering route /task/{{task_id}} [GET,PUT,DELETE]");
    tracing::info!("Registering route /task/{{task_id}}/link [POST]");
    tracing::info!("Registering route /task/{{task_id}}/comments [GET,POST]");
    tracing::info!("Registering route /task/{{task_id}}/comments/{{comment_id}} [PUT,DELETE]");
    tracing::info!("Registering route /task/{{task_id}}/attachment/{{file_id}} [POST,DELETE]");
    tracing::info!("Registering route /task/{{task_id}}/attachment/list [GET]");
    tracing::info!("Registering route /task/{{task_id}}/impact [GET]");
    tracing::info!("Registering route /task/{{task_id}}/link/{{other_task_id}} [DELETE]");
    tracing::info!("Registering route /task/{{task_id}}/log [GET]");
    tracing::info!("Registering route /task/{{task_id}}/transition [POST]");
    tracing::info!("Registering route /task/{{task_id}}/export [GET]");
    tracing::info!("Registering route /task/{{task_id}}/export/markdown [GET]");

    Router::new()
        .merge(projects::routes::<S>())
        .merge(milestones::routes::<S>())
        .merge(tasks::routes::<S>())
}
