use diesel::{deserialize::FromSqlRow, AsExpression};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AsExpression, FromSqlRow,
)]
#[diesel(sql_type = diesel::sql_types::Integer)]
#[serde(rename_all = "camelCase")]
pub enum TaskLinkType {
    SubtaskOf,
    DependsOn,
    RelatedTo,
    AssignmentOrder,
}

impl From<i32> for TaskLinkType {
    fn from(entry: i32) -> Self {
        match entry {
            0 => Self::SubtaskOf,
            1 => Self::DependsOn,
            3 => Self::AssignmentOrder,
            _ => Self::RelatedTo,
        }
    }
}

impl From<TaskLinkType> for i32 {
    fn from(val: TaskLinkType) -> Self {
        match val {
            TaskLinkType::SubtaskOf => 0,
            TaskLinkType::DependsOn => 1,
            TaskLinkType::RelatedTo => 2,
            TaskLinkType::AssignmentOrder => 3,
        }
    }
}
