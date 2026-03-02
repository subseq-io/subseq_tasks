use serde::Deserialize;
use subseq_util::typed_uuid::{FromTypedUuid, TypedUuid};
use subseq_util::uuid_id_type;

uuid_id_type!(ProjectId, "project");
uuid_id_type!(MilestoneId, "milestone");
uuid_id_type!(TaskId, "task");
uuid_id_type!(TaskCommentId, "comment");
uuid_id_type!(TaskLogId, "log");
uuid_id_type!(TaskAttachmentFileId, "attachment");

pub(crate) fn deserialize_typed_uuid<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: FromTypedUuid,
{
    let value = TypedUuid::<T>::deserialize(deserializer)?;
    Ok(T::from_typed_uuid(value))
}

pub(crate) fn deserialize_optional_typed_uuid<'de, D, T>(
    deserializer: D,
) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: FromTypedUuid,
{
    let value = Option::<TypedUuid<T>>::deserialize(deserializer)?;
    Ok(value.map(T::from_typed_uuid))
}

pub(crate) fn parse_typed_uuid<T>(value: &str) -> Result<T, uuid::Error>
where
    T: FromTypedUuid,
{
    let typed = TypedUuid::<T>::from_str(value)?;
    Ok(T::from_typed_uuid(typed))
}
