use std::collections::{HashMap, HashSet};

use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use subseq_graph::db as graph_db;
use subseq_graph::invariants::graph_invariant_violations;
use subseq_graph::models::{
    GraphInvariantViolation, GraphKind, GraphNodeId, NewGraphNode, RemoveEdgePayload,
    ReparentNodePayload, UpsertEdgeMetadataPayload, UpsertNodePayload,
};

use super::*;

const TASK_DETAILS_LIMIT: i64 = 200;
const SUBTREE_MAX_TASKS: usize = 20_000;
const TASK_LINK_TYPES: [TaskLinkType; 4] = [
    TaskLinkType::SubtaskOf,
    TaskLinkType::DependsOn,
    TaskLinkType::RelatedTo,
    TaskLinkType::AssignmentOrder,
];

#[derive(Debug, Clone, Copy)]
struct ProjectLinkGraphBinding {
    project_id: ProjectId,
    link_type: TaskLinkType,
    graph_id: GraphId,
    root_node_id: Option<GraphNodeId>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ProjectLinkGraphRow {
    link_type: String,
    graph_id: Uuid,
    root_node_id: Option<Uuid>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct TaskLinkGraphNodeRow {
    node_id: Uuid,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct GraphEdgeMetadataRow {
    created_at: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct GraphIncidentLinkRow {
    metadata: serde_json::Value,
    created_at: chrono::NaiveDateTime,
    from_metadata: serde_json::Value,
    to_metadata: serde_json::Value,
    from_title: Option<String>,
    to_title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TaskLinkNodeApiMetadata {
    project_id: String,
    link_type: String,
    synthetic: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct TaskLinkNodeDbMetadata {
    project_id: String,
    link_type: String,
    synthetic: bool,
    #[serde(default)]
    external_id: Option<String>,
    #[serde(default)]
    task_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TaskLinkEdgeApiMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    subtask_parent_state: Option<String>,
    #[serde(default)]
    synthetic_root_edge: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct TaskLinkEdgeDbMetadata {
    #[serde(default)]
    subtask_parent_state: Option<String>,
    #[serde(default)]
    synthetic_root_edge: bool,
}

fn serialize_metadata<T: Serialize>(value: T, context: &'static str) -> Result<Value> {
    serde_json::to_value(value).map_err(|err| {
        LibError::invalid(
            "Invalid metadata payload",
            anyhow!("failed to serialize {context} metadata: {err}"),
        )
    })
}

fn deserialize_metadata<T: DeserializeOwned>(value: &Value, context: &'static str) -> Result<T> {
    serde_json::from_value::<T>(value.clone()).map_err(|err| {
        LibError::database(
            "Failed to decode graph metadata",
            anyhow!("failed to decode {context} metadata: {err}; payload={value}"),
        )
    })
}

fn storage_graph_kind_for_link(link_type: TaskLinkType) -> GraphKind {
    match link_type {
        // Tree-kind graphs cannot incrementally add disconnected task nodes with current graph APIs.
        // We store subtask edges in a DAG graph and enforce rooted-tree semantics at mutation boundaries.
        TaskLinkType::SubtaskOf => GraphKind::Dag,
        TaskLinkType::DependsOn => GraphKind::Dag,
        TaskLinkType::RelatedTo | TaskLinkType::AssignmentOrder => GraphKind::Directed,
    }
}

fn synthetic_root_label_for_link(link_type: TaskLinkType) -> &'static str {
    match link_type {
        TaskLinkType::SubtaskOf => "subtask_root",
        TaskLinkType::DependsOn => "depends_anchor",
        TaskLinkType::RelatedTo => "related_anchor",
        TaskLinkType::AssignmentOrder => "assignment_anchor",
    }
}

fn task_node_api_metadata(
    project_id: ProjectId,
    link_type: TaskLinkType,
    task_id: TaskId,
) -> Result<Value> {
    serialize_metadata(
        TaskLinkNodeApiMetadata {
            project_id: project_id.0.to_string(),
            link_type: link_type.as_db_value().to_string(),
            synthetic: false,
            external_id: Some(task_id.0.to_string()),
            task_id: Some(task_id.0.to_string()),
        },
        "task-link node (api)",
    )
}

fn synthetic_node_api_metadata(project_id: ProjectId, link_type: TaskLinkType) -> Result<Value> {
    serialize_metadata(
        TaskLinkNodeApiMetadata {
            project_id: project_id.0.to_string(),
            link_type: link_type.as_db_value().to_string(),
            synthetic: true,
            external_id: None,
            task_id: None,
        },
        "task-link synthetic node (api)",
    )
}

fn synthetic_root_edge_api_metadata() -> Result<Value> {
    serialize_metadata(
        TaskLinkEdgeApiMetadata {
            synthetic_root_edge: true,
            ..TaskLinkEdgeApiMetadata::default()
        },
        "task-link synthetic root edge (api)",
    )
}

fn subtask_parent_edge_api_metadata(state: TaskState) -> Result<Value> {
    serialize_metadata(
        TaskLinkEdgeApiMetadata {
            subtask_parent_state: Some(state.as_str().to_string()),
            ..TaskLinkEdgeApiMetadata::default()
        },
        "task-link subtask parent edge (api)",
    )
}

fn default_edge_api_metadata() -> Result<Value> {
    serialize_metadata(TaskLinkEdgeApiMetadata::default(), "task-link edge (api)")
}

fn parse_task_id_from_node_metadata(value: &Value) -> Result<Option<TaskId>> {
    let metadata: TaskLinkNodeDbMetadata = deserialize_metadata(value, "task-link node (db)")?;
    let Some(external_id) = metadata.external_id else {
        return Ok(None);
    };
    let uuid = Uuid::parse_str(&external_id).map_err(|err| {
        LibError::database(
            "Failed to decode graph node external_id",
            anyhow!("invalid external_id uuid '{external_id}' in graph metadata: {err}"),
        )
    })?;
    Ok(Some(TaskId(uuid)))
}

fn parse_subtask_parent_state_from_edge_metadata(value: &Value) -> Result<Option<TaskState>> {
    let metadata: TaskLinkEdgeDbMetadata = deserialize_metadata(value, "task-link edge (db)")?;
    metadata
        .subtask_parent_state
        .as_deref()
        .map(task_state_from_row)
        .transpose()
}

fn graph_violation_values(violations: &[GraphInvariantViolation]) -> serde_json::Value {
    json!(
        violations
            .iter()
            .map(|value| serde_json::to_value(value).unwrap_or_else(|_| json!({"type": "unknown"})))
            .collect::<Vec<_>>()
    )
}

async fn load_project_link_graph_bindings<'e, E>(
    executor: E,
    project_id: ProjectId,
) -> Result<HashMap<TaskLinkType, ProjectLinkGraphBinding>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let rows = sqlx::query_as::<_, ProjectLinkGraphRow>(
        r#"
        SELECT link_type, graph_id, root_node_id
        FROM tasks.project_link_graphs
        WHERE project_id = $1
        ORDER BY link_type ASC
        "#,
    )
    .bind(project_id.0)
    .fetch_all(executor)
    .await
    .map_err(|err| db_err("Failed to query project link graph bindings", err))?;

    let mut bindings = HashMap::with_capacity(rows.len());
    for row in rows {
        let link_type = task_link_type_from_row(&row.link_type)?;
        bindings.insert(
            link_type,
            ProjectLinkGraphBinding {
                project_id,
                link_type,
                graph_id: GraphId(row.graph_id),
                root_node_id: row.root_node_id.map(GraphNodeId),
            },
        );
    }

    Ok(bindings)
}

fn ensure_all_link_type_bindings_present(
    bindings: &HashMap<TaskLinkType, ProjectLinkGraphBinding>,
    project_id: ProjectId,
) -> Result<()> {
    let missing = TASK_LINK_TYPES
        .iter()
        .copied()
        .filter(|link_type| !bindings.contains_key(link_type))
        .map(|link_type| link_type.as_db_value().to_string())
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }

    Err(LibError::database(
        "Project link graph bindings are missing",
        anyhow!(
            "project {project_id} missing link graph bindings for {:?}",
            missing
        ),
    ))
}

pub(super) async fn initialize_project_link_graphs_if_missing(
    pool: &PgPool,
    actor: UserId,
    project_id: ProjectId,
) -> Result<()> {
    let existing = load_project_link_graph_bindings(pool, project_id).await?;
    if TASK_LINK_TYPES
        .iter()
        .all(|link_type| existing.contains_key(link_type))
    {
        return Ok(());
    }

    let Some(project) = load_project_row(pool, project_id).await? else {
        return Err(LibError::not_found(
            "Project not found",
            anyhow!("project {project_id} not found while initializing link graphs"),
        ));
    };
    let owner_group_id = project.owner_group_id.map(GroupId);
    let missing = TASK_LINK_TYPES
        .iter()
        .copied()
        .filter(|link_type| !existing.contains_key(link_type))
        .collect::<Vec<_>>();

    for link_type in missing {
        let graph = graph_db::create_graph(
            pool,
            actor,
            subseq_graph::models::CreateGraphPayload {
                kind: storage_graph_kind_for_link(link_type),
                name: format!("{}-{}", project.slug, link_type.as_db_value()),
                description: Some(format!(
                    "Task link graph ({}) for project {}",
                    link_type.as_db_value(),
                    project_id
                )),
                metadata: Some(serialize_metadata(
                    TaskLinkNodeApiMetadata {
                        project_id: project_id.0.to_string(),
                        link_type: link_type.as_db_value().to_string(),
                        synthetic: true,
                        external_id: None,
                        task_id: None,
                    },
                    "project link graph metadata",
                )?),
                owner_group_id,
                nodes: vec![NewGraphNode {
                    id: None,
                    label: synthetic_root_label_for_link(link_type).to_string(),
                    metadata: Some(synthetic_node_api_metadata(project_id, link_type)?),
                }],
                edges: vec![],
            },
            graph_perm::graph_create_access_roles(),
        )
        .await
        .map_err(LibError::from)?;

        let root_node_id = if link_type == TaskLinkType::SubtaskOf {
            Some(
                graph
                    .nodes
                    .first()
                    .ok_or_else(|| {
                        LibError::database(
                            "Failed to initialize subtask root node",
                            anyhow!("created graph {} has no seed node", graph.id),
                        )
                    })?
                    .id
                    .0,
            )
        } else {
            None
        };

        sqlx::query(
            r#"
            INSERT INTO tasks.project_link_graphs (
                project_id,
                link_type,
                graph_id,
                root_node_id
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (project_id, link_type)
            DO NOTHING
            "#,
        )
        .bind(project_id.0)
        .bind(link_type.as_db_value())
        .bind(graph.id.0)
        .bind(root_node_id)
        .execute(pool)
        .await
        .map_err(|err| db_err("Failed to persist project link graph binding", err))?;
    }

    Ok(())
}

async fn project_link_graph_bindings_for_actor(
    pool: &PgPool,
    actor: UserId,
    project_id: ProjectId,
) -> Result<HashMap<TaskLinkType, ProjectLinkGraphBinding>> {
    initialize_project_link_graphs_if_missing(pool, actor, project_id).await?;
    let bindings = load_project_link_graph_bindings(pool, project_id).await?;
    ensure_all_link_type_bindings_present(&bindings, project_id)?;
    Ok(bindings)
}

async fn node_id_for_task_link_graph_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    project_id: ProjectId,
    link_type: TaskLinkType,
    task_id: TaskId,
) -> Result<Option<GraphNodeId>> {
    let row = sqlx::query_as::<_, TaskLinkGraphNodeRow>(
        r#"
        SELECT node_id
        FROM tasks.task_link_graph_nodes
        WHERE project_id = $1
          AND link_type = $2
          AND task_id = $3
        LIMIT 1
        "#,
    )
    .bind(project_id.0)
    .bind(link_type.as_db_value())
    .bind(task_id.0)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|err| db_err("Failed to query task link graph node mapping", err))?;

    Ok(row.map(|row| GraphNodeId(row.node_id)))
}

async fn ensure_task_node_in_link_graph_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pool: &PgPool,
    actor: UserId,
    binding: ProjectLinkGraphBinding,
    task_id: TaskId,
    task_title: &str,
) -> Result<GraphNodeId> {
    if let Some(node_id) =
        node_id_for_task_link_graph_tx(tx, binding.project_id, binding.link_type, task_id).await?
    {
        graph_db::upsert_node_tx(
            tx,
            pool,
            actor,
            binding.graph_id,
            UpsertNodePayload {
                node_id: Some(node_id),
                label: task_title.to_string(),
                metadata: Some(task_node_api_metadata(
                    binding.project_id,
                    binding.link_type,
                    task_id,
                )?),
                expected_updated_at: None,
            },
            graph_perm::graph_update_access_roles(),
        )
        .await
        .map_err(LibError::from)?;
        return Ok(node_id);
    }

    let node_id = GraphNodeId(Uuid::new_v4());
    graph_db::upsert_node_tx(
        tx,
        pool,
        actor,
        binding.graph_id,
        UpsertNodePayload {
            node_id: Some(node_id),
            label: task_title.to_string(),
            metadata: Some(task_node_api_metadata(
                binding.project_id,
                binding.link_type,
                task_id,
            )?),
            expected_updated_at: None,
        },
        graph_perm::graph_update_access_roles(),
    )
    .await
    .map_err(LibError::from)?;

    if binding.link_type == TaskLinkType::SubtaskOf {
        let root_node_id = binding.root_node_id.ok_or_else(|| {
            LibError::database(
                "Subtask link graph root node is missing",
                anyhow!(
                    "project {} subtask graph has no root node",
                    binding.project_id
                ),
            )
        })?;
        graph_db::upsert_edge_metadata_tx(
            tx,
            pool,
            actor,
            binding.graph_id,
            UpsertEdgeMetadataPayload {
                from_node_id: root_node_id,
                to_node_id: node_id,
                metadata: synthetic_root_edge_api_metadata()?,
                expected_updated_at: None,
            },
            graph_perm::graph_update_access_roles(),
        )
        .await
        .map_err(LibError::from)?;
    }

    sqlx::query(
        r#"
        INSERT INTO tasks.task_link_graph_nodes (
            project_id,
            link_type,
            task_id,
            node_id
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (project_id, link_type, task_id)
        DO NOTHING
        "#,
    )
    .bind(binding.project_id.0)
    .bind(binding.link_type.as_db_value())
    .bind(task_id.0)
    .bind(node_id.0)
    .execute(&mut **tx)
    .await
    .map_err(|err| db_err("Failed to persist task link graph node mapping", err))?;

    let mapped_node_id =
        node_id_for_task_link_graph_tx(tx, binding.project_id, binding.link_type, task_id)
            .await?
            .ok_or_else(|| {
                LibError::database(
                    "Task link graph node mapping is missing",
                    anyhow!(
                        "mapping missing after insert for task {} in project {} ({})",
                        task_id,
                        binding.project_id,
                        binding.link_type.as_db_value()
                    ),
                )
            })?;

    Ok(mapped_node_id)
}

async fn edge_exists_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: GraphId,
    from_node_id: GraphNodeId,
    to_node_id: GraphNodeId,
) -> Result<bool> {
    let row: (bool,) = sqlx::query_as(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM graph.edges
            WHERE graph_id = $1
              AND from_node_id = $2
              AND to_node_id = $3
        )
        "#,
    )
    .bind(graph_id.0)
    .bind(from_node_id.0)
    .bind(to_node_id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(|err| db_err("Failed to query graph edge existence", err))?;

    Ok(row.0)
}

async fn edge_metadata_row_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: GraphId,
    from_node_id: GraphNodeId,
    to_node_id: GraphNodeId,
) -> Result<Option<GraphEdgeMetadataRow>> {
    sqlx::query_as::<_, GraphEdgeMetadataRow>(
        r#"
        SELECT created_at
        FROM graph.edges
        WHERE graph_id = $1
          AND from_node_id = $2
          AND to_node_id = $3
        LIMIT 1
        "#,
    )
    .bind(graph_id.0)
    .bind(from_node_id.0)
    .bind(to_node_id.0)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|err| db_err("Failed to query graph edge metadata", err))
}

fn normalize_free_text(value: Option<String>) -> Option<String> {
    value
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

fn transition_allowed(from: TaskState, to: TaskState) -> bool {
    if from == to {
        return false;
    }

    if matches!(to, TaskState::Done | TaskState::Rejected)
        && !matches!(from, TaskState::Done | TaskState::Rejected)
    {
        return true;
    }

    match from {
        TaskState::Open => matches!(to, TaskState::Todo),
        TaskState::Todo => matches!(
            to,
            TaskState::Assigned | TaskState::Open | TaskState::InProgress
        ),
        TaskState::Assigned => matches!(to, TaskState::InProgress | TaskState::Todo),
        TaskState::InProgress => matches!(to, TaskState::Todo | TaskState::Acceptance),
        TaskState::Acceptance => matches!(to, TaskState::InProgress),
        TaskState::Done | TaskState::Rejected => false,
    }
}

fn build_transition_comment(
    from_state: TaskState,
    to_state: TaskState,
    payload: &TransitionTaskPayload,
) -> Option<String> {
    let explicit_comment = normalize_free_text(payload.comment.clone());
    let mut context_lines = Vec::new();

    match (from_state, to_state) {
        (TaskState::Open, TaskState::Todo) => {
            if let Some(when) = payload.when {
                context_lines.push(format!("Moved to todo at {}", when.to_rfc3339()));
            }
        }
        (TaskState::Todo, TaskState::Open) => {
            if let Some(reason) = normalize_free_text(payload.deferral_reason.clone()) {
                context_lines.push(format!("Deferral reason: {reason}"));
            }
        }
        (TaskState::Assigned, TaskState::Todo) | (TaskState::InProgress, TaskState::Todo) => {
            if let Some(reason) = normalize_free_text(payload.cant_do_reason.clone()) {
                context_lines.push(format!("Can't do reason: {reason}"));
            }
        }
        (TaskState::Assigned, TaskState::InProgress) => {
            if let Some(estimate) = normalize_free_text(payload.estimated_time_to_complete.clone())
            {
                context_lines.push(format!("Estimated time-to-complete: {estimate}"));
            }
        }
        (TaskState::InProgress, TaskState::Acceptance) => {
            if let Some(details) = normalize_free_text(payload.work_log_details.clone()) {
                context_lines.push(format!("Work log details: {details}"));
            }
        }
        (TaskState::Acceptance, TaskState::InProgress) => {
            if let Some(feedback) = normalize_free_text(payload.feedback.clone()) {
                context_lines.push(format!("Feedback: {feedback}"));
            }
        }
        _ => {}
    }

    match (explicit_comment, context_lines.is_empty()) {
        (Some(comment), true) => Some(comment),
        (Some(comment), false) => Some(format!("{comment}\n\n{}", context_lines.join("\n"))),
        (None, false) => Some(context_lines.join("\n")),
        (None, true) => None,
    }
}

fn transition_details(
    from_state: TaskState,
    to_state: TaskState,
    payload: &TransitionTaskPayload,
    assignee_user_id: Option<Uuid>,
    completed_by_user_id: Option<Uuid>,
    completed_at: Option<chrono::NaiveDateTime>,
    rejected_reason: Option<&str>,
) -> serde_json::Value {
    json!({
        "fromState": from_state.as_str(),
        "toState": to_state.as_str(),
        "when": payload.when.map(|value| value.to_rfc3339()),
        "assignedToUserId": assignee_user_id,
        "deferralReason": normalize_free_text(payload.deferral_reason.clone()),
        "cantDoReason": normalize_free_text(payload.cant_do_reason.clone()),
        "estimatedTimeToComplete": normalize_free_text(payload.estimated_time_to_complete.clone()),
        "workLogDetails": normalize_free_text(payload.work_log_details.clone()),
        "feedback": normalize_free_text(payload.feedback.clone()),
        "doneByUserId": completed_by_user_id,
        "doneAt": completed_at,
        "rejectedReason": rejected_reason,
        "comment": normalize_free_text(payload.comment.clone()),
    })
}

fn normalize_subtask_parent_state_for_link(
    link_type: TaskLinkType,
    subtask_parent_state: Option<TaskState>,
) -> Result<Option<TaskState>> {
    match link_type {
        TaskLinkType::SubtaskOf => subtask_parent_state.map(Some).ok_or_else(|| {
            LibError::invalid(
                "Subtask links require a parent state",
                anyhow!("subtask_of link missing subtask_parent_state"),
            )
        }),
        _ => {
            if subtask_parent_state.is_some() {
                Err(LibError::invalid(
                    "Only subtask links may set subtaskParentState",
                    anyhow!("non-subtask link included subtaskParentState"),
                ))
            } else {
                Ok(None)
            }
        }
    }
}

fn resolve_transition_assignee(
    from_state: TaskState,
    to_state: TaskState,
    payload: &TransitionTaskPayload,
    actor: UserId,
    existing_assignee: Option<Uuid>,
) -> Result<Option<Uuid>> {
    let assignee = match (from_state, to_state) {
        (TaskState::Todo, TaskState::Assigned) => {
            Some(payload.assigned_to_user_id.map(|id| id.0).ok_or_else(|| {
                LibError::invalid(
                    "assignedToUserId is required for this transition",
                    anyhow!("todo -> assigned transition missing assigned_to_user_id"),
                )
            })?)
        }
        (TaskState::Todo, TaskState::InProgress) => Some(actor.0),
        _ => existing_assignee,
    };

    Ok(assignee)
}

async fn entry_node_for_graph(
    pool: &PgPool,
    actor: UserId,
    graph_id: GraphId,
) -> Result<Option<GraphNodeId>> {
    let graph = get_graph(pool, actor, graph_id, graph_perm::graph_read_access_roles())
        .await
        .map_err(LibError::from)?;
    Ok(graph.nodes.first().map(|node| node.id))
}

async fn validate_milestone_access_for_task(
    pool: &PgPool,
    actor: UserId,
    milestone_id: MilestoneId,
) -> Result<()> {
    let _ = load_accessible_milestone(pool, actor, milestone_id, perm::milestone_read()).await?;
    Ok(())
}

async fn shared_project_ids_for_tasks<'e, E>(
    executor: E,
    task_id: TaskId,
    other_task_id: TaskId,
) -> Result<Vec<ProjectId>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let rows = sqlx::query_as::<_, (Uuid,)>(
        r#"
        SELECT p1.project_id
        FROM tasks.task_projects p1
        JOIN tasks.task_projects p2
          ON p2.project_id = p1.project_id
        JOIN tasks.projects pr
          ON pr.id = p1.project_id
        WHERE p1.task_id = $1
          AND p2.task_id = $2
          AND pr.deleted_at IS NULL
        ORDER BY p1.project_id ASC
        "#,
    )
    .bind(task_id.0)
    .bind(other_task_id.0)
    .fetch_all(executor)
    .await
    .map_err(|err| db_err("Failed to query shared task projects", err))?;

    Ok(rows.into_iter().map(|row| ProjectId(row.0)).collect())
}

async fn lock_tasks_for_projects<'e, E>(executor: E, project_ids: &[ProjectId]) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let ids: Vec<Uuid> = project_ids.iter().map(|id| id.0).collect();
    if ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        SELECT t.id
        FROM tasks.tasks t
        JOIN tasks.task_projects tp
          ON tp.task_id = t.id
        WHERE tp.project_id = ANY($1)
          AND t.deleted_at IS NULL
        FOR UPDATE
        "#,
    )
    .bind(ids)
    .execute(executor)
    .await
    .map_err(|err| db_err("Failed to lock project task rows", err))?;

    Ok(())
}

async fn subtree_task_ids_for_root(pool: &PgPool, task_id: TaskId) -> Result<Vec<TaskId>> {
    let mut task_ids = HashSet::from([task_id]);
    let project_rows = sqlx::query_as::<_, (Uuid,)>(
        r#"
        SELECT tp.project_id
        FROM tasks.task_projects tp
        JOIN tasks.projects p
          ON p.id = tp.project_id
        WHERE tp.task_id = $1
          AND p.deleted_at IS NULL
        ORDER BY tp.project_id ASC
        "#,
    )
    .bind(task_id.0)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query task projects for subtree traversal", err))?;

    for (project_uuid,) in project_rows {
        let project_id = ProjectId(project_uuid);
        let bindings = load_project_link_graph_bindings(pool, project_id).await?;
        let Some(subtask_binding) = bindings.get(&TaskLinkType::SubtaskOf).copied() else {
            continue;
        };

        let node_row = sqlx::query_as::<_, TaskLinkGraphNodeRow>(
            r#"
            SELECT node_id
            FROM tasks.task_link_graph_nodes
            WHERE project_id = $1
              AND link_type = 'subtask_of'
              AND task_id = $2
            LIMIT 1
            "#,
        )
        .bind(project_id.0)
        .bind(task_id.0)
        .fetch_optional(pool)
        .await
        .map_err(|err| db_err("Failed to query subtree root graph node mapping", err))?;

        let Some(node_row) = node_row else {
            continue;
        };

        let rows = sqlx::query_as::<_, (Uuid,)>(
            r#"
            WITH RECURSIVE subtree(node_id) AS (
                SELECT $2::uuid
                UNION
                SELECT e.to_node_id
                FROM graph.edges e
                JOIN subtree s
                  ON s.node_id = e.from_node_id
                JOIN graph.nodes n_to
                  ON n_to.graph_id = e.graph_id
                 AND n_to.id = e.to_node_id
                WHERE e.graph_id = $1
                  AND n_to.metadata ->> 'external_id' IS NOT NULL
            )
            SELECT DISTINCT (n.metadata ->> 'external_id')::uuid AS task_id
            FROM subtree s
            JOIN graph.nodes n
              ON n.graph_id = $1
             AND n.id = s.node_id
            JOIN tasks.tasks t
              ON t.id = (n.metadata ->> 'external_id')::uuid
            WHERE n.metadata ->> 'external_id' IS NOT NULL
              AND t.deleted_at IS NULL
            ORDER BY task_id ASC
            LIMIT $3
            "#,
        )
        .bind(subtask_binding.graph_id.0)
        .bind(node_row.node_id)
        .bind(SUBTREE_MAX_TASKS as i64)
        .fetch_all(pool)
        .await
        .map_err(|err| db_err("Failed to traverse subtask graph subtree", err))?;

        for (subtree_task_id,) in rows {
            task_ids.insert(TaskId(subtree_task_id));
            if task_ids.len() >= SUBTREE_MAX_TASKS {
                break;
            }
        }
        if task_ids.len() >= SUBTREE_MAX_TASKS {
            break;
        }
    }

    let mut output = task_ids.into_iter().collect::<Vec<_>>();
    output.sort_by_key(|id| id.0);
    Ok(output)
}

async fn validate_project_link_graph_integrity(
    pool: &PgPool,
    actor: UserId,
    project_id: ProjectId,
) -> Result<()> {
    let bindings = load_project_link_graph_bindings(pool, project_id).await?;
    for link_type in TASK_LINK_TYPES {
        let Some(binding) = bindings.get(&link_type).copied() else {
            continue;
        };
        let graph = get_graph(
            pool,
            actor,
            binding.graph_id,
            graph_perm::graph_read_access_roles(),
        )
        .await
        .map_err(LibError::from)?;

        let target_kind = match link_type {
            TaskLinkType::SubtaskOf => GraphKind::Tree,
            TaskLinkType::DependsOn => GraphKind::Dag,
            TaskLinkType::RelatedTo | TaskLinkType::AssignmentOrder => GraphKind::Directed,
        };
        let violations = graph_invariant_violations(target_kind, &graph.nodes, &graph.edges);
        if !violations.is_empty() {
            return Err(LibError::invalid(
                "Project link graph contains invalid structure",
                anyhow!(
                    "project {project_id} link_type {} failed validation: {}",
                    link_type.as_db_value(),
                    graph_violation_values(&violations)
                ),
            ));
        }
    }

    Ok(())
}

async fn load_task_links_from_graph(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    project_ids: &[ProjectId],
) -> Result<(Vec<TaskLink>, Vec<TaskLink>)> {
    if project_ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut checked_graph_ids = HashSet::new();
    let mut deduped = HashMap::<(Uuid, Uuid, String), TaskLink>::new();

    for project_id in project_ids {
        let bindings = load_project_link_graph_bindings(pool, *project_id).await?;
        for link_type in TASK_LINK_TYPES {
            let Some(binding) = bindings.get(&link_type).copied() else {
                continue;
            };
            if checked_graph_ids.insert(binding.graph_id.0) {
                ensure_graph_read_access(pool, actor, binding.graph_id).await?;
            }

            let node_row = sqlx::query_as::<_, TaskLinkGraphNodeRow>(
                r#"
                SELECT node_id
                FROM tasks.task_link_graph_nodes
                WHERE project_id = $1
                  AND link_type = $2
                  AND task_id = $3
                LIMIT 1
                "#,
            )
            .bind(project_id.0)
            .bind(link_type.as_db_value())
            .bind(task_id.0)
            .fetch_optional(pool)
            .await
            .map_err(|err| db_err("Failed to query task link graph node mapping", err))?;

            let Some(node_row) = node_row else {
                continue;
            };

            let rows = sqlx::query_as::<_, GraphIncidentLinkRow>(
                r#"
                SELECT
                    e.metadata,
                    e.created_at,
                    n_from.metadata AS from_metadata,
                    n_to.metadata AS to_metadata,
                    t_from.title AS from_title,
                    t_to.title AS to_title
                FROM graph.edges e
                JOIN graph.nodes n_from
                  ON n_from.graph_id = e.graph_id
                 AND n_from.id = e.from_node_id
                JOIN graph.nodes n_to
                  ON n_to.graph_id = e.graph_id
                 AND n_to.id = e.to_node_id
                LEFT JOIN tasks.tasks t_from
                  ON t_from.id = (n_from.metadata ->> 'external_id')::uuid
                 AND t_from.deleted_at IS NULL
                LEFT JOIN tasks.tasks t_to
                  ON t_to.id = (n_to.metadata ->> 'external_id')::uuid
                 AND t_to.deleted_at IS NULL
                WHERE e.graph_id = $1
                  AND (e.from_node_id = $2 OR e.to_node_id = $2)
                ORDER BY e.created_at DESC, e.from_node_id ASC, e.to_node_id ASC
                "#,
            )
            .bind(binding.graph_id.0)
            .bind(node_row.node_id)
            .fetch_all(pool)
            .await
            .map_err(|err| db_err("Failed to query graph incident links", err))?;

            for row in rows {
                let from_task_id = match parse_task_id_from_node_metadata(&row.from_metadata)? {
                    Some(task) => task,
                    None => continue,
                };
                let to_task_id = match parse_task_id_from_node_metadata(&row.to_metadata)? {
                    Some(task) => task,
                    None => continue,
                };
                if from_task_id != task_id && to_task_id != task_id {
                    continue;
                }

                let subtask_parent_state = if link_type == TaskLinkType::SubtaskOf {
                    parse_subtask_parent_state_from_edge_metadata(&row.metadata)?
                } else {
                    None
                };
                let other_task_title = if from_task_id == task_id {
                    row.to_title
                } else {
                    row.from_title
                };

                let link = TaskLink {
                    task_from_id: from_task_id,
                    task_to_id: to_task_id,
                    link_type,
                    subtask_parent_state,
                    other_task_title,
                    created_at: row.created_at,
                };

                let key = (
                    link.task_from_id.0,
                    link.task_to_id.0,
                    link.link_type.as_db_value().to_string(),
                );
                match deduped.get_mut(&key) {
                    Some(existing) if existing.created_at >= link.created_at => {}
                    Some(existing) => *existing = link,
                    None => {
                        deduped.insert(key, link);
                    }
                }
            }
        }
    }

    let mut links_out = deduped
        .values()
        .filter(|link| link.task_from_id == task_id)
        .cloned()
        .collect::<Vec<_>>();
    let mut links_in = deduped
        .values()
        .filter(|link| link.task_to_id == task_id)
        .cloned()
        .collect::<Vec<_>>();

    links_out.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| a.task_to_id.0.cmp(&b.task_to_id.0))
    });
    links_in.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| a.task_from_id.0.cmp(&b.task_from_id.0))
    });

    Ok((links_out, links_in))
}

pub async fn create_task_with_roles(
    pool: &PgPool,
    actor: UserId,
    payload: CreateTaskPayload,
) -> Result<TaskDetails> {
    let project =
        load_accessible_project(pool, actor, payload.project_id, perm::task_create()).await?;

    if let Some(state) = payload.state
        && state != TaskState::Open
    {
        return Err(LibError::invalid(
            "Task must start in the open state",
            anyhow!("initial state {} is not allowed", state.as_str()),
        ));
    }

    let title = normalize_task_title(&payload.title)?;
    let description = normalize_description(payload.description);
    let project_link_bindings =
        project_link_graph_bindings_for_actor(pool, actor, payload.project_id).await?;

    if let Some(milestone_id) = payload.milestone_id {
        validate_milestone_access_for_task(pool, actor, milestone_id).await?;
    }

    let task_id = TaskId(Uuid::new_v4());
    let state = TaskState::Open;
    let metadata = payload.metadata.unwrap_or_else(|| json!({}));

    let mut graph_assignments: Vec<(GraphId, Option<GraphNodeId>, i32)> = Vec::new();
    let state_graph_id = GraphId(project.task_state_graph_id);
    graph_assignments.push((
        state_graph_id,
        entry_node_for_graph(pool, actor, state_graph_id).await?,
        0,
    ));

    if let Some(task_graph_id) = project.task_graph_id {
        let task_graph_id = GraphId(task_graph_id);
        if task_graph_id != state_graph_id {
            graph_assignments.push((
                task_graph_id,
                entry_node_for_graph(pool, actor, task_graph_id).await?,
                1,
            ));
        }
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|err| db_err("Failed to start transaction", err))?;

    sqlx::query(
        r#"
        SELECT id
        FROM tasks.projects
        WHERE id = $1
          AND deleted_at IS NULL
        FOR UPDATE
        "#,
    )
    .bind(payload.project_id.0)
    .execute(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to lock project", err))?;

    let row: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*)::bigint
        FROM tasks.task_projects tp
        JOIN tasks.tasks t
          ON t.id = tp.task_id
        WHERE tp.project_id = $1
          AND t.deleted_at IS NULL
        "#,
    )
    .bind(payload.project_id.0)
    .fetch_one(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to build project task slug", err))?;

    let order_added = (row.0 + 1) as i32;
    let slug = format!("{}-{}", project.slug, order_added);

    sqlx::query(
        r#"
        INSERT INTO tasks.tasks (
            id,
            slug,
            title,
            description,
            author_user_id,
            assignee_user_id,
            priority,
            due_date,
            milestone_id,
            state,
            archived,
            metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, FALSE, $11)
        "#,
    )
    .bind(task_id.0)
    .bind(slug)
    .bind(&title)
    .bind(&description)
    .bind(actor.0)
    .bind(payload.assignee_user_id.map(|id| id.0))
    .bind(payload.priority.unwrap_or(0))
    .bind(payload.due_date.map(|value| value.naive_utc()))
    .bind(payload.milestone_id.map(|id| id.0))
    .bind(state.as_str())
    .bind(metadata)
    .execute(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to create task", err))?;

    sqlx::query(
        r#"
        INSERT INTO tasks.task_projects (task_id, project_id, order_added)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(task_id.0)
    .bind(payload.project_id.0)
    .bind(order_added)
    .execute(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to attach task to project", err))?;

    for link_type in TASK_LINK_TYPES {
        let Some(binding) = project_link_bindings.get(&link_type).copied() else {
            continue;
        };
        ensure_task_node_in_link_graph_tx(&mut tx, pool, actor, binding, task_id, &title).await?;
    }

    for (graph_id, current_node_id, order) in graph_assignments {
        sqlx::query(
            r#"
            INSERT INTO tasks.task_graph_assignments (
                task_id,
                graph_id,
                current_node_id,
                order_added
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (task_id, graph_id)
            DO UPDATE SET
                current_node_id = EXCLUDED.current_node_id,
                order_added = EXCLUDED.order_added,
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(task_id.0)
        .bind(graph_id.0)
        .bind(current_node_id.map(|id| id.0))
        .bind(order)
        .execute(&mut *tx)
        .await
        .map_err(|err| db_err("Failed to attach task graph assignment", err))?;
    }

    tx.commit()
        .await
        .map_err(|err| db_err("Failed to commit transaction", err))?;

    append_task_log(
        pool,
        task_id,
        actor,
        "task_created",
        None,
        Some(TaskState::Open),
        json!({
            "projectId": payload.project_id.0,
            "milestoneId": payload.milestone_id.map(|id| id.0),
            "priority": payload.priority.unwrap_or(0),
        }),
    )
    .await?;

    get_task_with_roles(pool, actor, task_id).await
}

pub async fn get_task_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
) -> Result<TaskDetails> {
    let _ = load_accessible_task(pool, actor, task_id, perm::task_read()).await?;
    let row = load_task_presentation_row(pool, task_id).await?;
    let mut details = match row {
        Some(row) => to_task_details_from_presentation_row(row)?,
        None => {
            return Err(LibError::database(
                "Failed to load task details",
                anyhow!("task {task_id} was accessible but missing from task presentation view"),
            ));
        }
    };

    ensure_task_assignment_graph_access(pool, actor, &details.graph_assignments).await?;
    let (links_out, links_in) =
        load_task_links_from_graph(pool, actor, task_id, &details.project_ids).await?;
    details.links_out = links_out;
    details.links_in = links_in;
    Ok(details)
}

pub async fn list_tasks_with_roles(
    pool: &PgPool,
    actor: UserId,
    project_id: Option<ProjectId>,
    assignee_user_id: Option<UserId>,
    state: Option<TaskState>,
    archived: Option<bool>,
    page: u32,
    limit: u32,
) -> Result<Paged<Task>> {
    let allowed_project_ids = accessible_project_ids(pool, actor, perm::task_read()).await?;

    let offset = (page.saturating_sub(1) as i64).saturating_mul(limit as i64);

    let rows = sqlx::query_as::<_, TaskRow>(
        r#"
        SELECT
            t.id,
            t.slug,
            t.title,
            t.description,
            t.author_user_id,
            t.assignee_user_id,
            t.priority,
            t.due_date,
            t.milestone_id,
            t.state,
            t.archived,
            t.completed_by_user_id,
            t.completed_at,
            t.rejected_reason,
            t.metadata,
            t.created_at,
            t.updated_at
        FROM tasks.tasks t
        WHERE t.deleted_at IS NULL
          AND (
              t.author_user_id = $2
              OR EXISTS (
                  SELECT 1
                  FROM tasks.task_projects tp
                  WHERE tp.task_id = t.id
                    AND tp.project_id = ANY($1)
              )
          )
          AND (
              $3::uuid IS NULL
              OR EXISTS (
                  SELECT 1
                  FROM tasks.task_projects tp2
                  WHERE tp2.task_id = t.id
                    AND tp2.project_id = $3
              )
          )
          AND ($4::uuid IS NULL OR t.assignee_user_id = $4)
          AND ($5::text IS NULL OR t.state = $5)
          AND ($6::bool IS NULL OR t.archived = $6)
        ORDER BY t.updated_at DESC, t.id DESC
        LIMIT $7 OFFSET $8
        "#,
    )
    .bind(&allowed_project_ids)
    .bind(actor.0)
    .bind(project_id.map(|id| id.0))
    .bind(assignee_user_id.map(|id| id.0))
    .bind(state.map(|value| value.as_str()))
    .bind(archived)
    .bind(limit as i64)
    .bind(offset)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to list tasks", err))?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(to_task(row)?);
    }

    Ok(Paged { page, limit, items })
}

pub async fn update_task_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    payload: UpdateTaskPayload,
) -> Result<TaskDetails> {
    let existing = load_accessible_task(pool, actor, task_id, perm::task_update()).await?;

    if payload.state.is_some() {
        return Err(LibError::invalid(
            "State updates must use the transition endpoint",
            anyhow!("task update payload attempted direct state mutation"),
        ));
    }

    let title = payload
        .title
        .map(|value| normalize_task_title(&value))
        .transpose()?
        .unwrap_or_else(|| existing.title.clone());

    let description = payload
        .description
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|| existing.description.clone());

    let assignee_user_id = if payload.clear_assignee.unwrap_or(false) {
        None
    } else {
        payload
            .assignee_user_id
            .map(|id| id.0)
            .or(existing.assignee_user_id)
    };

    let due_date = if payload.clear_due_date.unwrap_or(false) {
        None
    } else {
        payload
            .due_date
            .map(|value| value.naive_utc())
            .or(existing.due_date)
    };

    let milestone_id = if payload.clear_milestone.unwrap_or(false) {
        None
    } else {
        payload
            .milestone_id
            .map(|id| id.0)
            .or(existing.milestone_id)
    };

    if let Some(milestone_id) = milestone_id {
        validate_milestone_access_for_task(pool, actor, MilestoneId(milestone_id)).await?;
    }

    let archived = payload.archived.unwrap_or(existing.archived);
    let archive_changed = archived != existing.archived;
    let subtree_task_ids = if archive_changed {
        subtree_task_ids_for_root(pool, task_id).await?
    } else {
        Vec::new()
    };

    if archive_changed {
        for affected_task_id in &subtree_task_ids {
            if *affected_task_id == task_id {
                continue;
            }
            let _ =
                load_accessible_task(pool, actor, *affected_task_id, perm::task_update()).await?;
        }

        if !archived {
            let subtree_uuids: Vec<Uuid> = subtree_task_ids.iter().map(|id| id.0).collect();
            if !subtree_uuids.is_empty() {
                let rows = sqlx::query_as::<_, (Uuid,)>(
                    r#"
                    SELECT DISTINCT tp.project_id
                    FROM tasks.task_projects tp
                    JOIN tasks.projects p
                      ON p.id = tp.project_id
                    WHERE tp.task_id = ANY($1)
                      AND p.deleted_at IS NULL
                    ORDER BY tp.project_id ASC
                    "#,
                )
                .bind(subtree_uuids)
                .fetch_all(pool)
                .await
                .map_err(|err| {
                    db_err("Failed to query project IDs for unarchive validation", err)
                })?;

                for (project_id,) in rows {
                    validate_project_link_graph_integrity(pool, actor, ProjectId(project_id))
                        .await?;
                }
            }
        }
    }
    let metadata = payload
        .metadata
        .unwrap_or_else(|| existing.metadata.clone());

    sqlx::query(
        r#"
        UPDATE tasks.tasks
        SET
            title = $1,
            description = $2,
            assignee_user_id = $3,
            priority = $4,
            due_date = $5,
            milestone_id = $6,
            archived = $7,
            metadata = $8,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $9
          AND deleted_at IS NULL
        "#,
    )
    .bind(&title)
    .bind(&description)
    .bind(assignee_user_id)
    .bind(payload.priority.unwrap_or(existing.priority))
    .bind(due_date)
    .bind(milestone_id)
    .bind(archived)
    .bind(&metadata)
    .bind(task_id.0)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to update task", err))?;

    let mut changed_fields = serde_json::Map::new();
    if title != existing.title {
        changed_fields.insert("title".to_string(), json!(title));
    }
    if description != existing.description {
        changed_fields.insert("description".to_string(), json!(description));
    }
    if assignee_user_id != existing.assignee_user_id {
        changed_fields.insert("assigneeUserId".to_string(), json!(assignee_user_id));
    }
    if payload.priority.unwrap_or(existing.priority) != existing.priority {
        changed_fields.insert(
            "priority".to_string(),
            json!(payload.priority.unwrap_or(existing.priority)),
        );
    }
    if due_date != existing.due_date {
        changed_fields.insert("dueDate".to_string(), json!(due_date));
    }
    if milestone_id != existing.milestone_id {
        changed_fields.insert("milestoneId".to_string(), json!(milestone_id));
    }
    if archived != existing.archived {
        changed_fields.insert("archived".to_string(), json!(archived));
    }
    if metadata != existing.metadata {
        changed_fields.insert("metadata".to_string(), metadata);
    }

    append_task_log(
        pool,
        task_id,
        actor,
        "task_updated",
        None,
        None,
        json!({ "changedFields": changed_fields }),
    )
    .await?;

    if archive_changed {
        let subtree_uuids: Vec<Uuid> = subtree_task_ids.iter().map(|id| id.0).collect();
        sqlx::query(
            r#"
            UPDATE tasks.tasks
            SET
                archived = $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ANY($2)
              AND deleted_at IS NULL
            "#,
        )
        .bind(archived)
        .bind(subtree_uuids)
        .execute(pool)
        .await
        .map_err(|err| db_err("Failed to cascade archive state across subtree", err))?;

        let cascade_action = if archived {
            "task_archived_cascade"
        } else {
            "task_unarchived_cascade"
        };

        append_task_log(
            pool,
            task_id,
            actor,
            cascade_action,
            None,
            None,
            json!({
                "affectedTaskCount": subtree_task_ids.len() as i64,
            }),
        )
        .await?;

        for affected_task_id in subtree_task_ids {
            if affected_task_id == task_id {
                continue;
            }
            append_task_log(
                pool,
                affected_task_id,
                actor,
                cascade_action,
                None,
                None,
                json!({
                    "rootTaskId": task_id.0,
                    "archived": archived,
                }),
            )
            .await?;
        }
    }

    get_task_with_roles(pool, actor, task_id).await
}

pub async fn delete_task_with_roles(pool: &PgPool, actor: UserId, task_id: TaskId) -> Result<()> {
    let _ = load_accessible_task(pool, actor, task_id, perm::task_delete()).await?;
    let subtree_task_ids = subtree_task_ids_for_root(pool, task_id).await?;
    for affected_task_id in &subtree_task_ids {
        if *affected_task_id == task_id {
            continue;
        }
        let _ = load_accessible_task(pool, actor, *affected_task_id, perm::task_delete()).await?;
    }

    let subtree_uuids: Vec<Uuid> = subtree_task_ids.iter().map(|id| id.0).collect();
    let state_rows = sqlx::query_as::<_, (Uuid, String)>(
        r#"
        SELECT id, state
        FROM tasks.tasks
        WHERE id = ANY($1)
          AND deleted_at IS NULL
        "#,
    )
    .bind(&subtree_uuids)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to load task states before cascade delete", err))?;

    sqlx::query(
        r#"
        UPDATE tasks.tasks
        SET
            deleted_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ANY($1)
          AND deleted_at IS NULL
        "#,
    )
    .bind(subtree_uuids)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to cascade delete tasks", err))?;

    let project_rows = sqlx::query_as::<_, (Uuid,)>(
        r#"
        SELECT DISTINCT tp.project_id
        FROM tasks.task_projects tp
        WHERE tp.task_id = ANY($1)
        ORDER BY tp.project_id ASC
        "#,
    )
    .bind(subtree_task_ids.iter().map(|id| id.0).collect::<Vec<_>>())
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query project scopes for graph cleanup", err))?;

    let mut tx = pool
        .begin()
        .await
        .map_err(|err| db_err("Failed to start graph cleanup transaction", err))?;
    for (project_uuid,) in project_rows {
        let project_id = ProjectId(project_uuid);
        let bindings = load_project_link_graph_bindings(&mut *tx, project_id).await?;
        for affected_task_id in &subtree_task_ids {
            for link_type in TASK_LINK_TYPES {
                let Some(binding) = bindings.get(&link_type).copied() else {
                    continue;
                };
                let Some(node_id) = node_id_for_task_link_graph_tx(
                    &mut tx,
                    project_id,
                    link_type,
                    *affected_task_id,
                )
                .await?
                else {
                    continue;
                };

                graph_db::remove_node_tx(
                    &mut tx,
                    pool,
                    actor,
                    binding.graph_id,
                    subseq_graph::models::RemoveNodePayload {
                        node_id,
                        expected_updated_at: None,
                    },
                    graph_perm::graph_update_access_roles(),
                )
                .await
                .map_err(LibError::from)?;

                sqlx::query(
                    r#"
                    DELETE FROM tasks.task_link_graph_nodes
                    WHERE project_id = $1
                      AND link_type = $2
                      AND task_id = $3
                    "#,
                )
                .bind(project_id.0)
                .bind(link_type.as_db_value())
                .bind(affected_task_id.0)
                .execute(&mut *tx)
                .await
                .map_err(|err| db_err("Failed to delete task link graph node mapping", err))?;
            }
        }
    }
    tx.commit()
        .await
        .map_err(|err| db_err("Failed to commit graph cleanup transaction", err))?;

    for (affected_task_uuid, state_value) in state_rows {
        let affected_task_id = TaskId(affected_task_uuid);
        let from_state = task_state_from_row(&state_value)?;
        let action = if affected_task_id == task_id {
            "task_deleted"
        } else {
            "task_deleted_cascade"
        };
        append_task_log(
            pool,
            affected_task_id,
            actor,
            action,
            Some(from_state),
            None,
            json!({
                "rootTaskId": task_id.0,
                "affectedTaskCount": subtree_task_ids.len() as i64,
            }),
        )
        .await?;
    }

    Ok(())
}

async fn load_task_titles_for_ids_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task_ids: &[TaskId],
) -> Result<HashMap<TaskId, String>> {
    let ids = task_ids.iter().map(|id| id.0).collect::<Vec<_>>();
    let rows = sqlx::query_as::<_, (Uuid, String)>(
        r#"
        SELECT id, title
        FROM tasks.tasks
        WHERE id = ANY($1)
          AND deleted_at IS NULL
        "#,
    )
    .bind(ids)
    .fetch_all(&mut **tx)
    .await
    .map_err(|err| db_err("Failed to query task titles for graph nodes", err))?;

    let mut titles = HashMap::with_capacity(rows.len());
    for (id, title) in rows {
        titles.insert(TaskId(id), title);
    }
    Ok(titles)
}

pub async fn create_task_link_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    payload: CreateTaskLinkPayload,
) -> Result<TaskLink> {
    load_accessible_task(pool, actor, task_id, perm::task_link()).await?;
    load_accessible_task(pool, actor, payload.other_task_id, perm::task_link()).await?;

    if task_id == payload.other_task_id {
        return Err(LibError::invalid(
            "Tasks cannot link to themselves",
            anyhow!("attempted self-link for task {task_id}"),
        ));
    }

    let shared_projects =
        shared_project_ids_for_tasks(pool, task_id, payload.other_task_id).await?;
    if shared_projects.is_empty() {
        return Err(LibError::invalid(
            "Task links require both tasks to be in at least one shared project",
            anyhow!(
                "tasks {task_id} and {} do not share a project",
                payload.other_task_id
            ),
        ));
    }

    let subtask_parent_state =
        normalize_subtask_parent_state_for_link(payload.link_type, payload.subtask_parent_state)?;

    let mut project_bindings = HashMap::with_capacity(shared_projects.len());
    for project_id in &shared_projects {
        project_bindings.insert(
            *project_id,
            project_link_graph_bindings_for_actor(pool, actor, *project_id).await?,
        );
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|err| db_err("Failed to start transaction", err))?;

    lock_tasks_for_projects(&mut *tx, &shared_projects).await?;
    let task_titles =
        load_task_titles_for_ids_tx(&mut tx, &[task_id, payload.other_task_id]).await?;
    let task_title = task_titles.get(&task_id).ok_or_else(|| {
        LibError::not_found(
            "Task not found",
            anyhow!("task {} disappeared before link creation", task_id),
        )
    })?;
    let other_task_title = task_titles.get(&payload.other_task_id).ok_or_else(|| {
        LibError::not_found(
            "Task not found",
            anyhow!(
                "task {} disappeared before link creation",
                payload.other_task_id
            ),
        )
    })?;

    let mut created_at = None;
    for project_id in &shared_projects {
        let bindings = project_bindings.get(project_id).ok_or_else(|| {
            LibError::database(
                "Project link graph bindings are missing",
                anyhow!("missing link graph map for project {}", project_id),
            )
        })?;
        let binding = bindings.get(&payload.link_type).copied().ok_or_else(|| {
            LibError::database(
                "Project link graph binding is missing",
                anyhow!(
                    "missing link graph binding for project {} link_type {}",
                    project_id,
                    payload.link_type.as_db_value()
                ),
            )
        })?;

        let from_node_id =
            ensure_task_node_in_link_graph_tx(&mut tx, pool, actor, binding, task_id, task_title)
                .await?;
        let to_node_id = ensure_task_node_in_link_graph_tx(
            &mut tx,
            pool,
            actor,
            binding,
            payload.other_task_id,
            other_task_title,
        )
        .await?;

        if payload.link_type == TaskLinkType::SubtaskOf {
            graph_db::reparent_node_tx(
                &mut tx,
                pool,
                actor,
                binding.graph_id,
                ReparentNodePayload {
                    node_id: to_node_id,
                    new_parent_node_id: Some(from_node_id),
                    metadata: subtask_parent_state
                        .map(subtask_parent_edge_api_metadata)
                        .transpose()?,
                    expected_updated_at: None,
                },
                graph_perm::graph_update_access_roles(),
            )
            .await
            .map_err(LibError::from)?;
        } else {
            graph_db::upsert_edge_metadata_tx(
                &mut tx,
                pool,
                actor,
                binding.graph_id,
                UpsertEdgeMetadataPayload {
                    from_node_id,
                    to_node_id,
                    metadata: default_edge_api_metadata()?,
                    expected_updated_at: None,
                },
                graph_perm::graph_update_access_roles(),
            )
            .await
            .map_err(LibError::from)?;
        }

        if created_at.is_none() {
            created_at = edge_metadata_row_tx(&mut tx, binding.graph_id, from_node_id, to_node_id)
                .await?
                .map(|row| row.created_at);
        }
    }

    tx.commit()
        .await
        .map_err(|err| db_err("Failed to commit transaction", err))?;

    append_task_log(
        pool,
        task_id,
        actor,
        "task_link_created",
        None,
        None,
        json!({
            "otherTaskId": payload.other_task_id.0,
            "linkType": payload.link_type.as_db_value(),
            "subtaskParentState": subtask_parent_state.map(|state| state.as_str()),
            "sharedProjectCount": shared_projects.len() as i64,
        }),
    )
    .await?;

    Ok(TaskLink {
        task_from_id: task_id,
        task_to_id: payload.other_task_id,
        link_type: payload.link_type,
        subtask_parent_state,
        other_task_title: None,
        created_at: created_at.unwrap_or_else(|| Utc::now().naive_utc()),
    })
}

pub async fn delete_task_links_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    other_task_id: TaskId,
) -> Result<()> {
    load_accessible_task(pool, actor, task_id, perm::task_link()).await?;
    load_accessible_task(pool, actor, other_task_id, perm::task_link()).await?;

    let shared_projects = shared_project_ids_for_tasks(pool, task_id, other_task_id).await?;
    let mut project_bindings = HashMap::with_capacity(shared_projects.len());
    for project_id in &shared_projects {
        project_bindings.insert(
            *project_id,
            project_link_graph_bindings_for_actor(pool, actor, *project_id).await?,
        );
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|err| db_err("Failed to start transaction", err))?;

    if !shared_projects.is_empty() {
        lock_tasks_for_projects(&mut *tx, &shared_projects).await?;
    }

    let mut isolated_subtree = false;
    let mut rows_affected: i64 = 0;
    for project_id in &shared_projects {
        let bindings = project_bindings.get(project_id).ok_or_else(|| {
            LibError::database(
                "Project link graph bindings are missing",
                anyhow!("missing link graph map for project {}", project_id),
            )
        })?;

        if let Some(subtask_binding) = bindings.get(&TaskLinkType::SubtaskOf).copied() {
            let root_node_id = subtask_binding.root_node_id.ok_or_else(|| {
                LibError::database(
                    "Subtask graph root node is missing",
                    anyhow!("project {} subtask graph has no root node", project_id),
                )
            })?;
            let task_node_id = node_id_for_task_link_graph_tx(
                &mut tx,
                *project_id,
                TaskLinkType::SubtaskOf,
                task_id,
            )
            .await?;
            let other_node_id = node_id_for_task_link_graph_tx(
                &mut tx,
                *project_id,
                TaskLinkType::SubtaskOf,
                other_task_id,
            )
            .await?;

            if let (Some(task_node_id), Some(other_node_id)) = (task_node_id, other_node_id) {
                if edge_exists_tx(
                    &mut tx,
                    subtask_binding.graph_id,
                    task_node_id,
                    other_node_id,
                )
                .await?
                {
                    isolated_subtree = true;
                    rows_affected += 1;
                    graph_db::reparent_node_tx(
                        &mut tx,
                        pool,
                        actor,
                        subtask_binding.graph_id,
                        ReparentNodePayload {
                            node_id: other_node_id,
                            new_parent_node_id: Some(root_node_id),
                            metadata: Some(synthetic_root_edge_api_metadata()?),
                            expected_updated_at: None,
                        },
                        graph_perm::graph_update_access_roles(),
                    )
                    .await
                    .map_err(LibError::from)?;
                }

                if edge_exists_tx(
                    &mut tx,
                    subtask_binding.graph_id,
                    other_node_id,
                    task_node_id,
                )
                .await?
                {
                    isolated_subtree = true;
                    rows_affected += 1;
                    graph_db::reparent_node_tx(
                        &mut tx,
                        pool,
                        actor,
                        subtask_binding.graph_id,
                        ReparentNodePayload {
                            node_id: task_node_id,
                            new_parent_node_id: Some(root_node_id),
                            metadata: Some(synthetic_root_edge_api_metadata()?),
                            expected_updated_at: None,
                        },
                        graph_perm::graph_update_access_roles(),
                    )
                    .await
                    .map_err(LibError::from)?;
                }
            }
        }

        for link_type in [
            TaskLinkType::DependsOn,
            TaskLinkType::RelatedTo,
            TaskLinkType::AssignmentOrder,
        ] {
            let Some(binding) = bindings.get(&link_type).copied() else {
                continue;
            };
            let task_node_id =
                node_id_for_task_link_graph_tx(&mut tx, *project_id, link_type, task_id).await?;
            let other_node_id =
                node_id_for_task_link_graph_tx(&mut tx, *project_id, link_type, other_task_id)
                    .await?;
            let (Some(task_node_id), Some(other_node_id)) = (task_node_id, other_node_id) else {
                continue;
            };

            if edge_exists_tx(&mut tx, binding.graph_id, task_node_id, other_node_id).await? {
                rows_affected += 1;
                graph_db::remove_edge_tx(
                    &mut tx,
                    pool,
                    actor,
                    binding.graph_id,
                    RemoveEdgePayload {
                        from_node_id: task_node_id,
                        to_node_id: other_node_id,
                        expected_updated_at: None,
                    },
                    graph_perm::graph_update_access_roles(),
                )
                .await
                .map_err(LibError::from)?;
            }

            if edge_exists_tx(&mut tx, binding.graph_id, other_node_id, task_node_id).await? {
                rows_affected += 1;
                graph_db::remove_edge_tx(
                    &mut tx,
                    pool,
                    actor,
                    binding.graph_id,
                    RemoveEdgePayload {
                        from_node_id: other_node_id,
                        to_node_id: task_node_id,
                        expected_updated_at: None,
                    },
                    graph_perm::graph_update_access_roles(),
                )
                .await
                .map_err(LibError::from)?;
            }
        }
    }

    tx.commit()
        .await
        .map_err(|err| db_err("Failed to commit transaction", err))?;

    append_task_log(
        pool,
        task_id,
        actor,
        "task_links_deleted",
        None,
        None,
        json!({
            "otherTaskId": other_task_id.0,
            "rowsAffected": rows_affected,
            "isolatedSubtree": isolated_subtree,
            "sharedProjectCount": shared_projects.len() as i64,
        }),
    )
    .await?;

    Ok(())
}

pub async fn create_task_comment_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    payload: CreateTaskCommentPayload,
) -> Result<TaskComment> {
    load_accessible_task(pool, actor, task_id, perm::task_update()).await?;

    let body = payload.body.trim().to_string();
    if body.is_empty() {
        return Err(LibError::invalid(
            "Comment body is required",
            anyhow!("empty task comment"),
        ));
    }

    let metadata = payload.metadata.unwrap_or_else(|| json!({}));
    let comment = insert_task_comment(pool, task_id, actor, body, metadata).await?;

    append_task_log(
        pool,
        task_id,
        actor,
        "task_comment_created",
        None,
        None,
        json!({ "commentId": comment.id.0 }),
    )
    .await?;

    Ok(comment)
}

pub async fn list_task_comments_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
) -> Result<Vec<TaskComment>> {
    load_accessible_task(pool, actor, task_id, perm::task_read()).await?;
    get_task_comments(pool, task_id, TASK_DETAILS_LIMIT).await
}

pub async fn get_task_log_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
) -> Result<Vec<TaskLogEntry>> {
    load_accessible_task(pool, actor, task_id, perm::task_read()).await?;
    get_task_log_entries(pool, task_id, TASK_DETAILS_LIMIT).await
}

pub async fn task_cascade_impact_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    query: TaskCascadeImpactQuery,
) -> Result<TaskCascadeImpact> {
    let required_permission = match query.operation {
        TaskCascadeOperation::Archive | TaskCascadeOperation::Unarchive => perm::task_update(),
        TaskCascadeOperation::Delete => perm::task_delete(),
    };
    let _ = load_accessible_task(pool, actor, task_id, required_permission).await?;

    let subtree_task_ids = subtree_task_ids_for_root(pool, task_id).await?;
    for affected_task_id in &subtree_task_ids {
        if *affected_task_id == task_id {
            continue;
        }
        let _ = load_accessible_task(pool, actor, *affected_task_id, required_permission).await?;
    }

    let subtree_uuids: Vec<Uuid> = subtree_task_ids.iter().map(|id| id.0).collect();
    let rows = sqlx::query_as::<_, (Uuid, bool, Option<chrono::NaiveDateTime>)>(
        r#"
        SELECT id, archived, deleted_at
        FROM tasks.tasks
        WHERE id = ANY($1)
        "#,
    )
    .bind(subtree_uuids)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query subtree impact rows", err))?;

    let affected_task_count = match query.operation {
        TaskCascadeOperation::Delete => rows
            .iter()
            .filter(|(_, _, deleted_at)| deleted_at.is_none())
            .count(),
        TaskCascadeOperation::Archive => rows
            .iter()
            .filter(|(_, archived, deleted_at)| deleted_at.is_none() && !*archived)
            .count(),
        TaskCascadeOperation::Unarchive => rows
            .iter()
            .filter(|(_, archived, deleted_at)| deleted_at.is_none() && *archived)
            .count(),
    } as i64;

    Ok(TaskCascadeImpact {
        task_id,
        operation: query.operation,
        affected_task_count,
    })
}

pub async fn transition_task_with_roles(
    pool: &PgPool,
    actor: UserId,
    task_id: TaskId,
    payload: TransitionTaskPayload,
) -> Result<TaskDetails> {
    let existing = load_accessible_task(pool, actor, task_id, perm::task_transition()).await?;
    let from_state = task_state_from_row(&existing.state)?;
    let to_state = payload.to_state;

    if !transition_allowed(from_state, to_state) {
        return Err(LibError::invalid(
            "Transition is not allowed",
            anyhow!(
                "invalid transition for task {task_id}: {} -> {}",
                from_state.as_str(),
                to_state.as_str()
            ),
        ));
    }

    let assignee_user_id = resolve_transition_assignee(
        from_state,
        to_state,
        &payload,
        actor,
        existing.assignee_user_id,
    )?;

    let (completed_by_user_id, completed_at) = if to_state == TaskState::Done {
        let done_by = payload.done_by_user_id.unwrap_or(actor);
        let done_at = payload.done_at.unwrap_or_else(Utc::now).naive_utc();
        (Some(done_by.0), Some(done_at))
    } else {
        (None, None)
    };

    let rejected_reason = if to_state == TaskState::Rejected {
        normalize_free_text(payload.rejected_reason.clone())
    } else {
        None
    };

    sqlx::query(
        r#"
        UPDATE tasks.tasks
        SET
            state = $1,
            assignee_user_id = $2,
            completed_by_user_id = $3,
            completed_at = $4,
            rejected_reason = $5,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $6
          AND deleted_at IS NULL
        "#,
    )
    .bind(to_state.as_str())
    .bind(assignee_user_id)
    .bind(completed_by_user_id)
    .bind(completed_at)
    .bind(rejected_reason.clone())
    .bind(task_id.0)
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to transition task state", err))?;

    let transition_comment = build_transition_comment(from_state, to_state, &payload);
    let mut transition_comment_id: Option<Uuid> = None;
    if let Some(body) = transition_comment {
        let comment = insert_task_comment(pool, task_id, actor, body, json!({})).await?;
        transition_comment_id = Some(comment.id.0);
    }

    let mut details = transition_details(
        from_state,
        to_state,
        &payload,
        assignee_user_id,
        completed_by_user_id,
        completed_at,
        rejected_reason.as_deref(),
    );
    if let Some(comment_id) = transition_comment_id
        && let Some(details_obj) = details.as_object_mut()
    {
        details_obj.insert("commentId".to_string(), json!(comment_id));
    }

    append_task_log(
        pool,
        task_id,
        actor,
        "task_transitioned",
        Some(from_state),
        Some(to_state),
        details,
    )
    .await?;

    get_task_with_roles(pool, actor, task_id).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result as AnyResult;
    use serde_json::json;
    use sqlx::PgPool;
    use std::future::Future;
    use subseq_auth::db::{GroupMembershipRow, GroupRoleRow, GroupRow, UserRoleRow, UserRow};
    use subseq_graph::db as graph_db;
    use subseq_graph::models::{CreateGraphPayload as GraphCreatePayload, NewGraphNode};
    use subseq_graph::permissions as graph_perm;

    #[derive(Clone, Copy)]
    struct Actors {
        owner: UserId,
        member: UserId,
        outsider: UserId,
        group_id: GroupId,
    }

    #[derive(Clone)]
    struct Fixture {
        actors: Actors,
        project: Project,
        seed_task_id: TaskId,
    }

    fn map_tasks_lib<T>(result: Result<T>) -> AnyResult<T> {
        result.map_err(|err| anyhow::anyhow!("{}: {}", err.public, err.source))
    }

    fn map_graph_lib<T>(
        result: std::result::Result<T, subseq_graph::error::LibError>,
    ) -> AnyResult<T> {
        result.map_err(|err| anyhow::anyhow!("{}: {}", err.public, err.source))
    }

    async fn with_test_db<F, Fut>(run: F) -> AnyResult<()>
    where
        F: FnOnce(PgPool) -> Fut,
        Fut: Future<Output = AnyResult<()>>,
    {
        let test_db = crate::test_harness::TestDb::new().await?;
        test_db.prepare().await?;

        let pool = test_db.pool.clone();
        let run_result = run(pool).await;
        let teardown_result = test_db.teardown().await;

        teardown_result?;
        run_result
    }

    fn transition_payload(to_state: TaskState) -> TransitionTaskPayload {
        TransitionTaskPayload {
            to_state,
            when: None,
            assigned_to_user_id: None,
            deferral_reason: None,
            cant_do_reason: None,
            estimated_time_to_complete: None,
            work_log_details: None,
            feedback: None,
            done_by_user_id: None,
            done_at: None,
            rejected_reason: None,
            comment: None,
        }
    }

    fn create_task_payload(project_id: ProjectId, title: &str) -> CreateTaskPayload {
        CreateTaskPayload {
            project_id,
            title: title.to_string(),
            description: Some(format!("Description for {title}")),
            assignee_user_id: None,
            priority: Some(0),
            due_date: None,
            milestone_id: None,
            state: None,
            metadata: Some(json!({})),
        }
    }

    fn update_title_payload(title: &str) -> UpdateTaskPayload {
        UpdateTaskPayload {
            title: Some(title.to_string()),
            description: None,
            assignee_user_id: None,
            clear_assignee: None,
            priority: None,
            due_date: None,
            clear_due_date: None,
            milestone_id: None,
            clear_milestone: None,
            state: None,
            archived: None,
            metadata: None,
        }
    }

    fn archive_payload(archived: bool) -> UpdateTaskPayload {
        UpdateTaskPayload {
            title: None,
            description: None,
            assignee_user_id: None,
            clear_assignee: None,
            priority: None,
            due_date: None,
            clear_due_date: None,
            milestone_id: None,
            clear_milestone: None,
            state: None,
            archived: Some(archived),
            metadata: None,
        }
    }

    async fn insert_user(pool: &PgPool, user_id: UserId, username: &str) -> AnyResult<()> {
        let user = UserRow::new(
            user_id,
            Some(username.to_string()),
            format!("{username}-{}@example.com", user_id.0),
            Some(json!({ "displayName": username })),
        );
        UserRow::insert(pool, &user).await?;
        Ok(())
    }

    async fn setup_actors(pool: &PgPool) -> AnyResult<Actors> {
        let owner = UserId(Uuid::new_v4());
        let member = UserId(Uuid::new_v4());
        let outsider = UserId(Uuid::new_v4());
        let group_id = GroupId(Uuid::new_v4());

        insert_user(pool, owner, "owner").await?;
        insert_user(pool, member, "member").await?;
        insert_user(pool, outsider, "outsider").await?;

        GroupRow::insert(
            pool,
            &GroupRow::new(
                group_id.0,
                Some(json!({ "fixture": true })),
                &format!("group-{}", group_id.0),
            ),
        )
        .await?;

        GroupMembershipRow::add_member(pool, &GroupMembershipRow::new(group_id, owner, "owner"))
            .await?;
        GroupMembershipRow::add_member(pool, &GroupMembershipRow::new(group_id, member, "member"))
            .await?;

        Ok(Actors {
            owner,
            member,
            outsider,
            group_id,
        })
    }

    async fn clear_group_roles(pool: &PgPool, group_id: GroupId) -> AnyResult<()> {
        sqlx::query(
            r#"
            DELETE FROM auth.group_roles
            WHERE group_id = $1
            "#,
        )
        .bind(group_id.0)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn grant_group_role(
        pool: &PgPool,
        group_id: GroupId,
        scope: &str,
        scope_id: &str,
        role_name: &str,
    ) -> AnyResult<()> {
        GroupRoleRow::allow(
            pool,
            &GroupRoleRow::new(group_id, scope, scope_id, role_name),
        )
        .await?;
        Ok(())
    }

    async fn grant_user_graph_role(
        pool: &PgPool,
        user_id: UserId,
        group_id: GroupId,
        role_name: &str,
    ) -> AnyResult<()> {
        let scope_id = graph_perm::graph_role_scope_id_for_group(group_id);
        UserRoleRow::allow(
            pool,
            &UserRoleRow::new(
                user_id,
                graph_perm::graph_role_scope(),
                &scope_id,
                role_name,
            ),
        )
        .await?;
        Ok(())
    }

    async fn create_graph_for_group(
        pool: &PgPool,
        actor: UserId,
        group_id: GroupId,
        name: &str,
    ) -> AnyResult<GraphId> {
        let graph = map_graph_lib(
            graph_db::create_graph(
                pool,
                actor,
                GraphCreatePayload {
                    kind: GraphKind::Directed,
                    name: name.to_string(),
                    description: Some("fixture".to_string()),
                    metadata: Some(json!({ "fixture": true })),
                    owner_group_id: Some(group_id),
                    nodes: vec![NewGraphNode {
                        id: None,
                        label: "entry".to_string(),
                        metadata: Some(json!({})),
                    }],
                    edges: vec![],
                },
                graph_perm::graph_create_access_roles(),
            )
            .await,
        )?;
        Ok(graph.id)
    }

    async fn setup_fixture(pool: &PgPool) -> AnyResult<Fixture> {
        let actors = setup_actors(pool).await?;

        grant_user_graph_role(
            pool,
            actors.owner,
            actors.group_id,
            graph_perm::graph_create_role(),
        )
        .await?;
        grant_user_graph_role(
            pool,
            actors.owner,
            actors.group_id,
            graph_perm::graph_read_role(),
        )
        .await?;
        grant_user_graph_role(
            pool,
            actors.owner,
            actors.group_id,
            graph_perm::graph_update_role(),
        )
        .await?;

        clear_group_roles(pool, actors.group_id).await?;
        for role in [
            perm::project_create(),
            perm::project_read(),
            perm::task_create(),
            perm::task_link(),
            perm::task_update(),
            perm::task_delete(),
            perm::task_transition(),
        ] {
            grant_group_role(
                pool,
                actors.group_id,
                perm::scope_tasks(),
                perm::scope_id_global(),
                role,
            )
            .await?;
        }

        let state_graph_id =
            create_graph_for_group(pool, actors.owner, actors.group_id, "task-state").await?;
        let task_graph_id =
            create_graph_for_group(pool, actors.owner, actors.group_id, "task-flow").await?;

        let project = map_tasks_lib(
            create_project_with_roles(
                pool,
                actors.owner,
                CreateProjectPayload {
                    name: "Fixture Project".to_string(),
                    description: Some("Fixture project".to_string()),
                    owner_group_id: Some(actors.group_id),
                    task_state_graph_id: state_graph_id,
                    task_graph_id: Some(task_graph_id),
                    metadata: Some(json!({ "fixture": true })),
                },
            )
            .await,
        )?;

        let seed_task = map_tasks_lib(
            create_task_with_roles(
                pool,
                actors.owner,
                create_task_payload(project.id, "Seed task"),
            )
            .await,
        )?;

        Ok(Fixture {
            actors,
            project,
            seed_task_id: seed_task.task.id,
        })
    }

    #[test]
    fn transition_allowed_supports_expected_edges() {
        let allowed = [
            (TaskState::Open, TaskState::Todo),
            (TaskState::Todo, TaskState::Assigned),
            (TaskState::Todo, TaskState::Open),
            (TaskState::Todo, TaskState::InProgress),
            (TaskState::Assigned, TaskState::InProgress),
            (TaskState::Assigned, TaskState::Todo),
            (TaskState::InProgress, TaskState::Todo),
            (TaskState::InProgress, TaskState::Acceptance),
            (TaskState::Acceptance, TaskState::InProgress),
        ];

        for (from_state, to_state) in allowed {
            assert!(
                transition_allowed(from_state, to_state),
                "{} -> {} should be allowed",
                from_state.as_str(),
                to_state.as_str()
            );
        }

        for from_state in [
            TaskState::Open,
            TaskState::Todo,
            TaskState::Assigned,
            TaskState::InProgress,
            TaskState::Acceptance,
        ] {
            assert!(
                transition_allowed(from_state, TaskState::Done),
                "{} -> done should be allowed",
                from_state.as_str()
            );
            assert!(
                transition_allowed(from_state, TaskState::Rejected),
                "{} -> rejected should be allowed",
                from_state.as_str()
            );
        }
    }

    #[test]
    fn transition_allowed_rejects_invalid_edges() {
        let disallowed = [
            (TaskState::Open, TaskState::Open),
            (TaskState::Open, TaskState::Assigned),
            (TaskState::Open, TaskState::InProgress),
            (TaskState::Open, TaskState::Acceptance),
            (TaskState::Todo, TaskState::Acceptance),
            (TaskState::Assigned, TaskState::Acceptance),
            (TaskState::InProgress, TaskState::Assigned),
            (TaskState::Acceptance, TaskState::Todo),
            (TaskState::Done, TaskState::Todo),
            (TaskState::Rejected, TaskState::Todo),
            (TaskState::Done, TaskState::Rejected),
            (TaskState::Rejected, TaskState::Done),
        ];

        for (from_state, to_state) in disallowed {
            assert!(
                !transition_allowed(from_state, to_state),
                "{} -> {} should be rejected",
                from_state.as_str(),
                to_state.as_str()
            );
        }
    }

    #[test]
    fn normalize_subtask_parent_state_enforces_link_rules() {
        let subtask_state =
            normalize_subtask_parent_state_for_link(TaskLinkType::SubtaskOf, Some(TaskState::Todo))
                .expect("subtask state should be accepted");
        assert_eq!(subtask_state, Some(TaskState::Todo));

        let missing_state = normalize_subtask_parent_state_for_link(TaskLinkType::SubtaskOf, None)
            .expect_err("subtask links require state");
        assert_eq!(missing_state.public, "Subtask links require a parent state");

        let invalid_non_subtask =
            normalize_subtask_parent_state_for_link(TaskLinkType::DependsOn, Some(TaskState::Todo))
                .expect_err("non-subtask links should reject subtaskParentState");
        assert_eq!(
            invalid_non_subtask.public,
            "Only subtask links may set subtaskParentState"
        );

        let no_state = normalize_subtask_parent_state_for_link(TaskLinkType::DependsOn, None)
            .expect("depends_on should not require subtask state");
        assert_eq!(no_state, None);
    }

    #[test]
    fn resolve_transition_assignee_applies_transition_rules() {
        let actor = UserId(Uuid::new_v4());
        let existing_assignee = Some(Uuid::new_v4());

        let missing_assignee = resolve_transition_assignee(
            TaskState::Todo,
            TaskState::Assigned,
            &transition_payload(TaskState::Assigned),
            actor,
            existing_assignee,
        )
        .expect_err("todo -> assigned requires assigned_to_user_id");
        assert_eq!(
            missing_assignee.public,
            "assignedToUserId is required for this transition"
        );

        let assigned_user = UserId(Uuid::new_v4());
        let mut assigned_payload = transition_payload(TaskState::Assigned);
        assigned_payload.assigned_to_user_id = Some(assigned_user);
        let resolved = resolve_transition_assignee(
            TaskState::Todo,
            TaskState::Assigned,
            &assigned_payload,
            actor,
            existing_assignee,
        )
        .expect("todo -> assigned should resolve assignee");
        assert_eq!(resolved, Some(assigned_user.0));

        let in_progress = resolve_transition_assignee(
            TaskState::Todo,
            TaskState::InProgress,
            &transition_payload(TaskState::InProgress),
            actor,
            existing_assignee,
        )
        .expect("todo -> in_progress should self-assign");
        assert_eq!(in_progress, Some(actor.0));

        let preserved = resolve_transition_assignee(
            TaskState::Assigned,
            TaskState::Todo,
            &transition_payload(TaskState::Todo),
            actor,
            existing_assignee,
        )
        .expect("assigned -> todo should preserve existing assignee");
        assert_eq!(preserved, existing_assignee);
    }

    #[tokio::test]
    async fn secured_task_actions_enforce_owner_group_and_unauthorized_boundaries() -> AnyResult<()>
    {
        with_test_db(|pool| async move {
            let fixture = setup_fixture(&pool).await?;
            let Actors {
                owner,
                member,
                outsider,
                group_id,
            } = fixture.actors;
            let project_scope_id = fixture.project.id.0.to_string();

            grant_user_graph_role(&pool, member, group_id, graph_perm::graph_read_role()).await?;
            grant_user_graph_role(&pool, member, group_id, graph_perm::graph_update_role()).await?;

            clear_group_roles(&pool, group_id).await?;
            for role in [
                perm::task_create(),
                perm::task_update(),
                perm::task_delete(),
                perm::task_link(),
                perm::task_transition(),
            ] {
                grant_group_role(
                    &pool,
                    group_id,
                    perm::scope_project(),
                    &project_scope_id,
                    role,
                )
                .await?;
            }

            let _ = map_tasks_lib(get_task_with_roles(&pool, member, fixture.seed_task_id).await)?;

            let member_task = map_tasks_lib(
                create_task_with_roles(
                    &pool,
                    member,
                    create_task_payload(fixture.project.id, "Member task"),
                )
                .await,
            )?;

            let updated = map_tasks_lib(
                update_task_with_roles(
                    &pool,
                    member,
                    member_task.task.id,
                    update_title_payload("Member task updated"),
                )
                .await,
            )?;
            assert_eq!(updated.task.title, "Member task updated");

            let transitioned = map_tasks_lib(
                transition_task_with_roles(
                    &pool,
                    member,
                    member_task.task.id,
                    transition_payload(TaskState::Todo),
                )
                .await,
            )?;
            assert_eq!(transitioned.task.state, TaskState::Todo);

            let link = map_tasks_lib(
                create_task_link_with_roles(
                    &pool,
                    member,
                    fixture.seed_task_id,
                    CreateTaskLinkPayload {
                        other_task_id: member_task.task.id,
                        link_type: TaskLinkType::RelatedTo,
                        subtask_parent_state: None,
                    },
                )
                .await,
            )?;
            assert_eq!(link.link_type, TaskLinkType::RelatedTo);

            let owner_aux = map_tasks_lib(
                create_task_with_roles(
                    &pool,
                    owner,
                    create_task_payload(fixture.project.id, "Owner aux"),
                )
                .await,
            )?;

            map_tasks_lib(delete_task_with_roles(&pool, member, member_task.task.id).await)?;

            let outsider_read_err = get_task_with_roles(&pool, outsider, fixture.seed_task_id)
                .await
                .expect_err("outsider should not read group task");
            assert_eq!(outsider_read_err.kind, ErrorKind::Forbidden);

            let outsider_create_err = create_task_with_roles(
                &pool,
                outsider,
                create_task_payload(fixture.project.id, "Outsider task"),
            )
            .await
            .expect_err("outsider should not create group task");
            assert_eq!(outsider_create_err.kind, ErrorKind::Forbidden);

            let outsider_update_err = update_task_with_roles(
                &pool,
                outsider,
                fixture.seed_task_id,
                update_title_payload("nope"),
            )
            .await
            .expect_err("outsider should not update group task");
            assert_eq!(outsider_update_err.kind, ErrorKind::Forbidden);

            let outsider_transition_err = transition_task_with_roles(
                &pool,
                outsider,
                fixture.seed_task_id,
                transition_payload(TaskState::Todo),
            )
            .await
            .expect_err("outsider should not transition group task");
            assert_eq!(outsider_transition_err.kind, ErrorKind::Forbidden);

            let outsider_link_err = create_task_link_with_roles(
                &pool,
                outsider,
                fixture.seed_task_id,
                CreateTaskLinkPayload {
                    other_task_id: owner_aux.task.id,
                    link_type: TaskLinkType::RelatedTo,
                    subtask_parent_state: None,
                },
            )
            .await
            .expect_err("outsider should not link group tasks");
            assert_eq!(outsider_link_err.kind, ErrorKind::Forbidden);

            let outsider_delete_err = delete_task_with_roles(&pool, outsider, fixture.seed_task_id)
                .await
                .expect_err("outsider should not delete group task");
            assert_eq!(outsider_delete_err.kind, ErrorKind::Forbidden);

            clear_group_roles(&pool, group_id).await?;

            let _ = map_tasks_lib(get_task_with_roles(&pool, owner, fixture.seed_task_id).await)?;

            let owner_updated = map_tasks_lib(
                update_task_with_roles(
                    &pool,
                    owner,
                    fixture.seed_task_id,
                    update_title_payload("Owner override title"),
                )
                .await,
            )?;
            assert_eq!(owner_updated.task.title, "Owner override title");

            let owner_transitioned = map_tasks_lib(
                transition_task_with_roles(
                    &pool,
                    owner,
                    fixture.seed_task_id,
                    transition_payload(TaskState::Todo),
                )
                .await,
            )?;
            assert_eq!(owner_transitioned.task.state, TaskState::Todo);

            map_tasks_lib(delete_task_with_roles(&pool, owner, owner_aux.task.id).await)?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn scoped_write_roles_imply_read_for_tasks_and_projects() -> AnyResult<()> {
        with_test_db(|pool| async move {
            let fixture = setup_fixture(&pool).await?;
            let Actors {
                member, group_id, ..
            } = fixture.actors;
            let project_scope_id = fixture.project.id.0.to_string();
            let task_scope_id = fixture.seed_task_id.0.to_string();

            grant_user_graph_role(&pool, member, group_id, graph_perm::graph_read_role()).await?;

            clear_group_roles(&pool, group_id).await?;
            grant_group_role(
                &pool,
                group_id,
                perm::scope_project(),
                &project_scope_id,
                perm::task_update(),
            )
            .await?;
            let _ = map_tasks_lib(get_task_with_roles(&pool, member, fixture.seed_task_id).await)?;

            clear_group_roles(&pool, group_id).await?;
            grant_group_role(
                &pool,
                group_id,
                perm::scope_task(),
                &task_scope_id,
                perm::task_update(),
            )
            .await?;
            let _ = map_tasks_lib(get_task_with_roles(&pool, member, fixture.seed_task_id).await)?;

            clear_group_roles(&pool, group_id).await?;
            grant_group_role(
                &pool,
                group_id,
                perm::scope_tasks(),
                perm::scope_id_global(),
                perm::task_update(),
            )
            .await?;
            let _ = map_tasks_lib(get_task_with_roles(&pool, member, fixture.seed_task_id).await)?;

            clear_group_roles(&pool, group_id).await?;
            grant_group_role(
                &pool,
                group_id,
                perm::scope_project(),
                &project_scope_id,
                perm::project_update(),
            )
            .await?;
            let _ = map_tasks_lib(get_project_with_roles(&pool, member, fixture.project.id).await)?;

            clear_group_roles(&pool, group_id).await?;
            let project_err = get_project_with_roles(&pool, member, fixture.project.id)
                .await
                .expect_err("member without project role should not read project");
            assert_eq!(project_err.kind, ErrorKind::Forbidden);

            let task_err = get_task_with_roles(&pool, member, fixture.seed_task_id)
                .await
                .expect_err("member without task roles should not read task");
            assert_eq!(task_err.kind, ErrorKind::Forbidden);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn project_and_task_reads_require_graph_access() -> AnyResult<()> {
        with_test_db(|pool| async move {
            let fixture = setup_fixture(&pool).await?;
            let Actors {
                member, group_id, ..
            } = fixture.actors;
            let project_scope_id = fixture.project.id.0.to_string();

            clear_group_roles(&pool, group_id).await?;
            grant_group_role(
                &pool,
                group_id,
                perm::scope_project(),
                &project_scope_id,
                perm::project_read(),
            )
            .await?;
            grant_group_role(
                &pool,
                group_id,
                perm::scope_project(),
                &project_scope_id,
                perm::task_read(),
            )
            .await?;

            let project_err = get_project_with_roles(&pool, member, fixture.project.id)
                .await
                .expect_err("project read should require graph read permission");
            assert_eq!(project_err.kind, ErrorKind::Forbidden);

            let task_err = get_task_with_roles(&pool, member, fixture.seed_task_id)
                .await
                .expect_err("task read should require graph read permission");
            assert_eq!(task_err.kind, ErrorKind::Forbidden);

            grant_user_graph_role(&pool, member, group_id, graph_perm::graph_read_role()).await?;

            let _ = map_tasks_lib(get_project_with_roles(&pool, member, fixture.project.id).await)?;
            let _ = map_tasks_lib(get_task_with_roles(&pool, member, fixture.seed_task_id).await)?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn transitions_append_comment_and_log_context() -> AnyResult<()> {
        with_test_db(|pool| async move {
            let fixture = setup_fixture(&pool).await?;
            let owner = fixture.actors.owner;

            let mut payload = transition_payload(TaskState::Todo);
            payload.when = Some(Utc::now());
            payload.comment = Some("Kickoff".to_string());

            let details = map_tasks_lib(
                transition_task_with_roles(&pool, owner, fixture.seed_task_id, payload).await,
            )?;
            assert_eq!(details.task.state, TaskState::Todo);
            assert_eq!(details.comments.len(), 1);
            let transition_comment = details
                .comments
                .last()
                .expect("transition should create one comment");
            assert!(transition_comment.body.contains("Kickoff"));
            assert!(transition_comment.body.contains("Moved to todo at"));

            let log_entries =
                map_tasks_lib(get_task_log_with_roles(&pool, owner, fixture.seed_task_id).await)?;
            let transition_entry = log_entries
                .iter()
                .find(|entry| entry.action == "task_transitioned")
                .expect("transition log entry should exist");
            assert_eq!(transition_entry.from_state, Some(TaskState::Open));
            assert_eq!(transition_entry.to_state, Some(TaskState::Todo));
            assert_eq!(
                transition_entry.details.get("commentId"),
                Some(&json!(transition_comment.id.0))
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn subtree_cascade_impact_matches_archive_unarchive_delete() -> AnyResult<()> {
        with_test_db(|pool| async move {
            let fixture = setup_fixture(&pool).await?;
            let owner = fixture.actors.owner;
            let root_task_id = fixture.seed_task_id;

            let child_task = map_tasks_lib(
                create_task_with_roles(
                    &pool,
                    owner,
                    create_task_payload(fixture.project.id, "Child task"),
                )
                .await,
            )?;
            let grandchild_task = map_tasks_lib(
                create_task_with_roles(
                    &pool,
                    owner,
                    create_task_payload(fixture.project.id, "Grandchild task"),
                )
                .await,
            )?;

            let _ = map_tasks_lib(
                create_task_link_with_roles(
                    &pool,
                    owner,
                    root_task_id,
                    CreateTaskLinkPayload {
                        other_task_id: child_task.task.id,
                        link_type: TaskLinkType::SubtaskOf,
                        subtask_parent_state: Some(TaskState::Todo),
                    },
                )
                .await,
            )?;
            let _ = map_tasks_lib(
                create_task_link_with_roles(
                    &pool,
                    owner,
                    child_task.task.id,
                    CreateTaskLinkPayload {
                        other_task_id: grandchild_task.task.id,
                        link_type: TaskLinkType::SubtaskOf,
                        subtask_parent_state: Some(TaskState::Todo),
                    },
                )
                .await,
            )?;

            let archive_impact = map_tasks_lib(
                task_cascade_impact_with_roles(
                    &pool,
                    owner,
                    root_task_id,
                    TaskCascadeImpactQuery {
                        operation: TaskCascadeOperation::Archive,
                    },
                )
                .await,
            )?;
            assert_eq!(archive_impact.affected_task_count, 3);

            let _ = map_tasks_lib(
                update_task_with_roles(&pool, owner, root_task_id, archive_payload(true)).await,
            )?;
            assert!(
                map_tasks_lib(get_task_with_roles(&pool, owner, root_task_id).await)?
                    .task
                    .archived
            );
            assert!(
                map_tasks_lib(get_task_with_roles(&pool, owner, child_task.task.id).await)?
                    .task
                    .archived
            );
            assert!(
                map_tasks_lib(get_task_with_roles(&pool, owner, grandchild_task.task.id).await)?
                    .task
                    .archived
            );

            let archive_again = map_tasks_lib(
                task_cascade_impact_with_roles(
                    &pool,
                    owner,
                    root_task_id,
                    TaskCascadeImpactQuery {
                        operation: TaskCascadeOperation::Archive,
                    },
                )
                .await,
            )?;
            assert_eq!(archive_again.affected_task_count, 0);

            let unarchive_impact = map_tasks_lib(
                task_cascade_impact_with_roles(
                    &pool,
                    owner,
                    root_task_id,
                    TaskCascadeImpactQuery {
                        operation: TaskCascadeOperation::Unarchive,
                    },
                )
                .await,
            )?;
            assert_eq!(unarchive_impact.affected_task_count, 3);

            let _ = map_tasks_lib(
                update_task_with_roles(&pool, owner, root_task_id, archive_payload(false)).await,
            )?;
            assert!(
                !map_tasks_lib(get_task_with_roles(&pool, owner, root_task_id).await)?
                    .task
                    .archived
            );

            let delete_impact = map_tasks_lib(
                task_cascade_impact_with_roles(
                    &pool,
                    owner,
                    root_task_id,
                    TaskCascadeImpactQuery {
                        operation: TaskCascadeOperation::Delete,
                    },
                )
                .await,
            )?;
            assert_eq!(delete_impact.affected_task_count, 3);

            map_tasks_lib(delete_task_with_roles(&pool, owner, root_task_id).await)?;

            let deleted_count: (i64,) = sqlx::query_as(
                r#"
                SELECT COUNT(*)::bigint
                FROM tasks.tasks
                WHERE id = ANY($1)
                  AND deleted_at IS NOT NULL
                "#,
            )
            .bind(vec![
                root_task_id.0,
                child_task.task.id.0,
                grandchild_task.task.id.0,
            ])
            .fetch_one(&pool)
            .await?;
            assert_eq!(deleted_count.0, 3);

            let child_err = get_task_with_roles(&pool, owner, child_task.task.id)
                .await
                .expect_err("deleted subtree task should not be accessible");
            assert_eq!(child_err.kind, ErrorKind::NotFound);

            Ok(())
        })
        .await
    }
}
