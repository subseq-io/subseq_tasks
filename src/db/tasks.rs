use std::collections::{HashMap, HashSet};

use chrono::Utc;
use serde_json::json;
use subseq_graph::invariants::{GraphMutationIndex, graph_invariant_violations};
use subseq_graph::models::{GraphEdge, GraphInvariantViolation, GraphKind, GraphNode, GraphNodeId};

use super::*;

const TASK_DETAILS_LIMIT: i64 = 200;
const SUBTREE_MAX_TASKS: usize = 20_000;

fn graph_violation_values(violations: &[GraphInvariantViolation]) -> serde_json::Value {
    json!(
        violations
            .iter()
            .map(|value| serde_json::to_value(value).unwrap_or_else(|_| json!({"type": "unknown"})))
            .collect::<Vec<_>>()
    )
}

fn task_node(task_id: TaskId) -> GraphNode {
    GraphNode {
        id: GraphNodeId(task_id.0),
        label: task_id.0.to_string(),
        metadata: json!({ "taskId": task_id.0 }),
    }
}

fn task_edge(from: TaskId, to: TaskId) -> GraphEdge {
    GraphEdge {
        from_node_id: GraphNodeId(from.0),
        to_node_id: GraphNodeId(to.0),
        metadata: json!({}),
    }
}

fn synthetic_project_root_node(task_ids: &[TaskId]) -> GraphNodeId {
    let mut task_id_set = HashSet::with_capacity(task_ids.len());
    for task_id in task_ids {
        task_id_set.insert(task_id.0);
    }

    let mut root = Uuid::new_v4();
    while task_id_set.contains(&root) {
        root = Uuid::new_v4();
    }

    GraphNodeId(root)
}

fn build_project_subtask_tree_graph(
    project_id: ProjectId,
    task_ids: &[TaskId],
    subtask_edges: &[(TaskId, TaskId)],
) -> (Vec<GraphNode>, Vec<GraphEdge>, GraphNodeId) {
    let _ = project_id;
    let root_node_id = synthetic_project_root_node(task_ids);
    let mut nodes = Vec::with_capacity(task_ids.len() + 1);
    nodes.push(GraphNode {
        id: root_node_id,
        label: "project_root".to_string(),
        metadata: json!({ "synthetic": true }),
    });

    let mut task_set = HashSet::with_capacity(task_ids.len());
    for task_id in task_ids {
        task_set.insert(*task_id);
        nodes.push(task_node(*task_id));
    }

    let mut incoming_counts: HashMap<TaskId, usize> = HashMap::new();
    let mut edges = Vec::with_capacity(subtask_edges.len() + task_ids.len());
    for (from, to) in subtask_edges {
        if task_set.contains(from) && task_set.contains(to) {
            *incoming_counts.entry(*to).or_insert(0) += 1;
            edges.push(task_edge(*from, *to));
        }
    }

    for task_id in task_ids {
        if incoming_counts.get(task_id).copied().unwrap_or(0) == 0 {
            edges.push(GraphEdge {
                from_node_id: root_node_id,
                to_node_id: GraphNodeId(task_id.0),
                metadata: json!({"syntheticRootEdge": true}),
            });
        }
    }

    (nodes, edges, root_node_id)
}

fn build_project_graph(
    kind: GraphKind,
    task_ids: &[TaskId],
    edges: &[(TaskId, TaskId)],
) -> (Vec<GraphNode>, Vec<GraphEdge>) {
    let mut nodes = Vec::with_capacity(task_ids.len());
    let mut task_set = HashSet::with_capacity(task_ids.len());
    for task_id in task_ids {
        task_set.insert(*task_id);
        nodes.push(task_node(*task_id));
    }

    let mut graph_edges = Vec::with_capacity(edges.len());
    for (from, to) in edges {
        if task_set.contains(from) && task_set.contains(to) {
            graph_edges.push(task_edge(*from, *to));
        }
    }

    let _ = kind;
    (nodes, graph_edges)
}

fn link_type_graph_kind(link_type: TaskLinkType) -> GraphKind {
    match link_type {
        TaskLinkType::SubtaskOf => GraphKind::Tree,
        TaskLinkType::DependsOn => GraphKind::Dag,
        TaskLinkType::RelatedTo | TaskLinkType::AssignmentOrder => GraphKind::Directed,
    }
}

fn merged_reparent_subtask_edges(
    existing_edges: &[(TaskId, TaskId)],
    new_parent: TaskId,
    child: TaskId,
) -> Vec<(TaskId, TaskId)> {
    let mut dedupe = HashSet::new();
    let mut merged = Vec::with_capacity(existing_edges.len() + 1);

    for (from, to) in existing_edges {
        if *to == child {
            continue;
        }
        if dedupe.insert((*from, *to)) {
            merged.push((*from, *to));
        }
    }

    if dedupe.insert((new_parent, child)) {
        merged.push((new_parent, child));
    }

    merged
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

async fn project_task_ids<'e, E>(executor: E, project_id: ProjectId) -> Result<Vec<TaskId>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let rows = sqlx::query_as::<_, (Uuid,)>(
        r#"
        SELECT tp.task_id
        FROM tasks.task_projects tp
        JOIN tasks.tasks t
          ON t.id = tp.task_id
        WHERE tp.project_id = $1
          AND t.deleted_at IS NULL
        ORDER BY tp.task_id ASC
        "#,
    )
    .bind(project_id.0)
    .fetch_all(executor)
    .await
    .map_err(|err| db_err("Failed to query project tasks", err))?;

    Ok(rows.into_iter().map(|row| TaskId(row.0)).collect())
}

async fn project_link_edges<'e, E>(
    executor: E,
    project_id: ProjectId,
    link_type: TaskLinkType,
) -> Result<Vec<(TaskId, TaskId)>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let rows = sqlx::query_as::<_, (Uuid, Uuid)>(
        r#"
        SELECT tl.task_from_id, tl.task_to_id
        FROM tasks.task_links tl
        JOIN tasks.task_projects p_from
          ON p_from.task_id = tl.task_from_id
         AND p_from.project_id = $1
        JOIN tasks.task_projects p_to
          ON p_to.task_id = tl.task_to_id
         AND p_to.project_id = $1
        JOIN tasks.tasks t_from
          ON t_from.id = tl.task_from_id
         AND t_from.deleted_at IS NULL
        JOIN tasks.tasks t_to
          ON t_to.id = tl.task_to_id
         AND t_to.deleted_at IS NULL
        WHERE tl.link_type = $2
        ORDER BY tl.task_from_id ASC, tl.task_to_id ASC
        "#,
    )
    .bind(project_id.0)
    .bind(link_type.as_db_value())
    .fetch_all(executor)
    .await
    .map_err(|err| db_err("Failed to query project task links", err))?;

    Ok(rows
        .into_iter()
        .map(|(from_id, to_id)| (TaskId(from_id), TaskId(to_id)))
        .collect())
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
    let rows = sqlx::query_as::<_, (Uuid,)>(
        r#"
        WITH RECURSIVE subtree AS (
            SELECT $1::uuid AS task_id
            UNION
            SELECT tl.task_to_id
            FROM tasks.task_links tl
            JOIN subtree s
              ON s.task_id = tl.task_from_id
            JOIN tasks.tasks t
              ON t.id = tl.task_to_id
            WHERE tl.link_type = 'subtask_of'
              AND t.deleted_at IS NULL
        )
        SELECT task_id
        FROM subtree
        ORDER BY task_id ASC
        LIMIT $2
        "#,
    )
    .bind(task_id.0)
    .bind(SUBTREE_MAX_TASKS as i64)
    .fetch_all(pool)
    .await
    .map_err(|err| db_err("Failed to query task subtree", err))?;

    Ok(rows.into_iter().map(|row| TaskId(row.0)).collect())
}

async fn validate_project_link_graph_integrity<'e, E>(
    executor: E,
    project_id: ProjectId,
) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres> + Copy,
{
    let task_ids = project_task_ids(executor, project_id).await?;
    let subtask_edges = project_link_edges(executor, project_id, TaskLinkType::SubtaskOf).await?;
    let depends_edges = project_link_edges(executor, project_id, TaskLinkType::DependsOn).await?;
    let related_edges = project_link_edges(executor, project_id, TaskLinkType::RelatedTo).await?;
    let assignment_edges =
        project_link_edges(executor, project_id, TaskLinkType::AssignmentOrder).await?;

    let (subtask_nodes, subtask_graph_edges, _) =
        build_project_subtask_tree_graph(project_id, &task_ids, &subtask_edges);
    let subtask_violations =
        graph_invariant_violations(GraphKind::Tree, &subtask_nodes, &subtask_graph_edges);
    if !subtask_violations.is_empty() {
        return Err(LibError::invalid(
            "Subtask tree contains invalid structure",
            anyhow!(
                "subtask tree integrity failed for project {project_id}: {}",
                graph_violation_values(&subtask_violations)
            ),
        ));
    }

    let (depends_nodes, depends_graph_edges) =
        build_project_graph(GraphKind::Dag, &task_ids, &depends_edges);
    let depends_violations =
        graph_invariant_violations(GraphKind::Dag, &depends_nodes, &depends_graph_edges);
    if !depends_violations.is_empty() {
        return Err(LibError::invalid(
            "Dependency graph contains invalid structure",
            anyhow!(
                "dependency graph integrity failed for project {project_id}: {}",
                graph_violation_values(&depends_violations)
            ),
        ));
    }

    let mut related_all_edges = related_edges;
    related_all_edges.extend(assignment_edges);
    let (related_nodes, related_graph_edges) =
        build_project_graph(GraphKind::Directed, &task_ids, &related_all_edges);
    let related_violations =
        graph_invariant_violations(GraphKind::Directed, &related_nodes, &related_graph_edges);
    if !related_violations.is_empty() {
        return Err(LibError::invalid(
            "Related-task graph contains invalid structure",
            anyhow!(
                "related graph integrity failed for project {project_id}: {}",
                graph_violation_values(&related_violations)
            ),
        ));
    }

    Ok(())
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
    .bind(title)
    .bind(description)
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
    let details = match row {
        Some(row) => to_task_details_from_presentation_row(row)?,
        None => {
            return Err(LibError::database(
                "Failed to load task details",
                anyhow!("task {task_id} was accessible but missing from task presentation view"),
            ));
        }
    };

    ensure_task_assignment_graph_access(pool, actor, &details.graph_assignments).await?;
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
                    validate_project_link_graph_integrity(pool, ProjectId(project_id)).await?;
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

    sqlx::query(
        r#"
        DELETE FROM tasks.task_links
        WHERE task_from_id = ANY($1)
           OR task_to_id = ANY($1)
        "#,
    )
    .bind(
        subtree_task_ids
            .iter()
            .map(|id| id.0)
            .collect::<Vec<Uuid>>(),
    )
    .execute(pool)
    .await
    .map_err(|err| db_err("Failed to excise deleted task links", err))?;

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

    let mut tx = pool
        .begin()
        .await
        .map_err(|err| db_err("Failed to start transaction", err))?;

    lock_tasks_for_projects(&mut *tx, &shared_projects).await?;

    for project_id in &shared_projects {
        let project_tasks = project_task_ids(&mut *tx, *project_id).await?;
        let task_set: HashSet<TaskId> = project_tasks.iter().copied().collect();
        if !task_set.contains(&task_id) || !task_set.contains(&payload.other_task_id) {
            continue;
        }

        match payload.link_type {
            TaskLinkType::SubtaskOf => {
                let existing_edges =
                    project_link_edges(&mut *tx, *project_id, TaskLinkType::SubtaskOf).await?;
                let candidate_edges =
                    merged_reparent_subtask_edges(&existing_edges, task_id, payload.other_task_id);
                let (nodes, edges, _) =
                    build_project_subtask_tree_graph(*project_id, &project_tasks, &candidate_edges);
                let violations = graph_invariant_violations(GraphKind::Tree, &nodes, &edges);
                if !violations.is_empty() {
                    return Err(LibError::invalid(
                        "Subtask link would violate tree constraints",
                        anyhow!(
                            "project {project_id} subtask add failed with violations: {}",
                            graph_violation_values(&violations)
                        ),
                    ));
                }
            }
            _ => {
                let existing_edges =
                    project_link_edges(&mut *tx, *project_id, payload.link_type).await?;
                let graph_kind = link_type_graph_kind(payload.link_type);
                let (nodes, edges) =
                    build_project_graph(graph_kind, &project_tasks, &existing_edges);
                let index = GraphMutationIndex::new(graph_kind, &nodes, &edges);
                let violations = index.would_add_edge_violations(
                    GraphNodeId(task_id.0),
                    GraphNodeId(payload.other_task_id.0),
                );
                if !violations.is_empty() {
                    return Err(LibError::invalid(
                        "Task link would violate graph constraints",
                        anyhow!(
                            "project {project_id} {:?} add failed with violations: {}",
                            payload.link_type,
                            graph_violation_values(&violations)
                        ),
                    ));
                }
            }
        }
    }

    if payload.link_type == TaskLinkType::SubtaskOf {
        sqlx::query(
            r#"
            DELETE FROM tasks.task_links
            WHERE link_type = 'subtask_of'
              AND task_to_id = $1
              AND task_from_id <> $2
            "#,
        )
        .bind(payload.other_task_id.0)
        .bind(task_id.0)
        .execute(&mut *tx)
        .await
        .map_err(|err| db_err("Failed to reparent existing subtask edges", err))?;
    }

    sqlx::query(
        r#"
        INSERT INTO tasks.task_links (
            task_from_id,
            task_to_id,
            link_type,
            subtask_parent_state
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (task_from_id, task_to_id, link_type)
        DO UPDATE SET
            subtask_parent_state = EXCLUDED.subtask_parent_state,
            created_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(task_id.0)
    .bind(payload.other_task_id.0)
    .bind(payload.link_type.as_db_value())
    .bind(subtask_parent_state.map(|state| state.as_str().to_string()))
    .execute(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to create task link", err))?;

    let row = sqlx::query_as::<_, TaskLinkRow>(
        r#"
        SELECT
            task_from_id,
            task_to_id,
            link_type,
            subtask_parent_state,
            created_at
        FROM tasks.task_links
        WHERE task_from_id = $1
          AND task_to_id = $2
          AND link_type = $3
        LIMIT 1
        "#,
    )
    .bind(task_id.0)
    .bind(payload.other_task_id.0)
    .bind(payload.link_type.as_db_value())
    .fetch_one(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to fetch task link", err))?;

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

    to_task_link(row)
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
    let mut tx = pool
        .begin()
        .await
        .map_err(|err| db_err("Failed to start transaction", err))?;

    if !shared_projects.is_empty() {
        lock_tasks_for_projects(&mut *tx, &shared_projects).await?;
    }

    let mut isolated_subtree = false;
    for project_id in &shared_projects {
        let project_tasks = project_task_ids(&mut *tx, *project_id).await?;
        let subtask_edges =
            project_link_edges(&mut *tx, *project_id, TaskLinkType::SubtaskOf).await?;
        if subtask_edges.is_empty() {
            continue;
        }

        let (nodes, edges, _) =
            build_project_subtask_tree_graph(*project_id, &project_tasks, &subtask_edges);
        let index = GraphMutationIndex::new(GraphKind::Tree, &nodes, &edges);
        if index.would_remove_edge_isolate_subgraph(
            GraphNodeId(task_id.0),
            GraphNodeId(other_task_id.0),
        ) || index.would_remove_edge_isolate_subgraph(
            GraphNodeId(other_task_id.0),
            GraphNodeId(task_id.0),
        ) {
            isolated_subtree = true;
        }

        let candidate_edges: Vec<(TaskId, TaskId)> = subtask_edges
            .into_iter()
            .filter(|(from, to)| {
                !((*from == task_id && *to == other_task_id)
                    || (*from == other_task_id && *to == task_id))
            })
            .collect();
        let (candidate_nodes, candidate_graph_edges, _) =
            build_project_subtask_tree_graph(*project_id, &project_tasks, &candidate_edges);
        let violations =
            graph_invariant_violations(GraphKind::Tree, &candidate_nodes, &candidate_graph_edges);
        if !violations.is_empty() {
            return Err(LibError::invalid(
                "Removing this link would violate subtree constraints",
                anyhow!(
                    "project {project_id} remove edge validation failed: {}",
                    graph_violation_values(&violations)
                ),
            ));
        }
    }

    let result = sqlx::query(
        r#"
        DELETE FROM tasks.task_links
        WHERE (task_from_id = $1 AND task_to_id = $2)
           OR (task_from_id = $2 AND task_to_id = $1)
        "#,
    )
    .bind(task_id.0)
    .bind(other_task_id.0)
    .execute(&mut *tx)
    .await
    .map_err(|err| db_err("Failed to delete task links", err))?;

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
            "rowsAffected": result.rows_affected(),
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

            map_tasks_lib(delete_task_with_roles(&pool, member, member_task.task.id).await)?;

            let owner_aux = map_tasks_lib(
                create_task_with_roles(
                    &pool,
                    owner,
                    create_task_payload(fixture.project.id, "Owner aux"),
                )
                .await,
            )?;

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
