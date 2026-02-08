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
    let row = load_accessible_task(pool, actor, task_id, perm::task_read()).await?;
    let task = to_task(row)?;
    let project_ids = get_task_project_ids(pool, task_id).await?;
    let graph_assignments = get_task_graph_assignments(pool, task_id).await?;
    ensure_task_assignment_graph_access(pool, actor, &graph_assignments).await?;
    let links_out = get_task_links_out(pool, task_id).await?;
    let links_in = get_task_links_in(pool, task_id).await?;
    let comments = get_task_comments(pool, task_id, TASK_DETAILS_LIMIT).await?;
    let log = get_task_log_entries(pool, task_id, TASK_DETAILS_LIMIT).await?;

    Ok(TaskDetails {
        task,
        project_ids,
        graph_assignments,
        links_out,
        links_in,
        comments,
        log,
    })
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

    let subtask_parent_state = match payload.link_type {
        TaskLinkType::SubtaskOf => payload.subtask_parent_state.ok_or_else(|| {
            LibError::invalid(
                "Subtask links require a parent state",
                anyhow!("subtask_of link missing subtask_parent_state"),
            )
        })?,
        _ => {
            if payload.subtask_parent_state.is_some() {
                return Err(LibError::invalid(
                    "Only subtask links may set subtaskParentState",
                    anyhow!("non-subtask link included subtaskParentState"),
                ));
            }
            TaskState::Open
        }
    };

    let subtask_parent_state = match payload.link_type {
        TaskLinkType::SubtaskOf => Some(subtask_parent_state),
        _ => None,
    };

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

    let assignee_user_id = match (from_state, to_state) {
        (TaskState::Todo, TaskState::Assigned) => {
            Some(payload.assigned_to_user_id.map(|id| id.0).ok_or_else(|| {
                LibError::invalid(
                    "assignedToUserId is required for this transition",
                    anyhow!("todo -> assigned transition missing assigned_to_user_id"),
                )
            })?)
        }
        (TaskState::Todo, TaskState::InProgress) => Some(actor.0),
        _ => existing.assignee_user_id,
    };

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
