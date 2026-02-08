pub const SCOPE_TASKS: &str = "tasks";
pub const SCOPE_PROJECT: &str = "project";
pub const SCOPE_TASK: &str = "task";
pub const SCOPE_ID_GLOBAL: &str = "global";

pub const ROLE_PROJECT_CREATE: &str = "project_create";
pub const ROLE_PROJECT_READ: &str = "project_read";
pub const ROLE_PROJECT_UPDATE: &str = "project_update";
pub const ROLE_PROJECT_DELETE: &str = "project_delete";

pub const ROLE_MILESTONE_CREATE: &str = "milestone_create";
pub const ROLE_MILESTONE_READ: &str = "milestone_read";
pub const ROLE_MILESTONE_UPDATE: &str = "milestone_update";
pub const ROLE_MILESTONE_DELETE: &str = "milestone_delete";

pub const ROLE_TASK_CREATE: &str = "task_create";
pub const ROLE_TASK_READ: &str = "task_read";
pub const ROLE_TASK_UPDATE: &str = "task_update";
pub const ROLE_TASK_DELETE: &str = "task_delete";
pub const ROLE_TASK_LINK: &str = "task_link";
pub const ROLE_TASK_TRANSITION: &str = "task_transition";

static PROJECT_READ_ACCESS_ROLES: &[&str] = &[
    ROLE_PROJECT_READ,
    ROLE_PROJECT_CREATE,
    ROLE_PROJECT_UPDATE,
    ROLE_PROJECT_DELETE,
];

static MILESTONE_READ_ACCESS_ROLES: &[&str] = &[
    ROLE_MILESTONE_READ,
    ROLE_MILESTONE_CREATE,
    ROLE_MILESTONE_UPDATE,
    ROLE_MILESTONE_DELETE,
];

static TASK_READ_ACCESS_ROLES: &[&str] = &[
    ROLE_TASK_READ,
    ROLE_TASK_CREATE,
    ROLE_TASK_UPDATE,
    ROLE_TASK_DELETE,
    ROLE_TASK_LINK,
    ROLE_TASK_TRANSITION,
];

static PROJECT_CREATE_ACCESS_ROLES: &[&str] = &[ROLE_PROJECT_CREATE];
static PROJECT_UPDATE_ACCESS_ROLES: &[&str] = &[ROLE_PROJECT_UPDATE];
static PROJECT_DELETE_ACCESS_ROLES: &[&str] = &[ROLE_PROJECT_DELETE];
static MILESTONE_CREATE_ACCESS_ROLES: &[&str] = &[ROLE_MILESTONE_CREATE];
static MILESTONE_UPDATE_ACCESS_ROLES: &[&str] = &[ROLE_MILESTONE_UPDATE];
static MILESTONE_DELETE_ACCESS_ROLES: &[&str] = &[ROLE_MILESTONE_DELETE];
static TASK_CREATE_ACCESS_ROLES: &[&str] = &[ROLE_TASK_CREATE];
static TASK_UPDATE_ACCESS_ROLES: &[&str] = &[ROLE_TASK_UPDATE];
static TASK_DELETE_ACCESS_ROLES: &[&str] = &[ROLE_TASK_DELETE];
static TASK_LINK_ACCESS_ROLES: &[&str] = &[ROLE_TASK_LINK];
static TASK_TRANSITION_ACCESS_ROLES: &[&str] = &[ROLE_TASK_TRANSITION];
static NO_ACCESS_ROLES: &[&str] = &[];

static READ_PERMISSIONS: &[&str] = &[ROLE_PROJECT_READ, ROLE_MILESTONE_READ, ROLE_TASK_READ];

static WRITE_PERMISSIONS: &[&str] = &[
    ROLE_PROJECT_CREATE,
    ROLE_PROJECT_UPDATE,
    ROLE_PROJECT_DELETE,
    ROLE_MILESTONE_CREATE,
    ROLE_MILESTONE_UPDATE,
    ROLE_MILESTONE_DELETE,
    ROLE_TASK_CREATE,
    ROLE_TASK_UPDATE,
    ROLE_TASK_DELETE,
    ROLE_TASK_LINK,
    ROLE_TASK_TRANSITION,
];

static FULL_PERMISSIONS: &[&str] = &[
    ROLE_PROJECT_CREATE,
    ROLE_PROJECT_READ,
    ROLE_PROJECT_UPDATE,
    ROLE_PROJECT_DELETE,
    ROLE_MILESTONE_CREATE,
    ROLE_MILESTONE_READ,
    ROLE_MILESTONE_UPDATE,
    ROLE_MILESTONE_DELETE,
    ROLE_TASK_CREATE,
    ROLE_TASK_READ,
    ROLE_TASK_UPDATE,
    ROLE_TASK_DELETE,
    ROLE_TASK_LINK,
    ROLE_TASK_TRANSITION,
];

pub fn scope_tasks() -> &'static str {
    SCOPE_TASKS
}

pub fn scope_project() -> &'static str {
    SCOPE_PROJECT
}

pub fn scope_task() -> &'static str {
    SCOPE_TASK
}

pub fn scope_id_global() -> &'static str {
    SCOPE_ID_GLOBAL
}

pub fn project_create() -> &'static str {
    ROLE_PROJECT_CREATE
}

pub fn project_read() -> &'static str {
    ROLE_PROJECT_READ
}

pub fn project_update() -> &'static str {
    ROLE_PROJECT_UPDATE
}

pub fn project_delete() -> &'static str {
    ROLE_PROJECT_DELETE
}

pub fn milestone_create() -> &'static str {
    ROLE_MILESTONE_CREATE
}

pub fn milestone_read() -> &'static str {
    ROLE_MILESTONE_READ
}

pub fn milestone_update() -> &'static str {
    ROLE_MILESTONE_UPDATE
}

pub fn milestone_delete() -> &'static str {
    ROLE_MILESTONE_DELETE
}

pub fn task_create() -> &'static str {
    ROLE_TASK_CREATE
}

pub fn task_read() -> &'static str {
    ROLE_TASK_READ
}

pub fn task_update() -> &'static str {
    ROLE_TASK_UPDATE
}

pub fn task_delete() -> &'static str {
    ROLE_TASK_DELETE
}

pub fn task_link() -> &'static str {
    ROLE_TASK_LINK
}

pub fn task_transition() -> &'static str {
    ROLE_TASK_TRANSITION
}

pub fn project_read_access_roles() -> &'static [&'static str] {
    PROJECT_READ_ACCESS_ROLES
}

pub fn milestone_read_access_roles() -> &'static [&'static str] {
    MILESTONE_READ_ACCESS_ROLES
}

pub fn task_read_access_roles() -> &'static [&'static str] {
    TASK_READ_ACCESS_ROLES
}

pub fn access_roles(permission: &str) -> &'static [&'static str] {
    match permission {
        ROLE_PROJECT_CREATE => PROJECT_CREATE_ACCESS_ROLES,
        ROLE_PROJECT_READ => PROJECT_READ_ACCESS_ROLES,
        ROLE_PROJECT_UPDATE => PROJECT_UPDATE_ACCESS_ROLES,
        ROLE_PROJECT_DELETE => PROJECT_DELETE_ACCESS_ROLES,
        ROLE_MILESTONE_CREATE => MILESTONE_CREATE_ACCESS_ROLES,
        ROLE_MILESTONE_READ => MILESTONE_READ_ACCESS_ROLES,
        ROLE_MILESTONE_UPDATE => MILESTONE_UPDATE_ACCESS_ROLES,
        ROLE_MILESTONE_DELETE => MILESTONE_DELETE_ACCESS_ROLES,
        ROLE_TASK_CREATE => TASK_CREATE_ACCESS_ROLES,
        ROLE_TASK_READ => TASK_READ_ACCESS_ROLES,
        ROLE_TASK_UPDATE => TASK_UPDATE_ACCESS_ROLES,
        ROLE_TASK_DELETE => TASK_DELETE_ACCESS_ROLES,
        ROLE_TASK_LINK => TASK_LINK_ACCESS_ROLES,
        ROLE_TASK_TRANSITION => TASK_TRANSITION_ACCESS_ROLES,
        _ => NO_ACCESS_ROLES,
    }
}

pub fn read_permissions() -> &'static [&'static str] {
    READ_PERMISSIONS
}

pub fn write_permissions() -> &'static [&'static str] {
    WRITE_PERMISSIONS
}

pub fn full_permissions() -> &'static [&'static str] {
    FULL_PERMISSIONS
}
