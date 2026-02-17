use anyhow::{Result, anyhow};
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use crate::db;

const DEFAULT_POSTGRES_TEST_BASE_URL_PRIMARY: &str = "postgres://postgres@127.0.0.1:55432";
const DEFAULT_POSTGRES_TEST_BASE_URL_FALLBACK: &str = "postgres://postgres@127.0.0.1:5432";

pub struct TestDb {
    db_name: String,
    admin_url: String,
    test_url: String,
    pub pool: PgPool,
    schema_name: Option<String>,
}

impl TestDb {
    pub async fn new() -> Result<Self> {
        let base_url = resolve_postgres_test_base_url().await?;
        let admin_url = format!("{base_url}/postgres");
        let db_name = format!("subseq_tasks_test_{}", Uuid::new_v4().simple());
        let schema_name = None;

        let mut admin = PgConnection::connect(&admin_url).await?;
        let create_db = format!(r#"CREATE DATABASE "{}""#, db_name);
        sqlx::query(&create_db).execute(&mut admin).await?;
        let test_url = format!("{base_url}/{db_name}");
        let pool = PgPool::connect(&test_url).await?;

        Ok(Self {
            db_name,
            admin_url,
            test_url,
            pool,
            schema_name,
        })
    }

    pub fn db_dsn(&self) -> &str {
        &self.test_url
    }

    pub async fn prepare(&self) -> Result<()> {
        db::create_task_tables(&self.pool).await?;
        Ok(())
    }

    pub async fn teardown(self) -> Result<()> {
        self.pool.close().await;

        if let Some(schema) = self.schema_name {
            let mut admin = PgConnection::connect(&self.admin_url).await?;
            let drop_schema = format!(r#"DROP SCHEMA IF EXISTS "{}" CASCADE"#, schema);
            sqlx::query(&drop_schema).execute(&mut admin).await?;
        } else {
            let mut admin = PgConnection::connect(&self.admin_url).await?;
            sqlx::query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
            )
            .bind(&self.db_name)
            .execute(&mut admin)
            .await?;
            let drop_db = format!(r#"DROP DATABASE IF EXISTS "{}""#, self.db_name);
            sqlx::query(&drop_db).execute(&mut admin).await?;
        }
        Ok(())
    }
}

async fn resolve_postgres_test_base_url() -> Result<String> {
    let candidates = postgres_test_base_url_candidates();
    let mut failures = Vec::new();

    for candidate in candidates {
        let base_url = normalize_postgres_base_url(&candidate);
        let admin_url = format!("{base_url}/postgres");
        match PgConnection::connect(&admin_url).await {
            Ok(connection) => {
                connection.close().await?;
                return Ok(base_url);
            }
            Err(err) => failures.push(format!("{base_url}: {err}")),
        }
    }

    Err(anyhow!(
        "unable to connect to local postgres for tests. Set POSTGRES_TEST_BASE_URL. Attempted: {}",
        failures.join(" | ")
    ))
}

fn postgres_test_base_url_candidates() -> Vec<String> {
    if let Ok(base_url) = std::env::var("POSTGRES_TEST_BASE_URL") {
        let trimmed = base_url.trim();
        if !trimmed.is_empty() {
            return vec![trimmed.to_string()];
        }
    }

    vec![
        DEFAULT_POSTGRES_TEST_BASE_URL_PRIMARY.to_string(),
        DEFAULT_POSTGRES_TEST_BASE_URL_FALLBACK.to_string(),
    ]
}

fn normalize_postgres_base_url(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    trimmed
        .strip_suffix("/postgres")
        .unwrap_or(trimmed)
        .to_string()
}
