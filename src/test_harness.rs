use anyhow::Result;
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use crate::db;

const POSTGRES_TEST_BASE_URL: &str = "postgres://postgres@localhost:5432";

pub struct TestDb {
    db_name: String,
    admin_url: String,
    test_url: String,
    pub pool: PgPool,
    schema_name: Option<String>,
}

impl TestDb {
    pub async fn new() -> Result<Self> {
        let admin_url = format!("{POSTGRES_TEST_BASE_URL}/postgres");
        let db_name = format!("subseq_tasks_test_{}", Uuid::new_v4().simple());
        let schema_name = None;

        let mut admin = PgConnection::connect(&admin_url).await?;
        let create_db = format!(r#"CREATE DATABASE "{}""#, db_name);
        sqlx::query(&create_db).execute(&mut admin).await?;
        let test_url = format!("{POSTGRES_TEST_BASE_URL}/{db_name}");
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
