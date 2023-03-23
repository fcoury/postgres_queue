//! A library for managing and executing tasks in a PostgreSQL-backed queue.
//!
//! This library provides a simple way to define, enqueue, and process tasks in a concurrent
//! and fault-tolerant manner using a PostgreSQL database as the task queue.
//!
//! # Example
//!
//! ```rust
//! use my_task_queue::{TaskRegistry, TaskData, TaskError, connect, initialize_database};
//! use chrono::{Utc, Duration};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let database_url = "postgres://user:password@localhost/dbname";
//!     let pool = connect(database_url).await?;
//!     initialize_database(&pool).await?;
//!
//!     let mut task_registry = TaskRegistry::new();
//!     task_registry.register_task("my_task", my_task_handler);
//!
//!     let task_data = serde_json::json!({ "message": "Hello, world!" });
//!     let run_at = Utc::now() + Duration::seconds(10);
//!     let task_id = my_task_queue::enqueue(&pool, "my_task", task_data.clone(), run_at, None).await?;
//!
//!     task_registry.run(&pool, 4).await?;
//!
//!     Ok(())
//! }
//!
//! async fn my_task_handler(task_id: i32, task_data: TaskData) -> Result<(), TaskError> {
//!     println!("Task {}: {:?}", task_id, task_data);
//!     Ok(())
//! }
//! ```
use chrono::{DateTime, Utc};
use deadpool_postgres::{Client, Config, Pool, PoolError, Runtime};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use url::Url;

/// A type alias for Task ID.
pub type TaskId = i32;

/// A type alias for Task Data.
pub type TaskData = JsonValue;

/// A type alias for Task Status.
pub type TaskStatus = String;

/// A type alias for Task Handler.
pub type TaskHandler = Box<
    dyn Fn(
            TaskId,
            TaskData,
        )
            -> Pin<Box<dyn std::future::Future<Output = Result<(), TaskError>> + Send + Sync>>
        + Send
        + Sync,
>;

/// An enumeration of possible errors that can occur while working with tasks.
#[derive(Error, Debug)]
pub enum TaskError {
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Database error: {0}")]
    DatabaseError(#[from] tokio_postgres::Error),

    #[error("Database pool error: {0}")]
    PoolError(#[from] PoolError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("URL parsing error: {0}")]
    UrlError(#[from] url::ParseError),
}

/// An enumeration of possible errors that can occur while connecting to the database.
#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("URL parsing error: {0}")]
    UrlError(#[from] url::ParseError),

    #[error("Error creating pool: {0}")]
    CreatePoolError(#[from] deadpool_postgres::CreatePoolError),
}

/// A struct representing a task in the task queue.
#[derive(Debug, Deserialize, Serialize)]
pub struct Task {
    pub id: TaskId,
    pub name: String,
    pub data: TaskData,
    pub status: TaskStatus,
    pub run_at: DateTime<Utc>,
    pub interval: Option<Duration>,
}

/// A struct for managing a registry of task handlers.
pub struct TaskRegistry {
    handlers: Arc<HashMap<String, TaskHandler>>,
}

impl TaskRegistry {
    /// Creates a new TaskRegistry.
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(HashMap::new()),
        }
    }

    /// Registers a task handler with the provided name.
    pub fn register_task<F, Fut>(&mut self, name: String, handler: F)
    where
        F: Fn(i32, TaskData) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), TaskError>> + Send + Sync + 'static,
    {
        let wrapped_handler = move |task_id: i32, task_data: TaskData| {
            Box::pin(handler(task_id, task_data))
                as Pin<Box<dyn Future<Output = Result<(), TaskError>> + Send + Sync>>
        };

        Arc::get_mut(&mut self.handlers)
            .unwrap()
            .insert(name, Box::new(wrapped_handler));
    }

    /// Returns a reference to the task handlers.
    pub fn handlers(&self) -> &Arc<HashMap<String, TaskHandler>> {
        &self.handlers
    }

    /// Runs the task handlers with the provided number of workers.
    pub async fn run(
        &self,
        pool: &Pool,
        num_workers: usize,
    ) -> Result<Vec<JoinHandle<()>>, TaskError> {
        let mut tasks = Vec::new();

        for _ in 0..num_workers {
            let pool = pool.clone(); // Clone the pool for each worker
            let handlers = self.handlers.clone();

            let task = tokio::spawn(async move {
                let mut client = pool.get().await.expect("Failed to get client");
                loop {
                    let task_opt = dequeue(&mut client).await.expect("Failed to dequeue task");

                    if let Some(task) = task_opt {
                        if let Some(handler) = handlers.get(&task.name) {
                            match handler(task.id, task.data).await {
                                Ok(_) => {
                                    complete_task(&client, task.id, task.interval)
                                        .await
                                        .expect("Failed to complete task");
                                }
                                Err(err) => {
                                    let error_message = format!("{}", err);
                                    fail_task(&client, task.id, &error_message)
                                        .await
                                        .expect("Failed to fail task");
                                }
                            }
                        } else {
                            eprintln!("No handler found for task: {}", task.name);
                        }
                    } else {
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            });

            tasks.push(task);
        }

        Ok(tasks)
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Creates a Deadpool configuration from a database URL.
fn create_deadpool_config_from_url(url: &str) -> Result<Config, url::ParseError> {
    let parsed_url = Url::parse(url)?;

    let config = Config {
        user: Some(parsed_url.username().to_owned()),
        password: parsed_url.password().map(ToString::to_string),
        host: Some(parsed_url.host_str().unwrap().to_owned()),
        port: Some(parsed_url.port().unwrap_or(5432)),
        dbname: Some(
            parsed_url
                .path_segments()
                .map(|mut segments| segments.next().unwrap().to_owned())
                .unwrap(),
        ),
        ..Default::default()
    };

    // TODO
    // for (key, value) in parsed_url.query_pairs() {
    //     config.options.push((key.to_owned(), value.to_owned()));
    // }

    Ok(config)
}

/// Connects to the PostgreSQL database using the provided URL.
pub async fn connect(database_url: &str) -> Result<Pool, ConnectionError> {
    let config = create_deadpool_config_from_url(database_url)?;
    let pool = config.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)?;
    Ok(pool)
}

/// Initializes the task queue database schema.
pub async fn initialize_database(pool: &Pool) -> Result<(), TaskError> {
    let client = pool.get().await?;
    client
        .batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS task_queue (
                id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL,
                task_data JSONB NOT NULL,
                status VARCHAR NOT NULL DEFAULT 'queued',
                run_at TIMESTAMPTZ NOT NULL,
                interval BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#,
        )
        .await?;
    Ok(())
}

/// Enqueues a task with the specified parameters.
pub async fn enqueue(
    client: &Client,
    name: &str,
    task_data: TaskData,
    run_at: DateTime<Utc>,
    interval: Option<Duration>,
) -> Result<TaskId, TaskError> {
    let task_data_json = serde_json::to_value(task_data)?;
    let interval_ms: Option<i64> = interval.map(|i| i.as_millis() as i64);
    let row = client
        .query_one(
            "INSERT INTO task_queue (task_data, name, run_at, interval) VALUES ($1, $2, $3, $4) RETURNING id",
            &[&task_data_json, &name, &run_at, &interval_ms],
        )
        .await?;
    Ok(row.get(0))
}

/// Dequeues a task from the task queue.
pub async fn dequeue(client: &mut Client) -> Result<Option<Task>, TaskError> {
    let tx = client.transaction().await?;
    let row = tx
        .query_opt(
            "SELECT id, name, task_data, status, run_at, interval FROM task_queue WHERE status = 'queued' AND run_at <= NOW() ORDER BY run_at LIMIT 1 FOR UPDATE SKIP LOCKED",
            &[],
        )
        .await?;

    if let Some(row) = row {
        let interval_ms: Option<i64> = row.get(5);
        let interval = interval_ms.map(|i| Duration::from_millis(i as u64)); // Convert i64 to Duration

        let task = Task {
            id: row.get(0),
            name: row.get(1),
            data: row.get(2),
            status: row.get(3),
            run_at: row.get(4),
            interval,
        };

        tx.execute(
            "UPDATE task_queue SET status = 'processing', updated_at = NOW() WHERE id = $1",
            &[&task.id],
        )
        .await?;

        tx.commit().await?;

        Ok(Some(task))
    } else {
        Ok(None)
    }
}

/// Marks a task as complete and reschedules it if it has an interval.
pub async fn complete_task(
    client: &Client,
    task_id: TaskId,
    interval: Option<Duration>,
) -> Result<(), TaskError> {
    if let Some(interval) = interval {
        let interval_ms = interval.as_millis() as i64; // Convert Duration to i64
        let next_run_at = Utc::now() + chrono::Duration::milliseconds(interval_ms);
        client
            .execute(
                "UPDATE task_queue SET status = 'queued', updated_at = NOW(), run_at = $1 WHERE id = $2",
                &[&next_run_at, &task_id],
            )
            .await?;
    } else {
        client
            .execute(
                "UPDATE task_queue SET status = 'completed', updated_at = NOW() WHERE id = $1",
                &[&task_id],
            )
            .await?;
    }
    Ok(())
}

/// Marks a task as failed and stores the error message in the task data.
pub async fn fail_task(
    client: &Client,
    task_id: TaskId,
    error_message: &str,
) -> Result<(), TaskError> {
    let error_json = serde_json::json!({ "error": error_message });
    client
        .execute(
            "UPDATE task_queue SET status = 'failed', updated_at = NOW(), task_data = task_data || $1::jsonb WHERE id = $2",
            &[&error_json, &task_id],
        )
        .await?;
    Ok(())
}
