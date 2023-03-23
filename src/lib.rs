use deadpool_postgres::{Client, Config, Pool, PoolError, Runtime};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use url::Url;

pub type TaskId = i32;
pub type TaskData = HashMap<String, JsonValue>;
pub type TaskStatus = String;
pub type TaskHandler = Arc<
    dyn Fn(
            &Client,
            TaskId,
            TaskData,
        )
            -> Pin<Box<dyn std::future::Future<Output = Result<(), TaskError>> + Send + Sync>>
        + Send
        + Sync,
>;

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

#[derive(Debug, Deserialize, Serialize)]
pub struct Task {
    pub id: TaskId,
    pub name: String,
    pub data: TaskData,
    pub status: TaskStatus,
}

pub struct TaskRegistry {
    handlers: Arc<Mutex<HashMap<String, TaskHandler>>>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register_task(&self, name: String, handler: TaskHandler) {
        let mut handlers = self.handlers.lock().await;
        handlers.insert(name, handler);
    }

    pub fn clone_handlers(&self) -> Arc<Mutex<HashMap<String, TaskHandler>>> {
        self.handlers.clone()
    }

    pub async fn run(
        &self,
        pool: &Pool,
        parallelism: usize,
    ) -> Result<Vec<JoinHandle<()>>, TaskError> {
        let mut tasks = Vec::new();

        for _ in 0..parallelism {
            let mut client = pool.get().await?;
            let handlers = self.clone_handlers();

            let task = tokio::spawn(async move {
                loop {
                    let task_opt = dequeue(&mut client).await.expect("Failed to dequeue task");

                    if let Some(task) = task_opt {
                        let handlers = handlers.lock().await;
                        if let Some(handler) = handlers.get(&task.name) {
                            match handler(&client, task.id, task.data).await {
                                Ok(_) => {
                                    complete_task(&client, task.id)
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
                        break;
                    }
                }
            });

            tasks.push(task);
        }

        Ok(tasks)
    }
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("URL parsing error: {0}")]
    UrlError(#[from] url::ParseError),

    #[error("Error creating pool: {0}")]
    CreatePoolError(#[from] deadpool_postgres::CreatePoolError),
}

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

    // for (key, value) in parsed_url.query_pairs() {
    //     config.options.push((key.to_owned(), value.to_owned()));
    // }

    Ok(config)
}

pub async fn connect(database_url: &str) -> Result<Pool, ConnectionError> {
    let config = create_deadpool_config_from_url(database_url)?;
    let pool = config.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)?;
    Ok(pool)
}

pub async fn enqueue(client: &Client, task_data: TaskData) -> Result<TaskId, TaskError> {
    let task_data_json = serde_json::to_value(task_data)?;
    let row = client
        .query_one(
            "INSERT INTO task_queue (task_data) VALUES ($1) RETURNING id",
            &[&task_data_json],
        )
        .await?;
    Ok(row.get(0))
}

pub async fn dequeue(client: &mut Client) -> Result<Option<Task>, TaskError> {
    let tx = client.transaction().await?;
    let row = tx
        .query_opt(
            "SELECT id, task_data, status FROM task_queue WHERE status = 'queued' ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED",
            &[],
        )
        .await?;

    if let Some(row) = row {
        let task = Task {
            id: row.get(0),
            name: serde_json::from_value(row.get(1))?,
            data: serde_json::from_value(row.get(2))?,
            status: row.get(3),
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

pub async fn complete_task(client: &Client, task_id: TaskId) -> Result<(), TaskError> {
    client
        .execute(
            "UPDATE task_queue SET status = 'completed', updated_at = NOW() WHERE id = $1",
            &[&task_id],
        )
        .await?;
    Ok(())
}

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
