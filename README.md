# postgres_queue

A library for managing and executing tasks in a PostgreSQL-backed queue.

This library provides a simple way to define, enqueue, and process tasks in a concurrent and fault-tolerant manner using a PostgreSQL database as the task queue.

## Features

- Define and register task handlers
- Enqueue tasks with optional scheduling and intervals
- Concurrent task processing with adjustable worker count
- Fault-tolerant task execution with error handling

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
postgres_queue = "0.1.0"
```

## Example

Here's a basic example demonstrating how to use the `postgres_queue` crate:

```rust
use postgres_queue::{TaskRegistry, TaskData, TaskError, connect, initialize_database};
use chrono::{Utc, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = "postgres://user:password@localhost/dbname";
    let pool = connect(database_url).await?;
    initialize_database(&pool).await?;

    let mut task_registry = TaskRegistry::new();
    task_registry.register_task("my_task", my_task_handler);

    let task_data = serde_json::json!({ "message": "Hello, world!" });
    let run_at = Utc::now() + Duration::seconds(10);
    let task_id = postgres_queue::enqueue(&pool, "my_task", task_data.clone(), run_at, None).await?;

    task_registry.run(&pool, 4).await?;

    Ok(())
}

async fn my_task_handler(task_id: i32, task_data: TaskData) -> Result<(), TaskError> {
    println!("Task {}: {:?}", task_id, task_data);
    Ok(())
}
```

## License

This project is licensed under the MIT License.
