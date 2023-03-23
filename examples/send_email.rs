use postgres_queue::{initialize_database, TaskData, TaskError, TaskRegistry};
use serde_json::json;

async fn send_email_handler(task_id: i32, task_data: TaskData) -> Result<(), TaskError> {
    let recipient = task_data.get("recipient").unwrap().as_str().unwrap();
    let subject = task_data.get("subject").unwrap().as_str().unwrap();
    let body = task_data.get("body").unwrap().as_str().unwrap();

    println!(
        "[{}] Sending email to {} with subject '{}' and body '{}'",
        task_id, recipient, subject, body
    );

    // Simulate sending the email
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    println!("[{}] Email sent to {}", task_id, recipient);

    Ok(())
}

#[tokio::main]
async fn main() {
    let database_url = "postgresql://postgres:postgres@localhost/queue";
    let num_workers = 20;

    let pool = postgres_queue::connect(database_url)
        .await
        .expect("Failed to connect to the database");

    initialize_database(&pool)
        .await
        .expect("Failed to initialize database");

    let mut registry = TaskRegistry::new();
    registry.register_task("send_email".to_string(), send_email_handler);

    let pool_arc = std::sync::Arc::new(pool);

    // Enqueue tasks
    let enqueue_tasks: Vec<_> = (0..10)
        .map(|_| {
            let pool = std::sync::Arc::clone(&pool_arc);
            let task_data = json!({
                "recipient": "user@example.com",
                "subject": "Hello",
                "body": "This is a test email.",
            });

            tokio::spawn(async move {
                let task_id = postgres_queue::enqueue(
                    &pool.get().await.unwrap(),
                    "send_email",
                    task_data.clone(),
                )
                .await
                .expect("Failed to enqueue task");
                println!("Enqueued task with ID: {}", task_id);
            })
        })
        .collect();

    // Wait for all tasks to be enqueued
    for task in enqueue_tasks {
        task.await.expect("Failed to enqueue task");
    }

    // Run the task processor
    let tasks = registry
        .run(&pool_arc, num_workers)
        .await
        .expect("Failed to run tasks");

    println!("Running {} tasks", tasks.len());

    // Wait for all tasks to complete
    for task in tasks {
        task.await.expect("Task failed");
    }

    println!("All tasks completed.");
}
