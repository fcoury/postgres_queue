use chrono::Utc;
use postgres_queue::{connect, initialize_database};
use serde_json::json;
use std::env;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: producer <num_tasks>");
        std::process::exit(1);
    }

    let num_tasks: i32 = args[1].parse().expect("Invalid number of tasks");

    let database_url = "postgresql://postgres:postgres@localhost/queue";

    let pool = connect(database_url)
        .await
        .expect("Failed to connect to the database");

    initialize_database(&pool)
        .await
        .expect("Failed to initialize database");

    let pool_arc = std::sync::Arc::new(pool);

    // Enqueue tasks
    let enqueue_tasks: Vec<_> = (0..num_tasks)
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
                    Utc::now(), // Run the task immediately
                    None,       // No interval
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

    println!("All tasks enqueued.");
}
