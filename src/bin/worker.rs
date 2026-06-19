/// Worker binary – polls the "math-task-queue" task queue and executes
/// AddWorkflow / add_numbers activity tasks assigned by the Temporal server.
///
/// Run multiple copies in parallel to demonstrate Temporal's load-balancing:
///   cargo run --bin worker   # terminal 1
///   cargo run --bin worker   # terminal 2
///   cargo run --bin worker   # terminal 3
///
/// Each instance receives a unique ID: worker-<hostname>-<pid>.
use anyhow::Result;
use tracing_subscriber::EnvFilter;
use url::Url;

use mini_temporal::{
    shared::{init_globals, NAMESPACE, TASK_QUEUE, TEMPORAL_SERVER},
    AddActivities, AddWorkflow,
};
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions};

#[tokio::main]
async fn main() -> Result<()> {
    // ── logging ────────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("mini_temporal=info,warn")),
        )
        .init();

    // ── worker identity ────────────────────────────────────────────────────
    // Use hostname + PID so each parallel worker instance has a unique ID
    // that is visible in the UI and in server logs.
    let hostname = hostname();
    let pid = std::process::id();
    let worker_id = format!("worker-{hostname}-{pid}");

    tracing::info!(worker_id = %worker_id, "Starting Temporal worker");

    // ── Temporal client ────────────────────────────────────────────────────
    let target_url: Url = TEMPORAL_SERVER.parse()?;
    let connection = Connection::connect(
        ConnectionOptions::new(target_url)
            .build(),
    )
    .await?;
    // ClientOptions::new(namespace) is the canonical constructor.
    let client = Client::new(connection, ClientOptions::new(NAMESPACE).build())?;

    // Store client + worker_id so activities can send signals back to their
    // parent workflow without needing to carry a client reference explicitly.
    init_globals(client.clone(), worker_id.clone());

    // ── core runtime (reuses the current tokio runtime) ────────────────────
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder().build().map_err(|e| anyhow::anyhow!("{e}"))?,
    )?;

    // ── worker registration ─────────────────────────────────────────────────
    // register_activities takes a value of the activities struct (here a unit
    // struct); the macro generates the dispatch glue automatically.
    let worker_options = WorkerOptions::new(TASK_QUEUE)
        // Sets the identity reported to the Temporal server for this worker.
        .client_identity_override(worker_id.clone())
        .register_activities(AddActivities)
        .register_workflow::<AddWorkflow>()
        .build();

    tracing::info!(
        worker_id = %worker_id,
        task_queue = TASK_QUEUE,
        "Registered; polling for tasks …",
    );

    Worker::new(&runtime, client, worker_options)
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .run()
        .await?;

    Ok(())
}

/// Returns the machine hostname, falling back to the HOSTNAME env-var or "localhost".
fn hostname() -> String {
    // Try to run the system `hostname` command first (most portable).
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if s.is_empty() { None } else { Some(s) }
        })
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "localhost".to_string())
}
