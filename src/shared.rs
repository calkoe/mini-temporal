/// Shared types, constants, and cross-binary globals for the mini-temporal demo.
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use temporalio_client::Client;

pub const TASK_QUEUE: &str = "math-task-queue";
pub const NAMESPACE: &str = "default";
pub const TEMPORAL_SERVER: &str = "http://localhost:7233";

// ---------------------------------------------------------------------------
// Wire types (serialised as Temporal payloads / JSON API responses)
// ---------------------------------------------------------------------------

/// Input for the addition workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddInput {
    pub x: i64,
    pub y: i64,
}

/// The activity (and workflow) return value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddResult {
    pub sum: i64,
    pub worker_id: String,
}

/// Progress update sent by the activity as a workflow signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressSignal {
    pub worker_id: String,
    pub current: u32,
    pub total: u32,
}

/// Progress snapshot stored in the workflow state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Progress {
    pub current: u32,
    pub total: u32,
}

/// What the workflow query handler returns (read by the API server).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStatus {
    pub worker_id: Option<String>,
    pub progress: Option<Progress>,
}

// ---------------------------------------------------------------------------
// Global singletons initialised by the worker binary at startup.
// The server binary never calls these; it only uses its own Temporal client.
// ---------------------------------------------------------------------------

/// The worker's Temporal client (used by activities to signal the workflow).
/// Client is Send + Sync because it wraps an Arc<Channel>.
static GLOBAL_CLIENT: OnceLock<Client> = OnceLock::new();

/// The human-readable ID of this worker process, e.g. "worker-macbook-12345".
static GLOBAL_WORKER_ID: OnceLock<String> = OnceLock::new();

/// Called once in the worker binary after the client is created.
pub fn init_globals(client: Client, worker_id: String) {
    // Ignore the error returned when the cell is already occupied (shouldn't
    // happen in normal operation but safe to swallow).
    let _ = GLOBAL_CLIENT.set(client);
    let _ = GLOBAL_WORKER_ID.set(worker_id);
}

pub fn global_client() -> Option<&'static Client> {
    GLOBAL_CLIENT.get()
}

pub fn global_worker_id() -> &'static str {
    GLOBAL_WORKER_ID.get().map(|s| s.as_str()).unwrap_or("unknown")
}
