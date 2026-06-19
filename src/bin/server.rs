/// HTTP server binary (axum).
///
/// Routes:
///   POST /calculate          – start a new addition workflow
///   GET  /result/:workflow_id – poll the live status / final result
///   GET  /workers            – list pollers on the math-task-queue
///   GET  /                   – serve static/index.html
///   GET  /static/*           – serve other static assets
///
/// Status model
/// ┌───────────────────────────────────────────────────────┐
/// │ PENDING   workflow started, activity not yet claimed  │
/// │ RUNNING   worker claimed task, progress 1-5/5         │
/// │ COMPLETED x + y = result, worker_id reported          │
/// │ FAILED    workflow error (stored in Temporal history)  │
/// └───────────────────────────────────────────────────────┘
use std::{net::SocketAddr, sync::Arc};

use tonic;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::json;
use tower_http::services::ServeDir;
use tracing_subscriber::EnvFilter;
use url::Url;
use uuid::Uuid;

use mini_temporal::{
    shared::{AddInput, AddResult, WorkflowStatus, NAMESPACE, TASK_QUEUE, TEMPORAL_SERVER},
    AddWorkflow,
};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowDescribeOptions,
    WorkflowGetResultOptions, WorkflowQueryOptions, WorkflowStartOptions,
};
use temporalio_common::protos::temporal::api::{
    taskqueue::v1::TaskQueue,
    workflowservice::v1::DescribeTaskQueueRequest,
};

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    client: Client,
}

// ---------------------------------------------------------------------------
// API request / response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CalcRequest {
    x: i64,
    y: i64,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /calculate  – start an add_workflow and return its ID.
async fn calculate(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CalcRequest>,
) -> impl IntoResponse {
    let workflow_id = format!("add-{}-{}-{}", req.x, req.y, &Uuid::new_v4().to_string()[..8]);

    tracing::info!(workflow_id = %workflow_id, x = req.x, y = req.y, "Starting workflow");

    let start_opts = WorkflowStartOptions::new(TASK_QUEUE, &workflow_id)
        .build();

    match state
        .client
        .start_workflow(AddWorkflow::run, AddInput { x: req.x, y: req.y }, start_opts)
        .await
    {
        Ok(_handle) => (
            StatusCode::OK,
            Json(json!({ "workflow_id": workflow_id })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// GET /result/:workflow_id – return live status or final result.
async fn get_result(
    State(state): State<Arc<AppState>>,
    Path(workflow_id): Path<String>,
) -> impl IntoResponse {
    let handle = state
        .client
        .get_workflow_handle::<AddWorkflow>(&workflow_id);

    // ── Describe the workflow to determine high-level status ────────────────
    let desc = match handle.describe(WorkflowDescribeOptions::default()).await {
        Ok(d) => d,
        Err(e) => {
            let msg = e.to_string();
            tracing::warn!(workflow_id = %workflow_id, error = %msg, "describe failed");
            // Distinguish "not found" from other errors.
            let code = if msg.to_lowercase().contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            return (code, Json(json!({ "error": msg }))).into_response();
        }
    };

    // close_time() is Some only when the workflow has fully stopped.
    if desc.close_time().is_none() {
        // ── RUNNING or PENDING ──────────────────────────────────────────────
        // Query the workflow for the status stored by progress_update signals.
        match handle
            .query(AddWorkflow::get_status, (), WorkflowQueryOptions::default())
            .await
        {
            Ok(WorkflowStatus { worker_id: None, .. }) => {
                // No signal received yet → activity has not been claimed.
                (
                    StatusCode::OK,
                    Json(json!({
                        "status": "PENDING",
                        "worker_id": null,
                        "progress": null,
                        "result": null,
                        "error": null,
                    })),
                )
                    .into_response()
            }
            Ok(WorkflowStatus { worker_id, progress }) => {
                // Activity is running, progress is known.
                (
                    StatusCode::OK,
                    Json(json!({
                        "status": "RUNNING",
                        "worker_id": worker_id,
                        "progress": progress,
                        "result": null,
                        "error": null,
                    })),
                )
                    .into_response()
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("query failed: {e}") })),
            )
                .into_response(),
        }
    } else {
        // ── COMPLETED or FAILED ─────────────────────────────────────────────
        // get_result() returns immediately because the workflow is already done.
        match handle
            .get_result(WorkflowGetResultOptions::default())
            .await
        {
            Ok(AddResult { sum, worker_id }) => {
                tracing::info!(
                    workflow_id = %workflow_id,
                    result      = sum,
                    worker      = %worker_id,
                    "Workflow completed"
                );
                (
                    StatusCode::OK,
                    Json(json!({
                        "status": "COMPLETED",
                        "worker_id": worker_id,
                        "progress": { "current": 5, "total": 5 },
                        "result": sum,
                        "error": null,
                    })),
                )
                    .into_response()
            }
            Err(e) => {
                tracing::warn!(workflow_id = %workflow_id, error = %e, "Workflow failed");
                (
                    StatusCode::OK,
                    Json(json!({
                        "status": "FAILED",
                        "worker_id": null,
                        "progress": null,
                        "result": null,
                        "error": e.to_string(),
                    })),
                )
                    .into_response()
            }
        }
    }
}

/// GET /workers – return all pollers currently registered on math-task-queue.
///
/// Calls Temporal's DescribeTaskQueue RPC (activity queue type) so the browser
/// can display a live list of connected workers and how recently they polled.
async fn get_workers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let req = DescribeTaskQueueRequest {
        namespace: NAMESPACE.to_string(),
        task_queue: Some(TaskQueue {
            name: TASK_QUEUE.to_string(),
            ..Default::default()
        }),
        task_queue_type: 2, // TASK_QUEUE_TYPE_ACTIVITY
        ..Default::default()
    };

    let mut svc = state.client.connection().workflow_service();
    match svc
        .describe_task_queue(tonic::Request::new(req))
        .await
    {
        Ok(resp) => {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let workers: Vec<serde_json::Value> = resp
                .into_inner()
                .pollers
                .into_iter()
                .map(|p| {
                    let secs_ago = p
                        .last_access_time
                        .map(|t| (now_secs - t.seconds).max(0))
                        .unwrap_or(0);
                    json!({ "id": p.identity, "last_seen_secs_ago": secs_ago })
                })
                .collect();
            (StatusCode::OK, Json(json!({ "workers": workers }))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("mini_temporal=info,warn")),
        )
        .init();

    // ── Temporal client ────────────────────────────────────────────────────
    let target_url: Url = TEMPORAL_SERVER.parse()?;
    let connection = Connection::connect(
        ConnectionOptions::new(target_url).build(),
    )
    .await?;
    let client = Client::new(connection, ClientOptions::new(NAMESPACE).build())?;

    let state = Arc::new(AppState { client });

    // ── Router ─────────────────────────────────────────────────────────────
    let app = Router::new()
        .route("/calculate", post(calculate))
        .route("/result/{workflow_id}", get(get_result))
        .route("/workers", get(get_workers))
        // Serve static/index.html at the root and anything under /static/.
        .nest_service("/static", ServeDir::new("static"))
        .route(
            "/",
            get(|| async {
                Html(include_str!("../../static/index.html"))
            }),
        )
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    tracing::info!("HTTP server listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
