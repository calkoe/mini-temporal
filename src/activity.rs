/// Temporal activity: performs the actual addition with an artificial 5-second delay.
///
/// Every second the activity:
///  1. Sends a Temporal heartbeat (allows cancellation and progress persistence).
///  2. Signals the parent workflow with the current progress so the query handler
///     can relay it to the API server in real-time.
///
/// Worker-ID is captured from the process-level global set at worker startup, so
/// each activity report carries the identity of the physical worker that ran it.
use std::time::Duration;

use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::shared::{global_worker_id, AddInput, AddResult};
use crate::workflow::send_progress_signal;

/// Zero-sized unit struct; state is held in the process globals (worker_id, client).
pub struct AddActivities;

#[activities]
impl AddActivities {
    /// The activity registered on the "math-task-queue" task queue.
    /// Execution time: ~5 seconds (one second per step, 5 steps).
    #[activity]
    pub async fn add_numbers(ctx: ActivityContext, input: AddInput) -> Result<AddResult, ActivityError> {
        let worker_id = global_worker_id().to_string();
        let workflow_id = ctx
            .info()
            .workflow_execution
            .as_ref()
            .map(|w| w.workflow_id.clone())
            .unwrap_or_default();
        const STEPS: u32 = 5;

        tracing::info!(
            worker  = %worker_id,
            wf      = %workflow_id,
            x       = input.x,
            y       = input.y,
            "add_numbers: activity started",
        );

        // Announce that we have claimed the task (progress 0 / STEPS).
        send_progress_signal(&workflow_id, worker_id.clone(), 0, STEPS).await;

        for step in 1..=STEPS {
            tokio::time::sleep(Duration::from_secs(1)).await;

            // Temporal heartbeat: informs the server we are still alive and
            // allows an operator to cancel the activity cleanly mid-run.
            ctx.record_heartbeat(vec![]);

            tracing::info!(worker = %worker_id, step, total = STEPS, "heartbeat {step}/{STEPS}");

            send_progress_signal(&workflow_id, worker_id.clone(), step, STEPS).await;
        }

        let sum = input.x + input.y;

        tracing::info!(
            worker = %worker_id,
            result = sum,
            "add_numbers: activity completed",
        );

        Ok(AddResult { sum, worker_id })
    }
}
