/// Temporal workflow: receives x + y, delegates computation to an activity,
/// and exposes a query handler so the API server can poll live status.
///
/// Progress tracking uses signals sent by the activity back to the workflow:
///   Activity ──signal──> Workflow state ──query──> API server ──JSON──> UI
use std::time::Duration;

use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};

use temporalio_client::WorkflowSignalOptions;

use crate::activity::AddActivities;
use crate::shared::{global_client, AddInput, AddResult, Progress, ProgressSignal, WorkflowStatus};

#[workflow]
pub struct AddWorkflow {
    /// Original inputs – needed because #[run] doesn't receive parameters directly.
    x: i64,
    y: i64,
    /// Set by the first progress_update signal (identifies which worker claimed the task).
    worker_id: Option<String>,
    /// Updated every second by the activity via signals.
    progress: Option<Progress>,
}

#[workflow_methods]
impl AddWorkflow {
    /// Temporal calls #[init] once to construct the workflow state from the serialised input.
    #[init]
    fn new(_ctx: &WorkflowContextView, input: AddInput) -> Self {
        Self {
            x: input.x,
            y: input.y,
            worker_id: None,
            progress: None,
        }
    }

    /// Main workflow body – schedules the add_numbers activity and awaits its result.
    /// All actual computation (including the 5 s delay) happens in the activity, which
    /// keeps this workflow function deterministic and replayable.
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<AddResult> {
        let (x, y) = ctx.state(|s| (s.x, s.y));

        tracing::info!(x, y, "add_workflow: scheduling activity");

        let result = ctx
            .start_activity(
                AddActivities::add_numbers,
                AddInput { x, y },
                // Give the activity plenty of time; it will finish in ~5 s.
                ActivityOptions::start_to_close_timeout(Duration::from_secs(120)),
            )
            .await?;

        tracing::info!(sum = result.sum, worker = %result.worker_id, "add_workflow: completed");

        Ok(result)
    }

    /// Receives live progress updates sent by the activity via the Temporal client.
    /// Temporal guarantees signal delivery order, so the state is always consistent.
    #[signal]
    pub fn progress_update(&mut self, _ctx: &mut SyncWorkflowContext<Self>, update: ProgressSignal) {
        self.worker_id = Some(update.worker_id);
        self.progress = Some(Progress {
            current: update.current,
            total: update.total,
        });
    }

    /// Read-only query that the API server calls to get the current execution state.
    /// Queries are answered by Temporal without requiring a workflow task.
    #[query]
    pub fn get_status(&self, _ctx: &WorkflowContextView) -> WorkflowStatus {
        WorkflowStatus {
            worker_id: self.worker_id.clone(),
            progress: self.progress.clone(),
        }
    }
}

/// Fire-and-forget helper so activities can signal the parent workflow without
/// needing to name the private `progress_update` descriptor directly.
pub(crate) async fn send_progress_signal(
    workflow_id: &str,
    worker_id: String,
    current: u32,
    total: u32,
) {
    if let Some(client) = global_client() {
        let handle = client.get_workflow_handle::<AddWorkflow>(workflow_id);
        let signal = ProgressSignal { worker_id, current, total };
        if let Err(e) = handle
            .signal(AddWorkflow::progress_update, signal, WorkflowSignalOptions::default())
            .await
        {
            tracing::warn!("send_progress_signal failed: {e}");
        }
    }
}
