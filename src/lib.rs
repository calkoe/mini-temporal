/// Library root – exposes modules to the two binaries (worker + server).
///
/// `activity` and `workflow` are private modules to avoid E0446 triggered by
/// the `#[workflow_methods]` / `#[activities]` macros leaking their
/// internally-generated `Run`/`Activities` marker types through a public
/// module boundary.  Everything the binaries actually need is re-exported
/// below.
pub mod shared;

mod activity;
mod workflow;

// Types the binaries import directly.
pub use activity::AddActivities;
pub use workflow::AddWorkflow;
