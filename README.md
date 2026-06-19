# mini-temporal

A minimal but complete demo of [Temporal](https://temporal.io) workflow orchestration written in Rust.

The premise is deliberately simple — adding two numbers — so the focus stays on Temporal's mechanics rather than business logic. Enter X and Y in a browser, watch Temporal dispatch the work to one of several running workers, and observe live progress, worker identity, and automatic failover.

---

## What this demo shows

| Concept             | How it appears here                                                    |
| ------------------- | ---------------------------------------------------------------------- |
| **Workflows**       | `AddWorkflow` — the durable, replayable orchestrator                   |
| **Activities**      | `add_numbers` — the actual work, running on a worker process           |
| **Task queues**     | Workers compete for tasks on `math-task-queue`                         |
| **Load balancing**  | Three workers, each calculation goes to a different one                |
| **Heartbeating**    | Activity signals the server every second it's still alive              |
| **Signals**         | Activity → Workflow: live progress updates                             |
| **Queries**         | API server asks the workflow for current state without interrupting it |
| **Fault tolerance** | Kill a worker mid-task; Temporal reassigns after heartbeat timeout     |

---

## How it works

```
Browser
  │
  │  POST /calculate
  ▼
HTTP Server (axum, :8080)
  │
  │  client.start_workflow(AddWorkflow, {x, y})
  ▼
Temporal Server (:7233)
  │
  │  dispatches AddWorkflow task
  ▼
Worker process  ←──────────────────────── one of N workers polling math-task-queue
  │
  │  schedules add_numbers activity
  │
  │  Activity loop (5 × 1 s):
  │    ├── ctx.record_heartbeat()          ← tells Temporal "still alive"
  │    └── client.signal(progress_update)  ← pushes progress into workflow state
  │
  │  returns AddResult { sum, worker_id }
  ▼
Workflow completes, result stored in Temporal history

  Meanwhile, every ~1 s the browser polls:
  GET /result/{workflow_id}
    ├── workflow running  →  handle.query(get_status)  →  { status, worker_id, progress }
    └── workflow done     →  handle.get_result()        →  { status, sum, worker_id }
```

### Progress tracking in detail

The live progress bar is powered by a signal/query pattern:

1. The **activity** sends a `ProgressSignal` to its parent workflow at each step
2. The **workflow** stores `worker_id` and `progress` in its own state (signal handler `progress_update`)
3. The **API server** reads that state on every poll via a query handler (`get_status`)
4. The **browser** renders the result — no WebSocket, no database, just Temporal's built-in state

### Status lifecycle

```
PENDING   →   workflow started, no worker has claimed the activity yet
RUNNING   →   a worker is executing; worker_id and progress are known
COMPLETED →   activity returned a result; sum is available
FAILED    →   workflow error (visible in Temporal Web UI at :8233)
```

---

## Prerequisites

- **Rust** ≥ 1.76 (stable, edition 2021) — install via [rustup.rs](https://rustup.rs)
- **protoc** (Protocol Buffers compiler) — needed by the SDK build scripts
  ```bash
  brew install protobuf        # macOS
  apt install -y protobuf-compiler  # Debian/Ubuntu
  ```
- **Temporal server** — either Docker Compose (below) or the [temporal CLI](https://docs.temporal.io/cli)

---

## Running the demo

### 1 — Start the Temporal server

**Option A: Docker Compose (recommended)**

```bash
docker compose up -d
```

This starts three containers:

| Container     | Purpose                        | Port |
| ------------- | ------------------------------ | ---- |
| `postgresql`  | Temporal's persistence backend | 5432 |
| `temporal`    | Temporal server (gRPC)         | 7233 |
| `temporal-ui` | Web UI                         | 8233 |

The UI container starts immediately but the Temporal server takes ~30–60 s to finish schema setup. Check progress with:

```bash
docker compose ps
```

Once `temporal` shows `(healthy)`, everything is ready. Temporal Web UI → **http://localhost:8233**

**Option B: temporal CLI**

```bash
temporal server start-dev --ui-port 8233
```

The dev server is a single binary with SQLite — no Docker needed. The Web UI is at the same address.

---

### 2 — Start the HTTP server

```bash
cargo run --bin server
# INFO  HTTP server listening on http://0.0.0.0:8080
```

---

### 3 — Start multiple workers

Open three terminals and run the worker in each:

```bash
cargo run --bin worker
```

Each worker gets a unique ID derived from hostname + PID:

```
INFO  worker-macbook-pro-83241  Registered; polling for tasks …
INFO  worker-macbook-pro-83299  Registered; polling for tasks …
INFO  worker-macbook-pro-83341  Registered; polling for tasks …
```

---

### 4 — Open the UI

Go to **http://localhost:8080**, enter two numbers, click **Calculate**.

You will see:

- **Status badge** cycling through PENDING → RUNNING → COMPLETED
- **Worker ID** showing which worker handled the task
- **Progress bar** advancing 1/5 → 5/5 over five seconds
- **Result** displayed prominently once complete

---

## Things to try

### Load balancing

Submit several calculations in quick succession. Each goes to the next available worker — you can see different worker IDs in the UI and in the Temporal Web UI at :8233.

```
Calculation 1  →  worker-macbook-pro-83241
Calculation 2  →  worker-macbook-pro-83299
Calculation 3  →  worker-macbook-pro-83341
Calculation 4  →  worker-macbook-pro-83241  ← round-trips back
```

### Fault tolerance

1. Start a calculation and note which worker ID appears in the UI.
2. Press `Ctrl+C` in that worker's terminal while the progress bar is moving.
3. The progress bar will pause.
4. After the heartbeat timeout (~10 s for this demo, up to 30 s default), Temporal detects the missed heartbeat and reschedules the activity.
5. A surviving worker picks it up — you'll see a new worker ID appear.

The calculation completes correctly. The caller never needed to know anything failed.

---

## Project structure

```
mini-temporal/
├── Cargo.toml
├── docker-compose.yml
├── src/
│   ├── lib.rs          re-exports AddWorkflow and AddActivities
│   ├── shared.rs       shared types, constants, process-global client + worker ID
│   ├── workflow.rs     AddWorkflow — run / signal / query handlers
│   ├── activity.rs     AddActivities::add_numbers — 5 s loop with heartbeat
│   └── bin/
│       ├── server.rs   axum HTTP server  (POST /calculate, GET /result/{id})
│       └── worker.rs   Temporal worker process
└── static/
    └── index.html      single-page UI (vanilla JS, 1 s polling)
```

### Key files

**`src/workflow.rs`** — the durable orchestrator

```rust
#[run]   // called once by Temporal; schedules the activity and waits
#[signal] fn progress_update(...)  // activity pushes progress here
#[query]  fn get_status(...)       // API server reads state here
```

**`src/activity.rs`** — the actual work

```rust
for step in 1..=5 {
    sleep(1s);
    ctx.record_heartbeat(vec![]);        // "I'm still alive"
    send_progress_signal(step, 5).await; // update workflow state
}
```

**`src/bin/worker.rs`** — registers both workflow and activity, then polls forever

```rust
WorkerOptions::new(TASK_QUEUE)
    .register_workflow::<AddWorkflow>()
    .register_activities(AddActivities)
    .build()
```

---

## Environment

| Variable   | Default                   | Notes                                 |
| ---------- | ------------------------- | ------------------------------------- |
| `RUST_LOG` | `mini_temporal=info,warn` | Increase to `debug` for SDK internals |

To point at a different Temporal server, change `TEMPORAL_SERVER` in `src/shared.rs`.
