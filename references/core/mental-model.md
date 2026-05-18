# The Mental Model Shift: Moving to Temporal

Before mapping any specific concept from your source tool, internalize this foundational shift. It explains why many migrations stall or produce fragile results.

---

## The Core Problem These Tools All Share

Every tool you are migrating FROM — BPMN engines, job schedulers, low-code platforms, DAG orchestrators — shares a common design philosophy: **your process lives outside your code**.

- In Camunda/BPMN: your process is an XML file deployed to an engine. Your code writes `JavaDelegate` plugins that the engine calls.
- In Control-M/Tidal: your process is a job plan configured in a UI. Your code is a script the scheduler invokes.
- In Airflow: your process is a DAG Python file, but the *execution state* (task states, XCom, retries) is owned by the database and scheduler, not your code.
- In N8n: your process is a JSON graph. Your code is confined to isolated "code nodes" the runtime calls.

In every case, **the orchestration runtime owns the state** and calls your code. Your code is a plugin.

---

## Temporal's Inversion: Your Code Owns the Process

Temporal inverts this relationship. **Your workflow function IS the process definition.**

```python
# This is not a "job worker" plugin. This is the process itself.
@workflow.defn
class OrderFulfillmentWorkflow:
    @workflow.run
    async def run(self, order: Order) -> FulfillmentResult:
        # The full process lives here, in regular code.
        inventory_result = await workflow.execute_activity(
            reserve_inventory, order, start_to_close_timeout=timedelta(minutes=5)
        )
        payment_result = await workflow.execute_activity(
            charge_payment, order, start_to_close_timeout=timedelta(minutes=2)
        )
        await workflow.execute_activity(
            ship_order, ShipRequest(order, inventory_result, payment_result),
            start_to_close_timeout=timedelta(hours=1)
        )
        return FulfillmentResult(success=True)
```

There is no XML. There is no configuration file. There is no visual graph. The process is code you own, test, and version like any other software.

---

## How Temporal Makes This Durable

You might ask: "If my code is the process, what happens when my server crashes mid-workflow?"

This is where Temporal's **durable execution** model comes in. The Temporal cluster stores a complete **event history** for every running workflow. When a Worker recovers or a new Worker starts, it replays that history through your workflow function to reconstruct the exact state the workflow was in — no matter how much time has passed.

```
Your workflow function
        │
        ▼
  Activity result comes back
        │
        ├── Event appended to durable history (persisted in Temporal cluster)
        │
        ▼
  Worker crashes
        │
        ├── New Worker starts
        │
        ▼
  Workflow function replayed from beginning
        │
        ├── SDK matches replayed commands against stored events
        │
        ▼
  Workflow resumes from exactly where it was
```

Your code doesn't need a checkpoint system, a database to persist state, or a message queue to resume. The Temporal cluster handles all of that.

---

## Key Conceptual Shifts

### 1. From "Steps" to a Continuous Function

In Airflow / BPMN / job schedulers, you think in *steps*: step 1 runs, step 2 runs, step 3 runs. Each step is stateless; state is passed between steps via external mechanisms (XCom, process variables, shared DB).

In Temporal, you write a **continuous function** with normal control flow. Local variables persist across `await` calls (because replay reconstructs them). You don't need to externalize state between steps.

```python
# Old mindset: three stateless steps sharing state via XCom / process vars
# step1 → push result to XCom → step2 pulls it → push result → step3 pulls it

# Temporal mindset: one continuous function with local variables
async def run(self):
    result1 = await workflow.execute_activity(step1)       # result1 is a local variable
    result2 = await workflow.execute_activity(step2, result1)  # passed directly
    result3 = await workflow.execute_activity(step3, result2)  # no external store needed
```

### 2. From "Gateway/Branch config" to Regular Code

In BPMN, branching is an Exclusive Gateway — an XML element with conditions. In Airflow, it's a `BranchPythonOperator`. In N8n, it's a Switch node.

In Temporal, branching is just `if/else`:

```python
async def run(self, order: Order):
    if order.value > 10_000:
        result = await workflow.execute_activity(manual_approval_activity, order)
    else:
        result = await workflow.execute_activity(auto_approve_activity, order)
```

This also applies to parallel execution (no Parallel Gateway XML needed), loops (no Loop construct needed), and error handling (no Error Boundary Event needed — just `try/except`).

### 3. From "Retry configuration in the engine" to Retry Policies in Code

In Control-M/Tidal, you configure retries in the job scheduler UI. In BPMN engines, retry behavior is configured on service tasks. In Airflow, it's `retries=3` on the operator.

In Temporal, retry policies are part of the activity options defined in code:

```python
result = await workflow.execute_activity(
    call_payment_api,
    args=[order_id],
    start_to_close_timeout=timedelta(minutes=5),
    retry_policy=RetryPolicy(
        maximum_attempts=5,
        initial_interval=timedelta(seconds=10),
        backoff_coefficient=2.0,
        non_retryable_error_types=["InvalidCardError"],
    ),
)
```

Retry configuration lives with the code, is testable, and is version-controlled.

### 4. From "External State Store" to Workflow State

In most tools, state between steps lives in a database, process variable store, or message payload. If the store becomes inconsistent with the running process, you get bugs.

In Temporal, state is implicit in the workflow's event history. You don't need an external store for "what step am I on" or "what was the result of step 2." The SDK reconstructs all of that during replay.

> **Exception**: Activity implementations are intentionally stateless and should store external side effects in external systems (databases, APIs). Only the *orchestration* state is managed by Temporal.

### 5. From "Monitoring Dashboard" to Temporal Web UI + CLI

Every tool you're migrating from has its own monitoring UI. Temporal provides the Temporal Web UI and `temporal` CLI. These show:
- All running and completed workflow executions
- Full event history for each execution (every activity input/output, every signal received)
- Current workflow state (blocked on which activity or timer)
- Search by workflow type, status, custom search attributes

This is not something you need to build — it's built into the platform.

---

## What Stays the Same

Not everything is different. These concepts carry over directly:

| Old Concept | In Temporal |
|---|---|
| "Do I need to retry this?" | Yes — put the operation in an Activity with a RetryPolicy |
| "I need to run this on a schedule" | Yes — use a Temporal Schedule |
| "I need parallel execution" | Yes — use language-native parallel constructs (goroutines, async/await, etc.) |
| "I need human approval / pause" | Yes — use a Signal or Update to pause and resume the workflow |
| "I need to know what's running" | Yes — use Temporal Web UI, CLI, or search via Visibility API |

---

## The Determinism Requirement

One constraint that Temporal introduces that most other tools do not have: **workflow code must be deterministic**.

Because workflows are replayed from event history, the same code path must execute the same way on replay as it did originally. This means:

- **No I/O in workflow code** — no HTTP calls, no DB queries, no file reads. Put those in Activities.
- **No random numbers or timestamps** — use `workflow.now()` instead of `datetime.now()`, `workflow.random()` instead of `random.random()`.
- **No threading/goroutine creation** outside of Temporal's API.
- **No global mutable state** read inside workflow code.

This is the most important rule to internalize before writing any Temporal workflow code. See the `temporal-developer` skill's `references/core/determinism.md` for a complete treatment.

---

## Summary: The Three Things to Internalize

1. **Your workflow function is the process.** There is no XML, no JSON graph, no external definition. The code *is* the workflow.
2. **Activities are where side effects live.** Every I/O operation — API calls, DB queries, file reads, sending messages — goes in an activity, which can fail and be retried independently.
3. **Temporal handles durability.** You do not need checkpoints, external state stores, or restart logic. Write linear code; Temporal makes it survive failures.
