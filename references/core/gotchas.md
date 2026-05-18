# Migration Gotchas: Anti-Patterns and Common Mistakes

These are the most common mistakes made when migrating to Temporal from other orchestration tools. Most stem from carrying over the mental model of the source tool into Temporal code.

---

## 1. Reconstructing the Source Tool's Structure in Code

**The mistake**: After migrating from BPMN, you write a workflow that has a function per gateway, a function per activity, and a dispatch loop that calls them in sequence — essentially re-implementing a BPMN engine in code.

After migrating from Airflow, you write a workflow that builds a task graph at runtime and "executes" tasks in topological order.

**Why it's a problem**: This defeats the entire purpose of using Temporal. It adds complexity, breaks testability, and makes the code harder to read than the original tool.

**What to do instead**: Write the process as a straightforward function with normal control flow. The DAG, the gateway logic, the step sequence — that's just `if/else`, `for`, `try/except`, and function calls.

```python
# Anti-pattern: reconstructing a BPMN engine
def execute_process(self, process_definition):
    for step in process_definition.steps:
        if step.type == "service_task":
            self.run_activity(step.activity_name)
        elif step.type == "gateway":
            self.evaluate_gateway(step)
        ...

# Correct: just write the process
async def run(self, order):
    inventory = await workflow.execute_activity(check_inventory, order)
    if not inventory.available:
        await workflow.execute_activity(notify_out_of_stock, order)
        return OrderResult(status="OUT_OF_STOCK")
    await workflow.execute_activity(charge_payment, order)
    await workflow.execute_activity(ship_order, order)
```

---

## 2. Doing I/O Inside Workflow Code

**The mistake**: Placing database queries, HTTP calls, file reads, or any I/O directly inside the workflow function (not in an activity).

```python
# WRONG — I/O inside workflow code
@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str):
        # THIS IS WRONG — directly querying DB in workflow
        order = await db.fetch_one("SELECT * FROM orders WHERE id = $1", order_id)
        response = await httpx.post("https://payment-api.com/charge", json=order)
        ...
```

**Why it's a problem**: Workflow code is **replayed** from history. If the DB returns different results on replay, Temporal will see a non-determinism error and block the workflow. I/O has side effects, is non-deterministic (network failures, different data), and can't be safely replayed.

**What to do instead**: Every I/O operation goes in an Activity. Activities are safe to retry, are not replayed, and can fail without corrupting the workflow.

```python
# CORRECT — I/O in activities
@activity.defn
async def fetch_order(order_id: str) -> Order:
    return await db.fetch_one("SELECT * FROM orders WHERE id = $1", order_id)

@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str):
        order = await workflow.execute_activity(fetch_order, order_id)  # ← correct
```

**Migrating from TIBCO BW**: This is especially common because TIBCO BW processes freely mix JDBC and HTTP activities with process logic. Every BW Activity that touches an external system becomes a Temporal Activity.

---

## 3. Using `datetime.now()`, `random()`, or Other Non-Deterministic Calls in Workflow Code

**The mistake**: Calling `datetime.now()`, `time.time()`, `random.random()`, `uuid.uuid4()`, or any other function that returns different values on each call.

```python
# WRONG
@workflow.run
async def run(self):
    if datetime.now().hour < 12:  # Different on replay!
        await workflow.execute_activity(morning_task)
```

**Why it's a problem**: These calls return different values on replay, causing a non-determinism error.

**What to do instead**: Use Temporal's deterministic equivalents:

```python
# Python
now = workflow.now()                    # instead of datetime.now()
rand = workflow.random().random()       # instead of random.random()
uid = workflow.uuid4()                  # instead of uuid.uuid4()

# Go
t := workflow.Now(ctx)
n := workflow.NewRandom(ctx).Intn(100)

# Java
Date now = Workflow.currentTimeMillis();
Random rand = Workflow.newRandom();
```

**Migrating from Airflow**: Airflow tasks receive `execution_date` via context. In Temporal, the schedule time is available via `workflow.now()` at the time the workflow starts, or passed as an input parameter from the Schedule trigger.

---

## 4. Non-Idempotent Activities Without Idempotency Keys

**The mistake**: Writing Activities that have side effects (charging a payment, sending an email, creating a record) without making them idempotent.

**Why it's a problem**: Temporal **will retry Activities** on failure. If an activity sends an email and then fails before returning, Temporal retries it — and sends a duplicate email. If a payment is charged and fails after the charge but before returning, the retry double-charges.

**What to do instead**: Make activities idempotent. Use idempotency keys.

```python
@activity.defn
async def charge_payment(order_id: str, amount: int) -> ChargeResult:
    # Use order_id as an idempotency key — payment provider deduplicates
    response = await stripe.charges.create(
        amount=amount,
        currency="usd",
        idempotency_key=f"order-{order_id}",  # ← idempotency key
    )
    return ChargeResult(charge_id=response.id)
```

**Migrating from Control-M / job schedulers**: In Control-M, a job failing meant a human would inspect and manually re-run it. Automatic retries were rare. In Temporal, automatic retry is the default. If your existing job scripts weren't written with idempotency in mind, you need to add it during migration.

---

## 5. Replicating the Old Tool's Retry Table / Audit Log

**The mistake**: Building an external "retry tracker" table in a database, a "job audit log" table, or a "workflow state" table alongside Temporal.

```python
# Anti-pattern: maintaining an external state table
@activity.defn
async def process_with_tracking(job_id: str):
    await db.execute("UPDATE jobs SET status='RUNNING' WHERE id=$1", job_id)
    try:
        result = do_work(job_id)
        await db.execute("UPDATE jobs SET status='DONE' WHERE id=$1", job_id)
    except Exception as e:
        await db.execute("UPDATE jobs SET status='FAILED' WHERE id=$1", job_id)
        raise
```

**Why it's a problem**: Temporal already provides a durable event history, execution state tracking, and retry semantics. Maintaining a separate status table creates a split-brain scenario where the DB and Temporal can disagree on the state of a workflow.

**What to do instead**: Use Temporal as the source of truth. Use Search Attributes to make workflow executions queryable. Use the Temporal Web UI and Visibility API for operational queries.

```python
# Correct: use Search Attributes for queryable metadata
await client.start_workflow(
    ProcessWorkflow.run,
    job_id,
    id=f"job-{job_id}",
    task_queue="jobs",
    search_attributes=TypedSearchAttributes([
        SearchAttributePair(SearchAttributeKey.for_keyword("JobId"), job_id),
        SearchAttributePair(SearchAttributeKey.for_keyword("JobType"), "nightly-batch"),
    ]),
)

# Query via Visibility API:
# temporal workflow list --query 'JobType="nightly-batch" AND ExecutionStatus="Running"'
```

---

## 6. Treating Temporal Like a Message Queue

**The mistake**: Designing a system where workflows are very short-lived ("start, call one activity, return") and using `start_workflow()` as a message queue replacement, firing thousands of tiny workflows per second.

**Why it's a problem**: Temporal workflows carry overhead (history creation, state persistence) that message queues don't. For pure fan-out/fan-in messaging patterns with no durable state, a message queue (Kafka, RabbitMQ, SQS) is more appropriate.

**What to do instead**: Use Temporal when you need durable orchestration: workflows that span multiple activities, may run for a long time, need retry logic, or need to be queryable. For high-throughput stateless message processing, consider keeping a queue and using a Temporal Activity to process messages.

**Migrating from N8n**: N8n workflows often have a single HTTP request trigger that calls an API and returns immediately. If that's all you need, don't migrate it — it's not a Temporal use case. Migrate when the workflow has multiple steps, error handling, or needs to survive restarts.

---

## 7. Forgetting Continue-As-New for Long-Running Loops

**The mistake**: Writing a `while True` loop in a workflow that runs forever without ever calling `continue_as_new()`.

```python
# Problematic for very long-running workflows
@workflow.run
async def run(self, config):
    while True:
        await workflow.execute_activity(check_and_process, config)
        await workflow.sleep(timedelta(minutes=5))
        # After months of running, history is enormous → slow replays, high memory
```

**Why it's a problem**: Every activity execution and timer creates events in the workflow's history. Over time, the history grows unboundedly. Large histories cause slow replay, high memory usage, and eventually hit size limits (default 50,000 events or 50MB).

**What to do instead**: Use `continue_as_new()` periodically to start a fresh execution with a clean history while preserving state as the new execution's input.

```python
@workflow.run
async def run(self, state: PollerState):
    for _ in range(100):  # process 100 iterations per execution
        await workflow.execute_activity(check_and_process, state)
        await workflow.sleep(timedelta(minutes=5))
        state.iteration_count += 1

    # Start fresh execution with accumulated state
    workflow.continue_as_new(state)
```

**Migrating from Control-M cyclic jobs**: Control-M cyclic jobs run indefinitely. When migrating to Temporal, use a Temporal Schedule instead of an infinite loop — it handles the scheduling durably without accumulating history.

---

## 8. Skipping Activity Timeouts (Relying on Infinite Retry)

**The mistake**: Not setting `start_to_close_timeout` on activities, or setting it to an extremely large value "just in case."

**Why it's a problem**: Without a timeout, an activity that hangs (due to a deadlock, infinite loop, or network stall) will block the workflow indefinitely. Temporal cannot detect a stuck activity without a heartbeat or timeout.

**What to do instead**: Set a realistic `start_to_close_timeout` for every activity. For long-running activities, combine a generous timeout with regular `heartbeat()` calls so that a stuck activity is detected quickly.

```python
# Activities that run fast
await workflow.execute_activity(
    validate_input, data,
    start_to_close_timeout=timedelta(seconds=30),
)

# Activities that run longer
await workflow.execute_activity(
    run_ml_training, config,
    start_to_close_timeout=timedelta(hours=4),
    heartbeat_timeout=timedelta(minutes=5),  # detect if worker dies
)
```

**Migrating from Airflow**: Airflow's `execution_timeout` on a task is a direct equivalent. Set `start_to_close_timeout` to match your existing `execution_timeout` values.

---

## 9. Sharing Mutable State Between Activity Instances

**The mistake**: Using a class-level or module-level mutable variable in an Activity implementation to share state between concurrent activity executions.

```python
# WRONG — shared mutable state between concurrent activity invocations
class OrderActivitiesImpl:
    results = []  # ← shared mutable state

    async def process_order(self, order_id: str):
        result = do_work(order_id)
        self.results.append(result)  # ← race condition under concurrent execution
```

**Why it's a problem**: Multiple workflow executions may invoke the same activity type concurrently on the same worker. Shared mutable state causes race conditions.

**What to do instead**: Activity instances should be stateless. Use thread-safe components (connection pools, metrics clients) at the class level, but never mutable result state.

---

## 10. Not Testing Workflow Code

**The mistake**: Only integration-testing the full workflow end-to-end against a live Temporal cluster, never unit-testing the workflow logic.

**Why it's a problem**: Workflow code with complex branching (replacing BPMN gateways, Airflow trigger rules, Control-M `ONERR` conditions) needs thorough test coverage. Running every branch through an integration test is slow and brittle.

**What to do instead**: Use the Temporal test framework to unit-test workflow code with mocked activities. Every BPMN gateway path, every Airflow trigger rule variant, every error path becomes a test case.

```python
# Python: unit testing workflow with mocked activities
async def test_low_stock_path():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="test", workflows=[OrderWorkflow],
                          activities=[mock_check_inventory, mock_notify_out_of_stock]):
            result = await env.client.execute_workflow(
                OrderWorkflow.run,
                Order(id="123", quantity=100),
                id="test-order",
                task_queue="test",
            )
    assert result.status == "OUT_OF_STOCK"
```

**Rule of thumb**: One test per gateway path (BPMN), one test per trigger rule variant (Airflow), one test per ONERR condition (Control-M).
