# Migrating from Apache Airflow to Temporal

*Verified against: Apache Airflow 2.x (2.6+)*

---

## Airflow vs. Temporal: Core Difference

**Airflow** is a DAG-based pipeline orchestrator. You define a Directed Acyclic Graph of Tasks in Python, and the Airflow Scheduler decides when and how to execute them. Task state (success, failure, running, skipped) is stored in a database. Data between tasks flows via XCom (cross-communication, stored in the database). You cannot write loops, conditional branches with dynamic structure, or long-running processes in Airflow's native model.

**Temporal** is a code-first durable execution platform. Your workflow is a regular Python function. There are no DAG constraints — you can use loops, conditionals, dynamic forks, and the workflow function can run for years. The Temporal cluster stores event history, not task state.

**When to migrate from Airflow to Temporal:**
- Your DAGs contain complex conditional logic that strains `BranchPythonOperator`
- You need long-running workflows (Airflow is typically batch-oriented, not long-running)
- You need workflows to respond to external events (Airflow's polling model adds latency)
- You need sub-minute scheduling granularity
- You want unit-testable workflow logic without an Airflow environment
- Your team is building application-facing workflows, not data pipelines

> **Note**: If your Airflow DAGs are straightforward data pipelines (ETL, ML training, data ingestion), Temporal can replace them. However, Airflow still excels at data-specific concerns (native integrations with Spark, Snowflake, dbt, Airflow providers ecosystem). Evaluate whether those are a significant part of your usage before migrating data-heavy workloads.

---

## Concept Mapping

| Airflow Concept | Temporal Equivalent |
|---|---|
| DAG | Workflow Definition (`@workflow.defn`) |
| DAG Run | Workflow Execution |
| Task | Activity (`@activity.defn`) |
| Operator | Activity implementation |
| TaskGroup | Section of workflow code; or Child Workflow for complex groups |
| Task Instance | Activity Execution |
| `schedule_interval` / `@daily` | Temporal Schedule |
| `catchup=True` | Temporal Schedule `catchup_window` |
| XCom push/pull | Activity return values stored as local variables in workflow code |
| `dag_run.conf` (runtime params) | Workflow input parameters |
| `Variable` (global key-value store) | Passed as workflow input parameters, or loaded at Worker startup |
| `Connection` (stored credentials) | Environment variables / secrets manager in Activity |
| `Pool` (concurrency limits) | Resource-based slot suppliers (see `temporal-workertuning` skill) |
| `trigger_rule` | Conditional logic in workflow code |
| `on_failure_callback` / `on_retry_callback` | `try/except` in workflow + notification Activity |
| SLA / `sla` | Workflow Run Timeout + notification Activity |
| `execution_timeout` on task | `start_to_close_timeout` on `execute_activity()` |
| `retries` on operator | `RetryPolicy.maximum_attempts` on `execute_activity()` |
| `retry_delay` | `RetryPolicy.initial_interval` |
| Sensor (polls until condition is true) | Activity with `activity.heartbeat()` polling loop |
| `TriggerDagRunOperator` | `workflow.execute_child_workflow()` or `client.start_workflow()` from Activity |
| `SubDagOperator` | Child Workflow |
| Dynamic task mapping | `for` loop calling `execute_activity()` in workflow code |
| Airflow Scheduler | Temporal Cluster (manages schedules and task dispatch) |
| Worker (Celery, K8s, Local) | Temporal Worker |
| Airflow Web UI | Temporal Web UI |
| `airflow dags trigger` | `temporal workflow start` or `client.start_workflow()` |

---

## Operator-by-Operator Migration

### PythonOperator → Activity

The most common Airflow operator. The callable becomes an Activity function.

```python
# Airflow: PythonOperator
def process_records(**context):
    date = context["ds"]  # execution date
    records = fetch_records_for_date(date)
    return transform_records(records)  # returned value goes to XCom

with DAG("daily_processing", schedule_interval="@daily") as dag:
    process_task = PythonOperator(
        task_id="process_records",
        python_callable=process_records,
    )
```

```python
# Temporal equivalent
@activity.defn
async def process_records(date: str) -> list[Record]:
    records = await fetch_records_for_date(date)
    return transform_records(records)

@workflow.defn
class DailyProcessingWorkflow:
    @workflow.run
    async def run(self, date: str) -> list[Record]:
        return await workflow.execute_activity(
            process_records, date,
            start_to_close_timeout=timedelta(hours=2),
        )
```

### BashOperator → Activity

```python
# Airflow: BashOperator
run_script = BashOperator(
    task_id="run_etl_script",
    bash_command="python /scripts/etl.py --date {{ ds }}",
)

# Temporal equivalent:
@activity.defn
async def run_etl_script(date: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        "python", "/scripts/etl.py", "--date", date,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"ETL script failed: {stderr.decode()}")
```

### Sensor → Activity with heartbeat

Airflow Sensors poll until a condition is true, blocking a worker slot the whole time. Temporal Activities with `heartbeat()` accomplish the same thing more efficiently — the heartbeat lets the cluster detect if the activity has died, and the activity can be configured with a `heartbeat_timeout` to auto-retry if heartbeats stop.

```python
# Airflow: FileSensor — wait until a file appears
wait_for_file = FileSensor(
    task_id="wait_for_input",
    filepath="/data/input/{{ ds }}.csv",
    poke_interval=60,
    timeout=3600,
)

# Temporal equivalent: polling Activity with heartbeat
@activity.defn
async def wait_for_file(file_path: str) -> str:
    """Polls until the file exists. Returns the file path when found."""
    while True:
        if os.path.exists(file_path):
            return file_path
        activity.heartbeat()  # tell Temporal we're still alive
        await asyncio.sleep(60)

# Called from workflow with heartbeat_timeout:
file_path = await workflow.execute_activity(
    wait_for_file,
    f"/data/input/{date}.csv",
    start_to_close_timeout=timedelta(hours=2),
    heartbeat_timeout=timedelta(minutes=5),  # retry if no heartbeat for 5 min
)
```

### HttpSensor / ExternalTaskSensor → polling Activity

```python
# Airflow: HttpSensor — poll until endpoint returns 200
wait_for_api = HttpSensor(
    task_id="wait_for_api",
    http_conn_id="my_api",
    endpoint="status/{{ run_id }}",
    poke_interval=30,
)

# Temporal equivalent:
@activity.defn
async def wait_for_api_ready(run_id: str) -> None:
    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get(f"https://api.example.com/status/{run_id}") as resp:
                if resp.status == 200:
                    return
            activity.heartbeat()
            await asyncio.sleep(30)
```

### BranchPythonOperator → if/else

```python
# Airflow: BranchPythonOperator — choose which downstream task to run
def choose_path(**context):
    amount = context["dag_run"].conf.get("amount", 0)
    if amount > 1000:
        return "high_value_task"
    return "standard_task"

branch = BranchPythonOperator(
    task_id="choose_path",
    python_callable=choose_path,
)
high_value = PythonOperator(task_id="high_value_task", ...)
standard = PythonOperator(task_id="standard_task", ...)
branch >> [high_value, standard]

# Temporal equivalent — just if/else:
@workflow.run
async def run(self, request: ProcessRequest):
    if request.amount > 1000:
        return await workflow.execute_activity(process_high_value, request)
    else:
        return await workflow.execute_activity(process_standard, request)
```

### Task Dependencies (`>>`) → Sequential or Parallel Activity Calls

```python
# Airflow: Three tasks in sequence
extract >> transform >> load

# Temporal equivalent:
@workflow.run
async def run(self, config):
    raw = await workflow.execute_activity(extract, config)
    transformed = await workflow.execute_activity(transform, raw)
    await workflow.execute_activity(load, transformed)
```

```python
# Airflow: Parallel tasks (fan-out) then join
start >> [task_a, task_b, task_c] >> end

# Temporal equivalent:
@workflow.run
async def run(self, config):
    a, b, c = await asyncio.gather(
        workflow.execute_activity(task_a, config),
        workflow.execute_activity(task_b, config),
        workflow.execute_activity(task_c, config),
    )
    await workflow.execute_activity(end_task, a, b, c)
```

### Dynamic Task Mapping → for loop

Airflow 2.3+ added dynamic task mapping. In Temporal, dynamic execution is natural — just use a loop.

```python
# Airflow: Dynamic task mapping
@task
def process_item(item: dict) -> dict:
    ...

items = get_items()
process_item.expand(item=items)  # creates N tasks dynamically

# Temporal equivalent:
@workflow.run
async def run(self, batch_id: str):
    items = await workflow.execute_activity(get_items, batch_id)
    results = []
    for item in items:
        result = await workflow.execute_activity(process_item, item)
        results.append(result)
    return results

# Or in parallel:
results = await asyncio.gather(*[
    workflow.execute_activity(process_item, item) for item in items
])
```

### XCom → Return Values as Local Variables

XCom is Airflow's mechanism for passing data between tasks. It stores serialized values in a database. In Temporal, there is no equivalent needed — activity return values are passed as function arguments.

```python
# Airflow: XCom push and pull
def extract_task(**context):
    data = extract()
    context["ti"].xcom_push(key="data", value=data)

def transform_task(**context):
    data = context["ti"].xcom_pull(task_ids="extract_task", key="data")
    return transform(data)

# Temporal equivalent — no XCom needed; just return values and pass them:
@workflow.run
async def run(self, config):
    data = await workflow.execute_activity(extract, config)      # return value
    result = await workflow.execute_activity(transform, data)    # passed directly
    await workflow.execute_activity(load, result)
```

### Trigger Rules → Explicit Conditional Logic

Airflow's trigger rules (`all_success`, `one_failed`, `all_failed`, `none_failed`, etc.) control when a downstream task runs based on upstream task states.

In Temporal, there are no trigger rules. You write the conditions explicitly in workflow code.

```python
# Airflow: task_d runs if any of task_a, task_b, task_c fail
task_d = PythonOperator(task_id="task_d", trigger_rule=TriggerRule.ONE_FAILED, ...)

# Temporal equivalent:
@workflow.run
async def run(self, config):
    results = await asyncio.gather(
        workflow.execute_activity(task_a, config),
        workflow.execute_activity(task_b, config),
        workflow.execute_activity(task_c, config),
        return_exceptions=True,
    )
    if any(isinstance(r, Exception) for r in results):
        await workflow.execute_activity(task_d, config)
```

### on_failure_callback → try/except + Notification Activity

```python
# Airflow: on_failure_callback sends Slack alert
def alert_slack(context):
    slack_hook.send_text(f"DAG {context['dag'].dag_id} failed!")

with DAG(..., on_failure_callback=alert_slack):
    ...

# Temporal equivalent:
@workflow.run
async def run(self, config):
    try:
        await workflow.execute_activity(main_process, config)
    except ActivityError as e:
        await workflow.execute_activity(
            send_slack_alert,
            f"Workflow {workflow.info().workflow_type} failed: {e}",
        )
        raise
```

---

## Scheduling: DAG schedule_interval → Temporal Schedule

```python
# Airflow: DAG with schedule
with DAG(
    "nightly_etl",
    schedule_interval="0 2 * * *",  # 2am daily
    start_date=datetime(2024, 1, 1),
    catchup=True,  # run for missed intervals
) as dag:
    ...

# Temporal: Schedule equivalent
await client.create_schedule(
    "nightly-etl",
    Schedule(
        action=ScheduleActionStartWorkflow(
            NightlyETLWorkflow.run,
            id="nightly-etl-{scheduled_time}",
            task_queue="etl",
        ),
        spec=ScheduleSpec(
            cron_expressions=["0 2 * * *"],
        ),
        policy=SchedulePolicy(
            catchup_window=timedelta(days=7),  # equivalent to catchup=True up to 7 days
        ),
    ),
)
```

---

## Migration Checklist for Airflow

- [ ] Every DAG → Workflow Definition
- [ ] Every Task/Operator → Activity function
- [ ] `PythonOperator` callable → Activity function body
- [ ] `BashOperator` command → Activity running subprocess
- [ ] `BranchPythonOperator` → `if/else` in workflow code
- [ ] Sensor → polling Activity with `activity.heartbeat()`
- [ ] `>>`/`<<` dependency chains → sequential Activity calls
- [ ] Parallel tasks → `asyncio.gather` (Python) / `Promise.all` (TS) / etc.
- [ ] Dynamic task mapping → `for` loop in workflow code
- [ ] XCom push/pull → Activity return values stored as local workflow variables
- [ ] `schedule_interval` → Temporal Schedule
- [ ] `catchup=True` → `catchup_window` on Schedule
- [ ] `dag_run.conf` → Workflow input parameters
- [ ] `on_failure_callback` → `try/except` + notification Activity
- [ ] `retries` + `retry_delay` → `RetryPolicy` on `execute_activity()`
- [ ] `execution_timeout` → `start_to_close_timeout` on `execute_activity()`
- [ ] `Variable` / `Connection` → Worker startup config / secrets manager
- [ ] Remove Airflow Scheduler, Webserver, and database dependencies
- [ ] Deploy Temporal Workers + connect to Temporal Cluster
