# Python Migration Examples

Side-by-side code translations for Python developers migrating to Temporal. Primary source tools covered: **Apache Airflow** and **N8n** (Python code nodes).

For SDK setup and Worker configuration, refer to the `temporal-developer` skill (`references/python/python.md`).

*Verified against: Temporal Python SDK 1.x, Airflow 2.x*

---

## Airflow DAG → Temporal Workflow

### Basic linear DAG

```python
# ── AIRFLOW ──────────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract(**context):
    date = context["ds"]
    return fetch_raw_data(date)          # returned value goes to XCom

def transform(**context):
    raw = context["ti"].xcom_pull(task_ids="extract")
    return clean_and_transform(raw)

def load(**context):
    data = context["ti"].xcom_pull(task_ids="transform")
    write_to_warehouse(data)

with DAG(
    "daily_etl",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)
    t1 >> t2 >> t3


# ── TEMPORAL ─────────────────────────────────────────────────────────────────
import asyncio
from datetime import timedelta
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.client import Client, Schedule, ScheduleActionStartWorkflow, ScheduleSpec


# Activities (one per Airflow task)
@activity.defn
async def extract(date: str) -> list[dict]:
    return fetch_raw_data(date)          # no XCom needed — just return the value

@activity.defn
async def transform(raw: list[dict]) -> list[dict]:
    return clean_and_transform(raw)      # receives data as a parameter

@activity.defn
async def load(data: list[dict]) -> None:
    write_to_warehouse(data)


# Workflow (replaces the DAG)
@workflow.defn
class DailyETLWorkflow:
    @workflow.run
    async def run(self, date: str) -> None:
        retry = RetryPolicy(maximum_attempts=3, initial_interval=timedelta(minutes=5))
        opts = dict(start_to_close_timeout=timedelta(hours=2), retry_policy=retry)

        raw = await workflow.execute_activity(extract, date, **opts)
        transformed = await workflow.execute_activity(transform, raw, **opts)
        await workflow.execute_activity(load, transformed, **opts)


# Schedule (replaces schedule_interval="@daily")
async def create_schedule():
    client = await Client.connect("localhost:7233")
    await client.create_schedule(
        "daily-etl",
        Schedule(
            action=ScheduleActionStartWorkflow(
                DailyETLWorkflow.run,
                id="daily-etl-{scheduled_time}",
                task_queue="etl",
            ),
            spec=ScheduleSpec(cron_expressions=["0 0 * * *"]),
        ),
    )
```

---

### Branching: BranchPythonOperator → if/else

```python
# ── AIRFLOW ──────────────────────────────────────────────────────────────────
from airflow.operators.python import BranchPythonOperator

def route(**context):
    amount = context["dag_run"].conf.get("amount", 0)
    return "high_value" if amount > 10_000 else "standard"

branch = BranchPythonOperator(task_id="route", python_callable=route)
high_value = PythonOperator(task_id="high_value", python_callable=process_high_value)
standard = PythonOperator(task_id="standard", python_callable=process_standard)
branch >> [high_value, standard]


# ── TEMPORAL ─────────────────────────────────────────────────────────────────
@workflow.defn
class ProcessWorkflow:
    @workflow.run
    async def run(self, request: ProcessRequest) -> ProcessResult:
        if request.amount > 10_000:
            return await workflow.execute_activity(
                process_high_value, request,
                start_to_close_timeout=timedelta(hours=1),
            )
        else:
            return await workflow.execute_activity(
                process_standard, request,
                start_to_close_timeout=timedelta(minutes=10),
            )
```

---

### Parallel tasks → asyncio.gather

```python
# ── AIRFLOW ──────────────────────────────────────────────────────────────────
# Fan-out: three tasks run in parallel, then aggregate
start >> [validate_inventory, check_fraud, verify_credit] >> aggregate


# ── TEMPORAL ─────────────────────────────────────────────────────────────────
@workflow.defn
class OrderCheckWorkflow:
    @workflow.run
    async def run(self, order: Order) -> CheckResult:
        opts = dict(start_to_close_timeout=timedelta(minutes=5))
        inventory, fraud, credit = await asyncio.gather(
            workflow.execute_activity(validate_inventory, order, **opts),
            workflow.execute_activity(check_fraud, order, **opts),
            workflow.execute_activity(verify_credit, order, **opts),
        )
        return await workflow.execute_activity(
            aggregate_checks, order, inventory, fraud, credit, **opts
        )
```

---

### Sensor → Polling Activity with heartbeat

```python
# ── AIRFLOW ──────────────────────────────────────────────────────────────────
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_input",
    filepath="/data/input/{{ ds }}.csv",
    poke_interval=60,
    timeout=3600,
)


# ── TEMPORAL ─────────────────────────────────────────────────────────────────
import os
from temporalio import activity

@activity.defn
async def wait_for_file(file_path: str) -> str:
    """Polls until file exists; heartbeat keeps the activity alive."""
    while not os.path.exists(file_path):
        activity.heartbeat(f"waiting for {file_path}")
        await asyncio.sleep(60)
    return file_path

# Called with a heartbeat_timeout so a dead worker is detected:
file_path = await workflow.execute_activity(
    wait_for_file,
    f"/data/input/{date}.csv",
    start_to_close_timeout=timedelta(hours=2),
    heartbeat_timeout=timedelta(minutes=5),
)
```

---

### Dynamic task mapping → for loop

```python
# ── AIRFLOW ──────────────────────────────────────────────────────────────────
@task
def process_item(item: dict) -> dict:
    return transform(item)

items = get_items.expand(...)  # dynamic task mapping


# ── TEMPORAL ─────────────────────────────────────────────────────────────────
@workflow.defn
class BatchWorkflow:
    @workflow.run
    async def run(self, batch_id: str) -> list[dict]:
        items = await workflow.execute_activity(
            get_items, batch_id, start_to_close_timeout=timedelta(minutes=5)
        )
        # Sequential processing
        results = []
        for item in items:
            result = await workflow.execute_activity(
                process_item, item, start_to_close_timeout=timedelta(minutes=2)
            )
            results.append(result)
        return results

        # Or parallel (be mindful of Worker concurrency):
        # results = await asyncio.gather(*[
        #     workflow.execute_activity(process_item, item, ...) for item in items
        # ])
```

---

### Signal: waiting for external event

```python
# ── AIRFLOW ──────────────────────────────────────────────────────────────────
# Airflow has no native "wait for external event" — requires a sensor polling an API
wait_for_approval = HttpSensor(task_id="wait_approval", endpoint="/approval/{{ run_id }}", ...)


# ── TEMPORAL ─────────────────────────────────────────────────────────────────
@dataclass
class ApprovalResult:
    approved: bool
    reviewer: str

@workflow.defn
class ApprovalWorkflow:
    def __init__(self):
        self._approval: ApprovalResult | None = None

    @workflow.signal
    def approval_received(self, result: ApprovalResult) -> None:
        self._approval = result

    @workflow.run
    async def run(self, request: ApprovalRequest) -> str:
        await workflow.execute_activity(send_for_review, request,
                                        start_to_close_timeout=timedelta(minutes=1))

        # Wait up to 7 days for a human to approve via signal
        await workflow.wait_condition(
            lambda: self._approval is not None,
            timeout=timedelta(days=7),
        )

        if self._approval.approved:
            return await workflow.execute_activity(approve_request, request,
                                                   start_to_close_timeout=timedelta(minutes=5))
        return await workflow.execute_activity(reject_request, request,
                                               start_to_close_timeout=timedelta(minutes=5))

# Send the signal from your web app / CLI:
# handle = client.get_workflow_handle("approval-123")
# await handle.signal(ApprovalWorkflow.approval_received, ApprovalResult(approved=True, reviewer="alice"))
```

---

## N8n Python Code Node → Activity

```python
# ── N8N (JavaScript Code Node — Python equivalent) ────────────────────────────
# Transform each item: calculate total and normalize currency
items = $input.all()
results = []
for item in items:
    results.append({
        "id": item["json"]["orderId"],
        "total": item["json"]["price"] * item["json"]["quantity"],
        "currency": item["json"]["currency"].upper(),
    })
return results


# ── TEMPORAL ─────────────────────────────────────────────────────────────────
from dataclasses import dataclass

@dataclass
class OrderItem:
    order_id: str
    price: float
    quantity: int
    currency: str

@dataclass
class TransformedItem:
    id: str
    total: float
    currency: str

@activity.defn
async def transform_order_items(items: list[OrderItem]) -> list[TransformedItem]:
    return [
        TransformedItem(
            id=item.order_id,
            total=item.price * item.quantity,
            currency=item.currency.upper(),
        )
        for item in items
    ]
```

---

## Quartz-style Recurring Job → Schedule + Workflow

```python
# ── QUARTZ (conceptual — Quartz is Java, but pattern applies) ─────────────────
# Job: run report every weekday at 8am

# ── TEMPORAL ─────────────────────────────────────────────────────────────────
from temporalio.client import (
    Schedule, ScheduleActionStartWorkflow, ScheduleSpec, SchedulePolicy,
    ScheduleOverlapPolicy,
)

@workflow.defn
class WeeklyReportWorkflow:
    @workflow.run
    async def run(self, report_type: str) -> None:
        data = await workflow.execute_activity(
            fetch_report_data, report_type, start_to_close_timeout=timedelta(hours=1)
        )
        await workflow.execute_activity(
            send_report_email, data, start_to_close_timeout=timedelta(minutes=5)
        )

# Create the schedule once (e.g., at app startup or via a setup script):
async def setup():
    client = await Client.connect("localhost:7233")
    await client.create_schedule(
        "weekday-report",
        Schedule(
            action=ScheduleActionStartWorkflow(
                WeeklyReportWorkflow.run,
                args=["daily-summary"],
                id="report-{scheduled_time}",
                task_queue="reports",
            ),
            spec=ScheduleSpec(cron_expressions=["0 8 * * 1-5"]),  # weekdays 8am
            policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
        ),
    )
```
