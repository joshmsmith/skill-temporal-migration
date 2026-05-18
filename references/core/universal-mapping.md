# Universal Concept Mapping: All Tools → Temporal

This table provides a fast cross-reference for concepts from every supported source tool. For deeper coverage of any specific tool, read the corresponding `from-*.md` file.

---

## Orchestration Structure

| Concept | Camunda / BPMN | Control-M / Tidal | Airflow | N8n | Quartz | **Temporal Equivalent** |
|---|---|---|---|---|---|---|
| Process / Flow definition | Process Definition (BPMN XML) | Job Plan / Job Flow | DAG | Workflow (JSON) | — | **Workflow Definition** (`@workflow.defn` / `@WorkflowInterface`) |
| A running instance | Process Instance | Job Run | DAG Run | Execution | — | **Workflow Execution** (identified by `workflow_id` + `run_id`) |
| Unit of work | Service Task, Send Task | Job Step / Job | Task + Operator | Node | Job (`implements Job`) | **Activity** (`@activity.defn` / `@ActivityInterface`) |
| Subprocess / nested flow | Subprocess, Call Activity | Sub-job / Nested Job | SubDAG, TaskGroup | Sub-workflow | — | **Child Workflow** (`workflow.execute_child_workflow`) |
| The engine / runtime | Camunda Engine / Zeebe | Scheduler | Airflow Scheduler | N8n Platform | `Scheduler` singleton | **Temporal Cluster** |
| The execution agent | External Task Worker (C7), Job Worker (C8) | Job Agent | Worker (Celery/K8s/etc.) | — | — | **Temporal Worker** (polls Task Queue, runs Workflow + Activity code) |

---

## Scheduling & Timing

| Concept | Camunda / BPMN | Control-M / Tidal | Airflow | N8n | Quartz | **Temporal Equivalent** |
|---|---|---|---|---|---|---|
| Recurring execution | Timer Start Event | Job Calendar / Cyclic Job | `schedule_interval` / `@daily` | Cron Trigger Node | `CronTrigger` | **Temporal Schedule** (cron, interval, or calendar) |
| Delay / wait within a process | Timer Intermediate Event | `Wait` step / sleep | — (external, via sensor) | Wait Node | `Thread.sleep` (bad practice) | **`workflow.sleep(duration)`** |
| Calendar exceptions | Process Calendar | Job Calendar with exceptions | — | — | `CalendarIntervalTrigger` exclusions | **Schedule Calendar** with exclusion windows |
| Catch up on missed runs | — | Backfill run | `catchup=True` on DAG | — | — | **Schedule backfill** via `client.schedule.backfill()` |

---

## Control Flow

| Concept | Camunda / BPMN | Control-M / Tidal | Airflow | N8n | Quartz | **Temporal Equivalent** |
|---|---|---|---|---|---|---|
| Conditional branch | Exclusive Gateway (`XOR`) | `In/Out Condition` on job | `BranchPythonOperator` | Switch / IF node | — | **`if/else`** in workflow code |
| Parallel execution | Parallel Gateway (`AND`) | Multiple jobs with same predecessor | Parallel tasks (no dep) | Parallel branch | — | Language-native parallel: `asyncio.gather`, `Promise.all`, `workflow.Go`, `Task.WhenAll` |
| Join / synchronize parallel | Parallel Gateway (join) | All predecessors must complete | All parallel tasks complete | Merge node | — | `await asyncio.gather(...)` or `workflow.wait_all(...)` |
| Loop / iteration | Loop Sub-process | Cyclic jobs | Dynamic task mapping | Loop node | — | **`for` loop** in workflow code; use Continue-As-New for very large loops |
| Error path | Error Boundary Event | Failure action / `ONERR` | `on_failure_callback` | Error Workflow | `JobListener.jobWasExecuted` | **`try/except`** in workflow code + Activity RetryPolicy |
| Compensation | Compensation Event | Manual / scripted rollback | — | — | — | **Saga pattern** (explicit compensation activities) |

---

## Data & State

| Concept | Camunda / BPMN | Control-M / Tidal | Airflow | N8n | Quartz | **Temporal Equivalent** |
|---|---|---|---|---|---|---|
| Passing data between steps | Process Variables | Job output conditions / auto-edit | XCom push/pull | Item JSON flowing through connections | `JobDataMap` | **Activity return values** stored as local variables in workflow code |
| Global / shared state | Process Variables (engine-managed) | Shared job context / auto-edit variables | Airflow Variables | — | `JobDataMap` shared | **Avoid.** Pass data explicitly as activity arguments and return values. |
| Storing metadata | Process Instance variables | Job definition fields | DAG `doc_md`, tags | Workflow notes | — | **Search Attributes** (queryable) or **Memo** (not queryable) on Workflow Execution |
| Querying running process state | Camunda Cockpit / REST API | Control-M API | Airflow REST API | N8n API | — | **Workflow Query** (`@workflow.query`) or Visibility API search |

---

## External Interaction & Events

| Concept | Camunda / BPMN | Control-M / Tidal | Airflow | N8n | Quartz | **Temporal Equivalent** |
|---|---|---|---|---|---|---|
| Receive external message | Message Catch Event | File trigger, MFT completion | `ExternalTaskSensor` / `TriggerDagRunOperator` | Webhook Trigger Node | — | **Signal** (`@workflow.signal`) or **Update** (`@workflow.update`) |
| Wait for human input | User Task | Hold / Release | — | Manual Trigger / Wait node | — | **Signal** or **Update** from external application |
| Wait for external condition | Intermediate Catching Event | Resource trigger / file sensor | Sensor Operator (polls) | Wait node | — | **`workflow.wait_condition()`** triggered by Signal, or polling Activity with heartbeat |
| Trigger another process | Call Activity / Message Throw Event | Job dependency chain | `TriggerDagRunOperator` | Execute Workflow node | `scheduler.scheduleJob()` | **Child Workflow** or `client.start_workflow()` from an Activity |
| Webhook inbound | Message Start Event | — | `TriggerDagRunOperator` (external) | Webhook Trigger | `JobBuilder.newJob()` | HTTP endpoint calling `client.start_workflow()` or sending a Signal |

---

## Reliability & Retries

| Concept | Camunda / BPMN | Control-M / Tidal | Airflow | N8n | Quartz | **Temporal Equivalent** |
|---|---|---|---|---|---|---|
| Retry on failure | Service Task retry config | `MAXRERUN` / retry settings | `retries=3` on operator | Retry on Error settings | `@DisallowConcurrentExecution` + re-fire | **`RetryPolicy`** on `execute_activity()` call |
| Timeout | Service Task timeout | Job timeout | `execution_timeout` | — | `misfire_instruction` | **`start_to_close_timeout`** (per attempt), **`schedule_to_close_timeout`** (total) |
| Heartbeat (long-running) | — | `keepalive` / ping | — | — | — | **`activity.heartbeat()`** to signal liveness; enables mid-activity cancellation |
| Dead-letter / permanent failure | Incident (C8) / Error event | Job failure + alert | Task failure state | — | `jobWasExecuted` listener | **Workflow blocked** in Running state; inspect via Web UI; fix + replay or terminate |

---

## Deployment & Operations

| Concept | Camunda / BPMN | Control-M / Tidal | Airflow | N8n | Quartz | **Temporal Equivalent** |
|---|---|---|---|---|---|---|
| Deploying a new process version | Deploy BPMN to engine | Upload job definition to agent | Upload DAG file to `dags/` folder | Export/import workflow JSON | — | **Deploy new Worker** (new code version + register Workflow/Activity types) |
| Versioning existing processes | Process version in engine | Job definition versioning | DAG `dag_id` + versioning | — | — | **`workflow.get_version()`** / Worker Versioning API |
| Monitoring / observability | Camunda Cockpit / Operate (C8) | Control-M GUI / API | Airflow Web UI | N8n executions panel | — | **Temporal Web UI** + `temporal` CLI |
| Isolating workloads | Process Application | Agent / Workload | Queues (CeleryExecutor) | — | — | **Task Queues** (one Task Queue per logical workload; Workers poll specific queues) |
| Multi-tenancy | Multiple engine deployments | Multiple environments | Multiple Airflow instances | Multiple N8n instances | — | **Namespaces** (logical isolation within one cluster) |

---

## Notes on Concept Differences

### "No BPMN diagram" is not a limitation
Temporal does not have a visual designer. The workflow code *is* the diagram. This is intentional — code has IDE support, type checking, unit tests, and git history that XML diagrams lack. Tools like [Temporalite](https://github.com/temporalio/temporalite) and the Temporal Web UI provide runtime visualization of running executions.

### Temporal Workers are not "agents" in the scheduler sense
Job scheduler agents are passive executors that run scripts on a target machine. Temporal Workers are long-running processes that actively poll Task Queues. They run your business logic — not scripts. You deploy them like any other application service (Docker, Kubernetes, Lambda, etc.).

### Activities are not "tasks" in the Airflow sense
Airflow tasks are stateless, isolated invocations. Temporal Activities can be long-running (hours, days) and are expected to call `heartbeat()` periodically. The Temporal cluster uses heartbeats to detect stuck activities and trigger retries. An activity is more like a reliable RPC call than a batch task.

### Temporal Schedules replace cron-based scheduling
A Temporal Schedule is not just a cron expression. It supports:
- Cron, interval, or calendar-based triggers
- Overlapping execution policies (skip, buffer, allow)
- Pause/resume/trigger manually via CLI or API
- Backfill for missed intervals
- Jitter to spread load across a time window
