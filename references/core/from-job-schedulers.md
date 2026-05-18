# Migrating from Enterprise Job Schedulers to Temporal

This covers **Control-M (BMC)**, **Tidal Automation** (formerly Tidal Enterprise Scheduler), **Talon**, and **Quartz Scheduler**.

---

## The Core Difference: Jobs vs. Workflows

**Job schedulers** think in terms of *jobs* — individual scripts or executables run on specific agents at specific times, with dependency chains to control ordering. The scheduler owns the execution graph; your code is just a script it invokes.

**Temporal** thinks in terms of *workflows* — durable functions that express the entire business process in code, with activities to perform individual units of work. Your code owns the process; Temporal provides the durability and scheduling infrastructure.

The migration is not just a translation of concepts. It is a shift from *scheduled scripts with dependencies* to *code-first durable processes with scheduling support*.

---

## Control-M (BMC)

*Verified against: Control-M 9.x / 21.x*

### Core Concept Mapping

| Control-M Concept | Temporal Equivalent |
|---|---|
| Job | Temporal Workflow Execution (started by a Temporal Schedule) |
| Folder | Namespace convention (e.g., `{team}.{process}`) or a parent Workflow that manages related child Workflows |
| Job Flow (jobs with In/Out conditions) | Workflow function with sequential or parallel Activity calls |
| Smart Folder | Parent Workflow that orchestrates child Workflows |
| Predecessor / Successor dependency | Sequential activity or child workflow call (natural code ordering) |
| `ONERR` / `ONDO` condition | `try/except` in workflow code |
| `In Condition` | Signal sent from a dependency workflow, or `workflow.wait_condition()` |
| `Out Condition` | Signal sent by this workflow to dependents when complete |
| Calendar | Temporal Schedule — cron or calendar-based, with exclusion windows |
| Cyclic Job (re-run after interval) | Temporal Schedule with a fixed interval |
| Job Agent | Temporal Worker (processes Activities on the target host) |
| Control-M Server | Temporal Cluster |
| Hold / Free (manual pause) | Signal to the workflow to wait; `workflow.wait_condition(lambda: self._released)` |
| Force Job (manual trigger) | `client.start_workflow()` or a Schedule backfill/trigger |
| Job output (JOBID, exit codes) | Workflow Execution ID + Activity return values |
| `%%VARIABLE%%` auto-edit variables | Workflow input parameters |
| Quantitative Resources | Resource-based slot suppliers (see `temporal-workertuning` skill) |
| Control Resources (mutex) | Signal-based mutex pattern in workflow code |
| Alert | Activity that sends an alert; or notification on `on_failure_callback` pattern |
| Job log | Temporal event history + Activity stdout captured in your logging infrastructure |

### Job with Predecessor/Successor → Sequential Workflow

```python
# Control-M: Three jobs in a folder:
#   extract_data → transform_data → load_data (linear chain)

# Temporal equivalent: One workflow with sequential activities
@workflow.defn
class ETLWorkflow:
    @workflow.run
    async def run(self, config: ETLConfig) -> ETLResult:
        raw_data = await workflow.execute_activity(
            extract_data, config,
            start_to_close_timeout=timedelta(hours=2),
        )
        transformed = await workflow.execute_activity(
            transform_data, raw_data,
            start_to_close_timeout=timedelta(hours=1),
        )
        return await workflow.execute_activity(
            load_data, transformed,
            start_to_close_timeout=timedelta(hours=1),
        )
```

### Parallel Jobs → Parallel Activities

```python
# Control-M: Three jobs that can run concurrently, then a final aggregation job
#   extract_region_a ─┐
#   extract_region_b ─┼──► aggregate_regions
#   extract_region_c ─┘

# Temporal equivalent:
@workflow.run
async def run(self, config):
    # All three run in parallel
    a, b, c = await asyncio.gather(
        workflow.execute_activity(extract_region_a, config),
        workflow.execute_activity(extract_region_b, config),
        workflow.execute_activity(extract_region_c, config),
    )
    return await workflow.execute_activity(aggregate_regions, [a, b, c])
```

### File Trigger / Resource Trigger → Polling Activity + Signal

```python
# Control-M: File Watcher job — wait for /data/input/*.csv to appear, then start ETL

# Temporal equivalent: polling activity with heartbeat, then signal to workflow
@activity.defn
async def wait_for_input_file(path_pattern: str) -> str:
    import glob
    while True:
        matches = glob.glob(path_pattern)
        if matches:
            return matches[0]
        activity.heartbeat()  # keep the activity alive while polling
        await asyncio.sleep(30)

# Or: a separate file-watching service that calls client.start_workflow() when a file arrives
```

### ONERR / Retry → RetryPolicy + try/except

```python
# Control-M: MAXRERUN=3, RERUNINTERVAL=5 minutes on job failure

# Temporal equivalent: RetryPolicy on the activity
result = await workflow.execute_activity(
    run_batch_job,
    job_config,
    start_to_close_timeout=timedelta(hours=4),
    retry_policy=RetryPolicy(
        maximum_attempts=3,
        initial_interval=timedelta(minutes=5),
    ),
)
```

### Hold / Free → Signal-Based Pause

```python
# Control-M: Hold a job, then Free it later (manual operator control)

@workflow.defn
class BatchWorkflow:
    def __init__(self):
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self, config):
        # Wait until explicitly released (equivalent to Hold → Free)
        await workflow.wait_condition(lambda: self._released)
        await workflow.execute_activity(run_batch, config)
```

### Scheduling a Recurring Job

```python
# Control-M: Job scheduled with a calendar — run weekdays at 02:00 EST

# Temporal equivalent: Temporal Schedule
await client.create_schedule(
    "nightly-etl",
    Schedule(
        action=ScheduleActionStartWorkflow(
            ETLWorkflow.run,
            args=[config],
            id="nightly-etl-{scheduled_time}",
            task_queue="etl-workers",
        ),
        spec=ScheduleSpec(
            cron_expressions=["0 2 * * 1-5"],  # Weekdays at 02:00
        ),
        policy=SchedulePolicy(
            overlap=ScheduleOverlapPolicy.SKIP,  # Skip if previous run still going
        ),
    ),
)
```

---

## Tidal Automation (formerly Tidal Enterprise Scheduler)

Tidal is conceptually very similar to Control-M. The mapping is largely the same.

| Tidal Concept | Temporal Equivalent |
|---|---|
| Job | Temporal Workflow Execution |
| Job Class | Workflow Definition type |
| Job Group | Namespace or parent Workflow |
| Job Stream | Workflow with sequential Activity calls |
| Master File (job definition repository) | Workflow and Activity code in your version control |
| Calendar | Temporal Schedule spec (cron or calendar) |
| Runtime Parameters | Workflow input parameters |
| Predecessor / Successor dependencies | Sequential activity calls in workflow code |
| Agent | Temporal Worker |
| Machine Group | Task Queue (route work to specific Workers by queue name) |
| Production Control | Temporal Web UI + `temporal` CLI |
| Job Log / Output | Activity stdout captured by your logging stack; Temporal event history |
| Alerts | Activity that sends notifications on failure |
| Connection (database, FTP, etc.) | Activity implementation (handles the connection) |

**Key difference from Control-M**: Tidal's "Job Streams" are more explicit DAG-like structures. When migrating, map each Stream to a Workflow, and each Job within the Stream to an Activity or Child Workflow depending on complexity.

---

## Talon

Talon is an enterprise workload automation platform similar in concept to Control-M and Tidal.

| Talon Concept | Temporal Equivalent |
|---|---|
| Task | Temporal Activity |
| Flow | Workflow |
| Trigger (schedule, event, dependency) | Temporal Schedule or Signal |
| Agent | Temporal Worker |
| Dependency | Sequential/parallel activity calls |
| Retry policy | `RetryPolicy` on `execute_activity()` |
| Task group | Child Workflow or parent Workflow with parallel activities |

The migration pattern from Talon is the same as Control-M — map flows to Workflows and tasks to Activities.

---

## Quartz Scheduler

Quartz is a Java job scheduling library (not a separate system). It runs inside your Java application.

*Verified against: Quartz 2.3.x*

### Core Concept Mapping

| Quartz Concept | Temporal Equivalent |
|---|---|
| `Job` interface | Activity implementation |
| `JobDetail` (job metadata, `JobDataMap`) | Activity method signature (strongly typed parameters) |
| `Trigger` (SimpleTrigger, CronTrigger) | Temporal Schedule |
| `Scheduler` (singleton, manages jobs and triggers) | Temporal Client + Temporal Schedule |
| `JobDataMap` (pass data to a Job) | Workflow/Activity input parameters (strongly typed) |
| `JobListener` / `TriggerListener` | Workflow logic + error handlers |
| `JobExecutionContext` | Activity context (`activity.info()` for metadata) |
| `@DisallowConcurrentExecution` | `ScheduleOverlapPolicy.SKIP` on Schedule; or `WorkflowIdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY` |
| `@PersistJobDataAfterExecution` | Activity return values stored in workflow state (implicit) |
| `Scheduler.pauseJob()` / `resumeJob()` | `client.schedule.pause()` / `client.schedule.unpause()` |
| `Scheduler.triggerJob()` (manual fire) | `client.schedule.trigger()` or `client.start_workflow()` |
| `RAMJobStore` (in-memory) | Not applicable — Temporal always persists |
| `JDBCJobStore` (persistent) | Not applicable — Temporal always persists |
| Clustered Quartz (multiple nodes) | Temporal Workers (naturally distributed; no special config needed) |
| Misfire instructions | `ScheduleOverlapPolicy` + `catchup_window` on Schedule |

### Simple Quartz Job → Temporal Schedule + Activity

```java
// Quartz: Job class
public class SendReportJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String reportType = context.getMergedJobDataMap().getString("reportType");
        reportService.generate(reportType);
        emailService.send(reportType);
    }
}

// Quartz: Scheduling it
JobDetail job = JobBuilder.newJob(SendReportJob.class)
    .withIdentity("sendReport", "reports")
    .usingJobData("reportType", "daily-summary")
    .build();

Trigger trigger = TriggerBuilder.newTrigger()
    .withSchedule(CronScheduleBuilder.cronSchedule("0 0 8 * * ?"))  // 8am daily
    .build();

scheduler.scheduleJob(job, trigger);
```

```java
// Temporal equivalent: Activity + Workflow + Schedule

// Activity
@ActivityInterface
public interface ReportActivities {
    @ActivityMethod void generateReport(String reportType);
    @ActivityMethod void sendReport(String reportType);
}

// Workflow
@WorkflowInterface
public interface SendReportWorkflow {
    @WorkflowMethod void run(String reportType);
}

public class SendReportWorkflowImpl implements SendReportWorkflow {
    private final ReportActivities activities = Workflow.newActivityStub(
        ReportActivities.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofMinutes(30))
            .build()
    );

    @Override
    public void run(String reportType) {
        activities.generateReport(reportType);
        activities.sendReport(reportType);
    }
}

// Schedule (replaces CronTrigger)
Schedule schedule = Schedule.newBuilder()
    .setAction(ScheduleActionStartWorkflow.newBuilder()
        .setWorkflowType(SendReportWorkflow.class)
        .setArguments("daily-summary")
        .setOptions(WorkflowOptions.newBuilder()
            .setTaskQueue("reports")
            .build())
        .build())
    .setSpec(ScheduleSpec.newBuilder()
        .addCronExpression("0 8 * * *")  // 8am daily
        .build())
    .build();

client.newScheduleClient().createSchedule("send-daily-report", schedule, ScheduleOptions.getDefaultInstance());
```

### Replacing Quartz Clustering

Quartz clustering required a shared JDBC store and careful node coordination to avoid double-firing. Temporal handles this automatically:

- Workers poll Task Queues; the cluster routes one task to exactly one Worker
- No shared database config between Workers is needed
- Adding or removing Worker nodes requires no Quartz-style reconfiguration

Simply run multiple Worker processes — they automatically participate in work distribution.

---

## Migration Checklist for Job Schedulers

- [ ] Identify all recurring jobs → create one Temporal Schedule per job (or per job group)
- [ ] Identify all job dependency chains → translate to sequential/parallel activity calls in a Workflow
- [ ] Map job parameters/variables → Workflow input parameters (strongly typed)
- [ ] Map file/event triggers → polling Activity with heartbeat, or external trigger calling `client.start_workflow()`
- [ ] Map error/retry config → `RetryPolicy` on `execute_activity()` calls
- [ ] Map Hold/Pause functionality → Signal handler + `workflow.wait_condition()`
- [ ] Replace scheduling infrastructure (job agent, scheduler server) → Temporal Workers + Temporal Schedule
- [ ] Replace monitoring UI → Temporal Web UI + `temporal` CLI
- [ ] Test: kill a Worker mid-execution; confirm the workflow resumes on the next Worker restart
