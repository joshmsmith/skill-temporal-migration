# Migrating from TIBCO BusinessWorks to Temporal

This covers **TIBCO BusinessWorks 5.x** and **TIBCO BusinessWorks 6.x (TIBCO ActiveMatrix BusinessWorks)**.

*TIBCO BusinessWorks is an Enterprise Application Integration (EAI) / Enterprise Service Bus (ESB) platform. It is conceptually different from BPMN engines — it focuses on connecting systems via adapters and messaging, not modeling business processes visually.*

---

## Core Conceptual Mapping

| TIBCO BusinessWorks Concept | Temporal Equivalent |
|---|---|
| Process Definition (`.process` file) | Workflow Definition (`@workflow.defn` / `@WorkflowInterface`) |
| Process Instance | Workflow Execution |
| Activity (HTTP, JDBC, JMS, File, etc.) | Temporal Activity (one Activity per integration call) |
| Shared Variable | **Avoid** — pass data as activity return values; see below |
| Checkpoint | **Not needed** — Temporal's event history provides implicit durability |
| Error Handler (Catch/Fault scope) | `try/except` in workflow code + Activity RetryPolicy |
| Group (Sequence, Critical Section, Loop, Pick First) | Workflow code constructs (`for` loop, `try/except`, `asyncio.gather`) |
| Transition / Condition | `if/else` in workflow code |
| Starter (receive message and start process) | Trigger: external HTTP endpoint / message consumer calls `client.start_workflow()` |
| BusinessWorks Engine | Temporal Cluster + Temporal Worker |
| Application | Temporal Worker (deploys workflow and activity types) |
| Module | Python/Java/Go/TypeScript package hosting Activity implementations |
| Adapter (SAP, Salesforce, etc.) | Activity that calls the target system's API or SDK |
| Transport (JMS, HTTP, FTP) | Activity implementation (handles the transport concern) |

---

## Activity Types → Temporal Activities

In TIBCO BusinessWorks, "Activity" means a pre-built connector step. In Temporal, an Activity is a function that you write. The mapping is one-to-one: each BW Activity type becomes a Temporal Activity implementation.

### HTTP (REST / SOAP)

```python
# TIBCO BW: HTTP Client Activity — POST to payment service
# (configured in visual designer: URL, method, headers, timeout)

# Temporal equivalent: Activity function
@activity.defn
async def call_payment_service(request: PaymentRequest) -> PaymentResponse:
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://payments.example.com/charge",
            json=dataclasses.asdict(request),
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            return PaymentResponse(**await resp.json())
```

### JDBC / Database

```python
# TIBCO BW: JDBC Query Activity — SELECT from orders table
# (configured with connection pool, SQL, output mapping)

# Temporal equivalent:
@activity.defn
async def fetch_order(order_id: str) -> Order:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
        return Order.from_row(row)
```

### JMS / Messaging

```python
# TIBCO BW: JMS Send Activity — publish to queue

# Temporal equivalent: Activity that publishes the message
@activity.defn
async def publish_to_queue(queue_name: str, payload: dict) -> None:
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(payload).encode()),
            routing_key=queue_name,
        )
```

### File Operations

```python
# TIBCO BW: Read File Activity, Write File Activity

# Temporal equivalent:
@activity.defn
async def read_input_file(path: str) -> str:
    return Path(path).read_text()

@activity.defn
async def write_output_file(path: str, content: str) -> None:
    Path(path).write_text(content)
```

### Timer / Wait

```python
# TIBCO BW: Sleep Activity or Timer (wait N seconds/minutes in a process)

# Temporal equivalent: workflow.sleep (NOT an Activity — runs in the workflow)
@workflow.defn
class RetryLaterWorkflow:
    @workflow.run
    async def run(self, request):
        result = await workflow.execute_activity(try_operation, request)
        if result.should_retry_later:
            await workflow.sleep(timedelta(hours=1))   # ← replaces Sleep Activity
            await workflow.execute_activity(retry_operation, request)
```

---

## Shared Variables → Pass Data Explicitly

This is the most important migration pitfall in TIBCO BW.

**In TIBCO BW**, Shared Variables are global singletons that any process or activity in the engine can read and write. They are commonly used to share configuration, caches, or pass state between processes.

**In Temporal**, there is no equivalent. You should not use global mutable state in workflow code (it breaks determinism) or shared mutable state in activities (it causes race conditions under concurrent execution).

**Migration approaches:**

| Use of Shared Variable | Temporal Replacement |
|---|---|
| Configuration / connection strings | Environment variables or config files read at Worker startup, not in workflow/activity code |
| State shared between activity steps | Activity return values stored as local workflow variables |
| Counters / metrics | External metrics system (Prometheus, StatsD) written to from activities |
| Caches (avoid DB round-trips) | Application-level cache inside the Activity implementation (not shared across workers) |
| Cross-process coordination | Signals between workflows, or a shared external database |

---

## Checkpoints → Not Needed

TIBCO BW Checkpoints save the current process state to the database so it can recover after an engine restart. This was TIBCO's durability mechanism.

**In Temporal, you do not need checkpoints.** Every activity completion is automatically persisted as an event in the workflow's history. If a Worker restarts, the Temporal cluster replays the event history to reconstruct the workflow state. There is nothing to configure.

Remove all checkpoint logic when migrating. It adds complexity without any benefit in Temporal.

---

## Groups → Workflow Code Constructs

TIBCO BW Groups control execution behavior of a set of activities:

| BW Group Type | Temporal Equivalent |
|---|---|
| Sequence Group | Sequential activity calls (default in workflow code) |
| Loop Group (iterate) | `for` loop in workflow code; call activity inside the loop |
| While Loop Group | `while` loop in workflow code |
| Critical Section Group | Not typically needed (activities are isolated by default); use a Mutex signal pattern if truly needed |
| Pick First Group | `asyncio.wait(..., return_when=FIRST_COMPLETED)` or similar race pattern |
| Transaction Group | Saga pattern for compensating transactions |

---

## Error Handling

| TIBCO BW Error Construct | Temporal Equivalent |
|---|---|
| Catch Activity | `try/except` around `execute_activity()` |
| Fault scope on Group | `try/except` block wrapping multiple activity calls |
| Generate Error Activity | Raise/throw an exception in workflow code |
| Error Handler Process | Error-handling logic inline in the `except` block |
| Retry on Error (loop back to activity) | `RetryPolicy` on the `execute_activity()` call (preferred) or manual retry loop |

---

## Process Starters → Workflow Triggers

In TIBCO BW, a **Starter Activity** (HTTP Receiver, JMS Subscriber, File Poller, Timer) is what initiates a process instance.

In Temporal, the process is started externally — your application code calls `client.start_workflow()`. The starter logic moves out of the BW process into:

| BW Starter Type | Temporal Equivalent |
|---|---|
| HTTP Receiver | An HTTP handler (FastAPI, Spring, Express) that calls `client.start_workflow()` |
| JMS Subscriber | A message consumer that calls `client.start_workflow()` per message |
| File Poller | A polling loop (can be a long-running Activity or a separate service) that calls `client.start_workflow()` per file |
| Timer Starter | A Temporal Schedule that starts the workflow on a cron/interval schedule |
| TIBCO EMS Topic Subscriber | Message consumer that signals a running workflow or starts a new one |

---

## TIBCO BW 6.x (ActiveMatrix) Specifics

BW 6.x introduced OSGi-based module packaging and deployed to TIBCO ActiveMatrix. The migration is conceptually the same as BW 5.x, but the deployment model changes more significantly:

| BW 6.x Concept | Temporal Equivalent |
|---|---|
| Application Module (.bwm) | Go/Python/Java package with Activity and Workflow implementations |
| Application Archive (.ear/.bwear) | Container image / deployable artifact hosting a Temporal Worker |
| Application Node / Engine | Temporal Worker process |
| TIBCO Administrator / BW Agent | Temporal Web UI + `temporal` CLI for operations |
| Domain / Space | Temporal Namespace |
| Binding (expose process as service) | External HTTP/gRPC API that starts workflows or sends signals |

---

## Migration Checklist for TIBCO BusinessWorks

- [ ] Identify all Process Definitions → each becomes a Workflow Definition
- [ ] Map every Activity type to an Activity function implementation
- [ ] Remove all Shared Variable usage → pass data as activity arguments/return values
- [ ] Remove all Checkpoint activities → not needed in Temporal
- [ ] Convert each Starter Activity to an external trigger that calls `client.start_workflow()`
- [ ] Convert Loop Groups and While Groups to workflow `for`/`while` loops
- [ ] Convert Fault scopes and Catch activities to `try/except` blocks
- [ ] Convert Timer/Sleep activities to `workflow.sleep()` calls
- [ ] Convert Parallel execution to language-native parallel constructs
- [ ] Replace TIBCO Administrator operations with Temporal Web UI / `temporal` CLI
