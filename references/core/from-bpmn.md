# Migrating from BPMN-Based Tools to Temporal

This covers: **Camunda 7**, **Camunda 8 / Zeebe**, **Pega BPM**, **Appian**, **TIBCO BPM Enterprise**, and other BPMN 2.0 / process engine platforms.

*Verified against: Camunda 7.21, Camunda 8.4 / Zeebe 8.4*

---

## The BPMN Mental Model vs. Temporal

In BPMN-based systems, a **Process Definition** is an XML artifact deployed to a process engine. The engine maintains **Process Instances** and calls your code (Java Delegates, external task workers, REST services) when it reaches a task. The engine owns the state.

In Temporal, **your workflow function is the process definition**. There is no XML. There is no engine calling your code — your code runs on Workers that you deploy and manage. For the full mental model shift, read `references/core/mental-model.md`.

---

## BPMN Element → Temporal Equivalent

### Process / Subprocess

| BPMN | Temporal |
|---|---|
| Process Definition | Workflow Definition (`@WorkflowInterface` / `@workflow.defn`) |
| Process Instance | Workflow Execution (identified by `workflow_id`) |
| Subprocess | Child Workflow |
| Call Activity (reusable subprocess) | Child Workflow (same pattern) |
| Pool / Lane | Separate Task Queues; separate Worker deployments |

```java
// Camunda 7: Process deployed as BPMN XML, engine manages instances

// Temporal equivalent: Workflow interface + implementation
@WorkflowInterface
public interface OrderFulfillmentWorkflow {
    @WorkflowMethod
    OrderResult fulfill(OrderRequest request);
}

public class OrderFulfillmentWorkflowImpl implements OrderFulfillmentWorkflow {
    @Override
    public OrderResult fulfill(OrderRequest request) {
        // The entire process lives here in code
        ...
    }
}
```

### Tasks

| BPMN Task Type | Temporal Equivalent |
|---|---|
| Service Task (calls a Java class or REST) | Activity |
| Send Task (sends a message to external system) | Activity (performs the send) |
| Receive Task (waits for external message) | Signal handler |
| User Task (waits for human input via tasklist) | Signal handler (or Update) + external task management UI |
| Script Task (executes inline script) | Activity (move the script logic into an activity) |
| Business Rule Task (calls DMN decision) | Activity (calls your decision service) |
| Manual Task | Signal handler to acknowledge completion |

```java
// Camunda 7: Java Delegate for a Service Task
public class CheckInventoryDelegate implements JavaDelegate {
    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String orderId = (String) execution.getVariable("orderId");
        boolean inStock = inventoryService.check(orderId);
        execution.setVariable("inStock", inStock);
    }
}

// Temporal equivalent: Activity
@ActivityInterface
public interface InventoryActivities {
    @ActivityMethod
    boolean checkInventory(String orderId);
}

public class InventoryActivitiesImpl implements InventoryActivities {
    @Override
    public boolean checkInventory(String orderId) {
        return inventoryService.check(orderId);
    }
}
```

### Gateways

| BPMN Gateway | Temporal Equivalent |
|---|---|
| Exclusive Gateway (XOR) | `if/else` in workflow code |
| Inclusive Gateway (OR) | Multiple `if` statements, launch activities for all true conditions |
| Parallel Gateway (AND split) | Language-native parallel: `asyncio.gather`, `Promise.all`, `workflow.Go`, `Task.WhenAll` |
| Parallel Gateway (AND join) | Await all parallel activities before continuing |
| Event-Based Gateway | First signal/timer/message to arrive wins; use `workflow.wait_condition` or select/race patterns |
| Complex Gateway | Custom conditional logic in workflow code |

```python
# Camunda: Exclusive Gateway with two outgoing sequence flows
# condition1: ${amount > 1000}
# condition2: ${amount <= 1000}

# Temporal equivalent:
async def run(self, order: Order):
    if order.amount > 1000:
        result = await workflow.execute_activity(
            high_value_review, order, start_to_close_timeout=timedelta(hours=2)
        )
    else:
        result = await workflow.execute_activity(
            auto_approve, order, start_to_close_timeout=timedelta(minutes=1)
        )
```

```python
# Camunda: Parallel Gateway splits into 3 parallel service tasks, joins at a merge gateway

# Temporal equivalent: asyncio.gather
async def run(self, request):
    inventory, credit, fraud = await asyncio.gather(
        workflow.execute_activity(check_inventory, request),
        workflow.execute_activity(check_credit, request),
        workflow.execute_activity(check_fraud, request),
    )
    # All three have completed here — equivalent to the parallel join gateway
    if inventory and credit and not fraud:
        await workflow.execute_activity(approve_order, request)
```

### Events

| BPMN Event | Temporal Equivalent |
|---|---|
| Start Event (None) | Workflow starts when a client calls `start_workflow()` |
| Timer Start Event | Temporal Schedule (not an inline event — separate scheduling construct) |
| Message Start Event | `client.start_workflow()` called from a webhook handler or message consumer |
| Timer Intermediate Catch Event | `await workflow.sleep(duration)` |
| Message Intermediate Catch Event | Signal handler (`@workflow.signal`) |
| Signal Intermediate Catch Event | Signal handler (`@workflow.signal`) |
| Boundary Timer Event (interrupt) | `asyncio.wait_for` with `workflow.sleep` as the timeout; or start a timer and handle first-to-arrive |
| Boundary Error Event | `try/except` around `execute_activity()` |
| Boundary Signal Event | Signal handler that cancels or diverts current activity |
| End Event (None) | Workflow function returns normally |
| Error End Event | Raise / throw an exception from workflow code |
| Terminate End Event | `workflow.cancel()` or raise a non-retryable error |

```python
# Camunda: Timer Intermediate Catch Event — wait 3 days before sending reminder

# Temporal equivalent:
async def run(self, subscription):
    await workflow.execute_activity(send_welcome_email, subscription)
    await workflow.sleep(timedelta(days=3))            # ← Timer Intermediate Event
    await workflow.execute_activity(send_reminder_email, subscription)
```

```python
# Camunda: Message Intermediate Catch Event — wait for "payment_received" message

# Temporal equivalent: Signal handler
@workflow.defn
class OrderWorkflow:
    def __init__(self):
        self._payment_received = False
        self._payment_data = None

    @workflow.signal
    def payment_received(self, payment: PaymentData) -> None:
        self._payment_received = True
        self._payment_data = payment

    @workflow.run
    async def run(self, order: Order):
        await workflow.execute_activity(reserve_inventory, order)
        # Wait for payment signal (equivalent to Message Intermediate Catch Event)
        await workflow.wait_condition(lambda: self._payment_received)
        await workflow.execute_activity(fulfill_order, order, self._payment_data)
```

### Error Handling

| BPMN Error Construct | Temporal Equivalent |
|---|---|
| Error Boundary Event on Service Task | `try/except ActivityError` around `execute_activity()` |
| Error End Event | Raise/throw an exception |
| Error Boundary Event (non-interrupting) | `asyncio.gather` with error catch on one branch |
| Escalation Event | Signal + conditional logic in workflow code |
| Compensation Event | Saga pattern (run compensation activities in reverse order on failure) |

```python
# Camunda: Error Boundary Event on "Charge Payment" task catches "PaymentDeclined" error

# Temporal equivalent:
async def run(self, order):
    await workflow.execute_activity(reserve_inventory, order)
    try:
        await workflow.execute_activity(
            charge_payment, order,
            retry_policy=RetryPolicy(non_retryable_error_types=["PaymentDeclinedError"])
        )
    except ActivityError as e:
        if "PaymentDeclined" in str(e.cause):
            await workflow.execute_activity(release_inventory, order)
            await workflow.execute_activity(notify_customer_payment_failed, order)
            return OrderResult(status="PAYMENT_FAILED")
    await workflow.execute_activity(ship_order, order)
```

### Data Objects and Variables

| BPMN Concept | Temporal Equivalent |
|---|---|
| Process Variable | Local variable in workflow function (passed between activity calls) |
| Data Object | Activity input/output types (regular data structures) |
| Data Store (DB read/write from process) | Activity that reads/writes the data store |
| Input/Output mapping on tasks | Activity function parameters and return types |

**Key difference**: In BPMN, process variables are stored in the engine's database and can be read/written by any task at any time. In Temporal, state is implicit in local workflow variables. Pass data explicitly between activities as function arguments and return values.

---

## Camunda 7 Specifics

### Deployment Model Changes

| Camunda 7 | Temporal |
|---|---|
| Deploy BPMN XML via REST API or maven plugin | Deploy code — build and restart Workers |
| `processEngine.getRepositoryService().deploy(...)` | `git push`, build pipeline, rolling restart of Workers |
| Process Definition versioning in engine | Worker Versioning API or `workflow.get_version()` for in-flight compatibility |
| `processEngine.getRuntimeService().startProcessInstanceByKey(...)` | `client.start_workflow(MyWorkflow.run, args=[...], id="...", task_queue="...")` |

### Replacing Camunda Tasklist (User Tasks)

Camunda Tasklist provides a UI for humans to claim and complete User Tasks. In Temporal, User Tasks become:

1. A workflow that pauses waiting for a Signal
2. An external application (web app, mobile app) that shows the pending tasks by querying Temporal's Visibility API for workflows in a specific state
3. When the human submits their decision, the external app sends a Signal to the workflow

---

## Camunda 8 / Zeebe Specifics

Camunda 8 replaced the internal Java engine with Zeebe, a distributed broker. This makes the migration to Temporal conceptually closer since Zeebe also uses an external worker poll model.

| Camunda 8 / Zeebe | Temporal |
|---|---|
| Job Worker (polls for jobs of a specific type) | Temporal Worker (polls a Task Queue; registers Activity implementations) |
| `client.newActivateJobsCommand().jobType("check-inventory")` | Worker registered with `activities=[CheckInventoryActivitiesImpl()]`; Activity type derived from method name |
| `client.newCompleteJobCommand(job).variables(map)` | Activity function returns a value (SDK handles completion) |
| `client.newFailJobCommand(job).errorMessage("...")` | Activity throws an exception (SDK handles failure + retry) |
| Zeebe `process-id` (deploy process) | `workflow_type` (derived from Workflow class/function name) |
| `client.newCreateInstanceCommand().bpmnProcessId("...")` | `client.start_workflow(MyWorkflow.run, ...)` |

---

## Pega BPM

Pega uses a proprietary rule-based BPM platform. Key mappings:

| Pega Concept | Temporal Equivalent |
|---|---|
| Case Type | Workflow Definition |
| Case Instance | Workflow Execution |
| Stage / Step | Sequential section in workflow code |
| Process / Flow | Workflow function |
| Assignment (routed to user/queue) | Signal handler + external UI polling Visibility API |
| Connector | Activity (performs the integration call) |
| Decision Rule | Activity (calls decision service) or `if/else` in workflow code |
| Circumstance | Conditional logic in workflow code |
| Service Level Agreement (SLA) | Workflow/Activity timeout + notification activity on timeout |
| Work Queue | Task Queue |
| Correspondence (automated email/letter) | Activity that sends the email/letter |

---

## Appian

Appian is a low-code BPM platform. Key mappings:

| Appian Concept | Temporal Equivalent |
|---|---|
| Process Model | Workflow Definition |
| Process Instance | Workflow Execution |
| Smart Service (automated node) | Activity |
| User Input Task | Signal handler + external UI |
| Gateway | `if/else` or parallel execution in workflow code |
| Process Variable | Local workflow variable |
| Integration | Activity (calls the integration) |
| Record Type | External data model (accessed via Activity) |
| Expression Rule | Regular function in your code |
| Timer (delay, calendar) | `workflow.sleep()` or Temporal Schedule |

---

## Migration Checklist for BPMN Tools

- [ ] Every Service Task → Activity function
- [ ] Every Exclusive Gateway → `if/else` branch in workflow code
- [ ] Every Parallel Gateway → parallel activity execution (`asyncio.gather`, etc.)
- [ ] Every Timer Event → `workflow.sleep()` or Temporal Schedule
- [ ] Every Message/Signal Catch Event → `@workflow.signal` handler
- [ ] Every User Task → Signal handler + external task UI
- [ ] Every Subprocess / Call Activity → Child Workflow
- [ ] Every Error Boundary Event → `try/except` with appropriate catch type
- [ ] Every Compensation → Saga pattern (tracked list of compensations to run on failure)
- [ ] Process Variables → local variables passed explicitly as activity arguments/return values
- [ ] Remove all BPMN process deployment infrastructure
- [ ] Build and deploy Temporal Workers instead
