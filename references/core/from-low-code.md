# Migrating from N8n to Temporal

*Verified against: N8n 1.x*

---

## N8n vs. Temporal: Core Difference

**N8n** is a visual, node-based workflow automation tool. You drag and drop nodes onto a canvas, connect them with edges, and the N8n runtime executes them in sequence. It is primarily designed for *integrations* — connecting SaaS tools and APIs with minimal code.

**Temporal** is a code-first durable execution platform. There is no visual canvas. Your workflow logic is code you own, test, and deploy. Temporal is designed for *complex business processes* that need reliability, long-running execution, strong error handling, and developer-grade tooling.

**When to migrate from N8n to Temporal:**
- Your N8n workflows are becoming too complex to maintain visually
- You need long-running workflows (hours, days, weeks) that survive restarts
- You need strong retry semantics, compensation, or saga patterns
- You need unit-testable workflow logic
- You need code review, branching, and CI/CD for workflow changes

---

## Concept Mapping

| N8n Concept | Temporal Equivalent |
|---|---|
| Workflow (the graph of nodes) | Workflow Definition (`@workflow.defn`) |
| Workflow Execution (a run) | Workflow Execution |
| Node (a step in the graph) | Activity |
| Connection (data flowing between nodes) | Activity return value passed as argument to next activity |
| Item (a piece of data flowing through the workflow) | Activity input/output type (a data class or typed object) |
| Trigger Node (starts the workflow) | External trigger: HTTP handler / message consumer calling `client.start_workflow()`, or a Temporal Schedule |
| Cron Trigger Node | Temporal Schedule |
| Webhook Trigger Node | HTTP endpoint calling `client.start_workflow()` or sending a Signal |
| Manual Trigger Node | Direct call to `client.start_workflow()` from CLI or test code |
| Wait Node (pause until time or webhook) | `workflow.sleep(duration)` or Signal handler |
| IF Node / Switch Node | `if/else` in workflow code |
| Loop Over Items Node | `for` loop in workflow code |
| Set Node (transform data) | Inline transformation in workflow code or a small Activity |
| Merge Node (combine branches) | Collect results from parallel activities |
| HTTP Request Node | Activity that makes an HTTP call |
| Code Node (run custom JS/Python) | Activity function (the code you wrote in the Code node, promoted to a full activity) |
| Credentials (stored secrets) | Environment variables / secrets manager accessed in Activity implementation |
| Error Workflow (handle failures) | `try/except` in workflow code + Activity RetryPolicy |
| Execute Workflow Node | Child Workflow |
| N8n Platform / Cloud | Temporal Cluster + Temporal Workers |
| N8n Worker | Temporal Worker |
| Execution log | Temporal Web UI event history |

---

## Node-by-Node Migration

### Trigger Nodes

N8n trigger nodes start a workflow. In Temporal, the trigger is external — something calls `client.start_workflow()`.

```python
# N8n: Webhook Trigger Node (POST /webhook/order-received)
# → Automatically starts the workflow when webhook fires

# Temporal equivalent: FastAPI endpoint that starts the workflow
from fastapi import FastAPI
app = FastAPI()

@app.post("/webhook/order-received")
async def order_received(order: OrderPayload):
    handle = await temporal_client.start_workflow(
        OrderWorkflow.run,
        order,
        id=f"order-{order.order_id}",
        task_queue="orders",
    )
    return {"workflow_id": handle.id}
```

```python
# N8n: Cron Trigger Node (run daily at 6am)

# Temporal equivalent: Temporal Schedule
await temporal_client.create_schedule(
    "daily-report",
    Schedule(
        action=ScheduleActionStartWorkflow(
            DailyReportWorkflow.run,
            id="daily-report-{scheduled_time}",
            task_queue="reports",
        ),
        spec=ScheduleSpec(cron_expressions=["0 6 * * *"]),
    ),
)
```

### HTTP Request Node → Activity

```python
# N8n: HTTP Request Node
# Method: POST
# URL: https://api.stripe.com/v1/charges
# Body: { amount, currency, source }
# Auth: Bearer token from Credentials

# Temporal equivalent:
@activity.defn
async def create_stripe_charge(request: StripeChargeRequest) -> StripeCharge:
    headers = {"Authorization": f"Bearer {os.environ['STRIPE_SECRET_KEY']}"}
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.stripe.com/v1/charges",
            data=request.to_form_data(),
            headers=headers,
        ) as resp:
            resp.raise_for_status()
            return StripeCharge(**await resp.json())
```

### Code Node → Activity

The N8n Code Node is the closest thing to a Temporal Activity. The migration is straightforward: take the code from the Code Node and put it in an Activity function.

```javascript
// N8n: Code Node (JavaScript)
const items = $input.all();
const results = [];
for (const item of items) {
  const transformed = {
    id: item.json.orderId,
    total: item.json.price * item.json.quantity,
    currency: item.json.currency.toUpperCase(),
  };
  results.push({ json: transformed });
}
return results;
```

```typescript
// Temporal equivalent: Activity (TypeScript)
import { defineActivity } from '@temporalio/activity';

export const transformOrderItems = defineActivity(
  'transformOrderItems',
  async (items: OrderItem[]): Promise<TransformedItem[]> => {
    return items.map(item => ({
      id: item.orderId,
      total: item.price * item.quantity,
      currency: item.currency.toUpperCase(),
    }));
  }
);
```

### IF Node / Switch Node → Conditional Code

```python
# N8n: IF Node
# Condition: {{ $json.amount }} > 1000

# Temporal equivalent: if/else in workflow code
@workflow.run
async def run(self, order: Order):
    if order.amount > 1000:
        result = await workflow.execute_activity(process_high_value_order, order)
    else:
        result = await workflow.execute_activity(process_standard_order, order)
    return result
```

### Loop Over Items → for loop

```python
# N8n: Loop Over Items — process each customer in a list

# Temporal equivalent: for loop in workflow code
@workflow.run
async def run(self, customer_ids: list[str]):
    results = []
    for customer_id in customer_ids:
        result = await workflow.execute_activity(
            process_customer, customer_id,
            start_to_close_timeout=timedelta(minutes=5),
        )
        results.append(result)
    return results

# Note: For very large lists (thousands of items), use Continue-As-New to avoid
# hitting the history size limit. See temporal-developer skill references/core/patterns.md.
```

### Wait Node → workflow.sleep or Signal

```python
# N8n: Wait Node (wait 24 hours, then continue)

# Temporal equivalent: workflow.sleep
@workflow.run
async def run(self, user):
    await workflow.execute_activity(send_welcome_email, user)
    await workflow.sleep(timedelta(hours=24))   # ← survives restarts; no polling needed
    await workflow.execute_activity(send_onboarding_followup, user)
```

```python
# N8n: Wait Node (wait until webhook fires with approval)
# (N8n resumes the execution when a specific webhook is called)

# Temporal equivalent: Signal handler
@workflow.defn
class ApprovalWorkflow:
    def __init__(self):
        self._approved = False

    @workflow.signal
    def approve(self) -> None:
        self._approved = True

    @workflow.run
    async def run(self, request):
        await workflow.execute_activity(send_for_approval, request)
        await workflow.wait_condition(lambda: self._approved)
        await workflow.execute_activity(process_approved_request, request)
```

### Merge Node → Collect parallel results

```python
# N8n: Three parallel branches → Merge Node combines results

# Temporal equivalent: asyncio.gather
@workflow.run
async def run(self, order):
    inventory, fraud, credit = await asyncio.gather(
        workflow.execute_activity(check_inventory, order),
        workflow.execute_activity(check_fraud, order),
        workflow.execute_activity(check_credit, order),
    )
    # All three have completed — equivalent to Merge Node
    return await workflow.execute_activity(finalize_order, order, inventory, fraud, credit)
```

### Execute Workflow Node → Child Workflow

```python
# N8n: Execute Workflow Node (runs another N8n workflow as a sub-process)

# Temporal equivalent: execute_child_workflow
@workflow.run
async def run(self, order):
    # Start a child workflow for shipping
    shipping_result = await workflow.execute_child_workflow(
        ShippingWorkflow.run,
        order,
        id=f"shipping-{order.id}",
    )
    return shipping_result
```

### Error Workflow → try/except + RetryPolicy

```python
# N8n: Error Workflow (separate workflow triggered when the main one fails)

# Temporal equivalent: try/except in workflow code + Activity RetryPolicy
@workflow.run
async def run(self, order):
    try:
        result = await workflow.execute_activity(
            process_payment, order,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                non_retryable_error_types=["InvalidCardError"],
            ),
        )
    except ActivityError as e:
        # Error handling inline — no separate "error workflow" needed
        await workflow.execute_activity(notify_payment_failure, order, str(e))
        raise  # re-raise to mark workflow as failed, or return a failure result
```

---

## Credentials → Environment Variables / Secrets Manager

N8n Credentials are stored in the N8n platform and injected into nodes at runtime.

In Temporal, credentials are the responsibility of the Activity implementation. Recommended approaches:

1. **Environment variables** injected into the Worker process at deploy time
2. **AWS Secrets Manager / HashiCorp Vault** — fetch the secret in the Activity function (cache it at the Worker level; do not fetch it on every activity call)
3. **Kubernetes Secrets** mounted into the Worker pod

Never pass credentials as Workflow or Activity inputs — they will be stored in the event history.

---

## Migration Checklist for N8n

- [ ] Every N8n node → Temporal Activity function
- [ ] Code Node content → promoted to a standalone Activity function
- [ ] Webhook / Cron Trigger → external HTTP handler or Temporal Schedule
- [ ] IF / Switch → `if/else` in workflow code
- [ ] Loop Over Items → `for` loop in workflow code
- [ ] Wait Node (time-based) → `workflow.sleep()`
- [ ] Wait Node (webhook-based) → Signal handler
- [ ] Execute Workflow Node → Child Workflow
- [ ] Error Workflow → `try/except` + RetryPolicy
- [ ] Credentials → environment variables / secrets manager in Activity
- [ ] Remove N8n platform; deploy Temporal Workers
