# TypeScript Migration Examples

Side-by-side code translations for TypeScript/JavaScript developers migrating to Temporal. Primary source tools covered: **N8n** and general webhook/automation patterns.

For SDK setup and Worker configuration, refer to the `temporal-developer` skill (`references/typescript/typescript.md`).

*Verified against: Temporal TypeScript SDK 1.x, N8n 1.x*

---

## N8n Workflow → Temporal Workflow + Activities

### Basic node sequence → sequential Activities

```typescript
// ── N8N (workflow JSON description) ─────────────────────────────────────────
// Nodes:
//   Webhook Trigger → HTTP Request (fetch customer) → Code Node (transform)
//   → HTTP Request (send to CRM) → End


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
// activities.ts
import * as activity from '@temporalio/activity';

export async function fetchCustomer(customerId: string): Promise<Customer> {
  const res = await fetch(`https://api.example.com/customers/${customerId}`);
  if (!res.ok) throw new Error(`Fetch failed: ${res.status}`);
  return res.json();
}

export async function transformCustomer(customer: Customer): Promise<CrmPayload> {
  return {
    externalId: customer.id,
    fullName: `${customer.firstName} ${customer.lastName}`,
    email: customer.email.toLowerCase(),
  };
}

export async function sendToCrm(payload: CrmPayload): Promise<void> {
  const res = await fetch('https://crm.example.com/contacts', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${process.env.CRM_API_KEY}`,
    },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`CRM sync failed: ${res.status}`);
}


// workflow.ts
import { defineWorkflow, proxyActivities } from '@temporalio/workflow';
import type * as activities from './activities';

const { fetchCustomer, transformCustomer, sendToCrm } =
  proxyActivities<typeof activities>({
    startToCloseTimeout: '30 seconds',
    retry: { maximumAttempts: 3 },
  });

export async function customerSyncWorkflow(customerId: string): Promise<void> {
  const customer = await fetchCustomer(customerId);
  const payload = await transformCustomer(customer);
  await sendToCrm(payload);
}


// Webhook endpoint that starts the workflow (replaces N8n Webhook Trigger node):
// server.ts (Express / Fastify / etc.)
app.post('/webhook/customer-sync', async (req, res) => {
  const { customerId } = req.body;
  await temporalClient.workflow.start(customerSyncWorkflow, {
    args: [customerId],
    taskQueue: 'customer-sync',
    workflowId: `customer-sync-${customerId}`,
  });
  res.status(202).json({ message: 'sync started' });
});
```

---

### N8n Code Node → Activity

```typescript
// ── N8N: Code Node ────────────────────────────────────────────────────────────
// JavaScript code in the N8n Code Node:
const items = $input.all();
const results = [];
for (const item of items) {
  results.push({
    json: {
      id: item.json.orderId,
      total: item.json.price * item.json.quantity,
      currency: item.json.currency.toUpperCase(),
    },
  });
}
return results;


// ── TEMPORAL: Activity ────────────────────────────────────────────────────────
// activities.ts
export interface OrderItem {
  orderId: string;
  price: number;
  quantity: number;
  currency: string;
}

export interface TransformedItem {
  id: string;
  total: number;
  currency: string;
}

export async function transformOrderItems(items: OrderItem[]): Promise<TransformedItem[]> {
  return items.map((item) => ({
    id: item.orderId,
    total: item.price * item.quantity,
    currency: item.currency.toUpperCase(),
  }));
}
```

---

### N8n IF Node → if/else in workflow code

```typescript
// ── N8N: IF Node ─────────────────────────────────────────────────────────────
// Condition: {{ $json.amount }} > 1000
// True branch → High Value node
// False branch → Standard node


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
// workflow.ts
const { processHighValue, processStandard } = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 minutes',
});

export async function processOrderWorkflow(order: Order): Promise<OrderResult> {
  if (order.amount > 1000) {
    return await processHighValue(order);
  }
  return await processStandard(order);
}
```

---

### N8n Loop Over Items → for loop

```typescript
// ── N8N: Loop Over Items node ─────────────────────────────────────────────────
// Loops over each item in the input, processes them one by one


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
export async function processBatchWorkflow(batchId: string): Promise<ProcessResult[]> {
  const items = await getItems(batchId);
  const results: ProcessResult[] = [];

  for (const item of items) {
    const result = await processItem(item);  // sequential
    results.push(result);
  }
  return results;

  // Or process in parallel:
  // return await Promise.all(items.map((item) => processItem(item)));
}
```

---

### N8n Wait Node (time-based) → workflow.sleep

```typescript
// ── N8N: Wait Node ────────────────────────────────────────────────────────────
// Wait 24 hours, then continue to next node


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
import { sleep } from '@temporalio/workflow';

export async function onboardingWorkflow(userId: string): Promise<void> {
  await sendWelcomeEmail(userId);
  await sleep('24 hours');           // durable — survives Worker restarts
  await sendOnboardingFollowup(userId);
  await sleep('6 days');
  await sendWeekOneCheckIn(userId);
}
```

---

### N8n Wait Node (webhook resume) → Signal handler

```typescript
// ── N8N: Wait Node (webhook resume) ──────────────────────────────────────────
// Pauses execution until a specific webhook URL is called


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
// workflow.ts
import { defineSignal, setHandler, condition } from '@temporalio/workflow';

export const approvalSignal = defineSignal<[ApprovalDecision]>('approvalReceived');

export async function approvalWorkflow(requestId: string): Promise<string> {
  await sendForApproval(requestId);

  let decision: ApprovalDecision | undefined;
  setHandler(approvalSignal, (d) => { decision = d; });

  // Wait up to 7 days for the signal
  const received = await condition(() => decision !== undefined, '7 days');
  if (!received) {
    await escalateApproval(requestId);
    return 'ESCALATED';
  }

  if (decision!.approved) {
    await processApproved(requestId);
    return 'APPROVED';
  }
  await processRejected(requestId);
  return 'REJECTED';
}

// Send the signal from your web app (replaces N8n webhook resume URL):
// await temporalClient.workflow.getHandle(workflowId)
//   .signal(approvalSignal, { approved: true, reviewer: 'alice' });
```

---

### N8n Execute Workflow → Child Workflow

```typescript
// ── N8N: Execute Workflow node ────────────────────────────────────────────────
// Runs another N8n workflow as a sub-process


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
import { executeChild } from '@temporalio/workflow';

export async function parentOrderWorkflow(order: Order): Promise<void> {
  await processOrderWorkflow(order);  // could also be executeChild if separate type

  // For a distinct workflow type:
  const shippingResult = await executeChild(shippingWorkflow, {
    args: [order],
    workflowId: `shipping-${order.id}`,
    taskQueue: 'shipping',
  });

  await sendConfirmation(order, shippingResult);
}
```

---

### N8n Error Workflow → try/catch + RetryPolicy

```typescript
// ── N8N: Error Workflow ───────────────────────────────────────────────────────
// Separate workflow triggered automatically when the main workflow fails


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
import { ActivityFailure } from '@temporalio/workflow';

const { processOrder, sendSlackAlert, sendEmail } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: {
    maximumAttempts: 3,
    initialInterval: '10 seconds',
    nonRetryableErrorTypes: ['InvalidOrderError'],
  },
});

export async function orderWorkflow(order: Order): Promise<void> {
  try {
    await processOrder(order);
  } catch (err) {
    if (err instanceof ActivityFailure) {
      // Inline error handling — no separate "error workflow" needed
      await sendSlackAlert(`Order ${order.id} failed: ${err.message}`);
      await sendEmail(order.customerId, 'order-failed', { orderId: order.id });
    }
    throw err;  // re-throw to mark workflow as failed
  }
}
```

---

## Cron Schedule → Temporal Schedule

```typescript
// ── N8N: Cron Trigger Node ────────────────────────────────────────────────────
// Runs workflow every day at 06:00


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
import {
  ScheduleClient,
  ScheduleOverlapPolicy,
  ScheduleSpec,
} from '@temporalio/client';

const scheduleClient = new ScheduleClient({ connection });

await scheduleClient.create({
  scheduleId: 'daily-report',
  spec: {
    cronExpressions: ['0 6 * * *'],  // 06:00 daily
  },
  action: {
    type: 'startWorkflow',
    workflowType: dailyReportWorkflow,
    args: [{ reportType: 'daily-summary' }],
    taskQueue: 'reports',
    workflowId: 'daily-report-{scheduled_time}',
  },
  policies: {
    overlap: ScheduleOverlapPolicy.SKIP,
  },
});
```

---

## Parallel HTTP Calls (replaces N8n parallel branches)

```typescript
// ── N8N: Three parallel HTTP Request nodes → Merge ────────────────────────────
// Three nodes run in parallel, their results are merged


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
const { checkInventory, checkFraud, checkCredit, finalizeOrder } =
  proxyActivities<typeof activities>({ startToCloseTimeout: '1 minute' });

export async function orderValidationWorkflow(order: Order): Promise<ValidationResult> {
  const [inventory, fraud, credit] = await Promise.all([
    checkInventory(order),
    checkFraud(order),
    checkCredit(order),
  ]);
  // All three have completed — equivalent to N8n Merge node
  return await finalizeOrder(order, { inventory, fraud, credit });
}
```
