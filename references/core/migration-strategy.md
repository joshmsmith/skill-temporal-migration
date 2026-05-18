# Migration Strategy: Greenfield, Strangler-Fig, and Parallel Run

Choosing the wrong migration strategy is the second most common reason Temporal migrations stall (after carrying the wrong mental model). This guide covers the three main strategies and when to use each.

---

## Strategy 1: Greenfield Rewrite

**What it is:** Rewrite the workflow logic from scratch in Temporal. Decommission the source system once done.

**When to use it:**
- You're building a net-new feature and the old process is being replaced entirely
- The old process is simple enough to fully map to Temporal in one sprint
- The source system is end-of-life or you can afford a maintenance window
- There are no in-flight workflow instances that need to complete (or you can terminate them)

**Steps:**

1. **Map concepts**: Read the source-tool-specific reference file and list every construct in your existing process. For each one, identify the Temporal equivalent.

2. **Identify activities**: Any operation that touches the outside world (API call, DB write, file read, message send) becomes a Temporal Activity. Identify these first.

3. **Define interfaces**: Write the Activity interfaces and the Workflow interface before writing any implementation. These define the contract.

4. **Write and test the workflow**: Write the Workflow function. Unit test it with mocked activities using the Temporal test framework before connecting to a real cluster.

5. **Deploy workers**: Deploy your Temporal Workers alongside the existing system. Start with a dev/staging environment.

6. **Shadow run** (optional but recommended): Route a fraction of new work to Temporal while the old system still processes the rest. Compare outcomes.

7. **Cut over**: Stop sending new work to the old system. Let the old system drain (complete in-flight instances). Decommission.

**Common failure mode:** Trying to replicate the old system's structure exactly. Don't reconstruct BPMN gateways, Airflow task graphs, or job plan dependency trees in Temporal. Write natural code.

---

## Strategy 2: Strangler Fig (Incremental Migration)

**What it is:** Route new work to Temporal while old work continues to run in the legacy system. Replace the legacy system piecemeal over time.

Named after the strangler fig tree, which grows around an existing tree and eventually replaces it without ever needing to cut the old tree down.

**When to use it:**
- You have many long-running workflows that cannot be terminated
- The source system processes diverse job types and you can migrate them one type at a time
- You need to validate Temporal in production before committing fully
- Your team needs to build Temporal expertise incrementally

**Approaches:**

### A. Migration by Workflow Type / Job Type

Route specific workflow types or job types to Temporal while others remain in the legacy system.

```
Old System (Control-M)        Temporal
─────────────────────         ─────────────────────
  payroll_job        ──────►  (migrated to Temporal)
  data_export_job    (stays in Control-M for now)
  report_job         (stays in Control-M for now)
```

**Implementation:**
1. Pick the simplest, least-risky job type first
2. Implement it in Temporal and run it in staging
3. Once validated, stop creating new instances of that type in the old system
4. Let existing instances of that type drain to completion
5. Repeat for the next job type

### B. Migration by Environment

Migrate prod-equivalent staging environment first, validate, then migrate production.

### C. Feature-Flag Routing

Use a feature flag or configuration to decide whether a new process instance goes to the old system or Temporal:

```python
def start_order_process(order_id: str):
    if feature_flags.use_temporal_for_orders():
        temporal_client.start_workflow(
            OrderWorkflow.run,
            order_id,
            id=f"order-{order_id}",
            task_queue="orders",
        )
    else:
        legacy_bpm_engine.start_process("order-fulfillment", {"orderId": order_id})
```

This allows gradual rollout (1% → 10% → 100%) with easy rollback.

**Key constraint:** During the strangler-fig period, both systems run simultaneously. Avoid writing business logic that depends on state from both systems at once — that leads to complex synchronization bugs.

---

## Strategy 3: Parallel Run (Validation)

**What it is:** Run both the old system and Temporal for the same work simultaneously. Compare results. Use the parallel run to build confidence before cutting over.

**When to use it:**
- The process is business-critical and you need high confidence before cutting over
- You want to detect behavior differences between the old system and the new implementation before they affect production
- Your organization has a "shadow mode" or "dark launch" culture

**Pattern:**

```
Incoming trigger / job start
        │
        ├──► Old System (authoritative — results used in production)
        │
        └──► Temporal (shadow — results discarded, but compared)
                │
                ▼
         Comparison service / log comparison
```

**Implementation steps:**
1. Build the Temporal workflow implementation
2. Add an "also start in Temporal" side path to the existing trigger point
3. Both systems process the same input
4. Compare outputs: same result? Same timing? Same retries triggered?
5. Log discrepancies for investigation
6. Once discrepancy rate is acceptable, flip Temporal to authoritative

**Cost:** This doubles the operational work for the duration of the parallel run. It also requires the old system and Temporal to process the same inputs idempotently (important for side effects like sending emails or writing to databases — use idempotency keys or deduplication).

---

## Handling In-Flight Workflow Instances

The hardest part of any migration is what to do with workflows that are already running when you want to cut over.

### Option A: Drain and Wait

Stop creating new instances in the old system. Wait for all existing instances to complete naturally. Then decommission.

**Best when:** Workflows are short-lived (minutes to hours) and you can afford to wait.

**Risk:** Some workflows may be stuck or have very long timers. Audit and terminate/cancel any workflows that are in an indefinite wait state.

### Option B: Signal-Based Drain

Add a "drain" mode to the legacy system. When drain mode is enabled:
1. The old system processes the current step, then stops
2. It records the current state to a shared store (DB table, queue)
3. A Temporal workflow is started with that state as input

This is complex but allows you to migrate long-running workflows mid-execution. It requires custom handoff logic for each workflow type.

### Option C: Hard Cutover with Accepted Data Loss Window

Pick a cutover datetime. Before that datetime, all instances run in the old system. After that datetime, all new instances run in Temporal. Instances that were running at cutover time are terminated (with appropriate business notification).

**Best when:** The process is idempotent or can be restarted from the beginning without business impact.

### Option D: Replay from Event Log

If the old system can export its event log (process instance history), you can reconstruct the state and replay it into a new Temporal workflow. This is tool-specific and often requires custom tooling.

---

## Post-Migration Verification Checklist

After going live on Temporal, verify:

- [ ] All expected workflow types are showing up in Temporal Web UI
- [ ] Retry behavior matches or improves on the old system
- [ ] Scheduling (cron/intervals) fires at the expected times
- [ ] Long-running workflows survive Worker restarts without issue
- [ ] Search attributes and memo fields populate correctly for operational queries
- [ ] Alerts/notifications fire on workflow/activity failures
- [ ] Workers are sized appropriately (see `temporal-workertuning` skill)
- [ ] No workflows are stuck in Running state unexpectedly
- [ ] The old system is confirmed decommissioned (not just dormant) to avoid confusion

---

## Choosing Between Strategies: Decision Guide

```
Start here: Do you have running instances in the old system?
│
├── No → Are you replacing the entire system at once?
│         ├── Yes → Greenfield Rewrite
│         └── No  → Strangler Fig (by job/workflow type)
│
└── Yes → Can you terminate/drain them within your timeline?
          ├── Yes → Drain + Greenfield Rewrite
          └── No  → Strangler Fig + Signal-Based Drain for critical in-flight instances
                    (consider Parallel Run for high-risk validation)
```
