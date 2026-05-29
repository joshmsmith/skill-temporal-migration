# TIBCO BusinessWorks → Temporal: Credit App Migration Sample

This sample demonstrates a complete migration from two TIBCO BW 6.x applications
to a single Temporal Python worker, using the
[temporal-migration skill](../../SKILL.md).

**Source code**: [TIBCOSoftware/bw-samples — TN2018/Apps](https://github.com/TIBCOSoftware/bw-samples/tree/master/TN2018/Apps)

---

## BW Applications Migrated

| BW Application | BW Process | Role |
|---|---|---|
| `CreditAppService` | `MainProcess.bwp` | HTTP entry point; fans out to Equifax + Experian |
| `CreditAppService` | `EquifaxScore.bwp` | Sub-process; HTTP POST to Equifax mock |
| `CreditAppService` | `ExperianScore.bwp` | Sub-process; HTTP POST to Experian mock |
| `CreditCheckBackendService` | `Process.bwp` | HTTP entry point; calls LookupDatabase |
| `CreditCheckBackendService` | `LookupDatabase.bwp` | Sub-process; JDBC SELECT + UPDATE |

---

## Complete BW → Temporal Mapping

### Structural Mappings

| BW Construct | Temporal Equivalent | Notes |
|---|---|---|
| BW Application Module | Worker process | `worker.py` |
| Process Definition (`.bwp`) | `@workflow.defn` class | One per top-level service process |
| Sub-process (CallProcess) | `@activity.defn` function | Flattened — no sub-workflow overhead |
| HTTP Receiver Starter | FastAPI route → `client.start_workflow()` | Stubs in `worker.py` |
| bpws:flow (parallel group) | `asyncio.gather()` | `CreditApplicationWorkflow` |
| bpws:catchAll fault handler | `try / except` | `CreditCheckWorkflow` |
| JDBC Connection SharedResource | `asyncpg.Pool` (module-level) | `init_db_pool()` in `worker.py` |
| HTTP Client SharedResource | `aiohttp.ClientSession` (per-activity) | Stateless; no pool needed |
| bpws:throw DefaultFault | `ApplicationError(non_retryable=True)` | `lookup_database` activity |
| bw.generalactivities.log | `workflow.logger.info/error()` | No separate activity needed |
| Checkpoint activity | _(dropped)_ | Temporal event history provides durability |
| Shared Variable | _(dropped)_ | Data is passed as activity args / return values |
| RenderJSON / ParseJSON activities | _(dropped)_ | Merged into HTTP activities; Python dicts |

### Activity Mappings

| BW Activity (in BW process) | Temporal Activity | File |
|---|---|---|
| `bw.http.sendHTTPRequest` in `EquifaxScore.bwp` | `get_equifax_score` | `activities.py` |
| `bw.http.sendHTTPRequest` in `ExperianScore.bwp` | `get_experian_score` | `activities.py` |
| `bw.jdbc.JDBCQuery` in `LookupDatabase.bwp` | `lookup_database` | `activities.py` |
| `bw.jdbc.update` in `LookupDatabase.bwp` | `update_pull_count` | `activities.py` |

### Workflow Mappings

| BW Process | Temporal Workflow | Key BW → Temporal Translation |
|---|---|---|
| `MainProcess.bwp` + `EquifaxScore.bwp` + `ExperianScore.bwp` | `CreditApplicationWorkflow` | Parallel `bpws:flow` → `asyncio.gather()` |
| `Process.bwp` + `LookupDatabase.bwp` | `CreditCheckWorkflow` | `bpws:catchAll` → `try/except`; DB split into two activities |

---

## Architecture

```
HTTP Client
    │
    ├─ POST /creditdetails ──► FastAPI stub ──► CreditApplicationWorkflow
    │                                                │
    │                                    asyncio.gather()  ← parallel bpws:flow
    │                                      ┌────────┴────────┐
    │                               get_equifax_score   get_experian_score
    │                               (HTTP → Equifax)    (HTTP → Experian)
    │
    └─ POST /creditscore ───► FastAPI stub ──► CreditCheckWorkflow
                                                    │
                                           lookup_database (JDBC SELECT)
                                                    │
                                         update_pull_count (JDBC UPDATE)
```

---

## Key Design Decisions

**Sub-processes flattened to Activities, not sub-Workflows**

`EquifaxScore.bwp`, `ExperianScore.bwp`, and `LookupDatabase.bwp` are all
single-step stateless operations (one HTTP call or one SQL query).  Wrapping
them in `@workflow.defn` would add event-history overhead with no benefit.
See [from-tibco-bw.md](../../references/core/from-tibco-bw.md) — *Sub-Processes
→ Activities or Sub-Workflows*.

**`lookup_database` and `update_pull_count` are separate activities**

In BW, `LookupDatabase.bwp` contained both operations in one process.  Splitting
them means each has its own retry policy — a transient DB connection error on the
UPDATE retries without re-running the SELECT.

**`asyncio.gather` for the Equifax/Experian fan-out**

BW's parallel `bpws:flow` runs both sub-processes concurrently.  `asyncio.gather`
is the direct Python equivalent and preserves determinism because both branches
are Activities (their scheduling and results are recorded in Temporal event history).

---

## Running Locally

```bash
# 1. Start Temporal dev server
temporal server start-dev

# 2. Install dependencies
pip install temporalio aiohttp asyncpg

# 3. Set DB connection string (or edit worker.py)
export DB_DSN="postgresql://postgres:postgres@localhost:5432/creditdb"

# 4. Start the worker
python -m samples.tibco-creditapp.temporal.worker

# 5. Trigger via CLI (no FastAPI needed for testing)
temporal workflow start \
  --type CreditApplicationWorkflow \
  --task-queue credit-app \
  --input '{"ssn":"123-45-6789","first_name":"Jane","last_name":"Doe","dob":"1985-04-12"}'
```

---

## Skill References Used

- [references/core/from-tibco-bw.md](../../references/core/from-tibco-bw.md) — primary mapping guide
- [references/python/examples.md](../../references/python/examples.md) — asyncio.gather, RetryPolicy patterns
- [references/core/mental-model.md](../../references/core/mental-model.md) — why sub-processes become Activities
