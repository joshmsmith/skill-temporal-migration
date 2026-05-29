"""
worker.py — Temporal Worker for the TIBCO BW CreditApp migration sample.

Starts a single worker that handles both services that were separate BW applications:
  - CreditAppService        (MainProcess → CreditApplicationWorkflow)
  - CreditCheckBackendService (Process   → CreditCheckWorkflow)

Both services share the same asyncpg connection pool and task queue.

BW → Temporal infrastructure mapping:
  BW Application Runtime             → Temporal Worker process
  BW HTTP Receiver Starter           → FastAPI route (stubs below) calling client.start_workflow()
  BW JDBC Connection Shared Resource → asyncpg.Pool (module-level, init'd at startup)
  BW HTTP Client Shared Resource     → aiohttp.ClientSession per-activity (stateless)
  BW AppSpace / Engine               → Temporal Namespace (default)
"""

from __future__ import annotations

import asyncio
import logging
import os

import asyncpg
from temporalio.client import Client
from temporalio.worker import Worker

from .activities import (
    close_db_pool,
    get_equifax_score,
    get_experian_score,
    init_db_pool,
    lookup_database,
    update_pull_count,
)
from .workflows import CreditApplicationWorkflow, CreditCheckWorkflow

# ---------------------------------------------------------------------------
# Configuration — in production, read from environment or a secrets manager.
# ---------------------------------------------------------------------------

TEMPORAL_HOST = os.environ.get("TEMPORAL_HOST", "localhost:7233")
TASK_QUEUE = "credit-app"
DB_DSN = os.environ.get(
    "DB_DSN",
    "postgresql://postgres:postgres@localhost:5432/creditdb",
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Worker entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    # ── 1. Initialise shared DB connection pool ──────────────────────────────
    # Replaces BW JDBC Connection Shared Resource (JDBCConnectionResource).
    # BW created this once per Application Module and shared it across all
    # JDBC activities.  Here we do the same: one pool, shared via the module-
    # level _db_pool variable in activities.py.
    log.info("Initialising DB connection pool → %s", DB_DSN.split("@")[-1])
    try:
        await init_db_pool(DB_DSN)
        log.info("DB pool ready.")
    except Exception as exc:
        log.warning("DB pool unavailable (%s) — lookup_database / update_pull_count activities will fail at runtime.", exc)

    # ── 2. Connect to Temporal ───────────────────────────────────────────────
    client = await Client.connect(TEMPORAL_HOST)

    # ── 3. Run the worker ────────────────────────────────────────────────────
    # Single task queue handles both BW services.  In production you might
    # split these onto separate queues / workers for independent scaling.
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            CreditApplicationWorkflow,   # was: CreditAppService MainProcess.bwp
            CreditCheckWorkflow,         # was: CreditCheckBackendService Process.bwp
        ],
        activities=[
            get_equifax_score,           # was: EquifaxScore.bwp sub-process
            get_experian_score,          # was: ExperianScore.bwp sub-process
            lookup_database,             # was: LookupDatabase.bwp JDBC Query
            update_pull_count,           # was: LookupDatabase.bwp JDBC Update
        ],
    )

    log.info("Worker started on task queue: %s", TASK_QUEUE)
    try:
        await worker.run()
    finally:
        await close_db_pool()


# ---------------------------------------------------------------------------
# HTTP Trigger stubs (FastAPI)
#
# In BW both services used an "HTTP Receiver" Process Starter — an embedded
# HTTP server that accepted incoming requests and started process instances.
#
# In Temporal, the workflow is started by external code (a FastAPI handler,
# a queue consumer, a CLI script, etc.).  Below are minimal stubs showing
# how the BW HTTP Receivers translate to FastAPI routes.
#
# Run alongside the worker process, or as a separate service.
# ---------------------------------------------------------------------------

# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
#
# app = FastAPI()
# _temporal_client: Client | None = None
#
#
# @app.on_event("startup")
# async def startup() -> None:
#     global _temporal_client
#     _temporal_client = await Client.connect(TEMPORAL_HOST)
#
#
# class CreditDetailsRequest(BaseModel):
#     """Replaces BW HTTP Receiver input schema GiveNewSchemaNameHere."""
#     ssn: str
#     first_name: str
#     last_name: str
#     dob: str
#
#
# @app.post("/creditdetails")
# async def post_credit_details(req: CreditDetailsRequest):
#     """
#     Replaces: CreditAppService HTTP Receiver Starter (POST /creditdetails).
#     BW started MainProcess.bwp synchronously and returned the merged result.
#     """
#     handle = await _temporal_client.start_workflow(
#         CreditApplicationWorkflow.run,
#         Applicant(ssn=req.ssn, first_name=req.first_name, last_name=req.last_name, dob=req.dob),
#         id=f"credit-app-{req.ssn}",
#         task_queue=TASK_QUEUE,
#     )
#     result = await handle.result()
#     return {"equifax": dataclasses.asdict(result.equifax), "experian": dataclasses.asdict(result.experian)}
#
#
# class CreditScoreRequest(BaseModel):
#     """Replaces BW HTTP Receiver input schema for creditcheckservice."""
#     ssn: str
#
#
# @app.post("/creditscore")
# async def post_credit_score(req: CreditScoreRequest):
#     """
#     Replaces: CreditCheckBackendService HTTP Receiver Starter (POST /creditscore).
#     BW called LookupDatabase sub-process and replied synchronously.
#     On any fault BW returned HTTP 404.
#     """
#     handle = await _temporal_client.start_workflow(
#         CreditCheckWorkflow.run,
#         req.ssn,
#         id=f"credit-check-{req.ssn}",
#         task_queue=TASK_QUEUE,
#     )
#     try:
#         record = await handle.result()
#         return dataclasses.asdict(record)
#     except Exception:
#         raise HTTPException(status_code=404, detail="Credit record not found")


if __name__ == "__main__":
    asyncio.run(main())
