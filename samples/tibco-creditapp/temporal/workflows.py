"""
workflows.py — Temporal Workflow definitions for the TIBCO BW CreditApp migration sample.

Source: TIBCOSoftware/bw-samples TN2018/Apps
  - CreditAppService/CreditApp.module/Processes/creditapp/module/MainProcess.bwp
  - CreditCheckBackendService/CreditCheckService/Processes/creditcheckservice/Process.bwp

Skill references used:
  - references/core/from-tibco-bw.md  (Process Starters, Groups, Error Handling)
  - references/python/examples.md     (asyncio.gather fan-out, try/except fault handler)

BW → Temporal Workflow mapping:

  MainProcess.bwp
    HTTP Receiver Starter (POST /creditdetails) → external FastAPI route (see worker.py)
    Parallel bpws:flow (EquifaxScore ∥ ExperianScore callprocess)  → asyncio.gather()
    FICOScoreTopostOut + ExperianScoreTopostOut merge             → tuple unpacking

  Process.bwp (creditcheckservice)
    HTTP Receiver Starter (POST /creditscore)    → external FastAPI route (see worker.py)
    LookupDatabase CallProcess                   → execute_activity(lookup_database, ...)
    UpdatePulls step (in LookupDatabase.bwp)     → execute_activity(update_pull_count, ...)
    bw.generalactivities.log ("Invocation Successful")  → workflow.logger.info(...)
    bpws:catchAll fault handler
      → bw.generalactivities.log ("Invocation Failed")  → workflow.logger.error(...)
      → bpws:reply with HTTP 404                         → re-raise (caller handles HTTP code)

Dropped BW constructs:
  - OnMessageStart / OnMessageEnd markers  — BW internal lifecycle hooks; no equivalent needed.
  - bpws:pick                              — Replaced by the workflow's run() entry point.
  - Transition links (FICOScoreTopostOut etc.) — Pure sequencing; expressed as Python code flow.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from .activities import (
        Applicant,
        CreditRecord,
        CreditScore,
        UpdatePullCountInput,
        get_equifax_score,
        get_experian_score,
        lookup_database,
        update_pull_count,
    )


# ---------------------------------------------------------------------------
# Shared retry / timeout defaults
# Mirrors BW's per-activity timeout (30 s) and max retry (3).
# ---------------------------------------------------------------------------

_ACTIVITY_OPTS = dict(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(maximum_attempts=3),
)


# ---------------------------------------------------------------------------
# CreditApplicationWorkflow
# Replaces: MainProcess.bwp  +  EquifaxScore.bwp  +  ExperianScore.bwp
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class CreditApplicationResult:
    """
    Replaces BW schema CreditScoreSuccessSchema:
      EquifaxResponse  { FICOScore, NoOfInquiries, Rating }
      ExperianResponse { FICOScore, NoOfInquiries, Rating }
    """
    equifax: CreditScore
    experian: CreditScore


@workflow.defn
class CreditApplicationWorkflow:
    """
    MainProcess.bwp received an HTTP POST /creditdetails and used a parallel bpws:flow
    to invoke EquifaxScore and ExperianScore as sub-processes simultaneously.  Both
    sub-processes used RenderJSON → HTTP POST → ParseJSON internally, then their
    outputs merged at the postOut reply node via FICOScoreTopostOut and
    ExperianScoreTopostOut transition links.

    In Temporal:
      - asyncio.gather() replaces the parallel bpws:flow — both HTTP activities run
        concurrently on the same worker.
      - The sub-process call overhead is eliminated; each sub-process becomes a
        directly-invoked Activity function.
      - Workflow determinism is preserved: asyncio.gather is deterministic here
        because both branches are activities (their I/O is recorded in event history).
    """

    @workflow.run
    async def run(self, applicant: Applicant) -> CreditApplicationResult:
        # Fan-out: replaces BW's parallel bpws:flow containing two bpws:extensionActivity
        # nodes (EquifaxScore and ExperianScore) with no ordering links between them.
        equifax_score, experian_score = await asyncio.gather(
            workflow.execute_activity(get_equifax_score, applicant, **_ACTIVITY_OPTS),
            workflow.execute_activity(get_experian_score, applicant, **_ACTIVITY_OPTS),
        )

        # Fan-in: replaces the FICOScoreTopostOut + ExperianScoreTopostOut merge at postOut.
        return CreditApplicationResult(equifax=equifax_score, experian=experian_score)


# ---------------------------------------------------------------------------
# CreditCheckWorkflow
# Replaces: Process.bwp  +  LookupDatabase.bwp  (creditcheckservice)
# ---------------------------------------------------------------------------

@workflow.defn
class CreditCheckWorkflow:
    """
    Process.bwp received an HTTP POST /creditscore and invoked the LookupDatabase
    sub-process.  LookupDatabase performed a JDBC SELECT; if the record was found it
    did a JDBC UPDATE, otherwise it threw a DefaultFault.  Back in Process.bwp, a
    bpws:catchAll fault handler logged "Invocation Failed" and replied with HTTP 404.

    In Temporal:
      - The LookupDatabase sub-process is split across two Activities (lookup_database
        and update_pull_count) — the JDBC SELECT and UPDATE are independent I/O
        operations and benefit from independent retry policies.
      - ApplicationError(non_retryable=True) in lookup_database replaces bpws:throw
        DefaultFault.  non_retryable prevents retrying a "not found" condition.
      - try/except replaces the bpws:catchAll fault handler.
      - workflow.logger replaces bw.generalactivities.log (no separate activity needed).
      - The HTTP 404 reply is handled by the FastAPI route that started this workflow
        (see worker.py); the workflow re-raises so the caller can map exception → HTTP code.
    """

    @workflow.run
    async def run(self, ssn: str) -> CreditRecord:
        # look_up_no_retry: the "not found" ApplicationError must not be retried.
        # Other transient errors (DB connectivity) use the default RetryPolicy.
        lookup_opts = dict(
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        update_opts = dict(
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        try:
            # Replaces: LookupDatabase CallProcess → QueryRecords (bw.jdbc.JDBCQuery)
            # Raises ApplicationError(non_retryable=True) if not found (was: bpws:throw DefaultFault)
            record = await workflow.execute_activity(lookup_database, ssn, **lookup_opts)

            # Replaces: UpdatePulls (bw.jdbc.update) — only reached when record was found.
            # BW expressed this as a conditional transition: JDBCQueryToEnd (when rating found)
            # → UpdatePulls → End.  Here it's a sequential await after a successful lookup.
            await workflow.execute_activity(
                update_pull_count,
                UpdatePullCountInput(ssn=record.ssn, current_pulls=record.num_inquiries),
                **update_opts,
            )

            # Replaces: LogSuccess (bw.generalactivities.log, message="Invocation Successful")
            # workflow.logger writes to the Temporal worker log; no separate activity needed.
            workflow.logger.info("Credit check successful for SSN ending ...%s", ssn[-4:])

            return record

        except ApplicationError:
            # Replaces: bpws:catchAll → LogFailure (bw.generalactivities.log) + Reply 404
            workflow.logger.error("Credit check failed for SSN ending ...%s", ssn[-4:])
            raise  # caller (FastAPI trigger) maps ApplicationError → HTTP 404
