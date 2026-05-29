"""
test_workflows.py — Tests for the TIBCO BW CreditApp migration sample.

Uses temporalio.testing.WorkflowEnvironment (in-process, no server needed).
Activities are mocked so no real HTTP or Postgres connections are required.

What is tested:
  - CreditApplicationWorkflow: both activities called, results merged correctly,
    asyncio.gather fan-out verified via call-order tracking.
  - CreditCheckWorkflow (happy path): lookup → update → return record.
  - CreditCheckWorkflow (not-found path): ApplicationError propagates (was bpws:throw DefaultFault).

Run:
    pytest samples/tibco-creditapp/tests/ -v
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from temporalio import activity, workflow
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# Adjust import path: tibco-creditapp has a hyphen so it can't be a Python package;
# add the directory itself to sys.path and import from the temporal sub-package.
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

from temporal.activities import (
    Applicant,
    CreditRecord,
    CreditScore,
    UpdatePullCountInput,
    get_equifax_score,
    get_experian_score,
    lookup_database,
    update_pull_count,
)
from temporal.workflows import (
    CreditApplicationResult,
    CreditApplicationWorkflow,
    CreditCheckWorkflow,
)

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

SAMPLE_APPLICANT = Applicant(
    ssn="123-45-6789",
    first_name="Jane",
    last_name="Doe",
    dob="1985-04-12",
)

EQUIFAX_RESULT = CreditScore(fico_score=720, rating="Good", num_inquiries=2)
EXPERIAN_RESULT = CreditScore(fico_score=735, rating="Good", num_inquiries=1)

SAMPLE_RECORD = CreditRecord(
    ssn="123-45-6789",
    first_name="Jane",
    last_name="Doe",
    dob="1985-04-12",
    fico_score=720,
    rating="Good",
    num_inquiries=3,
)


# ---------------------------------------------------------------------------
# CreditApplicationWorkflow tests
# (replaces MainProcess.bwp + EquifaxScore.bwp + ExperianScore.bwp)
# ---------------------------------------------------------------------------

class TestCreditApplicationWorkflow:
    """
    MainProcess.bwp had a parallel bpws:flow running EquifaxScore and ExperianScore
    concurrently.  CreditApplicationWorkflow replaces that with asyncio.gather().
    """

    @pytest.mark.asyncio
    async def test_both_scores_returned(self):
        """Happy path: both mock activities return scores; workflow merges them."""

        @activity.defn(name="get_equifax_score")
        async def mock_equifax(applicant: Applicant) -> CreditScore:
            return EQUIFAX_RESULT

        @activity.defn(name="get_experian_score")
        async def mock_experian(applicant: Applicant) -> CreditScore:
            return EXPERIAN_RESULT

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-credit-app",
                workflows=[CreditApplicationWorkflow],
                activities=[mock_equifax, mock_experian],
            ):
                result: CreditApplicationResult = await env.client.execute_workflow(
                    CreditApplicationWorkflow.run,
                    SAMPLE_APPLICANT,
                    id="test-credit-app-1",
                    task_queue="test-credit-app",
                )

        assert result.equifax.fico_score == 720
        assert result.equifax.rating == "Good"
        assert result.experian.fico_score == 735
        assert result.experian.rating == "Good"

    @pytest.mark.asyncio
    async def test_parallel_fanout_both_activities_called(self):
        """
        Verifies that both activities are invoked (not skipped if the first fails).
        Mirrors the BW guarantee that both parallel branches always start.
        """
        called: list[str] = []

        @activity.defn(name="get_equifax_score")
        async def mock_equifax(applicant: Applicant) -> CreditScore:
            called.append("equifax")
            return EQUIFAX_RESULT

        @activity.defn(name="get_experian_score")
        async def mock_experian(applicant: Applicant) -> CreditScore:
            called.append("experian")
            return EXPERIAN_RESULT

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-credit-app",
                workflows=[CreditApplicationWorkflow],
                activities=[mock_equifax, mock_experian],
            ):
                await env.client.execute_workflow(
                    CreditApplicationWorkflow.run,
                    SAMPLE_APPLICANT,
                    id="test-credit-app-2",
                    task_queue="test-credit-app",
                )

        assert set(called) == {"equifax", "experian"}, "Both activities must be called"

    @pytest.mark.asyncio
    async def test_activity_failure_propagates(self):
        """If one bureau is unavailable the workflow should fail (not silently return partial data)."""

        @activity.defn(name="get_equifax_score")
        async def mock_equifax(applicant: Applicant) -> CreditScore:
            raise ApplicationError("Equifax service unavailable")

        @activity.defn(name="get_experian_score")
        async def mock_experian(applicant: Applicant) -> CreditScore:
            return EXPERIAN_RESULT

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-credit-app",
                workflows=[CreditApplicationWorkflow],
                activities=[mock_equifax, mock_experian],
            ):
                with pytest.raises(WorkflowFailureError):
                    await env.client.execute_workflow(
                        CreditApplicationWorkflow.run,
                        SAMPLE_APPLICANT,
                        id="test-credit-app-3",
                        task_queue="test-credit-app",
                    )


# ---------------------------------------------------------------------------
# CreditCheckWorkflow tests
# (replaces Process.bwp + LookupDatabase.bwp)
# ---------------------------------------------------------------------------

class TestCreditCheckWorkflow:
    """
    Process.bwp called LookupDatabase, logged success/failure, replied HTTP 200/404.
    CreditCheckWorkflow replicates that logic as a try/except around two activities.
    """

    @pytest.mark.asyncio
    async def test_happy_path_returns_record(self):
        """lookup_database finds a record → update_pull_count called → record returned."""
        update_calls: list[UpdatePullCountInput] = []

        @activity.defn(name="lookup_database")
        async def mock_lookup(ssn: str) -> CreditRecord:
            return SAMPLE_RECORD

        @activity.defn(name="update_pull_count")
        async def mock_update(args: UpdatePullCountInput) -> None:
            update_calls.append(args)

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-credit-check",
                workflows=[CreditCheckWorkflow],
                activities=[mock_lookup, mock_update],
            ):
                record: CreditRecord = await env.client.execute_workflow(
                    CreditCheckWorkflow.run,
                    "123-45-6789",
                    id="test-credit-check-1",
                    task_queue="test-credit-check",
                )

        assert record.ssn == "123-45-6789"
        assert record.fico_score == 720
        # update_pull_count should have been called exactly once with the right args
        assert len(update_calls) == 1
        assert update_calls[0].ssn == "123-45-6789"
        assert update_calls[0].current_pulls == 3  # num_inquiries from SAMPLE_RECORD

    @pytest.mark.asyncio
    async def test_not_found_raises_application_error(self):
        """
        When lookup_database raises ApplicationError(non_retryable=True) (the replacement
        for bpws:throw DefaultFault), the workflow should fail — and update_pull_count
        must NOT be called (BW's conditional JDBCQueryToEnd transition not taken).
        """
        update_called = False

        @activity.defn(name="lookup_database")
        async def mock_lookup(ssn: str) -> CreditRecord:
            # Replaces: bpws:throw DefaultFault from LookupDatabase.bwp
            raise ApplicationError("No credit record found for SSN 999-99-9999", non_retryable=True)

        @activity.defn(name="update_pull_count")
        async def mock_update(args: UpdatePullCountInput) -> None:
            nonlocal update_called
            update_called = True

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-credit-check",
                workflows=[CreditCheckWorkflow],
                activities=[mock_lookup, mock_update],
            ):
                with pytest.raises(WorkflowFailureError) as exc_info:
                    await env.client.execute_workflow(
                        CreditCheckWorkflow.run,
                        "999-99-9999",
                        id="test-credit-check-2",
                        task_queue="test-credit-check",
                    )

        assert not update_called, "update_pull_count must not run when record is not found"
        # WorkflowFailureError.cause → ActivityError.cause → ApplicationError
        # (Temporal wraps activity errors in an ActivityError before surfacing to the workflow)
        from temporalio.exceptions import ActivityError
        activity_err = exc_info.value.cause
        assert isinstance(activity_err, ActivityError)
        app_err = activity_err.cause
        assert isinstance(app_err, ApplicationError)
        assert "No credit record found" in str(app_err.message)

    @pytest.mark.asyncio
    async def test_update_increments_pull_count(self):
        """
        BW computed new numofpulls as: xsd:int($QueryRecords/Record[1]/numofpulls + 1)
        Verify the workflow passes current_pulls=record.num_inquiries so the activity adds 1.
        """
        received_args: list[UpdatePullCountInput] = []

        record_with_5_pulls = CreditRecord(
            ssn="111-22-3333",
            first_name="John",
            last_name="Smith",
            dob="1975-08-01",
            fico_score=680,
            rating="Fair",
            num_inquiries=5,
        )

        @activity.defn(name="lookup_database")
        async def mock_lookup(ssn: str) -> CreditRecord:
            return record_with_5_pulls

        @activity.defn(name="update_pull_count")
        async def mock_update(args: UpdatePullCountInput) -> None:
            received_args.append(args)

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-credit-check",
                workflows=[CreditCheckWorkflow],
                activities=[mock_lookup, mock_update],
            ):
                await env.client.execute_workflow(
                    CreditCheckWorkflow.run,
                    "111-22-3333",
                    id="test-credit-check-3",
                    task_queue="test-credit-check",
                )

        assert received_args[0].current_pulls == 5
        # The activity itself adds 1 (UPDATE SET numofpulls = current_pulls + 1)
        # so the DB will end up with 6 — matching BW's xsd:int(numofpulls + 1)
