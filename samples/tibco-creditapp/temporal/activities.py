"""
activities.py — Temporal Activities for the TIBCO BW CreditApp migration sample.

Source: TIBCOSoftware/bw-samples TN2018/Apps
  - CreditAppService/CreditApp.module/Processes/creditapp/module/EquifaxScore.bwp
  - CreditAppService/CreditApp.module/Processes/creditapp/module/ExperianScore.bwp
  - CreditCheckBackendService/CreditCheckService/Processes/creditcheckservice/LookupDatabase.bwp

Skill references used:
  - references/core/from-tibco-bw.md  (Activity Types → Temporal Activities)
  - references/python/examples.md     (RetryPolicy, aiohttp pattern)

BW → Temporal Activity mapping:
  bw.http.sendHTTPRequest + RenderJSON + ParseJSON  →  get_equifax_score / get_experian_score
    (RenderJSON and ParseJSON were pure data-transformation steps with no I/O; merged into
     the HTTP activity. BW required them as separate palette items; Temporal doesn't.)

  bw.jdbc.JDBCQuery   →  lookup_database
  bw.jdbc.update      →  update_pull_count

  bw.generalactivities.log  →  NOT an activity in Temporal; use workflow.logger in the workflow.

Dropped BW constructs (not needed in Temporal):
  - Checkpoint activities          — Temporal event history provides durability automatically.
  - Shared Variables               — Data is passed as activity arguments and return values.
  - JDBC/HTTP Shared Resources     — Connection pool and HTTP session are created at worker
                                     startup (see worker.py) and shared via module-level state.
"""

from __future__ import annotations

import dataclasses
import os
from typing import Optional

import aiohttp
import asyncpg
from temporalio import activity
from temporalio.exceptions import ApplicationError


# ---------------------------------------------------------------------------
# Shared connection pool — replaces BW "Shared Resource" (JDBCConnectionResource)
# Initialized once at worker startup via init_db_pool(); never inside an activity.
# See: references/core/from-tibco-bw.md — "Shared Variables → Pass Data Explicitly"
# ---------------------------------------------------------------------------

_db_pool: Optional[asyncpg.Pool] = None


async def init_db_pool(dsn: str) -> None:
    """Call this from worker.py before starting the worker."""
    global _db_pool
    _db_pool = await asyncpg.create_pool(dsn)


async def close_db_pool() -> None:
    global _db_pool
    if _db_pool:
        await _db_pool.close()
        _db_pool = None


# ---------------------------------------------------------------------------
# Data classes — replace BW XML schema types
#
# BW used XSLT data-binding to map between XML schemas.  Here we use plain
# Python dataclasses; the Temporal SDK serialises them to/from JSON.
# ---------------------------------------------------------------------------

EQUIFAX_BASE_URL = os.environ.get("EQUIFAX_BASE_URL", "http://localhost:13080")   # BW: HttpClientResource2
EXPERIAN_BASE_URL = os.environ.get("EXPERIAN_BASE_URL", "https://integration.cloud.tibcoapps.com:443")  # BW: external Experian


@dataclasses.dataclass
class Applicant:
    """Input to CreditApplicationWorkflow — maps to BW schema GiveNewSchemaNameHere."""
    ssn: str
    first_name: str
    last_name: str
    dob: str  # date of birth as ISO string


@dataclasses.dataclass
class CreditScore:
    """Return type of get_equifax_score / get_experian_score."""
    fico_score: int
    rating: str
    num_inquiries: int


@dataclasses.dataclass
class UpdatePullCountInput:
    """Input for update_pull_count — groups the two SQL params into one serialisable arg."""
    ssn: str
    current_pulls: int


@dataclasses.dataclass
class CreditRecord:
    """Row returned from the creditscore table — output of lookup_database."""
    ssn: str
    first_name: str
    last_name: str
    dob: str
    fico_score: int
    rating: str
    num_inquiries: int


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

@activity.defn
async def get_equifax_score(applicant: Applicant) -> CreditScore:
    """
    Replaces: EquifaxScore.bwp (called as a sub-process from MainProcess.bwp)

    BW flow inside EquifaxScore.bwp:
      Start → RenderJSON (XSLT → JSON string) → bw.http.sendHTTPRequest (POST /creditscore)
            → ParseJSON (JSON string → schema) → End

    In Temporal: aiohttp handles serialisation/deserialisation directly; the
    RenderJSON and ParseJSON steps collapse into standard Python dict operations.
    The HTTP Client Resource (HttpClientResource2, localhost:13080) becomes the
    EQUIFAX_BASE_URL constant read at worker startup.
    """
    payload = {
        "SSN": applicant.ssn,
        "FirstName": applicant.first_name,
        "LastName": applicant.last_name,
        "DOB": applicant.dob,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{EQUIFAX_BASE_URL}/creditscore",
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

    # BW output schema SuccessSchema: FICOScore, NoOfInquiries, Rating
    return CreditScore(
        fico_score=int(data.get("FICOScore", 0)),
        rating=str(data.get("Rating", "")),
        num_inquiries=int(data.get("NoOfInquiries", 0)),
    )


@activity.defn
async def get_experian_score(applicant: Applicant) -> CreditScore:
    """
    Replaces: ExperianScore.bwp (called as a sub-process from MainProcess.bwp)

    Same pattern as get_equifax_score but targets the Experian endpoint.
    Note: the Experian response schema uses camelCase (fiCOScore, rating,
    noOfInquiries) where Equifax used PascalCase — kept here to match BW.
    """
    payload = {
        "SSN": applicant.ssn,
        "FirstName": applicant.first_name,
        "LastName": applicant.last_name,
        "DOB": applicant.dob,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{EXPERIAN_BASE_URL}/creditscore",
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

    # BW output schema ExperianResponseSchemaElement: fiCOScore, rating, noOfInquiries
    return CreditScore(
        fico_score=int(data.get("fiCOScore", 0)),
        rating=str(data.get("rating", "")),
        num_inquiries=int(data.get("noOfInquiries", 0)),
    )


@activity.defn
async def lookup_database(ssn: str) -> CreditRecord:
    """
    Replaces: LookupDatabase.bwp — JDBC Query section

    BW flow:
      Start → QueryRecords (bw.jdbc.JDBCQuery, SELECT * FROM public.creditscore WHERE ssn LIKE ?)
            ─[rating not empty]→ UpdatePulls → End
            ─[otherwise]──────→ Throw DefaultFault

    This activity handles only the SELECT.  The conditional and the UPDATE are
    expressed in CreditCheckWorkflow as normal Python control flow (see workflows.py).

    Raises ApplicationError (non_retryable) when no record is found — this
    replaces the bpws:throw DefaultFault in LookupDatabase.bwp.  non_retryable=True
    prevents Temporal from retrying a "not found" condition.
    """
    if _db_pool is None:
        raise RuntimeError("DB pool not initialised — call init_db_pool() in worker.py")

    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT firstname, lastname, ssn, \"dateofBirth\", ficoscore, rating, numofpulls "
            "FROM public.creditscore WHERE ssn LIKE $1",
            ssn,
        )

    # Replicates BW transition condition: string-length($QueryRecords/Record[1]/rating) > 0
    if row is None or not row["rating"]:
        raise ApplicationError(
            f"No credit record found for SSN {ssn}",
            non_retryable=True,  # replaces bpws:throw DefaultFault
        )

    return CreditRecord(
        ssn=row["ssn"],
        first_name=row["firstname"],
        last_name=row["lastname"],
        dob=str(row["dateofBirth"]),
        fico_score=int(row["ficoscore"]),
        rating=str(row["rating"]),
        num_inquiries=int(row["numofpulls"]),
    )


@activity.defn
async def update_pull_count(args: UpdatePullCountInput) -> None:
    """
    Replaces: LookupDatabase.bwp — JDBC Update section

    BW activity: bw.jdbc.update
    SQL: UPDATE creditscore SET numofpulls = ? WHERE ssn LIKE ?
    BW computed new value as: xsd:int($QueryRecords/Record[1]/numofpulls + 1)
    Here the caller (workflow) reads current_pulls from the CreditRecord returned
    by lookup_database and passes it in; this activity adds 1 and writes.
    """
    if _db_pool is None:
        raise RuntimeError("DB pool not initialised — call init_db_pool() in worker.py")

    async with _db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE creditscore SET numofpulls = $1 WHERE ssn LIKE $2",
            args.current_pulls + 1,
            args.ssn,
        )
