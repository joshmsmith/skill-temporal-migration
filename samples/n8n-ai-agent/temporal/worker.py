"""
worker.py — Temporal Worker for the n8n AI Agent migration sample.

n8n equivalent infrastructure mapping:
  n8n Platform / Cloud           → Temporal Cluster + this Worker process
  n8n Worker                     → Temporal Worker (handles workflow tasks + activity tasks)
  n8n Chat Trigger node          → FastAPI POST /sessions          (start a new session)
                                   FastAPI POST /sessions/{id}/messages (send a message)
                                   FastAPI DELETE /sessions/{id}   (close session)
  n8n Execution log / UI         → Temporal Web UI event history

Run:
    GEMINI_API_KEY=<your-key> python -m temporal.worker
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from temporalio.client import Client
from temporalio.worker import Worker

from .activities import get_news, get_weather, run_llm_step
from .workflows import AiAgentWorkflow, SessionInput

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TEMPORAL_HOST = os.environ.get("TEMPORAL_HOST", "localhost:7233")
TASK_QUEUE = "n8n-ai-agent"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Worker entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    """
    Start the Temporal worker.

    Unlike the TIBCO sample there is no shared connection pool to initialise —
    activities create short-lived aiohttp sessions (get_weather, get_news) and
    the Gemini API is stateless (run_llm_step).
    """
    client = await Client.connect(TEMPORAL_HOST)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[AiAgentWorkflow],
        activities=[
            run_llm_step,   # was: AI Agent node + Google Gemini node (one LLM call)
            get_weather,    # was: Get Weather HTTP Request tool sub-node
            get_news,       # was: Get News RSS Read tool sub-node
        ],
    )

    log.info("Worker started on task queue: %s", TASK_QUEUE)
    await worker.run()


# ---------------------------------------------------------------------------
# HTTP trigger endpoints (FastAPI)
#
# n8n equivalent:
#   Chat Trigger node — embedded HTTP server that received chat messages and
#   started (or continued) a workflow execution.
#
# In Temporal, the workflow is started and signalled by external code.
# The FastAPI routes below mirror the three operations needed:
#   POST /sessions                  → start a new conversation session
#   POST /sessions/{id}/messages    → deliver a user message (signal)
#   DELETE /sessions/{id}           → close the session (signal)
#   GET  /sessions/{id}/response    → query the last assistant reply (poll)
#
# To run the FastAPI app alongside the worker:
#   uvicorn temporal.worker:app --reload
# ---------------------------------------------------------------------------

# Lazy import: FastAPI is an optional dependency for running the HTTP trigger.
# Tests and the worker itself do not require it.
try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel

    _temporal_client: Optional[Client] = None

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        global _temporal_client
        _temporal_client = await Client.connect(TEMPORAL_HOST)
        yield
        # No explicit close needed for the Temporal client

    app = FastAPI(
        title="n8n AI Agent → Temporal",
        description="HTTP trigger endpoints replacing n8n's Chat Trigger node.",
        lifespan=_lifespan,
    )

    class StartSessionRequest(BaseModel):
        """
        n8n equivalent: opening the Chat interface and sending the first message.
        system_prompt overrides the default personality / tool instructions.
        """
        system_prompt: str = ""
        initial_message: Optional[str] = None

    class SendMessageRequest(BaseModel):
        """n8n equivalent: user submitting a message in the Chat interface."""
        text: str

    @app.post("/sessions/{session_id}", status_code=201)
    async def start_session(session_id: str, body: StartSessionRequest):
        """
        Start a new conversation session.

        n8n equivalent:
          Opening the Chat interface creates a new Chat Trigger execution context.
          Here we start a long-running AiAgentWorkflow keyed by session_id.
        """
        from .workflows import DEFAULT_SYSTEM_PROMPT

        inp = SessionInput(
            session_id=session_id,
            system_prompt=body.system_prompt or DEFAULT_SYSTEM_PROMPT,
            initial_message=body.initial_message,
        )
        handle = await _temporal_client.start_workflow(
            AiAgentWorkflow.run,
            inp,
            id=session_id,
            task_queue=TASK_QUEUE,
        )
        return {"session_id": handle.id, "status": "started"}

    @app.post("/sessions/{session_id}/messages", status_code=202)
    async def send_message(session_id: str, body: SendMessageRequest):
        """
        Deliver a user message to an existing session.

        n8n equivalent:
          User types in the Chat interface; n8n re-triggers the workflow.
          Here we signal the long-running workflow, preserving conversation
          history in workflow state without any external memory store.
        """
        handle = _temporal_client.get_workflow_handle(session_id)
        await handle.signal(AiAgentWorkflow.send_message, body.text)
        return {"session_id": session_id, "status": "message_queued"}

    @app.delete("/sessions/{session_id}", status_code=200)
    async def close_session(session_id: str):
        """
        Close a conversation session gracefully.

        n8n equivalent: no explicit equivalent — n8n executions simply end.
        """
        handle = _temporal_client.get_workflow_handle(session_id)
        await handle.signal(AiAgentWorkflow.close_session)
        return {"session_id": session_id, "status": "closing"}

    @app.get("/sessions/{session_id}/response")
    async def get_last_response(session_id: str):
        """
        Poll for the most recent assistant reply.

        n8n equivalent: the Chat interface receives the response synchronously.
        This endpoint is for simple polling; production use should prefer
        Server-Sent Events or a WebSocket for real-time delivery.
        """
        handle = _temporal_client.get_workflow_handle(session_id)
        response = await handle.query(AiAgentWorkflow.last_response)
        if response is None:
            raise HTTPException(status_code=204, detail="No response yet")
        return {"session_id": session_id, "response": response}

except ImportError:
    # FastAPI not installed — worker still runs fine without HTTP trigger.
    app = None  # type: ignore[assignment]
    log.info("FastAPI not installed; HTTP trigger endpoints not available.")


if __name__ == "__main__":
    asyncio.run(main())
