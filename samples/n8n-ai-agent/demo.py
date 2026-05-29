"""
demo.py — End-to-end demo for the n8n AI Agent → Temporal migration sample.

Simulates what a user does in n8n's Chat interface:
  1. Open a chat session (start_workflow)
  2. Send a message → get a reply
  3. Send a follow-up → get a reply
  4. Close the session

Usage (from the samples/n8n-ai-agent/ directory):
    # Terminal 1 — start the worker:
    GEMINI_API_KEY=<your-key> python -m temporal.worker

    # Terminal 2 — run this demo:
    python demo.py
"""

from __future__ import annotations

import asyncio
import sys
import time

from temporalio.client import Client

sys.path.insert(0, ".")
from temporal.workflows import AiAgentWorkflow, SessionInput, DEFAULT_SYSTEM_PROMPT


TEMPORAL_HOST = "localhost:7233"
TASK_QUEUE = "n8n-ai-agent"
SESSION_ID = f"demo-session-{int(time.time())}"

MESSAGES = [
    "What's the weather like in Tokyo right now?",
    "And what's in the tech news today?",
]


async def main() -> None:
    client = await Client.connect(TEMPORAL_HOST)

    print(f"\n▶  Starting session: {SESSION_ID}")
    handle = await client.start_workflow(
        AiAgentWorkflow.run,
        SessionInput(
            session_id=SESSION_ID,
            system_prompt=DEFAULT_SYSTEM_PROMPT,
        ),
        id=SESSION_ID,
        task_queue=TASK_QUEUE,
    )
    print(f"   Workflow started → {handle.id}\n")

    for msg in MESSAGES:
        print(f"👤 You:       {msg}")
        await handle.signal(AiAgentWorkflow.send_message, msg)

        # Poll until the workflow sets a new response.
        # In production use Server-Sent Events or a webhook instead.
        prev_response = None
        for _ in range(60):   # up to ~30 s
            await asyncio.sleep(0.5)
            response = await handle.query(AiAgentWorkflow.last_response)
            if response and response != prev_response:
                break
        else:
            print("🤖 Agent:     (timeout waiting for response)")
            continue

        prev_response = response
        print(f"🤖 Agent:     {response}\n")

    print("◼  Closing session.")
    await handle.signal(AiAgentWorkflow.close_session)
    final = await handle.result()
    print(f"\n   Session complete. Last response:\n   {final}")
    print(f"\n   View in Temporal UI → http://localhost:8233/namespaces/default/workflows/{SESSION_ID}")


if __name__ == "__main__":
    asyncio.run(main())
