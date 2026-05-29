"""
workflows.py — Temporal Workflow definitions for the n8n AI Agent migration sample.

Source: n8n workflow #6270 "Build your first AI agent" (lucaspeyrin)
        n8n workflow #1954 "AI agent chat" (n8n-team)
        https://n8n.io/workflows/6270-build-your-first-ai-agent/

Skill references used:
  - references/core/from-low-code.md  (Node-by-Node Migration, Wait Node → Signal)
  - references/python/examples.md     (signal pattern, wait_condition)

n8n → Temporal Workflow mapping:

  Chat Trigger node
    → AiAgentWorkflow.send_message signal
    n8n restarts a fresh execution for every chat message; the workflow
    ID doubles as session ID.  In Temporal we use a LONG-RUNNING WORKFLOW
    per session: each user message arrives as a signal, conversation history
    accumulates in self._messages, and the workflow only completes when the
    session is explicitly closed.  This is the key architectural difference:
    n8n requires an external memory store for continuity; Temporal's event
    history IS the memory store.

  AI Agent node (internal ReAct loop)
    → while True: loop inside AiAgentWorkflow.run()
    The n8n AI Agent node runs a hidden ReAct loop internally.  Here the loop
    is explicit in workflow code, making each step observable as an activity
    in the Temporal event history.

  Google Gemini node
    → workflow.execute_activity(run_llm_step, ...)
    One activity call per LLM invocation.  If the LLM returns a function call
    rather than a final answer, the workflow dispatches the appropriate tool
    activity and loops.

  Get Weather tool (HTTP Request sub-node)
    → workflow.execute_activity(get_weather, ...)
    Dispatched explicitly by the workflow when run_llm_step returns a ToolCall
    with name=="get_weather".  Independent activity = independent retry policy
    and independent event in history.

  Get News tool (RSS Read sub-node)
    → workflow.execute_activity(get_news, ...)
    Same pattern as get_weather.

  Simple Memory (Window Buffer Memory sub-node)
    → self._messages: list[Message]  (local workflow state)
    No external database, no Redis, no n8n memory node.  Temporal's event
    history replays self._messages from recorded activity results on worker
    restart — durability is free.

Architecture notes:
  - One workflow execution = one conversation session (keyed by session_id).
  - The outer loop blocks on workflow.wait_condition() until a new message
    arrives via send_message signal or until close_session signal fires.
  - The inner agent loop iterates run_llm_step → optional tool dispatch
    → run_llm_step until the LLM returns a text reply.
  - MAX_AGENT_ITERATIONS guards against runaway tool loops (replaces n8n's
    implicit 10-iteration limit on the AI Agent node).
"""

from __future__ import annotations

import dataclasses
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .activities import (
        LLMInput,
        LLMStepResult,
        Message,
        NewsResult,
        ToolCall,
        WeatherResult,
        get_news,
        get_weather,
        run_llm_step,
    )


# ---------------------------------------------------------------------------
# Shared activity options
# ---------------------------------------------------------------------------

# LLM calls can be slow; give them up to 60 s and only retry once on transient
# network errors (avoid double-billing on idempotent LLM calls).
_LLM_OPTS = dict(
    start_to_close_timeout=timedelta(seconds=60),
    retry_policy=RetryPolicy(maximum_attempts=2),
)

# Tool activities are fast HTTP calls; 15 s is generous, retry up to 3 times.
_TOOL_OPTS = dict(
    start_to_close_timeout=timedelta(seconds=15),
    retry_policy=RetryPolicy(maximum_attempts=3),
)

# Safety cap on agent iterations per user message to prevent infinite tool loops.
# n8n's AI Agent node has an internal limit of 10 iterations by default.
MAX_AGENT_ITERATIONS = 10

# Default system prompt, matching the n8n template's System Message field.
DEFAULT_SYSTEM_PROMPT = (
    "You are a helpful assistant. You have access to tools that let you check "
    "the weather and get news headlines. Use them when they are relevant to the "
    "user's question. Be concise and friendly."
)


# ---------------------------------------------------------------------------
# AiAgentWorkflow
#
# Replaces: n8n Chat Trigger + AI Agent node + Google Gemini node +
#           Get Weather tool + Get News tool + Simple Memory node
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class SessionInput:
    """
    Input provided when starting a new session via client.start_workflow().
    The first user message can optionally be included here to avoid a
    separate signal round-trip for the opening turn.
    """
    session_id: str
    system_prompt: str = DEFAULT_SYSTEM_PROMPT
    initial_message: Optional[str] = None


@workflow.defn
class AiAgentWorkflow:
    """
    Long-running conversation session workflow.

    Lifecycle:
      1. Started by FastAPI POST /sessions/{session_id}  (see worker.py)
         Optionally accepts an initial_message so the first turn can begin
         immediately without a separate signal.
      2. Each subsequent user message arrives via the send_message signal.
      3. For every message, the inner agent loop runs:
           a. Append user message to self._messages
           b. Call run_llm_step with the full history
           c. If LLM returns a ToolCall → dispatch get_weather or get_news
              activity, append tool result, loop back to (b)
           d. If LLM returns text → append to history, store as last response
      4. Workflow waits for the next message or a close_session signal.
      5. On close_session, the workflow completes normally.
    """

    def __init__(self) -> None:
        # Replaces: n8n Simple Memory (Window Buffer Memory) node.
        # Accumulates the full conversation; Temporal event history provides
        # durability — no external store needed.
        self._messages: list[Message] = []
        # Inbox: signals queue here while the agent loop is busy.
        self._inbox: list[str] = []
        # Most recent assistant response; accessible via last_response query.
        self._last_response: Optional[str] = None
        self._closed: bool = False
        self._system_prompt: str = DEFAULT_SYSTEM_PROMPT

    # ── Signals ─────────────────────────────────────────────────────────────

    @workflow.signal
    def send_message(self, text: str) -> None:
        """
        Deliver a user message to the workflow.

        n8n equivalent: Chat Trigger node fires when the user submits a message.
        In Temporal, the HTTP endpoint (worker.py) sends this signal; the
        workflow wakes from wait_condition, processes the message, then
        waits again.
        """
        self._inbox.append(text)

    @workflow.signal
    def close_session(self) -> None:
        """
        Terminate the conversation session gracefully.

        n8n equivalent: n8n has no explicit session-close — executions simply
        end.  Here the client signals when the user's session is done (e.g.
        browser tab closed, /sessions/{id} DELETE endpoint).
        """
        self._closed = True

    # ── Queries ─────────────────────────────────────────────────────────────

    @workflow.query
    def last_response(self) -> Optional[str]:
        """Return the most recent assistant reply (for polling clients)."""
        return self._last_response

    @workflow.query
    def message_count(self) -> int:
        """Return the number of messages in conversation history."""
        return len(self._messages)

    # ── Run ─────────────────────────────────────────────────────────────────

    @workflow.run
    async def run(self, inp: SessionInput) -> str:
        """
        Outer session loop — waits for messages and dispatches the agent loop.

        n8n equivalent:
          The Chat Trigger + AI Agent node combination.  n8n runs a fresh
          graph execution per message; here one workflow execution handles
          the entire session lifetime.
        """
        self._system_prompt = inp.system_prompt

        # If an initial message was provided at start time, seed the inbox so
        # the first iteration of the loop below processes it immediately.
        if inp.initial_message:
            self._inbox.append(inp.initial_message)

        while True:
            # Block until a message is queued or the session is closed.
            # Replaces: n8n Chat Trigger blocking for user input.
            await workflow.wait_condition(
                lambda: bool(self._inbox) or self._closed
            )

            if self._closed:
                break

            # Drain one message from the inbox and run the agent loop for it.
            user_text = self._inbox.pop(0)
            await self._run_agent_turn(user_text)

        return self._last_response or ""

    # ── Internal: agent loop ─────────────────────────────────────────────────

    async def _run_agent_turn(self, user_text: str) -> None:
        """
        Inner ReAct loop for a single user message.

        n8n equivalent: the hidden iteration loop inside the AI Agent node.
        n8n keeps this opaque; here every LLM call and every tool dispatch is
        a visible, auditable activity in the Temporal event history.

        Loop:
          1. Append user message to history
          2. Call run_llm_step
          3a. If ToolCall → dispatch get_weather or get_news, append result, goto 2
          3b. If text     → append reply, store as last_response, return
        """
        self._messages.append(Message(role="user", content=user_text))

        for _ in range(MAX_AGENT_ITERATIONS):
            step: LLMStepResult = await workflow.execute_activity(
                run_llm_step,
                LLMInput(
                    messages=list(self._messages),
                    system_prompt=self._system_prompt,
                ),
                **_LLM_OPTS,
            )

            if step.text is not None:
                # LLM produced a final reply — agent loop ends.
                self._messages.append(Message(role="model", content=step.text))
                self._last_response = step.text
                return

            if step.tool_call is not None:
                # LLM requested a tool — dispatch as a SEPARATE activity, then
                # append the result and loop back to run_llm_step.
                # This is the key difference vs n8n: each tool call is an
                # independent event in Temporal's history, independently
                # retryable with its own timeout and retry policy.
                tool_result_text = await self._dispatch_tool(step.tool_call)

                # Append the tool result as a "tool" message so the LLM sees it
                # on the next run_llm_step call.
                self._messages.append(Message(
                    role="tool",
                    content=tool_result_text,
                    tool_call_id=step.tool_call.call_id,
                ))
                # Loop: call run_llm_step again with the updated history.
                continue

        # Safety: if we exhausted iterations without a final answer, emit a
        # fallback message.  n8n raises an error; we degrade gracefully.
        fallback = "I'm sorry, I wasn't able to complete the request in time."
        self._messages.append(Message(role="model", content=fallback))
        self._last_response = fallback

    async def _dispatch_tool(self, tool_call: ToolCall) -> str:
        """
        Dispatch the appropriate tool activity and return its result as a string.

        n8n equivalent: the tool sub-node execution inside the AI Agent node.
        In n8n, each tool is a node wired to the agent — execution is implicit.
        In Temporal, we dispatch explicitly so each tool call has its own entry
        in the event history.
        """
        if tool_call.name == "get_weather":
            location = tool_call.args.get("location", "London")
            result: WeatherResult = await workflow.execute_activity(
                get_weather,
                location,
                **_TOOL_OPTS,
            )
            return (
                f"Weather in {result.location}: {result.description}, "
                f"{result.temp_c}°C (feels like {result.feels_like_c}°C), "
                f"humidity {result.humidity_pct}%."
            )

        if tool_call.name == "get_news":
            rss_url = tool_call.args.get("rss_url", "https://feeds.reuters.com/reuters/technologyNews")
            count = int(tool_call.args.get("count", 5))
            result: NewsResult = await workflow.execute_activity(
                get_news,
                args=[rss_url, count],
                **_TOOL_OPTS,
            )
            headlines = "\n".join(f"- {h}" for h in result.headlines)
            return f"Latest headlines from {result.feed_title}:\n{headlines}"

        # Unknown tool — return a safe error string rather than crashing.
        return f"Tool '{tool_call.name}' is not available."
