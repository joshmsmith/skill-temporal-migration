"""
test_workflows.py — Tests for the n8n AI Agent migration sample.

Uses temporalio.testing.WorkflowEnvironment (in-process, no Temporal server needed).
All activities are mocked — no real Gemini API key or network access required.

What is tested:
  - Single weather turn: user asks about weather → run_llm_step returns ToolCall
    → get_weather dispatched → run_llm_step returns final text → response stored.
  - Single news turn: user asks for news → run_llm_step returns ToolCall
    → get_news dispatched → run_llm_step returns final text.
  - Multi-turn conversation: messages accumulate in workflow state; each turn
    is independently handled via signal.
  - Session close: close_session signal causes the workflow to complete normally.
  - Max iterations guard: if run_llm_step never returns text, the workflow
    emits the fallback message rather than looping forever.
  - Unknown tool: run_llm_step requests a tool that doesn't exist; the workflow
    returns a safe error string and continues.

Run:
    pytest samples/n8n-ai-agent/tests/ -v
"""

from __future__ import annotations

import asyncio
import sys
import pathlib
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# n8n-ai-agent has a hyphen so it isn't a Python package; add the sample root.
sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

from temporal.activities import (
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
from temporal.workflows import AiAgentWorkflow, SessionInput, DEFAULT_SYSTEM_PROMPT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session(initial_message: str | None = None) -> SessionInput:
    return SessionInput(
        session_id="test-session",
        system_prompt=DEFAULT_SYSTEM_PROMPT,
        initial_message=initial_message,
    )


async def _start_worker_and_env(env, activities: list):
    """Return a started Worker context manager for the test task queue."""
    return Worker(
        env.client,
        task_queue="test-ai-agent",
        workflows=[AiAgentWorkflow],
        activities=activities,
    )


# ---------------------------------------------------------------------------
# Test: single weather turn
# ---------------------------------------------------------------------------

class TestWeatherTurn:
    """
    User asks about weather.
    run_llm_step (first call) → ToolCall(get_weather)
    get_weather              → WeatherResult
    run_llm_step (second call) → text "It is sunny in Paris."
    """

    @pytest.mark.asyncio
    async def test_weather_response_returned(self):
        llm_calls: list[LLMInput] = []

        @activity.defn(name="run_llm_step")
        async def mock_llm(inp: LLMInput) -> LLMStepResult:
            llm_calls.append(inp)
            if len(llm_calls) == 1:
                # First call: LLM requests the weather tool
                return LLMStepResult(tool_call=ToolCall(
                    name="get_weather",
                    args={"location": "Paris"},
                    call_id="get_weather",
                ))
            # Second call: LLM produces a final answer
            return LLMStepResult(text="It is sunny in Paris, 22°C.")

        @activity.defn(name="get_weather")
        async def mock_weather(location: str) -> WeatherResult:
            assert location == "Paris"
            return WeatherResult(
                location="Paris",
                description="Sunny",
                temp_c=22.0,
                feels_like_c=21.0,
                humidity_pct=45,
            )

        @activity.defn(name="get_news")
        async def mock_news(rss_url: str, count: int = 5) -> NewsResult:
            raise AssertionError("get_news should not be called in this test")

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-ai-agent",
                workflows=[AiAgentWorkflow],
                activities=[mock_llm, mock_weather, mock_news],
            ):
                handle = await env.client.start_workflow(
                    AiAgentWorkflow.run,
                    _make_session(initial_message="What's the weather in Paris?"),
                    id="test-weather-1",
                    task_queue="test-ai-agent",
                )
                # Close session so the workflow completes
                await handle.signal(AiAgentWorkflow.close_session)
                result = await handle.result()

        assert result == "It is sunny in Paris, 22°C."
        assert len(llm_calls) == 2, "run_llm_step should be called twice (tool + final)"

    @pytest.mark.asyncio
    async def test_tool_result_appended_to_history(self):
        """
        The tool result must appear in the message history that the LLM sees
        on the second call.  This verifies the workflow's message accumulation.
        """
        second_call_messages: list[Message] = []

        @activity.defn(name="run_llm_step")
        async def mock_llm(inp: LLMInput) -> LLMStepResult:
            if len(inp.messages) == 1:
                return LLMStepResult(tool_call=ToolCall(
                    name="get_weather", args={"location": "Berlin"}, call_id="get_weather"
                ))
            second_call_messages.extend(inp.messages)
            return LLMStepResult(text="Berlin is cloudy today.")

        @activity.defn(name="get_weather")
        async def mock_weather(location: str) -> WeatherResult:
            return WeatherResult("Berlin", "Cloudy", 12.0, 10.0, 80)

        @activity.defn(name="get_news")
        async def mock_news(rss_url: str, count: int = 5) -> NewsResult:
            raise AssertionError("get_news should not be called")

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-ai-agent",
                workflows=[AiAgentWorkflow],
                activities=[mock_llm, mock_weather, mock_news],
            ):
                handle = await env.client.start_workflow(
                    AiAgentWorkflow.run,
                    _make_session(initial_message="Weather in Berlin?"),
                    id="test-weather-2",
                    task_queue="test-ai-agent",
                )
                await handle.signal(AiAgentWorkflow.close_session)
                await handle.result()

        # History on second LLM call: [user msg, tool result]
        assert len(second_call_messages) == 2
        assert second_call_messages[0].role == "user"
        assert second_call_messages[1].role == "tool"
        assert "Berlin" in second_call_messages[1].content


# ---------------------------------------------------------------------------
# Test: single news turn
# ---------------------------------------------------------------------------

class TestNewsTurn:
    """
    User asks for news.
    run_llm_step → ToolCall(get_news) → get_news → run_llm_step → final text
    """

    @pytest.mark.asyncio
    async def test_news_response_returned(self):
        news_calls: list[tuple] = []

        @activity.defn(name="run_llm_step")
        async def mock_llm(inp: LLMInput) -> LLMStepResult:
            if len(inp.messages) == 1:
                return LLMStepResult(tool_call=ToolCall(
                    name="get_news",
                    args={"rss_url": "https://feeds.reuters.com/reuters/technologyNews", "count": 3},
                    call_id="get_news",
                ))
            return LLMStepResult(text="Here are the latest tech headlines.")

        @activity.defn(name="get_weather")
        async def mock_weather(location: str) -> WeatherResult:
            raise AssertionError("get_weather should not be called")

        @activity.defn(name="get_news")
        async def mock_news(rss_url: str, count: int = 5) -> NewsResult:
            news_calls.append((rss_url, count))
            return NewsResult(
                feed_title="Reuters Technology",
                headlines=["AI chip demand surges", "New model released", "Tech stocks up"],
            )

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-ai-agent",
                workflows=[AiAgentWorkflow],
                activities=[mock_llm, mock_weather, mock_news],
            ):
                handle = await env.client.start_workflow(
                    AiAgentWorkflow.run,
                    _make_session(initial_message="What's in the news?"),
                    id="test-news-1",
                    task_queue="test-ai-agent",
                )
                await handle.signal(AiAgentWorkflow.close_session)
                result = await handle.result()

        assert result == "Here are the latest tech headlines."
        assert len(news_calls) == 1
        assert news_calls[0] == ("https://feeds.reuters.com/reuters/technologyNews", 3)


# ---------------------------------------------------------------------------
# Test: multi-turn conversation
# ---------------------------------------------------------------------------

class TestMultiTurn:
    """
    Verify that multiple user messages (sent as signals) accumulate in history
    and each gets its own independent agent turn.

    This is the central value proposition vs n8n:
    n8n reloads state from the memory buffer on each execution;
    Temporal accumulates it in self._messages, replayed from event history.
    """

    @pytest.mark.asyncio
    async def test_two_turns_accumulate_history(self):
        turn_histories: list[int] = []

        @activity.defn(name="run_llm_step")
        async def mock_llm(inp: LLMInput) -> LLMStepResult:
            # Record how many messages are in history at each LLM call
            turn_histories.append(len(inp.messages))
            return LLMStepResult(text=f"Response to message {len(inp.messages)}.")

        @activity.defn(name="get_weather")
        async def mock_weather(location: str) -> WeatherResult:
            raise AssertionError("get_weather should not be called")

        @activity.defn(name="get_news")
        async def mock_news(rss_url: str, count: int = 5) -> NewsResult:
            raise AssertionError("get_news should not be called")

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-ai-agent",
                workflows=[AiAgentWorkflow],
                activities=[mock_llm, mock_weather, mock_news],
            ):
                handle = await env.client.start_workflow(
                    AiAgentWorkflow.run,
                    _make_session(),
                    id="test-multiturn-1",
                    task_queue="test-ai-agent",
                )

                # First turn
                await handle.signal(AiAgentWorkflow.send_message, "Hello")
                # Give the workflow time to process
                await asyncio.sleep(0.1)

                # Query message count after first turn
                count_after_first = await handle.query(AiAgentWorkflow.message_count)

                # Second turn
                await handle.signal(AiAgentWorkflow.send_message, "How are you?")
                await asyncio.sleep(0.1)

                await handle.signal(AiAgentWorkflow.close_session)
                await handle.result()

        # After first turn: 1 user msg + 1 model reply = 2 messages
        assert count_after_first == 2
        # LLM was called once per turn → twice total
        assert len(turn_histories) == 2
        # First call saw 1 message (the user's "Hello")
        assert turn_histories[0] == 1
        # Second call saw 3 messages (Hello + reply + "How are you?")
        assert turn_histories[1] == 3

    @pytest.mark.asyncio
    async def test_last_response_query(self):
        """last_response query returns the most recent assistant reply."""
        call_count = [0]

        @activity.defn(name="run_llm_step")
        async def mock_llm(inp: LLMInput) -> LLMStepResult:
            call_count[0] += 1
            return LLMStepResult(text=f"Reply number {call_count[0]}")

        @activity.defn(name="get_weather")
        async def mock_weather(location: str) -> WeatherResult:
            raise AssertionError("get_weather should not be called")

        @activity.defn(name="get_news")
        async def mock_news(rss_url: str, count: int = 5) -> NewsResult:
            raise AssertionError("get_news should not be called")

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-ai-agent",
                workflows=[AiAgentWorkflow],
                activities=[mock_llm, mock_weather, mock_news],
            ):
                handle = await env.client.start_workflow(
                    AiAgentWorkflow.run,
                    _make_session(initial_message="First message"),
                    id="test-query-1",
                    task_queue="test-ai-agent",
                )
                await asyncio.sleep(0.1)
                response = await handle.query(AiAgentWorkflow.last_response)
                await handle.signal(AiAgentWorkflow.close_session)
                await handle.result()

        assert response == "Reply number 1"


# ---------------------------------------------------------------------------
# Test: session close
# ---------------------------------------------------------------------------

class TestSessionClose:
    """
    close_session signal causes the workflow to exit its outer loop and complete.
    """

    @pytest.mark.asyncio
    async def test_close_with_no_messages_completes_cleanly(self):
        @activity.defn(name="run_llm_step")
        async def mock_llm(inp: LLMInput) -> LLMStepResult:
            raise AssertionError("LLM should not be called if no messages sent")

        @activity.defn(name="get_weather")
        async def mock_weather(location: str) -> WeatherResult:
            raise AssertionError("get_weather should not be called")

        @activity.defn(name="get_news")
        async def mock_news(rss_url: str, count: int = 5) -> NewsResult:
            raise AssertionError("get_news should not be called")

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-ai-agent",
                workflows=[AiAgentWorkflow],
                activities=[mock_llm, mock_weather, mock_news],
            ):
                handle = await env.client.start_workflow(
                    AiAgentWorkflow.run,
                    _make_session(),
                    id="test-close-1",
                    task_queue="test-ai-agent",
                )
                await handle.signal(AiAgentWorkflow.close_session)
                result = await handle.result()

        # No messages were sent; last_response is None → workflow returns ""
        assert result == ""


# ---------------------------------------------------------------------------
# Test: max iterations guard
# ---------------------------------------------------------------------------

class TestMaxIterations:
    """
    If run_llm_step keeps returning ToolCalls and never produces a final text
    reply, the workflow should emit a fallback message after MAX_AGENT_ITERATIONS.
    """

    @pytest.mark.asyncio
    async def test_fallback_after_max_iterations(self):
        from temporal.workflows import MAX_AGENT_ITERATIONS

        llm_call_count = [0]

        @activity.defn(name="run_llm_step")
        async def mock_llm(inp: LLMInput) -> LLMStepResult:
            llm_call_count[0] += 1
            # Always return a tool call — never a final text
            return LLMStepResult(tool_call=ToolCall(
                name="get_weather", args={"location": "Nowhere"}, call_id="get_weather"
            ))

        @activity.defn(name="get_weather")
        async def mock_weather(location: str) -> WeatherResult:
            return WeatherResult("Nowhere", "Unknown", 0.0, 0.0, 0)

        @activity.defn(name="get_news")
        async def mock_news(rss_url: str, count: int = 5) -> NewsResult:
            raise AssertionError("get_news should not be called")

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-ai-agent",
                workflows=[AiAgentWorkflow],
                activities=[mock_llm, mock_weather, mock_news],
            ):
                handle = await env.client.start_workflow(
                    AiAgentWorkflow.run,
                    _make_session(initial_message="Loop forever"),
                    id="test-maxiter-1",
                    task_queue="test-ai-agent",
                )
                await handle.signal(AiAgentWorkflow.close_session)
                result = await handle.result()

        assert "sorry" in result.lower() or "wasn't able" in result.lower()
        assert llm_call_count[0] == MAX_AGENT_ITERATIONS


# ---------------------------------------------------------------------------
# Test: unknown tool graceful degradation
# ---------------------------------------------------------------------------

class TestUnknownTool:
    """
    If run_llm_step returns a ToolCall for a tool the workflow doesn't know,
    the workflow should append a safe error string and loop back to the LLM
    rather than crashing.
    """

    @pytest.mark.asyncio
    async def test_unknown_tool_does_not_crash(self):
        call_count = [0]

        @activity.defn(name="run_llm_step")
        async def mock_llm(inp: LLMInput) -> LLMStepResult:
            call_count[0] += 1
            if call_count[0] == 1:
                return LLMStepResult(tool_call=ToolCall(
                    name="send_email",  # not a registered tool
                    args={"to": "test@example.com", "body": "Hello"},
                    call_id="send_email",
                ))
            return LLMStepResult(text="I couldn't send that email, but here's my reply.")

        @activity.defn(name="get_weather")
        async def mock_weather(location: str) -> WeatherResult:
            raise AssertionError("get_weather should not be called")

        @activity.defn(name="get_news")
        async def mock_news(rss_url: str, count: int = 5) -> NewsResult:
            raise AssertionError("get_news should not be called")

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-ai-agent",
                workflows=[AiAgentWorkflow],
                activities=[mock_llm, mock_weather, mock_news],
            ):
                handle = await env.client.start_workflow(
                    AiAgentWorkflow.run,
                    _make_session(initial_message="Send an email to test@example.com"),
                    id="test-unknown-tool-1",
                    task_queue="test-ai-agent",
                )
                await handle.signal(AiAgentWorkflow.close_session)
                result = await handle.result()

        assert result == "I couldn't send that email, but here's my reply."
        # LLM was called twice: once got unknown tool, once got final text
        assert call_count[0] == 2
