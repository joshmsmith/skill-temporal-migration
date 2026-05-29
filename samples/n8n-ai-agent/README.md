# n8n AI Agent → Temporal: AI Agent Migration Sample

This sample demonstrates a complete migration from an n8n AI Agent workflow to a
Temporal Python worker, using the
[temporal-migration skill](../../SKILL.md).

**Source workflow**: [n8n #6270 "Build your first AI agent"](https://n8n.io/workflows/6270-build-your-first-ai-agent/) (Lucas Peyrin)
also covers: [n8n #1954 "AI agent chat"](https://n8n.io/workflows/1954-ai-agent-chat/) (n8n Team)

---

## n8n Nodes Migrated

| n8n Node | Type | Role |
|---|---|---|
| Chat Trigger | Trigger | Receives user messages; starts or re-triggers the workflow |
| AI Agent | Orchestrator | Internal ReAct loop; routes messages to LLM and tools |
| Google Gemini | LLM sub-node | Language model powering the agent's reasoning |
| Get Weather | HTTP Request tool sub-node | Fetches current weather from wttr.in |
| Get News | RSS Read tool sub-node | Fetches headlines from an RSS feed |
| Simple Memory (Window Buffer) | Memory sub-node | Stores the last N messages for conversation context |

---

## Complete n8n → Temporal Mapping

### Structural Mappings

| n8n Concept | Temporal Equivalent | Notes |
|---|---|---|
| Workflow graph | `@workflow.defn` class | Code is the process — no visual canvas |
| Workflow execution (per message) | Long-running `AiAgentWorkflow` (per session) | One workflow per conversation session, not per message |
| Chat Trigger node | `send_message` signal + FastAPI `POST /sessions/{id}/messages` | See `worker.py` |
| Session start | FastAPI `POST /sessions/{id}` → `client.start_workflow()` | See `worker.py` |
| Session close | `close_session` signal + FastAPI `DELETE /sessions/{id}` | No n8n equivalent — executions simply ended |
| n8n Platform / Cloud | Temporal Cluster + Worker process | `worker.py` |
| n8n Worker | Temporal Worker | Handles workflow + activity tasks |
| Execution log | Temporal Web UI event history | Every LLM call and tool call is a separate event |

### Node → Activity Mappings

| n8n Node | Temporal Activity | File | Notes |
|---|---|---|---|
| Google Gemini node (one LLM call) | `run_llm_step` | `activities.py` | Returns `LLMStepResult(text=...)` OR `LLMStepResult(tool_call=...)` |
| Get Weather HTTP Request tool | `get_weather` | `activities.py` | Dispatched by workflow when LLM requests it |
| Get News RSS Read tool | `get_news` | `activities.py` | Dispatched by workflow when LLM requests it |
| Simple Memory sub-node | _(dropped)_ | — | `self._messages` in workflow state; Temporal event history is the store |

### Memory Mapping

| n8n Concept | Temporal Equivalent | Notes |
|---|---|---|
| Simple Memory (Window Buffer Memory) | `self._messages: list[Message]` | Local workflow state |
| n8n memory store (external buffer) | Temporal event history | Durability is free — no Redis, no database |
| Memory window (last N messages) | Unlimited by default; trim `self._messages` if needed | Event history replay handles reconstruction on worker restart |

---

## Architecture

```
HTTP Client (browser / curl)
    │
    ├─ POST /sessions/{id}               ──► start AiAgentWorkflow
    ├─ POST /sessions/{id}/messages      ──► send_message signal
    ├─ GET  /sessions/{id}/response      ──► last_response query
    └─ DELETE /sessions/{id}             ──► close_session signal

AiAgentWorkflow (one per conversation session)
    │
    ├─ outer loop: wait_condition(inbox or closed)
    │
    └─ inner agent loop (per user message):
         │
         ├─ run_llm_step activity ──► Gemini API
         │     returns ToolCall("get_weather", args)
         │
         ├─ get_weather activity ──► wttr.in JSON API
         │     appends result to self._messages
         │
         ├─ run_llm_step activity ──► Gemini API (with tool result in history)
         │     returns text (final answer)
         │
         └─ store in self._last_response; signal caller
```

---

## Key Design Decisions

**Long-running session workflow instead of per-message executions**

In n8n, each chat message triggers a fresh workflow execution; conversation memory
is loaded from an external Window Buffer Memory node on every run.  In Temporal,
one `AiAgentWorkflow` runs for the entire session lifetime.  Each user message
arrives as a `send_message` signal, and `self._messages` accumulates in workflow
state — durable in the event history without any external store.  This is the
central architectural difference.

See [from-low-code.md](../../references/core/from-low-code.md) — *Wait Node →
`workflow.sleep()` or Signal handler* and *Simple Memory → workflow state*.

**Tool dispatch in the workflow, not inside `run_llm_step`**

n8n's AI Agent node is a black box — it calls the LLM, receives a tool request,
calls the tool, and loops, all invisibly within a single node execution.  Here,
`run_llm_step` only calls the LLM and returns a `ToolCall` descriptor (or a
final text reply).  The workflow explicitly dispatches `get_weather` or `get_news`
as a separate activity for each tool request.

This means every LLM call and every tool call is a **separate, auditable event**
in the Temporal event history.  Each has its own retry policy, its own timeout,
and its own execution record in the Web UI.

**`run_llm_step` is stateless and independently retryable**

The full `self._messages` list is passed to `run_llm_step` on every call.  The
activity carries no state.  If it fails and retries (e.g. Gemini rate-limit), the
workflow simply calls it again with the same inputs — safe and correct.

**`MAX_AGENT_ITERATIONS` guards against infinite loops**

n8n's AI Agent node has a built-in iteration limit (10 by default).  The explicit
`for _ in range(MAX_AGENT_ITERATIONS):` loop in the workflow provides the same
guard.  If exhausted, the workflow stores a graceful fallback message rather than
failing hard.

---

## Running Locally

```bash
# 1. Start Temporal dev server (skip if already running)
temporal server start-dev

# 2. Install dependencies
pip install temporalio aiohttp

# 3. Start the worker with your Gemini API key
#    Get a free key at: https://aistudio.google.com/app/apikey
cd samples/n8n-ai-agent
GEMINI_API_KEY=<your-key> python -m temporal.worker
```

### Option A — interactive demo script

```bash
# In a second terminal:
cd samples/n8n-ai-agent
python demo.py
```

The demo sends two messages ("What's the weather in Tokyo?" and "What's in the
tech news?"), polls for responses, and closes the session.

### Option B — Temporal CLI

```bash
# Start a session
temporal workflow start \
  --type AiAgentWorkflow \
  --task-queue n8n-ai-agent \
  --workflow-id my-chat-session \
  --input '{"session_id":"my-chat-session","system_prompt":"You are a helpful assistant with weather and news tools.","initial_message":null}'

# Send a message
temporal workflow signal \
  --workflow-id my-chat-session \
  --name send_message \
  --input '"What is the weather in London?"'

# Query the response
temporal workflow query \
  --workflow-id my-chat-session \
  --type last_response

# Close the session
temporal workflow signal \
  --workflow-id my-chat-session \
  --name close_session
```

### Option C — FastAPI HTTP endpoints

Install FastAPI and run alongside the worker:

```bash
pip install fastapi uvicorn
uvicorn temporal.worker:app --reload
```

Then use the auto-generated docs at http://localhost:8000/docs.

---

## Running the Tests

No Gemini API key needed — all activities are mocked.

```bash
cd samples/n8n-ai-agent
pip install temporalio pytest pytest-asyncio
pytest tests/ -v
```

**Tests covered:**

| Test | What it verifies |
|---|---|
| `test_weather_response_returned` | Weather tool turn: `run_llm_step` → `get_weather` → `run_llm_step` (final) |
| `test_tool_result_appended_to_history` | Tool result appears in history passed to second LLM call |
| `test_news_response_returned` | News tool turn: `run_llm_step` → `get_news` → `run_llm_step` (final) |
| `test_two_turns_accumulate_history` | Multi-turn: messages accumulate across signal-based turns |
| `test_last_response_query` | `last_response` query returns most recent assistant reply |
| `test_close_with_no_messages_completes_cleanly` | `close_session` before any messages — workflow exits normally |
| `test_fallback_after_max_iterations` | Infinite tool loop → fallback message, no crash |
| `test_unknown_tool_does_not_crash` | Unrecognised tool name → safe error string, agent continues |

---

## Observing in the Temporal Web UI

Open http://localhost:8233 while running the demo.  Find the workflow by its
session ID and open the Event History.  You will see:

- `ActivityTaskScheduled` + `ActivityTaskCompleted` for every `run_llm_step` call
- `ActivityTaskScheduled` + `ActivityTaskCompleted` for every `get_weather` or `get_news` call
- `WorkflowExecutionSignaled` for each `send_message` and `close_session` signal

This is the key difference from n8n: n8n's agent loop is invisible inside a
single node execution.  In Temporal, every step is a first-class event.

---

## Skill References Used

- [references/core/from-low-code.md](../../references/core/from-low-code.md) — primary n8n mapping guide
- [references/python/examples.md](../../references/python/examples.md) — signal pattern, wait_condition, RetryPolicy
- [references/core/mental-model.md](../../references/core/mental-model.md) — why conversation state lives in the workflow
