"""
activities.py — Temporal Activities for the n8n AI Agent migration sample.

Source: n8n workflow #6270 "Build your first AI agent" (lucaspeyrin)
        n8n workflow #1954 "AI agent chat" (n8n-team)
        https://n8n.io/workflows/6270-build-your-first-ai-agent/

Skill references used:
  - references/core/from-low-code.md  (Node-by-Node Migration, AI Agent pattern)
  - references/python/examples.md     (activity pattern, RetryPolicy)

n8n Node → Temporal Activity mapping:
  Google Gemini node (LLM)   →  run_llm_step
    The node received the full message list + tool schemas, called the LLM,
    and returned either a text reply OR a function-call request.  Here we do
    the same in a single activity, returning a discriminated LLMStepResult.

  Get Weather tool (HTTP Request node)  →  get_weather
    Fetches current weather for a location from wttr.in (JSON format).
    In n8n this was an HTTP Request node wired as an AI Tool sub-node of
    the AI Agent node.  In Temporal it is a plain @activity.defn — the
    workflow dispatches it explicitly when run_llm_step returns a tool call.

  Get News tool (RSS Read node)  →  get_news
    Fetches the top N headlines from an RSS feed URL.
    Same pattern as get_weather: separate activity, dispatched by the workflow.

Dropped n8n constructs:
  - Credentials node         — API keys come from environment variables.
  - Simple Memory sub-node   — Conversation history is self._messages in the
                               workflow (durable in Temporal event history;
                               no external store needed).
  - Chat Trigger node        — Replaced by a FastAPI POST endpoint (see worker.py)
                               that calls client.start_workflow() or
                               handle.signal(AiAgentWorkflow.send_message, ...).

Key design decisions:
  1. run_llm_step does NOT dispatch tools — it returns a ToolCall descriptor.
     The WORKFLOW reads the descriptor and executes the appropriate tool activity.
     Each tool invocation is therefore a separate, auditable event in the history.
  2. run_llm_step uses Gemini's function-calling API.  The tool schemas are
     passed in every call so the activity is stateless and independently retryable.
  3. get_weather and get_news are thin HTTP activities.  They do not call the LLM.
"""

from __future__ import annotations

import dataclasses
import os
import xml.etree.ElementTree as ET
from typing import Optional

import aiohttp
from temporalio import activity


# ---------------------------------------------------------------------------
# Configuration — read from environment; no secrets hard-coded.
# ---------------------------------------------------------------------------

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
# n8n "Get Weather" used wttr.in — we keep the same backing service.
WEATHER_BASE_URL = os.environ.get("WEATHER_BASE_URL", "https://wttr.in")
# n8n "Get News" used a configurable RSS URL; default to Reuters tech feed.
DEFAULT_NEWS_RSS_URL = os.environ.get(
    "NEWS_RSS_URL",
    "https://feeds.reuters.com/reuters/technologyNews",
)


# ---------------------------------------------------------------------------
# Data classes
#
# n8n passes data between nodes as JSON "items".  Here we use typed dataclasses;
# the Temporal SDK serialises them to/from JSON automatically.
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class Message:
    """
    A single message in the conversation history.
    Replaces n8n Simple Memory's window buffer entries.

    role: "user" | "model" | "tool"
    content: text content or tool result (serialised as a string for simplicity)
    tool_call_id: set when role=="tool" to link back to the LLM's function call
    """
    role: str
    content: str
    tool_call_id: Optional[str] = None


@dataclasses.dataclass
class ToolCall:
    """
    Returned by run_llm_step when the LLM requests a tool invocation.

    name: the tool function name, e.g. "get_weather" or "get_news"
    args: dict of arguments as returned by the LLM's function-calling response
    call_id: opaque ID from the Gemini response; echoed back in the tool result
    """
    name: str
    args: dict
    call_id: str


@dataclasses.dataclass
class LLMStepResult:
    """
    Discriminated union returned by run_llm_step:
      - text is set → LLM produced a final reply; agent loop ends.
      - tool_call is set → LLM wants to invoke a tool; workflow dispatches it.
    Exactly one of text / tool_call is non-None.
    """
    text: Optional[str] = None
    tool_call: Optional[ToolCall] = None


@dataclasses.dataclass
class LLMInput:
    """Input to run_llm_step — the full conversation history so far."""
    messages: list[Message]
    system_prompt: str


@dataclasses.dataclass
class WeatherResult:
    """Structured weather data returned by get_weather."""
    location: str
    description: str          # e.g. "Partly cloudy"
    temp_c: float
    feels_like_c: float
    humidity_pct: int


@dataclasses.dataclass
class NewsResult:
    """Top headlines returned by get_news."""
    feed_title: str
    headlines: list[str]      # trimmed to requested count


# ---------------------------------------------------------------------------
# Tool schema definitions
#
# Passed to Gemini in every run_llm_step call so the activity is stateless.
# Mirrors the two Tool sub-nodes wired into the n8n AI Agent node.
# ---------------------------------------------------------------------------

_TOOL_SCHEMAS = [
    {
        "name": "get_weather",
        "description": (
            "Get the current weather forecast for a city or location. "
            "Use this when the user asks about weather, temperature, or conditions."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": "City name or location, e.g. 'Paris' or 'New York'",
                }
            },
            "required": ["location"],
        },
    },
    {
        "name": "get_news",
        "description": (
            "Get the latest news headlines from an RSS feed. "
            "Use this when the user asks about news, current events, or recent updates."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rss_url": {
                    "type": "string",
                    "description": "RSS feed URL to fetch headlines from",
                },
                "count": {
                    "type": "integer",
                    "description": "Number of headlines to return (default 5)",
                },
            },
            "required": ["rss_url"],
        },
    },
]


# ---------------------------------------------------------------------------
# Activity: run_llm_step
#
# n8n equivalent: AI Agent node (ReAct loop step) + Google Gemini node
#
# In n8n the AI Agent node is a black-box loop that internally calls the LLM,
# receives a function call, calls the tool, and loops — all within one node
# execution.  Here we break that loop open: run_llm_step is ONE call to the
# LLM.  If it wants a tool, it returns a ToolCall; the workflow dispatches the
# tool as a separate activity, appends the result, and calls run_llm_step again.
# This makes each LLM call and each tool call independently visible in the
# Temporal event history.
# ---------------------------------------------------------------------------

@activity.defn
async def run_llm_step(inp: LLMInput) -> LLMStepResult:
    """
    Call the Gemini LLM with the current conversation history.
    Returns a final text reply OR a single ToolCall request.

    n8n equivalent:
      - Google Gemini node (model configuration)
      - AI Agent node (one iteration of its internal ReAct loop)
    """
    if not GEMINI_API_KEY:
        raise RuntimeError(
            "GEMINI_API_KEY environment variable is not set. "
            "Get a key at https://aistudio.google.com/app/apikey"
        )

    # Build the Gemini REST request payload.
    # Gemini multiTurn format: contents is a list of {role, parts} objects.
    contents = []
    for msg in inp.messages:
        if msg.role == "tool":
            # Tool results use the "function" role in Gemini v1beta
            contents.append({
                "role": "function",
                "parts": [{"functionResponse": {
                    "name": msg.tool_call_id or "tool",
                    "response": {"content": msg.content},
                }}],
            })
        else:
            contents.append({
                "role": msg.role,  # "user" or "model"
                "parts": [{"text": msg.content}],
            })

    payload = {
        "system_instruction": {"parts": [{"text": inp.system_prompt}]},
        "contents": contents,
        "tools": [{"function_declarations": _TOOL_SCHEMAS}],
        "tool_config": {"function_calling_config": {"mode": "AUTO"}},
    }

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()

    # Parse response: either a text part or a functionCall part.
    candidate = data["candidates"][0]
    parts = candidate["content"]["parts"]

    for part in parts:
        if "functionCall" in part:
            fc = part["functionCall"]
            return LLMStepResult(tool_call=ToolCall(
                name=fc["name"],
                args=fc.get("args", {}),
                call_id=fc["name"],  # Gemini v1beta doesn't return a call ID; use name
            ))
        if "text" in part:
            return LLMStepResult(text=part["text"])

    # Fallback: empty response treated as empty text
    return LLMStepResult(text="")


# ---------------------------------------------------------------------------
# Activity: get_weather
#
# n8n equivalent: "Get Weather" HTTP Request node (tool sub-node of AI Agent)
#
# The n8n node made a GET request to wttr.in/{location}?format=j1 and returned
# the raw JSON to the agent.  We do the same but parse into a typed struct.
# ---------------------------------------------------------------------------

@activity.defn
async def get_weather(location: str) -> WeatherResult:
    """
    Fetch current weather for a location from wttr.in.

    n8n equivalent:
      HTTP Request node (GET https://wttr.in/{location}?format=j1)
      wired as a Tool sub-node of the AI Agent node.
    """
    url = f"{WEATHER_BASE_URL}/{location}?format=j1"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

    current = data["current_condition"][0]
    nearest = data["nearest_area"][0]

    area_name = (
        nearest["areaName"][0]["value"]
        if nearest.get("areaName")
        else location
    )

    return WeatherResult(
        location=area_name,
        description=current["weatherDesc"][0]["value"],
        temp_c=float(current["temp_C"]),
        feels_like_c=float(current["FeelsLikeC"]),
        humidity_pct=int(current["humidity"]),
    )


# ---------------------------------------------------------------------------
# Activity: get_news
#
# n8n equivalent: "Get News" RSS Read node (tool sub-node of AI Agent)
#
# The n8n RSS Read node fetched an RSS feed and returned structured items to
# the agent.  We fetch the XML directly and parse out titles, keeping the
# same count-limiting behaviour.
# ---------------------------------------------------------------------------

@activity.defn
async def get_news(rss_url: str, count: int = 5) -> NewsResult:
    """
    Fetch the top N headlines from an RSS feed.

    n8n equivalent:
      RSS Read node (tool sub-node of the AI Agent node).
      n8n returned all items; we trim to `count` just like the agent's
      system message typically instructed.
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(rss_url) as resp:
            resp.raise_for_status()
            body = await resp.text()

    root = ET.fromstring(body)
    channel = root.find("channel")
    if channel is None:
        channel = root  # Atom feeds use the root directly

    # Extract feed title
    title_el = channel.find("title")
    feed_title = title_el.text if title_el is not None else rss_url

    # Extract item titles (RSS) or entry titles (Atom)
    headlines: list[str] = []
    for item in channel.findall("item"):
        t = item.find("title")
        if t is not None and t.text:
            headlines.append(t.text.strip())
        if len(headlines) >= count:
            break

    # Atom fallback
    if not headlines:
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall("atom:entry", ns):
            t = entry.find("atom:title", ns)
            if t is not None and t.text:
                headlines.append(t.text.strip())
            if len(headlines) >= count:
                break

    return NewsResult(feed_title=feed_title, headlines=headlines[:count])
