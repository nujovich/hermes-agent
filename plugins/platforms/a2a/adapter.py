"""
A2A (Agent-to-Agent) platform adapter for Hermes Agent.

Implements the Google Agent-to-Agent protocol (https://google.github.io/A2A/),
exposing Hermes as an A2A-compatible agent server.

Endpoints:
  GET  /.well-known/agent.json   — Agent Card discovery
  POST /                         — JSON-RPC 2.0 task dispatcher
  GET  /health                   — health probe

Supported JSON-RPC methods:
  tasks/send            — submit a task and wait for the final result
  tasks/sendSubscribe   — submit a task and stream events via SSE
  tasks/get             — retrieve the current status of a task
  tasks/cancel          — request cancellation of a running task

No external dependencies beyond aiohttp (already used by the gateway).

Configuration via env vars or config.yaml::

    gateway:
      platforms:
        a2a:
          enabled: true
          extra:
            host: 127.0.0.1
            port: 10000
            api_key: ""          # optional Bearer token
            agent_name: "Hermes Agent"
            public_url: ""       # advertised in Agent Card
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket as _socket
import time
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from aiohttp import web
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    web = None  # type: ignore[assignment]
    aiohttp = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    is_network_accessible,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 10000
_MAX_REQUEST_BYTES = 4_000_000  # 4 MB

# A2A task state values (spec §3.1.4)
_STATE_SUBMITTED = "submitted"
_STATE_WORKING = "working"
_STATE_COMPLETED = "completed"
_STATE_FAILED = "failed"
_STATE_CANCELLED = "cancelled"
_STATE_INPUT_REQUIRED = "input-required"

# How long to keep completed task records in memory
_TASK_TTL_SECONDS = 3600


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _coerce_port(value: Any, default: int = DEFAULT_PORT) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
    return default


def _hermes_version() -> str:
    try:
        from importlib.metadata import version
        return version("hermes-agent")
    except Exception:
        pass
    try:
        from hermes_cli import __version__
        return __version__
    except Exception:
        return "dev"


def _json_error(request_id: Any, code: int, message: str, data: Any = None) -> Dict[str, Any]:
    """Build a JSON-RPC 2.0 error response object."""
    err: Dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": err,
    }


def _json_result(request_id: Any, result: Any) -> Dict[str, Any]:
    """Build a JSON-RPC 2.0 success response object."""
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": result,
    }


def _text_part(text: str) -> Dict[str, Any]:
    return {"type": "text", "text": text}


def _extract_text_from_message(message: Dict[str, Any]) -> str:
    """Pull all text parts from an A2A message into a single string."""
    parts = message.get("parts") or []
    chunks: List[str] = []
    for part in parts:
        if isinstance(part, dict):
            if part.get("type") == "text":
                t = part.get("text", "")
                if t:
                    chunks.append(t)
            elif part.get("type") == "data":
                # Embed structured data as JSON so the agent can reason over it
                payload = part.get("data")
                if payload is not None:
                    try:
                        chunks.append(json.dumps(payload, ensure_ascii=False))
                    except (TypeError, ValueError):
                        pass
    return "\n".join(chunks)


def _build_task_response(
    task_id: str,
    state: str,
    agent_message: Optional[str] = None,
    error_message: Optional[str] = None,
    history: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Construct an A2A Task object."""
    status: Dict[str, Any] = {"state": state, "timestamp": _iso_now()}
    if error_message:
        status["message"] = {
            "role": "agent",
            "parts": [_text_part(error_message)],
        }

    artifacts: List[Dict[str, Any]] = []
    if agent_message and state == _STATE_COMPLETED:
        artifacts.append({
            "name": "response",
            "parts": [_text_part(agent_message)],
            "index": 0,
            "lastChunk": True,
        })

    task: Dict[str, Any] = {
        "id": task_id,
        "status": status,
        "artifacts": artifacts,
    }
    if history is not None:
        task["history"] = history
    return task


def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# SSE helpers
# ---------------------------------------------------------------------------

def _sse_line(data: Any) -> bytes:
    """Encode a single SSE 'data:' frame."""
    return ("data: " + json.dumps(data, ensure_ascii=False) + "\n\n").encode()


def _task_status_event(task_id: str, state: str, message_text: Optional[str] = None, final: bool = False) -> Dict[str, Any]:
    ev: Dict[str, Any] = {
        "id": task_id,
        "status": {"state": state, "timestamp": _iso_now()},
        "final": final,
    }
    if message_text is not None:
        ev["status"]["message"] = {
            "role": "agent",
            "parts": [_text_part(message_text)],
        }
    return ev


def _artifact_event(task_id: str, chunk: str, index: int, last: bool) -> Dict[str, Any]:
    return {
        "id": task_id,
        "artifact": {
            "name": "response",
            "parts": [_text_part(chunk)],
            "index": index,
            "append": index > 0,
            "lastChunk": last,
        },
    }


# ---------------------------------------------------------------------------
# A2A Adapter
# ---------------------------------------------------------------------------

class A2AAdapter(BasePlatformAdapter):
    """
    HTTP server adapter that exposes Hermes as an A2A agent.

    Architecture
    ~~~~~~~~~~~~
    * ``connect()`` starts an aiohttp web server on the configured host/port.
    * Each incoming A2A task maps to a ``MessageEvent`` that is dispatched
      through ``handle_message()``, which runs the Hermes agent loop.
    * For ``tasks/send`` the adapter waits for the agent response by parking
      a per-task asyncio.Future in ``_pending`` and resolving it inside
      ``send()``.
    * For ``tasks/sendSubscribe`` the adapter pushes SSE frames as the agent
      calls back via a streaming queue.
    """

    MAX_MESSAGE_LENGTH = 0  # no truncation — tasks may be large

    def __init__(self, config: PlatformConfig) -> None:
        super().__init__(config, Platform("a2a"))
        extra = config.extra or {}

        self._host: str = str(
            os.environ.get("A2A_HOST") or extra.get("host") or DEFAULT_HOST
        )
        self._port: int = _coerce_port(
            os.environ.get("A2A_PORT") or extra.get("port"), DEFAULT_PORT
        )
        self._api_key: Optional[str] = (
            os.environ.get("A2A_API_KEY") or extra.get("api_key") or None
        )
        self._agent_name: str = str(
            os.environ.get("A2A_AGENT_NAME") or extra.get("agent_name") or "Hermes Agent"
        )
        self._agent_description: str = str(
            os.environ.get("A2A_AGENT_DESCRIPTION")
            or extra.get("agent_description")
            or "Self-improving open-source AI agent by Nous Research."
        )
        self._public_url: str = str(
            os.environ.get("A2A_PUBLIC_URL") or extra.get("public_url") or ""
        )

        # task_id -> asyncio.Future[str] for tasks/send
        self._pending: Dict[str, asyncio.Future] = {}
        # task_id -> asyncio.Queue for SSE streaming (tasks/sendSubscribe)
        self._streams: Dict[str, asyncio.Queue] = {}
        # task_id -> state str for tasks/get
        self._task_states: Dict[str, Dict[str, Any]] = {}
        # task_id -> cancel event
        self._cancel_events: Dict[str, asyncio.Event] = {}
        # task creation timestamps for TTL cleanup
        self._task_created: Dict[str, float] = {}

        self._app: Optional[Any] = None
        self._runner: Optional[Any] = None
        self._site: Optional[Any] = None
        self._sweep_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Agent Card
    # ------------------------------------------------------------------

    def _build_agent_card(self) -> Dict[str, Any]:
        base_url = self._public_url or f"http://{self._host}:{self._port}"
        version = _hermes_version()
        return {
            "name": self._agent_name,
            "description": self._agent_description,
            "url": base_url,
            "version": version,
            "capabilities": {
                "streaming": True,
                "pushNotifications": False,
                "stateTransitionHistory": False,
            },
            "skills": [
                {
                    "id": "general",
                    "name": "General Assistant",
                    "description": (
                        "Answers questions, executes tasks, writes code, browses the web, "
                        "manages files, and uses tools on behalf of the user."
                    ),
                    "tags": ["general", "coding", "research", "tools"],
                    "examples": [
                        "Summarize the latest news on AI.",
                        "Write a Python script to rename files.",
                        "Search the web for the best Python HTTP libraries.",
                    ],
                }
            ],
            "defaultInputModes": ["text/plain"],
            "defaultOutputModes": ["text/plain"],
        }

    # ------------------------------------------------------------------
    # Middleware / Auth
    # ------------------------------------------------------------------

    async def _auth_middleware(self, request: Any, handler: Any) -> Any:
        """Validate Bearer token if A2A_API_KEY is set."""
        # Health and Agent Card are always public
        if request.path in {"/.well-known/agent.json", "/health"}:
            return await handler(request)
        if self._api_key:
            auth = request.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                raise web.HTTPUnauthorized(
                    reason="Missing Authorization header",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            if auth[len("Bearer "):].strip() != self._api_key:
                raise web.HTTPUnauthorized(
                    reason="Invalid API key",
                    headers={"WWW-Authenticate": "Bearer"},
                )
        return await handler(request)

    # ------------------------------------------------------------------
    # HTTP handlers
    # ------------------------------------------------------------------

    async def _handle_agent_card(self, request: Any) -> Any:
        return web.json_response(self._build_agent_card())

    async def _handle_health(self, request: Any) -> Any:
        return web.json_response({"status": "ok", "platform": "a2a"})

    async def _handle_jsonrpc(self, request: Any) -> Any:
        """Main JSON-RPC 2.0 dispatcher for A2A task methods."""
        try:
            body = await request.read()
            if len(body) > _MAX_REQUEST_BYTES:
                return web.json_response(
                    _json_error(None, -32600, "Request too large"), status=413
                )
            payload = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return web.json_response(
                _json_error(None, -32700, f"Parse error: {exc}"), status=400
            )

        rpc_id = payload.get("id")
        method = payload.get("method", "")
        params = payload.get("params") or {}

        if payload.get("jsonrpc") != "2.0":
            return web.json_response(
                _json_error(rpc_id, -32600, "Invalid Request: jsonrpc must be '2.0'"),
                status=400,
            )

        if method == "tasks/send":
            return await self._rpc_tasks_send(rpc_id, params)
        elif method == "tasks/sendSubscribe":
            return await self._rpc_tasks_send_subscribe(request, rpc_id, params)
        elif method == "tasks/get":
            return await self._rpc_tasks_get(rpc_id, params)
        elif method == "tasks/cancel":
            return await self._rpc_tasks_cancel(rpc_id, params)
        else:
            return web.json_response(
                _json_error(rpc_id, -32601, f"Method not found: {method}"),
                status=404,
            )

    # ------------------------------------------------------------------
    # tasks/send — synchronous
    # ------------------------------------------------------------------

    async def _rpc_tasks_send(self, rpc_id: Any, params: Dict[str, Any]) -> Any:
        task_id = str(params.get("id") or uuid.uuid4())
        message = params.get("message")
        if not message:
            return web.json_response(
                _json_error(rpc_id, -32602, "Invalid params: 'message' is required"),
                status=400,
            )

        user_text = _extract_text_from_message(message)
        if not user_text.strip():
            return web.json_response(
                _json_error(rpc_id, -32602, "Invalid params: message has no text content"),
                status=400,
            )

        session_id = params.get("sessionId") or task_id

        # Register task
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()
        self._pending[task_id] = fut
        cancel_ev = asyncio.Event()
        self._cancel_events[task_id] = cancel_ev
        self._task_states[task_id] = {
            "state": _STATE_SUBMITTED,
            "session_id": session_id,
            "created": _iso_now(),
        }
        self._task_created[task_id] = time.time()

        try:
            # Dispatch to Hermes agent via handle_message
            source = self.build_source(
                chat_id=session_id,
                chat_name=f"A2A Task {task_id}",
                chat_type="dm",
                user_id=params.get("callerId") or "a2a-client",
                user_name=params.get("callerName") or "A2A Client",
            )
            event = MessageEvent(
                text=user_text,
                message_type=MessageType.TEXT,
                source=source,
                message_id=task_id,
            )
            self._task_states[task_id]["state"] = _STATE_WORKING
            await self.handle_message(event)

            # Wait for send() to resolve the future
            try:
                response_text = await asyncio.wait_for(fut, timeout=300.0)
            except asyncio.TimeoutError:
                self._task_states[task_id]["state"] = _STATE_FAILED
                task_obj = _build_task_response(
                    task_id, _STATE_FAILED,
                    error_message="Task timed out after 300 seconds.",
                )
                return web.json_response(_json_result(rpc_id, task_obj))

            if cancel_ev.is_set():
                self._task_states[task_id]["state"] = _STATE_CANCELLED
                task_obj = _build_task_response(task_id, _STATE_CANCELLED)
            else:
                self._task_states[task_id]["state"] = _STATE_COMPLETED
                task_obj = _build_task_response(task_id, _STATE_COMPLETED, agent_message=response_text)

            return web.json_response(_json_result(rpc_id, task_obj))

        except Exception as exc:
            logger.exception("[a2a] tasks/send error for task %s", task_id)
            self._task_states[task_id]["state"] = _STATE_FAILED
            task_obj = _build_task_response(task_id, _STATE_FAILED, error_message=str(exc))
            return web.json_response(_json_result(rpc_id, task_obj))
        finally:
            self._pending.pop(task_id, None)
            self._cancel_events.pop(task_id, None)

    # ------------------------------------------------------------------
    # tasks/sendSubscribe — SSE streaming
    # ------------------------------------------------------------------

    async def _rpc_tasks_send_subscribe(
        self, request: Any, rpc_id: Any, params: Dict[str, Any]
    ) -> Any:
        task_id = str(params.get("id") or uuid.uuid4())
        message = params.get("message")
        if not message:
            return web.json_response(
                _json_error(rpc_id, -32602, "Invalid params: 'message' is required"),
                status=400,
            )

        user_text = _extract_text_from_message(message)
        if not user_text.strip():
            return web.json_response(
                _json_error(rpc_id, -32602, "Invalid params: message has no text content"),
                status=400,
            )

        session_id = params.get("sessionId") or task_id

        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        self._streams[task_id] = queue
        cancel_ev = asyncio.Event()
        self._cancel_events[task_id] = cancel_ev
        self._task_states[task_id] = {
            "state": _STATE_SUBMITTED,
            "session_id": session_id,
            "created": _iso_now(),
        }
        self._task_created[task_id] = time.time()

        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            }
        )
        await response.prepare(request)

        # Send initial "submitted" event
        await response.write(
            _sse_line(_task_status_event(task_id, _STATE_SUBMITTED))
        )

        try:
            source = self.build_source(
                chat_id=session_id,
                chat_name=f"A2A Task {task_id}",
                chat_type="dm",
                user_id=params.get("callerId") or "a2a-client",
                user_name=params.get("callerName") or "A2A Client",
            )
            event = MessageEvent(
                text=user_text,
                message_type=MessageType.TEXT,
                source=source,
                message_id=task_id,
            )
            self._task_states[task_id]["state"] = _STATE_WORKING
            # Fire-and-forget: agent runs in background, pushing to queue
            asyncio.ensure_future(self._run_and_push(task_id, event, loop))

            # Send working event
            await response.write(_sse_line(_task_status_event(task_id, _STATE_WORKING)))

            # Drain queue until sentinel (None) or cancellation
            chunk_index = 0
            SENTINEL = object()
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    # Send keepalive comment
                    await response.write(b": keepalive\n\n")
                    if cancel_ev.is_set():
                        break
                    continue

                if item is SENTINEL or item is None:
                    break

                if isinstance(item, dict) and item.get("_type") == "chunk":
                    text_chunk = item.get("text", "")
                    last = item.get("last", False)
                    await response.write(
                        _sse_line(_artifact_event(task_id, text_chunk, chunk_index, last))
                    )
                    chunk_index += 1
                    if last:
                        break
                elif isinstance(item, dict) and item.get("_type") == "error":
                    error_text = item.get("text", "An error occurred.")
                    self._task_states[task_id]["state"] = _STATE_FAILED
                    await response.write(
                        _sse_line(_task_status_event(task_id, _STATE_FAILED, error_text, final=True))
                    )
                    return response

            # Final status event
            if cancel_ev.is_set():
                self._task_states[task_id]["state"] = _STATE_CANCELLED
                await response.write(
                    _sse_line(_task_status_event(task_id, _STATE_CANCELLED, final=True))
                )
            else:
                self._task_states[task_id]["state"] = _STATE_COMPLETED
                await response.write(
                    _sse_line(_task_status_event(task_id, _STATE_COMPLETED, final=True))
                )

        except Exception as exc:
            logger.exception("[a2a] sendSubscribe error for task %s", task_id)
            try:
                self._task_states[task_id]["state"] = _STATE_FAILED
                await response.write(
                    _sse_line(_task_status_event(task_id, _STATE_FAILED, str(exc), final=True))
                )
            except Exception:
                pass
        finally:
            self._streams.pop(task_id, None)
            self._cancel_events.pop(task_id, None)

        return response

    async def _run_and_push(
        self, task_id: str, event: MessageEvent, loop: asyncio.AbstractEventLoop
    ) -> None:
        """Dispatch a MessageEvent and forward agent response to the SSE queue."""
        queue = self._streams.get(task_id)
        if queue is None:
            return
        SENTINEL = None
        try:
            # Collect streaming chunks via a Future (same as tasks/send path)
            fut: asyncio.Future[str] = loop.create_future()
            self._pending[task_id] = fut

            await self.handle_message(event)

            try:
                response_text = await asyncio.wait_for(asyncio.shield(fut), timeout=300.0)
            except asyncio.TimeoutError:
                await queue.put({"_type": "error", "text": "Task timed out."})
                return

            # Push the full response as a single chunk (streaming delta support
            # requires gateway-level hooks not yet exposed to plugin adapters)
            await queue.put({"_type": "chunk", "text": response_text, "last": True})
        except Exception as exc:
            logger.exception("[a2a] _run_and_push error for task %s", task_id)
            await queue.put({"_type": "error", "text": str(exc)})
        finally:
            self._pending.pop(task_id, None)
            await queue.put(SENTINEL)

    # ------------------------------------------------------------------
    # tasks/get
    # ------------------------------------------------------------------

    async def _rpc_tasks_get(self, rpc_id: Any, params: Dict[str, Any]) -> Any:
        task_id = str(params.get("id") or "")
        if not task_id:
            return web.json_response(
                _json_error(rpc_id, -32602, "Invalid params: 'id' is required"),
                status=400,
            )

        state_info = self._task_states.get(task_id)
        if state_info is None:
            return web.json_response(
                _json_error(rpc_id, -32001, f"Task not found: {task_id}"),
                status=404,
            )

        task_obj = _build_task_response(task_id, state_info.get("state", _STATE_SUBMITTED))
        return web.json_response(_json_result(rpc_id, task_obj))

    # ------------------------------------------------------------------
    # tasks/cancel
    # ------------------------------------------------------------------

    async def _rpc_tasks_cancel(self, rpc_id: Any, params: Dict[str, Any]) -> Any:
        task_id = str(params.get("id") or "")
        if not task_id:
            return web.json_response(
                _json_error(rpc_id, -32602, "Invalid params: 'id' is required"),
                status=400,
            )

        state_info = self._task_states.get(task_id)
        if state_info is None:
            return web.json_response(
                _json_error(rpc_id, -32001, f"Task not found: {task_id}"),
                status=404,
            )

        cancel_ev = self._cancel_events.get(task_id)
        if cancel_ev:
            cancel_ev.set()

        # Resolve pending future with empty string so tasks/send returns quickly
        fut = self._pending.get(task_id)
        if fut and not fut.done():
            fut.get_loop().call_soon_threadsafe(fut.set_result, "")

        state_info["state"] = _STATE_CANCELLED
        task_obj = _build_task_response(task_id, _STATE_CANCELLED)
        return web.json_response(_json_result(rpc_id, task_obj))

    # ------------------------------------------------------------------
    # BasePlatformAdapter: send — called by gateway with agent response
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Resolve the pending Future / push to SSE queue for this task."""
        task_id = metadata.get("message_id") if metadata else None
        task_id = task_id or chat_id

        fut = self._pending.get(task_id)
        if fut and not fut.done():
            try:
                fut.get_loop().call_soon_threadsafe(fut.set_result, content)
                return SendResult(success=True, message_id=task_id)
            except Exception as exc:
                logger.warning("[a2a] send() future resolve error: %s", exc)

        # Fall through: SSE path uses the Future too; queue push handled in _run_and_push
        return SendResult(success=True, message_id=task_id or str(uuid.uuid4()))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        pass  # A2A has no typing indicator concept

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": f"A2A Task {chat_id}", "type": "dm", "chat_id": chat_id}

    # ------------------------------------------------------------------
    # Background sweep
    # ------------------------------------------------------------------

    async def _sweep_tasks(self) -> None:
        """Periodically remove completed task records past the TTL."""
        while True:
            await asyncio.sleep(120)
            now = time.time()
            expired = [
                tid
                for tid, created in list(self._task_created.items())
                if now - created > _TASK_TTL_SECONDS
                and self._task_states.get(tid, {}).get("state")
                in {_STATE_COMPLETED, _STATE_FAILED, _STATE_CANCELLED}
            ]
            for tid in expired:
                self._task_states.pop(tid, None)
                self._task_created.pop(tid, None)

    # ------------------------------------------------------------------
    # BasePlatformAdapter lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        if not AIOHTTP_AVAILABLE:
            logger.error("[a2a] aiohttp is required but not installed")
            return False

        # Port conflict detection
        try:
            with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
                s.settimeout(1)
                s.connect((self._host if self._host != "0.0.0.0" else "127.0.0.1", self._port))
            logger.error(
                "[a2a] Port %d is already in use. Set A2A_PORT to a different port.", self._port
            )
            return False
        except (ConnectionRefusedError, OSError):
            pass  # port free

        # Network accessibility + key check
        if is_network_accessible(self._host) and not self._api_key:
            logger.warning(
                "[a2a] A2A server bound to %s without an API key. "
                "Set A2A_API_KEY to restrict access.",
                self._host,
            )

        try:
            middlewares = [self._auth_middleware]
            self._app = web.Application(
                middlewares=middlewares, client_max_size=_MAX_REQUEST_BYTES
            )
            self._app.router.add_get("/.well-known/agent.json", self._handle_agent_card)
            self._app.router.add_get("/health", self._handle_health)
            self._app.router.add_post("/", self._handle_jsonrpc)

            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()

            self._sweep_task = asyncio.create_task(self._sweep_tasks())

            self._mark_connected()
            logger.info(
                "[a2a] A2A server listening on http://%s:%d "
                "(Agent Card: http://%s:%d/.well-known/agent.json)",
                self._host, self._port, self._host, self._port,
            )
            return True

        except Exception as exc:
            logger.error("[a2a] Failed to start A2A server: %s", exc)
            return False

    async def disconnect(self) -> None:
        self._mark_disconnected()
        if self._sweep_task and not self._sweep_task.done():
            self._sweep_task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(self._sweep_task), timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        if self._site:
            await self._site.stop()
            self._site = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        self._app = None
        logger.info("[a2a] A2A server stopped")


# ---------------------------------------------------------------------------
# Requirements check
# ---------------------------------------------------------------------------

def check_requirements() -> bool:
    return AIOHTTP_AVAILABLE


def validate_config(config: PlatformConfig) -> Optional[str]:
    extra = config.extra or {}
    port = _coerce_port(os.environ.get("A2A_PORT") or extra.get("port"), DEFAULT_PORT)
    if not (1 <= port <= 65535):
        return f"A2A port {port} is out of range (1–65535)"
    return None


def is_connected(config: PlatformConfig) -> bool:
    return bool(os.environ.get("A2A_HOST") or (config.extra or {}).get("host"))


def _env_enablement() -> Optional[Dict[str, Any]]:
    """Seed PlatformConfig.extra from env vars for gateway status display."""
    host = os.environ.get("A2A_HOST")
    port_raw = os.environ.get("A2A_PORT")
    if not host and not port_raw:
        return None
    extra: Dict[str, Any] = {}
    if host:
        extra["host"] = host
    if port_raw:
        extra["port"] = _coerce_port(port_raw, DEFAULT_PORT)
    if os.environ.get("A2A_API_KEY"):
        extra["api_key"] = os.environ["A2A_API_KEY"]
    if os.environ.get("A2A_AGENT_NAME"):
        extra["agent_name"] = os.environ["A2A_AGENT_NAME"]
    if os.environ.get("A2A_AGENT_DESCRIPTION"):
        extra["agent_description"] = os.environ["A2A_AGENT_DESCRIPTION"]
    if os.environ.get("A2A_PUBLIC_URL"):
        extra["public_url"] = os.environ["A2A_PUBLIC_URL"]
    home_channel = os.environ.get("A2A_HOME_CHANNEL")
    return {"extra": extra, "home_channel": home_channel}


def interactive_setup() -> None:
    """Minimal CLI setup wizard."""
    print("\n=== A2A (Agent-to-Agent) Platform Setup ===\n")
    print("The A2A adapter exposes Hermes as an A2A-compatible agent server.")
    print("Other A2A agents and orchestrators can discover and invoke Hermes")
    print("via the JSON-RPC 2.0 task protocol.\n")
    print("Required environment variables:")
    print("  A2A_HOST   — bind address (default: 127.0.0.1)")
    print("  A2A_PORT   — listen port  (default: 10000)\n")
    print("Optional:")
    print("  A2A_API_KEY        — Bearer token for auth")
    print("  A2A_PUBLIC_URL     — URL advertised in Agent Card")
    print("  A2A_AGENT_NAME     — name in Agent Card")
    print("  A2A_AGENT_DESCRIPTION — description in Agent Card\n")
    print("After setting these, add to config.yaml:")
    print("  gateway:")
    print("    platforms:")
    print("      a2a:")
    print("        enabled: true\n")


# ---------------------------------------------------------------------------
# Plugin entry point
# ---------------------------------------------------------------------------

def register(ctx) -> None:
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="a2a",
        label="A2A (Agent-to-Agent)",
        adapter_factory=lambda cfg: A2AAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["A2A_HOST", "A2A_PORT"],
        install_hint="Requires aiohttp (pip install aiohttp)",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="A2A_HOME_CHANNEL",
        allowed_users_env="A2A_ALLOWED_USERS",
        allow_all_env="A2A_ALLOW_ALL_USERS",
        emoji="🤖",
        pii_safe=True,
        platform_hint=(
            "You are operating as an A2A (Agent-to-Agent) server. "
            "Your responses are returned to other AI agents or orchestrators "
            "as A2A Task artifacts. Be precise and structured in your answers. "
            "Plain text and JSON are both acceptable output formats. "
            "Avoid platform-specific markdown unless the caller requests it."
        ),
    )
