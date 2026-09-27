"""Native PTY terminal page and WebSocket transport."""

import asyncio
import base64
import json
import time

from fastapi import APIRouter, Depends, Request, WebSocket, WebSocketDisconnect

from auth.dependencies import get_current_user
from auth.token import InvalidToken, get_token_claims
from config.settings import settings
from config.templates import templates
from core.request_context import is_cockpit_request
from services.audit_logger import audit_logger
from services.shell import TerminalBusyError, TerminalProcess
from services.shell_recording import RecordingError, ShellRecorder
from services.shell_settings import can_use_shell, shell_status

router = APIRouter()
MAX_START_MESSAGE_CHARS = 4096
MAX_TERMINAL_MESSAGE_CHARS = 131072
MAX_INPUT_BYTES = 65536
OUTPUT_DRAIN_TIMEOUT_SECONDS = 3
COMPLETED_RECORDING_REASONS = frozenset(
    {"shell_exit", "client_close", "client_disconnect", "idle_timeout", "token_expired"}
)


def recording_status_for(session_started: bool, close_reason: str) -> str:
    """Classify a finalized recording without treating worker failure as normal."""
    if session_started and close_reason in COMPLETED_RECORDING_REASONS:
        return "completed"
    return "incomplete"


def _client_ip(connection: Request | WebSocket) -> str:
    if connection.client:
        return connection.client.host
    return "unknown"


def _origin_allowed(websocket: WebSocket) -> bool:
    origin = websocket.headers.get("origin")
    host = websocket.headers.get("host")
    if not origin or not host:
        return False
    return origin in {f"http://{host}", f"https://{host}"}


@router.get("/")
def index(request: Request, username: str = Depends(get_current_user)):
    """Render native terminal, access-denied, or Cockpit handoff state."""
    context = shell_status(username)
    context.update(
        {
            "username": username,
            "cockpit_context": is_cockpit_request(request),
            "page_title": "Shell Terminal",
        }
    )
    return templates.TemplateResponse(
        request,
        name="utils/shell/index.jinja",
        context=context,
    )


@router.websocket("/terminal")
async def terminal(websocket: WebSocket):
    """Bridge one authenticated native WebSocket to one local PTY shell."""
    if is_cockpit_request(websocket) or not _origin_allowed(websocket):
        await websocket.close(code=4403)
        return
    token = websocket.cookies.get("token")
    try:
        claims = get_token_claims(token or "")
    except InvalidToken:
        await websocket.close(code=4401)
        return
    username = claims["username"]
    if not can_use_shell(username):
        await websocket.close(code=4403)
        return

    await websocket.accept()
    process = TerminalProcess(username)
    recorder = None
    session_id = None
    recording_enabled = settings.SHELL_RECORDING_ENABLED
    last_activity = time.monotonic()
    close_reason = "session_starting"
    session_started = False
    output_task = None
    input_task = None
    try:
        start_text = await asyncio.wait_for(websocket.receive_text(), timeout=15)
        if len(start_text) > MAX_START_MESSAGE_CHARS:
            raise ValueError("Terminal start message is too large")
        start_message = json.loads(start_text)
        if start_message.get("type") != "start":
            raise RecordingError("Terminal start acknowledgement was not received")
        recording_expected = start_message.get("recording_expected")
        if (
            not isinstance(recording_expected, bool)
            or recording_expected != recording_enabled
        ):
            await websocket.send_json(
                {
                    "type": "configuration_changed",
                    "recording": recording_enabled,
                    "message": "Terminal recording configuration changed. Reload this page before starting a terminal.",
                }
            )
            close_reason = "recording_configuration_changed"
            return
        if recording_enabled and not start_message.get("recording_acknowledged"):
            raise RecordingError("Terminal recording acknowledgement is required")
        process.resize(start_message.get("columns", 120), start_message.get("rows", 32))
        await asyncio.to_thread(process.acquire_lock)
        if recording_enabled:
            recorder = await asyncio.to_thread(
                ShellRecorder,
                {
                    "authenticated_user": username,
                    "effective_user": process.identity.username,
                    "effective_uid": process.identity.uid,
                    "shell": process.identity.shell,
                    "client_ip": _client_ip(websocket),
                    "initial_columns": process.columns,
                    "initial_rows": process.rows,
                },
            )
            session_id = recorder.session_id
        await asyncio.to_thread(process.start)
        audit_logger.log_shell_session(
            username,
            "open",
            effective_user=process.identity.username,
            session_id=session_id,
            client_ip=_client_ip(websocket),
            recording=recording_enabled,
            shell=process.identity.shell,
        )
        session_started = True
        await websocket.send_json(
            {
                "type": "ready",
                "session_id": session_id,
                "effective_user": process.identity.username,
                "recording": recording_enabled,
            }
        )

        async def send_output() -> None:
            nonlocal last_activity, close_reason
            websocket_available = True
            while True:
                output = await process.read()
                if not output:
                    if close_reason not in COMPLETED_RECORDING_REASONS:
                        close_reason = "shell_exit"
                    return
                last_activity = time.monotonic()
                if recorder:
                    await asyncio.to_thread(recorder.record, "output", output)
                if websocket_available:
                    try:
                        await websocket.send_json(
                            {
                                "type": "output",
                                "data": base64.b64encode(output).decode("ascii"),
                            }
                        )
                    except (RuntimeError, WebSocketDisconnect):
                        websocket_available = False
                        close_reason = "client_disconnect"
                        await asyncio.to_thread(process.terminate)

        async def receive_input() -> None:
            nonlocal last_activity, close_reason
            token_expires_at = float(claims["exp"])
            while True:
                timeout = min(
                    max(0.1, token_expires_at - time.time()),
                    max(
                        0.1,
                        settings.SHELL_IDLE_TIMEOUT_SECONDS
                        - (time.monotonic() - last_activity),
                    ),
                )
                try:
                    message_text = await asyncio.wait_for(
                        websocket.receive_text(), timeout=timeout
                    )
                    if len(message_text) > MAX_TERMINAL_MESSAGE_CHARS:
                        raise ValueError("Terminal message is too large")
                    message = json.loads(message_text)
                except asyncio.TimeoutError:
                    close_reason = (
                        "token_expired"
                        if time.time() >= token_expires_at
                        else "idle_timeout"
                    )
                    return
                except WebSocketDisconnect:
                    close_reason = "client_disconnect"
                    return
                message_type = message.get("type")
                if message_type == "input":
                    data = base64.b64decode(message.get("data", ""), validate=True)
                    if len(data) > MAX_INPUT_BYTES:
                        raise ValueError("Terminal input frame is too large")
                    if recorder:
                        await asyncio.to_thread(recorder.record, "input", data)
                    await asyncio.to_thread(process.write, data)
                    last_activity = time.monotonic()
                elif message_type == "resize":
                    process.resize(message.get("columns", 120), message.get("rows", 32))
                    if recorder:
                        await asyncio.to_thread(
                            recorder.record,
                            "resize",
                            {"columns": process.columns, "rows": process.rows},
                        )
                    last_activity = time.monotonic()
                elif message_type == "close":
                    close_reason = "client_close"
                    return

        output_task = asyncio.create_task(send_output())
        input_task = asyncio.create_task(receive_input())
        done, pending = await asyncio.wait(
            {output_task, input_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if input_task in done and output_task in pending:
            requested_close_reason = close_reason
            await asyncio.to_thread(process.terminate)
            try:
                await asyncio.wait_for(
                    asyncio.shield(output_task), timeout=OUTPUT_DRAIN_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                output_task.cancel()
            close_reason = requested_close_reason
        for task in pending:
            if not task.done():
                task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            task.result()
    except TerminalBusyError as exc:
        close_reason = "terminal_busy"
        await websocket.send_json({"type": "error", "message": str(exc)})
    except WebSocketDisconnect:
        close_reason = "client_disconnect"
    except asyncio.CancelledError:
        if session_started:
            close_reason = "worker_shutdown"
        current_task = asyncio.current_task()
        if current_task is not None:
            while current_task.cancelling():
                current_task.uncancel()
    except Exception:
        close_reason = "terminal_error"
        try:
            await websocket.send_json(
                {
                    "type": "error",
                    "message": "The terminal closed because of an internal error.",
                }
            )
        except Exception:
            pass
    finally:
        if input_task is not None and not input_task.done():
            input_task.cancel()
            await asyncio.gather(input_task, return_exceptions=True)
        if output_task is not None and not output_task.done():
            await asyncio.to_thread(process.terminate)
            try:
                await asyncio.wait_for(
                    asyncio.shield(output_task), timeout=OUTPUT_DRAIN_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                output_task.cancel()
                await asyncio.gather(output_task, return_exceptions=True)
        exit_code = await asyncio.to_thread(process.close)
        recording_status = None
        if recorder:
            try:
                final_metadata = await asyncio.to_thread(
                    recorder.close,
                    recording_status_for(session_started, close_reason),
                    exit_code,
                    close_reason,
                )
                recording_status = final_metadata["status"]
            except Exception:
                close_reason = "recording_finalize_error"
        if session_started:
            audit_logger.log_shell_session(
                username,
                "close",
                effective_user=process.identity.username,
                session_id=session_id,
                client_ip=_client_ip(websocket),
                recording=recording_enabled,
                exit_code=exit_code,
                reason=close_reason,
            )
        try:
            await websocket.send_json(
                {
                    "type": "closed",
                    "reason": close_reason,
                    "recording_status": recording_status,
                }
            )
            await websocket.close(code=1000)
        except Exception:
            pass
