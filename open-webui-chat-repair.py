"""
title: Open WebUI Chat Repair Pipe
description: Interactive pipe that scans and repairs chats with invalid Unicode (null bytes, stray surrogate pairs) before PostgreSQL indexes crash
id: chat-repair
version: 0.1.0
author: rbb-dev
author_url: https://github.com/rbb-dev
git_url: https://github.com/rbb-dev/Open-WebUI-Chat-Repair
required_open_webui_version: 0.6.28
license: MIT
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections import Counter
from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import datetime, timezone
import threading
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

from fastapi import Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from starlette.responses import StreamingResponse

from open_webui.internal.db import get_db
from open_webui.models.chats import Chat
from open_webui.models.users import Users
from sqlalchemy import func, or_

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


@dataclass
class ChatSanitizeReport:
    """Per-chat summary describing what would change after sanitization."""

    changed: bool
    counts: Counter = field(default_factory=Counter)
    fields: List[str] = field(default_factory=list)
    sanitized_values: Dict[str, Any] = field(default_factory=dict)


class ChatRepairService:
    """Encapsulates scanning and repair logic so it can run in a thread."""

    DETAIL_SAMPLE_MAX = 20

    def __init__(self, chunk_size: int = 200):
        self.chunk_size = max(50, chunk_size)

    def scan(
        self,
        *,
        max_results: int,
        user_filter: Optional[str] = None,
        user_query: Optional[str] = None,
        chat_ids: Optional[Sequence[str]] = None,
        status_callback: Optional[Callable[[str, int], None]] = None,
        result_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        user_sorter: Optional[Callable[[str], str]] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        """Return chats that would change if sanitized."""

        summary_counter: Counter = Counter()
        matches: List[Dict[str, Any]] = []
        examined = 0
        has_more = False

        with get_db() as db:
            user_batches = self._collect_user_batches(
                db,
                user_filter=user_filter,
                user_query=user_query,
                chat_ids=chat_ids,
            )
            if user_sorter:
                user_batches.sort(key=lambda batch: user_sorter(batch[0]))
            else:
                user_batches.sort(key=lambda batch: (batch[0] or ""))

            for user_id, chat_count in user_batches:
                if cancel_event and cancel_event.is_set():
                    has_more = True
                    break
                if status_callback:
                    try:
                        status_callback(user_id, chat_count)
                    except Exception:
                        logger.warning("Failed to emit scan status", exc_info=True)

                query = self._build_query(
                    db,
                    user_filter=user_id,
                    chat_ids=chat_ids,
                )
                iterator = query.yield_per(self.chunk_size)
                for chat in iterator:
                    if cancel_event and cancel_event.is_set():
                        has_more = True
                        break
                    examined += 1
                    report = self._analyse_chat(chat, mutate=False)
                    if not report.changed:
                        continue
                    summary_counter.update(report.counts)
                    row = {
                        "chat_id": chat.id,
                        "user_id": chat.user_id,
                        "title": chat.title or "(untitled)",
                        "updated_at": chat.updated_at,
                        "issue_counts": dict(report.counts),
                        "fields": list(report.fields),
                    }
                    matches.append(row)
                    if result_callback:
                        try:
                            result_callback(row)
                        except Exception:
                            logger.warning(
                                "Failed to emit result callback", exc_info=True
                            )
                    if max_results and len(matches) >= max_results:
                        has_more = True
                        break
                if has_more:
                    break

        return {
            "examined": examined,
            "matches": len(matches),
            "results": matches,
            "counters": dict(summary_counter),
            "has_more": has_more,
        }

    def repair(
        self,
        *,
        max_repairs: Optional[int],
        user_filter: Optional[str] = None,
        user_query: Optional[str] = None,
        chat_ids: Optional[Sequence[str]] = None,
        status_callback: Optional[Callable[[str, int], None]] = None,
        user_sorter: Optional[Callable[[str], str]] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        """Sanitize chats in-place and commit changes."""

        repaired = 0
        examined = 0
        has_more = False
        summary_counter: Counter = Counter()
        details: List[Dict[str, Any]] = []

        with get_db() as db:
            try:
                user_batches = self._collect_user_batches(
                    db,
                    user_filter=user_filter,
                    user_query=user_query,
                    chat_ids=chat_ids,
                )
                if user_sorter:
                    user_batches.sort(key=lambda batch: user_sorter(batch[0]))
                else:
                    user_batches.sort(key=lambda batch: (batch[0] or ""))

                for user_id, chat_count in user_batches:
                    if cancel_event and cancel_event.is_set():
                        has_more = True
                        break
                    if status_callback:
                        try:
                            status_callback(user_id, chat_count)
                        except Exception:
                            logger.warning(
                                "Failed to emit repair status", exc_info=True
                            )

                    query = self._build_query(
                        db,
                        user_filter=user_id,
                        chat_ids=chat_ids,
                    )
                    iterator = query.yield_per(self.chunk_size)
                    for chat in iterator:
                        if cancel_event and cancel_event.is_set():
                            has_more = True
                            break
                        examined += 1
                        report = self._analyse_chat(chat, mutate=True)
                        if not report.changed:
                            continue
                        summary_counter.update(report.counts)
                        repaired += 1
                        if len(details) < self.DETAIL_SAMPLE_MAX:
                            details.append(
                                {
                                    "chat_id": chat.id,
                                    "user_id": chat.user_id,
                                    "title": chat.title or "(untitled)",
                                    "issue_counts": dict(report.counts),
                                    "fields": list(report.fields),
                                    "updated_at": chat.updated_at,
                                }
                            )
                        if max_repairs and repaired >= max_repairs:
                            has_more = True
                            break
                    if has_more:
                        break

                if repaired:
                    db.commit()
                else:
                    db.rollback()
            except Exception:
                db.rollback()
                raise

        return {
            "examined": examined,
            "repaired": repaired,
            "details": details,
            "counters": dict(summary_counter),
            "has_more": has_more,
        }

    def _build_query(
        self,
        session,
        *,
        user_filter: Optional[str],
        chat_ids: Optional[Sequence[str]],
    ):
        query = session.query(Chat)
        if user_filter:
            query = query.filter(Chat.user_id == user_filter)
        if chat_ids:
            ids = [cid for cid in chat_ids if cid]
            if ids:
                query = query.filter(Chat.id.in_(ids))
            else:
                query = query.filter(False)
        return query.order_by(Chat.updated_at.desc())

    def _collect_user_batches(
        self,
        session,
        *,
        user_filter: Optional[str],
        user_query: Optional[str],
        chat_ids: Optional[Sequence[str]],
    ) -> List[Tuple[str, int]]:
        query = session.query(Chat.user_id, func.count(Chat.id))
        if user_filter:
            query = query.filter(Chat.user_id == user_filter)
        if user_query:
            user_ids = self._lookup_user_ids_by_query(session, user_query)
            if not user_ids:
                return []
            query = query.filter(Chat.user_id.in_(user_ids))
        if chat_ids:
            ids = [cid for cid in chat_ids if cid]
            if ids:
                query = query.filter(Chat.id.in_(ids))
            else:
                return []
        query = query.group_by(Chat.user_id)
        return [(row[0], int(row[1])) for row in query]

    def _lookup_user_ids_by_query(
        self, session, query: str, limit: int = 50
    ) -> List[str]:
        from open_webui.models.users import User

        text = (query or "").strip()
        if not text:
            return []
        escaped = text.replace("%", "\\%").replace("_", "\\_")
        like_pattern = f"%{escaped}%"
        user_rows = (
            session.query(User.id)
            .filter(
                or_(
                    User.name.ilike(like_pattern),
                    User.username.ilike(like_pattern),
                    User.email.ilike(like_pattern),
                )
            )
            .limit(limit)
            .all()
        )
        return [row[0] for row in user_rows]

    def _analyse_chat(self, chat: Chat, *, mutate: bool) -> ChatSanitizeReport:
        counts: Counter = Counter()
        fields: List[str] = []
        sanitized_values: Dict[str, Any] = {}

        title_value, title_changed, title_counts = self._sanitize_value(chat.title)
        counts.update(title_counts)
        if title_changed:
            sanitized_values["title"] = title_value
            fields.append("title")
            if mutate:
                chat.title = title_value

        chat_payload, chat_changed, chat_counts = self._sanitize_value(chat.chat)
        counts.update(chat_counts)
        if chat_changed:
            sanitized_values["chat"] = chat_payload
            fields.append("chat")
            if mutate:
                chat.chat = chat_payload

        meta_payload, meta_changed, meta_counts = self._sanitize_value(chat.meta)
        counts.update(meta_counts)
        if meta_changed:
            sanitized_values["meta"] = meta_payload
            fields.append("meta")
            if mutate:
                chat.meta = meta_payload

        if mutate and sanitized_values:
            chat.updated_at = max(int(time.time()), chat.updated_at or 0)

        return ChatSanitizeReport(
            changed=bool(sanitized_values),
            counts=counts,
            fields=fields,
            sanitized_values=sanitized_values,
        )

    def _sanitize_value(self, value: Any):
        """Sanitize any JSON-compatible value and report counts."""

        if value is None:
            return value, False, Counter()

        if isinstance(value, str):
            sanitized, counts, changed = self._sanitize_string(value)
            return sanitized if changed else value, changed, counts

        if isinstance(value, list):
            changed = False
            counts: Counter = Counter()
            new_items: List[Any] = []
            for item in value:
                sanitized_item, item_changed, item_counts = self._sanitize_value(item)
                counts.update(item_counts)
                changed = changed or item_changed
                new_items.append(sanitized_item)
            return (new_items if changed else value), changed, counts

        if isinstance(value, tuple):
            changed = False
            counts: Counter = Counter()
            new_items: List[Any] = []
            for item in value:
                sanitized_item, item_changed, item_counts = self._sanitize_value(item)
                counts.update(item_counts)
                changed = changed or item_changed
                new_items.append(sanitized_item)
            sanitized_value = tuple(new_items)
            return (sanitized_value if changed else value), changed, counts

        if isinstance(value, dict):
            changed = False
            counts: Counter = Counter()
            sanitized_dict: Dict[str, Any] = {}
            for key, item in value.items():
                sanitized_item, item_changed, item_counts = self._sanitize_value(item)
                counts.update(item_counts)
                if item_changed:
                    changed = True
                    sanitized_dict[key] = sanitized_item
                else:
                    sanitized_dict[key] = item
            return (sanitized_dict if changed else value), changed, counts

        return value, False, Counter()

    def _sanitize_string(self, value: str):
        if not value:
            return value, Counter(), False

        counts: Counter = Counter()
        builder: List[str] = []
        changed = False
        i = 0
        length = len(value)

        while i < length:
            ch = value[i]
            code = ord(ch)

            if ch == "\x00":
                counts["null_bytes"] += 1
                changed = True
                i += 1
                continue

            if 0xD800 <= code <= 0xDBFF:
                if i + 1 < length:
                    next_code = ord(value[i + 1])
                    if 0xDC00 <= next_code <= 0xDFFF:
                        builder.append(ch)
                        builder.append(value[i + 1])
                        i += 2
                        continue
                counts["lone_high"] += 1
                builder.append("\ufffd")
                changed = True
                i += 1
                continue

            if 0xDC00 <= code <= 0xDFFF:
                counts["lone_low"] += 1
                builder.append("\ufffd")
                changed = True
                i += 1
                continue

            builder.append(ch)
            i += 1

        sanitized = "".join(builder)
        if changed:
            counts["strings_touched"] += 1
            return sanitized, counts, True
        return value, counts, False


class Pipe:
    PIPE_ID = "chat-repair"
    PIPE_NAME = "Open WebUI: Chat Repair"

    class Valves(BaseModel):
        ENABLE_LOGGING: bool = Field(
            default=False,
            description="Emit INFO logs for the repair pipe (disabled by default).",
        )
        SCAN_DEFAULT_LIMIT: int = Field(
            default=0,
            ge=0,
            le=5000,
            description="How many problematic chats to list per scan command (0 = no limit).",
        )
        SCAN_MAX_LIMIT: int = Field(
            default=200,
            ge=25,
            le=1000,
            description="Hard cap for scan output rows.",
        )
        REPAIR_DEFAULT_LIMIT: int = Field(
            default=10,
            ge=0,
            le=200,
            description="How many chats to repair per run (0 = no cap).",
        )
        REPAIR_MAX_LIMIT: int = Field(
            default=200,
            ge=10,
            le=1000,
            description="Safety ceiling for repairs per command.",
        )
        DB_CHUNK_SIZE: int = Field(
            default=200,
            ge=50,
            le=1000,
            description="Rows fetched from the chat table per batch when scanning.",
        )

    def __init__(self):
        self.valves = self.Valves()
        self.service = ChatRepairService(chunk_size=self.valves.DB_CHUNK_SIZE)
        self._apply_logging_valve()

    def _apply_logging_valve(self) -> None:
        level = logging.DEBUG if self.valves.ENABLE_LOGGING else logging.INFO
        logger.setLevel(level)
        logger.propagate = True

    async def pipes(self) -> List[dict]:
        return [{"id": self.PIPE_ID, "name": self.PIPE_NAME}]

    async def pipe(
        self,
        body: dict,
        __user__: dict,
        __request__: Request,
        __event_emitter__: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> StreamingResponse | PlainTextResponse:
        command_text = self._extract_prompt_text(body)
        if not command_text:
            message = self._help_markdown()
            return await self._respond(True, message, body)

        command, options = self._parse_command(command_text)
        if not command:
            message = self._help_markdown()
            return await self._respond(False, message, body)

        user_filter = options.get("user_id")
        if user_filter in {"me", "self"}:
            user_filter = __user__.get("id")
        user_query = options.get("user_query")

        target_ids = options.get("ids") or []
        limit_option = options.get("limit")

        try:
            loop = asyncio.get_running_loop()
            if command == "help":
                return await self._respond(True, self._help_markdown(), body)

            if command == "scan":
                is_stream = bool(body.get("stream", True))
                status_cache: Dict[str, str] = {}
                user_sorter = self._make_user_sorter(status_cache)
                if is_stream:
                    return await self._stream_scan(
                        loop=loop,
                        emitter=__event_emitter__,
                        limit_option=limit_option,
                        user_filter=user_filter,
                        target_ids=target_ids,
                        body=body,
                        status_cache=status_cache,
                        user_sorter=user_sorter,
                        user_query=user_query,
                    )
                status_callback = self._threadsafe_user_status_callback(
                    loop, __event_emitter__, status_cache, action="Scanning"
                )
                limit = self._clamp_limit(
                    limit_option,
                    default=self.valves.SCAN_DEFAULT_LIMIT,
                    ceiling=self.valves.SCAN_MAX_LIMIT,
                    allow_zero=True,
                )
                await self._emit_status(
                    __event_emitter__,
                    "Scanning chats (no limit)" if limit == 0 else f"Scanning chats (limit={limit})...",
                )
                scan_result = await asyncio.to_thread(
                    self.service.scan,
                    max_results=limit,
                    user_filter=user_filter,
                    user_query=user_query,
                    chat_ids=target_ids,
                    status_callback=status_callback,
                    user_sorter=user_sorter,
                )
                await self._emit_status(__event_emitter__, "Scan complete", done=True)
                user_labels = await self._resolve_user_labels([row["user_id"] for row in scan_result["results"]])
                message = self._build_scan_report(
                    scan_result,
                    user_labels=user_labels,
                    scope=self._describe_scope(user_filter, target_ids, user_query),
                )
                return await self._respond(True, message, body)

            if command == "repair":
                if not options.get("confirm"):
                    reminder = (
                        "Add `confirm` to run repairs (example: `repair confirm limit=10`)."
                    )
                    return await self._respond(False, reminder, body)

                repair_status_cache: Dict[str, str] = {}
                repair_user_sorter = self._make_user_sorter(repair_status_cache)
                status_callback = self._threadsafe_user_status_callback(
                    loop, __event_emitter__, repair_status_cache, action="Repairing"
                )
                if not target_ids:
                    target_ids = self._extract_chat_ids_from_history(body)
                limit = self._clamp_limit(
                    limit_option,
                    default=self.valves.REPAIR_DEFAULT_LIMIT,
                    ceiling=self.valves.REPAIR_MAX_LIMIT,
                    allow_zero=True,
                )
                await self._emit_status(__event_emitter__, "Repair pass running...")
                repair_result = await asyncio.to_thread(
                    self.service.repair,
                    max_repairs=(None if limit == 0 else limit),
                    user_filter=user_filter,
                    user_query=user_query,
                    chat_ids=target_ids,
                    status_callback=status_callback,
                    user_sorter=repair_user_sorter,
                )
                await self._emit_status(__event_emitter__, "Repair pass finished", done=True)
                user_ids = [row["user_id"] for row in repair_result["details"]]
                user_labels = await self._resolve_user_labels(user_ids)
                message = self._build_repair_report(
                    repair_result,
                    user_labels=user_labels,
                    limit=limit,
                    scope=self._describe_scope(user_filter, target_ids, user_query),
                )
                return await self._respond(True, message, body)

            message = (
                f"Unknown command `{command}`. Available commands: help, scan, repair.\n\n"
                + self._help_markdown()
            )
            return await self._respond(False, message, body)
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.exception("Chat repair pipe failed")
            await self._emit_status(__event_emitter__, "Chat repair command failed", done=True)
            return await self._respond(False, f"Unable to complete command: {exc}", body)

    async def _respond(self, ok: bool, message: str, body: dict):
        is_stream = bool(body.get("stream"))
        status_code = 200 if ok else 400
        if not is_stream:
            safe_message = self._sanitize_output_text(message)
            return PlainTextResponse(safe_message, status_code=status_code)

        async def stream():
            payload = self._format_data(is_stream=True, model=self.PIPE_NAME, content=message, finish_reason="stop")
            yield payload
            yield "data: [DONE]\n\n"

        return StreamingResponse(stream(), media_type="text/event-stream")

    async def _emit_status(
        self,
        emitter: Optional[Callable[[dict], Awaitable[None]]],
        message: str,
        *,
        done: bool = False,
    ) -> None:
        if emitter:
            await emitter({"type": "status", "data": {"description": message, "done": done}})

    async def _emit_message_chunk(
        self,
        emitter: Optional[Callable[[dict], Awaitable[None]]],
        content: Optional[str],
    ) -> None:
        if not emitter or not content:
            return
        safe = self._sanitize_output_text(content)
        if not safe:
            return
        await emitter({"type": "message", "data": {"content": safe}})

    async def _stream_scan(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        emitter: Optional[Callable[[dict], Awaitable[None]]],
        limit_option: Optional[int],
        user_filter: Optional[str],
        user_query: Optional[str],
        target_ids: List[str],
        body: dict,
        status_cache: Dict[str, str],
        user_sorter: Callable[[str], str],
    ) -> StreamingResponse:
        limit = self._clamp_limit(
            limit_option,
            default=self.valves.SCAN_DEFAULT_LIMIT,
            ceiling=self.valves.SCAN_MAX_LIMIT,
            allow_zero=True,
        )
        queue: asyncio.Queue[Optional[Dict[str, Any]]] = asyncio.Queue()
        stream_user_cache: Dict[str, str] = {}
        status_callback = self._threadsafe_user_status_callback(
            loop, emitter, status_cache, action="Scanning"
        )
        result_callback = self._threadsafe_result_callback(loop, queue)
        scope = self._describe_scope(user_filter, target_ids, user_query)
        thread_cancel = threading.Event()

        async def run_scan():
            await self._emit_status(
                emitter,
                "Scanning chats (no limit)" if limit == 0 else f"Scanning chats (limit={limit})...",
            )
            result = None

            def _scan_thread():
                return self.service.scan(
                    max_results=limit,
                    user_filter=user_filter,
                    user_query=user_query,
                    chat_ids=target_ids,
                    status_callback=status_callback,
                    result_callback=result_callback,
                    user_sorter=user_sorter,
                    cancel_event=thread_cancel,
                )

            try:
                result = await asyncio.to_thread(_scan_thread)
                return result
            finally:
                await queue.put(None)
                await self._emit_status(emitter, "Scan complete", done=True)

        scan_task = asyncio.create_task(run_scan())

        async def stream():
            intro = (
                "Scan in progress – the table below will populate with chats that need fixing.\n\n"
                "| User | Chat ID | Title | Issues |\n"
                "| --- | --- | --- | --- |"
            )
            yield self._format_data(is_stream=True, model=self.PIPE_NAME, content=intro, finish_reason=None)
            await self._emit_message_chunk(emitter, intro)
            cancelled = False
            try:
                while True:
                    row = await queue.get()
                    if row is None:
                        break
                    line = await self._format_stream_row(row, stream_user_cache)
                    await self._emit_message_chunk(emitter, line)
                    yield self._format_data(
                        is_stream=True,
                        model=self.PIPE_NAME,
                        content=line,
                        finish_reason=None,
                    )

                scan_result = await scan_task
                summary = self._build_stream_scan_summary(scan_result, scope)
                await self._emit_message_chunk(emitter, summary)
                yield self._format_data(
                    is_stream=True,
                    model=self.PIPE_NAME,
                    content=summary,
                    finish_reason="stop",
                )
                yield "data: [DONE]\n\n"
            except asyncio.CancelledError:
                cancelled = True
                raise
            finally:
                thread_cancel.set()
                if cancelled:
                    with contextlib.suppress(asyncio.CancelledError):
                        await scan_task

        return StreamingResponse(stream(), media_type="text/event-stream")

    def _threadsafe_user_status_callback(
        self,
        loop: asyncio.AbstractEventLoop,
        emitter: Optional[Callable[[dict], Awaitable[None]]],
        cache: Dict[str, str],
        action: str = "Scanning",
    ) -> Callable[[str, int], None]:
        seen_users: set[str] = set()

        def _callback(user_id: str, chat_count: int) -> None:
            user_id = user_id or "unknown"
            if user_id in seen_users:
                return
            seen_users.add(user_id)
            label = self._lookup_user_label_sync(user_id, cache)
            message = (
                f"{action} {label}'s chats for corruption (total chats: {chat_count})"
            )
            logger.info(message)
            if not emitter:
                return
            future = asyncio.run_coroutine_threadsafe(
                self._emit_status(emitter, message),
                loop,
            )
            future.add_done_callback(self._log_future_exception)

        return _callback

    @staticmethod
    def _log_future_exception(future: Future) -> None:
        try:
            future.result()
        except Exception as exc:
            logger.warning("Status emission failed: %s", exc)

    def _threadsafe_result_callback(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: "asyncio.Queue[Optional[Dict[str, Any]]]",
    ) -> Callable[[Dict[str, Any]], None]:
        def _callback(row: Dict[str, Any]) -> None:
            asyncio.run_coroutine_threadsafe(queue.put(row), loop)

        return _callback

    def _make_user_sorter(self, cache: Dict[str, str]) -> Callable[[str], str]:
        def _sorter(user_id: str) -> str:
            label = self._lookup_user_label_sync(user_id or "unknown", cache)
            return (label or user_id or "").lower()

        return _sorter

    def _extract_chat_ids_from_history(self, body: dict) -> List[str]:
        ids: List[str] = []
        messages = body.get("messages") or []
        for message in reversed(messages):
            if message.get("role") != "assistant":
                continue
            content = self._collapse_content(message.get("content"))
            if not content:
                continue
            ids.extend(self._extract_chat_ids_from_text(content))
            if ids:
                break
        deduped: List[str] = []
        seen: set[str] = set()
        for cid in ids:
            if cid not in seen:
                seen.add(cid)
                deduped.append(cid)
        return deduped

    def _extract_chat_ids_from_text(self, text: str) -> List[str]:
        ids: List[str] = []
        for line in text.splitlines():
            line = line.strip()
            if not line.startswith("|"):
                continue
            cells = [cell.strip() for cell in line.split("|") if cell.strip()]
            if len(cells) < 2:
                continue
            for cell in cells:
                if cell.startswith("`") and cell.endswith("`"):
                    chat_id = cell.strip("`").strip()
                    if chat_id:
                        ids.append(chat_id)
                        break
        return ids

    def _lookup_user_label_sync(self, user_id: str, cache: Dict[str, str]) -> str:
        if user_id in cache:
            return cache[user_id]
        label = user_id
        if user_id and user_id != "unknown":
            try:
                user = Users.get_user_by_id(user_id)
                if user:
                    label = (
                        getattr(user, "name", None)
                        or getattr(user, "username", None)
                        or getattr(user, "email", None)
                        or user_id
                    )
            except Exception:
                logger.debug("Failed to resolve user %s", user_id, exc_info=True)
        cache[user_id] = label
        return label

    async def _get_user_label_async(self, user_id: str, cache: Dict[str, str]) -> str:
        if user_id in cache:
            return cache[user_id]
        label = await asyncio.to_thread(self._lookup_user_label_sync, user_id, cache)
        return label

    async def _format_stream_row(self, row: Dict[str, Any], cache: Dict[str, str]) -> str:
        user_label = await self._get_user_label_async(row.get("user_id") or "unknown", cache)
        chat_id = row.get("chat_id") or "unknown"
        title = self._shorten(self._sanitize_output_text(row.get("title") or "(untitled)"))
        issues = self._describe_counts(row.get("issue_counts", {}))
        if not issues:
            issues = ", ".join(row.get("fields", [])) or "Needs sanitizing"
        return f"\n| {user_label} | `{chat_id}` | {title} | {issues} |"

    def _build_stream_scan_summary(self, scan_result: Dict[str, Any], scope: str) -> str:
        lines = ["", "### Scan summary", ""]
        lines.append(f"- Rows inspected: {scan_result['examined']}")
        lines.append(f"- Chats needing repair: {scan_result['matches']}")
        lines.append(f"- Scope: {scope}")
        counts_summary = self._describe_counts(scan_result.get("counters", {}))
        if counts_summary:
            lines.append(f"- Character fixes applied if you run repair: {counts_summary}")
        if scan_result.get("has_more"):
            lines.append("- Limit reached. Re-run scan to continue browsing results.")
        if not scan_result.get("results"):
            lines.append("- No malformed Unicode detected in this batch.")
        lines.append("")
        lines.append("Run `repair confirm` (use `limit=<n>` or `limit=0` for unlimited) to sanitize the listed chats.")
        return "\n".join(lines)

    def _sanitize_output_text(self, text: Optional[str]) -> str:
        if text is None:
            return ""
        value = str(text)
        if not value:
            return ""
        builder: List[str] = []
        i = 0
        length = len(value)
        while i < length:
            ch = value[i]
            code = ord(ch)
            if ch == "\x00":
                i += 1
                continue
            if 0xD800 <= code <= 0xDBFF:
                if i + 1 < length:
                    next_code = ord(value[i + 1])
                    if 0xDC00 <= next_code <= 0xDFFF:
                        builder.append(ch)
                        builder.append(value[i + 1])
                        i += 2
                        continue
                builder.append("\ufffd")
                i += 1
                continue
            if 0xDC00 <= code <= 0xDFFF:
                builder.append("\ufffd")
                i += 1
                continue
            builder.append(ch)
            i += 1
        return "".join(builder)

    async def _resolve_user_labels(self, user_ids: Sequence[str]) -> Dict[str, str]:
        unique_ids = sorted({uid for uid in user_ids if uid})
        if not unique_ids:
            return {}

        def _load(ids: Sequence[str]):
            mapping: Dict[str, str] = {}
            for uid in ids:
                try:
                    user = Users.get_user_by_id(uid)
                except Exception:
                    user = None
                if user:
                    label = getattr(user, "name", None) or getattr(user, "username", None) or getattr(user, "email", None)
                    mapping[uid] = label or uid
                else:
                    mapping[uid] = uid
            return mapping

        return await asyncio.to_thread(_load, unique_ids)

    def _extract_prompt_text(self, body: dict) -> str:
        messages = body.get("messages") or []
        for message in reversed(messages):
            if message.get("role") != "user":
                continue
            text = self._collapse_content(message.get("content"))
            if text:
                return text.strip()
        for fallback_key in ("prompt", "input", "text"):
            if fallback_key in body and body[fallback_key]:
                return str(body[fallback_key]).strip()
        return ""

    def _collapse_content(self, content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    parts.append(item.get("text", ""))
            return " ".join(parts)
        if isinstance(content, dict):
            if content.get("type") == "text":
                return content.get("text", "")
            if "content" in content:
                return str(content["content"])
        return ""

    def _parse_command(self, text: str):
        tokens = text.strip().split()
        if not tokens:
            return "", {}
        command = tokens[0].lower()
        options: Dict[str, Any] = {
            "limit": None,
            "ids": [],
            "user_id": None,
            "user_query": None,
            "confirm": False,
        }
        for token in tokens[1:]:
            lowered = token.lower()
            if lowered == "confirm":
                options["confirm"] = True
                continue
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            key = key.lower().strip()
            value = value.strip()
            if key in {"limit", "max"}:
                try:
                    options["limit"] = int(value)
                except ValueError:
                    pass
            elif key in {"id", "chat", "chat_id"}:
                ids = [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
                options["ids"].extend(ids)
            elif key in {"user", "user_id"}:
                options["user_id"] = value
            elif key in {"user_query", "user_name", "username", "name", "email"}:
                options["user_query"] = value
        return command, options

    def _clamp_limit(
        self,
        value: Optional[int],
        *,
        default: int,
        ceiling: int,
        allow_zero: bool,
    ) -> int:
        if value is None:
            return default
        if value == 0 and allow_zero:
            return 0
        if value <= 0:
            return default
        return min(value, ceiling)

    def _describe_scope(
        self,
        user_filter: Optional[str],
        target_ids: Sequence[str],
        user_query: Optional[str] = None,
    ) -> str:
        scope = []
        if user_filter:
            scope.append(f"user `{user_filter}`")
        elif user_query:
            scope.append(f'users matching "{user_query}"')
        if target_ids:
            scope.append(f"chat ids ({len(target_ids)})")
        return ", ".join(scope) if scope else "entire workspace"

    def _build_scan_report(self, scan_result: Dict[str, Any], *, user_labels: Dict[str, str], scope: str) -> str:
        lines = ["### Scan summary", ""]
        lines.append(f"- Rows inspected: {scan_result['examined']}")
        lines.append(f"- Problematic chats listed: {scan_result['matches']}")
        lines.append(f"- Scope: {scope}")
        counts_summary = self._describe_counts(scan_result["counters"])
        if counts_summary:
            lines.append(f"- Potential fixes: {counts_summary}")
        if scan_result["has_more"]:
            lines.append("- Limit reached before finishing the data set.")
        lines.append("")
        results = scan_result["results"]
        if not results:
            lines.append("No malformed Unicode was detected in the scanned chats.")
            lines.append("Add `limit=50` if you only want to spot-check a subset next time.")
            return "\n".join(lines)

        lines.append("| Chat ID | User | Title | Updated (UTC) | Issues | Fields |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for row in results:
            chat_id = row["chat_id"]
            user_label = user_labels.get(row["user_id"], row["user_id"])
            title = self._shorten(self._sanitize_output_text(row.get("title") or "(untitled)"))
            updated = self._format_timestamp(row.get("updated_at"))
            issues = self._describe_counts(row["issue_counts"])
            fields = ", ".join(row["fields"]) or "title"
            lines.append(
                f"| `{chat_id}` | {user_label} | {title} | {updated} | {issues} | {fields} |"
            )

        lines.append("")
        lines.append(
            "Next step: run `repair confirm limit=10` (or `limit=0` for no cap) to clean the listed chats."
        )
        return "\n".join(lines)

    def _build_repair_report(
        self,
        repair_result: Dict[str, Any],
        *,
        user_labels: Dict[str, str],
        limit: int,
        scope: str,
    ) -> str:
        lines = ["### Repair summary", ""]
        lines.append(f"- Rows inspected: {repair_result['examined']}")
        lines.append(f"- Chats repaired: {repair_result['repaired']}")
        lines.append(f"- Scope: {scope}")
        counts_summary = self._describe_counts(repair_result["counters"])
        if counts_summary:
            lines.append(f"- Characters replaced/removed: {counts_summary}")
        if limit == 0:
            lines.append("- Limit: unlimited (ran until scope finished)")
        else:
            lines.append(f"- Limit: {limit}")
        if repair_result["has_more"]:
            lines.append("- Limit reached. Run the command again to continue.")
        lines.append("")
        details = repair_result["details"]
        if not details:
            lines.append("No chats required changes. You're good to go!")
            return "\n".join(lines)

        lines.append("| Chat ID | User | Issues | Fields |")
        lines.append("| --- | --- | --- | --- |")
        for row in details:
            chat_id = row["chat_id"]
            user_label = user_labels.get(row["user_id"], row["user_id"])
            issues = self._describe_counts(row["issue_counts"])
            fields = ", ".join(row["fields"]) or "title"
            lines.append(f"| `{chat_id}` | {user_label} | {issues} | {fields} |")

        if repair_result["has_more"]:
            lines.append("")
            lines.append("Additional chats still need repairs. Re-run the command to keep going.")
        return "\n".join(lines)

    def _describe_counts(self, counts: Dict[str, int]) -> str:
        if not counts:
            return ""
        parts = []
        nulls = counts.get("null_bytes", 0)
        if nulls:
            parts.append(f"{nulls} null byte{'s' if nulls != 1 else ''}")
        high = counts.get("lone_high", 0)
        if high:
            parts.append(f"{high} lone high surrogate{'s' if high != 1 else ''}")
        low = counts.get("lone_low", 0)
        if low:
            parts.append(f"{low} lone low surrogate{'s' if low != 1 else ''}")
        touched = counts.get("strings_touched", 0)
        if touched and not parts:
            parts.append(f"{touched} string{'s' if touched != 1 else ''} sanitized")
        elif touched:
            parts.append(f"{touched} string{'s' if touched != 1 else ''}")
        return ", ".join(parts)

    def _format_timestamp(self, value: Optional[int]) -> str:
        if not value:
            return "—"
        try:
            dt = datetime.fromtimestamp(int(value), tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            return str(value)

    def _shorten(self, text: str, limit: int = 60) -> str:
        text = self._sanitize_output_text(text or "")
        text = text.replace("|", "\\|")
        if len(text) <= limit:
            return text
        return text[: limit - 3] + "..."

    def _help_markdown(self) -> str:
        return """# Open WebUI Chat Repair Pipe

Use this model as a command console for cleaning malformed Unicode inside stored chats. Commands:

- `help` – show this guide.
- `scan [limit=<n>] [user=<id>|user=me] [user_query="name"] [id=<chat-id>]` – walk the entire chat database (default is unlimited) and live-stream any rows that need repairs. Add `limit=50` (or any number up to 200) only if you want to stop early, or use `user_query="alice"` / `user=<uuid>` to focus on specific people.
- `repair confirm [limit=<n>] [user=<id>|user=me] [user_query="name"] [id=<chat-id>]` – sanitize the matching chats in-place using the same logic as the backend. `limit=0` repairs everything in scope; otherwise the value is capped at 200 (default 10). If you just ran `scan`, you can omit `id=` and it will fix the chats from that table automatically.

Notes:
- You must include `confirm` to run repairs (safety net).
- Provide multiple chat ids with commas: `id=abc,id=def`.
- The pipe never deletes content; it only removes null bytes and replaces orphaned UTF-16 surrogate halves with `\ufffd`.
- Re-run `scan` after a repair if you want to verify the database is clean.
"""

    def _format_data(
        self,
        *,
        is_stream: bool,
        model: str,
        content: Optional[str],
        usage: Optional[dict] = None,
        finish_reason: Optional[str] = None,
    ) -> str:
        safe_content = (
            self._sanitize_output_text(content) if isinstance(content, str) else content
        )
        data: Dict[str, Any] = {
            "id": f"chat.{int(time.time()*1000)}",
            "object": "chat.completion.chunk" if is_stream else "chat.completion",
            "created": int(time.time()),
            "model": model,
        }
        if is_stream:
            is_stop_chunk = finish_reason == "stop" and safe_content is None
            delta: Dict[str, Any] = {}
            if not is_stop_chunk:
                delta["role"] = "assistant"
                if safe_content is not None:
                    delta["content"] = safe_content
            data["choices"] = [
                {
                    "index": 0,
                    "finish_reason": finish_reason,
                    "delta": delta,
                }
            ]
        else:
            data["choices"] = [
                {
                    "index": 0,
                    "finish_reason": finish_reason or "stop",
                    "message": {
                        "role": "assistant",
                        "content": safe_content or "",
                    },
                }
            ]
        if usage:
            data["usage"] = usage
        return f"data: {json.dumps(data)}\n\n"
