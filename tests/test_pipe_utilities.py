from __future__ import annotations

import pytest

from open_webui_chat_repair import Pipe


@pytest.fixture(scope="module")
def pipe():
    return Pipe()


def test_parse_command_extracts_options(pipe):
    command, options = pipe._parse_command('scan limit=25 id=a,b;c user=me user_query="alice" confirm')
    assert command == "scan"
    assert options["limit"] == 25
    assert options["ids"] == ["a", "b", "c"]
    assert options["user_id"] == "me"
    assert options["user_query"].strip('"') == "alice"
    assert options["confirm"] is True


def test_clamp_limit_bounds(pipe):
    assert pipe._clamp_limit(None, default=10, ceiling=50, allow_zero=True) == 10
    assert pipe._clamp_limit(0, default=10, ceiling=50, allow_zero=True) == 0
    assert pipe._clamp_limit(-5, default=10, ceiling=50, allow_zero=False) == 10
    assert pipe._clamp_limit(500, default=10, ceiling=50, allow_zero=False) == 50


def test_describe_scope(pipe):
    assert pipe._describe_scope("user-1", []) == "user `user-1`"
    assert pipe._describe_scope(None, ["a", "b"], user_query="alice") == 'users matching "alice", chat ids (2)'
    assert pipe._describe_scope(None, []) == "entire workspace"


def test_extract_chat_ids_from_text(pipe):
    table = """
    | User | Chat ID | Title | Issues |
    | --- | --- | --- | --- |
    | Alice | `chat-1` | Hello | 2 null bytes |
    | Bob | `chat-2` | Hi | lone low |
    """
    assert pipe._extract_chat_ids_from_text(table) == ["chat-1", "chat-2"]


@pytest.mark.asyncio
async def test_format_stream_row(pipe):
    cache = {"user-1": "Alice"}
    row = {
        "user_id": "user-1",
        "chat_id": "chat-123",
        "title": "Hello",
        "issue_counts": {"null_bytes": 2},
        "fields": ["title"],
    }
    line = await pipe._format_stream_row(row, cache)
    assert "`chat-123`" in line
    assert "2 null bytes" in line


def test_build_scan_report_contains_table(pipe):
    scan_result = {
        "examined": 3,
        "matches": 1,
        "results": [
            {
                "chat_id": "chat-1",
                "user_id": "user-1",
                "title": "Hello",
                "updated_at": 0,
                "issue_counts": {"null_bytes": 1},
                "fields": ["title"],
            }
        ],
        "counters": {"null_bytes": 1},
        "has_more": False,
    }
    user_labels = {"user-1": "Alice"}
    report = pipe._build_scan_report(scan_result, user_labels=user_labels, scope="entire workspace")
    assert "| `chat-1` | Alice |" in report
    assert "Rows inspected: 3" in report
