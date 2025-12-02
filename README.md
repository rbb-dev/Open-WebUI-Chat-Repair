# Open WebUI Chat Repair Pipe

Open WebUI Chat Repair is a maintenance-focused function/pipe for [Open WebUI](https://github.com/open-webui/open-webui) administrators. It scans every stored chat transcript for malformed Unicode (embedded `\x00` bytes, orphaned surrogate code points, etc.), streams a live markdown table while it analyzes users, and can repair the damaged rows in-place using the exact sanitization logic that the backend expects.

The pipe is designed for “break-glass” scenarios where corrupted text is blocking PostgreSQL indexes or causing responses to crash. It emphasizes observability (per-user status, live table updates), safety (dry-run first, repair requires `confirm`), and repeatability (you can re-run the same scope as often as needed).

---

## Table of Contents
1. [Features](#features)
2. [How It Works](#how-it-works)
3. [Installation](#installation)
4. [Usage](#usage)
   - [Commands](#commands)
   - [Options](#options)
   - [Examples](#examples)
5. [Streaming Output](#streaming-output)
6. [Sanitization Rules](#sanitization-rules)
7. [Valve Reference](#valve-reference)
8. [Testing](#testing)
9. [Repository Layout](#repository-layout)
10. [Contributing](#contributing)

---

## Features
- **Workspace-wide scan:** Walks every chat row (or a filtered subset) and reports only the rows that would change after sanitization.
- **Live visibility:** Emits status updates such as “Scanning Alice’s chats (total chats: 1 234)” and streams a markdown table (`| User | Chat ID | Title | Issues |`).
- **Zero-copy repair:** Sanitizes the same fields that Open WebUI would (title, chat payload, metadata) without deleting content.
- **Automatic targeting:** `repair confirm` reuses the chat IDs from the last streamed table if you don’t provide `id=` explicitly.
- **User-aware filtering:** Scope scans and repairs by exact `user=<uuid>` or fuzzy `user_query="alice"` matches.
- **Safety nets:** Dry-run first, `repair` requires `confirm`, and optional `limit=<n>` lets you chunk large work.
- **Valve-driven behavior:** Toggle logging, default limits, and chunk sizes without editing code (see [Valve Reference](#valve-reference)).

## How It Works
1. **Command parsing:** The pipe treats every prompt as a CLI command (`scan`, `repair`, or `help`).
2. **ChatRepairService:** All database interaction stays inside `ChatRepairService`, which batches users, streams results through callbacks, and applies sanitization.
3. **Sanitization:** Strings are scanned character-by-character. `\x00` bytes are removed, orphaned UTF‑16 surrogate halves are replaced with `\ufffd`, and valid surrogate pairs are preserved.
4. **Streaming:** When `stream=true` (the Open WebUI default for functions), a background thread performs the scan while the async layer streams table rows and status updates.
5. **Repair:** The same sanitizer runs with `mutate=True`, updating `updated_at`, committing only when something actually changed, and producing a concise summary table.

## Installation
1. Ensure you are running Open WebUI **0.6.28 or newer** (matches the `required_open_webui_version` in the plugin header).
2. Clone this repository or copy `open-webui-chat-repair.py` into your Open WebUI functions directory.
3. From the Open WebUI UI, add/update the function via **Admin → Functions** and point it to this file (or use the provided Git URL).
4. After installation, the function will appear as **“Open WebUI: Chat Repair”**.

## Usage
Interact with the pipe as if it were a CLI accessible through a chat window.

### Commands
- `help` — print the in-product guide.
- `scan [options]` — stream a live table of problematic chats. Unlimited by default.
- `repair confirm [options]` — sanitize chats in-place. Requires the literal word `confirm`.

### Options
| Option | Description |
| --- | --- |
| `limit=<n>` | Cap the number of streamed rows (scan) or repairs. `limit=0` means “no cap”. |
| `user=<uuid>` / `user=me` | Restrict work to a single user (current user when using `me`). |
| `user_query="alice"` | Case-insensitive substring match against name, username, or email. Useful when you only know the human name. |
| `id=<chat-id>` | One or more comma/semicolon-separated chat IDs to inspect/repair. |
| `confirm` | Required with `repair` to avoid accidental writes. |

### Examples
```
scan                    # walk the entire DB, stream every corrupt chat
scan user=me            # focus on the current admin's chats
scan user_query="John Citizen" limit=50
repair confirm          # fix the chats that were just listed (auto-detect IDs)
repair confirm id=abc   # fix a single chat
repair confirm limit=0  # run until the scoped dataset is clean
```

## Streaming Output
- The first chunk always prints:
  ```
  Scan in progress – the table below will populate with chats that need fixing.

  | User | Chat ID | Title | Issues |
  | --- | --- | --- | --- |
  ```
- Every corrupted chat emits another row such as `| Alice | \\`12345`\\ | Broken title | 3 null bytes, 1 strings |`.
- When the background worker finishes (or hits the provided limit), a **Scan summary** section is streamed with totals and next steps.
- Status updates (`Scanning Alice's chats…`) appear once per user so that the UI stays responsive even on large installations.

## Sanitization Rules
| Character Issue | Action |
| --- | --- |
| `\x00` bytes | Removed entirely. |
| Lone high surrogate (`0xD800–0xDBFF` without a following low surrogate) | Replaced with `\ufffd`. |
| Lone low surrogate (`0xDC00–0xDFFF` without a preceding high surrogate) | Replaced with `\ufffd`. |
| Valid surrogate pairs | Preserved as-is. |
| Structured data (`list`, `tuple`, `dict`) | Traversed recursively; only mutated entries are rewritten. |

## Valve Reference
The pipe exposes runtime-tunable valves (Pydantic settings). A quick summary is below — see [`docs/VALVES.md`](docs/VALVES.md) for full guidance.

| Valve | Default | Purpose |
| --- | --- | --- |
| `ENABLE_LOGGING` | `False` | Set to `True` to get INFO logs per user scan/repair. |
| `SCAN_DEFAULT_LIMIT` | `0` | How many problematic chats to stream when the user omits `limit=` (0 = unlimited). |
| `SCAN_MAX_LIMIT` | `200` | Hard cap for `limit=` on scans. |
| `REPAIR_DEFAULT_LIMIT` | `10` | Default number of chats to repair per command (0 = unlimited). |
| `REPAIR_MAX_LIMIT` | `200` | Hard ceiling for repairs per invocation. |
| `DB_CHUNK_SIZE` | `200` | Rows fetched from PostgreSQL per batch. Increase carefully on large DBs. |

## Testing
The repository ships with an extensive pytest suite that exercises the sanitizer, parsing utilities, and streaming formatting without requiring a live Open WebUI installation.

Run the suite:
```
python -m venv .venv && source .venv/bin/activate  # optional but recommended
pip install -r requirements-dev.txt                # or pip install -r requirements.txt if you add one
pytest
```

Current coverage highlights:
- `ChatRepairService._sanitize_string` and `_sanitize_value` (null bytes, lone surrogates, nested structures)
- `_analyse_chat` mutation semantics
- Command parsing and limit clamping
- Chat ID extraction from streamed tables
- Markdown row/summary renderers (async and sync helpers)

> The tests inject lightweight stubs for `open_webui.*` modules so they can run anywhere, including CI.

Live usage:

<img width="751" height="1478" alt="image" src="https://github.com/user-attachments/assets/b334eadb-7b77-4763-8bd4-65c12dd11279" />


## Repository Layout
```
open-webui-chat-repair/
├── open-webui-chat-repair.py   # The function/pipe implementation
├── README.md                   # This document
├── docs/
│   └── VALVES.md               # Extended valve documentation
└── tests/
    ├── conftest.py             # Stubs Open WebUI modules for pytest
    ├── test_chat_repair_service.py
    └── test_pipe_utilities.py
```

## Contributing
Issues and PRs are welcome! Please include:
1. A clear description of the bug or enhancement.
2. Reproduction steps or test coverage.
3. Confirmation that `pytest` passes locally.

Happy repairing! 🛠️
