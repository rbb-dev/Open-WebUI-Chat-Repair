# Valve Reference

The Chat Repair pipe exposes a handful of environment-configurable knobs (“valves”) via the `Pipe.Valves` Pydantic model. Administrators can override them in Open WebUI’s **Function → Valves** UI without touching the source file.

Each valve is validated (min/max) before the pipe boots. Invalid values fall back to defaults.

| Valve | Type | Default | Range | When to change |
| --- | --- | --- | --- | --- |
| `ENABLE_LOGGING` | bool | `False` | `True`/`False` | Enable INFO logs per user scan/repair when you need audit trails. Leave `False` to minimize noise. |
| `SCAN_DEFAULT_LIMIT` | int | `0` | `0 – 5000` | Set a non-zero default if you prefer short spot-checks instead of full-database scans. `0` keeps scans unlimited unless the operator specifies `limit=` manually. |
| `SCAN_MAX_LIMIT` | int | `200` | `25 – 1000` | Absolute ceiling for `limit=` on scans. Raise carefully if you expect more than 200 bad chats and want a single run to stream them all. |
| `REPAIR_DEFAULT_LIMIT` | int | `10` | `0 – 200` | Default number of chats the `repair` command modifies when the operator omits `limit=`. Use `0` to make “repair everything” the default. |
| `REPAIR_MAX_LIMIT` | int | `200` | `10 – 1000` | Upper bound for `limit=` on repairs. Keeps accidental bulk edits in check. |
| `DB_CHUNK_SIZE` | int | `200` | `50 – 1000` | Number of chat rows fetched per SQLAlchemy batch during scans/repairs. Increase on high-latency databases; decrease if you observe long transactions or memory pressure. |

## Usage Notes
- **Changes apply instantly.** Once you tweak a valve through the UI, the next invocation of the pipe will pick it up.
- **Safety first.** The two “MAX” valves exist to prevent runaway scans/repairs started by a simple typo. Keep them conservative unless you have strong operational safeguards.
- **Chunk size trade-offs.** Larger `DB_CHUNK_SIZE` values reduce the number of database round-trips but increase memory usage and the time a transaction stays open.
- **Logging valve.** When `ENABLE_LOGGING=True`, the pipe sets the module logger to `DEBUG` and propagates logs to the hosting application. This is helpful while triaging but should be turned off once the environment is stable.

## Updating Valves Programmatically
Valves are regular Pydantic model fields, so you can also override them when instantiating the pipe manually:

```python
from open_webui_chat_repair import Pipe

pipe = Pipe()
pipe.valves.SCAN_DEFAULT_LIMIT = 100
pipe._apply_logging_valve()  # re-evaluate logging level after manual changes
```

However, in production you typically adjust them via Open WebUI’s UI to keep settings in sync across replicas.
