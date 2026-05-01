# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Dependency management uses [uv](https://docs.astral.sh/uv/). Python version is pinned in `.python-version` (3.13).

- Install deps: `uv sync --locked`
- Run the server locally: `uv run celerymon --broker-url=redis://localhost:6379/0 --queue=celery`
- Type check (matches CI): `uv run mypy celerymon/`
- Pyright is also configured as a dev dep: `uv run pyright celerymon/`
- Lint/format: handled by `pre-commit` (ruff + ruff-format + pyupgrade). Run `pre-commit run --all-files` or rely on the hook.
- Build the Docker image: `docker build -t celerymon .` (entrypoint is `celerymon`).

There is no test suite in this repo — CI runs only `pre-commit` and `mypy`.

## Architecture

`celerymon` is a single-process Prometheus exporter for Celery. The CLI (`celerymon/cli/__init__.py`) wires three independent **watchers** into one **collector** and serves both `/metrics` and `/healthz` from a single `wsgiref` server.

The three watchers correspond to the three observation mechanisms described in the README, and each owns its own background thread:

- `RedisWatcher` (`redis_watcher.py`) — polls Redis directly with `LLEN` for each `(queue, priority)` pair on a `RepeatTimer`. This is **the only data source for queue length** and is the reason the project is currently coupled to Redis as a broker. Priority queue keys follow Celery's convention (`queue\x06\x166` etc., see `_format_key`).
- `WorkerWatcher` (`worker_watcher.py`) — uses `app.control.inspect()` to ask workers about `active`, `reserved`, and `scheduled` tasks on a `RepeatTimer`.
- `EventWatcher` (`event_watcher.py`) — consumes the Celery event stream in a dedicated thread with auto-reconnect (exponential backoff up to 60s). A second `RepeatTimer` periodically calls `app.control.enable_events()` so newly started workers begin emitting events. To keep memory bounded under high task churn, it maintains its **own** UUID→task-name `OrderedDict` cache (`_TASK_NAMES_CACHE_LIMIT = 100_000`) instead of using Celery's built-in `State` object — this is intentional, do not replace it with `State`.

`metrics.py` defines a single `Collector` that is registered with a fresh `CollectorRegistry`. Its `collect()` reads from each watcher's public attributes. `RedisWatcher` and `WorkerWatcher` update fields atomically by reassignment, so no locking is needed for those; `EventWatcher` mutates its accumulators in-place from the event handler, so `collect()` takes defensive snapshots before iterating over them. Each watcher exposes a `last_updated_timestamp` / `last_received_timestamp` field; `_check_health` in the CLI returns 500 from `/healthz` if any of these is older than `--healthz-unhealthy-threshold-sec`.

`timer.py` is a tiny `threading.Timer` subclass that re-arms forever — used by every watcher.

### Conventions worth knowing

- All metric names are prefixed `celerymon_` and use one of three sub-namespaces (`redis_`, `inspect_`, `events_`) corresponding to the watcher that produces them. New metrics should follow this pattern.
- Watchers are constructed via a `create_started` classmethod that returns the instance with its background thread already running. The constructor itself does not start anything.
- Files carry an `SPDX-License-Identifier: MIT` header — preserve it when editing or creating files.
- Ruff is configured (in `pyproject.toml`) with isort enabled and `celerymon` as the first-party package.
