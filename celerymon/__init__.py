# Copyright 2023 Aviator Technologies, Inc.
# SPDX-License-Identifier: MIT

import collections
import datetime
import threading
from typing import Any, Iterable, Sequence

import celery
import celery.events  # type: ignore[import]
import celery.events.state  # type: ignore[import]
import prometheus_client
import redis

PRIORITY_SEP = "\x06\x16"
PRIORITY_STEPS = [0, 3, 6, 9]


class RepeatTimer(threading.Timer):
    """Timer that repeats forever."""

    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


class LastUpdatedTimestampReporter:
    """Keep track of the last updated timestamps for each data source."""

    def __init__(self):
        self.redis_last_updated = None
        self.worker_inspect_last_updated = None
        self.event_last_updated = None

    def redis_updated(self):
        self.redis_last_updated = datetime.datetime.utcnow()

    def worker_inspect_updated(self):
        self.worker_inspect_last_updated = datetime.datetime.utcnow()

    def event_updated(self):
        self.event_last_updated = datetime.datetime.utcnow()


def format_key(queue_name: str, priority: int) -> str:
    """
    Format the Celery queue Redis key.

    When Redis backend is used for Celery, it uses one key for storing the
    pending tasks. Different priority items are stored in a different key. This
    returns the Redis key for a given queue name and a priority.

    See https://stackoverflow.com/a/43420719 for details.
    """
    if not priority:
        return queue_name
    return "{0}{1}{2}".format(queue_name, PRIORITY_SEP, priority)


def start_celerymon_redis_watcher(
    client: redis.StrictRedis,
    interval: float,
    queues: Iterable[str],
    registry: prometheus_client.registry.CollectorRegistry
    | None = prometheus_client.REGISTRY,
    ts_reporter: LastUpdatedTimestampReporter | None = None,
) -> threading.Timer:
    """Start the monitoring task that uses Redis API directly."""
    last_updated_timestamp_seconds = prometheus_client.Gauge(
        "celerymon_redis_last_updated_timestamp_seconds",
        "Timestamp that the Redis data is updated.",
        unit="seconds",
        registry=registry,
        multiprocess_mode="livemax",
    )
    queue_item_count = prometheus_client.Gauge(
        "celerymon_redis_queue_item_count",
        "Number of tasks held in queues",
        ["queue_name", "priority"],
        registry=registry,
        multiprocess_mode="livemax",
    )

    def update() -> None:
        lengths: dict[tuple[str, int], float] = {}
        for queue_name in queues:
            for priority in PRIORITY_STEPS:
                key = format_key(queue_name, priority)
                lengths[(queue_name, priority)] = client.llen(key)
        last_updated_timestamp_seconds.set_to_current_time()
        queue_item_count.clear()
        for (queue_name, priority), length in lengths.items():
            queue_item_count.labels(queue_name=queue_name, priority=priority).set(
                length
            )
        if ts_reporter:
            ts_reporter.redis_updated()

    timer = RepeatTimer(interval, update)
    timer.start()
    return timer


def start_celerymon_worker_inspector(
    app: celery.Celery,
    interval: float,
    registry: prometheus_client.registry.CollectorRegistry
    | None = prometheus_client.REGISTRY,
    ts_reporter: LastUpdatedTimestampReporter | None = None,
) -> threading.Timer:
    """Start the monitoring task that uses Celery worker inspection API."""
    inspect = app.control.inspect()
    last_updated_timestamp_seconds = prometheus_client.Gauge(
        "celerymon_inspect_last_updated_timestamp_seconds",
        "Timestamp that the worker inspection data is updated.",
        unit="seconds",
        registry=registry,
        multiprocess_mode="livemax",
    )
    oldest_started_task_timestamp_seconds = prometheus_client.Gauge(
        "celerymon_inspect_oldest_started_task_timestamp_seconds",
        "Oldest timestamps for each task name for all actively running tasks",
        ["task_name"],
        unit="seconds",
        registry=registry,
        multiprocess_mode="livemin",
    )
    active_task_count = prometheus_client.Gauge(
        "celerymon_inspect_worker_held_task_count",
        "Number of tasks held in all workers",
        ["task_name", "state"],
        registry=registry,
        multiprocess_mode="livesum",
    )

    def update() -> None:
        oldest_timestamp_seconds: dict[str, float] = dict()
        task_count: dict[tuple[str, str], float] = collections.defaultdict(float)

        # active(), reserved() and scheduled() have wrong type annotations.
        for tasks in (inspect.active() or {}).values():  # type: ignore[union-attr]
            for task in tasks:
                if isinstance(task["time_start"], str):
                    start_time = datetime.datetime.fromisoformat(task["time_start"])
                else:
                    start_time = datetime.datetime.fromtimestamp(task["time_start"])
                task_name = task["type"]
                task_count[("active", task_name)] += 1
                if task_name not in oldest_timestamp_seconds:
                    oldest_timestamp_seconds[task_name] = start_time.timestamp()
                else:
                    oldest_timestamp_seconds[task_name] = min(
                        oldest_timestamp_seconds[task_name], start_time.timestamp()
                    )
        for tasks in (inspect.reserved() or {}).values():  # type: ignore[union-attr]
            for task in tasks:
                task_count[("reserved", task["type"])] += 1
        for scheduled_tasks in (inspect.scheduled() or {}).values():  # type: ignore[union-attr]
            for scheduled_task in scheduled_tasks:
                task_count[("scheduled", scheduled_task["request"]["type"])] += 1

        last_updated_timestamp_seconds.set_to_current_time()
        oldest_started_task_timestamp_seconds.clear()
        active_task_count.clear()
        for task_name, sec in oldest_timestamp_seconds.items():
            oldest_started_task_timestamp_seconds.labels(task_name=task_name).set(sec)
        for (state, task_name), count in task_count.items():
            active_task_count.labels(task_name=task_name, state=state).set(count)

        if ts_reporter:
            ts_reporter.worker_inspect_updated()

    timer = RepeatTimer(interval, update)
    timer.start()
    return timer


def start_celerymon_event_receiver(
    app: celery.Celery,
    registry: prometheus_client.registry.CollectorRegistry
    | None = prometheus_client.REGISTRY,
    buckets: Sequence[float | str] = prometheus_client.Histogram.DEFAULT_BUCKETS,
    ts_reporter: LastUpdatedTimestampReporter | None = None,
) -> threading.Thread:
    """Start the monitoring task that uses Celery Event API."""
    store = EventStore(
        # This has a wrong type annotation.
        app.events.State(),  # type: ignore[attr-defined]
        registry=registry,
        buckets=buckets,
        ts_reporter=ts_reporter,
    )

    def run() -> None:
        with app.connection() as conn:
            # This has a wrong type annotation.
            recv = app.events.Receiver(conn, handlers={"*": store.on_event})  # type: ignore[attr-defined]
            recv.capture(limit=None)

    def update_enable_event() -> None:
        app.control.enable_events()

    timer = RepeatTimer(10, update_enable_event)
    timer.start()

    thread = threading.Thread(target=run)
    thread.start()
    return thread


class EventStore:
    def __init__(
        self,
        state: celery.events.state.State,
        registry: prometheus_client.registry.CollectorRegistry
        | None = prometheus_client.REGISTRY,
        buckets: Sequence[float | str] = prometheus_client.Histogram.DEFAULT_BUCKETS,
        ts_reporter: LastUpdatedTimestampReporter | None = None,
    ):
        self._state = state
        self._last_received_timestamp_seconds = prometheus_client.Gauge(
            "celerymon_events_last_received_timestamp_seconds",
            "Last received timestamps for each task name and event name.",
            ["task_name", "event_name"],
            unit="seconds",
            registry=registry,
            multiprocess_mode="max",
        )
        self._count = prometheus_client.Counter(
            "celerymon_events_count",
            "The task event count per task name and event name.",
            ["task_name", "event_name"],
            registry=registry,
        )
        self._success_task_runtime_seconds = prometheus_client.Histogram(
            "celerymon_events_success_task_runtime_seconds",
            "The task runtime per task name for finished success tasks.",
            ["task_name"],
            unit="seconds",
            registry=registry,
            buckets=buckets,
        )
        self._ts_reporter = ts_reporter

    def on_event(self, event: dict[str, Any]):
        if self._ts_reporter:
            self._ts_reporter.event_updated()

        self._state.event(event)
        event_name: str = event["type"]
        if not event_name.startswith("task-"):
            return
        task: celery.events.Task = self._state.get_or_create_task(event["uuid"])[0]
        task_name = task.name or "(UNKNOWN)"

        self._last_received_timestamp_seconds.labels(
            task_name=task_name, event_name=event_name
        ).set_to_current_time()
        self._count.labels(task_name=task_name, event_name=event_name).inc()
        if event_name == "task-succeeded":
            # Not documented, but looking into the Celery codebase, the runtime
            # looks like seconds.
            self._success_task_runtime_seconds.labels(task_name=task_name).observe(
                event["runtime"]
            )
