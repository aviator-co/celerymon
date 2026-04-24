# Copyright 2024 Aviator Technologies, Inc.
# SPDX-License-Identifier: MIT

import datetime
import logging
import threading
import time
from collections import OrderedDict, defaultdict
from typing import Any, Sequence

import celery

from .timer import RepeatTimer

logger = logging.getLogger(__name__)


class EventWatcher:
    _TASK_NAMES_CACHE_LIMIT = 100_000

    last_received_timestamp: datetime.datetime | None
    last_received_timestamp_per_task_event: dict[tuple[str, str], datetime.datetime]
    num_events_per_task_count: dict[tuple[str, str], int]
    upper_bounds: list[float]
    task_names: set[str]
    task_runtime_sec: list[dict[tuple[str, str], int]]
    task_runtime_sec_sum: dict[tuple[str, str], float]

    @classmethod
    def create_started(
        cls,
        app: celery.Celery,
        buckets: Sequence[float | str],
    ):
        store = cls(buckets)

        def run() -> None:
            backoff = 1.0
            max_backoff = 60.0
            while True:
                try:
                    with app.connection() as conn:
                        recv = app.events.Receiver(conn, handlers={"*": store.on_event})
                        logger.info("EventWatcher connected, capturing events")
                        backoff = 1.0
                        recv.capture(limit=None)
                except Exception:
                    logger.exception(
                        "EventWatcher connection lost, reconnecting in %.1fs",
                        backoff,
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

        def update_enable_event() -> None:
            try:
                app.control.enable_events()
            except Exception:
                logger.exception("Failed to enable events")

        timer = RepeatTimer(10, update_enable_event)
        timer.daemon = True
        timer.start()

        thread = threading.Thread(
            target=run, daemon=True, name="celerymon-event-watcher"
        )
        thread.start()

        return store

    def __init__(self, buckets: Sequence[float | str]):
        self._task_names_by_uuid: OrderedDict[str, str] = OrderedDict()
        self._worker_last_heartbeat: dict[str, datetime.datetime] = dict()

        self.upper_bounds = [float(b) for b in buckets]
        if self.upper_bounds and self.upper_bounds[-1] != float("inf"):
            self.upper_bounds.append(float("inf"))
        self.upper_bounds.sort()
        self.task_names = set()

        self.last_received_timestamp = None
        self.last_received_timestamp_per_task_event = dict()
        self.num_events_per_task_count = defaultdict(int)
        self.task_runtime_sec = [
            defaultdict(int) for _ in range(len(self.upper_bounds))
        ]
        self.task_runtime_sec_sum = defaultdict(float)

    def on_event(self, event: dict[str, Any]):
        now = datetime.datetime.now(tz=datetime.UTC)
        self.last_received_timestamp = now

        event_name: str = event["type"]

        if event_name.startswith("worker-"):
            self._on_worker_event(event_name, event, now)
            return

        if not event_name.startswith("task-"):
            return

        uuid: str = event["uuid"]
        if "name" in event:
            self._task_names_by_uuid.pop(uuid, None)
            self._task_names_by_uuid[uuid] = event["name"]
            while len(self._task_names_by_uuid) > self._TASK_NAMES_CACHE_LIMIT:
                self._task_names_by_uuid.popitem(last=False)

        task_name = self._task_names_by_uuid.get(uuid, "(UNKNOWN)")
        self.task_names.add(task_name)

        self.last_received_timestamp_per_task_event[(task_name, event_name)] = now
        self.num_events_per_task_count[(task_name, event_name)] += 1

        if event_name == "task-succeeded":
            self._record_task_runtime(task_name, "success", event)

        if event_name == "task-failed":
            self._record_task_runtime(task_name, "failed", event)

    def _on_worker_event(
        self,
        event_name: str,
        event: dict[str, Any],
        now: datetime.datetime,
    ) -> None:
        hostname = event.get("hostname")
        if not hostname:
            return
        if event_name == "worker-offline":
            self._worker_last_heartbeat.pop(hostname, None)
        else:
            self._worker_last_heartbeat[hostname] = now

    def online_worker_count(
        self,
        now: datetime.datetime,
        ttl_sec: int = 120,
    ) -> int:
        cutoff = now - datetime.timedelta(seconds=ttl_sec)
        return sum(1 for ts in self._worker_last_heartbeat.values() if ts > cutoff)

    def _record_task_runtime(
        self,
        task_name: str,
        result: str,
        event: dict[str, Any],
    ) -> None:
        # Celery sets `runtime` (seconds) on task-succeeded consistently, and on
        # task-failed when the task actually ran. Defensive `.get()` handles
        # failure modes where the task never executed.
        runtime_sec = event.get("runtime")
        if runtime_sec is None:
            logger.debug(
                "task event missing runtime field; task_name=%s result=%s",
                task_name,
                result,
            )
            return
        key = (task_name, result)
        self.task_runtime_sec_sum[key] += runtime_sec
        for i, bound in enumerate(self.upper_bounds):
            if runtime_sec <= bound:
                self.task_runtime_sec[i][key] += 1
