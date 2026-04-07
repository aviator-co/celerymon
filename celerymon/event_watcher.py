# Copyright 2024 Aviator Technologies, Inc.
# SPDX-License-Identifier: MIT

import datetime
import logging
import threading
import time
from collections import OrderedDict, defaultdict
from typing import Any, Sequence

import celery  # type: ignore[import]

from .timer import RepeatTimer

logger = logging.getLogger(__name__)


class EventWatcher:
    _TASK_NAMES_CACHE_LIMIT = 100_000

    last_received_timestamp: datetime.datetime | None
    last_received_timestamp_per_task_event: dict[tuple[str, str], datetime.datetime]
    num_events_per_task_count: dict[tuple[str, str], int]
    upper_bounds: list[float]
    task_names: set[str]
    succeeded_task_runtime_sec: list[dict[str, int]]
    succeeded_task_runtime_sec_sum: dict[str, float]

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
                        recv = app.events.Receiver(conn, handlers={"*": store.on_event})  # type: ignore[attr-defined]
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

        thread = threading.Thread(target=run, daemon=True, name="celerymon-event-watcher")
        thread.start()

        return store

    def __init__(self, buckets: Sequence[float | str]):
        self._task_names_by_uuid: OrderedDict[str, str] = OrderedDict()

        self.upper_bounds = [float(b) for b in buckets]
        if self.upper_bounds and self.upper_bounds[-1] != float("inf"):
            self.upper_bounds.append(float("inf"))
        self.upper_bounds.sort()
        self.task_names = set()

        self.last_received_timestamp = None
        self.last_received_timestamp_per_task_event = dict()
        self.num_events_per_task_count = defaultdict(int)
        self.succeeded_task_runtime_sec = []
        self.succeeded_task_runtime_sec_sum = defaultdict(float)
        for _ in range(0, len(self.upper_bounds)):
            self.succeeded_task_runtime_sec.append(defaultdict(int))

    def on_event(self, event: dict[str, Any]):
        now = datetime.datetime.now(tz=datetime.UTC)
        self.last_received_timestamp = now

        event_name: str = event["type"]
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
            # Not documented, but looking into the Celery codebase, the runtime
            # looks like seconds.
            runtime_sec = event["runtime"]
            self.succeeded_task_runtime_sec_sum[task_name] += runtime_sec
            for i, bound in enumerate(self.upper_bounds):
                if runtime_sec <= bound:
                    self.succeeded_task_runtime_sec[i][task_name] += 1
