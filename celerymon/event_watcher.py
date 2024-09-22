# Copyright 2024 Aviator Technologies, Inc.
# SPDX-License-Identifier: MIT

import datetime
import threading
from collections import defaultdict
from typing import Any, Sequence

import celery.events  # type: ignore[import]
import celery.events.state  # type: ignore[import]

from .timer import RepeatTimer


class EventWatcher:
    last_received_timestamp: datetime.datetime | None
    last_received_timestamp_per_task_event: dict[tuple[str, str], datetime.datetime]
    num_events_per_task_count: dict[tuple[str, str], int]
    upper_bounds: list[float]
    task_names: set[str]
    succeeded_task_runtime_sec: list[dict[str, int]]

    @classmethod
    def create_started(
        cls,
        app: celery.Celery,
        state: celery.events.state.State,
        buckets: Sequence[float | str],
    ):
        store = cls(state, buckets)

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

        return store

    def __init__(
        self, state: celery.events.state.State, buckets: Sequence[float | str]
    ):
        self._state = state

        self.upper_bounds = [float(b) for b in buckets]
        if self.upper_bounds and self.upper_bounds[-1] != float("inf"):
            self.upper_bounds.append(float("inf"))
        self.upper_bounds.sort()
        self.task_names = set()

        self.last_received_timestamp = None
        self.last_received_timestamp_per_task_event = dict()
        self.num_events_per_task_count = defaultdict(int)
        self.succeeded_task_runtime_sec = []
        for _ in range(0, len(self.upper_bounds)):
            self.succeeded_task_runtime_sec.append(defaultdict(int))

    def on_event(self, event: dict[str, Any]):
        now = datetime.datetime.now(tz=datetime.UTC)
        self.last_received_timestamp = now

        self._state.event(event)
        event_name: str = event["type"]
        if not event_name.startswith("task-"):
            return

        task: celery.events.Task = self._state.get_or_create_task(event["uuid"])[0]
        task_name = task.name or "(UNKNOWN)"
        self.task_names.add(task_name)

        self.last_received_timestamp_per_task_event[(task_name, event_name)] = now
        self.num_events_per_task_count[(task_name, event_name)] += 1

        if event_name == "task-succeeded":
            # Not documented, but looking into the Celery codebase, the runtime
            # looks like seconds.
            runtime_sec = event["runtime"]
            for i, bound in enumerate(self.upper_bounds):
                if runtime_sec <= bound:
                    self.succeeded_task_runtime_sec[i][task_name] += 1
