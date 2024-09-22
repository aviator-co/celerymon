# Copyright 2024 Aviator Technologies, Inc.
# SPDX-License-Identifier: MIT

from __future__ import annotations

import datetime
from collections import defaultdict

import celery

from .timer import RepeatTimer


class WorkerWatcher:
    last_updated_timestamp: datetime.datetime | None
    oldest_started_task_timestamp: dict[str, datetime.datetime]
    task_count: dict[tuple[str, str], int]

    @classmethod
    def create_started(
        cls,
        app: celery.Celery,
        interval: float,
    ) -> WorkerWatcher:
        watcher = cls(app)

        timer = RepeatTimer(interval, watcher._update)
        timer.start()

        return watcher

    def __init__(self, app: celery.Celery):
        self._inspect = app.control.inspect()
        self.last_updated_timestamp = None
        self.oldest_started_task_timestamp = dict()
        self.task_count = dict()

    def _update(self) -> None:
        oldest_timestamp: dict[str, datetime.datetime] = dict()
        task_count: dict[tuple[str, str], int] = defaultdict(int)

        # active(), reserved() and scheduled() have wrong type annotations.
        for tasks in (self._inspect.active() or {}).values():  # type: ignore[union-attr]
            for task in tasks:
                if isinstance(task["time_start"], str):
                    start_time = datetime.datetime.fromisoformat(task["time_start"])
                else:
                    start_time = datetime.datetime.fromtimestamp(task["time_start"])
                task_name = task["type"]
                task_count[("active", task_name)] += 1
                if task_name not in oldest_timestamp:
                    oldest_timestamp[task_name] = start_time
                else:
                    oldest_timestamp[task_name] = min(
                        oldest_timestamp[task_name], start_time
                    )
        for tasks in (self._inspect.reserved() or {}).values():  # type: ignore[union-attr]
            for task in tasks:
                task_count[("reserved", task["type"])] += 1
        for scheduled_tasks in (self._inspect.scheduled() or {}).values():  # type: ignore[union-attr]
            for scheduled_task in scheduled_tasks:
                task_count[("scheduled", scheduled_task["request"]["type"])] += 1

        self.last_updated_timestamp = datetime.datetime.now(tz=datetime.UTC)
        self.oldest_started_task_timestamp = oldest_timestamp
        self.task_count = task_count
